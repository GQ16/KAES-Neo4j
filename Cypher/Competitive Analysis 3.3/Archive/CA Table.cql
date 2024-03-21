CALL{
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca)
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:LocationGroup)<-[:OWNS_STATION_GROUP]-(oca2)
    MATCH (dca)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:LocationGroup)<-[:OWNS_STATION_GROUP]-(dca2)

    WITH oca, oca2, dca, dca2 
    WITH collect(DISTINCT oca) + collect(DISTINCT oca2) + collect(DISTINCT dca) + collect(DISTINCT dca2) AS carriers 
    WITH apoc.coll.toSet(carriers) AS carrierSet

    UNWIND carrierSet AS ca
    MATCH (s:STCC)-[:REPRESENTS]->(:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH (s)-[r:IN_STCCGROUP]->(sg:STCCGroup)<-[:HAS_STCCGROUP]-(ca) 
    //This collection step is for the case when a Product group has multiple stccs
    WITH collect(DISTINCT toString(s.id)) as stcc, collect(DISTINCT sg.id) as groups
    RETURN stcc + groups AS validRateStccs
}
CALL{
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca:Carrier)
    MATCH (dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    
    WITH ors, drs, oca, dca
    MATCH (i:Interchange)
    WHERE 1 = 1
    AND EXISTS{(oca)-[:OWNS_STATION]->()-[:AT_INTERCHANGE]->(i)<-[:AT_INTERCHANGE]-()<-[:OWNS_STATION]-(dca)}
    AND COUNT{(i)<-[:AT_INTERCHANGE]-()} > toInteger($neodash_interchange_stations_count)

    WITH collect(DISTINCT i) AS validCarrierInterchanges
    , avg(ors.coordinate.latitude) AS avgOriginLat, avg(ors.coordinate.longitude) AS avgOriginLong
    , avg(drs.coordinate.latitude) AS avgDestLat, avg(drs.coordinate.longitude) AS avgDestLong

    //Logic for defining corners of BBox for NorthWestern Hemisphere
    WITH validCarrierInterchanges
    , CASE 
        WHEN avgOriginLat > avgDestLat
        THEN {upper:avgOriginLat, lower: avgDestLat}
        ELSE {lower:avgOriginLat, upper: avgDestLat}
    END AS latBounds
    , CASE 
        WHEN avgOriginLong < avgDestLong 
        THEN {left:avgOriginLong, right: avgDestLong}
        ELSE {right:avgOriginLong, left: avgDestLong}
    END AS longBounds

    WITH validCarrierInterchanges
    , point({latitude:latBounds.lower-5, longitude: longBounds.left-5}) AS lowerLeft
    , point({latitude:latBounds.upper+5, longitude: longBounds.right+5}) AS upperRight

    WITH [x IN validCarrierInterchanges WHERE point.withinBBox(x.coordinate,lowerLeft,upperRight)] AS validInterchanges
    RETURN validInterchanges
}
WITH validInterchanges, validRateStccs
CALL{
    WITH validInterchanges, validRateStccs

    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    WITH validInterchanges, validRateStccs, CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:LocationGroup)
    MATCH dp =(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:LocationGroup)

    WITH collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , validInterchanges
        , validRateStccs

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH origin, destination, validInterchanges, validRateStccs

    MATCH p = 
    (origin)
    (
        (s)<-[f:FROM]-(:RailRoute)-[:TO]->(e) 
        WHERE f.stcc IN validRateStccs
        AND point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){0,1}
    (:LocationGroup|RailStation)-[:HAS_STATION_IN|AT_INTERCHANGE]->(i)<-[:HAS_STATION_IN|AT_INTERCHANGE]-(:LocationGroup|RailStation)
    (
        (s2)<-[f2:FROM]-(:RailRoute)-[:TO]->(e2) 
        WHERE f2.stcc IN validRateStccs
        AND point.distance(destination.coordinate, e2.coordinate) < point.distance(destination.coordinate, s2.coordinate)
    ){0,1}
    (destination)
    WHERE i IN validInterchanges

    WITH DISTINCT p, [x IN nodes(p) WHERE x:RailRoute] AS routes, i
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    UNWIND routes AS route
    MATCH (route)-[:HAS_CURRENT_RATE]->(ra:RailRate)
    WITH p, route, i,
        CASE 
            WHEN toLower(ra.uom) = 'car'
            THEN ra.rate/97
            ELSE ra.rate
        END AS rate
    WITH p, route, i, min(rate) AS lowestRate
    ORDER BY i.stationCount DESC
    WITH 
    nodes(p)[0].id AS origin
    , nodes(p)[-1].id AS dest
    , i.id AS interchange
    , round(reduce(price = 0, x IN collect(lowestRate) | price + x ),2) AS price
    , p
    RETURN DISTINCT origin, collect(interchange)[0] AS interchanges, dest, price
    //Aggregate by the one strongest interchange if multiple all have the same price
    ORDER BY price

    UNION

    WITH validRateStccs
	
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    WITH validRateStccs, CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:LocationGroup)
    MATCH dp =(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:LocationGroup)

    WITH collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , validRateStccs

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH origin, destination, validRateStccs

    MATCH p = 
    (origin)
    (
        (s)<-[f:FROM]-(:RailRoute)-[:TO]->(e) 
        WHERE f.stcc IN validRateStccs
        AND point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){1,3}
    (destination)

    WITH DISTINCT p, [x IN nodes(p) WHERE x:RailRoute] AS routes
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    UNWIND routes AS route
    MATCH (route)-[:HAS_CURRENT_RATE]->(ra:RailRate)
    WITH p, route, 
        CASE 
            WHEN toLower(ra.uom) = 'car'
            THEN ra.rate/97
            ELSE ra.rate
        END AS rate
    WITH p, route, min(rate) AS lowestRate
    WITH 
    nodes(p)[0].id AS origin
    , nodes(p)[-1].id AS dest
    , "" AS interchanges
    , round(reduce(price = 0, x IN collect(lowestRate) | price + x ),2) AS price
    , p
    RETURN DISTINCT origin, interchanges, dest, price
    ORDER BY price
}
RETURN origin, interchanges, dest, price
ORDER BY price