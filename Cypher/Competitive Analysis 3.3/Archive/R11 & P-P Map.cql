//Based on Initial tests, this one is more performant than #2
CALL{
    MATCH (oz:ZipCode{id:$neodash_zipcode_id_1, country:$neodash_country_id_1})<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca)
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
    MATCH (oz:ZipCode{id:$neodash_zipcode_id_1, country:$neodash_country_id_1})<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca:Carrier)
    MATCH (dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    
    WITH ors, drs, oca, dca
    MATCH (i:Interchange)
    WHERE 1 = 1
    AND EXISTS{(oca)-[:OWNS_STATION]->()-[:AT_INTERCHANGE]->(i)<-[:AT_INTERCHANGE]-()<-[:OWNS_STATION]-(dca)}
    AND i.stationCount > toInteger($neodash_interchange_stations_count)

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
CALL {
    WITH validInterchanges, validRateStccs
    MATCH op=(oz:ZipCode{id:$neodash_zipcode_id_1, country:$neodash_country_id_1})<-[:IN_ZIPCODE]-(ors:RailStation)
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

    WITH DISTINCT p, [x IN nodes(p) WHERE x:RailRoute] AS routes
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    WITH [x IN nodes(p) WHERE NOT x:RailRoute] AS stops
    WITH apoc.coll.pairs(stops) AS stopPairList

    UNWIND stopPairList as stopPair
    WITH DISTINCT stopPair
    WHERE stopPair[1] IS NOT NULL
    RETURN stopPair, apoc.create.vRelationship(stopPair[0],'RAIL_TO',{},stopPair[1]) AS vRel

    UNION

    WITH validRateStccs
    MATCH op=(oz:ZipCode{id:$neodash_zipcode_id_1, country:$neodash_country_id_1})<-[:IN_ZIPCODE]-(ors:RailStation)
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

    WITH [x IN nodes(p) WHERE NOT x:RailRoute] AS stops
    WITH apoc.coll.pairs(stops) AS stopPairList

    UNWIND stopPairList as stopPair
    WITH DISTINCT stopPair
    WHERE stopPair[1] IS NOT NULL
    RETURN stopPair, apoc.create.vRelationship(stopPair[0],'RAIL_TO',{},stopPair[1]) AS vRel
}
RETURN stopPair, vRel