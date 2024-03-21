CALL{
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca)
    MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)<-[:OWNS_STATION_GROUP]-(oca2)
    MATCH (dca)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)<-[:OWNS_STATION_GROUP]-(dca2)

	WHERE CASE 
      WHEN NOT isEmpty($neodash_zipcode_id_1) AND NOT isEmpty($neodash_country_id_1)
      THEN oz.id = $neodash_zipcode_id_1 AND oz.country = $neodash_country_id_1 
      ELSE TRUE 
    END

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
    MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})

	WHERE CASE 
      WHEN NOT isEmpty($neodash_zipcode_id_1) AND NOT isEmpty($neodash_country_id_1)
      THEN oz.id = $neodash_zipcode_id_1 AND oz.country = $neodash_country_id_1 
      ELSE TRUE 
    END

    WITH ors, drs, oca, dca
    MATCH (i:Interchange)
    WHERE 1 = 1
    AND EXISTS{(oca)-[:OWNS_STATION]->()-[:AT_INTERCHANGE]->(i)<-[:AT_INTERCHANGE]-()<-[:OWNS_STATION]-(dca)}
    // AND COUNT{(i)<-[:AT_INTERCHANGE]-()} > toInteger($neodash_interchange_stations_count)

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
    MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    MATCH dp =(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)

	WHERE CASE 
      WHEN NOT isEmpty($neodash_zipcode_id_1) AND NOT isEmpty($neodash_country_id_1)
      THEN oz.id = $neodash_zipcode_id_1 AND oz.country = $neodash_country_id_1 
      ELSE TRUE 
    END

    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , validInterchanges
        , validRateStccs

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH l,dz,origin, destination, validInterchanges, validRateStccs

    MATCH p = 
    (origin)
    (
        (s)<-[f:FROM]-(:RailRoute)-[:TO]->(e) 
        WHERE f.stcc IN validRateStccs
        AND point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){1,1}
    (:StationGroup|RailStation)-[:HAS_STATION_IN|AT_INTERCHANGE]->(i)<-[:HAS_STATION_IN|AT_INTERCHANGE]-(:StationGroup|RailStation)
    (
        (s2)<-[f2:FROM]-(:RailRoute)-[:TO]->(e2) 
        WHERE f2.stcc IN validRateStccs
        AND point.distance(destination.coordinate, e2.coordinate) < point.distance(destination.coordinate, s2.coordinate)
    ){1,1}
    (destination)
    WHERE i IN validInterchanges

    WITH DISTINCT l,dz,p, [x IN nodes(p) WHERE x:RailRoute] AS routes, i
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    UNWIND routes AS route
	MATCH (route)-[:HAS_CURRENT_RATE]->(ra:RailRate)-[:IN_CURRENCY]->(rC)
	MATCH (oc:Country)<-[:IN_COUNTRY]-()<-[:FROM]-(route)-[:TO]->()-[:IN_COUNTRY]->(dc:Country)
	, (oc)<-[:FOR_ORIGIN_COUNTRY]-(f:RailFSC)-[:FOR_DESTINATION_COUNTRY]->(dc)
	, (route)<-[:HAS_ROUTE]-(ca:Carrier)-[:HAS_FSC]->(f)
	, (f)-[:IN_CURRENCY]->(fC)
	MATCH (tC:Currency{id: $neodash_currency_id})
	OPTIONAL MATCH (fC)-[fe:HAS_EXCHANGE_RATE]->(tC)
	OPTIONAL MATCH (rC)-[re:HAS_EXCHANGE_RATE]->(tC)
	OPTIONAL MATCH (lqUOM:MassUOM {id:'ST'})-[cr:HAS_CONVERSION_RATE]->(:MassUOM{id: $neodash_massuom_id})
	WHERE (NOT (f)-[:FOR_PRODUCTGROUP]->() OR (f)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name}))
    
	WITH l, dz, p, route, i, f.rate*coalesce(fe.rate,1) AS fsc, coalesce(route.distance,0) AS miles, toLower(ra.uom) AS rateType
	, ra.rate AS baseRate, coalesce(re.rate,1) AS rateCurExch, coalesce(cr.rate,1) AS massConvRate, 97 AS carVol
	
	WITH *,
        CASE rateType 
            WHEN 'ton' THEN baseRate * rateCurExch * massConvRate
            WHEN 'car' THEN baseRate * rateCurExch * massConvRate / carVol
			WHEN 'mileage' THEN baseRate * rateCurExch * miles * massConvRate / carVol
			WHEN 'mile' THEN baseRate * rateCurExch * miles * massConvRate / carVol
			ELSE 0
        END AS perTonRate
	
	WITH l, dz, p, route, i, min(perTonRate) AS lowestRate, fsc/carVol AS perTonFuel, miles
	WITH l, dz, p, route, i, {lowestRate:lowestRate, fsc:perTonFuel, miles:miles} AS routeCostMap
	
	WITH l,dz,
    nodes(p)[0].id AS origin
    , nodes(p)[-1].id AS dest
    , i.id AS interchange
	, round(reduce(price = 0, x IN collect(routeCostMap) | price + x.lowestRate + (x.fsc * x.miles) ),2) AS price
    , p
    RETURN DISTINCT l,dz,origin, collect(interchange)[0] AS interchanges, dest, price
    //Aggregate by the one strongest interchange if multiple all have the same price
    ORDER BY price

    UNION

    WITH validRateStccs
	
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    WITH validRateStccs, CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
    MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    MATCH dp =(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)

	WHERE CASE 
      WHEN NOT isEmpty($neodash_zipcode_id_1) AND NOT isEmpty($neodash_country_id_1)
      THEN oz.id = $neodash_zipcode_id_1 AND oz.country = $neodash_country_id_1 
      ELSE TRUE 
    END

    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , validRateStccs

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH l,dz,origin, destination, validRateStccs

    MATCH p = 
    (origin)
    (
        (s)<-[f:FROM]-(:RailRoute)-[:TO]->(e) 
        WHERE f.stcc IN validRateStccs
        AND point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){1,3}
    (destination)

    WITH DISTINCT l,dz,p, [x IN nodes(p) WHERE x:RailRoute] AS routes
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    UNWIND routes AS route
    MATCH (route)-[:HAS_CURRENT_RATE]->(ra:RailRate)-[:IN_CURRENCY]->(rC)
	MATCH (oc:Country)<-[:IN_COUNTRY]-()<-[:FROM]-(route)-[:TO]->()-[:IN_COUNTRY]->(dc:Country)
	, (oc)<-[:FOR_ORIGIN_COUNTRY]-(f:RailFSC)-[:FOR_DESTINATION_COUNTRY]->(dc)
	, (route)<-[:HAS_ROUTE]-(ca:Carrier)-[:HAS_FSC]->(f)
	, (f)-[:IN_CURRENCY]->(fC)
	MATCH (tC:Currency{id: $neodash_currency_id})
	OPTIONAL MATCH (fC)-[fe:HAS_EXCHANGE_RATE]->(tC)
	OPTIONAL MATCH (rC)-[re:HAS_EXCHANGE_RATE]->(tC)
	OPTIONAL MATCH (lqUOM:MassUOM {id:'ST'})-[cr:HAS_CONVERSION_RATE]->(:MassUOM{id: $neodash_massuom_id})
	WHERE (NOT (f)-[:FOR_PRODUCTGROUP]->() OR (f)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name}))
    
	WITH l, dz, p, route, f.rate*coalesce(fe.rate,1) AS fsc, coalesce(route.distance,0) AS miles, toLower(ra.uom) AS rateType
	, ra.rate AS baseRate, coalesce(re.rate,1) AS rateCurExch, coalesce(cr.rate,1) AS massConvRate, 97 AS carVol
	
	WITH *,
        CASE rateType 
            WHEN 'ton' THEN baseRate * rateCurExch * massConvRate
            WHEN 'car' THEN baseRate * rateCurExch * massConvRate / carVol
			WHEN 'mileage' THEN baseRate * rateCurExch * miles * massConvRate / carVol
			WHEN 'mile' THEN baseRate * rateCurExch * miles * massConvRate / carVol
			ELSE 0
        END AS perTonRate
	
	WITH l, dz, p, route, min(perTonRate) AS lowestRate, fsc/carVol AS perTonFuel, miles
	WITH l, dz, p, route, {lowestRate:lowestRate, fsc:perTonFuel, miles:miles} AS routeCostMap
    
    WITH l,dz
    , nodes(p)[0].id AS origin
    , nodes(p)[-1].id AS dest
	, round(reduce(price = 0, x IN collect(routeCostMap) | price + x.lowestRate + (x.fsc * x.miles) ),2) AS price
    , p
    RETURN DISTINCT l,dz,origin, "" AS interchanges, dest, price
    ORDER BY price
}
WITH l,dz,origin, interchanges, dest, price
ORDER BY price
WITH l.name AS originLocation, $neodash_citystate_id_2 AS destinationCity, dz.id AS destinationZip, ceil(min(price)) AS freight
RETURN originLocation, destinationCity, freight