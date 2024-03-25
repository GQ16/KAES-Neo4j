CALL{
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    return CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
}
WITH products
CALL{
    with products

    MATCH (dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    MATCH (p:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-()<-[:HAS_OCCUPANT]-(l:Location)//-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    MATCH (l)-[:IN_ZIPCODE]->()<-[:IN_ZIPCODE]-(ors:RailStation)//<-[:OWNS_STATION]-(oca:Carrier)
    MATCH (dca)-[:USES_INTERCHANGE]->(i:Interchange)
    WHERE p IN products AND l.id = $neodash_location_id_1
    // AND i.stationCount > 7
    
    // //We leave this match after the with in order to guide the cypher query planner
    // WITH DISTINCT i, oca, ors, drs
    // MATCH (i)<-[:USES_INTERCHANGE]-(oca)

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
WITH products, validInterchanges

    MATCH (p:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)
    , (l)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
    , (o)<-[:IS_OCCUPANT]-(party:Party)
    MATCH (drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    // MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    WHERE p IN products AND l.id = $neodash_location_id_1

    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)

    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , validInterchanges

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH DISTINCT l,dz,origin, destination, validInterchanges
CALL {
    WITH l,dz,origin, destination

    MATCH p = 
    (origin)
    (
        (s)<-[:`UREA_FROM`]-(:RailRoute)-[:`UREA_TO`]->(e) 
        WHERE point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){1,2}
    (destination)
    
    RETURN DISTINCT p, [x IN nodes(p) WHERE x:RailRoute] AS routes

    UNION

    WITH l,dz,origin, destination, validInterchanges

    MATCH p = 
    (origin)
    (
        (s)<-[:`UREA_FROM`]-(:RailRoute)-[:`UREA_TO`]->(e) 
        WHERE point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){1,1}
    (:StationGroup|RailStation)-[:HAS_STATION_IN|AT_INTERCHANGE]->(i)<-[:HAS_STATION_IN|AT_INTERCHANGE]-(:StationGroup|RailStation)
    (
        (s2)<-[:`UREA_FROM`]-(:RailRoute)-[:`UREA_TO`]->(e2) 
        WHERE point.distance(destination.coordinate, e2.coordinate) < point.distance(destination.coordinate, s2.coordinate)
    ){1,1}
    (destination)
    WHERE i IN validInterchanges

    RETURN DISTINCT p, [x IN nodes(p) WHERE x:RailRoute] AS routes
}
WITH l, dz, p, routes
//Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    UNWIND routes AS route

    MATCH (route)-[:HAS_CURRENT_RATE]->(ra:RailRate)-[:IN_CURRENCY]->(rC)
	MATCH (oc:Country)<-[:IN_COUNTRY]-()<-[:FROM]-(route)-[:TO]->()-[:IN_COUNTRY]->(dc:Country)
	, (oc)<-[:FOR_ORIGIN_COUNTRY]-(f:RailFSC)-[:FOR_DESTINATION_COUNTRY]->(dc)
	, (route)<-[:HAS_ROUTE]-(ca:Carrier)-[:HAS_FSC]->(f)
	, (f)-[:IN_CURRENCY]->(fC)
	MATCH (tC:Currency{id: $neodash_currency_id})
    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
	OPTIONAL MATCH (fC)-[fe:HAS_EXCHANGE_RATE]->(tC)
	OPTIONAL MATCH (rC)-[re:HAS_EXCHANGE_RATE]->(tC)
	OPTIONAL MATCH (lqUOM:MassUOM {id:'ST'})-[cr:HAS_CONVERSION_RATE]->(:MassUOM{id: $neodash_massuom_id})
	WHERE (NOT (f)-[:FOR_PRODUCTGROUP]->() OR (f)-[:FOR_PRODUCTGROUP]->(lpg))

	WITH l, dz, p, route, f.rate*coalesce(fe.rate,1) AS fsc, coalesce(route.distance,0) AS miles, toLower(ra.uom) AS rateType
	, ra.rate AS baseRate, coalesce(re.rate,1) AS rateCurExch, coalesce(cr.rate,1) AS massConvRate, coalesce(lpg.railCarVol,1) AS carVol
	
	WITH *,
        CASE rateType 
            WHEN 'ton' THEN (baseRate * rateCurExch) / massConvRate
            WHEN 'car' THEN (baseRate * rateCurExch) / (carVol * massConvRate)
			WHEN 'mileage' THEN (baseRate * rateCurExch * miles) / (carVol * massConvRate)
			WHEN 'mile' THEN (baseRate * rateCurExch * miles) / (carVol * massConvRate)
			ELSE 0
        END AS perTonRate
	
	WITH l, dz, p, route, min(perTonRate) AS lowestRate, fsc/carVol AS perTonFuel, miles
	WITH l, dz, p, route, {lowestRate:lowestRate, fsc:perTonFuel, miles:miles} AS routeCostMap
	
    WITH p,
	round(reduce(price = 0, x IN collect(routeCostMap) | price + x.lowestRate + (x.fsc * x.miles) ),2) AS price

RETURN DISTINCT 
[x IN nodes(p) WHERE NOT x:RailRoute|
    CASE
        WHEN x:RailStation THEN "Station: " + x.station_name
        WHEN x:StationGroup THEN "Group: " + x.id
        WHEN x:Interchange THEN "Interchange: " + x.id
    END
] AS stops,
price
ORDER BY price