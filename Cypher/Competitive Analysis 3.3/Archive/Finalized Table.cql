PROFILE
CALL{
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    return CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
}
WITH products
//MARK: Interchanges
CALL{ 
    with products

    MATCH (dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    MATCH (dz)-[:IN_STATE]->(ds)<-[:COMPETES_IN]-(l:Location)
    MATCH (p:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-()<-[:HAS_OCCUPANT]-(l)
    MATCH (l)-[:IN_ZIPCODE]->()<-[:IN_ZIPCODE]-(ors:RailStation)//<-[:OWNS_STATION]-(oca:Carrier)
    MATCH (dca)-[:USES_INTERCHANGE]->(i:Interchange)
    WHERE p IN products 
    AND (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    AND CASE WHEN isEmpty($neodash_location_id) THEN TRUE ELSE l.id = $neodash_location_id END
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
    , (dz)-[:IN_STATE]->(:StateOrProvince)<-[:COMPETES_IN]-(o)
    WHERE p IN products 
    AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'RAIL'})

    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)

	CALL{
		WITH o
		OPTIONAL MATCH (o)<-[fo:FOR_OCCUPANT]-(m:MPI)-[fp:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
		, (m)-[:FOR_MODE]->(:Mode{id:'RAIL'})
		WHERE m.startDate <= date($neodash_ship_date) <= m.endDate
		
		WITH m
		ORDER BY m.createdDate DESC LIMIT 1	//Get latest MPI

		OPTIONAL MATCH (m)-[:IN_CURRENCY]->(mpiC:Currency)
		OPTIONAL MATCH (mpiC)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
		OPTIONAL MATCH (m)-[:HAS_QTY_UOM]->(mpiU:MassUOM)
		OPTIONAL MATCH (mpiU)-[conv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})
		
		WITH 
		CASE m.type
			WHEN 'DESTINATION' THEN coalesce(m.singularPrice-m.freight,((m.minPrice+m.maxPrice)/2) -m.freight) 
			WHEN 'ORIGIN' THEN coalesce(m.singularPrice,(m.minPrice+m.maxPrice)/2) 
			ELSE NULL
		END AS indPrice
			, coalesce(exch.rate, 1) AS exchRate
			, coalesce(conv.rate,1) AS convRate
		RETURN indPrice * exchRate / convRate AS indication
	}

    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , party.name AS occupant,  [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes
        , validInterchanges, indication

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH DISTINCT l,dz,origin, destination, validInterchanges, occupant, occupantTypes, indication
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
        (s)<-[:`UREA_FROM`]-(rr1:RailRoute)-[:`UREA_TO`]->(e) 
        WHERE point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
        AND rr1.rule_11_at_destination_allowed 
        AND NOT rr1.rule_11_at_origin_required
    ){1,1}
    (:StationGroup|RailStation)-[:HAS_STATION_IN|AT_INTERCHANGE]->(i)<-[:HAS_STATION_IN|AT_INTERCHANGE]-(:StationGroup|RailStation)
    (
        (s2)<-[:`UREA_FROM`]-(rr2:RailRoute)-[:`UREA_TO`]->(e2) 
        WHERE point.distance(destination.coordinate, e2.coordinate) < point.distance(destination.coordinate, s2.coordinate)
        AND rr2.rule_11_at_origin_allowed 
        AND NOT rr2.rule_11_at_destination_required
    ){1,1}
    (destination)
    WHERE i IN validInterchanges

    RETURN DISTINCT p, [x IN nodes(p) WHERE x:RailRoute] AS routes
}
WITH l, dz, occupant, occupantTypes, p, routes, indication
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
	WHERE (NOT (f)-[:FOR_PRODUCTGROUP]->() OR (f)-[:FOR_PRODUCTGROUP]->(lpg))
    AND f.expirationDate IS NOT NULL
	
	OPTIONAL MATCH (fC)-[fe:HAS_EXCHANGE_RATE]->(tC)
	OPTIONAL MATCH (rC)-[re:HAS_EXCHANGE_RATE]->(tC)
	OPTIONAL MATCH (lqUOM:MassUOM {id:'ST'})-[cr:HAS_CONVERSION_RATE]->(:MassUOM{id: $neodash_massuom_id})

	WITH l, dz, p, route,  occupant, occupantTypes, f.rate*coalesce(fe.rate,1) AS fsc, coalesce(route.distance,0) AS miles, toLower(ra.uom) AS rateType
	, ra.rate AS baseRate, coalesce(re.rate,1) AS rateCurExch, coalesce(cr.rate,1) AS massConvRate, coalesce(lpg.railCarVol,1) AS carVol, indication
	
	WITH *,
        CASE rateType 
            WHEN 'ton' THEN (baseRate * rateCurExch) / massConvRate
            WHEN 'car' THEN (baseRate * rateCurExch) / (carVol * massConvRate)
			WHEN 'mileage' THEN (baseRate * rateCurExch * miles) / (carVol * massConvRate)
			WHEN 'mile' THEN (baseRate * rateCurExch * miles) / (carVol * massConvRate)
			ELSE 0
        END AS perTonRate
	
	WITH l, dz, p, route, min(perTonRate) AS lowestRate, fsc/carVol AS perTonFuel, miles, occupant, occupantTypes, indication
	WITH l, dz, p, route, {lowestRate:lowestRate, fsc:perTonFuel, miles:miles} AS routeCostMap, occupant, occupantTypes, indication
	
	WITH l,dz, occupant, occupantTypes, indication
    , nodes(p)[0].id AS origin
    , nodes(p)[-1].id AS dest
	, round(reduce(price = 0, x IN collect(routeCostMap) | price + x.lowestRate + (x.fsc * x.miles) ),2) AS price
    , p

    WITH l.name AS originLocation, dz.id AS destinationZip, min(price) AS freight, occupant, occupantTypes, indication

RETURN originLocation, occupant, occupantTypes, "RAIL" AS mode, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight,3) ELSE round(freight,2) END AS freight, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(indication,3) ELSE round(indication,2) END AS indication, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight + indication,3) ELSE round(freight + indication, 2) END AS deliveredPrice 
ORDER BY freight

UNION

    MATCH (selProd:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    MATCH (selDestZip:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    MATCH (selDestCity:CityState{id:$neodash_citystate_id_2, country:$neodash_country_id_2})

    CALL apoc.case(
        [
            selProd.name IN ['UREA', 'MAP', 'DAP', 'PHOSPHATE', 'DRY', 'POTASH', 'SUPERU'],
            "
                MATCH (o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)
                , (l)-[:IN_CITY]->(ocs:CityState)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(dz)
                , (tr)-[:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name:'DRY'})
                , (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
                , (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
                , (oz)-[:IN_COUNTRY]->(c:Country)
                , (tFSC:TruckFSC)-[:FOR_PRODUCTGROUP]->(lpg)
                , (p:Party)-[:IS_OCCUPANT]->(o)

                WHERE 1=1
                AND cr.distanceLower < tmt.distance <= cr.distanceUpper
                AND date($neodash_ship_date) <= tFSC.expirationDate
                AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
                AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
                AND (
                    (prod)<-[:HAS_PRODUCTGROUP]-(o)
                    OR (lpg)<-[:HAS_PRODUCTGROUP]-(o)
                )
                RETURN l, o, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, tmt, tFSC
            ",
            selProd.name IN ['LIQUID', 'UAN', 'ATS', '10-34-0', 'AMMONIA', 'NITAMIN'],
            "
                MATCH (o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)
                , (l)-[:IN_STATE]->(os:StateOrProvince)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dc:Country)
                , (dc)<-[:IN_COUNTRY]-(dz)
                , (tr)-[:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name:'LIQUID'})
                , (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
                , (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
                , (oz)-[:IN_COUNTRY]->(c:Country)
                , (tFSC)-[:FOR_PRODUCTGROUP]->(lpg)
                , (p:Party)-[:IS_OCCUPANT]->(o)

                WHERE 1=1
                AND cr.distanceLower < tmt.distance <= cr.distanceUpper
                AND date($neodash_ship_date) <= tFSC.expirationDate
                AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
                AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
                AND (
                    (prod)<-[:HAS_PRODUCTGROUP]-(o)
                    OR (lpg)<-[:HAS_PRODUCTGROUP]-(o)
                )

                RETURN l, o, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, tmt, tFSC
            ",
            selProd.name = 'METHANOL',
            "
                MATCH (prod)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)
                , (l)-[:IN_CITY]->(ocs:CityState)
                , (ocs)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dcs)
                , (dcs)<-[:IN_CITY]-(dz)
                , (tr)-[:FOR_PRODUCTGROUP]->(prod)
                , (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
                , (l)-[:IN_COUNTRY]->(c:Country)
                , (tFSC:TruckFSC)-[:FOR_PRODUCTGROUP]->(prod)
                , (p:Party)-[:IS_OCCUPANT]->(o)

                WHERE (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
                AND date($neodash_ship_date) <= tFSC.expirationDate
                and (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
                
                RETURN l, o, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, null as tmt, tFSC
            "
        ],
        "RETURN null AS l",
        {
            prod:selProd,
            dz:selDestZip,
            dcs:selDestCity
        }
    )
    YIELD value
    WITH value.l AS l, value.o AS o, value.occupant AS occupant, value.occupantTypes AS occupantTypes, value.cr AS cr, value.tFSC AS tFSC, value.tmt AS tmt
    
    CALL{
        WITH o
        OPTIONAL MATCH (o)<-[fo:FOR_OCCUPANT]-(m:MPI)-[fp:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
        , (m)-[:FOR_MODE]->(:Mode{id:'TRUCK'})
        WHERE m.startDate <= date($neodash_ship_date) <= m.endDate
        
        WITH m
        ORDER BY m.createdDate DESC LIMIT 1
        
        OPTIONAL MATCH (m)-[:IN_CURRENCY]->(mpiC:Currency)
        OPTIONAL MATCH (mpiC)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
        OPTIONAL MATCH (m)-[:HAS_QTY_UOM]->(mpiU:MassUOM)
        OPTIONAL MATCH (mpiU)-[conv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})
        
        WITH coalesce(m.singularPrice,(m.minPrice+m.maxPrice)/2) AS indPrice
            , coalesce(exch.rate, 1) AS exchRate, coalesce(conv.rate,1) AS convRate
        RETURN round(indPrice * exchRate / convRate, 2) AS indication
    }
    
    MATCH (cr)-[:HAS_RATE_UOM]->(rUOM), (cr)-[:HAS_DISTANCE_UOM]->(dUOM), (cr)-[:IN_CURRENCY]->(cur), (cr)-[:HAS_LOAD_QTY_UOM]->(lqUOM:MassUOM)
    OPTIONAL MATCH (cur)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
    OPTIONAL MATCH (lqUOM)-[massConv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})

    WITH l.name AS originLocation, occupant, occupantTypes, tmt.distance AS miles, tFSC.rate AS fsc
    , cr.rate AS baseRate, cr.ratePerUom AS ratePerUom, coalesce(massConv.rate,1) AS massConv
    , coalesce(exch.rate,1) AS exchRate, cr.loadQuantity AS loadQty, toUpper(cr.rateFactorType) AS rateFactor
    , indication
    WITH originLocation, occupant, occupantTypes, fsc, miles, indication,
    CASE rateFactor
        WHEN "DISTANCE" THEN (baseRate*exchRate*miles)/(loadQty*massConv)
        WHEN "UNIT" THEN baseRate*exchRate/massConv
        WHEN "LOAD" THEN ratePerUom*exchRate/massConv
    END AS rate

WITH originLocation, occupant, occupantTypes, "TRUCK" AS mode, rate * (1+fsc) AS freight, indication
RETURN originLocation, occupant, occupantTypes, mode,
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight,3) ELSE round(freight,2) END AS freight, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(indication,3) ELSE round(indication,2) END AS indication, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight + indication,3) ELSE round(freight + indication, 2) END AS deliveredPrice 
ORDER BY freight