CALL{
    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    OPTIONAL MATCH pp=(:LogisticsProductGroup)-[r:HAS_CHILD*]->(lpg)
    WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(pp) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca)
    MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)<-[:OWNS_STATION_GROUP]-(oca2)
    MATCH (dca)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id: $neodash_zipcode_id_2, country: $neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)<-[:OWNS_STATION_GROUP]-(dca2)

    WITH oca, oca2, dca, dca2 
    WITH collect(DISTINCT oca) + collect(DISTINCT oca2) + collect(DISTINCT dca) + collect(DISTINCT dca2) AS carriers 
    WITH apoc.coll.toSet(carriers) AS carrierSet

    UNWIND carrierSet AS ca
    MATCH (s:STCC)-[:REPRESENTS]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    OPTIONAL MATCH (s)-[r:IN_STCCGROUP]->(sg:STCCGroup)<-[:HAS_STCCGROUP]-(ca) 
    //This collection step is for the case when a Product group has multiple stccs
    WITH collect(DISTINCT toString(s.id)) as stcc, collect(DISTINCT sg.id) as groups
    RETURN stcc + groups AS validRateStccs
}
CALL{
    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    OPTIONAL MATCH pp=(:LogisticsProductGroup)-[r:HAS_CHILD*]->(lpg)
    WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(pp) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca:Carrier)
    MATCH (dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id: $neodash_zipcode_id_2, country: $neodash_country_id_2})
    MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})

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

    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    OPTIONAL MATCH pp=(:LogisticsProductGroup)-[r:HAS_CHILD*]->(lpg)
    WITH validInterchanges, validRateStccs, CASE WHEN r IS NULL THEN lpg ELSE nodes(pp) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
    MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
	MATCH (p:Party)-[:IS_OCCUPANT]->(o)
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    MATCH dp =(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id: $neodash_zipcode_id_2, country: $neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)

	CALL{
		WITH o
		OPTIONAL MATCH (o)<-[fo:FOR_OCCUPANT]-(m:MPI)-[fp:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
		, (m)-[:FOR_MODE]->(:Mode{id:'RAIL'})
		WHERE m.startDate <= $neodash_ship_date <= m.endDate
		
		WITH m
		ORDER BY m.createdDate DESC
		
		WITH collect(m)[0] AS latestMPI
		OPTIONAL MATCH (latestMPI)-[:IN_CURRENCY]->(mpiC:Currency)
		OPTIONAL MATCH (mpiC)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
		OPTIONAL MATCH (latestMPI)-[:HAS_QTY_UOM]->(mpiU:MassUOM)
		OPTIONAL MATCH (mpiU)-[conv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})
		
		WITH 
		CASE latestMPI.type
			WHEN 'DESTINATION' 
			THEN coalesce(latestMPI.singularPrice-latestMPI.freight,((latestMPI.minPrice+latestMPI.maxPrice)/2) -latestMPI.freight) 
			WHEN 'ORIGIN' 
			THEN coalesce(latestMPI.singularPrice,(latestMPI.minPrice+latestMPI.maxPrice)/2) 
			ELSE NULL
		END AS indPrice
			, coalesce(exch.rate, 1) AS exchRate
			, coalesce(conv.rate,1) AS convRate
		RETURN indPrice * exchRate / convRate AS indication
	}
	
    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , validInterchanges
        , validRateStccs
		, p.name AS occupant, indication
		, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH l,dz,origin, destination, validInterchanges, validRateStccs, occupant, occupantTypes, indication

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

    WITH DISTINCT l,dz,p, [x IN nodes(p) WHERE x:RailRoute] AS routes, i, occupant, occupantTypes, indication
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
	, ra.rate AS baseRate, coalesce(re.rate,1) AS rateCurExch, coalesce(cr.rate,1) AS massConvRate, 97 AS carVol, occupant, occupantTypes
	, indication
	
	WITH *,
        CASE rateType 
            WHEN 'ton' THEN baseRate * rateCurExch * massConvRate
            WHEN 'car' THEN baseRate * rateCurExch * massConvRate / carVol
			WHEN 'mileage' THEN baseRate * rateCurExch * miles * massConvRate / carVol
			WHEN 'mile' THEN baseRate * rateCurExch * miles * massConvRate / carVol
			ELSE 0
        END AS perTonRate
	
	WITH l, dz, p, route, i, min(perTonRate) AS lowestRate, fsc/carVol AS perTonFuel, miles, occupant, occupantTypes, indication
	WITH l, dz, p, route, i, {lowestRate:lowestRate, fsc:perTonFuel, miles:miles} AS routeCostMap, occupant, occupantTypes, indication
	
	WITH l,dz, occupant, occupantTypes, indication
    , nodes(p)[0].id AS origin
    , nodes(p)[-1].id AS dest
    , i.id AS interchange
	, round(reduce(price = 0, x IN collect(routeCostMap) | price + x.lowestRate + (x.fsc * x.miles) ),2) AS price
    , p
    RETURN DISTINCT l, occupant, occupantTypes, dz, origin, collect(interchange)[0] AS interchanges, dest, price, indication
    //Aggregate by the one strongest interchange if multiple all have the same price
    ORDER BY price

    UNION

    WITH validRateStccs
	
    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    OPTIONAL MATCH pp=(:LogisticsProductGroup)-[r:HAS_CHILD*]->(lpg)
    WITH validRateStccs, CASE WHEN r IS NULL THEN lpg ELSE nodes(pp) END AS products
    UNWIND products AS product

    MATCH (product)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
    MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
	MATCH (p:Party)-[:IS_OCCUPANT]->(o)
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    MATCH dp =(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id: $neodash_zipcode_id_2, country: $neodash_country_id_2})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)
	
	CALL{
		WITH o
		OPTIONAL MATCH (o)<-[fo:FOR_OCCUPANT]-(m:MPI)-[fp:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
		, (m)-[:FOR_MODE]->(:Mode{id:'RAIL'})
		WHERE m.startDate <= $neodash_ship_date <= m.endDate
		
		WITH m
		ORDER BY m.createdDate DESC
		
		WITH collect(m)[0] AS latestMPI
		OPTIONAL MATCH (latestMPI)-[:IN_CURRENCY]->(mpiC:Currency)
		OPTIONAL MATCH (mpiC)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
		OPTIONAL MATCH (latestMPI)-[:HAS_QTY_UOM]->(mpiU:MassUOM)
		OPTIONAL MATCH (mpiU)-[conv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})
		
		WITH 
		CASE latestMPI.type
			WHEN 'DESTINATION' 
			THEN coalesce(latestMPI.singularPrice-latestMPI.freight,((latestMPI.minPrice+latestMPI.maxPrice)/2) -latestMPI.freight) 
			WHEN 'ORIGIN' 
			THEN coalesce(latestMPI.singularPrice,(latestMPI.minPrice+latestMPI.maxPrice)/2) 
			ELSE NULL
		END AS indPrice
			, coalesce(exch.rate, 1) AS exchRate
			, coalesce(conv.rate,1) AS convRate
		RETURN indPrice * exchRate / convRate AS indication
	}
	
    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
		, p.name AS occupant, indication
        , validRateStccs, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH l,dz,origin, destination, validRateStccs, occupant, occupantTypes, indication

    MATCH p = 
    (origin)
    (
        (s)<-[f:FROM]-(:RailRoute)-[:TO]->(e) 
        WHERE f.stcc IN validRateStccs
        AND point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){1,2}
    (destination)

    WITH DISTINCT l,dz,p, [x IN nodes(p) WHERE x:RailRoute] AS routes, occupant, occupantTypes, indication
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
	, ra.rate AS baseRate, coalesce(re.rate,1) AS rateCurExch, coalesce(cr.rate,1) AS massConvRate, 97 AS carVol, occupant, occupantTypes
	, indication
	
	WITH *,
        CASE rateType 
            WHEN 'ton' THEN baseRate * rateCurExch * massConvRate
            WHEN 'car' THEN baseRate * rateCurExch * massConvRate / carVol
			WHEN 'mileage' THEN baseRate * rateCurExch * miles * massConvRate / carVol
			WHEN 'mile' THEN baseRate * rateCurExch * miles * massConvRate / carVol
			ELSE 0
        END AS perTonRate
	
	WITH l, dz, p, route, min(perTonRate) AS lowestRate, fsc/carVol AS perTonFuel, miles, occupant, occupantTypes, indication
	WITH l, dz, p, route, {lowestRate:lowestRate, fsc:perTonFuel, miles:miles} AS routeCostMap, occupant, occupantTypes, indication
    
    WITH l,dz, occupant, occupantTypes, indication
    , nodes(p)[0].id AS origin
    , nodes(p)[-1].id AS dest
	, round(reduce(price = 0, x IN collect(routeCostMap) | price + x.lowestRate + (x.fsc * x.miles) ),2) AS price
    , p
    RETURN DISTINCT l,occupant, occupantTypes,dz,origin, "" AS interchanges, dest, price, indication
    ORDER BY price
}
WITH l,occupant, occupantTypes,dz,origin, interchanges, dest, price, indication
ORDER BY price
WITH l.name AS originLocation, min(price) AS freight, occupant, occupantTypes, indication
RETURN originLocation, occupant, occupantTypes, "RAIL" AS mode,
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight,3) ELSE round(freight,2) END AS freight, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(indication,3) ELSE round(indication,2) END AS indication, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight + indication,3) ELSE round(freight + indication, 2) END AS deliveredPrice 
ORDER BY freight

UNION

MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
OPTIONAL MATCH pp=(:LogisticsProductGroup)-[r:HAS_CHILD*]->(lpg)
WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(pp) END AS products
CALL {
	WITH products
	WITH products
	WHERE any(x IN products WHERE x.name IN ['UREA', 'MAP', 'DAP', 'PHOSPHATE', 'DRY', 'POTASH', 'SUPERU'])
	MATCH (lpg1:LogisticsProductGroup)<-[r1:HAS_PRODUCTGROUP]-(o:Occupant)<-[r2:HAS_OCCUPANT]-(l:Location)-[r3:IN_ZIPCODE]->(oz:ZipCode)
	, (l)-[r4:IN_CITY]->(ocs:CityState)<-[r5:FROM]-(tr:TruckRoute)-[r6:TO]->(dlg:LocationGroup)<-[r7:IN_LOCATION_GROUP]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    , (l)-[:HAS_OUTBOUND]->(m:Mode{id:'TRUCK'})
	, (tr)-[:FOR_PRODUCTGROUP]->(lpg2:LogisticsProductGroup)
	, (tr)-[r8:HAS_CURRENT_RATE]->(cr:TruckRate)
	, (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
	, (oz)-[r9:IN_COUNTRY]->(c:Country)<-[r10:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
	, (tFSC:TruckFSC)-[r11:FOR_PRODUCTGROUP]->(lpg3:LogisticsProductGroup)
	, (p:Party)-[:IS_OCCUPANT]->(o)
	
	WHERE lpg1 IN products AND lpg2 IN products AND lpg3 IN products
	AND cr.distanceLower < tmt.distance <= cr.distanceUpper
	AND date('2024-02-29') <= tFSC.expirationDate
	
	RETURN l, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, tmt, tFSC, o

	UNION
 
	WITH products
	WITH products
	WHERE any(x IN products WHERE x.name IN ['LIQUID', 'UAN', 'ATS', '10-34-0', 'AMMONIA', 'NITAMIN'])
	MATCH (lpg1:LogisticsProductGroup)<-[r1:HAS_PRODUCTGROUP]-(o:Occupant)<-[r2:HAS_OCCUPANT]-(l:Location)-[r3:IN_ZIPCODE]->(oz:ZipCode)
	, (l)-[r4:IN_STATE]->(os:StateOrProvince)<-[r5:FROM]-(tr:TruckRoute)-[r6:TO]->(dc:Country)<-[r7:IN_COUNTRY]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
	, (tr)-[:FOR_PRODUCTGROUP]->(lpg2:LogisticsProductGroup)
    , (l)-[:HAS_OUTBOUND]->(m:Mode{id:'TRUCK'})
	, (tr)-[r8:HAS_CURRENT_RATE]->(cr:TruckRate)
	, (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
	, (oz)-[r9:IN_COUNTRY]->(oc:Country)<-[r10:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
	, (tFSC:TruckFSC)-[r11:FOR_PRODUCTGROUP]->(lpg3:LogisticsProductGroup)
	, (p:Party)-[:IS_OCCUPANT]->(o)
	
	WHERE lpg1 IN products AND lpg2 IN products AND lpg3 IN products
	AND cr.distanceLower < tmt.distance <= cr.distanceUpper
	AND date('2024-02-29') <= tFSC.expirationDate
	
	RETURN l, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, tmt, tFSC, o
	
	UNION
 
	WITH products
	WITH products
	WHERE any(x IN products WHERE x.name IN ['METHANOL'])
	MATCH (lpg:LogisticsProductGroup)<-[r1:HAS_PRODUCTGROUP]-(o:Occupant)<-[r2:HAS_OCCUPANT]-(l:Location)
	, (l)-[r4:IN_CITY]->(ocs:CityState)<-[r5:FROM]-(tr:TruckRoute)-[r6:TO]->(dcs:CityState)
    , (l)-[:HAS_OUTBOUND]->(m:Mode{id:'TRUCK'})
	, (tr)-[:FOR_PRODUCTGROUP]->(lpg)
	, (tr)-[r8:HAS_CURRENT_RATE]->(cr:TruckRate)
	, (l)-[r9:IN_COUNTRY]->(c:Country)<-[r10:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
	, (tFSC:TruckFSC)-[r11:FOR_PRODUCTGROUP]->(lpg)
	, (p:Party)-[:IS_OCCUPANT]->(o)
		
	WHERE lpg.name = 'METHANOL'
	AND dcs.id = $neodash_citystate_id_2
	AND date('2024-02-29') <= tFSC.expirationDate
	
	RETURN l, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, null AS tmt, tFSC, o
}

CALL{
	WITH o
	OPTIONAL MATCH (o)<-[fo:FOR_OCCUPANT]-(m:MPI)-[fp:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
	, (m)-[:FOR_MODE]->(:Mode{id:'TRUCK'})
	WHERE m.startDate <= $neodash_ship_date <= m.endDate
	
	WITH m
	ORDER BY m.createdDate DESC
	
	WITH collect(m)[0] AS latestMPI
	OPTIONAL MATCH (latestMPI)-[:IN_CURRENCY]->(mpiC:Currency)
	OPTIONAL MATCH (mpiC)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
	OPTIONAL MATCH (latestMPI)-[:HAS_QTY_UOM]->(mpiU:MassUOM)
	OPTIONAL MATCH (mpiU)-[conv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})
	
	WITH coalesce(latestMPI.singularPrice,(latestMPI.minPrice+latestMPI.maxPrice)/2) AS indPrice
		, coalesce(exch.rate, 1) AS exchRate, coalesce(conv.rate,1) AS convRate
	RETURN round(indPrice * exchRate / convRate, 2) AS indication
}

MATCH (cr)-[:HAS_DISTANCE_UOM]->(dUOM), (cr)-[:IN_CURRENCY]->(cur), (cr)-[:HAS_LOAD_QTY_UOM]->(lqUOM:MassUOM)
OPTIONAL MATCH (cur)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
OPTIONAL MATCH (lqUOM)-[massConv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})

WITH l.name AS originLocation, occupant, occupantTypes, tmt.distance AS miles, tFSC.rate AS fsc, indication,
CASE toUpper(cr.rateFactorType)
	WHEN "DISTANCE" THEN (cr.rate*coalesce(exch.rate,1)*tmt.distance)/(cr.loadQuantity*coalesce(massConv.rate,1))
	WHEN "UNIT" THEN cr.rate*coalesce(exch.rate,1)/coalesce(massConv.rate,1)
	WHEN "LOAD" THEN cr.ratePerUom*coalesce(exch.rate,1)/coalesce(massConv.rate,1)
END AS rate

WITH originLocation, occupant, occupantTypes, "TRUCK" AS mode, rate * (1+fsc) AS freight, indication
RETURN originLocation, occupant, occupantTypes, mode,
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight,3) ELSE round(freight,2) END AS freight, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(indication,3) ELSE round(indication,2) END AS indication, 
	CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight + indication,3) ELSE round(freight + indication, 2) END AS deliveredPrice 
ORDER BY freight