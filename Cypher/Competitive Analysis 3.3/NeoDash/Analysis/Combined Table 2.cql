//MARK: Rail Finding Locations
    MATCH (st:StateOrProvince)<-[IN_STATE]-(dl:Location{id:$neodash_location_id_2})-[:IN_SPLC]->(s2:SPLC)
    MATCH (occ:Koch|Competitor)<-[:HAS_OCCUPANT]-(ol:Location)
    , (ol)-[:IN_SPLC]->(s1:SPLC)
    , (occ)-[:COMPETES_IN]->(st)
    , (occ)<-[:IS_OCCUPANT]-(party:Party)
    // , (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    WHERE 1= 1
    AND (
        (occ)-[:HAS_PRODUCTGROUP]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name}) 
        OR (occ)-[:HAS_PRODUCTGROUP]->()-[:HAS_CHILD]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    )
    AND CASE WHEN isEmpty($neodash_location_id_1) THEN TRUE ELSE ol.id = $neodash_location_id_1 END

//MARK: Routes
    MATCH path = 
    (s1:SPLC)
    (
        (ls:SPLC)<-[:IN_SPLC]-()<-[:UREA_FROM]-(:RailRoute)-[:UREA_TO]->()-[:IN_SPLC]->(le:SPLC)
        // WHERE point.distance(s2.coordinate, s1.coordinate) >= point.distance(ls.coordinate, le.coordinate)
    ){1,2}
    (s2:SPLC)

    WITH ol.name AS originLocationName, party.name AS occPartyName, occ
    , [r IN nodes(path) WHERE r:RailRoute] AS routes, [x IN nodes(path) WHERE x:SPLC] AS splcList
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    //MARK: Get Rates & Is Valid Route
    CALL{
        WITH routes
        WITH routes[0] AS route1, routes[1] AS route2
        MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
        OPTIONAL MATCH (route2)-[:HAS_CURRENT_RATE]->(rate2:RailRate)
        RETURN 
        CASE 
            WHEN route2 IS NULL THEN [rate1]
            ELSE [rate1, rate2]
        END AS rates, 
        CASE 
            WHEN route2 IS NULL 
                THEN NOT rate1.rule_11_at_origin_required
                AND NOT rate1.rule_11_at_destination_required
            ELSE 
                rate1.rule_11_at_destination_allowed 
                AND NOT rate1.rule_11_at_origin_required 
                AND rate2.rule_11_at_origin_allowed 
                AND NOT rate2.rule_11_at_destination_required
                AND (
                    (rate1.car_owner_shipper AND rate2.car_owner_shipper)
                    OR
                    (rate1.car_owner_carrier AND rate2.car_owner_carrier)
                )
        END AS isValid
    }
    WITH originLocationName, occPartyName,  occ, routes, splcList, rates, isValid
    WHERE isValid

    WITH originLocationName, occPartyName,  occ, routes, splcList, rates
    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})

    //MARK: Get Fuel
    CALL{
        WITH routes, lpg
        UNWIND routes AS route
        MATCH (route)<-[:HAS_ROUTE]-(ca:Carrier)-[:HAS_FSC]->(f:RailFSC)-[:IN_CURRENCY]->(fC:Currency)
        MATCH (oc:Country)<-[:IN_COUNTRY]-()<-[:FROM]-(route)-[:TO]->()-[:IN_COUNTRY]->(dc:Country)
            , (oc)<-[:FOR_ORIGIN_COUNTRY]-(f)-[:FOR_DESTINATION_COUNTRY]->(dc)
        WHERE (
            NOT (f)-[:FOR_PRODUCTGROUP]->() 
            OR (f)-[:FOR_PRODUCTGROUP]->(lpg) 
            OR (f)-[:FOR_PRODUCTGROUP]->()-[:HAS_CHILD]->(lpg)
        )
        AND f.expirationDate IS NULL
        RETURN collect({
            baseFuel: f.rate, 
            carVol: lpg.railCarVol,
            rate:f.rate/lpg.railCarVol, 
            carrier:ca.id, 
            currency:fC.id
        }) AS fuels
    }
    //MARK: Get Miles
    CALL{
        WITH routes, splcList
        WITH apoc.coll.pairsMin(splcList) AS splcPairs, routes
        WITH apoc.coll.zip(splcPairs,routes) AS splcPairRoutes
        UNWIND splcPairRoutes AS splcPairRoute
        WITH splcPairRoute[0] AS splcPair, splcPairRoute[1] AS route
        WITH route, splcPair[0] AS originSPLC, splcPair[1] AS destSPLC

        MATCH (route)<-[:HAS_ROUTE]-(ca:Carrier)
        OPTIONAL MATCH (originSPLC)-[rdt:RAIL_DISTANCE_TO{carrier:ca.id}]-(destSPLC)

        //Get one mileage per route node
        WITH route, min(coalesce(rdt.distance,10000)) AS dist
        RETURN collect(dist) AS miles
    }
    //What is available at this point in the query: routes, rates, splcList, fuels, miles
    WITH originLocationName, occPartyName,  occ, fuels, miles, [s IN splcList|properties(s)] AS splcs
        , CASE 
            WHEN coalesce(rates[0].min_cars, 1) >= coalesce(rates[1].min_cars, 1) 
            THEN coalesce(rates[0].min_cars, 1) 
            ELSE rates[1].min_cars 
        END AS minCars
        , [r IN rates| {
            baseRate: r.rate,
            rateType: r.uom,
            carVol: lpg.railCarVol,
            perTonRate: CASE toLower(r.uom)
                WHEN 'ton' THEN r.rate
                WHEN 'car' THEN r.rate / lpg.railCarVol
                ELSE 0
            END,
            currency: r.currency,
            carrier: r.carrier
        }] AS rateMaps
        //since the whole path has already been checked, we can just take the value of the first
        , rates[0].car_owner_carrier AS isPublic
        , rates[0].car_owner_shipper AS isPrivate

    WITH *, 
        [x IN range(0,size(rateMaps)-1)|
            {
                rate: round(rateMaps[x].perTonRate,3),
                fsc: round(fuels[x].rate, 6),
                miles: toFloat(miles[x])
            }
        ] AS legs

    WITH originLocationName, occPartyName,  occ, splcs, isPublic, isPrivate, minCars, 
        round(reduce(price = 0, x IN legs | price + x.rate + (x.fsc * x.miles) ),2) AS price,
        round(reduce(miles = 0, m IN legs | miles + m.miles),0) AS totalMiles
    ORDER BY price

    WITH originLocationName, occPartyName,  occ, isPrivate, minCars,
        collect(totalMiles)[0] AS cheapestMiles,
        collect(splcs)[0] AS cheapestRoute, 
        collect(price)[0] AS freight
	CALL{
		WITH occ
		OPTIONAL MATCH (occ)<-[:FOR_OCCUPANT]-(m:MPI)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
		, (m)-[:FOR_MODE]->(:Mode{id:'RAIL'})
		WHERE m.startDate <= date($neodash_ship_date) <= m.endDate
		
		WITH m
		ORDER BY m.createdDate DESC LIMIT 1	//Get latest MPI

		// OPTIONAL MATCH (m)-[:IN_CURRENCY]->(mpiC:Currency)
		// OPTIONAL MATCH (mpiC)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
		// OPTIONAL MATCH (m)-[:HAS_QTY_UOM]->(mpiU:MassUOM)
		// OPTIONAL MATCH (mpiU)-[conv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})
		
		WITH 
		CASE m.type
			WHEN 'DESTINATION' 
			THEN coalesce(m.singularPrice-m.freight,((m.minPrice+m.maxPrice)/2) -m.freight) 
			WHEN 'ORIGIN' 
			THEN coalesce(m.singularPrice,(m.minPrice+m.maxPrice)/2) 
			ELSE NULL
		END AS indPrice
			// , coalesce(exch.rate, 1) AS exchRate
			// , coalesce(conv.rate,1) AS convRate
		// RETURN indPrice * exchRate / convRate AS indication
        RETURN indPrice AS indication
	}
RETURN originLocationName,
    occPartyName,
    [type IN labels(occ) WHERE type <> 'Occupant'] AS occupantTypes,
    'RAIL' AS mode
    // isPrivate,
    // minCars,
    // cheapestMiles,
    // cheapestRoute,
    freight,
    indication,
    round(freight + indication,0) AS delivered
ORDER BY freight LIMIT 50

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
                AND date('2024-02-29') <= tFSC.expirationDate
                AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
                AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
                AND (
                (prod)<-[:HAS_PRODUCTGROUP]-(o)
                OR (lpg)<-[:HAS_PRODUCTGROUP]-(o)
                )
                RETURN l, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, tmt, tFSC
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
                AND date('2024-02-29') <= tFSC.expirationDate
                AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
                AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
                AND (
                (prod)<-[:HAS_PRODUCTGROUP]-(o)
                OR (lpg)<-[:HAS_PRODUCTGROUP]-(o)
                )

                RETURN l, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, tmt, tFSC
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
                AND date('2024-02-29') <= tFSC.expirationDate
                and (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
                
                RETURN l, p.name AS occupant, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, null as tmt, tFSC
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
    WITH value.l AS l, value.o AS o, value.occupant AS occPartyName, value.occupantTypes AS occupantTypes, value.cr AS cr, value.tFSC AS tFSC, value.tmt AS tmt
    
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
    
    MATCH (cr)-[:HAS_RATE_UOM]->(rUOM), 
        (cr)-[:HAS_DISTANCE_UOM]->(dUOM), 
        (cr)-[:IN_CURRENCY]->(cur), 
        (cr)-[:HAS_LOAD_QTY_UOM]->(lqUOM:MassUOM)
    MATCH (cur)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
    MATCH (lqUOM)-[massConv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})

    WITH l.name AS originLocationName, occPartyName, occupantTypes, tmt.distance AS miles, tFSC.rate AS fsc
    , cr.rate AS baseRate, cr.ratePerUom AS ratePerUom, massConv.rate AS massConv
    , exch.rate AS exchRate, cr.loadQuantity AS loadQty, toUpper(cr.rateFactorType) AS rateFactor
    , indication
    WITH originLocationName, occPartyName, occupantTypes, fsc, miles, indication,
    CASE rateFactor
        WHEN "DISTANCE" THEN (baseRate*exchRate*miles)/(loadQty*massConv)
        WHEN "UNIT" THEN baseRate*exchRate/massConv
        WHEN "LOAD" THEN ratePerUom*exchRate/massConv
    END AS rate

WITH originLocationName, occPartyName, occupantTypes, "TRUCK" AS mode, rate * (1+fsc) AS freight, indication
RETURN 
    originLocationName, 
    occPartyName, 
    occupantTypes, 
    mode,
	CASE $neodash_logisticsproductgroup_name 
        WHEN 'METHANOL' THEN round(freight,3) 
        ELSE round(freight,2) 
    END AS freight, 
	CASE $neodash_logisticsproductgroup_name 
        WHEN 'METHANOL' THEN round(indication,3) 
        ELSE round(indication,2) 
    END AS indication, 
	CASE $neodash_logisticsproductgroup_name 
        WHEN 'METHANOL' THEN round(freight + indication,3) 
        ELSE round(freight + indication, 2) 
    END AS delivered 
ORDER BY freight