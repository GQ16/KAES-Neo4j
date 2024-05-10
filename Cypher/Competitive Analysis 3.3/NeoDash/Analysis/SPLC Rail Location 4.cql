//MARK: Rail Finding Locations
    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    MATCH (st:StateOrProvince)<-[IN_STATE]-(dl:Location{id:$neodash_location_id_2})-[:IN_SPLC]->(s2:SPLC)
    MATCH (occ:Koch|Competitor)<-[:HAS_OCCUPANT]-(ol:Location)
    , (ol)-[:IN_SPLC]->(s1:SPLC)
    , (occ)-[:COMPETES_IN]->(st)
    // , (ol)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    WHERE 1= 1
    AND (
        (occ)-[:HAS_PRODUCTGROUP]->(lpg) 
        OR (occ)-[:HAS_PRODUCTGROUP]->()-[:HAS_CHILD]->(lpg)
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
    
    OPTIONAL MATCH (occ)<-[:IS_OCCUPANT]-(party:Party)

    WITH ol, dl,  coalesce(party.name, 'Unknown') AS occPartyName, occ, lpg
    , [r IN nodes(path) WHERE r:RailRoute] AS routes, [x IN nodes(path) WHERE x:SPLC] AS splcList
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    //MARK: Check Validity & Get Rates
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
    WITH ol, dl,  occPartyName,  occ, routes, splcList, rates, isValid, lpg
    WHERE isValid

//MARK: Get Fuel
    CALL{
        WITH routes, lpg
        UNWIND routes AS route
        MATCH (ca:Carrier)-[:HAS_CURRENT_FSC]->(f:RailFSC)-[:IN_CURRENCY]->(fC:Currency)<-[:IN_CURRENCY]-(route)
        WHERE (
            NOT (f)-[:FOR_PRODUCTGROUP]->() 
            OR (f)-[:FOR_PRODUCTGROUP]->(lpg) 
            OR (f)-[:FOR_PRODUCTGROUP]->()-[:HAS_CHILD]->(lpg)
        )

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

        CALL apoc.cypher.run(
            "OPTIONAL MATCH (s1)-[rdt:"+ route.origin_carrier +"_DISTANCE_TO_" + route.destination_carrier + "]-(s2) RETURN coalesce(rdt.distance,10000) AS result",
            {s1:originSPLC, s2:destSPLC}
        ) YIELD value
        //Get one mileage per route node
        WITH route, min(value.result) AS dist
        RETURN collect(dist) AS miles
    }
//MARK: Legs
    WITH ol, dl,  occPartyName,  occ, fuels, miles, [s IN splcList|properties(s)] AS splcs
        , CASE 
            WHEN coalesce(rates[0].min_cars, 1) >= coalesce(rates[1].min_cars, 1) 
            THEN coalesce(rates[0].min_cars, 1) 
            ELSE rates[1].min_cars 
        END AS minCars
        , CASE 
            WHEN size(rates) > 1 THEN "Rule 11" 
            ELSE "$" + rates[0].rate + "/" + rates[0].uom
        END AS rateDesc
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
        // , rates[0].car_owner_carrier AS isPublic
        , rates[0].car_owner_shipper AS isPrivate

    WITH *, 
        [x IN range(0,size(rateMaps)-1)|
            {
                rate: round(rateMaps[x].perTonRate,3),
                rateCurr: rateMaps[x].currency,
                fsc: round(fuels[x].rate, 6),
                fscCurr: fuels[x].currency,
                miles: toFloat(miles[x])
            }
        ] AS legs

    WITH ol, dl,  occPartyName,  occ, splcs, isPrivate, minCars, rateDesc,
        round(reduce(price = 0, x IN legs | price + x.rate + (x.fsc * x.miles) ),2) AS price,
        round(reduce(miles = 0, m IN legs | miles + m.miles),0) AS totalMiles
    ORDER BY price

    WITH ol, dl,  occPartyName,  occ, isPrivate, minCars, rateDesc,
        collect(totalMiles)[0] AS cheapestMiles,
        collect(splcs)[0] AS cheapestRoute, 
        collect(price)[0] AS freight

//MARK: Get Indication
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
RETURN 
    ol.id AS originLocationId,
    ol.name AS originLocationName,
    ol.coordinate.latitude AS originLat,
    ol.coordinate.longitude AS originLong,
    occPartyName,
    [type IN labels(occ) WHERE type <> 'Occupant'] AS occupantTypes,
    dl.id AS destLocationId,
    dl.name AS destLocationName,
    dl.coordinate.latitude AS destLat,
    dl.coordinate.longitude AS destLong,
    'RAIL' AS mode,
    rateDesc,
    isPrivate,
    minCars,
    // cheapestMiles,
    // cheapestRoute,
    freight,
    indication,
    round(freight + indication,0) AS delivered
ORDER BY freight LIMIT 10