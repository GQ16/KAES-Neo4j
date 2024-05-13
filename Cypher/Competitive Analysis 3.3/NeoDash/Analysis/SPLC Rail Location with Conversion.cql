PROFILE
//MARK: Rail Finding Locations
    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    MATCH (st:StateOrProvince)<-[IN_STATE]-(dl:Location{id:$neodash_location_id_2})-[:IN_SPLC]->(s2:SPLC)
    MATCH (occ:Koch|Competitor)<-[:HAS_OCCUPANT]-(ol:Location)
    , (ol)-[:IN_SPLC]->(s1:SPLC)
    , (occ)-[:COMPETES_IN]->(st)
    , (occ)<-[:IS_OCCUPANT]-(party:Party)
    , (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
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

    WITH ol.name AS originLocationName, party.name AS occPartyName, occ, lpg
    , [r IN nodes(path) WHERE r:RailRoute] AS routes, [x IN nodes(path) WHERE x:SPLC] AS splcList
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    //MARK: Get Rates & Is Valid Route
    CALL{
        WITH routes, lpg
        WITH routes[0] AS route1, routes[1] AS route2, lpg
        MATCH (lqUOM:MassUOM {id:'ST'})-[uomConv:HAS_CONVERSION_RATE]->(:MassUOM{id: $neodash_massuom_id})
        MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
        MATCH (route1)<-[:FOR_RAIL_ROUTE]-(rate1Curr:Currency)
        MATCH (rate1Curr)-[exch1:HAS_EXCHANGE_RATE {exchangeDate:date('2023-01-10')}]->(finalCurr:Currency{id: $neodash_currency_id})
        OPTIONAL MATCH (route2)-[:HAS_CURRENT_RATE]->(rate2:RailRate)
        OPTIONAL MATCH (route2)<-[:FOR_RAIL_ROUTE]-(rate2Curr:Currency)
        OPTIONAL MATCH (rate2Curr)-[exch2:HAS_EXCHANGE_RATE {exchangeDate:date('2023-01-10')}]->(finalCurr)

        RETURN
        CASE 
            WHEN route2 IS NULL 
            THEN [{
                carOwnerShipper: rate1.car_owner_shipper,
                carOwnerCarrier: rate1.car_owner_carrier,
                minCars: rate1.min_cars,
                baseRate: rate1.rate,
                rateType: rate1.uom,
                carVol: lpg.railCarVol,
                currency: rate1Curr.id,
                carrier: rate1.carrier,
                currConvRate: exch1.rate,
                uomConvRate: uomConv.rate,
                perTonRate: CASE toLower(rate1.uom)
                    WHEN 'ton' THEN (rate1.rate * exch1.rate) / uomConv.rate
                    WHEN 'car' THEN (rate1.rate * exch1.rate) / (lpg.railCarVol * uomConv.rate)
                    ELSE 0
                END
            }]
            ELSE [{
                carOwnerShipper: rate1.car_owner_shipper,
                carOwnerCarrier: rate1.car_owner_carrier,
                minCars: rate1.min_cars,
                baseRate: rate1.rate,
                rateType: rate1.uom,
                carVol: lpg.railCarVol,
                currency: rate1Curr.id,
                carrier: rate1.carrier,
                currConvRate: exch1.rate,
                uomConvRate: uomConv.rate,
                perTonRate: CASE toLower(rate1.uom)
                    WHEN 'ton' THEN (rate1.rate * exch1.rate) / uomConv.rate
                    WHEN 'car' THEN (rate1.rate * exch1.rate) / (lpg.railCarVol * uomConv.rate)
                    ELSE 0
                END
                }, {
                carOwnerShipper: rate2.car_owner_shipper,
                carOwnerCarrier: rate2.car_owner_carrier,
                minCars: rate2.min_cars,
                baseRate: rate2.rate,
                rateType: rate2.uom,
                carVol: lpg.railCarVol,
                currency: rate2Curr.id,
                carrier: rate2.carrier,
                currConvRate: exch2.rate,
                uomConvRate: uomConv.rate,
                perTonRate: CASE toLower(rate2.uom)
                    WHEN 'ton' THEN (rate2.rate * exch2.rate) / uomConv.rate
                    WHEN 'car' THEN (rate2.rate * exch2.rate) / (lpg.railCarVol * uomConv.rate)
                    ELSE 0
                END
            }]
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
    WITH originLocationName, occPartyName,  occ, routes, splcList, rates, lpg, isValid
    WHERE isValid

    //MARK: Get Fuel
    CALL{
        WITH routes, lpg
        UNWIND routes AS route
        MATCH (ca:Carrier)-[:HAS_CURRENT_FSC]->(f:RailFSC)<-[:FOR_RAIL_FSC]-(fscCurr:Currency)-[:FOR_RAIL_ROUTE]->(route)
        MATCH (fscCurr)-[exch:HAS_EXCHANGE_RATE {exchangeDate:date('2023-01-10')}]->(:Currency{id: $neodash_currency_id})
        WHERE (
            NOT (f)-[:FOR_PRODUCTGROUP]->() 
            OR (f)-[:FOR_PRODUCTGROUP]->(lpg) 
            OR (f)-[:FOR_PRODUCTGROUP]->()-[:HAS_CHILD]->(lpg)
        )

        RETURN collect({
            baseFuel: f.rate, 
            carVol: lpg.railCarVol,
            rate:f.rate * exch.rate/lpg.railCarVol, 
            exchRate: exch.rate,
            carrier:ca.id, 
            currency:fscCurr.id
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
    //What is available at this point in the query: routes, rates, splcList, fuels, miles
    WITH originLocationName, occPartyName,  occ, fuels, miles, [s IN splcList|properties(s)] AS splcs, rates
        , CASE 
            WHEN coalesce(rates[0].minCars, 1) >= coalesce(rates[1].minCars, 1) 
            THEN coalesce(rates[0].minCars, 1) 
            ELSE rates[1].minCars 
        END AS minCars
        //since the whole path has already been checked, we can just take the value of the first
        , rates[0].carOwnerCarrier AS isPublic
        , rates[0].carOwnerShipper AS isPrivate

    WITH *, 
        [x IN range(0,size(rates)-1)|
            {
                rate: round(rates[x].perTonRate,3),
                fsc: round(fuels[x].rate, 6),
                miles: toFloat(miles[x])
            }
        ] AS legs

    WITH originLocationName, occPartyName,  occ, splcs, isPublic, isPrivate, minCars, round(reduce(price = 0, x IN legs | price + x.rate + (x.fsc * x.miles) ),2) AS price
    ORDER BY price
    WITH originLocationName, occPartyName,  occ, splcs, isPublic, isPrivate, minCars, collect(price)[0] AS lowestPrice
    //MARK: Get MPI's
	CALL{
		WITH occ
		OPTIONAL MATCH (occ)<-[:FOR_OCCUPANT]-(m:MPI)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
		, (m)-[:FOR_MODE]->(:Mode{id:'RAIL'})
		WHERE m.startDate <= date($neodash_ship_date) <= m.endDate
		
		WITH m
		ORDER BY m.createdDate DESC LIMIT 1	//Get latest MPI

		OPTIONAL MATCH (m)<-[:FOR_MPI]-(mpiC:Currency)
		OPTIONAL MATCH (mpiC)-[exch:HAS_EXCHANGE_RATE {exchangeDate:date('2023-01-10')}]->(:Currency{id: $neodash_currency_id})
		OPTIONAL MATCH (m)-[:HAS_QTY_UOM]->(mpiU:MassUOM)
		OPTIONAL MATCH (mpiU)-[conv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})
		
		WITH exch.rate AS exchRate, conv.rate AS convRate,
        CASE m.type
			WHEN 'DESTINATION' THEN coalesce(m.singularPrice-m.freight,((m.minPrice+m.maxPrice)/2) -m.freight) 
			WHEN 'ORIGIN' THEN coalesce(m.singularPrice,(m.minPrice+m.maxPrice)/2) 
			ELSE 0
		END AS indPrice

		RETURN indPrice * exchRate / convRate AS indication
	}
RETURN *
ORDER BY lowestPrice LIMIT 50