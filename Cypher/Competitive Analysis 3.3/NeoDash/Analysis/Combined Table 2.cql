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
        MATCH (ca:Carrier)-[:HAS_CURRENT_FSC]->(f:RailFSC)<-[:FOR_RAIL_FSC]-(fC:Currency)-[:FOR_RAIL_ROUTE]->(route)
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
                fsc: round(fuels[x].rate, 6),
                curr: rateMaps[x].currency,
                miles: toFloat(miles[x]),
                exchRate: CASE 
                    WHEN rateMaps[x].currency = $neodash_currency_id THEN 1 
                    WHEN rateMaps[x].currency = 'USD' AND $neodash_currency_id = 'CAD' THEN 1.36
                    WHEN rateMaps[x].currency = 'CAD' AND $neodash_currency_id = 'USD' THEN 0.73
                END
            }
        ] AS legs

    WITH ol, dl,  occPartyName,  occ, splcs, isPrivate, minCars, rateDesc,
        round(reduce(price = 0, x IN legs | price + (x.rate + (x.fsc * x.miles)) * x.exchRate ),2) AS price,
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
        , (c:Currency)-[:FOR_MPI]->(m)
		WHERE m.startDate <= date($neodash_ship_date) <= m.endDate
		
		WITH m, c.id AS curr
		ORDER BY m.createdDate DESC LIMIT 1	//Get latest MPI
		
		WITH m.createdDate AS dateLogged,
		CASE 
			WHEN m.type = 'DESTINATION' AND m.singularPrice IS NOT NULL
			THEN m.singularPrice-m.freight 
			WHEN m.type = 'DESTINATION' AND m.singularPrice IS NULL
			THEN ((m.minPrice+m.maxPrice)/2) - m.freight
            WHEN m.type = 'ORIGIN' AND m.singularPrice IS NOT NULL
			THEN m.singularPrice
            WHEN m.type = 'ORIGIN' AND m.singularPrice IS NULL
			THEN (m.minPrice+m.maxPrice)/2
            ELSE NULL
		END AS indPrice,
        CASE 
            WHEN curr = $neodash_currency_id THEN 1 
            WHEN curr = 'USD' AND $neodash_currency_id = 'CAD' THEN 1.36
            WHEN curr = 'CAD' AND $neodash_currency_id = 'USD' THEN 0.73
        END AS exchRate

        RETURN indPrice * exchRate AS indication, dateLogged
	}
RETURN 
    ol.id AS originLocationId,
    ol.name AS originLocationName,
    round(ol.coordinate.latitude,5) AS originLat,
    round(ol.coordinate.longitude,5) AS originLong,
    occPartyName,
    [type IN labels(occ) WHERE type <> 'Occupant'] AS occupantTypes,
    dl.id AS destLocationId,
    dl.name AS destLocationName,
    round(dl.coordinate.latitude,5) AS destLat,
    round(dl.coordinate.longitude,5) AS destLong,
    'RAIL' AS mode,
    rateDesc,
    isPrivate,
    minCars,
    cheapestMiles AS miles,
    // cheapestRoute,
    freight,
    indication,
    round(freight + indication,0) AS delivered
ORDER BY freight LIMIT 10

UNION
    
    MATCH (selProd:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    MATCH (selDestCity:CityState)<-[:IN_CITY]-(dl:Location{id:$neodash_location_id_2})-[:IN_ZIPCODE]->(selDestZip:ZipCode)

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
                AND tFSC.expirationDate IS NULL
                AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
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
                AND tFSC.expirationDate IS NULL
                AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
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

                WHERE 1=1
                AND tFSC.expirationDate IS NULL
                AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
                
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
    WITH dl, value.l AS ol, value.o AS o, value.occupant AS occPartyName, value.occupantTypes AS occupantTypes, value.cr AS cr, value.tFSC AS tFSC, value.tmt AS tmt
    
    CALL{
        WITH o
        OPTIONAL MATCH (o)<-[fo:FOR_OCCUPANT]-(m:MPI)-[fp:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
        , (m)-[:FOR_MODE]->(:Mode{id:'TRUCK'})
        WHERE m.startDate <= date($neodash_ship_date) <= m.endDate
        
        WITH m
        ORDER BY m.createdDate DESC LIMIT 1
        
        WITH coalesce(m.singularPrice,(m.minPrice+m.maxPrice)/2) AS indPrice
        RETURN round(indPrice, 2) AS indication
    }

    WITH ol, dl, occPartyName, occupantTypes, tmt.distance AS miles, tFSC.rate AS fsc
    , cr.rate AS baseRate, cr.ratePerUom AS ratePerUom, cr.loadQuantity AS loadQty, toUpper(cr.rateFactorType) AS rateFactor
    , indication
    WITH ol, dl, occPartyName, occupantTypes, fsc, miles, indication,
    CASE rateFactor
        WHEN "DISTANCE" THEN (baseRate*miles)/(loadQty)
        WHEN "UNIT" THEN baseRate
        WHEN "LOAD" THEN ratePerUom
    END AS rate,
    CASE rateFactor
        WHEN "DISTANCE" THEN "Mile"
        WHEN "UNIT" THEN "Unit"
        WHEN "LOAD" THEN "Load"
    END AS rateFactor

WITH ol, dl, occPartyName, occupantTypes, "TRUCK" AS mode, rate, rateFactor, rate * (1+fsc) AS freight, indication, miles
RETURN 
    ol.id AS originLocationId,
    ol.name AS originLocationName,
    ol.coordinate.latitude AS originLat,
    ol.coordinate.longitude AS originLong,
    occPartyName,
    occupantTypes,
    dl.id AS destLocationId,
    dl.name AS destLocationName,
    dl.coordinate.latitude AS destLat,
    dl.coordinate.longitude AS destLong,
    mode,
    "$" + toString(round(rate,2)) + "/" + rateFactor AS rateDesc,
    null AS isPrivate,
    null AS minCars,
    miles,
    freight,
    indication,
    freight + indication AS delivered
ORDER BY freight LIMIT 10