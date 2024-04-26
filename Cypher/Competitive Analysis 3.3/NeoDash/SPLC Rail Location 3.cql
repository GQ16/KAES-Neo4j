PROFILE
//MARK: Rail Finding Locations
    MATCH (st:StateOrProvince)<-[IN_STATE]-(dl:Location{id:$neodash_location_id_2})-[:IN_SPLC]->(s2:SPLC)
    MATCH (occ:Koch|Competitor)<-[:HAS_OCCUPANT]-(ol:Location)
    , (ol)-[:IN_SPLC]->(s1:SPLC)
    , (occ)-[:COMPETES_IN]->(st)
    // , (occ)<-[:IS_OCCUPANT]-(party:Party)
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
        (ls:SPLC)<-[:IN_SPLC]-(o)<-[:UREA_FROM]-(:RailRoute)-[:UREA_TO]->(d)-[:IN_SPLC]->(le:SPLC)
        // WHERE point.distance(s2.coordinate, s1.coordinate) >= point.distance(ls.coordinate, le.coordinate)
    ){1,2}
    (s2:SPLC)

    WITH [r IN nodes(path) WHERE r:RailRoute] AS routes, [x IN nodes(path) WHERE x:SPLC] AS splcList
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
    WITH routes, splcList, rates, isValid
    WHERE isValid

    WITH routes, splcList, rates
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
    WITH fuels, miles, [s IN splcList|properties(s)] AS splcs
        , CASE WHEN coalesce(rates[0].min_cars, 1) >= coalesce(rates[1].min_cars, 1) THEN coalesce(rates[0].min_cars, 1) ELSE rates[1].min_cars END AS minCars
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

    WITH splcs, isPublic, isPrivate, round(reduce(price = 0, x IN legs | price + x.rate + (x.fsc * x.miles) ),2) AS price
    ORDER BY price
    WITH splcs, isPublic, isPrivate, collect(price)[0] AS lowestPrice

RETURN *