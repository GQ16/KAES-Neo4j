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
    //MARK: Is Valid Route
    CALL{
        WITH routes
        WITH routes[0] AS route1, routes[1] AS route2
        MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
        OPTIONAL MATCH (route2)-[:HAS_CURRENT_RATE]->(rate2:RailRate)
        RETURN [rate1, rate2] AS rates, 
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

    WITH routes, splcList, rates, 
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
        RETURN collect({rate:f.rate/lpg.railCarVol, carrier:ca.id, currency:fC.id}) AS fuels
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
        WITH route, min(coalesce(rdt.distance,10000))
        RETURN collect(dist) AS miles
    }
    //What is available at this point in the query: routes, rates, splcList, fuels, miles
    WITH
        , ra.rate AS baseRate
        , lpg.railCarVol AS carVol
        , routeCarrier.id AS carrier
        , ra.car_owner_carrier AS isPublic
        , ra.car_owner_shipper AS isPrivate

    WITH miles, fsc/carVol AS perTonFuel, carrier, splcPair, isPublic, isPrivate,
    CASE rateType 
        WHEN 'ton' THEN baseRate
        WHEN 'car' THEN baseRate / carVol
        ELSE 0
    END AS perTonRate

    WITH {
        perTonRate:perTonRate,
        perTonFuel:perTonFuel,
        miles:miles,
        carrier:carrier,
        splcPair:splcPair,
        isPublic:isPublic,
        isPrivate: isPrivate
    } AS routeInfo

    RETURN collect(routeInfo) AS routeInfoList

return path, routeInfoList //limit 5