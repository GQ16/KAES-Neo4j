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

    WITH path, [r IN nodes(path) WHERE r:RailRoute] AS routes
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
    CALL{
        WITH routes
        WITH routes[0] AS route1, routes[1] AS route2
        MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
        OPTIONAL MATCH (route2)-[:HAS_CURRENT_RATE]->(rate2:RailRate)
        RETURN CASE 
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
    WITH path, routes, isValid
    WHERE isValid

    WITH path, routes, [x IN nodes(path) WHERE x:SPLC] AS splcList
    CALL{
        WITH routes, splcList
        WITH apoc.coll.pairsMin(splcList) AS splcPairs, routes
        WITH apoc.coll.zip(splcPairs,routes) AS splcPairRoutes
        UNWIND splcPairRoutes AS splcPairRoute
        WITH splcPairRoute[0] AS splcPair, splcPairRoute[1] AS route

        MATCH (route)<-[:HAS_ROUTE]-(ca:Carrier)
        WITH route, splcPair[0] AS originSPLC, splcPair[1] AS destSPLC, ca AS routeCarrier
        MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
        MATCH (route)-[:HAS_CURRENT_RATE]->(ra:RailRate)-[:IN_CURRENCY]->(rC)
        MATCH (oc:Country)<-[:IN_COUNTRY]-()<-[:FROM]-(route)-[:TO]->()-[:IN_COUNTRY]->(dc:Country)
            , (oc)<-[:FOR_ORIGIN_COUNTRY]-(f:RailFSC)-[:FOR_DESTINATION_COUNTRY]->(dc)
            , (routeCarrier)-[:HAS_FSC]->(f)-[:IN_CURRENCY]->(fC)
        // MATCH (fC)-[fe:HAS_EXCHANGE_RATE]->(tC)
        // MATCH (rC)-[re:HAS_EXCHANGE_RATE]->(tC)
        // MATCH (lqUOM:MassUOM {id:'ST'})-[cr:HAS_CONVERSION_RATE]->(:MassUOM{id: $neodash_massuom_id})
        WHERE (
            NOT (f)-[:FOR_PRODUCTGROUP]->() 
            OR (f)-[:FOR_PRODUCTGROUP]->(lpg) 
            OR (f)-[:FOR_PRODUCTGROUP]->()-[:HAS_CHILD]->(lpg)
        )
        AND f.expirationDate IS NULL

        OPTIONAL MATCH (originSPLC)-[rdt:RAIL_DISTANCE_TO{carrier:routeCarrier.id}]-(destSPLC)
        
        WITH f.rate AS fsc 
            , toLower(ra.uom) AS rateType
            , coalesce(rdt.distance,10000) AS miles
            , ra.rate AS baseRate
            , lpg.railCarVol AS carVol
            , routeCarrier.id AS carrier
            , [originSPLC.id, destSPLC.id] AS splcPair
            , ra.car_owner_carrier AS isPublic
            , ra.car_owner_shipper AS isPrivate

        WITH miles, fsc/carVol AS perTonFuel, carrier, splcPair, isPublic, isPrivate,
        CASE rateType 
            WHEN 'ton' THEN baseRate
            WHEN 'car' THEN baseRate / carVol
            ELSE 0
        END AS perTonRate

        // WITH f.rate*fe.rate AS fsc, toLower(ra.uom) AS rateType, coalesce(rdt.distance,10000) AS miles
        //     , ra.rate AS baseRate, re.rate AS rateCurExch, cr.rate AS massConvRate
        //     , lpg.railCarVol AS carVol, routeCarrier.id AS carrier, [originSPLC.id, destSPLC.id] AS splcPair
        //     , ra.car_owner_carrier AS isPublic, ra.car_owner_shipper AS isPrivate

        // WITH miles, fsc/carVol AS perTonFuel, carrier, splcPair, isPublic, isPrivate,
        // CASE rateType 
        //     WHEN 'ton' THEN (baseRate * rateCurExch) / massConvRate
        //     WHEN 'car' THEN (baseRate * rateCurExch) / (carVol * massConvRate)
        //     ELSE 0
        // END AS perTonRate

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
    }

return path, routeInfoList //limit 5