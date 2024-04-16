PROFILE
MATCH (s1:SPLC)
MATCH (s2:SPLC)
WHERE 1 = 1
and s1.id = '623550000'
and s2.id = '349617000'
    
MATCH path = 
(s1:SPLC)
(
    (ls:SPLC)<-[:IN_SPLC]-(o)<-[:UREA_FROM]-(:RailRoute)-[:UREA_TO]->(d)-[:IN_SPLC]->(le:SPLC)
    // WHERE point.distance(s2.coordinate, s1.coordinate) >= point.distance(ls.coordinate, le.coordinate)
){1,2}
(s2:SPLC)

WITH path, [r IN nodes(path) WHERE r:RailRoute] AS routes
WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->()) //may move into QPP to allow for R11 checks
WITH path, routes, [x IN nodes(path) WHERE x:SPLC] AS splcList
WITH path, apoc.coll.pairsMin(splcList) AS splcPairs, routes
CALL apoc.coll.zipToRows(splcPairs,routes) YIELD value AS splcPairRoute
WITH path, splcPairRoute[0] AS splcPair, splcPairRoute[1] AS route

    MATCH (route)<-[:HAS_ROUTE]-(ca:Carrier)
    WITH path, route, splcPair[0] AS originSPLC, splcPair[1] AS destSPLC, ca AS routeCarrier

    MATCH (tC:Currency{id: $neodash_currency_id})
    MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    MATCH (route)-[:HAS_CURRENT_RATE]->(ra:RailRate)-[:IN_CURRENCY]->(rC)
    MATCH (oc:Country)<-[:IN_COUNTRY]-()<-[:FROM]-(route)-[:TO]->()-[:IN_COUNTRY]->(dc:Country)
        , (oc)<-[:FOR_ORIGIN_COUNTRY]-(f:RailFSC)-[:FOR_DESTINATION_COUNTRY]->(dc)
        , (routeCarrier)-[:HAS_FSC]->(f)-[:IN_CURRENCY]->(fC)
    WHERE (
        NOT (f)-[:FOR_PRODUCTGROUP]->() 
        OR (f)-[:FOR_PRODUCTGROUP]->(lpg) 
        OR (f)-[:FOR_PRODUCTGROUP]->()-[:HAS_CHILD]->(lpg)
    )
    AND f.expirationDate IS NULL

    OPTIONAL MATCH (fC)-[fe:HAS_EXCHANGE_RATE]->(tC)
    OPTIONAL MATCH (rC)-[re:HAS_EXCHANGE_RATE]->(tC)
    OPTIONAL MATCH (lqUOM:MassUOM {id:'ST'})-[cr:HAS_CONVERSION_RATE]->(:MassUOM{id: $neodash_massuom_id})
    OPTIONAL MATCH (originSPLC)-[rdt:RAIL_DISTANCE_TO{carrier:routeCarrier.id}]-(destSPLC)

    WITH path, route, f.rate*coalesce(fe.rate,1) AS fsc, toLower(ra.uom) AS rateType, coalesce(rdt.distance,10000) AS miles
    , ra.rate AS baseRate, coalesce(re.rate,1) AS rateCurExch, coalesce(cr.rate,1) AS massConvRate
    , coalesce(lpg.railCarVol,1) AS carVol, routeCarrier.id AS carrier, [originSPLC.id, destSPLC.id] AS sp

    WITH *,
        CASE rateType 
            WHEN 'ton' THEN (baseRate * rateCurExch) / massConvRate
            WHEN 'car' THEN (baseRate * rateCurExch) / (carVol * massConvRate)
            ELSE 0
        END AS perTonRate

    WITH path, route, miles, min(perTonRate) AS lowestRate, fsc/carVol AS perTonFuel, sp, carrier


return path, collect({lowestRate:lowestRate, fsc:perTonFuel, miles:miles}) AS routeCostMaps, collect(sp), collect(carrier) 

// WITH path
// , nodes(path)[0].id AS origin
// , nodes(path)[-1].id AS dest
// , round(reduce(price = 0, x IN routeCostMaps | price + x.lowestRate + (x.fsc * x.miles) ),2) AS price

// RETURN origin, dest, min(price) AS freight 