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
    WHERE point.distance(s2.coordinate, s1.coordinate) >= point.distance(ls.coordinate, le.coordinate)
){1,2}
(s2:SPLC)

WITH path, [r IN nodes(path) WHERE r:RailRoute] AS routes
WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
WITH path, routes

CALL{
    WITH path, routes
    WITH path, routes, [x IN nodes(path) WHERE x:SPLC] AS splcList
    WITH path, apoc.coll.pairsMin(splcList) AS splcPairs, routes
    CALL apoc.coll.zipToRows(splcPairs,routes) YIELD value AS splcPairRoute
    WITH path, splcPairRoute[0] AS splcPair, splcPairRoute[1] AS route
    MATCH (route)<-[:HAS_ROUTE]-(c:Carrier)
    WITH path, splcPair[0] AS originSPLC, splcPair[1] AS destSPLC, c.id AS routeCarrier
    MATCH (originSPLC)-[rdt:RAIL_DISTANCE_TO{carrier:routeCarrier}]-(destSPLC)
    RETURN rdt.distance AS miles
}
CALL{
    WITH path, routes
    UNWIND routes AS route

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

    WITH path, route, f.rate*coalesce(fe.rate,1) AS fsc, miles, toLower(ra.uom) AS rateType
    , ra.rate AS baseRate, coalesce(re.rate,1) AS rateCurExch, coalesce(cr.rate,1) AS massConvRate, coalesce(lpg.railCarVol,1) AS carVol

    WITH *,
        CASE rateType 
            WHEN 'ton' THEN (baseRate * rateCurExch) / massConvRate
            WHEN 'car' THEN (baseRate * rateCurExch) / (carVol * massConvRate)
            ELSE 0
        END AS perTonRate

    RETURN min(perTonRate) AS lowestRate, fsc/carVol AS perTonFuel
}

WITH path, {lowestRate:lowestRate, fsc:perTonFuel, miles:miles} AS routeCostMap

WITH path
, nodes(path)[0].id AS origin
, nodes(path)[-1].id AS dest
, round(reduce(price = 0, x IN collect(routeCostMap) | price + x.lowestRate + (x.fsc * x.miles) ),2) AS price

WITH origin, dest, min(price) AS freight 

// RETURN origin, dest, "RAIL" AS mode, 
//     CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight,3) ELSE round(freight,2) END AS freight, 
//     CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(indication,3) ELSE round(indication,2) END AS indication, 
//     CASE $neodash_logisticsproductgroup_name WHEN 'METHANOL' THEN round(freight + indication,3) ELSE round(freight + indication, 2) END AS deliveredPrice 
// ORDER BY freight