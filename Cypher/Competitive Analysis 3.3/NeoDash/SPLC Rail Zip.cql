PROFILE
//MARK: Products
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products

//MARK: Rail Finding Locations
    MATCH (p:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)
    , (l)-[:IN_SPLC]->(s:SPLC)
    , (o)<-[:IS_OCCUPANT]-(party:Party)
    // , (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    WHERE p IN products
    AND (
        (dz.id = $neodash_zipcode_id_2 AND dz.country = $neodash_country_id_2)
        OR l.id = $neodash_location_id
    )

    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)

    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , party.name AS occupant,  [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes
        , validInterchanges

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH DISTINCT l,dz,origin, destination, validInterchanges, occupant, occupantTypes
//MARK: Routes

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
    END AS isValid
}
WITH path, routes, isValid
WHERE isValid
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