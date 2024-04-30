//SPLC Rail Miles
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
WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->()) //may move into QPP to allow for R11 checks
WITH path, routes, [x IN nodes(path) WHERE x:SPLC] AS splcList
WITH path, apoc.coll.pairsMin(splcList) AS splcPairs, routes
WHERE all(s IN splcPairs WHERE s)
CALL apoc.coll.zipToRows(splcPairs,routes) YIELD value AS splcPairRoute
WITH path, splcPairRoute[0] AS splcPair, splcPairRoute[1] AS route

MATCH (route)<-[:HAS_ROUTE]-(c:Carrier)
WITH path, route, splcPair[0] AS originSPLC, splcPair[1] AS destSPLC, c.id AS routeCarrier
OPTIONAL MATCH (originSPLC)-[rdt:RAIL_DISTANCE_TO{carrier:routeCarrier}]-(destSPLC)
with path, route, coalesce(rdt.distance,10000) AS miles

with path, sum(miles) AS totalMiles
WHERE totalMiles < 10000
return path, totalMiles