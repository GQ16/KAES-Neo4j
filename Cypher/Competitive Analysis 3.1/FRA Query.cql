MATCH op=(orn:FRARailNode)-[:CLOSE_TO]->(ors:RailStation)-[:IN_ZIPCODE]->(:ZipCode)-[:IN_CITY]->(oc:CityState)-->(os:StateOrProvince)
MATCH dp=(drn:FRARailNode)-[:CLOSE_TO]->(drs:RailStation)-[:IN_ZIPCODE]->(:ZipCode)-[:IN_CITY]->(dc:CityState)-->(ds:StateOrProvince)
WHERE orn.id = 414739
and drn.id = 417472
MATCH p = 
(orn)(
    (f:FRARailNode)<--(l:FRARailLine)-->(t:FRARailNode) 
    WHERE 1=1
    // AND (:Carrier{id:'UP'})-[:OWNS_RAIL_LINE]->(l)
    AND (point.distance(t.coordinate, drn.coordinate) - point.distance(f.coordinate, drn.coordinate)) < 500
)+(drn)
WITH  
p
// ,op,dp
LIMIT 1
WITH p, [x in nodes(p) where x:FRARailNode] AS railNodes
UNWIND railNodes AS rn
MATCH (rn)-[:CLOSE_TO]->(rs:RailStation)-[:IN_LOCATION_GROUP]->(lg:LocationGroup)
// WHERE lg.id STARTS WITH 'CSXT'
WITH collect(DISTINCT lg) AS groupList
// WITH apoc.coll.pairs(groupList) AS groupPairs
// UNWIND groupPairs AS groupLeg
// RETURN groupLeg[0] AS startGroup, groupLeg[1] AS endGroup
WITH groupList[0] AS startGroup, groupList[-1] AS endGroup
MATCH path = (startGroup)<--(r:Rate)-->(endGroup),(efd:Date)<-[:HAS_EFFECTIVE]-(r)-[:HAS_EXPIRATION]->(exd:Date)
WHERE efd.id <= date() AND exd.id >= date()
OPTIONAL MATCH path2 = (r)-[:FOR_STCC]->(:STCC)-[:REPRESENTS]->(:LogisticsProductGroup)
OPTIONAL MATCH path3 = (r)-[:FOR_STCCGROUP]->(:STCCGroup)<-[:IN_STCCGROUP]-(:STCC)-[:REPRESENTS]->(:LogisticsProductGroup)
RETURN path, path2, path3