match (c:Carrier)-[:OWNS_STATION]->(rs:RailStation)-[:IN_LOCATION_GROUP]->(l:LocationGroup)
MERGE (c)-[:OWNS_STATIONS_IN]->(l)
//
match (l:LocationGroup)<--(r:Rate)<-[:HAS_RATE]-(c:Carrier)
MERGE (c)-[:OWNS_STATION_GROUP]->(l)
//
MATCH (lg:LocationGroup)<-[:IN_LOCATION_GROUP]-(:RailStation)-[:PART_OF_INTERCHANGE]->(i:Interchange)
MERGE (lg)-[:HAS_STATION_IN]->(i)
//
MATCH (n:Interchange)<-[:HAS_STATION_IN]-(:LocationGroup)<-[:OWNS_STATION_GROUP]-(c:Carrier) 
MERGE (c)-[:USES_INTERCHANGE]->(n)
//
MATCH (s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
CREATE (s)-[r:RAIL_TO]->(e)
SET r.rate = ra.rate
//
PROFILE MATCH op=(oc:CityState)<-[:IN_CITY]-(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca:Carrier)
WHERE oc.id = $neodash_citystate_id_1 AND oc.country = 'US'

MATCH dp=(dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode)-[:IN_CITY]->(dc:CityState)
WHERE dc.id = $neodash_citystate_id_2 AND dc.country = 'US'

CALL apoc.algo.aStarConfig(ors, drs, 'RAIL_TO>|PART_OF_INTERCHANGE|HAS_STATION_IN|IN_LOCATION_GROUP',{weight:'rate', pointPropName:'coordinate', default:0}) 
YIELD path, weight
RETURN path, weight