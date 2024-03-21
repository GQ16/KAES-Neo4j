profile
MATCH op=(orn:FRARailNode)-[:CLOSE_TO]->(ors:RailStation)-[:IN_ZIPCODE]->(:ZipCode)-[:IN_CITY]->(oc:CityState{id:'ENID, OK'})
MATCH dp=(drn:FRARailNode)-[:CLOSE_TO]->(drs:RailStation)-[:IN_ZIPCODE]->(:ZipCode)-[:IN_CITY]->(dc:CityState{id:'BOTKINS, OH'})
WITH orn, drn LIMIT 1
call apoc.algo.aStar(orn, drn, 'RAILLINE', 'miles', 'latitude', 'longitude') YIELD path, weight
// call apoc.algo.aStarConfig(orn, drn, 'RAILLINE',{weight:'miles', pointPropName:'coordinate'}) YIELD path, weight
return *

MATCH op=(orn:FRARailNode)-[:CLOSE_TO]->(ors:RailStation)-[:IN_ZIPCODE]->(:ZipCode)-[:IN_CITY]->(oc:CityState{id:'ENID, OK'})
MATCH dp=(drn:FRARailNode)-[:CLOSE_TO]->(drs:RailStation)-[:IN_ZIPCODE]->(:ZipCode)-[:IN_CITY]->(dc:CityState{id:'BOTKINS, OH'})
WITH orn, drn LIMIT 1
call apoc.algo.dijkstra(orn, drn, 'RAILLINE', 'miles') YIELD path, weight
return orn, drn, weight