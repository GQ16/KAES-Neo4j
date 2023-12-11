CALL apoc.periodic.iterate(
"MATCH (co:Country{id:'CA'})
WITH co
MATCH path= (z:ZipCode)-[:IN_CITY]->(c:CityState)-[:IN_STATE]->(s:StateOrProvince)<-[:IN_STATE]-(z)
WHERE size(z.id) = 7
RETURN co,z,c,s",
"MERGE (c)-[:IN_COUNTRY]->(co)
MERGE (s)-[:IN_COUNTRY]->(co)
MERGE (z)-[:IN_COUNTRY]->(co)"
,{})