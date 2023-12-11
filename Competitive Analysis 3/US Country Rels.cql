CALL apoc.periodic.iterate(
"MATCH (co:Country{id:'US'})
WITH co
MATCH (z:ZipCode)-[:IN_CITY]->(c:CityState)-[:IN_STATE]->(s:StateOrProvince)<-[:IN_STATE]-(z)-[:IN_COUNTY]->(c1:County)-[:IN_STATE]->(s)
WHERE size(z.id) = 5
RETURN c,s,z,c1,co",
"MERGE (c)-[:IN_COUNTRY]->(co)
MERGE (s)-[:IN_COUNTRY]->(co)
MERGE (z)-[:IN_COUNTRY]->(co)
MERGE (c1)-[:IN_COUNTRY]->(co)"
,{})