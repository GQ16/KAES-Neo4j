MATCH (n:FRARailNode)<-[:CONNECTS]-(l:FRARailLine)-[:CONNECTS]->(n2:FRARailNode) 
WHERE n <> n2 
WITH distinct l, collect(n)[0] AS n,collect(n2)[0] AS n2
WITH n,n2,l.miles as mileagePerRel,l.km AS kmPerRel
//, (n.coordinate.latitude + n2.coordinate.latitude)/2 AS avgLat, (n.coordinate.longitude + n2.coordinate.longitude)/2 AS avgLong
WHERE NOT (n)-[:RAILLINE]-(n2)
MERGE (n)-[nr:RAILLINE]->(n2)
set nr.miles = mileagePerRel, nr.km = kmPerRel
//, nr.latitude = avgLat, nr.longitude = avgLong