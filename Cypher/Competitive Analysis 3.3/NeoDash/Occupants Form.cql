//Occupant
//Inputs: Location Id, Occupant Id, Occupant Type, Market Id, Party Id, ProductGroup Id,
MATCH (l:Location{name:$neodash_location_name})
MERGE (o:Occupant {id: randomUUID()})<-[:HAS_OCCUPANT]-(l)
SET o.type = $neodash_occupant_type,
o.update_date = datetime()
WITH o
CALL apoc.create.addLabels(o,o.type)
YIELD node
REMOVE node.type
WITH node AS o

//Party Rel
CALL{
    WITH o
	WITH o, $neodash_party_name_2 AS pa
	WHERE NOT isEmpty(pa) //pa IS NOT NULL
    MERGE (p:Party {name: pa})
    WITH o, p
    
    OPTIONAL MATCH (:Party)-[r:IS_OCCUPANT]->(o) DELETE r
    
    WITH o, p
    
    MERGE (p)-[r2:IS_OCCUPANT]->(o)
}
//Competes In Relationship
CALL{
  WITH o
  WITH o, $neodash_stateorprovince_id AS stateList
  WHERE NOT isEmpty(stateList)
  MATCH (s:StateOrProvince)
  WHERE s.id IN stateList
  MERGE (o)-[:COMPETES_IN]->(s)
}
//Competes In Relationship when empty
CALL{
  WITH o
  WITH o, $neodash_stateorprovince_id AS stateList
  WHERE isEmpty(stateList)
  MATCH (cs:StateOrProvince)<-[:COMPETES_IN]-(os:StateOrProvince)<-[:IN_STATE]-(l:Location)-[:HAS_OCCUPANT]->(o)
  WITH DISTINCT o, cs
  MERGE (o)-[:COMPETES_IN]->(cs)
}