WITH CASE 
	WHEN isEmpty($neodash_new_location_name) AND NOT isEmpty($neodash_location_name)
	THEN {old:$neodash_location_name,new:$neodash_location_name}
	WHEN NOT isEmpty($neodash_new_location_name) AND isEmpty($neodash_location_name)
	THEN {old:$neodash_new_location_name,new:$neodash_new_location_name}
	ELSE {old:$neodash_location_name,new:$neodash_new_location_name}
	END AS locationNameMap

MERGE (l:Location {name:locationNameMap.old})
SET
  l.name = locationNameMap.new
  ,l.address = $neodash_address
 
WITH l
//Party Rel (Location Owner)
CALL{
    WITH l
    OPTIONAL MATCH (l)<-[r:OWNS_LOCATION]-(:Party) DELETE r
    //someone could add an owner and then remove it, hence the order
	WITH l, $neodash_party_name_1 AS own
	WHERE NOT isEmpty(own)
    MATCH (p:Party {id: $neodash_party_name_1})
    WITH l, p
  
    WITH l, p
    MERGE (l)<-[r2:OWNS_LOCATION]-(p)
}
//City & State Rel
CALL{
    WITH l
    MATCH (c:CityState {id: $neodash_citystate_id})-[:IN_STATE]->(s:StateOrProvince)
    WITH l, c, s
    
    OPTIONAL MATCH (l)-[r:IN_CITY]->(:CityState) DELETE r
    WITH l, c, s
    OPTIONAL MATCH (l)-[r1:IN_STATE]->(:StateOrProvince) DELETE r1

    WITH l, c, s
    
    MERGE (s)<-[:IN_STATE]-(l)-[r:IN_CITY]->(c)
}
//Country Rel
CALL{
    WITH l
    MATCH (c:Country {id: $neodash_country_id})
    WITH l, c
    
    OPTIONAL MATCH (l)-[r:IN_COUNTRY]->(:Country) DELETE r
    WITH l, c

    MERGE (l)-[:IN_COUNTRY]->(c)
}
//Zip Rel
CALL{
    WITH l
    MATCH (z:ZipCode {id: $neodash_zipcode_id})
    WITH l, z

    OPTIONAL MATCH (l)-[r:IN_ZIPCODE]->(:ZipCode) DELETE r
    
    WITH l, z
    
    MERGE (l)-[r:IN_ZIPCODE]->(z)
    //SET l.latitude = z.latitude
    //, l.longitude = z.longitudeWITH l,z,
    SET l.coordinate =
    CASE 
      WHEN (isEmpty($neodash_latitude) or isEmpty($neodash_longitude))
      THEN z.coordinate
      ELSE point({
		latitude:toFloat($neodash_latitude),
		longitude:toFloat($neodash_longitude)
		})
    END
}
//Inbound Mode Rel
CALL{
  WITH l
  MATCH (m:Mode)
  WHERE m.id IN $neodash_mode_id_1
  WITH l, m

  OPTIONAL MATCH (l)-[r:HAS_INBOUND]->(:Mode)
  DELETE r

  WITH l, m
  MERGE (l)-[r1:HAS_INBOUND]->(m)
  SET r1.update_date = datetime()
}
//Outbound Mode Rel
CALL{
  WITH l
  MATCH (m:Mode)
  WHERE m.id IN $neodash_mode_id_2
  WITH l, m

  OPTIONAL MATCH (l)-[r:HAS_OUTBOUND]->(:Mode)
  DELETE r

  WITH l, m
  MERGE (l)-[r1:HAS_OUTBOUND]->(m)
  SET r1.update_date = datetime()
}
//SPLC Rel
CALL{
    WITH l
    MATCH (s:SPLC {id: $neodash_splc_id})
    WITH l, s
    
    OPTIONAL MATCH (l)-[r:IN_SPLC]->(:SPLC) DELETE r
    WITH l, s

    MERGE (l)-[:IN_SPLC]->(s)
}