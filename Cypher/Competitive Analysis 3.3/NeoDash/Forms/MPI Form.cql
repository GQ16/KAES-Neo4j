// Market Price Indications will not be editable. They will be created and that's all.
WITH 
CASE toUpper($neodash_mpi_type)
  WHEN 'ORIGIN' THEN {}
  WHEN 'DESTINATION' THEN
  {
    freight:toInteger($neodash_freight)
  }
END AS mpiFreight,
CASE toUpper($neodash_price_type)
  WHEN 'SINGLE' THEN
  {
    singularPrice:toFloat($neodash_singular_price)
  }
  WHEN 'RANGE' THEN 
  {
    minPrice:toFloat($neodash_minimum_price),
    maxPrice:toFloat($neodash_maximum_price)
  } 
END AS mpiPrice
CREATE (m:MPI)
SET
m.id= randomUUID(),
m.qty=toIntegerOrNull($neodash_mpi_qty),
m.comments= toStringOrNull($neodash_comments),
m.type= $neodash_mpi_type,
m.startDate = $neodash_start_date,
m.endDate = $neodash_end_date,
m.createdDate = datetime(),
m += mpiPrice,
m += mpiFreight

WITH m
//Marketer
CALL{
    WITH m
    MATCH (ma:ContractOwner{name:$neodash_marketer_id})
    CREATE (ma)-[r:LOGGED]->(m)
}
//Party and Contact
CALL{
    WITH m
    MATCH (p:Party{name:$neodash_party_name_3})
    MATCH (c:Contact{id:$neodash_contact_id})
    MERGE (p)<-[r2:BUYS_FOR]-(c)
    CREATE (m)<-[r:PROVIDED]-(p)
    CREATE (c)-[r3:PROVIDED]->(m)
}
//Mode: Location to Mode Rels can be generate from MPI
CALL{
    WITH m
    MATCH (mo:Mode{id:$neodash_mode_id})
    CREATE (m)-[r:FOR_MODE]->(mo)
}
//Location: All indications will have relationship with an origin
CALL{
    WITH m
    MATCH (l:Location{name:$neodash_location_name})
    CREATE (m)-[r:FOR_ORIGIN]->(l)
}
//Location: Delivered indications will also have a relationship with a destination
CALL{
    WITH m
    WITH m
    WHERE m.type = 'DESTINATION'
    MATCH (l2:Location{name:$neodash_location_name_2})
    CREATE (m)-[r:FOR_DESTINATION]->(l2)
}
//Occupant
CALL{
    WITH m
    MATCH (p:Party{name:$neodash_occupant})-[:IS_OCCUPANT]->(oc:Occupant)<-[:HAS_OCCUPANT]-(l:Location{name:$neodash_location_name})
    CREATE (m)-[r:FOR_OCCUPANT]->(oc)
}
//ProductGroup: Location to ProductGroup Rel can be Generated from MPI (when MPI is origin)
CALL{
    WITH m
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name_1})
    CREATE (m)-[r:FOR_PRODUCTGROUP]->(lpg)
}
//Currency
CALL{
    WITH m
    MATCH (c:Currency{id:$neodash_currency_id_1})
    CREATE (m)-[r:IN_CURRENCY]->(c)
}
//MassUOM
CALL{
    WITH m
    MATCH (mu:MassUOM{id:$neodash_massuom_id_1})
    CREATE (m)-[r:HAS_QTY_UOM]->(mu)
}