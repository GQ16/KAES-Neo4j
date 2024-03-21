MATCH (pt:ProductType)
WITH collect(pt.id) AS productTypes
MATCH (c2:City{id:$neodash_city_id_2})<-[:IN]-(d:Location)-[:IN]->(s2:StateOrProvince{id:$neodash_stateorprovince_id_2})
OPTIONAL MATCH (o:Location)-[t:TRUCK_TO]->(d)
OPTIONAL MATCH path = (o2:Location)-[r:RAIL_TO*..3]->(d)
WHERE 1=1
    AND all(
		x IN relationships(path)
		WHERE 1=1
		AND NOT x.miles = 0
		// AND (x.expirationDate >= date($neodash_ship_date) OR x.expirationDate IS NULL)
		)
    AND single(
        et IN ['SINGLE','85 UT', '100 UT'] 
        WHERE all(
            x IN relationships(path) 
            WHERE x.equipmentType = et
            )
        )
    AND single(
        pt IN productTypes
        WHERE all(
            x IN relationships(path) 
            WHERE x.productType = pt
            )
        )
WITH o,o2
CALL apoc.case([
  (o IS NULL AND o2 IS NOT NULL), 'MATCH (pt)<-[:HAS_PRODUCTTYPE]-(o2) RETURN DISTINCT pt.id AS Result',
  (o IS NOT NULL AND o2 IS NULL), 'MATCH (pt)<-[:HAS_PRODUCTTYPE]-(o) RETURN DISTINCT pt.id AS Result',
  (o IS NOT NULL AND o2 IS NOT NULL), 'MATCH (o)-[:HAS_PRODUCTTYPE]->(pt:ProductType)<-[:HAS_PRODUCTTYPE]-(o2) RETURN DISTINCT  pt.id AS Result'
],'RETURN "NO PRODUCTS" AS Result',{o:o,o2:o2}) YIELD value AS ProductType
RETURN DISTINCT ProductType.Result