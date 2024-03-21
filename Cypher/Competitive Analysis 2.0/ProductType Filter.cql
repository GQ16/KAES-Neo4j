//Rail
MATCH (c2:City{id:$neodash_city_id_2})<-[:IN_CITY]-(d)-[:IN_STATE]->(s2:StateOrProvince{id:$neodash_stateorprovince_id_2})
, (o:Location)-[r:RAIL_TO]->(d:Location), (c2)-[:IN_STATE]->(s2)
RETURN DISTINCT r.productType AS product

UNION

//Liquid Truck
MATCH (d:Location)-[:IN_CITY]->(c2:City{id:$neodash_city_id_2})-[:IN_STATE]->(s2{id:$neodash_stateorprovince_id_2})
    ,(d)-[:IN_STATE]->(s2)
    ,(s2)<-[:IN_STATE]-(dz:ZipCode)-[:IN_CITY]->(c2)
    ,(oz:ZipCode)-[tt:TRUCK_TO]->(dz)
    ,(s:StateOrProvince)<-[:IN_STATE]-(oz)-[:IN_CITY]->(c:City)
    ,(o:Location)-[:IN_CITY]->(c)-[:IN_STATE]->(s)
    ,(o)-[:HAS_PRODUCTTYPE]->(pt:ProductType)
    ,(o)-[:IN_STATE]->(s)
    ,(s)-[:IN_REGION]->(re:Region)-[hr:HAS_RATE]->(ra:Rate)
    ,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTTYPE]->(pt)
    ,(ra)-[:FOR_PRODUCTTYPE]->(pt)

	WHERE 1=1
	AND NOT o.type = 'JUNCTION'
	AND re.type = 'ORIGIN'

RETURN DISTINCT pt.id as product

UNION

//Dry Truck
MATCH (d:Location)-[:IN_CITY]->(c2:City{id:$neodash_city_id_2})-[:IN_STATE]->(s2{id:$neodash_stateorprovince_id_2})
    ,(d)-[:IN_STATE]->(s2)
    ,(dz:ZipCode)-[:IN_CITY]->(c2)
    ,(dz)-[:IN_STATE]->(s2)
    ,(oz:ZipCode)-[tt1:TRUCK_TO]->(dz)-[:IN_REGION]->(re:Region)
    ,(re)-[hr:HAS_RATE]->(ra:Rate)<-[:IS_ORIGIN_FOR]-(o:Location)
    ,(o)-[:HAS_PRODUCTTYPE]->(pt:ProductType)
    ,(ra)-[:FOR_PRODUCTTYPE]->(pt)
    ,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTTYPE]->(pt)
    ,(c:City)<-[:IN_CITY]-(o)-[:IN_STATE]->(s:StateOrProvince)
    ,(c)-[:IN_STATE]->(s)
    ,(c)<-[:IN_CITY]-(oz)
    ,(oz)-[:IN_STATE]->(s)

	WHERE 1=1
	AND NOT o.type = 'JUNCTION'
	AND re.type = 'DESTINATION'

RETURN DISTINCT pt.id AS product