//Rail
MATCH (o:Location)-[r:RAIL_TO]->(d:Location)
	,(c2:City{id:$neodash_city_id_2})<-[:IN]-(d)-[:IN]->(s2:StateOrProvince{id:$neodash_stateorprovince_id_2})
RETURN DISTINCT r.productType AS product

UNION

//Liquid Truck
MATCH (o:Location)-[tt:TRUCK_TO]->(d:Location)
	,(d)-[i1:IN]->(c2:City{id:$neodash_city_id_2})-[i2:IN]->(s2:StateOrProvince{id:$neodash_stateorprovince_id_2})
	,(d)-[i3:IN]->(s2)
	,(o)-[i5:IN]-(c:City),(o)-[i6:IN]->(s:StateOrProvince)
	,(c)-[i7:IN]->(s)
	,(s)-[i4:IN]->(re:Region)-[hr:HAS_RATE]->(ra:Rate)
	,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTTYPE]->(pt:ProductType)
	,(ra)-[fp:FOR_PRODUCTTYPE]->(pt)

	WHERE 1=1
	AND NOT o.type = 'JUNCTION'
	AND re.type = 'ORIGIN'

RETURN DISTINCT pt.id as product

UNION

//Dry Truck
MATCH (re:Region)-[hr:HAS_RATE]->(ra:Rate)<-[i4:IS_ORIGIN_FOR]-(o:Location)
	,(ra)-[fp:FOR_PRODUCTTYPE]->(pt:ProductType)
	,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTTYPE]->(pt)
	,(s2:StateOrProvince{id:$neodash_stateorprovince_id_2})<-[i9:IN]-(z:ZipCode)-[i8:IN]->(re)
	,(o)-[tt:TRUCK_TO]->(d:Location)
	,(d)-[i1:IN]->(c2:City{id:$neodash_city_id_2})-[i2:IN]->(s2)
	,(d)-[i3:IN]->(s2)

	WHERE 1=1
	AND NOT o.type = 'JUNCTION'
	AND re.type = 'DESTINATION'
	AND tt.destZip = z.id

RETURN DISTINCT pt.id AS product