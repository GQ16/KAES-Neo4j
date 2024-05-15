//Liquid Truck Query
MATCH (prod:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
WHERE prod.name IN ['LIQUID', 'UAN', 'ATS', '10-34-0', 'AMMONIA', 'NITAMIN']
MATCH (st:StateOrProvince)<-[:IN_STATE]-(dl:Location)-[:IN_ZIPCODE]->(dz:ZipCode)
MATCH (occ:Occupant)<-[:HAS_OCCUPANT]-(ol:Location)-[:IN_ZIPCODE]->(oz:ZipCode)
	, (ol)-[:IN_STATE]->(os:StateOrProvince)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dc:Country)
  	, (dc)<-[:IN_COUNTRY]-(dz)
	, (tr)-[:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup)
	, (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
	, (oz)-[:IN_COUNTRY]->(c:Country)
	, (tFSC)-[:FOR_PRODUCTGROUP]->(lpg)
	, (occ)-[:COMPETES_IN]->(st)

WHERE 1=1
AND dl.id = $neodash_location_id_2
AND lpg.name = 'LIQUID'
AND cr.distanceLower < tmt.distance <= cr.distanceUpper
AND tFSC.expirationDate IS NULL
AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
AND (
	(prod)<-[:HAS_PRODUCTGROUP]-(o)
	OR (lpg)<-[:HAS_PRODUCTGROUP]-(o)
)

OPTIONAL MATCH (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
OPTIONAL MATCH (occ)<-[:IS_OCCUPANT]-(party:Party)

RETURN l, p.name AS occupantPartyName, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, tmt, tFSC
