MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
CALL {
	WITH products
	WITH products
	WHERE any(x IN products WHERE x.name IN ['UREA', 'MAP', 'DAP', 'PHOSPHATE', 'DRY', 'POTASH', 'SUPERU'])
	MATCH (lpg1:LogisticsProductGroup)<-[r1:HAS_PRODUCTGROUP]-(o:Occupant)<-[r2:HAS_OCCUPANT]-(l:Location)-[r3:IN_ZIPCODE]->(oz:ZipCode)
    , (l)-[:HAS_OUTBOUND]->(m:Mode{id:'TRUCK'})
	, (l)-[r4:IN_CITY]->(ocs:CityState)<-[r5:FROM]-(tr:TruckRoute)-[r6:TO]->(dlg:LocationGroup)<-[r7:IN_LOCATION_GROUP]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
	, (tr)-[:FOR_PRODUCTGROUP]->(lpg2:LogisticsProductGroup)
	, (tr)-[r8:HAS_CURRENT_RATE]->(cr:TruckRate)
	, (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
	, (oz)-[r9:IN_COUNTRY]->(c:Country)<-[r10:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
	, (tFSC:TruckFSC)-[r11:FOR_PRODUCTGROUP]->(lpg3:LogisticsProductGroup)
	WHERE lpg1 IN products AND lpg2 IN products AND lpg3 IN products
	AND cr.distanceLower < tmt.distance <= cr.distanceUpper
	AND date() <= tFSC.expirationDate
    
	RETURN l,cr,tmt,tFSC,dz

	UNION 

	WITH products
	WITH products
	WHERE any(x IN products WHERE x.name IN ['LIQUID', 'UAN', 'ATS', '10-34-0', 'AMMONIA', 'NITAMIN'])
	MATCH (lpg1:LogisticsProductGroup)<-[r1:HAS_PRODUCTGROUP]-(o:Occupant)<-[r2:HAS_OCCUPANT]-(l:Location)-[r3:IN_ZIPCODE]->(oz:ZipCode)
    , (l)-[:HAS_OUTBOUND]->(m:Mode{id:'TRUCK'})
	, (l)-[r4:IN_STATE]->(os:StateOrProvince)<-[r5:FROM]-(tr:TruckRoute)-[r6:TO]->(dc:Country)<-[r7:IN_COUNTRY]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
	, (tr)-[:FOR_PRODUCTGROUP]->(lpg2:LogisticsProductGroup)
	, (tr)-[r8:HAS_CURRENT_RATE]->(cr:TruckRate)
	, (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
	, (oz)-[r9:IN_COUNTRY]->(oc:Country)<-[r10:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
	, (tFSC:TruckFSC)-[r11:FOR_PRODUCTGROUP]->(lpg3:LogisticsProductGroup)
	WHERE lpg1 IN products AND lpg2 IN products AND lpg3 IN products
	AND cr.distanceLower < tmt.distance <= cr.distanceUpper
	AND date() <= tFSC.expirationDate

	RETURN l,cr,tmt,tFSC,dz
}

MATCH (cr)-[:HAS_RATE_UOM]->(rUOM), (cr)-[:HAS_DISTANCE_UOM]->(dUOM), (cr)-[:IN_CURRENCY]->(cur), (cr)-[:HAS_LOAD_QTY_UOM]->(lqUOM:MassUOM)
OPTIONAL MATCH (cur)-[exch:HAS_EXCHANGE_RATE]->(fC:Currency{id: $neodash_currency_id})
OPTIONAL MATCH (lqUOM)-[massConv:HAS_CONVERSION_RATE]->(fM:MassUOM{id: $neodash_massuom_id})

WITH l AS origin,dz AS destination, tmt.distance AS miles, tFSC.rate AS fsc, 
CASE toUpper(cr.rateFactorType)
	WHEN "DISTANCE" THEN (cr.rate*coalesce(exch.rate,1)*tmt.distance)/(cr.loadQuantity*coalesce(massConv.rate,1))
	WHEN "UNIT" THEN cr.rate*coalesce(exch.rate,1)/coalesce(massConv.rate,1)
END AS rate

RETURN origin, destination,
apoc.create.vRelationship(origin,'TRUCK_TO',
{
  color:'turquoise',
  miles:miles,
  rate:round(rate,3),
  fsc:fsc,
  freight:round(rate * (1+fsc),2)
}
,destination) AS vRel