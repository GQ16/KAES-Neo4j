PROFILE
MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
CALL {
	WITH products
	WITH products
	WHERE any(x IN products WHERE x.name IN ['UREA', 'MAP', 'DAP', 'PHOSPHATE', 'DRY', 'POTASH', 'SUPERU'])
	MATCH (lpg1:LogisticsProductGroup)<-[r1:HAS_PRODUCTGROUP]-(o:Occupant)<-[r2:HAS_OCCUPANT]-(l:Location)-[r3:IN_ZIPCODE]->(oz:ZipCode)
	, (l)-[r4:IN_CITY]->(ocs:CityState)<-[r5:FROM]-(tr:TruckRoute)-[r6:TO]->(dlg:LocationGroup)<-[r7:IN_LOCATION_GROUP]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
	, (tr)-[:FOR_PRODUCTGROUP]->(lpg2:LogisticsProductGroup)
	, (tr)-[r8:HAS_CURRENT_RATE]->(cr:TruckRate)
	, (oz)-[tmt:TRUCK_MILES_TO]->(dz)
	, (oz)-[r9:IN_COUNTRY]->(c:Country)<-[r10:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
	, (tFSC:TruckFSC)-[r11:FOR_PRODUCTGROUP]->(lpg3:LogisticsProductGroup)
	WHERE lpg1 IN products AND lpg2 IN products AND lpg3 IN products
	AND cr.distanceLower < tmt.mileage <= cr.distanceUpper
	AND date() <= tFSC.expirationDate

	WITH l.name AS origin, $neodash_citystate_id_2 AS destination, tmt.mileage AS miles, 
	CASE toUpper(cr.rateFactorType)
		WHEN "DISTANCE" THEN cr.rate*tmt.mileage/cr.loadQuantity
		WHEN "UNIT" THEN cr.rate
	END AS rate
	, tFSC.rate AS fsc

	RETURN origin, destination, miles, round(rate,3) AS rate, fsc, round(rate * (1+fsc),2) AS freight
UNION 
	WITH products
	WITH products
	WHERE any(x IN products WHERE x.name IN ['LIQUID', 'UAN', 'ATS', '10-34-0', 'AMMONIA', 'NITAMIN'])
	MATCH (lpg1:LogisticsProductGroup)<-[r1:HAS_PRODUCTGROUP]-(o:Occupant)<-[r2:HAS_OCCUPANT]-(l:Location)-[r3:IN_ZIPCODE]->(oz:ZipCode)
	, (l)-[r4:IN_STATE]->(os:StateOrProvince)<-[r5:FROM]-(tr:TruckRoute)-[r6:TO]->(dc:Country)<-[r7:IN_COUNTRY]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
	, (tr)-[:FOR_PRODUCTGROUP]->(lpg2:LogisticsProductGroup)
	, (tr)-[r8:HAS_CURRENT_RATE]->(cr:TruckRate)
	, (oz)-[tmt:TRUCK_MILES_TO]->(dz)
	, (oz)-[r9:IN_COUNTRY]->(oc:Country)<-[r10:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
	, (tFSC:TruckFSC)-[r11:FOR_PRODUCTGROUP]->(lpg3:LogisticsProductGroup)
	WHERE lpg1 IN products AND lpg2 IN products AND lpg3 IN products
	AND cr.distanceLower < tmt.mileage <= cr.distanceUpper
	AND date() <= tFSC.expirationDate

	WITH l.name AS origin, $neodash_citystate_id_2 AS destination, tmt.mileage AS miles, 
	CASE toUpper(cr.rateFactorType)
		WHEN "DISTANCE" THEN cr.rate*tmt.mileage/cr.loadQuantity
		WHEN "UNIT" THEN cr.rate
	END AS rate
	, tFSC.rate AS fsc

	RETURN origin, destination, miles, round(rate,3) AS rate, fsc, round(rate * (1+fsc),2) AS freight
}
RETURN origin, destination, miles, rate, fsc, freight
ORDER BY freight