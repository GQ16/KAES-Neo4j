//More concise, but less performant than the union query
MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
WITH CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products

MATCH (lpg1:LogisticsProductGroup)<-[r1:HAS_PRODUCTGROUP]-(o:Occupant)<-[r2:HAS_OCCUPANT]-(l:Location)-[r3:IN_ZIPCODE]->(oz:ZipCode)
, (l)-[r4:IN_CITY|IN_STATE]->(ocs:CityState|StateOrProvince)<-[r5:FROM]-(tr:TruckRoute)-[r6:TO]->(dlg:LocationGroup|Country)<-[r7:IN_LOCATION_GROUP|IN_COUNTRY]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
, (tr)-[:FOR_PRODUCTGROUP]->(lpg2:LogisticsProductGroup)
, (tr)-[r8:HAS_CURRENT_RATE]->(cr:TruckRate)
, (oz)-[tmt:TRUCK_MILES_TO]->(dz)
, (oz)-[r9:IN_COUNTRY]->(c:Country)<-[r10:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
, (tFSC:TruckFSC)-[r11:FOR_PRODUCTGROUP]->(lpg3:LogisticsProductGroup)
WHERE lpg1 IN products AND lpg2 IN products AND lpg3 IN products
AND cr.distanceLower < tmt.mileage <= cr.distanceUpper
AND date() <= tFSC.expirationDate

WITH l AS origin, dz AS destination, tmt.mileage AS miles, 
CASE toUpper(cr.rateFactorType)
	WHEN "DISTANCE" THEN cr.rate*tmt.mileage/cr.loadQuantity
	WHEN "UNIT" THEN cr.rate
END AS rate
, tFSC.rate AS fsc

RETURN origin, destination,
  apoc.create.vRelationship(origin,'TRUCK_TO',
  {
    color:'turquoise',
    miles:miles,
    rate:round(rate,3),
    fsc:fsc,
    freight:round(rate * (1+fsc),2)
  }
  ,destination)