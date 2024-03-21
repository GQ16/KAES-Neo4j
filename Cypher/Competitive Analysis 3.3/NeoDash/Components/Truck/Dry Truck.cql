//Dry Truck Query
MATCH (prod:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
WHERE prod.name IN ['UREA', 'MAP', 'DAP', 'PHOSPHATE', 'DRY', 'POTASH', 'SUPERU']
MATCH (o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)
, (l)-[:IN_CITY]->(ocs:CityState)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
, (tr)-[:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name:'DRY'})
, (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
, (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
, (oz)-[:IN_COUNTRY]->(c:Country)
, (tFSC:TruckFSC)-[:FOR_PRODUCTGROUP]->(lpg)

WHERE 1=1
AND cr.distanceLower < tmt.distance <= cr.distanceUpper
AND date('2024-02-29') <= tFSC.expirationDate
AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
AND (
  (prod)<-[:HAS_PRODUCTGROUP]-(o)
  OR (lpg)<-[:HAS_PRODUCTGROUP]-(o)
)
RETURN l,cr,tmt,tFSC,dz