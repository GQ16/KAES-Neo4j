//Methanol Truck
MATCH (prod:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
MATCH (prod)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)
, (l)-[:IN_CITY]->(ocs:CityState)
, (ocs)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dcs:CityState{id:$neodash_citystate_id_2})
, (dcs)<-[:IN_CITY]-(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
, (tr)-[:FOR_PRODUCTGROUP]->(prod)
, (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
, (l)-[:IN_COUNTRY]->(c:Country)<-[:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
, (tFSC:TruckFSC)-[:FOR_PRODUCTGROUP]->(prod)

WHERE (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
AND date('2024-02-29') <= tFSC.expirationDate

RETURN l,cr,null AS tmt,tFSC,dz