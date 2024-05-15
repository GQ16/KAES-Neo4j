//Methanol Truck
MATCH (st:StateOrProvince)<-[:IN_STATE]-(dl:Location)-[:IN_ZIPCODE]->(dz:ZipCode)
MATCH (dl)-[:IN_CITY]->(dcs:CityState)
, (prod:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-(occ:Occupant)<-[:HAS_OCCUPANT]-(l:Location)
, (l)-[:IN_CITY]->(ocs:CityState)
, (ocs)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dcs)
, (dcs)<-[:IN_CITY]-(dz)
, (tr)-[:FOR_PRODUCTGROUP]->(prod)
, (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
, (l)-[:IN_COUNTRY]->(c:Country)<-[:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
, (tFSC:TruckFSC)-[:FOR_PRODUCTGROUP]->(prod)

WHERE 1=1
AND dl.id = $neodash_location_id_2
AND prod.name = 'METHANOL'
AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
AND tFSC.expirationDate IS NULL
AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)

OPTIONAL MATCH (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
OPTIONAL MATCH (occ)<-[:IS_OCCUPANT]-(party:Party)

RETURN l, p.name AS occupantPartyName, [x IN labels(o) WHERE x <> 'Occupant'] AS occupantTypes, cr, tmt, tFSC
