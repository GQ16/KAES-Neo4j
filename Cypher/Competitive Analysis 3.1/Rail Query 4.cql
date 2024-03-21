PROFILE
MATCH (lpg:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})

MATCH op=(oc:CityState)<-[:IN_CITY]-(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(ca:Carrier)
WHERE oc.id = $neodash_citystate_id_1 AND oc.country = 'US'

MATCH dp=(ca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode)-[:IN_CITY]->(dc:CityState)
WHERE dc.id = $neodash_citystate_id_2 AND dc.country = 'US'

WITH DISTINCT ors,drs, lpg, ca
CALL{
	WITH ors,drs, lpg, ca
	MATCH p=(ors)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 0
	){1,5}(drs)
	WHERE all(x IN [y IN nodes(p) WHERE y:Rate] WHERE (ca)-[:HAS_RATE]->(x)-[:FOR_PRODUCTGROUP]->(lpg))
	RETURN p AS path, 1 AS level
	
	UNION
	
	WITH ors,drs, lpg, ca
	MATCH p=(ors)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 0
	){1,5}(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)
	WHERE all(x IN [y IN nodes(p) WHERE y:Rate] WHERE (ca)-[:HAS_RATE]->(x)-[:FOR_PRODUCTGROUP]->(lpg))
	RETURN p AS path, 2 AS level
	
	UNION
	
	WITH ors,drs, lpg, ca
	MATCH p=(ors)-[:IN_LOCATION_GROUP]->(olg:LocationGroup)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 0
	){1,5}(drs)
	WHERE all(x IN [y IN nodes(p) WHERE y:Rate] WHERE (ca)-[:HAS_RATE]->(x)-[:FOR_PRODUCTGROUP]->(lpg))
	RETURN p AS path, 3 AS level	
	
	UNION

	WITH ors,drs, lpg, ca
	MATCH p=(ors)-[:IN_LOCATION_GROUP]->(olg:LocationGroup)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 0
	){1,5}(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)
	WHERE all(x IN [y IN nodes(p) WHERE y:Rate] WHERE (ca)-[:HAS_RATE]->(x)-[:FOR_PRODUCTGROUP]->(lpg))
	RETURN p AS path, 4 AS level
	
}
RETURN path, level limit 5