PROFILE
MATCH (lpg:LogisticsProductGroup{name:'UREA'})

MATCH op=(oc:CityState)<-[:IN_CITY]-(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca:Carrier)
WHERE oc.city = 'ENID' AND oc.stateOrProvince = 'OK' AND oc.country = 'US'

MATCH dp=(dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode)-[:IN_CITY]->(dc:CityState)
WHERE dc.city = 'CHICAGO' AND dc.stateOrProvince = 'IL' AND dc.country = 'US'
WITH DISTINCT ors,drs, lpg
CALL{
	WITH ors,drs, lpg
	WITH ors,drs, lpg
	WHERE EXISTS {MATCH (ors)<-[:FROM]-(:Rate)} 
		AND EXISTS {MATCH (:Rate)-[:TO]->(drs)}
	MATCH p=(ors)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 0
	){1,5}(drs)
	WHERE all(x IN [y IN nodes(p) WHERE y:Rate] WHERE (x)-[:FOR_PRODUCTGROUP]->(lpg))
	RETURN p AS path, 1 AS level
	
	UNION
	
	WITH ors,drs, lpg
	WITH ors,drs, lpg
	WHERE EXISTS {MATCH (ors)<-[:FROM]-(:Rate)} 
		AND EXISTS {MATCH (:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)}
	MATCH p=(ors)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 0
	){1,5}(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)
	WHERE all(x IN [y IN nodes(p) WHERE y:Rate] WHERE (x)-[:FOR_PRODUCTGROUP]->(lpg))
	RETURN p AS path, 2 AS level
	
	UNION
	
	WITH ors,drs, lpg
	WITH ors,drs, lpg
	WHERE EXISTS {MATCH (ors)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(:Rate)}
		AND EXISTS {MATCH (:Rate)-[:TO]->(drs)}
	MATCH p=(ors)-[:IN_LOCATION_GROUP]->(olg:LocationGroup)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 0
	){1,5}(drs)
	WHERE all(x IN [y IN nodes(p) WHERE y:Rate] WHERE (x)-[:FOR_PRODUCTGROUP]->(lpg))
	RETURN p AS path, 3 AS level	
	
	UNION

	WITH ors,drs, lpg
	WITH ors,drs, lpg
	WHERE EXISTS {MATCH (ors)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(:Rate)}
		AND EXISTS {MATCH (:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)}
	MATCH p=(ors)-[:IN_LOCATION_GROUP]->(olg:LocationGroup)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 0
	){1,5}(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)
	WHERE all(x IN [y IN nodes(p) WHERE y:Rate] WHERE (x)-[:FOR_PRODUCTGROUP]->(lpg))
	RETURN p AS path, 4 AS level
	
}
RETURN path, level limit 5