PROFILE
MATCH op=(oc:CityState)<-[:IN_CITY]-(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca:Carrier)
WHERE oc.city = 'ENID' AND oc.stateOrProvince = 'OK' AND oc.country = 'US'

MATCH dp=(dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode)-[:IN_CITY]->(dc:CityState)
WHERE dc.city = 'RUSHVILLE' AND dc.stateOrProvince = 'IN' AND dc.country = 'US'
WITH DISTINCT ors,drs LIMIT 10
CALL{
	WITH ors,drs
	WITH ors,drs
	WHERE EXISTS {MATCH (ors)<-[:FROM]-(:Rate)} 
		AND EXISTS {MATCH (:Rate)-[:TO]->(drs)}
	MATCH p=(ors)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 5
		AND (ra)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name:'UREA'})
	){1,5}(drs)
	RETURN p AS path, 1 AS level
	
	UNION
	
	WITH ors,drs
	WITH ors,drs
	WHERE EXISTS {MATCH (ors)<-[:FROM]-(:Rate)} 
		AND EXISTS {MATCH (:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)}
	MATCH p=(ors)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 5
		AND (ra)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name:'UREA'})
	){1,5}(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)
	RETURN p AS path, 2 AS level
	
	UNION
	
	WITH ors,drs
	WITH ors,drs
	WHERE EXISTS {MATCH (ors)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(:Rate)}
		AND EXISTS {MATCH (:Rate)-[:TO]->(drs)}
	MATCH p=(ors)-[:IN_LOCATION_GROUP]->(olg:LocationGroup)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 5
		AND (ra)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name:'UREA'})
	){1,5}(drs)
	RETURN p AS path, 3 AS level	
	
	UNION

	WITH ors,drs
	WITH ors,drs
	WHERE EXISTS {MATCH (ors)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(:Rate)}
		AND EXISTS {MATCH (:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)}
	MATCH p=(ors)-[:IN_LOCATION_GROUP]->(olg:LocationGroup)((s:RailStation|LocationGroup)<-[:FROM]-(ra:Rate)-[:TO]->(e:RailStation|LocationGroup)
		WHERE (point.distance(drs.coordinate, e.coordinate)-point.distance(drs.coordinate, s.coordinate))/1000 < 5
		AND (ra)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name:'UREA'})
	){1,5}(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)
	RETURN p AS path, 4 AS level
	
}
RETURN path, level limit 5