MATCH op=(oc:CityState)<-[:IN_CITY]-(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
WHERE oc.city = 'LEBANON' AND oc.stateOrProvince = 'PA' AND oc.country = 'US'
//'GIRARD, IL'

MATCH dp=(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode)-[:IN_CITY]->(dc:CityState)
WHERE dc.city = 'COHOCTON' AND dc.stateOrProvince = 'NY' AND dc.country = 'US'//'COUNCIL BLUFFS, IA'
CALL{
	WITH ors,drs
	WITH ors,drs
	WHERE EXISTS {MATCH (ors)<-[:FROM]-(:Rate)} 
		AND EXISTS {MATCH (:Rate)-[:TO]->(drs)}
	MATCH p=(ors)((:RailStation|LocationGroup)<-[:FROM]-(:Rate)-[:TO]->(:RailStation|LocationGroup)){1,5}(drs)
	RETURN p AS path, 1 AS level
	
	UNION
	
	WITH ors,drs
	WITH ors,drs
	WHERE EXISTS {MATCH (ors)<-[:FROM]-(:Rate)} 
		AND EXISTS {MATCH (:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)}
	MATCH p=(ors)((:RailStation|LocationGroup)<-[:FROM]-(:Rate)-[:TO]->(:RailStation|LocationGroup)){1,5}(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)
	RETURN p AS path, 2 AS level
	
	UNION
	
	WITH ors,drs
	WITH ors,drs
	WHERE EXISTS {MATCH (ors)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(:Rate)}
		AND EXISTS {MATCH (:Rate)-[:TO]->(drs)}
	MATCH p=(ors)-[:IN_LOCATION_GROUP]->(olg:LocationGroup)((:RailStation|LocationGroup)<-[:FROM]-(:Rate)-[:TO]->(:RailStation|LocationGroup)){1,5}(drs)
	RETURN p AS path, 3 AS level	
	
	UNION

	WITH ors,drs
	WITH ors,drs
	WHERE EXISTS {MATCH (ors)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(:Rate)}
		AND EXISTS {MATCH (:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)}
	MATCH p=(ors)-[:IN_LOCATION_GROUP]->(olg:LocationGroup)((:RailStation|LocationGroup)<-[:FROM]-(:Rate)-[:TO]->(:RailStation|LocationGroup)){1,5}(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)
	RETURN p AS path, 4 AS level
	
}
RETURN path, level limit 5