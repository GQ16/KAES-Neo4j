PROFILE
MATCH op=(oc:CityState)<-[:IN_CITY]-(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca:Carrier)
WHERE oc.city = 'ENID' AND oc.stateOrProvince = 'OK' AND oc.country = 'US'

MATCH dp=(dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode)-[:IN_CITY]->(dc:CityState)
WHERE dc.city = 'BOTKINS' AND dc.stateOrProvince = 'OH' AND dc.country = 'US'

WITH ors,drs, oca, dca
WHERE oca <> dca

MATCH (oca)-[:OWNS_STATION]->(:RailStation)-[:PART_OF_INTERCHANGE]->(i:Interchange)<-[:PART_OF_INTERCHANGE]-(:RailStation)<-[:OWNS_STATION]-(dca)
WHERE 1=1
AND COUNT{(i)<-[:PART_OF_INTERCHANGE]-(:RailStation)} > 10 //Filter based on the importance/connectedness of the interchange
AND EXISTS {MATCH (ors)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(:Rate)}
AND EXISTS {MATCH (:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs)}
WITH ors,drs,i ,point.distance(drs.coordinate,i.coordinate) + point.distance(ors.coordinate,i.coordinate) AS distance
ORDER BY distance LIMIT 10

WITH DISTINCT ors, drs, i

	MATCH p=
	(ors)
	(
		(rs1:RailStation)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(ra1:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(rs2:RailStation)
		WHERE (point.distance(drs.coordinate, rs2.coordinate)-point.distance(drs.coordinate, rs1.coordinate))/1000 < 10
		AND(
			EXISTS {
				MATCH (ra1)-[:FOR_STCC]->(:STCC)-[:REPRESENTS]->(lpg:LogisticsProductGroup)
				WHERE lpg.name = 'UREA'
			}
			OR EXISTS{
				MATCH (ra1)-[:FOR_STCCGROUP]->(:STCCGroup)<-[:IN_STCCGROUP]-(:STCC)-[:REPRESENTS]->(lpg:LogisticsProductGroup)
				WHERE lpg.name = 'UREA'
			}
		)
	){1,2}
	(irs1:RailStation)-[:PART_OF_INTERCHANGE]->(i)<-[:PART_OF_INTERCHANGE]-(irs2:RailStation)
	(
		(rs3:RailStation)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(ra2:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(rs4:RailStation)
		WHERE (point.distance(drs.coordinate, rs4.coordinate)-point.distance(drs.coordinate, rs3.coordinate))/1000 < 10
		AND(
			EXISTS {
				MATCH (ra2)-[:FOR_STCC]->(:STCC)-[:REPRESENTS]->(lpg:LogisticsProductGroup)
				WHERE lpg.name = 'UREA'
			}
			OR EXISTS{
				MATCH (ra2)-[:FOR_STCCGROUP]->(:STCCGroup)<-[:IN_STCCGROUP]-(:STCC)-[:REPRESENTS]->(lpg:LogisticsProductGroup)
				WHERE lpg.name = 'UREA'
			}
		)
	){1,2}
	(drs)

RETURN p LIMIT 10