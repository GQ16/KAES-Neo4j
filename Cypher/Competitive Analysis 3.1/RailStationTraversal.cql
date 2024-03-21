MATCH op=(oc:CityState)<-[:IN_CITY]-(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
// WHERE oc.id = ''
MATCH dp=(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode)-[:IN_CITY]->(dc:CityState)
// WHERE dc.id = ''
MATCH p=
(ors)
    (
        (rss:RailStation)<-[:FROM]-(r:Rate)-[:TO]->(rse:RailStation)
        WHERE 1=1
        AND(
			EXISTS {
				MATCH (r)-[:FOR_STCC]->(:STCC)-[:REPRESENTS]->(lpg:LogisticsProductGroup)
				WHERE lpg.name = 'UREA'
			}
			OR EXISTS{
				MATCH (r)-[:FOR_STCCGROUP]->(:STCCGroup)<-[:IN_STCCGROUP]-(:STCC)-[:REPRESENTS]->(lpg:LogisticsProductGroup)
				WHERE lpg.name = 'UREA'
			}
		)
		AND CASE WHEN rss = ors THEN r.rule11AtOriginRequired ELSE NOT r.rule11AtOriginRequired END
		AND CASE WHEN rse = drs THEN r.rule11AtDestinationRequired ELSE NOT r.rule11AtDestinationRequired END
		AND r.rule11AtOriginAllowed
		AND r.rule11AtDestinationAllowed
    ){1,5}
(drs)
RETURN op,p,dp LIMIT 10