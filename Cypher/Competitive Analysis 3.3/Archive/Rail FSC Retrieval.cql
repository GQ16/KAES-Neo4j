MATCH (oc:Country{id:$neodash_country_id_1})<-[:IN_COUNTRY]-(:RailStation|StationGroup)<-[:FROM]-(rr:RailRoute)-[:TO]->(:RailStation|StationGroup)-[:IN_COUNTRY]->(dc:Country{id:$neodash_country_id_2})
, (oc)<-[:FOR_ORIGIN_COUNTRY]-(f:RailFSC)-[:FOR_DESTINATION_COUNTRY]->(dc)
, (rr)<-[:HAS_ROUTE]-(ca:Carrier)-[:HAS_FSC]->(f)

WHERE (NOT (f)-[:FOR_PRODUCTGROUP]->() OR (f)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name}))

RETURN * limit 10