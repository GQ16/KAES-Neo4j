MATCH (l:Location {id:location_id})-[:IN_SPLC]->(:SPLC)<-[:IN_SPLC]-()<-[:TO]-(rr:RailRoute)
RETURN DISTINCT rr.destination_carrier AS servingCarriers