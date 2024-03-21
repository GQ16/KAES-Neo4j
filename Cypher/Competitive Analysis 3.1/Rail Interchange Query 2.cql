PROFILE
MATCH op=(oc:CityState)<-[:IN_CITY]-(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca:Carrier)
WHERE oc.city = 'ENID' AND oc.stateOrProvince = 'OK' AND oc.country = 'US'

MATCH dp=(dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode)-[:IN_CITY]->(dc:CityState)
WHERE dc.city = 'JEFFERSONVILLE' AND dc.stateOrProvince = 'IN' AND dc.country = 'US'

WITH  ors,drs, oca, dca
WHERE oca <> dca

MATCH (oca)-[:OWNS_STATION]->(:RailStation)-[:PART_OF_INTERCHANGE]->(i:Interchange)<-[:PART_OF_INTERCHANGE]-(:RailStation)<-[:OWNS_STATION]-(dca)
WHERE 1=1
AND COUNT{(i)<-[:PART_OF_INTERCHANGE]-(:RailStation)} > 5 //Filter based on the importance/connectedness of the interchange
WITH DISTINCT ors, drs, i
, CASE 
	WHEN ors.coordinate.latitude > drs.coordinate.latitude
	THEN {upper:ors.coordinate.latitude, lower: drs.coordinate.latitude}
	ELSE {lower:ors.coordinate.latitude, upper: drs.coordinate.latitude}
END AS latBounds
, CASE 
	WHEN ors.coordinate.longitude < drs.coordinate.longitude 
	THEN {left:ors.coordinate.longitude, right: drs.coordinate.longitude}
	ELSE {right:ors.coordinate.longitude, left: drs.coordinate.longitude}
END AS longBounds
WITH ors, drs, i
, point({latitude:latBounds.lower-5, longitude: longBounds.left-5}) AS lowerLeft
, point({latitude:latBounds.upper+5, longitude: longBounds.right+5}) AS upperRight

WITH ors, drs, i
WHERE point.withinBBox(i.coordinate, lowerLeft, upperRight)

MATCH p=(ors)-[:IN_LOCATION_GROUP]->(:LocationGroup)<-[:FROM]-(ra1:Rate)-[:TO]->
	(:LocationGroup)<-[:IN_LOCATION_GROUP]-(rs2:RailStation)-[:PART_OF_INTERCHANGE]->
	(i)<-[:PART_OF_INTERCHANGE]-(rs3:RailStation)-[:IN_LOCATION_GROUP]->(:LocationGroup)
	<-[:FROM]-(ra2:Rate)-[:TO]->(:LocationGroup)<-[:IN_LOCATION_GROUP]-(drs),
	(ra1)-[:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name:'UREA'})<-[:FOR_PRODUCTGROUP]-(ra2),
	(ra1)-[:SHIP_WINDOW_START]->(swsd1:Date),
	(ra2)-[:SHIP_WINDOW_START]->(swsd2:Date),
	(ra1)-[:SHIP_WINDOW_END]->(swed1:Date),
	(ra2)-[:SHIP_WINDOW_END]->(swed2:Date)
WHERE 1=1
	AND swsd1.id <= date() >= swsd2.id
	AND swed1.id >= date() <= swed2.id
	AND (point.distance(drs.coordinate, rs2.coordinate)-point.distance(drs.coordinate, ors.coordinate))/1000 < 0
RETURN DISTINCT p, reduce(price = 0, r IN [x IN nodes(p) WHERE x:Rate | x.rate] | price + r) AS totalPrice 