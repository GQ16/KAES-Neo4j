profile
CALL{
    MATCH op=(oz:ZipCode{id:'73701'})<-[:IN_ZIPCODE]-(ors:RailStation)-[:IN_STATION_GROUP]->(olg:LocationGroup)<-[:OWNS_STATION_GROUP]-(oca:Carrier)
    MATCH dp = (dca:Carrier)-[:OWNS_STATION_GROUP]->(dlg:LocationGroup)<-[:IN_STATION_GROUP]-(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:'45306'})
    WITH oca, dca
    WITH collect(oca) as originCarriers, collect(dca) as destinationCarriers
    WITH originCarriers + destinationCarriers AS carriers
    MATCH (s:STCC{id:2818170})
    OPTIONAL MATCH (s)-[r:IN_STCCGROUP]->(sg:STCCGroup)<-[:HAS_STCCGROUP]-(ca:Carrier) 
    WHERE ca IN carriers
    WITH toString(s.id) as stcc, apoc.coll.toSet(collect(sg.id)) as groups
    RETURN stcc + groups AS validRateStccs
}
CALL{
    MATCH op=(oz:ZipCode{id:'73701'})<-[:IN_ZIPCODE]-(ors:RailStation)-[:IN_STATION_GROUP]->(olg:LocationGroup)<-[:OWNS_STATION_GROUP]-(oca:Carrier)
    MATCH dp = (dca:Carrier)-[:OWNS_STATION_GROUP]->(dlg:LocationGroup)<-[:IN_STATION_GROUP]-(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:'45306'})
    
    WITH ors, drs, oca, dca

    MATCH (i:Interchange)
    WHERE EXISTS{ (oca)-[:OWNS_STATION]->()-[:AT_INTERCHANGE]->(i)<-[:AT_INTERCHANGE]-()<-[:OWNS_STATION]-(dca)}
    AND COUNT{(i)<-[:AT_INTERCHANGE]-()} > 5

    WITH collect(DISTINCT i) AS validCarrierInterchanges
    , avg(ors.coordinate.latitude) AS avgOriginLat, avg(ors.coordinate.longitude) AS avgOriginLong
    , avg(drs.coordinate.latitude) AS avgDestLat, avg(drs.coordinate.longitude) AS avgDestLong

    //Logic for defining corners of BBox
    WITH validCarrierInterchanges
    , CASE 
        WHEN avgOriginLat > avgDestLat
        THEN {upper:avgOriginLat, lower: avgDestLat}
        ELSE {lower:avgOriginLat, upper: avgDestLat}
    END AS latBounds
    , CASE 
        WHEN avgOriginLong < avgDestLong 
        THEN {left:avgOriginLong, right: avgDestLong}
        ELSE {right:avgOriginLong, left: avgDestLong}
    END AS longBounds

    WITH validCarrierInterchanges
    , point({latitude:latBounds.lower-5, longitude: longBounds.left-5}) AS lowerLeft
    , point({latitude:latBounds.upper+5, longitude: longBounds.right+5}) AS upperRight

    WITH [x IN validCarrierInterchanges WHERE point.withinBBox(x.coordinate,lowerLeft,upperRight)] AS validInterchanges
    RETURN validInterchanges
}
WITH validInterchanges, validRateStccs
MATCH op=(oz:ZipCode{id:'73701'})<-[:IN_ZIPCODE]-(ors:RailStation)-[:IN_STATION_GROUP]->(olg:LocationGroup)<-[:OWNS_STATION_GROUP]-(oca:Carrier)
MATCH dp = (dca:Carrier)-[:OWNS_STATION_GROUP]->(dlg:LocationGroup)<-[:IN_STATION_GROUP]-(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:'45306'})

MATCH p = 
(olg)
(
    (s)<-[f:FROM]-(:Rate)-[:TO]->(e) 
    WHERE f.stcc IN validRateStccs
	AND point.distance(drs.coordinate, e.coordinate) < point.distance(drs.coordinate, s.coordinate)
){1,2}
(:LocationGroup)-[:HAS_STATION_IN]->(i)<-[:HAS_STATION_IN]-(:LocationGroup)
(
    (s2)<-[f2:FROM]-(:Rate)-[:TO]->(e2) 
    WHERE f2.stcc IN validRateStccs
	AND point.distance(drs.coordinate, e2.coordinate) < point.distance(drs.coordinate, s2.coordinate)
){1,2}
(dlg)
WHERE i IN validInterchanges
//Add conversion from per car to per ton rates into reduce function
RETURN op, dp, p//, size(nodes(p)) AS moves,
// 	reduce(
// 		cost = 0 ,
// 		r IN [x IN nodes(p) WHERE x:Rate] |
// 		cost + CASE WHEN toLower(r.uom) = 'car' THEN r.rate/97 WHEN toLower(r.uom) = 'ton' THEN r.rate END
// 	) AS price
// ORDER BY moves, price 
LIMIT 10