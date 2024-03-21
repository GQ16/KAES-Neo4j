CALL{
    MATCH (oz:ZipCode{id:'73701', country:'US'})<-[:IN_ZIPCODE]-(ors:RailStation)<-[:OWNS_STATION]-(oca)
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:LocationGroup)<-[:OWNS_STATION_GROUP]-(oca2)
    MATCH (dca)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:'68901', country:'US'})
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:LocationGroup)<-[:OWNS_STATION_GROUP]-(dca2)

    WITH oca, oca2, dca, dca2 
    WITH collect(DISTINCT oca) + collect(DISTINCT oca2) + collect(DISTINCT dca) + collect(DISTINCT dca2) AS carriers 
    WITH apoc.coll.toSet(carriers) AS carrierSet

    UNWIND carrierSet AS ca
    MATCH (s:STCC)-[:REPRESENTS]->(:LogisticsProductGroup{name:'UREA'})
    OPTIONAL MATCH (s)-[r:IN_STCCGROUP]->(sg:STCCGroup)<-[:HAS_STCCGROUP]-(ca) 
    //This collection step is for the case when a Product group has multiple stccs
    WITH collect(DISTINCT toString(s.id)) as stcc, collect(DISTINCT sg.id) as groups
    RETURN stcc + groups AS validRateStccs
}

WITH validRateStccs
MATCH op=(oz:ZipCode{id:'73701', country:'US'})<-[:IN_ZIPCODE]-(ors:RailStation)
OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:LocationGroup)
MATCH dp =(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:'68901', country:'US'})
OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:LocationGroup)

WITH collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
    , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
    , validRateStccs

UNWIND origins AS origin
UNWIND destinations As destination
WITH origin, destination, validRateStccs

MATCH p = 
(origin)
(
    (s)<-[f:FROM]-(:Route)-[:TO]->(e) 
    WHERE f.stcc IN validRateStccs
	AND point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
){1,3}
(destination)

WITH DISTINCT p, [x IN nodes(p) WHERE x:Route] AS routes
//Filtering out paths that use a route that doesn't have a current rate
WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())

UNWIND routes AS route
MATCH (route)-[:HAS_CURRENT_RATE]->(ra:Rate)

RETURN *