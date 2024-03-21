PROFILE
CALL{
    MATCH (lpg:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
    OPTIONAL MATCH p=(:LogisticsProductGroup)-[r:HAS_CHILD]->(lpg)
    return CASE WHEN r IS NULL THEN lpg ELSE nodes(p) END AS products
}
WITH products
CALL{
    with products

    MATCH (dca:Carrier)-[:OWNS_STATION]->(drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    MATCH (p:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-()<-[:HAS_OCCUPANT]-(l:Location)//-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    MATCH (l)-[:IN_ZIPCODE]->()<-[:IN_ZIPCODE]-(ors:RailStation)//<-[:OWNS_STATION]-(oca:Carrier)
    MATCH (dca)-[:USES_INTERCHANGE]->(i:Interchange)
    where p IN products
    // AND i.stationCount > 7
    
    // //We leave this match after the with in order to guide the cypher query planner
    // WITH DISTINCT i, oca, ors, drs
    // MATCH (i)<-[:USES_INTERCHANGE]-(oca)

    WITH collect(DISTINCT i) AS validCarrierInterchanges
    , avg(ors.coordinate.latitude) AS avgOriginLat, avg(ors.coordinate.longitude) AS avgOriginLong
    , avg(drs.coordinate.latitude) AS avgDestLat, avg(drs.coordinate.longitude) AS avgDestLong

    //Logic for defining corners of BBox for NorthWestern Hemisphere
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
WITH products, validInterchanges
CALL {
    WITH products
	
    MATCH (p:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
    // MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    MATCH (drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    WHERE p IN products
    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)

    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH l,dz,origin, destination

    CALL{
        WITH origin, destination
        MATCH p = 
        (origin)<-[f:UREA_FROM]-(rr:RailRoute)-[:UREA_TO]->(destination)
        RETURN p

        UNION

        WITH origin, destination
        MATCH p = 
        (origin)<-[f:UREA_FROM]-(rr:RailRoute)-[:UREA_TO]->()<-[f2:UREA_FROM]-(rr2:RailRoute)-[:UREA_TO]->(destination)
        RETURN p
    }
    
    WITH DISTINCT l,dz,p, [x IN nodes(p) WHERE x:RailRoute] AS routes
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())

    WITH DISTINCT l AS origin, dz AS destination
    RETURN origin, destination, apoc.create.vRelationship(origin,'RAIL_TO',{color:'red'},destination) AS vRel

    UNION

    WITH products, validInterchanges

    MATCH (p:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)<-[:IN_ZIPCODE]-(ors:RailStation)
    MATCH (drs:RailStation)-[:IN_ZIPCODE]->(dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
    // MATCH (l)-[:HAS_OUTBOUND]->(m:Mode{id:'RAIL'})
    WHERE p IN products

    OPTIONAL MATCH (ors)-[:IN_STATION_GROUP]->(olg:StationGroup)
    OPTIONAL MATCH (drs)-[:IN_STATION_GROUP]->(dlg:StationGroup)

    WITH l,dz,collect(DISTINCT ors) + collect(DISTINCT olg) AS origins
        , collect(DISTINCT drs) + collect(DISTINCT dlg) AS destinations
        , validInterchanges

    UNWIND origins AS origin
    UNWIND destinations As destination
    WITH l,dz,origin, destination, validInterchanges

    MATCH p =  (origin)<-[f:UREA_FROM]-(:RailRoute)-[:UREA_TO]->(s1:StationGroup|RailStation)-[:HAS_STATION_IN|AT_INTERCHANGE]->(i)<-[:HAS_STATION_IN|AT_INTERCHANGE]-(s2:StationGroup|RailStation)<-[f2:UREA_FROM]-(:RailRoute)-[:UREA_TO]->(destination) 
    WHERE i IN validInterchanges
        // AND point.distance(destination.coordinate, s1.coordinate) < point.distance(destination.coordinate, origin.coordinate)

    WITH DISTINCT l,dz,p, [x IN nodes(p) WHERE x:RailRoute] AS routes
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())

    WITH DISTINCT l AS origin, dz AS destination
    RETURN origin, destination, apoc.create.vRelationship(origin,'RULE_11_TO',{color:'orange'},destination) AS vRel
}
return origin, destination, vRel