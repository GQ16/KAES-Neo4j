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
CALL {
    WITH l,dz,origin, destination

    MATCH p = 
    (origin)
    (
        (s)<-[:`UREA_FROM`]-(:RailRoute)-[:`UREA_TO`]->(e) 
        WHERE point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){1,2}
    (destination)
    
    WITH DISTINCT l,dz,p, [x IN nodes(p) WHERE x:RailRoute] AS routes
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())

    WITH DISTINCT l AS startLoc, dz AS endZip
    RETURN startLoc, endZip, apoc.create.vRelationship(startLoc,'RAIL_TO',{color:'red'},endZip) AS vRel

    UNION

    WITH l,dz,origin, destination, validInterchanges

    MATCH p = 
    (origin)
    (
        (s)<-[:`UREA_FROM`]-(:RailRoute)-[:`UREA_TO`]->(e) 
        WHERE point.distance(destination.coordinate, e.coordinate) < point.distance(destination.coordinate, s.coordinate)
    ){1,1}
    (:StationGroup|RailStation)-[:HAS_STATION_IN|AT_INTERCHANGE]->(i)<-[:HAS_STATION_IN|AT_INTERCHANGE]-(:StationGroup|RailStation)
    (
        (s2)<-[:`UREA_FROM`]-(:RailRoute)-[:`UREA_TO`]->(e2) 
        WHERE point.distance(destination.coordinate, e2.coordinate) < point.distance(destination.coordinate, s2.coordinate)
    ){1,1}
    (destination)
    WHERE i IN validInterchanges

    WITH DISTINCT l,dz,p, [x IN nodes(p) WHERE x:RailRoute] AS routes
    //Filtering out paths that use a route that doesn't have a current rate
    WHERE all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())

    WITH DISTINCT l AS startLoc, dz AS endZip
    RETURN startLoc, endZip, apoc.create.vRelationship(startLoc,'RULE_11_TO',{color:'orange'},endZip) AS vRel
}
RETURN startLoc, endZip, vRel

UNION

MATCH (selProd:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
MATCH (selDestZip:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
MATCH (selDestCity:CityState{id:$neodash_citystate_id_2, country:$neodash_country_id_2})

CALL apoc.case(
    [
        selProd.name IN ['UREA', 'MAP', 'DAP', 'PHOSPHATE', 'DRY', 'POTASH', 'SUPERU'],
        "
            MATCH (o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)
            , (l)-[:IN_CITY]->(ocs:CityState)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dlg:LocationGroup)<-[:IN_LOCATION_GROUP]-(dz)
            , (tr)-[:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name:'DRY'})
            , (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
            , (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
            , (oz)-[:IN_COUNTRY]->(c:Country)
            , (tFSC:TruckFSC)-[:FOR_PRODUCTGROUP]->(lpg)

            WHERE 1=1
            AND cr.distanceLower < tmt.distance <= cr.distanceUpper
            AND date('2024-02-29') <= tFSC.expirationDate
            AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
            AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
            AND (
            (prod)<-[:HAS_PRODUCTGROUP]-(o)
            OR (lpg)<-[:HAS_PRODUCTGROUP]-(o)
            )
            RETURN l,cr,tmt,tFSC,dz
        ",
        selProd.name IN ['LIQUID', 'UAN', 'ATS', '10-34-0', 'AMMONIA', 'NITAMIN'],
        "
            MATCH (o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)-[:IN_ZIPCODE]->(oz:ZipCode)
            , (l)-[:IN_STATE]->(os:StateOrProvince)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dc:Country)
            , (dc)<-[:IN_COUNTRY]-(dz)
            , (tr)-[:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup{name:'LIQUID'})
            , (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
            , (oz)-[tmt:TRUCK_DISTANCE_TO]->(dz)
            , (oz)-[:IN_COUNTRY]->(c:Country)
            , (tFSC)-[:FOR_PRODUCTGROUP]->(lpg)

            WHERE 1=1
            AND cr.distanceLower < tmt.distance <= cr.distanceUpper
            AND date('2024-02-29') <= tFSC.expirationDate
            AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
            AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
            AND (
            (prod)<-[:HAS_PRODUCTGROUP]-(o)
            OR (lpg)<-[:HAS_PRODUCTGROUP]-(o)
            )

            RETURN l,cr,tmt,tFSC,dz
        ",
        selProd.name = 'METHANOL',
        "
            MATCH (prod)<-[:HAS_PRODUCTGROUP]-(o:Occupant)<-[:HAS_OCCUPANT]-(l:Location)
            , (l)-[:IN_CITY]->(ocs:CityState)
            , (ocs)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dcs)
            , (dcs)<-[:IN_CITY]-(dz)
            , (tr)-[:FOR_PRODUCTGROUP]->(prod)
            , (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
            , (l)-[:IN_COUNTRY]->(c:Country)
            , (tFSC:TruckFSC)-[:FOR_PRODUCTGROUP]->(prod)

            WHERE (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
            AND date('2024-02-29') <= tFSC.expirationDate
            and (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)
            
            RETURN l,cr,null AS tmt,tFSC,dz
        "
    ],
    "RETURN null AS l",
    {
        prod:selProd,
        dz:selDestZip,
        dcs:selDestCity
    }
)
YIELD value
WITH value.l AS startLoc, value.dz AS endZip, value.cr AS rate, value.tmt AS miles, value.tFSC AS fsc
RETURN startLoc, endZip,
apoc.create.vRelationship(
    startLoc,
    'TRUCK_TO',
    {
        color: 'blue'        
    },
    endZip
) AS vRel