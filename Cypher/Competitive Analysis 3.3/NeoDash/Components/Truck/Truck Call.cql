//doesn't really work becuase the calls are dependent on one another
profile
MATCH (prod:LogisticsProductGroup{name:$neodash_logisticsproductgroup_name})
MATCH (dz:ZipCode{id:$neodash_zipcode_id_2, country:$neodash_country_id_2})
MATCH (dcs:CityState{id:$neodash_citystate_id_2, country:$neodash_country_id_2})

WITH *, CASE 
    WHEN prod.name IN ['UREA', 'MAP', 'DAP', 'PHOSPHATE', 'DRY', 'POTASH', 'SUPERU']
    THEN 1
    WHEN prod.name IN ['LIQUID', 'UAN', 'ATS', '10-34-0', 'AMMONIA', 'NITAMIN']
    THEN 2
    WHEN prod.name = 'METHANOL'
    THEN 3
END AS queryType


CALL {
    WITH *    
    WITH *
    WHERE queryType = 1
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
    
    RETURN l as l1,cr as cr1,tmt as tmt1,tFSC as tFSC1,dz as dz1
}
CALL {
    WITH *    
    WITH *
    WHERE queryType = 2
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

            RETURN l as l2,cr as cr2,tmt as tmt2,tFSC as tFSC2,dz as dz2
}
CALL {
    WITH *    
    WITH *
    WHERE queryType = 3
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
        
        RETURN l as l3,cr as cr3,null AS tmt3,tFSC as tFSC3,dz as dz3
}

RETURN coalesce(l1,l2,l3) AS l,
coalesce(cr1,cr2,cr3) AS cr,
coalesce(tmt1,tmt2,tmt3) AS tmt,
coalesce(tFSC1,tFSC2,tFSC3) as tFSC,
coalesce(dz1,dz2,dz3) AS dzFinal