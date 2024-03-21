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
WITH value.l AS origin, value.dz AS destination, value.cr AS rate, value.tmt AS miles, value.tFSC AS fsc
RETURN origin, destination,
apoc.create.vRelationship(
    origin,
    'TRUCK_TO',
    {
        color: 'blue'
    },
    destination
) AS vRel