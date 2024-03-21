//Liquid Truck Section
    MATCH (d:Location)-[:IN_CITY]->(c2:City)-[:IN_STATE]->(s2:StateOrProvince)
    ,(d)-[:IN_STATE]->(s2)
    ,(s2)<-[:IN_STATE]-(dz:ZipCode)-[:IN_CITY]->(c2)
    ,(oz:ZipCode)-[tt:TRUCK_TO]->(dz)
    ,(s:StateOrProvince)<-[:IN_STATE]-(oz)-[:IN_CITY]->(c:City)
    ,(o:Location)-[:IN_CITY]->(c)-[:IN_STATE]->(s)
    ,(o)-[:HAS_PRODUCTTYPE]->(pt:ProductType)
    ,(o)-[:IN_STATE]->(s)
    ,(s)-[:IN_REGION]->(re:Region)-[hr:HAS_RATE]->(ra:Rate)
    ,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTTYPE]->(pt)
    ,(ra)-[:FOR_PRODUCTTYPE]->(pt)

    WHERE 1=1
    AND NOT o.type = 'JUNCTION'
    AND re.type = 'ORIGIN'
    AND c2.id = $neodash_city_id_2
    AND s2.id = $neodash_stateorprovince_id_2
    AND pt.id = $neodash_producttype_id
    AND tt.miles > ra.mileLower
    AND tt.miles <= ra.mileUpper
    AND se.startDate <= date($neodash_ship_date)
    AND se.endDate >= date($neodash_ship_date)
    AND c.id =
        CASE WHEN isEmpty($neodash_city_id_1) THEN c.id
        ELSE $neodash_city_id_1 END
    AND s.id =
        CASE WHEN isEmpty($neodash_stateorprovince_id_1) THEN s.id
        ELSE $neodash_stateorprovince_id_1 END
        
    RETURN DISTINCT o, d, apoc.create.vRelationship(o,'TRUCK_TO',{
    Miles:tt.miles
    ,Freight:CASE ra.uom
    WHEN "$/ST" THEN ra.amount
    WHEN "$/MI" THEN round(ra.amount * tt.miles/ra.tons, 2)
    END
    ,color:"black"
    },d) AS vRel

UNION
//Dry Truck Section
    MATCH (d:Location)-[:IN_CITY]->(c2:City)-[:IN_STATE]->(s2:StateOrProvince)
    ,(d)-[:IN_STATE]->(s2)
    ,(dz:ZipCode)-[:IN_CITY]->(c2)
    ,(dz)-[:IN_STATE]->(s2)
    ,(oz:ZipCode)-[tt1:TRUCK_TO]->(dz)-[:IN_REGION]->(re:Region)
    ,(re)-[hr:HAS_RATE]->(ra:Rate)<-[:IS_ORIGIN_FOR]-(o:Location)
    ,(o)-[:HAS_PRODUCTTYPE]->(pt:ProductType)
    ,(ra)-[:FOR_PRODUCTTYPE]->(pt)
    ,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTTYPE]->(pt)
    ,(c:City)<-[:IN_CITY]-(o)-[:IN_STATE]->(s:StateOrProvince)
    ,(c)-[:IN_STATE]->(s)
    ,(c)<-[:IN_CITY]-(oz)
    ,(oz)-[:IN_STATE]->(s)

    WHERE 1=1
    AND NOT o.type = 'JUNCTION'
    AND re.type = 'DESTINATION'
    AND c2.id = $neodash_city_id_2
    AND s2.id = $neodash_stateorprovince_id_2
    AND pt.id = $neodash_producttype_id
    AND tt1.miles > ra.mileLower
    AND tt1.miles <= ra.mileUpper
    AND se.startDate <= date($neodash_ship_date)
    AND se.endDate >= date($neodash_ship_date)
    AND c.id =
        CASE WHEN isEmpty($neodash_city_id_1) THEN c.id
        ELSE $neodash_city_id_1 END
    AND s.id =
        CASE WHEN isEmpty($neodash_stateorprovince_id_1) THEN s.id
        ELSE $neodash_stateorprovince_id_1 END

    RETURN DISTINCT o, d, apoc.create.vRelationship(o,'TRUCK_TO',{
    ,Miles:tt1.miles
    ,Freight:CASE ra.uom
    WHEN "$/ST" THEN ra.amount
    WHEN "$/MI" THEN round(ra.amount * tt1.miles/ra.tons, 2)
    END
    ,color:"black"
    },d) AS vRel

UNION
// Rail Section
    MATCH path = (o:Location)-[:RAIL_TO*..3]->(d:Location)
    ,(c:City)<-[:IN_CITY]-(o)-[:IN_STATE]->(s:StateOrProvince)
    ,(c2:City)<-[:IN_CITY]-(d)-[:IN_STATE]->(s2:StateOrProvince)
    ,(o)-[:HAS_PRODUCTTYPE]->(pt:ProductType)

    WHERE 1=1
    AND pt.id = $neodash_producttype_id
    AND all(
    x IN relationships(path)
    WHERE 1=1
    AND x.productType = $neodash_producttype_id
    AND NOT x.miles = 0
    AND (x.expirationDate >= date($neodash_ship_date) OR x.expirationDate IS NULL)
    )
    AND NOT o.type = 'JUNCTION'
    AND c2.id = $neodash_city_id_2
    AND s2.id = $neodash_stateorprovince_id_2
    AND c.id =
        CASE WHEN isEmpty($neodash_city_id_1) THEN c.id
        ELSE $neodash_city_id_1 END
    AND s.id =
        CASE WHEN isEmpty($neodash_stateorprovince_id_1) THEN s.id
        ELSE $neodash_stateorprovince_id_1 END
    AND CASE WHEN isEmpty($neodash_rail_to_equipmenttype) THEN single(et IN ['SINGLE','85 UT', '100 UT'] WHERE all(x IN relationships(path) WHERE x.equipmentType = et))
        ELSE all(x IN relationships(path) WHERE x.equipmentType = $neodash_rail_to_equipmenttype) END

    WITH o,d,length(path) AS len
    ,reduce(dist = 0, r IN relationships(path) | dist + coalesce(r.miles,9999)) AS totalMiles //coalesce with 9999 miles is to expose rates with no mileage
    ,coalesce(min([x IN relationships(path)|x.expirationDate])[0],'unknown') AS earliestExpiration
    ,round(reduce(cost = 0, r IN relationships(path) | cost + coalesce(r.totalCost,r.pvtTotalCost,1)),2) AS totalCost
    ,[x IN relationships(path)|x.equipmentType] AS equipmentType
    ,[x IN nodes(path)|x.id] AS Route
    ,[x IN relationships(path)|x.carrier] AS Carriers
    ORDER BY len, totalCost

    WITH o,d, collect(totalMiles)[0] AS Miles, collect(totalCost)[0] AS Freight
    , collect(earliestExpiration)[0] AS RateExpiration
    , equipmentType[0] AS EquipmentType
    , collect(Carriers)[0] AS Carriers, collect(Route)[0] AS Route

    RETURN o,d, apoc.create.vRelationship(o,'RAIL_TO',{
    Freight:Freight
    ,Carriers:Carriers
    ,Route:Route
    ,Miles:Miles
    ,RateExpiration:RateExpiration
    ,EquipmentType:EquipmentType
    ,Carriers:Carriers
    ,Route:Route
    ,color:"white"
    },d) AS vRel