//Liquid Truck Section
    MATCH (o:Location)-[tt:TRUCK_TO]->(d:Location)
    ,(d)-[i1:IN]->(c2:City)-[i2:IN]->(s2:StateOrProvince)
    ,(d)-[i3:IN]->(s2)
    ,(o)-[i5:IN]-(c:City),(o)-[i6:IN]->(s:StateOrProvince)
    ,(c)-[i7:IN]->(s)
    ,(s)-[i4:IN]->(re:Region)-[hr:HAS_RATE]->(ra:Rate)
    ,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTTYPE]->(pt:ProductType)
	,(ra)-[fp:FOR_PRODUCTTYPE]->(pt)
    ,(o)-[:HAS_PRODUCTTYPE]->(pt)

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
    OriginZip:tt.originZip
    ,DestinationZip:tt.destZip
    ,Miles:tt.miles
    ,FreightRate:CASE ra.uom
    WHEN "$/ST" THEN ra.amount
    WHEN "$/MI" THEN round(ra.amount * tt.miles/ra.tons, 2)
    END
    ,color:"black"
    },d) AS vRel

UNION
//Dry Truck Section
    MATCH (re:Region)-[hr:HAS_RATE]->(ra:Rate)<-[i4:IS_ORIGIN_FOR]-(o:Location)
    ,(ra)-[fp:FOR_PRODUCTTYPE]->(pt:ProductType)
    ,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTTYPE]->(pt)
    ,(s2:StateOrProvince)<-[i9:IN]-(z:ZipCode)-[i8:IN]->(re)
    ,(o)-[tt:TRUCK_TO]->(d:Location)
    ,(d)-[i1:IN]->(c2:City)-[i2:IN]->(s2)
    ,(d)-[i3:IN]->(s2)
    ,(o)-[i5:IN]-(c:City),(o)-[i6:IN]->(s:StateOrProvince)
    ,(c)-[i7:IN]->(s)
    ,(o)-[:HAS_PRODUCTTYPE]->(pt)


    WHERE 1=1
    AND NOT o.type = 'JUNCTION'
    AND re.type = 'DESTINATION'
    AND c2.id = $neodash_city_id_2
    AND s2.id = $neodash_stateorprovince_id_2
    AND pt.id = $neodash_producttype_id
    AND tt.destZip = z.id
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
    OriginZip:tt.originZip
    ,DestinationZip:tt.destZip
    ,Miles:tt.miles
    ,FreightRate:CASE ra.uom
    WHEN "$/ST" THEN ra.amount
    WHEN "$/MI" THEN round(ra.amount * tt.miles/ra.tons, 2)
    END
    ,color:"black"
    },d) AS vRel

UNION
// Rail Section
    MATCH path = (o:Location)-[:RAIL_TO*..3]->(d:Location)
    ,(c:City)<-[:IN]-(o)-[:IN]->(s:StateOrProvince)
    ,(c2:City)<-[:IN]-(d)-[:IN]->(s2:StateOrProvince)
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

    WITH o,d
    ,reduce(dist = 0, r IN relationships(path) | dist + coalesce(r.miles,9999)) AS totalMiles //coalesce with 9999 miles is to expose rates with no mileage
    ,coalesce(min([x IN relationships(path)|x.expirationDate])[0],'unknown') AS earliestExpiration
    ,round(reduce(cost = 0, r IN relationships(path) | cost + coalesce(r.totalCost,r.pvtTotalCost,1)),2) AS totalCost
    ,[x IN relationships(path)|x.equipmentType] AS equipmentType
    ,[x IN nodes(path)|x.id] AS Route
    ,[x IN relationships(path)|x.carrier] AS Carriers
    ORDER BY totalCost ASC

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