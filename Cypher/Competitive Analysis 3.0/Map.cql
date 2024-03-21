//Liquid Truck Section
    MATCH (pt:ProductType{id:$neodash_producttype_id})
    OPTIONAL MATCH path=(pf:ProductForm)-[:HAS_CHILD]->(pt)
    WITH CASE WHEN pf IS NULL THEN pt ELSE nodes(path) END AS products

    MATCH (d:Location)-[:IN_CITY]->(c2:City)-[:IN_STATE]->(s2:StateOrProvince)
    ,(d)-[:IN_STATE]->(s2)
    ,(s2)<-[:IN_STATE]-(dz:ZipCode)-[:IN_CITY]->(c2)
    ,(oz:ZipCode)-[tt:TRUCK_TO]->(dz)
    ,(s:StateOrProvince)<-[:IN_STATE]-(oz)-[:IN_CITY]->(c:City)
    ,(o:Location)-[:IN_CITY]->(c)-[:IN_STATE]->(s)
    ,(o)-[:HAS_OCCUPANT]->(oc:Occupant)-[:HAS_PRODUCTGROUP]->(pg:ProductGroup)
    ,(o)-[:IN_STATE]->(s)
    ,(s)-[:IN_REGION]->(re:Region)-[hr:HAS_RATE]->(ra:Rate)
    ,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTGROUP]->(pg)
	,(se)-[:HAS_START]->(d1:Date)
	,(se)-[:HAS_END]->(d2:Date)
    ,(ra)-[:FOR_PRODUCTGROUP]->(pg)

    WHERE 1=1
    AND CASE 
        WHEN isEmpty($neodash_city_id_2) 
        AND isEmpty($neodash_location_name_2) 
        AND isEmpty($neodash_party_name) 
        AND isEmpty($neodash_market_id)
        THEN FALSE
        ELSE TRUE
    END //This statement makes it so that at least one destination filter must be populated
    AND re.type = 'ORIGIN'
    AND c2.id = 
        CASE 
            WHEN isEmpty($neodash_city_id_2) 
            THEN c2.id
            ELSE $neodash_city_id_2
        END
    AND d.name = 
        CASE 
            WHEN isEmpty($neodash_location_name_2) 
            THEN d.name 
            ELSE $neodash_location_name_2 
        END
    AND pg IN products
    AND tt.miles > ra.mileLower
    AND tt.miles <= ra.mileUpper
    AND d1.id <= date($neodash_ship_date)
    AND d2.id >= date($neodash_ship_date)
    AND c.id =
        CASE 
            WHEN isEmpty($neodash_city_id_1) THEN c.id
            ELSE $neodash_city_id_1 
        END
    AND (isEmpty($neodash_party_name) OR 
        EXISTS {
            MATCH (d)-[:HAS_OCCUPANT]->(:Occupant)<-[:IS_OCCUPANT]-(p:Party)
            WHERE p.name = $neodash_party_name
        }
    )
    AND (isEmpty($neodash_market_id) OR 
        EXISTS {
            MATCH (d)-[:HAS_OCCUPANT]->(:Occupant)-[:PART_OF_MARKET]->(m:Market)
            WHERE m.id = $neodash_market_id
        }
    )        

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
    MATCH (pt:ProductType{id:$neodash_producttype_id})
    OPTIONAL MATCH path=(pf:ProductForm)-[:HAS_CHILD]->(pt)
    WITH CASE WHEN pf IS NULL THEN pt ELSE nodes(path) END AS products

    MATCH (d:Location)-[:IN_CITY]->(c2:City)-[:IN_STATE]->(s2:StateOrProvince)<-[:IN_STATE]-(d)
    ,(s2)<-[:IN_STATE]-(dz:ZipCode)-[:IN_CITY]->(c2)
    ,(oz:ZipCode)-[tt:TRUCK_TO]->(dz)-[:IN_REGION]->(re:Region)
    ,(re)-[hr:HAS_RATE]->(ra:Rate)<-[:IS_ORIGIN_FOR]-(c:City)
    ,(c)<-[:IN_CITY]-(o:Location)-[:IN_STATE]->(s:StateOrProvince)<-[:IN_STATE]-(c)
    ,(c)<-[:IN_CITY]-(oz)-[:IN_STATE]->(s)
    ,(o)-[:HAS_OCCUPANT]->(oc:Occupant)-[:HAS_PRODUCTGROUP]->(pg:ProductGroup)
    ,(ra)-[:FOR_PRODUCTGROUP]->(pg)
    ,(ra)-[:FOR_SEASON]->(se:Season)-[:FOR_PRODUCTGROUP]->(pg)
    ,(se)-[:HAS_START]->(d1:Date)
	,(se)-[:HAS_END]->(d2:Date)


    WHERE 1=1
    AND CASE 
        WHEN isEmpty($neodash_city_id_2) 
        AND isEmpty($neodash_location_name_2) 
        AND isEmpty($neodash_party_name) 
        AND isEmpty($neodash_market_id)
        THEN FALSE
        ELSE TRUE
    END //This statement makes it so that at least one destination filter must be populated
    AND pg IN products
    AND re.type = 'DESTINATION'
    AND c2.id = 
        CASE 
            WHEN isEmpty($neodash_city_id_2)
            THEN c2.id
            ELSE $neodash_city_id_2
        END
    AND d.name = 
        CASE 
            WHEN isEmpty($neodash_location_name_2) 
            THEN d.name 
            ELSE $neodash_location_name_2 
        END
    AND tt.miles > ra.mileLower
    AND tt.miles <= ra.mileUpper
    AND d1.id <= date($neodash_ship_date)
    AND d2.id >= date($neodash_ship_date)
    AND c.id =
        CASE 
            WHEN isEmpty($neodash_city_id_1) THEN c.id
            ELSE $neodash_city_id_1 
        END
    AND (isEmpty($neodash_party_name) OR 
        EXISTS {
            MATCH (d)-[:HAS_OCCUPANT]->(:Occupant)<-[:IS_OCCUPANT]-(p:Party)
            WHERE p.name = $neodash_party_name
        }
    )
    AND (isEmpty($neodash_market_id) OR 
        EXISTS {
            MATCH (d)-[:HAS_OCCUPANT]->(:Occupant)-[:PART_OF_MARKET]->(m:Market)
            WHERE m.id = $neodash_market_id
        }
    )   
    
    RETURN DISTINCT o, d, apoc.create.vRelationship(o,'TRUCK_TO',{
        ,Miles:tt.miles
        ,Freight:CASE ra.uom
        WHEN "$/ST" THEN ra.amount
        WHEN "$/MI" THEN round(ra.amount * tt.miles/ra.tons, 2)
        END
        ,color:"black"
    },d) AS vRel

UNION
// Rail Section
    MATCH (pt:ProductType{id:$neodash_producttype_id})
    OPTIONAL MATCH path=(pf:ProductForm)-[:HAS_CHILD]->(pt)
    WITH CASE WHEN pf IS NULL THEN pt ELSE nodes(path) END AS products
    
    MATCH path = (c:City)-[:RAIL_TO*..3]->(c2:City)
    ,(o:Location)-[:IN_CITY]->(c)-[:IN_STATE]->(s:StateOrProvince)<-[:IN_STATE]-(o)
    ,(d:Location)-[:IN_CITY]->(c2)-[:IN_STATE]->(s2:StateOrProvince)<-[:IN_STATE]-(d)
    ,(o)-[:HAS_OCCUPANT]->(oc:Occupant)-[:HAS_PRODUCTGROUP]->(pg:ProductGroup)

    WHERE 1=1
    AND CASE 
        WHEN isEmpty($neodash_city_id_2) 
        AND isEmpty($neodash_location_name_2) 
        AND isEmpty($neodash_party_name) 
        AND isEmpty($neodash_market_id)
        THEN FALSE
        ELSE TRUE
    END //This statement makes it so that at least one destination filter must be populated
    AND c2.id = 
        CASE 
            WHEN isEmpty($neodash_city_id_2)
            THEN c2.id
            ELSE $neodash_city_id_2
        END
    AND d.name = 
        CASE 
            WHEN isEmpty($neodash_location_name_2) 
            THEN d.name 
            ELSE $neodash_location_name_2 
        END
    AND pg in products
    AND c.id =
        CASE 
            WHEN isEmpty($neodash_city_id_1) 
            THEN c.id
            ELSE $neodash_city_id_1 
        END
    AND CASE 
            WHEN isEmpty($neodash_rail_to_equipmenttype) 
            THEN single(et IN ['SINGLE','85 UT', '100 UT'] WHERE all(x IN relationships(path) WHERE x.equipmentType = et))
            ELSE all(x IN relationships(path) WHERE x.equipmentType = $neodash_rail_to_equipmenttype) 
        END
    AND all(
        x IN relationships(path)
        WHERE 1=1
        AND x.productType = $neodash_producttype_id
        AND NOT x.miles = 0
        AND (x.expirationDate >= date($neodash_ship_date) OR x.expirationDate IS NULL)
    )
    AND (isEmpty($neodash_party_name) OR 
        EXISTS {
            MATCH (d)-[:HAS_OCCUPANT]->(:Occupant)<-[:IS_OCCUPANT]-(p:Party)
            WHERE p.name = $neodash_party_name
        }
    )
    AND (isEmpty($neodash_market_id) OR 
        EXISTS {
            MATCH (d)-[:HAS_OCCUPANT]->(:Occupant)-[:PART_OF_MARKET]->(m:Market)
            WHERE m.id = $neodash_market_id
        }
    )   
    
    WITH o,d
        ,reduce(dist = 0, r IN relationships(path) | dist + coalesce(r.miles,9999)) AS totalMiles //coalesce with 9999 miles is to expose rates with no mileage
        ,coalesce(min([x IN relationships(path)|x.expirationDate])[0],'unknown') AS earliestExpiration
        ,round(reduce(cost = 0, r IN relationships(path) | cost + coalesce(r.totalCost,r.pvtTotalCost,1)),2) AS totalCost
        ,[x IN relationships(path)|x.equipmentType] AS equipmentType
        ,[x IN nodes(path)|x.id] AS Route
        ,[x IN relationships(path)|x.carrier] AS Carriers
    ORDER BY totalCost ASC

    WITH o,d
        , collect(totalMiles)[0] AS Miles
        , collect(totalCost)[0] AS Freight
        , collect(earliestExpiration)[0] AS RateExpiration
        , equipmentType[0] AS EquipmentType
        , collect(Carriers)[0] AS Carriers
        , collect(Route)[0] AS Route

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