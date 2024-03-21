CALL{
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
    WITH o.id AS OriginId, o.name AS Origin, d.id AS DestinationId, d.name AS Destination, tt.miles AS Miles
    , "Truck" AS Mode, d2.id AS RateExpiration, ra.amount AS rate, ra.uom AS uom
    , CASE ra.uom
        WHEN "$/ST" THEN ra.amount
        WHEN "$/MI" THEN ra.amount * tt.miles/ra.tons
    END AS Freight

    RETURN DISTINCT OriginId, Origin, DestinationId, Destination, ceil(avg(Freight)) AS Freight, Mode, round(avg(Miles),2) AS Miles, NULL AS EquipmentType, RateExpiration, rate, uom

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

    WITH o.id AS OriginId, o.name AS Origin, d.id AS DestinationId, d.name AS Destination, tt.miles AS Miles
    , d2.id AS RateExpiration, ra.amount AS rate, ra.uom AS uom, "Truck" AS Mode
    , CASE ra.uom
    WHEN "$/ST" THEN ra.amount
    WHEN "$/MI" THEN ra.amount * tt.miles/ra.tons
    END AS Freight

    RETURN DISTINCT OriginId, Origin, DestinationId, Destination, ceil(avg(Freight)) AS Freight, Mode, round(avg(Miles),2) AS Miles, NULL AS EquipmentType, RateExpiration, rate, uom

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

    WITH o.id AS OriginId, o.name AS Origin, d.id AS DestinationId, d.name AS Destination
        ,reduce(dist = 0, r IN relationships(path) | dist + coalesce(r.miles,9999)) AS totalMiles //coalesce with 9999 miles is to expose rates with no mileage
        ,min([x IN relationships(path)|x.expirationDate])[0] AS earliestExpiration
        ,ceil(reduce(cost = 0, r IN relationships(path) | cost + coalesce(r.totalCost,r.pvtTotalCost,1))) AS totalCost
        ,[x IN relationships(path)|x.equipmentType] AS equipmentType
        , CASE WHEN all(x IN relationships(path) WHERE x.totalCost IS NULL) THEN 'PVT' ELSE 'RR' END AS pvtRateFlag
    ORDER BY totalCost ASC

    WITH OriginId, Origin, DestinationId, Destination
        , collect(totalMiles)[0] AS Miles
        , collect(totalCost)[0] AS Freight
        , collect(earliestExpiration)[0] AS RateExpiration
        , equipmentType[0]+' '+pvtRateFlag AS EquipmentType

    RETURN OriginId, Origin, DestinationId, Destination, Freight,"Rail" AS Mode, Miles, EquipmentType, RateExpiration, NULL AS rate, NULL as uom
}
WITH OriginId, Origin, DestinationId, Destination, Freight, Mode, Miles, EquipmentType, RateExpiration, rate, uom
ORDER BY Freight

CALL{
    WITH OriginId, Origin, DestinationId, Destination, Freight, Mode, Miles, EquipmentType, RateExpiration, rate, uom
	OPTIONAL MATCH (pt:ProductType{id:$neodash_producttype_id})<-[:FOR_PRODUCTTYPE]-(m:MPI)-[:FOR_ORIGIN]->(o:Location{id:OriginId})
    OPTIONAL MATCH (d1:Date)<-[:HAS_START]-(m)-[:HAS_END]->(d2:Date), (m)-[:FOR_OCCUPANT]->(oc:Occupant)<-[:IS_OCCUPANT]-(p:Party)
    WHERE 1=1
        AND d1.id <= date($neodash_ship_date)
        AND d2.id >= date($neodash_ship_date)

    OPTIONAL MATCH (m)-[r:FOR_DESTINATION]->(d:Location)

	WITH Origin AS Origin2, Destination AS Destination2, p.name AS Party, oc.type AS Type, Mode AS Mode2
        , Freight AS Freight2, Miles AS Miles2, EquipmentType AS EquipmentType2, m.update_date AS LoggedDate
        , RateExpiration AS RateExpiration2, rate AS rate2, uom AS uom2
        , CASE WHEN r IS NULL
        THEN coalesce(m.singularPrice,(m.minPrice+m.maxPrice)/2) 
        ELSE coalesce(m.singularPrice,(m.minPrice+m.maxPrice)/2) - m.freight
        END AS Indication
    ORDER BY LoggedDate DESC

	WITH Origin2,Destination2, Mode2, Freight2, Miles2, EquipmentType2, RateExpiration2, rate2, uom2
    , collect(Party)[0] AS Party
    , collect(Type)[0] AS Type
    , collect(Indication)[0] AS Indication

	RETURN Origin2, Destination2, Party, Type, Mode2, Miles2, EquipmentType2, RateExpiration2
	, Indication, Freight2, ceil(Indication+Freight2) AS DeliveredPrice, rate2, uom2
}
RETURN Origin2 AS Origin, Destination2 AS Destination,Mode2 AS Mode, rate2 + ' '+ uom2 AS Rate, EquipmentType2 AS EquipmentType, Miles2 AS Miles, Party, Type, Indication, Freight2 AS Freight
, DeliveredPrice, RateExpiration2 AS RateExpiration