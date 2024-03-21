CALL{
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
			
		WITH o.id AS Origin, o.type AS OriginType, tt.originZip AS OriginZip
		, d.id AS Destination, tt.destZip AS DestinationZip, tt.miles AS Miles
		, "Truck" AS Mode, se.endDate AS RateExpiration, ra.amount AS rate, ra.uom AS uom
		, CASE ra.uom
		WHEN "$/ST" THEN ra.amount
		WHEN "$/MI" THEN ra.amount * tt.miles/ra.tons
		END AS Freight

		RETURN DISTINCT Origin, OriginType, round(avg(Freight),2) AS Freight, Mode, round(avg(Miles),2) AS Miles, NULL AS EquipmentType, RateExpiration, rate, uom

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

		WITH o.id AS Origin, o.type AS OriginType, tt.originZip AS OriginZip
		, d.id AS Destination, tt.destZip AS DestinationZip, tt.miles AS Miles
		, se.endDate AS RateExpiration, ra.amount AS rate, ra.uom AS uom, "Truck" AS Mode
		, CASE ra.uom
		WHEN "$/ST" THEN ra.amount
		WHEN "$/MI" THEN ra.amount * tt.miles/ra.tons
		END AS Freight

		RETURN DISTINCT Origin, OriginType, round(avg(Freight),2) AS Freight, Mode, round(avg(Miles),2) AS Miles, NULL AS EquipmentType, RateExpiration, rate, uom
		
	UNION
	// Rail Section
		MATCH path = (o:Location)-[:RAIL_TO*..3]->(d:Location)
		,(c:City)<-[:IN]-(o)-[:IN]->(s:StateOrProvince)
		,(c2:City)<-[:IN]-(d)-[:IN]->(s2:StateOrProvince)
        ,(o)-[:HAS_PRODUCTTYPE]->(pt:ProductType{id:$neodash_producttype_id})

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

		WITH o.id AS Origin, o.type AS OriginType, d.id AS Destination
		,reduce(dist = 0, r IN relationships(path) | dist + coalesce(r.miles,9999)) AS totalMiles //coalesce with 9999 miles is to expose rates with no mileage
		,min([x IN relationships(path)|x.expirationDate])[0] AS earliestExpiration
		,round(reduce(cost = 0, r IN relationships(path) | cost + coalesce(r.totalCost,r.pvtTotalCost,1)),2) AS totalCost
		,[x IN relationships(path)|x.equipmentType] AS equipmentType
		ORDER BY totalCost ASC

		WITH Origin, OriginType, Destination, collect(totalMiles)[0] AS Miles
		, collect(totalCost)[0] AS Freight, collect(earliestExpiration)[0] AS RateExpiration
		, equipmentType[0] AS EquipmentType

		RETURN Origin, OriginType, Freight,"Rail" AS Mode, Miles, EquipmentType, RateExpiration, NULL AS rate, NULL as uom
}

WITH Origin, OriginType, Freight, Mode, Miles, EquipmentType, RateExpiration, rate, uom
ORDER BY Freight

CALL{
	WITH Origin, Freight, Mode, Miles, EquipmentType, RateExpiration, rate, uom
	OPTIONAL MATCH (p:ProductType {id:$neodash_producttype_id})<-[:FOR_PRODUCTTYPE]-(m:MPI)-[:FOR_ORIGIN]->(o:Location{id:Origin})

    WHERE 1=1
	  AND m.startDate <= date($neodash_ship_date)
      AND m.endDate >= date($neodash_ship_date)

	WITH Origin AS Origin2, m.party AS Party, Mode AS Mode2
    , coalesce(m.singularPrice,(m.minPrice+m.maxPrice)/2) AS Indication, Freight AS Freight2
	, Miles AS Miles2, EquipmentType AS EquipmentType2, m.last_update_date AS LoggedDate, RateExpiration AS RateExpiration2
	, rate AS rate2, uom AS uom2
	ORDER BY LoggedDate DESC

	WITH Origin2, collect(Party)[0] AS Party, Mode2, collect(Indication)[0] AS Indication
	, Freight2, Miles2, EquipmentType2, RateExpiration2, rate2, uom2
	RETURN Origin2, Party, Mode2, Miles2, EquipmentType2, RateExpiration2
	, Indication, Freight2, round(Indication+Freight2,2) AS DeliveredPrice, rate2, uom2
}
RETURN Origin2 AS Origin, Mode2 AS Mode, rate2 + ' '+ uom2 AS Rate, Miles2 AS Miles, Party, Indication, Freight2 AS Freight
, DeliveredPrice, RateExpiration2 AS RateExpiration