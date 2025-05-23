MATCH (l:Location)-[:IN_CITY]->(c:CityState),
(l)-[:IN_STATE]->(s:StateOrProvince),
(l)-[:IN_ZIPCODE]->(z:ZipCode),
(l)-[:IN_COUNTRY]->(co:Country),
(l)-[:HAS_INBOUND]->(m1:Mode),
(l)-[:HAS_OUTBOUND]->(m2:Mode)

WHERE 1=1
AND CASE
	WHEN isEmpty($citystate_id) THEN TRUE
	ELSE c.id = $citystate_id
	END
AND CASE
	WHEN isEmpty($location_name) THEN TRUE
	ELSE l.name = $location_name
	END
AND CASE
	WHEN isEmpty($stateorprovince_id) THEN TRUE
	ELSE s.id = $stateorprovince_id
	END	
AND CASE
	WHEN isEmpty($mode_id) THEN TRUE
	ELSE m1.id = $mode_id
	END
AND CASE 
	WHEN isEmpty($party_id) THEN TRUE
	ELSE 
		EXISTS{
			MATCH (l)-[:HAS_OCCUPANT]->()<-[:IS_OCCUPANT]-(p1:Party)
			WHERE p1.id = $party_id 
		}
		OR EXISTS{
			MATCH (l)<-[:OWNS_LOCATION]-(p2:Party) 
			WHERE p2.id = $party_id
		}
	END
AND CASE
	WHEN isEmpty($occupant_type) THEN TRUE
	ELSE EXISTS{
			MATCH (l)-[:HAS_OCCUPANT]->(o)
			WHERE $occupant_type IN labels(o) 
		}
	END
AND CASE
	WHEN isEmpty($logisticsproductgroup_name) THEN TRUE
	ELSE 
		EXISTS{
			MATCH (l)-[:HAS_OCCUPANT]->()-[:HAS_PRODUCTGROUP]->(lpg:LogisticsProductGroup)
			WHERE lpg.name = $logisticsproductgroup_name
		}
		OR EXISTS{
			MATCH (l)-[:HAS_OCCUPANT]->()-[:HAS_PRODUCTGROUP]->()-[:HAS_CHILD]->(child:LogisticsProductGroup) 
			WHERE child.name = $logisticsproductgroup_name
		}
	END

CALL{
	WITH l
	
	MATCH (mLPG:LogisticsProductGroup)<-[:FOR_PRODUCTGROUP]-(mpi:MPI)-[:FOR_OCCUPANT]->(o)<-[:HAS_OCCUPANT]-(l)
	WHERE mLPG.name = $logisticsproductgroup_name
	AND CASE 
		WHEN isEmpty($ship_date) THEN mpi.startDate <= date() <= mpi.endDate 
		ELSE mpi.startDate <= date($ship_date) <= mpi.endDate 
		END
	WITH mpi ORDER BY mpi.createdDate DESC
	WITH collect(mpi)[0] AS latestMPI
	RETURN  
	CASE latestMPI.type
		WHEN 'DESTINATION' 
		THEN coalesce(latestMPI.singularPrice-latestMPI.freight,((latestMPI.minPrice+latestMPI.maxPrice)/2) -latestMPI.freight) 
		WHEN 'ORIGIN' 
		THEN coalesce(latestMPI.singularPrice,(latestMPI.minPrice+latestMPI.maxPrice)/2) 
		ELSE NULL
	END AS indPrice
}
CALL{
	WITH l
	MATCH (l)-[:HAS_OCCUPANT]->(o)
	UNWIND labels(o) AS occType
	RETURN collect(distinct occType) AS occTypes
}

RETURN 
CASE
	WHEN "Koch" IN occTypes AND "Competitor" IN occTypes THEN "kochAndCompetitor"
	WHEN "Koch" IN occTypes THEN "koch"
	WHEN "Competitor" IN occTypes THEN "competitor"
	ELSE "customer"
END AS pinType,
collect(DISTINCT{
	name:l.name,
	id:l.id,
	address:l.address,
	city:c.id,
	state:s.id,
	zip:z.id,
	country:co.id,
	lat:l.coordinate.latitude,
	long:l.coordinate.longitude,
	price:indPrice
}) AS pins

UNION

WITH $citystate_id AS cs
WHERE NOT isEmpty(cs)
MATCH (c:CityState)<-[:IN_CITY]-(z:ZipCode)
WHERE c.id = $citystate_id
RETURN "zipCodes" AS pinType,
collect(DISTINCT{
	id:z.id,
	country:z.country,
	lat:z.coordinate.latitude,
	long:z.coordinate.longitude
}) AS pins