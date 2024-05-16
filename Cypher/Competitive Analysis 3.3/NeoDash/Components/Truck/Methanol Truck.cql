//Methanol Truck
MATCH (st:StateOrProvince)<-[:IN_STATE]-(dl:Location)-[:IN_ZIPCODE]->(dz:ZipCode)
MATCH (dl)-[:IN_CITY]->(dcs:CityState)
, (prod:LogisticsProductGroup)<-[:HAS_PRODUCTGROUP]-(occ:Occupant)<-[:HAS_OCCUPANT]-(l:Location)
, (l)-[:IN_CITY]->(ocs:CityState)
, (ocs)<-[:FROM]-(tr:TruckRoute)-[:TO]->(dcs)
, (dcs)<-[:IN_CITY]-(dz)
, (tr)-[:FOR_PRODUCTGROUP]->(prod)
, (tr)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
, (l)-[:IN_COUNTRY]->(c:Country)<-[:FOR_ORIGIN_COUNTRY]-(tFSC:TruckFSC)
, (tFSC:TruckFSC)-[:FOR_PRODUCTGROUP]->(prod)

WHERE 1=1
AND dl.id = $neodash_location_id_2
AND prod.name = 'METHANOL'
AND (l)-[:HAS_OUTBOUND]->(:Mode{id:'TRUCK'})
AND tFSC.expirationDate IS NULL
AND (c)<-[:FOR_ORIGIN_COUNTRY]-(tFSC)

OPTIONAL MATCH (oz)-[tdt:TRUCK_DISTANCE_TO]->(dz)
OPTIONAL MATCH (occ)<-[:IS_OCCUPANT]-(party:Party)


  WITH ol, dl, occ, p.name AS occPartyName, [x IN labels(occ) WHERE x <> 'Occupant'] AS occupantTypes, cr, tdt, tFSC

  WITH ol, dl, occ, occPartyName, occupantTypes, tdt.distance AS miles, tFSC.rate AS fsc
  , cr.rate AS baseRate, cr.ratePerUom AS ratePerUom, cr.loadQuantity AS loadQty, toUpper(cr.rateFactorType) AS rateFactor
  
  WITH ol, dl, occ, occPartyName, occupantTypes, fsc, miles,
  CASE rateFactor
      WHEN "DISTANCE" THEN (baseRate*miles)/(loadQty)
      WHEN "UNIT" THEN baseRate
      WHEN "LOAD" THEN ratePerUom
  END AS rate,
  CASE rateFactor
      WHEN "DISTANCE" THEN "Mile"
      WHEN "UNIT" THEN "Unit"
      WHEN "LOAD" THEN "Load"
  END AS rateFactor

  WITH ol, dl, occ, occPartyName, occupantTypes, "TRUCK" AS mode, rate, rateFactor, rate * (1+fsc) AS freight, miles
  
  //MARK: Get Indication
  CALL{
    WITH occ
    OPTIONAL MATCH (occ)<-[:FOR_OCCUPANT]-(m:MPI)-[:FOR_PRODUCTGROUP]->(:LogisticsProductGroup{name: $neodash_logisticsproductgroup_name})
    , (m)-[:FOR_MODE]->(:Mode{id:'TRUCK'})
        , (c:Currency)-[:FOR_MPI]->(m)
    WHERE m.startDate <= date($neodash_ship_date) <= m.endDate
    
    WITH m, c.id AS curr
    ORDER BY m.createdDate DESC LIMIT 1	//Get latest MPI
    
    WITH m.createdDate AS dateLogged,
    CASE 
      WHEN m.type = 'DESTINATION' AND m.singularPrice IS NOT NULL
      THEN m.singularPrice-m.freight 
      WHEN m.type = 'DESTINATION' AND m.singularPrice IS NULL
      THEN ((m.minPrice+m.maxPrice)/2) - m.freight
            WHEN m.type = 'ORIGIN' AND m.singularPrice IS NOT NULL
      THEN m.singularPrice
            WHEN m.type = 'ORIGIN' AND m.singularPrice IS NULL
      THEN (m.minPrice+m.maxPrice)/2
            ELSE NULL
    END AS indPrice,
    CASE 
        WHEN curr = $neodash_currency_id THEN 1 
        WHEN curr = 'USD' AND $neodash_currency_id = 'CAD' THEN 1.36
        WHEN curr = 'CAD' AND $neodash_currency_id = 'USD' THEN 0.73
    END AS exchRate

    RETURN indPrice * exchRate AS indication, dateLogged
  }

RETURN 
    ol.id AS originLocationId,
    ol.name AS originLocationName,
    round(ol.coordinate.latitude,5) AS originLat,
    round(ol.coordinate.longitude,5) AS originLong,    
    occPartyName,
    occupantTypes,
    dl.id AS destLocationId,
    dl.name AS destLocationName,
    round(dl.coordinate.latitude,5) AS destLat,
    round(dl.coordinate.longitude,5) AS destLong,
    mode,
    "$" + toString(round(rate,2)) + "/" + rateFactor AS rateDesc,
    null AS isPrivate,
    null AS minCars,
    miles,
    freight,
    indication,
    dateLogged,
    freight + indication AS delivered
ORDER BY freight LIMIT 10