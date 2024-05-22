MATCH (lpg:LogisticsProductGroup)
WHERE lpg.name = $neodash_logisticsproductgroup_name
OPTIONAL MATCH prodPath=(:LogisticsProductGroup)-[:HAS_CHILD*]->(lpg)
WITH coalesce(nodes(prodPath),[lpg]) AS lpgHier

MATCH (ds:StateOrProvince)<-[:IN_STATE]-(dl:Location)-[:IN_ZIPCODE]->(dz:ZipCode)
, (co:Country)<-[:IN_COUNTRY]-(dz)-[:IN_LOCATION_GROUP]->(dlg)
, (co)<-[:IN_COUNTRY]-(oc:CityState)-[:FROM]-(route:TruckRoute)-[:TO]-(dlg)
, (route)-[:FOR_PRODUCTGROUP]->(routeProd:LogisticsProductGroup)
, (ol:Location)-[:IN_CITY]->(oc)
, (ol)-[:IN_ZIPCODE]->(oz)
, (occ:Koch|Competitor)<-[:HAS_OCCUPANT]-(ol)
, (occ)-[:HAS_PRODUCTGROUP]->(occProd:LogisticsProductGroup)
, (occ)-[:COMPETES_IN]->(ds)
, (route)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
, (oz)-[tdt:TRUCK_DISTANCE_TO]-(dz)
, (tFSC:TruckFSC)-[:FOR_ORIGIN_COUNTRY]->(co)
, (tFSC)-[:FOR_PRODUCTGROUP]->(fscProd:LogisticsProductGroup)
, (ol)-[:HAS_OUTBOUND]->(mo:Mode)<-[:HAS_INBOUND]-(dl)

WHERE 1=1
AND dl.name = $neodash_location_name_2
AND routeProd IN lpgHier
AND occProd IN lpgHier
AND fscProd IN lpgHier
AND cr.distanceLower < tdt.distance <= cr.distanceUpper
AND co.id = 'US'
AND tFSC.expirationDate IS NULL
AND mo.id = 'TRUCK'
    
OPTIONAL MATCH (occ)<-[:IS_OCCUPANT]-(party:Party)

WITH ol, dl, occ, party.name AS occPartyName, 
  [x IN labels(occ) WHERE x <> 'Occupant'] AS occupantTypes, cr, tdt, tFSC

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