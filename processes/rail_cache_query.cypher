MATCH (mo:Mode)<-[:HAS_INBOUND]-(dl:Location)-[:HAS_OCCUPANT]->()-[cs:CAN_STORE]->(lpg:LogisticsProductGroup)
WHERE lpg.name = $product
AND mo.id = 'RAIL'
AND dl.id = $locationId

MATCH (rr2:RailRoute)-[:`AMMONIA_TO`]->(:RailStation|StationGroup)-[:IN_SPLC]->(s2:SPLC)<-[:IN_SPLC]-(dl)
WHERE (rr2)-[:HAS_DESTINATION_CARRIER]->()<-[:SERVED_BY]-(dl)

MATCH (ol:Location)-[:HAS_OCCUPANT]->(occ:Koch|Competitor)
, (occ)-[:COMPETES_IN]->(ds:StateOrProvince)<-[:IN_STATE]-(dl)
, (occ)-[:CAN_STORE]->(lpg)
, (ol)-[:HAS_OUTBOUND]->(mo)
WHERE ol <> dl

MATCH path = SHORTEST 3 (rr2)(
    ()-[:`AMMONIA_FROM`]->(stop2)-[:AT_INTERCHANGE]->(interchange)<-[:AT_INTERCHANGE]-(stop1)<-[:`AMMONIA_TO`]-()
){0, 2}(rr1:RailRoute)-[:`AMMONIA_FROM`]->()-[:IN_SPLC]->(s1:SPLC)<-[:IN_SPLC]-(ol)
WHERE (rr1)-[:HAS_ORIGIN_CARRIER]->()<-[:SERVED_BY]-(ol)

WITH DISTINCT ol, dl, lpg, s1, s2
, reverse([r IN nodes(path) WHERE r:RailRoute]) AS routes
, reverse([x IN nodes(path) WHERE x:SPLC AND x <> s1]) AS interchangeList
WHERE 1=1
AND all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())

WITH *, size(routes) AS numberOfRoutes, [s1] + interchangeList + [s2] AS splcList

CALL (routes, numberOfRoutes) {
        WITH routes, numberOfRoutes
        WHERE numberOfRoutes = 1
        WITH routes[0] AS route1
        MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
        WHERE 1=1
        AND NOT rate1.rule_11_at_origin_required
        AND NOT rate1.rule_11_at_destination_required
        AND rate1.rate_effective <= date()
        AND (date() <= rate1.rate_expiration OR rate1.rate_expiration IS NULL)
        RETURN [rate1] AS rates
    UNION
        WITH routes, numberOfRoutes
        WITH routes, numberOfRoutes
        WHERE numberOfRoutes = 2
        WITH routes[0] AS route1, routes[1] AS route2
        MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
        MATCH (route2)-[:HAS_CURRENT_RATE]->(rate2:RailRate)
        WHERE 1=1
        AND rate1.rule_11_at_destination_allowed
        AND NOT rate1.rule_11_at_origin_required
        AND rate2.rule_11_at_origin_allowed
        AND NOT rate2.rule_11_at_destination_required
        AND rate1.rate_effective <= date()
        AND (date() <= rate1.rate_expiration OR rate1.rate_expiration IS NULL)
        AND rate2.rate_effective <= date()
        AND (date() <= rate2.rate_expiration OR rate2.rate_expiration IS NULL)
        AND rate1.min_cars = 1
        AND rate2.min_cars = 1
        AND ((rate1.car_owner_shipper = rate2.car_owner_shipper) OR (rate1.car_owner_carrier = rate2.car_owner_carrier))
        RETURN [rate1, rate2] AS rates
    UNION
        WITH routes, numberOfRoutes
        WITH routes, numberOfRoutes
        WHERE numberOfRoutes = 3
        WITH routes[0] AS route1, routes[1] AS route2, routes[2] AS route3
        MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
        MATCH (route2)-[:HAS_CURRENT_RATE]->(rate2:RailRate)
        MATCH (route3)-[:HAS_CURRENT_RATE]->(rate3:RailRate)
        WHERE 1=1
        AND rate1.rule_11_at_destination_allowed
        AND NOT rate1.rule_11_at_origin_required
        AND rate2.rule_11_at_destination_allowed
        AND rate2.rule_11_at_origin_allowed
        AND rate3.rule_11_at_origin_allowed
        AND NOT rate3.rule_11_at_destination_required
        AND rate1.rate_effective <= date()
        AND (date() <= rate1.rate_expiration OR rate1.rate_expiration IS NULL)
        AND rate2.rate_effective <= date()
        AND (date() <= rate2.rate_expiration OR rate2.rate_expiration IS NULL)
        AND rate3.rate_effective <= date()
        AND (date() <= rate3.rate_expiration OR rate3.rate_expiration IS NULL)
        AND rate1.min_cars = 1
        AND rate2.min_cars = 1
        AND rate3.min_cars = 1
        AND (
            (rate1.car_owner_shipper = rate2.car_owner_shipper = rate3.car_owner_shipper)
            OR (rate1.car_owner_carrier = rate2.car_owner_carrier = rate3.car_owner_carrier)
        )
        RETURN [rate1, rate2, rate3] AS rates
}

WITH ol, dl, routes, splcList, interchangeList, rates, lpg, numberOfRoutes

MATCH (cad:Currency {id:'CAD'})-[exch:HAS_EXCHANGE_RATE]->(usd:Currency {id:'USD'})

WITH DISTINCT ol, dl, lpg
, [s IN splcList| s.id] AS splcs
, [i IN interchangeList| i.r260] AS interchanges
, [r IN rates| {
    document: r.document,
    baseRate: r.rate,
    baseRateUom: r.uom,
    baseRateCurrency: r.currency,
    exchRate: CASE WHEN r.currency = 'CAD' THEN exch.rate ELSE 1 END,
    usdPerShortTonRate: CASE toLower(r.uom)
        WHEN 'ton' THEN r.rate * CASE WHEN r.currency = 'CAD' THEN exch.rate ELSE 1 END
        WHEN 'car' THEN round(r.rate * CASE WHEN r.currency = 'CAD' THEN exch.rate ELSE 1 END / lpg.railCarVol, 3)
        ELSE 0
    END,
    carrier: r.carrier,
    originCarrier: r.origin_carrier,
    destinationCarrier: r.destination_carrier,
    route: r.route,
    minCars: r.min_cars,
    carOwner: CASE
        WHEN r.car_owner_shipper AND NOT r.car_owner_carrier
            THEN 'PVT'
        WHEN NOT r.car_owner_shipper AND r.car_owner_carrier
            THEN 'RR'
        WHEN r.car_owner_shipper AND r.car_owner_carrier
            THEN 'RR/PVT'
        ELSE 'OTHER'
    END,
    expiration: coalesce(r.rate_expiration, date('2099-01-01'))
}] AS rateMaps

WITH ol, dl, rateMaps[0].minCars AS minCars, rateMaps[0].carOwner AS carOwner, rateMaps, lpg, splcs, interchanges
, round( reduce( price = 0, x IN rateMaps | price + x.usdPerShortTonRate ),4 ) AS rateSum
ORDER BY carOwner DESC, rateSum

WITH ol, dl, lpg, minCars
, collect(carOwner)[0] AS carOwner
, collect(splcs)[0] AS splcs
, collect(interchanges)[0] AS interchanges
, collect(rateMaps)[0] AS legs

WITH ol
, dl
, lpg
, {
    id: ol.id + '|#|' + dl.id + '|#|' + lpg.name + '|#|' + minCars
    , splcs			        : splcs
    , interchanges          : interchanges
    , carOwner              : carOwner
    , minCars               : minCars
    , expiration            : [x IN legs|x.expiration]
    , documents             : [x IN legs|x.document]
    , baseRates             : [x IN legs|x.baseRate]
    , baseRateUoms          : [x IN legs|x.baseRateUom]
    , baseRateCurrencies    : [x IN legs|x.baseRateCurrency]
    , exchRates             : [x IN legs|x.exchRate]
    , usdPerShortTonRates   : [x IN legs|x.usdPerShortTonRate]
    , carriers		        : [x IN legs|x.carrier]
    , originCarriers        : [x IN legs|x.originCarrier]
    , destinationCarriers   : [x IN legs|x.destinationCarrier]
    , routes                : [x IN legs|x.route]
    , miles                 : []
    , fscs                  : []
} AS cacheProperties

MERGE (rc:RailCache{id:cacheProperties.id})
SET rc = cacheProperties
SET rc.update_date = datetime()

WITH rc, lpg, dl, ol
MERGE (lpg)-[:FOR_RAIL_CACHE]->(rc)
MERGE (rc)-[:HAS_DESTINATION]->(dl)
MERGE (rc)-[:HAS_ORIGIN]->(ol)
