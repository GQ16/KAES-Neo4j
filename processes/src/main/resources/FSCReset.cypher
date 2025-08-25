MATCH (rc:RailCache)
SET rc.fscs = [];

MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(lpg:LogisticsProductGroup)
WHERE size(rc.splcs) > 0 + 1

WITH rc, lpg, rc.carriers[0] AS carrierId, rc.baseRateCurrencies[0] AS currencyId
MATCH (ca:Carrier{id:carrierId})
MATCH (cu:Currency{id:currencyId})
OPTIONAL MATCH (ca)-[:HAS_CURRENT_FSC]->(rFSC:RailFSC)<-[:FOR_RAIL_FSC]-(cu:Currency)
, (rFSC)-[:FOR_PRODUCTGROUP]->(lpg)
OPTIONAL MATCH (cu)-[exch:HAS_EXCHANGE_RATE]->(usd:Currency{id:'USD'})

WITH rc, coalesce(rFSC.rate, 0.0) AS perCarPerMileFuel, coalesce(round(rFSC.rate/lpg.railCarVol, 6), 0.0) AS perShortTonPerMileFuel, coalesce(exch.rate, 1) AS exchangeRate
WITH rc, round(perShortTonPerMileFuel * exchangeRate * rc.miles[0], 4) AS usdPerShortTonFuel
SET rc.fscs = rc.fscs + [usdPerShortTonFuel];

MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(lpg:LogisticsProductGroup)
WHERE size(rc.splcs) > 1 + 1

WITH rc, lpg, rc.carriers[1] AS carrierId, rc.baseRateCurrencies[1] AS currencyId
MATCH (ca:Carrier{id:carrierId})
MATCH (cu:Currency{id:currencyId})
OPTIONAL MATCH (ca)-[:HAS_CURRENT_FSC]->(rFSC:RailFSC)<-[:FOR_RAIL_FSC]-(cu:Currency)
, (rFSC)-[:FOR_PRODUCTGROUP]->(lpg)
OPTIONAL MATCH (cu)-[exch:HAS_EXCHANGE_RATE]->(usd:Currency{id:'USD'})

WITH rc, coalesce(rFSC.rate, 0.0) AS perCarPerMileFuel, coalesce(round(rFSC.rate/lpg.railCarVol, 6), 0.0) AS perShortTonPerMileFuel, coalesce(exch.rate, 1) AS exchangeRate
WITH rc, round(perShortTonPerMileFuel * exchangeRate * rc.miles[1], 4) AS usdPerShortTonFuel
SET rc.fscs = rc.fscs + [usdPerShortTonFuel];

MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(lpg:LogisticsProductGroup)
WHERE size(rc.splcs) > 2 + 1

WITH rc, lpg, rc.carriers[2] AS carrierId, rc.baseRateCurrencies[2] AS currencyId
MATCH (ca:Carrier{id:carrierId})
MATCH (cu:Currency{id:currencyId})
OPTIONAL MATCH (ca)-[:HAS_CURRENT_FSC]->(rFSC:RailFSC)<-[:FOR_RAIL_FSC]-(cu:Currency)
, (rFSC)-[:FOR_PRODUCTGROUP]->(lpg)
OPTIONAL MATCH (cu)-[exch:HAS_EXCHANGE_RATE]->(usd:Currency{id:'USD'})

WITH rc, coalesce(rFSC.rate, 0.0) AS perCarPerMileFuel, coalesce(round(rFSC.rate/lpg.railCarVol, 6), 0.0) AS perShortTonPerMileFuel, coalesce(exch.rate, 1) AS exchangeRate
WITH rc, round(perShortTonPerMileFuel * exchangeRate * rc.miles[2], 4) AS usdPerShortTonFuel
SET rc.fscs = rc.fscs + [usdPerShortTonFuel];

MATCH (rc:RailCache)
SET rc.freight = round( reduce( price = 0, x IN range(0,size(rc.splcs)-2) | price + rc.usdPerShortTonRates[x] + rc.fscs[x] ),4 )
SET rc.totalMiles = reduce( dist = 0, x IN range(0,size(rc.splcs)-2) | dist + rc.miles[x] );