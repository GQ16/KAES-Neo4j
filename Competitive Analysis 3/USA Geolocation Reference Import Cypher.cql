:param {
  file_path_root: 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/main/Competitive%20Analysis%203/',
  file_0: 'ZipData2',
  file_1: 'ZipCnty',
  file_2: 'Canada%20Geo%20Data',
  file_path_end: '.csv?raw=true'
};

CREATE CONSTRAINT `imp_uniq_CityState_city_and_stateOrProvince` IF NOT EXISTS
FOR (n: `CityState`)
REQUIRE (n.`city`,n.`stateOrProvince`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_StateOrProvince_id` IF NOT EXISTS
FOR (n: `StateOrProvince`)
REQUIRE (n.`id`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_ZipCode_id` IF NOT EXISTS
FOR (n: `ZipCode`)
REQUIRE (n.`id`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_County_id` IF NOT EXISTS
FOR (n: `County`)
REQUIRE (n.`id`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Geolocation_geoId` IF NOT EXISTS
FOR (n: `Geolocation`)
REQUIRE (n.`geoId`) IS UNIQUE;

// NODE load
// ---------
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
// ---------
// United States of America Nodes
  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`City` IS NULL
  CALL {
    WITH row
    MERGE (n: `CityState` { `city`: row.`City`, `stateOrProvince`: row.`State`})
    SET n.`abbreviation` = row.`CityAbbreviation`
    SET n.`id` = toUpper(row.`City` + ", " + row.`State`)
    SET n.geoId = randomUUID()
    SET n:Geolocation
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`State` IS NULL
  CALL {
    WITH row
    MERGE (n: `StateOrProvince` { `id`: row.`State` })
    SET n.geoId = randomUUID()
    SET n:Geolocation
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`ZIP` IS NULL
  CALL {
    WITH row
    MERGE (n: `ZipCode` { `id`: row.`ZIP`})
    SET n.coordinate = point({longitude: toFloatOrNull(row.`Longitude`),latitude:toFloatOrNull(row.`Latitude`)})
    SET n.geoId = randomUUID()
    SET n:Geolocation
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`Fips` IS NULL
  CALL {
    WITH row
    MERGE (n: `County` { `id`: row.`Fips` })
    SET n.`name` = row.`CountyName`
    SET n.geoId = randomUUID()
    SET n:Geolocation
  } IN TRANSACTIONS OF 10000 ROWS;

//Canadian Nodes
  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`CITY` IS NULL
  CALL {
    WITH row
    MERGE (n: `CityState` { `city`: row.`CITY`, `stateOrProvince`: row.`STATE`})
    SET n.`id` = toUpper(row.`CITY` + ", " + row.`STATE`)
    SET n.geoId = randomUUID()
    SET n:Geolocation
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`STATE` IS NULL
  CALL {
    WITH row
    MERGE (n: `StateOrProvince` { `id`: row.`STATE` })
    SET n.geoId = randomUUID()
    SET n:Geolocation
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`ZIP` IS NULL
  CALL {
    WITH row
    MERGE (n: `ZipCode` { `id`: left(row.`ZIP`,3) + ' ' + right(row.`ZIP`,3) })
    SET n.coordinate = point({longitude: toFloatOrNull(row.`LONG`),latitude:toFloatOrNull(row.`LAT`)})
    SET n.geoId = randomUUID()
    SET n:Geolocation
  } IN TRANSACTIONS OF 10000 ROWS;

// RELATIONSHIP load
// -----------------
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
// -----------------
// United States of America Relationships
  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `CityState` { `city`: row.`City`, `stateOrProvince`:row.`State`})
    MATCH (target: `StateOrProvince` { `id`: row.`State` })
    MERGE (source)-[r: `IN_STATE`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `ZipCode` { `id`: row.`ZIP` })
    MATCH (target: `CityState` { `city`: row.`City`, `stateOrProvince`:row.`State`})
    MERGE (source)-[r: `IN_CITY`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `ZipCode` { `id`: row.`ZIP` })
    MATCH (target: `StateOrProvince` { `id`: row.`State` })
    MERGE (source)-[r: `IN_STATE`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `ZipCode` { `id`: row.`zip` })
    MATCH (target: `County` { `id`: row.`Fips` })
    MERGE (source)-[r: `IN_COUNTY`]->(target)
    SET r.`percentageOfCounty` = toFloat(trim(row.`percentage`))
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `County` { `id`: row.`CountyFips`})
    MATCH (target: `StateOrProvince` { `id`: row.`State` })
    MERGE (source)-[r: `IN_STATE`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

//Canada Relationships
  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `CityState` { `city`: row.`CITY`, `stateOrProvince`:row.`STATE`})
    MATCH (target: `StateOrProvince` { `id`: row.`STATE` })
    MERGE (source)-[r: `IN_STATE`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `ZipCode` { `id`: left(row.`ZIP`,3) + ' ' + right(row.`ZIP`,3) })
    MATCH (target: `CityState` { `city`: row.`CITY`, `stateOrProvince`:row.`STATE`})
    MERGE (source)-[r: `IN_CITY`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_2 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `ZipCode` { `id`: left(row.`ZIP`,3) + ' ' + right(row.`ZIP`,3) })
    MATCH (target: `StateOrProvince` { `id`: row.`STATE` })
    MERGE (source)-[r: `IN_STATE`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;
