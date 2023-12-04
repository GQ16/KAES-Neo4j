:param {
  file_path_root: 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/main/Competitive%20Analysis%203/',
  file_0: 'csm_clean',
  file_path_end: '.csv?raw=true'
};

CREATE CONSTRAINT `imp_uniq_RailStation_id` IF NOT EXISTS
FOR (n: `RailStation`)
REQUIRE (n.`id`) IS UNIQUE;

// NODE load
// ---------
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique.
// Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
// ---------
// CSM Nodes
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
    WITH n
    MATCH (c:Country {id:'US'})
    MERGE (n)-[:IN_COUNTRY]->(c)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`State` IS NULL
  CALL {
    WITH row
    MERGE (n: `StateOrProvince` { `id`: row.`State` })
    SET n.geoId = randomUUID()
    SET n:Geolocation
    WITH n
    MATCH (c:Country {id:'US'})
    MERGE (n)-[:IN_COUNTRY]->(c)
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
    WITH n
    MATCH (c:Country {id:'US'})
    MERGE (n)-[:IN_COUNTRY]->(c)
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
    WITH n
    MATCH (c:Country {id:'US'})
    MERGE (n)-[:IN_COUNTRY]->(c)
  } IN TRANSACTIONS OF 10000 ROWS;

// RELATIONSHIP load
// -----------------
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement,
// meaning only one relationship of a given type will ever be created between a pair of nodes.
// -----------------
// CSM Relationships
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