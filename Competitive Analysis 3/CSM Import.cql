:param {
  file_path_root: 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/main/Competitive%20Analysis%203/',
  file_0: 'csm_clean',
  file_path_end: '.csv?raw=true'
};

CREATE CONSTRAINT `imp_uniq_RailStation_id` IF NOT EXISTS
FOR (n: `RailStation`)
REQUIRE (n.`id`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Carrier_id` IF NOT EXISTS
FOR (n: `Carrier`)
REQUIRE (n.`id`) IS UNIQUE;
CREATE CONSTRAINT `imp_uniq_Date_id` IF NOT EXISTS
FOR (n: `Date`)
REQUIRE (n.`id`) IS UNIQUE;

// NODE load
// ---------
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique.
// Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
// ---------
// CSM Nodes
  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`station_master_id` IS NULL AND NOT row.country = "MX"
  CALL {
    WITH row
    MERGE (n: `RailStation` { `id`: row.`station_master_id`})
    SET n.`r260` = row.`r260`
        , n.scac = row.scac
        , n.`splc` = row.`splc`
        , n.fsac = row.fsac
        , n.statusReason = row.station_status_reason
        , n.type = row.station_type
        , n.name = row.station_name
        , n.geoName = row.geopolitical_name
        , n.opslName = row.freight_station_opsl_name
        , n.county = row.county
        , n.stateOrProvince = n.administrative_area
        , n.country = row.country
        , n.zipCode = n.zip
        , n.effectiveDate = date(apoc.date.convertFormat(row.effective_date, "M/dd/yyyy", "yyyy-MM-dd"))
        , n.expirationDate = date(apoc.date.convertFormat(row.expiration_date , "M/dd/yyyy", "yyyy-MM-dd"))
        , n.cifId = row.cif_number_identification_code
        , n.importExportFlag = row.import_export_flag
        , n.customsFlag = row.customs_flag
        , n.grainFlag = row.grain_flag
        , n.autoRampFacilityFlag = row.automobile_ramp_facility_flag
        , n.intermodalFlag = row.intermodal_flag
        , n.embargoFlag = row.embargo_flag
        , n.operatingPlate = row.operating_plate
        , n.operatingWeight = row.operating_weight
        , n.coordinate = point({latitude: toFloatOrNull(row.latitude),longitude: toFloatOrNull(row.longitude)})
        , n.daylightSavingsFlag = row.daylight_indicator
        , n.timezone = row.timezone
        , n.opslNotes = row.opsl_notes
        , n.opslRefNumQualifier = row.opsl_reference_number_qualifier
        , n.canadianInterswitchArea = row.canadian_interswitch_area
        , n.abbreviation = row.abbreviation
        , n.newSCAC = row.new_scac
        , n.newFSAC = row.new_fsac
        , n.update_date = datetime()
    SET n:Geolocation
    SET n.geoId = randomUUID()
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`scac` IS NULL
  CALL {
    WITH row
    MERGE (n: `Carrier` { `id`: row.`scac` })
    SET n.update_date = datetime()
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row
  WHERE NOT row.`effective_date` IS NULL
  CALL {
    WITH row
    MERGE (n: `Date` { `id`: date(apoc.date.convertFormat(row.effective_date, "M/dd/yyyy", "yyyy-MM-dd"))})
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
    MATCH (source: `Carrier` { `id`: row.`scac`})
    MATCH (target: `RailStation` { `id`: row.`station_master_id` })
    MERGE (source)-[r: `OWNS_STATION`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `RailStation` { `id`: row.`station_master_id` })
    MATCH (target: `Date` { `id`: date(apoc.date.convertFormat(row.effective_date, "M/dd/yyyy", "yyyy-MM-dd"))})
    MERGE (source)-[r: `HAS_EFFECTIVE`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `RailStation` { `id`: row.`station_master_id` })
    MATCH (target: `Date` { `id`: date(apoc.date.convertFormat(row.expiration_date, "M/dd/yyyy", "yyyy-MM-dd"))})
    MERGE (source)-[r: `HAS_EXPIRATION`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `RailStation` { `id`: row.`station_master_id` })
    MATCH (target: `ZipCode` { `id`: row.`zip` })
    MERGE (source)-[r: `IN_ZIPCODE`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `RailStation` { `id`: row.`station_master_id` })
    MATCH (target: `CityState` { `city`: row.`geopolitical_name`, stateOrProvince: row.administrative_area })
    MERGE (source)-[r: `IN_CITY`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `RailStation` { `id`: row.`station_master_id` })
    MATCH (target: `StateOrProvince` { `id`: row.`administrative_area` })
    MERGE (source)-[r: `IN_STATE`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `RailStation` { `id`: row.`station_master_id` })
    MATCH (target: `County` { `name`: row.`county` })
    MERGE (source)-[r: `IN_COUNTY`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;

  :auto LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0 + $file_path_end) AS row
  WITH row 
  CALL {
    WITH row
    MATCH (source: `RailStation` { `id`: row.`station_master_id` })
    MATCH (target: `Country` { `id`: row.`country` })
    MERGE (source)-[r: `IN_COUNTRY`]->(target)
  } IN TRANSACTIONS OF 10000 ROWS;