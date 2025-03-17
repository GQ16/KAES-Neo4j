//MARK: Points
CREATE CONSTRAINT GasLocation_id_uniq IF NOT EXISTS FOR (g:GasLocation) REQUIRE g.id IS UNIQUE;
CREATE CONSTRAINT Segment_id_uniq IF NOT EXISTS FOR (s:Segment) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT Pipeline_id_uniq IF NOT EXISTS FOR (s:Pipeline) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT GasZone_pipeline_and_zone_name_uniq IF NOT EXISTS FOR (z:GasZone) REQUIRE (z.pipeline, z.name) IS UNIQUE;

MERGE (p:Pipeline{id:'GLGT'})
SET p.name = 'GREAT LAKES GAS TRANSMISSION'
WITH p
MERGE (wz:GasZone{pipeline:'GLGT', name:'WESTERN'})
MERGE (cz:GasZone{pipeline:'GLGT', name:'CENTRAL'})
MERGE (ez:GasZone{pipeline:'GLGT', name:'EASTERN'})
MERGE (wz)<-[:HAS_ZONE]-(p)
MERGE (cz)<-[:HAS_ZONE]-(p)
MERGE (ez)<-[:HAS_ZONE]-(p);

UNWIND [
    {
        fromPoint: "EMERSON",
        toPoint: "CLOW"
    },
    {
        fromPoint: "CLOW",
        toPoint: "THEIF RIVER FALLS"
    },
    {
        fromPoint: "THEIF RIVER FALLS",
        toPoint: "MAYFIELD"
    },
    {
        fromPoint: "MAYFIELD",
        toPoint: "CLEARBROOK"
    },
    {
        fromPoint: "CLEARBROOK",
        toPoint: "LAMMERS"
    },
    {
        fromPoint: "LAMMERS",
        toPoint: "SOLWAY"
    },
    {
        fromPoint: "SOLWAY",
        toPoint: "BEMIDJI"
    },
    {
        fromPoint: "BEMIDJI",
        toPoint: "GRACE LAKE"
    },
    {
        fromPoint: "GRACE LAKE",
        toPoint: "CASS LAKE"
    },
    {
        fromPoint: "CASS LAKE",
        toPoint: "DEER RIVER"
    },
    {
        fromPoint: "DEER RIVER",
        toPoint: "COHASSET"
    },
    {
        fromPoint: "COHASSET",
        toPoint: "LITTLE BASS"
    },
    {
        fromPoint: "LITTLE BASS",
        toPoint: "RANGE LINE ROAD"
    },
    {
        fromPoint: "RANGE LINE ROAD",
        toPoint: "GRAND RAPID"
    },
    {
        fromPoint: "GRAND RAPID",
        toPoint: "GRAND RAPID NN"
    },
    {
        fromPoint: "GRAND RAPID NN",
        toPoint: "BLACKBERRY"
    },
    {
        fromPoint: "BLACKBERRY",
        toPoint: "FLOODWOOD"
    },
    {
        fromPoint: "FLOODWOOD",
        toPoint: "CARLTON"
    },
    {
        fromPoint: "CARLTON",
        toPoint: "CLOQUET"
    },
    {
        fromPoint: "CLOQUET",
        toPoint: "DULUTH"
    },
    {
        fromPoint: "DULUTH",
        toPoint: "SUPERIOR"
    },
    {
        fromPoint: "SUPERIOR",
        toPoint: "ASHLAND"
    },
    {
        fromPoint: "ASHLAND",
        toPoint: "IRONWOOD"
    },
    {
        fromPoint: "IRONWOOD",
        toPoint: "WAKEFIELD"
    },
    {
        fromPoint: "WAKEFIELD",
        toPoint: "MARENISCO"
    },
    {
        fromPoint: "MARENISCO",
        toPoint: "DUCK CREEK"
    },
    {
        fromPoint: "DUCK CREEK",
        toPoint: "WATERSMEET"
    },
    {
        fromPoint: "WATERSMEET",
        toPoint: "FORTUNE LAKE"
    },
    {
        fromPoint: "FORTUNE LAKE",
        toPoint: "CRYSTAL FALLS"
    },
    {
        fromPoint: "CRYSTAL FALLS",
        toPoint: "SAGOLA"
    },
    {
        fromPoint: "SAGOLA",
        toPoint: "RAPID RIVER"
    },
    {
        fromPoint: "RAPID RIVER",
        toPoint: "MANISTIQUE"
    },
    {
        fromPoint: "MANISTIQUE",
        toPoint: "ENGADINE"
    },
    {
        fromPoint: "ENGADINE",
        toPoint: "RUDYARD"
    },
    {
        fromPoint: "RUDYARD",
        toPoint: "SAULT STE MARIE"
    },
    {
        fromPoint: "SAULT STE MARIE",
        toPoint: "SAULT STE MARIE TCPL"
    },
    {
        fromPoint: "ENGADINE",
        toPoint: "ST.IGNACE"
    },
    {
        fromPoint: "ST.IGNACE",
        toPoint: "MACKINAW"
    },
    {
        fromPoint: "MACKINAW",
        toPoint: "PELLSTON"
    },
    {
        fromPoint: "PELLSTON",
        toPoint: "PETOSKEY"
    },
    {
        fromPoint: "PETOSKEY",
        toPoint: "HUDSON"
    },
    {
        fromPoint: "HUDSON",
        toPoint: "ALPINE"
    },
    {
        fromPoint: "ALPINE",
        toPoint: "GAYLORD"
    },
    {
        fromPoint: "GAYLORD",
        toPoint: "JORDAN VALLEY"
    },
    {
        fromPoint: "JORDAN VALLEY",
        toPoint: "SOUTH CHESTER"
    },
    {
        fromPoint: "SOUTH CHESTER",
        toPoint: "WILDERNESS"
    },
    {
        fromPoint: "WILDERNESS",
        toPoint: "DEWARD"
    },
    {
        fromPoint: "DEWARD",
        toPoint: "GOOSE CREEK"
    },
    {
        fromPoint: "GOOSE CREEK",
        toPoint: "FARWELL"
    },
    {
        fromPoint: "FARWELL",
        toPoint: "CHIPPEWA"
    },
    {
        fromPoint: "CHIPPEWA",
        toPoint: "MIDLAND"
    },
    {
        fromPoint: "MIDLAND",
        toPoint: "WHEELER BASE DAIRY"
    },
    {
        fromPoint: "WHEELER BASE DAIRY",
        toPoint: "BIRCH RUN"
    },
    {
        fromPoint: "BIRCH RUN",
        toPoint: "OTISVILLE"
    },
    {
        fromPoint: "OTISVILLE",
        toPoint: "RICHFIELD"
    },
    {
        fromPoint: "RICHFIELD",
        toPoint: "CAPAC"
    },
    {
        fromPoint: "CAPAC",
        toPoint: "ALLENTON"
    },
    {
        fromPoint: "ALLENTON",
        toPoint: "MUTTONVILLE"
    },
    {
        fromPoint: "MUTTONVILLE",
        toPoint: "TRUMBLE RD."
    },
    {
        fromPoint: "TRUMBLE RD.",
        toPoint: "BAUMAN"
    },
    {
        fromPoint: "BAUMAN",
        toPoint: "RATTLE RUN"
    },
    {
        fromPoint: "RATTLE RUN",
        toPoint: "ST.CLAIR"
    },
    {
        fromPoint: "ST.CLAIR",
        toPoint: "BELLE RIVER MILLS"
    },
    {
        fromPoint: "BELLE RIVER MILLS",
        toPoint: "CHINA"
    }
] 
AS pointPairs
WITH pointPairs
MERGE (fp:GasLocation{id:pointPairs.fromPoint})
MERGE (tp:GasLocation{id:pointPairs.toPoint})
WITH fp, tp
OPTIONAL MATCH (fp)<-[:CONNECTS]-(es:Segment)-[:CONNECTS]->(tp)
WITH fp, tp, coalesce(es.id, randomUUID()) AS segmentId
MERGE (fp)<-[:CONNECTS]-(s:Segment{id:segmentId})-[:CONNECTS]->(tp);

//MARK: Set up zones
MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'EMERSON'
AND endPoint.id = 'SUPERIOR'
WITH [x IN nodes(path) WHERE x:GasLocation] AS points
MATCH (z:GasZone{pipeline:'GLGT', name:'WESTERN'})
FOREACH (p IN points | MERGE (p)-[:PART_OF_ZONE]->(z));

MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'ASHLAND'
AND (endPoint.id = 'JORDAN VALLEY' OR endPoint.id = 'SAULT STE MARIE TCPL')
WITH [x IN nodes(path) WHERE x:GasLocation] AS points
MATCH (z:GasZone{pipeline:'GLGT', name:'CENTRAL'})
FOREACH (p IN points | MERGE (p)-[:PART_OF_ZONE]->(z));

MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'SOUTH CHESTER'
AND endPoint.id = 'CHINA'
WITH [x IN nodes(path) WHERE x:GasLocation] AS points
MATCH (z:GasZone{pipeline:'GLGT', name:'EASTERN'})
FOREACH (p IN points | MERGE (p)-[:PART_OF_ZONE]->(z));

//MARK: Set Pipe Diameters
MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'EMERSON'
AND endPoint.id = 'ST.IGNACE'
WITH [x IN nodes(path) WHERE x:Segment] AS segments
FOREACH (s IN segments | SET s.minDiameter = 30, s.maxDiameter = 42);

MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'ST.IGNACE'
AND endPoint.id = 'MACKINAW'
WITH [x IN nodes(path) WHERE x:Segment] AS segments
FOREACH (s IN segments | SET s.minDiameter = 20, s.maxDiameter = 26);

MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'MACKINAW'
AND endPoint.id = 'CHINA'
WITH [x IN nodes(path) WHERE x:Segment] AS segments
FOREACH (s IN segments | SET s.minDiameter = 30, s.maxDiameter = 42);

MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'ENGADINE'
AND endPoint.id = 'SAULT STE MARIE'
WITH [x IN nodes(path) WHERE x:Segment] AS segments
FOREACH (s IN segments | SET s.minDiameter = 12, s.maxDiameter = 19);

//MARK: GASUTIL
CREATE CONSTRAINT GasUtil_uniq_id IF NOT EXISTS FOR (u:GasUtil) REQUIRE u.id IS unique;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/UTIL.csv' AS row
WITH row
MERGE (u:GasUtil{id:row.`Rate Definition ID`})
SET u.rate = toFloat(row.Rate)
, u.startDate = date(apoc.date.convertFormat(row.`Start Date` , "M/d/yyyy", "date"))
, u.endDate = date(apoc.date.convertFormat(row.`End Date` , "M/d/yyyy", "date"))
, u.firmOrInter = row.`Rate Schedule`;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/UTIL.csv' AS row
WITH row
WHERE row.`Rec Loc Type` = 'Zone'
MATCH (z:GasZone{pipeline:row.Pipeline , name:row.`Rec Loc ID`})
MATCH (u:GasUtil{id:row.`Rate Definition ID`})
MERGE (u)-[:HAS_RECEIPT]->(z);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/UTIL.csv' AS row
WITH row
WHERE row.`Del Loc Type` = 'Zone'
MATCH (z:GasZone{pipeline:row.Pipeline , name:row.`Del Loc ID`})
MATCH (u:GasUtil{id:row.`Rate Definition ID`})
MERGE (u)-[:HAS_DELIVERY]->(z);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/UTIL.csv' AS row
WITH row
WHERE row.`Rec Loc Type` = 'Location'
MATCH (g:GasLocation{id:row.`Rec Loc ID`})
MATCH (u:GasUtil{id:row.`Rate Definition ID`})
MERGE (u)-[:HAS_RECEIPT]->(g);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/UTIL.csv' AS row
WITH row
WHERE row.`Del Loc Type` = 'Location'
MATCH (g:GasLocation{id:row.`Del Loc ID`})
MATCH (u:GasUtil{id:row.`Rate Definition ID`})
MERGE (u)-[:HAS_DELIVERY]->(g);

//MARK: GASFUEL
CREATE CONSTRAINT GasFuel_uniq_id IF NOT EXISTS FOR (f:GasFuel) REQUIRE f.id IS unique;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/FUEL.csv' AS row
WITH row
MERGE (f:GasFuel{id:row.`Rate Definition ID`})
SET 
  f.rate = toFloat(row.Rate)
, f.startDate = date(apoc.date.convertFormat(row.`Start Date` , "M/d/yyyy", "date"))
, f.endDate = date(apoc.date.convertFormat(row.`End Date` , "M/d/yyyy", "date"))
, f.firmOrInter = row.`Rate Schedule`;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/FUEL.csv' AS row
WITH row
WHERE row.`Rec Loc Type` = 'Location'
MATCH (g:GasLocation{id:row.`Rec Loc ID`})
MATCH (f:GasFuel{id:row.`Rate Definition ID`})
MERGE (f)-[:HAS_RECEIPT]->(g);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/FUEL.csv' AS row
WITH row
WHERE row.`Del Loc Type` = 'Location'
MATCH (g:GasLocation{id:row.`Del Loc ID`})
MATCH (f:GasFuel{id:row.`Rate Definition ID`})
MERGE (f)-[:HAS_DELIVERY]->(g);

//MARK: GasLocationType
CREATE CONSTRAINT GasLocationType_uniq_id IF NOT EXISTS FOR (f:GasLocationType) REQUIRE f.id IS unique;

MERGE (r:GasLocationType{id:'RECEIPT'})
MERGE (d:GasLocationType{id:'DELIVERY'})
MERGE (s:GasLocationType{id:'STORAGE'});

MATCH (p:GasLocation)
WHERE p.id IN [
    'EMERSON'
    , 'DEWARD'
    , 'SOUTH CHESTER'
    , 'FARWELL'
    , 'MUTTONVILLE'
    , 'ST.CLAIR'
    , 'BELLE RIVER MILLS'
]
MATCH (type:GasLocationType{id:'RECEIPT'})
MERGE (p)-[:HAS_TYPE]->(type);

MATCH (p:GasLocation)
WHERE p.id IN [
    'EMERSON'
    , 'BEMIDJI'
    , 'CARLTON'
    , 'CLOQUET'
    , 'DULUTH'
    , 'WAKEFIELD'
    , 'FORTUNE LAKE'
    , 'CRYSTAL FALLS'
    , 'RAPID RIVER'
    , 'SAULT STE MARIE'
    , 'GAYLORD'
    , 'ALPINE'
    , 'DEWARD'
    , 'FARWELL'
    , 'CHIPPEWA'
    , 'MIDLAND'
    , 'BIRCH RUN'
    , 'MUTTONVILLE'
    , 'ST.CLAIR'
    , 'BELLE RIVER MILLS'
    , 'TRUMBLE RD.'
]
MATCH (type:GasLocationType{id:'DELIVERY'})
MERGE (p)-[:HAS_TYPE]->(type);

MATCH (p:GasLocation)
WHERE p.id IN [
    'SOUTH CHESTER'
    , 'DEWARD'
    , 'CHIPPEWA'
    , 'BELLE RIVER MILLS'
]
MATCH (type:GasLocationType{id:'STORAGE'})
MERGE (p)-[:HAS_TYPE]->(type);

//MARK: GasPrice
CREATE CONSTRAINT GasPrice_uniq_id IF NOT EXISTS FOR (p:GasPrice) REQUIRE p.id IS unique;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/GQ16/KAES-Neo4j/refs/heads/main/KES/PriceData.csv' AS row
WITH row, date(apoc.date.convertFormat(row.Date,"M/d/yyyy",'date')) AS formattedDate
WITH row, formattedDate, row.Location + "|" + formattedDate AS uniqueKey
MATCH (l:GasLocation{id:row.Location})
MERGE (p:GasPrice{id:uniqueKey})
MERGE (l)-[:HAS_PRICE]->(p)
SET p.price = toFloat(row.Price)
, p.date = formattedDate
, p.contract = row.Contract
, p.update_date = datetime()
;