//Point pairs table w/ util & fuel
MATCH path = (startPoint:Location)(
    (:Location)<-[]-(:Segment)-[]->(:Location)
)+(endPoint:Location)
// WHERE startPoint.id = 'CARLTON'
// AND endPoint.id = 'FARWELL'
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(u1:Util)-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(u2:Util)-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u3:Util)-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u4:Util)-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(f:Fuel)-[:HAS_DELIVERY]->(endPoint)

WITH DISTINCT startPoint, endPoint, coalesce(u1,u2,u3,u4) AS UtilNode, f
WHERE f.firmOrInter = UtilNode.firmOrInter
RETURN startPoint.id AS startPoint, endPoint.id AS endPoint, UtilNode.rate AS util, f.rate AS fuel, f.firmOrInter AS firmOrInter
;

//Start -> End Point
MATCH path = (startPoint:Location)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:Location)
WHERE startPoint.id = 'MAYFIELD'
AND endPoint.id = 'CASS LAKE'
RETURN path
;

//Start -> End Point AS List of Location names
MATCH path = (startPoint:Location)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:Location)
WHERE startPoint.id = 'EMERSON'
AND endPoint.id = 'ST.CLAIR'
RETURN [x IN nodes(path) WHERE x:Location | x.id] AS path
;

//Start -> End Point w/ util & fuel
MATCH path = (startPoint:Location)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:Location)
WHERE startPoint.id = 'CARLTON'
AND endPoint.id = 'FARWELL'
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(u1:Util)-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(u2:Util)-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u3:Util)-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u4:Util)-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(f:Fuel)-[:HAS_DELIVERY]->(endPoint)

RETURN path, coalesce(u1,u2,u3,u4) AS UtilNode, f
;

//Nodes Path Example
MATCH path = (startPoint:Location)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:Location)
WHERE startPoint.id = 'EMERSON'
AND endPoint.id = 'CARLTON'

MATCH path6 = (startPoint)-[:PART_OF_INDEX]->(si:Index)-[:HAS_PRICE]->(sp:Price)
WHERE 1 = 1
AND sp.date.year = date($userDate).year
AND sp.date.month = date($userDate).month

MATCH path7 = (endPoint)-[:PART_OF_INDEX]->(ei:Index)-[:HAS_PRICE]->(ep:Price)
WHERE 1 = 1
AND ep.date.year = date($userDate).year
AND ep.date.month = date($userDate).month

OPTIONAL MATCH path1 = (startPoint)<-[:HAS_RECEIPT]-(u1:Util{firmOrInter:$firmOrInter})-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH path2 = (startPoint)<-[:HAS_RECEIPT]-(u2:Util{firmOrInter:$firmOrInter})-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH path3 = (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u3:Util{firmOrInter:$firmOrInter})-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH path4 = (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u4:Util{firmOrInter:$firmOrInter})-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH path5 = (startPoint)<-[:HAS_RECEIPT]-(f:Fuel{firmOrInter:$firmOrInter})-[:HAS_DELIVERY]->(endPoint)

WITH DISTINCT startPoint, endPoint, coalesce(u1,u2,u3,u4) AS UtilNode, f, sp, ep
WHERE f.firmOrInter = UtilNode.firmOrInter
RETURN startPoint.id AS startPoint
, endPoint.id AS endPoint
, UtilNode.rate AS util
, f.rate AS fuel
, f.firmOrInter AS firmOrInter
, sp.price AS startPrice
, ep.price AS endPrice
;