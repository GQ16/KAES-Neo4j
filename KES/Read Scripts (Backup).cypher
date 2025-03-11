//Point pairs table w/ util & fuel
MATCH path = (startPoint:GasLocation)(
    (:GasLocation)<-[]-(:Segment)-[]->(:GasLocation)
)+(endPoint:GasLocation)
// WHERE startPoint.id = 'CARLTON'
// AND endPoint.id = 'FARWELL'
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(u1:GasUtil)-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(u2:GasUtil)-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u3:GasUtil)-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u4:GasUtil)-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(f:GasFuel)-[:HAS_DELIVERY]->(endPoint)

WITH DISTINCT startPoint, endPoint, coalesce(u1,u2,u3,u4) AS GasUtilNode, f
WHERE f.firmOrInter = GasUtilNode.firmOrInter
RETURN startPoint.id AS startPoint, endPoint.id AS endPoint, GasUtilNode.rate AS util, f.rate AS fuel, f.firmOrInter AS firmOrInter
;

//Start -> End Point
MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'MAYFIELD'
AND endPoint.id = 'CASS LAKE'
RETURN path
;

//Start -> End Point w/ util & fuel
MATCH path = (startPoint:GasLocation)(
    ()<-[:CONNECTS]-()-[:CONNECTS]->()
)+(endPoint:GasLocation)
WHERE startPoint.id = 'CARLTON'
AND endPoint.id = 'FARWELL'
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(u1:GasUtil)-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(u2:GasUtil)-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u3:GasUtil)-[:HAS_DELIVERY]->(endPoint)
OPTIONAL MATCH (startPoint)-[:PART_OF_ZONE]->()<-[:HAS_RECEIPT]-(u4:GasUtil)-[:HAS_DELIVERY]->()<-[:PART_OF_ZONE]-(endPoint)
OPTIONAL MATCH (startPoint)<-[:HAS_RECEIPT]-(f:GasFuel)-[:HAS_DELIVERY]->(endPoint)

RETURN path, coalesce(u1,u2,u3,u4) AS GasUtilNode, f
;