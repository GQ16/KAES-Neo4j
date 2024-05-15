MATCH (m:MPI)-[:FOR_MODE]->(mo:Mode)
, (m)-[:FOR_OCCUPANT]->(o:Occupant)
, (o)<-[:IS_OCCUPANT]-(p:Party)
, (m)-[:FOR_PRODUCTGROUP]->(lpg:LogisticsProductGroup)
, (m)-[:FOR_ORIGIN]->(ol:Location)
, (co:ContractOwner)-[:LOGGED]->(m)
, (p2:Party)-[:PROVIDED]->(m)
, (con:Contact)-[:PROVIDED]->(m)

WHERE ol.id = $location_id
AND lpg.name = $product_name
AND m.startDate <= date($ship_date) < m.endDate
AND mo.id = $mode

OPTIONAL MATCH (m)-[fd:FOR_DESTINATION]->(dl:Location)

RETURN  date(m.createdDate) AS dateLogged,
mo.id AS mode,
CASE WHEN m.type = 'DESTINATION' THEN 'DLVD' ELSE 'FOB' END AS fobOrDlvd,
dl.name AS destination,
CASE 
    WHEN m.type = 'DESTINATION' AND m.singularPrice IS NOT NULL
    THEN m.singularPrice-m.freight 
    WHEN m.type = 'DESTINATION' AND m.singularPrice IS NULL
    THEN ((m.minPrice+m.maxPrice)/2) - m.freight
    WHEN m.type = 'ORIGIN' AND m.singularPrice IS NOT NULL
    THEN m.singularPrice
    WHEN m.type = 'ORIGIN' AND m.singularPrice IS NULL
    THEN (m.minPrice+m.maxPrice)/2
END AS price,
p.name AS competitor,
p2.name AS account,
con.id AS contact,
co.name AS marketer,
m.startDate AS startDate,
m.endDate AS endDate

ORDER BY m.createdDate