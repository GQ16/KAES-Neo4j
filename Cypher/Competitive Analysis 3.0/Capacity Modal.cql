MATCH (l2:Location)
OPTIONAL MATCH (l2)-[r2:HAS_PRODUCTGROUP]->(pg2:ProductGroup)
OPTIONAL MATCH (pg2)<-[:HAS_CHILD]-(pf2:ProductForm)

WITH l2,collect({product:pg2.id, form:coalesce(pf2.id,pg2.id), qty:r2.qty}) AS capacity
WITH l2,
CASE WHEN capacity[0].product IS NOT NULL 
THEN capacity
ELSE [] END AS capacities

OPTIONAL MATCH (l2)-[:HAS_TENANT]->(t:Tenant)<-[:IS_TENANT]-(p:Party)
OPTIONAL MATCH (t)-[r:HAS_PRODUCTGROUP]->(pg:ProductGroup)
OPTIONAL MATCH (pg)<-[:HAS_CHILD]-(pf:ProductForm)
OPTIONAL MATCH (t)-[:PART_OF_MARKET]->(m:Market)

WITH capacities, p.id AS name, t.id AS id, t.type AS type, m.id AS market,
collect({
    product: pg.id,
    form: coalesce(pf.id, pg.id),
    qty: r.qty
}) AS tenantCap

WITH capacities, name, id, type, market,
CASE WHEN tenantCap[0].product IS NOT NULL 
THEN tenantCap
ELSE [] END AS tenantCaps

RETURN capacities,
CASE WHEN id IS NOT NULL 
THEN collect({
        name:name,
        id:id,
        type:type,
        market:market,
        capacities:tenantCaps
    }) 
ELSE [] END AS tenants