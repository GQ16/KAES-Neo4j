// Location Auto Fill
MATCH (l:Location {name: $facility_name})
MATCH (l)<-[:OWNS_LOCATION]-(p:Party)
MATCH (l)-[:IN_CITY]->(c:CityState)
MATCH (l)-[:IN_STATE]->(s:StateOrProvince)
MATCH (l)-[:IN_COUNTRY]->(co:Country)
MATCH (l)-[:IN_ZIPCODE]->(z:ZipCode)
MATCH (l)-[:HAS_MASS_UOM]->(u:MassUOM)
MATCH (l)-[:HAS_OCCUPANT]-(o:Occupant)
MATCH (o)<-[:IS_OCCUPANT]-(po:Party)
MATCH (o)-[r:HAS_PRODUCTGROUP]->(lpg:LogisticsProductGroup)
MATCH (o)-[:COMPETES_IN]->(st:StateOrProvince)
MATCH (l)-[:PROVIDES_SERVICE]->(cr:Carrier)
OPTIONAL MATCH (l)-[bt]->(m:Mode)
//OPTIONAL MATCH (l)-[ob:HAS_OUTBOUND]->(mo:Mode)
WITH l.name as facility_name,
    l.coordinate as coordinate,
    l.address as address,
    p.name as facility_owner,
    c.city as city,
    s.id as state,
    co.id as country,
    z.id as zipcode,
    u.id as MassUOM,
    m.id as mode_name,
    po.name as occ_name, // agg
    collect(distinct {product_group_name : lpg.name, qty: r.qty}) as productgroup,// agg o
    collect(distinct st.id) as competing_state, // agg o
    collect(distinct case when m.id = "RAIL" then cr.name end) as carrier,
    case when "Competitor" in labels(o) then true else false end as offer,
    case when "HAS_INBOUND" in collect(distinct type(bt)) then true else false end as inbound_flg,
    case when "HAS_OUTBOUND" in collect(distinct type(bt)) then true else false end as outbound_flg
WITH facility_name,
    coordinate,
    address,
    facility_owner,
    city,
    state,
    country,
    zipcode,
    MassUOM,
    collect(distinct {occupant_name: occ_name,
                                State: competing_state,
                                Offer: offer,
                                ProductGroup: productgroup
                                }) as occupant,
    collect(distinct {mode_name: mode_name,
                            provider: carrier,
                            on_off: true,
                            inbound: inbound_flg,
                            outbound: outbound_flg
                            }) as modes
    
WITH {Location: facility_name,
    Party: facility_owner,
    Address: address,
    City: city,
    State: state,
    Zip: zipcode,
    Country: country,
    Latitude: coordinate.x,
    Longitude: coordinate.y,
    UOM: MassUOM,
    Occupant: occupant,
    Mode: modes
    } as loc
return *;