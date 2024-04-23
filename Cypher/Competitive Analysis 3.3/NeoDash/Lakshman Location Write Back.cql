//Location Write back
WITH
    {
        Location: $facility_name,
        Party: $facility_owner,
        Address: $Address,
        City: $City,
        State: $State,
        Zip: $Zip,
        Country: $Country,
        Latitude: $Latitude,
        Longitude: $Longitude,
        UOM: $UOM,
        Occupant: [
            {
                occupant_name: $occupant_name,
                State: $state,
                Offer: $offer,
                ProductGroup: [
                    {
                        product_group_name: $product_group_name,
                        qty: $qty
                    }
                ]
            },
            {
                occupant_name: $occupant_name,
                State: $state,
                Offer: $offer,
                ProductGroup: [
                    {
                        product_group_name: $product_group_name,
                        qty: $qty
                    },
                    {
                        product_group_name: $product_group_name,
                        qty: $qty
                    }
                ]
            }
        ],
        Mode: [
            {
                mode_name: $mode_name,
                on_off: $on_off, //Needs to be true or false
                inbound: $inbound, //Needs to be true or false
                outbound: $outbound //Needs to be true or false
            },
            {
                mode_name: $mode_name,
                provider: [$provider1, $provider2],
                on_off: $on_off, //Needs to be true or false
                inbound: $inbound, //Needs to be true or false
                outbound: $outbound //Needs to be true or false
            }
        ]
    } as evnt

MERGE (l:Location {name: evnt.Location})
SET l.name = evnt.Location
    ,l.address = evnt.Address
    ,l.coordinate = point({latitude:toFloatOrNull(evnt.Latitude),longitude:toFloatOrNull(evnt.Longitude)})

WITH l, evnt
//Party Rel (Location Owner)
CALL{
    WITH l, evnt
    OPTIONAL MATCH (l)<-[r:OWNS_LOCATION]-(:Party) DELETE r
    //someone could add an owner and then remove it, hence the order
    WITH l, evnt
    MATCH (p:Party {name: evnt.Party})
    WITH l, p
  
    WITH l, p
    MERGE (l)<-[r2:OWNS_LOCATION]-(p)
}
//City & State Rel
CALL{
    WITH l, evnt
    MATCH (c:CityState {id: evnt.City})-[:IN_STATE]->(s:StateOrProvince)
    WITH l, c, s
    
    OPTIONAL MATCH (l)-[r:IN_CITY]->(:CityState) DELETE r
    WITH l, c, s
    OPTIONAL MATCH (l)-[r1:IN_STATE]->(:StateOrProvince) DELETE r1

    WITH l, c, s
    
    MERGE (s)<-[:IN_STATE]-(l)-[r:IN_CITY]->(c)
}
//Country Rel
CALL{
    WITH l, evnt
    MATCH (c:Country {id: evnt.Country})
    WITH l, c
    
    OPTIONAL MATCH (l)-[r:IN_COUNTRY]->(:Country) DELETE r
    WITH l, c

    MERGE (l)-[:IN_COUNTRY]->(c)
}
//Zip Rel
CALL{
    WITH l, evnt
    MATCH (z:ZipCode {id: evnt.Zip})
    WITH l, z, evnt

    OPTIONAL MATCH (l)-[r:IN_ZIPCODE]->(:ZipCode) DELETE r
    
    WITH l, z, evnt
    
    MERGE (l)-[r:IN_ZIPCODE]->(z)
    //SET l.latitude = z.latitude
    //, l.longitude = z.longitudeWITH l,z,
    SET l.coordinate =
    CASE 
      WHEN (isEmpty(evnt.Latitude) or isEmpty(evnt.Longitude)) or evnt.Latitude is null or evnt.Longitude is null
      THEN z.coordinate
      ELSE point({
		latitude:toFloatOrNull(evnt.Latitude),
		longitude:toFloatOrNull(evnt.Longitude)
		})
    END
}
//MassUOM Rel
CALL{
    WITH l, evnt
    MATCH (u:MassUOM {id: evnt.UOM})
    WITH l, u

    OPTIONAL MATCH (l)-[r:HAS_MASS_UOM]->(:MassUOM) DELETE r

    WITH l, u

    MERGE (l)-[r:HAS_MASS_UOM]->(u)
}
//Modes Rel
CALL{
    WITH l, evnt
    WITH l, evnt.Mode as modes
    UNWIND modes as md
    CALL{
        WITH l, md
        MATCH (m:Mode {id: md.mode_name})
        WITH l, m, md

        OPTIONAL MATCH (l)-[r:HAS_INBOUND]->(m)
        DELETE r

        WITH l, m, md
        WHERE md.inbound = true and md.on_off = true
        MERGE (l)-[r1:HAS_INBOUND]->(m)
        SET r1.update_date = datetime()
    }
    //Outbound Mode Rel
    CALL{
        WITH l, md
        
        MATCH (m:Mode {id: md.mode_name})
        WITH l, m, md

        OPTIONAL MATCH (l)-[r:HAS_OUTBOUND]->(m)
        DELETE r

        WITH l, m, md
        WHERE md.outbound = true and md.on_off = true
        MERGE (l)-[r1:HAS_OUTBOUND]->(m)
        SET r1.update_date = datetime()
    }
    //Carrier Rel
    CALL {
        WITH l, md
        WITH l, md

        WHERE NOT isEmpty(md.provider) or md.provider IS NOT NULL
        UNWIND md.provider as pr

        MATCH (c:Carrier {id: pr})
        WITH l, c

        OPTIONAL MATCH (l)<-[r:PROVIDES_SERVICE]-(:Carrier) DELETE r

        WITH l, c

        MERGE (l)<-[r2:PROVIDES_SERVICE]-(c)
    }
}
//Occupants Rel
CALL {
    WITH l, evnt
    WITH l, evnt.Occupant AS occupants
    UNWIND occupants as occ

    with l, occ

    MATCH (p:Party {name: occ.occupant_name})

    WITH l, p, occ, l.name + p.id as occ_id

    //Occupant Node
    OPTIONAL MATCH (l)-[:HAS_OCCUPANT]->(o:Occupant) DETACH DELETE o

    WITH l, p, occ, l.name + p.id as occ_id

    MERGE (t:Occupant {id: occ_id})
    SET t.update_date = datetime()
    MERGE (p)-[:IS_OCCUPANT]->(t)<-[:HAS_OCCUPANT]-(l)

    WITH l, p, occ, t,
        CASE WHEN  occ.offer = true then ["Customer", "Competitor"] else ["Customer"] end as type
    CALL apoc.create.addLabels(t,type) YIELD node

    WITH l, p, occ, node as t
    
    //State Relationship
    CALL {
        WITH t, occ
    	WITH t, occ
        UNWIND occ.State as st
        with t, st

    	WHERE NOT isEmpty(st) and st IS NOT NULL
        MATCH (m:StateOrProvince {id: st})
        WITH t, m

        OPTIONAL MATCH (t)-[r:COMPETES_IN]->(:StateOrProvince) DELETE r

        WITH t, m

        MERGE (t)-[r2:COMPETES_IN]->(m)
    }
    //LPG Rel
    CALL{
        WITH t, occ
    	WITH t, occ.ProductGroup AS prds
        UNWIND prds as prd
        MATCH (lpg:LogisticsProductGroup {name: prd.product_group_name})
        WITH t, lpg, prd

        OPTIONAL MATCH (:LogisticsProductGroup)<-[r:HAS_PRODUCTGROUP]-(t) DELETE r

        WITH t, lpg, prd

        MERGE (lpg)<-[r2:HAS_PRODUCTGROUP]-(t)
        SET r2.qty = toFLoatOrNull(prd.qty)
    }
}
return *;