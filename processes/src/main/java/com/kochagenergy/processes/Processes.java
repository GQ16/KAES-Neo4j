package com.kochagenergy.processes;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;

/**
 *
 * @author gehadq
 */
public class Processes {

    static int counter = 0;
    static final String dbUri = "neo4j+s://neo4j.data-services-dev.kaes.io";
    static final String dbUser = "gehad_qaki";
    static final String dbPassword = "frog-robin-jacket-halt-swim-7015";
    static String product = "UAN";

    public static void main(String[] args) {


        try (var driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword))) {
            driver.verifyConnectivity();
            System.out.println("Connection established.");

            try (Session session = driver.session()) {
                Map<String, Integer> locationHopsMap = new HashMap<>();
                session
                    .run("""
                        MATCH (mo:Mode)<-[:HAS_INBOUND]-(dl:Location)-[:HAS_OCCUPANT]->()-[cs:CAN_STORE]->(lpg:LogisticsProductGroup)
                        WHERE lpg.name = $product 
                        AND mo.id = 'RAIL'

                        RETURN DISTINCT dl.id AS locationId, CASE WHEN dl.threeLegsAllowed THEN 2 ELSE 1 END AS qppMax
                        """, Values.parameters("product", product))
                    .forEachRemaining(record -> {
                        locationHopsMap.put(
                            record.get("locationId").asString()
                            , record.get("qppMax").asInt()
                        );
                    });

                System.out.println("Location Count: " + locationHopsMap.size());
                locationHopsMap.forEach((locationId, qppMax) -> {
                    session.run(getCacheQueryString(product, qppMax), 
                        Values.parameters(
                            "locationId", locationId
                            , "product", product
                            , "uom", "ST"
                            , "currency", "USD")
                    );
                    counter++;
                    System.out.println("Compelted: " + String.valueOf(counter) + "/" + locationHopsMap.size());
                });
            } catch (ClientException e) {
                e.printStackTrace();
            }

            driver.close();
        }
    }

    private static String getCacheQueryString(String product, Integer qppMax) {
        return String.format("""
        MATCH (mo:Mode)<-[:HAS_INBOUND]-(dl:Location)-[:HAS_OCCUPANT]->()-[cs:CAN_STORE]->(lpg:LogisticsProductGroup)
        WHERE lpg.name = $product 
        AND mo.id = 'RAIL' 
        AND dl.id = $locationId
        
        MATCH (rr2:RailRoute)-[:`%s_TO`]->(:RailStation|StationGroup)-[:IN_SPLC]->(s2:SPLC)<-[:IN_SPLC]-(dl)
        WHERE (rr2)-[:HAS_DESTINATION_CARRIER]->()<-[:SERVED_BY]-(dl)
        
        MATCH (ol:Location)-[:HAS_OCCUPANT]->(occ:Koch|Competitor)
        , (occ)-[:COMPETES_IN]->(ds:StateOrProvince)<-[:IN_STATE]-(dl)
        , (occ)-[:CAN_STORE]->(lpg)
        , (ol)-[:HAS_OUTBOUND]->(mo)

        MATCH path = (rr2)(
            ()-[:`%s_FROM`]->(stop2)-[:AT_INTERCHANGE]->(interchange)<-[:AT_INTERCHANGE]-(stop1)<-[:`%s_TO`]-()
        ){0, %d}(rr1:RailRoute)-[:`%s_FROM`]->()-[:IN_SPLC]->(s1:SPLC)<-[:IN_SPLC]-(ol)
        WHERE (rr1)-[:HAS_ORIGIN_CARRIER]->()<-[:SERVED_BY]-(ol)

        WITH DISTINCT ol, dl, lpg
        , reverse([r IN nodes(path) WHERE r:RailRoute]) AS routes
        , reverse([s2] + [x IN nodes(path) WHERE x:SPLC]) AS splcList
        WHERE 1=1
        AND all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())

        //Start: Retrieving shortest paths using weird logic because ALL SHORTEST PATHS syntax doesn't
        //       with QPP's yet
        WITH ol, dl, lpg, [size(routes), routes, splcList] AS pathInfoArray, size(routes) AS numberOfRoutes
        ORDER BY numberOfRoutes

        WITH ol, dl, lpg, collect(pathInfoArray) AS pathInfoArrays, min(numberOfRoutes) AS minNumOfRoutes
        WITH ol, dl, lpg, [p IN pathInfoArrays WHERE p[0] = minNumOfRoutes] AS shortestPathInfoArrays, minNumOfRoutes
        UNWIND shortestPathInfoArrays AS shortestPathInfoArray
        WITH ol, dl, lpg, shortestPathInfoArray[1] AS routes, shortestPathInfoArray[2] AS splcList, minNumOfRoutes AS numberOfRoutes
        //End: Retrieving shortest paths by od pair
        
        CALL {
                WITH routes, numberOfRoutes
                WITH routes, numberOfRoutes
                WHERE numberOfRoutes = 1
                WITH routes[0] AS route1
                MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
                WHERE 1=1
                AND NOT rate1.rule_11_at_origin_required
                AND NOT rate1.rule_11_at_destination_required
                AND rate1.rate_effective <= date()
                AND (date() <= rate1.rate_expiration OR rate1.rate_expiration IS NULL)
                RETURN [rate1] AS rates
            UNION
                WITH routes, numberOfRoutes
                WITH routes, numberOfRoutes
                WHERE numberOfRoutes = 2
                WITH routes[0] AS route1, routes[1] AS route2
                MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
                MATCH (route2)-[:HAS_CURRENT_RATE]->(rate2:RailRate)
                WHERE 1=1
                AND rate1.rule_11_at_destination_allowed 
                AND NOT rate1.rule_11_at_origin_required 
                AND rate2.rule_11_at_origin_allowed 
                AND NOT rate2.rule_11_at_destination_required
                AND rate1.rate_effective <= date()
                AND (date() <= rate1.rate_expiration OR rate1.rate_expiration IS NULL)
                AND rate2.rate_effective <= date()
                AND (date() <= rate2.rate_expiration OR rate2.rate_expiration IS NULL)				
                AND rate1.min_cars = 1
                AND rate2.min_cars = 1
                AND ((rate1.car_owner_shipper = rate2.car_owner_shipper) OR (rate1.car_owner_carrier = rate2.car_owner_carrier))
                RETURN [rate1, rate2] AS rates
            UNION
                WITH routes, numberOfRoutes
                WITH routes, numberOfRoutes
                WHERE numberOfRoutes = 3
                WITH routes[0] AS route1, routes[1] AS route2, routes[2] AS route3
                MATCH (route1)-[:HAS_CURRENT_RATE]->(rate1:RailRate)
                MATCH (route2)-[:HAS_CURRENT_RATE]->(rate2:RailRate)
                MATCH (route3)-[:HAS_CURRENT_RATE]->(rate3:RailRate)
                WHERE 1=1
                AND rate1.rule_11_at_destination_allowed 
                AND NOT rate1.rule_11_at_origin_required 
                AND rate2.rule_11_at_destination_allowed 
                AND rate2.rule_11_at_origin_allowed 
                AND rate3.rule_11_at_origin_allowed 
                AND NOT rate3.rule_11_at_destination_required
                AND rate1.rate_effective <= date()
                AND (date() <= rate1.rate_expiration OR rate1.rate_expiration IS NULL)
                AND rate2.rate_effective <= date()
                AND (date() <= rate2.rate_expiration OR rate2.rate_expiration IS NULL)
                AND rate3.rate_effective <= date()
                AND (date() <= rate3.rate_expiration OR rate3.rate_expiration IS NULL)				
                AND rate1.min_cars = 1
                AND rate2.min_cars = 1
                AND rate3.min_cars = 1
                AND (
                    (rate1.car_owner_shipper = rate2.car_owner_shipper = rate3.car_owner_shipper) 
                    OR (rate1.car_owner_carrier = rate2.car_owner_carrier = rate3.car_owner_carrier)
                )
                RETURN [rate1, rate2, rate3] AS rates
        }
        
        WITH ol, dl, routes, splcList, rates, lpg
        , COLLECT{
            WITH apoc.coll.pairsMin(splcList) AS splcPairs, routes, range(1, numberOfRoutes) AS rowNums
            WITH splcPairs, apoc.coll.zip(rowNums,routes) AS rowRoutes
            WITH apoc.coll.zip(splcPairs,rowRoutes) AS splcPairRoutes
            UNWIND splcPairRoutes AS splcPairRoute
            WITH splcPairRoute[0] AS splcPair, splcPairRoute[1] AS rowRoute
            WITH rowRoute[0] AS rowNum, rowRoute[1] AS route, splcPair[0] AS originSPLC, splcPair[1] AS destSPLC

            OPTIONAL MATCH (originSPLC)<-[:FROM_SPLC]-(r:RailMileage)-[:TO_SPLC]->(destSPLC)

            WITH rowNum, route, coalesce(r.distance,0) AS dist
            
            , EXISTS{
                (route)-[:HAS_ORIGIN_CARRIER]->(:Carrier)<-[:FROM_CARRIER]-(r)
            } AS origCarrierMatches
            
            , EXISTS{
                (r)-[:TO_CARRIER]->(:Carrier)<-[:HAS_DESTINATION_CARRIER]-(route)
            } AS destCarrierMatches

            WITH route, dist
            , CASE 
                WHEN origCarrierMatches AND destCarrierMatches THEN 1
                WHEN origCarrierMatches OR destCarrierMatches THEN 2
                ELSE 3
            END AS mileageScore
            ORDER BY rowNum, mileageScore //Must order by rowNum to preserve route order
            
            WITH route, collect(dist)[0] AS selectedDist
            RETURN selectedDist
        } AS dist
        
        , COLLECT{
            UNWIND routes AS route
            MATCH (route)-[:HAS_CARRIER]->(ca:Carrier)
            OPTIONAL MATCH (ca)-[:HAS_CURRENT_FSC]->(rFSC:RailFSC)<-[:FOR_RAIL_FSC]-(fC:Currency)-[:FOR_RAIL_ROUTE]->(route)
            , (rFSC)-[:FOR_PRODUCTGROUP]->(lpg)

            RETURN {
                carrier: ca.id
                , baseFuel: coalesce(rFSC.rate, 0.0)
                , rate: coalesce(round(rFSC.rate/lpg.railCarVol, 6), 0.0)
            }
        } AS fuels
        
        WITH DISTINCT ol, dl, fuels, lpg, dist
        , [s IN splcList| coalesce(s.r260,s.id)] AS routeSplcs
        , [s IN splcList| s.id] AS splcs
        , [r IN rates| {
            baseRate: r.rate,
            rateType: r.uom,
            carVol: lpg.railCarVol,
            perTonRate: CASE toLower(r.uom)
                WHEN 'ton' THEN r.rate
                WHEN 'car' THEN round(r.rate / lpg.railCarVol, 3)
                ELSE 0
            END,
            currency: r.currency,
            exchRate: CASE 
                WHEN r.currency = $currency THEN 1 
                WHEN r.currency = 'USD' AND $currency = 'CAD' THEN 1.36
                WHEN r.currency = 'CAD' AND $currency = 'USD' THEN 0.73
            END,
            carrier: r.carrier,
            route: r.route,
            minCars: r.min_cars,
            carOwner: CASE 
                WHEN r.car_owner_shipper AND NOT r.car_owner_carrier 
                    THEN 'PVT'
                WHEN NOT r.car_owner_shipper AND r.car_owner_carrier
                    THEN 'RR'
                WHEN r.car_owner_shipper AND r.car_owner_carrier 
                    THEN 'RR/PVT'
                ELSE 'OTHER'
            END,
            expiration: r.rate_expiration
        }] AS rateMaps,
        CASE $uom
            WHEN 'ST' THEN 1
            WHEN 'MT' THEN 0.892857
            WHEN 'GAM' THEN 302.114803
        END AS uomConvRate


        WITH ol, dl, splcs, lpg,
        [x IN range(0,size(rateMaps)-1)|
            {
                rate: rateMaps[x].perTonRate * rateMaps[x].exchRate / uomConvRate
                , carrier: fuels[x].carrier
                , fsc: fuels[x].rate * rateMaps[x].exchRate / uomConvRate
                , fscRate: fuels[x].baseFuel * rateMaps[x].exchRate / uomConvRate
                , dist: dist[x]
                , route: rateMaps[x].route
                , exp: rateMaps[x].expiration
                , minCars: rateMaps[x].minCars
                , carOwner: rateMaps[x].carOwner
            }
        ] AS legs
        
        
        WITH ol, dl, legs[0].minCars AS minCars, legs[0].carOwner AS carOwner, legs, lpg, splcs
        , round(reduce(price = 0, x IN legs | price + ((x.rate + (x.fsc * x.dist)))),4) AS freight
        , round(reduce(dist = 0, m IN legs | dist + m.dist),0) AS totalDist
        ORDER BY freight
        
        
        WITH minCars, ol, dl, lpg
        , collect(splcs)[0] AS splcs
        , collect(legs)[0] AS legs
        , collect(freight)[0] AS freight
        , collect(carOwner)[0] AS carOwner
        , collect(totalDist)[0] AS totalDist
        , CASE WHEN $product = 'METHANOL' AND $uom = 'GAM' THEN 0.035 ELSE 0 END AS methanolRailLeaseFee


        WITH ol
        , dl
        , lpg
        , minCars
        , methanolRailLeaseFee AS fees
        , round((freight + methanolRailLeaseFee)/totalDist, 4) AS rate
        , 'MI' AS rateFactor
        , round(freight + methanolRailLeaseFee, 4) AS freight
        , legs
        , splcs
        
        
        WITH ol
        , dl
        , lpg
        , {
            id: ol.id + '|#|' + dl.id + '|#|' + lpg.name + '|#|' + minCars
            , splcs			: splcs
            , fees			: fees
            , rate			: rate
            , rateFactor	: rateFactor
            , freight		: freight
            , curr			: $currency
            , rateUom		: $uom
            , distUom		: 'MI'
            , fscRateUom	: '/' + $uom + '/MI'
            
            , rates			: [x IN legs|x.rate]
            , carriers		: [x IN legs|x.carrier]
            
            , rate1			: legs[0].rate
            , carrier1		: legs[0].carrier
            , fsc1			: legs[0].fsc
            , fscRate1		: legs[0].fscRate
            , dist1			: legs[0].dist
            , route1		: legs[0].route
            , exp1			: legs[0].exp
            , minCars1		: legs[0].minCars
            , carOwner1		: legs[0].carOwner

            , rate2			: legs[1].rate
            , carrier2		: legs[1].carrier
            , fsc2			: legs[1].fsc
            , fscRate2		: legs[1].fscRate
            , dist2			: legs[1].dist
            , route2		: legs[1].route
            , exp2			: legs[1].exp
            , minCars2		: legs[1].minCars
            , carOwner2		: legs[1].carOwner

            , rate3			: legs[2].rate
            , carrier3		: legs[2].carrier
            , fsc3			: legs[2].fsc
            , fscRate3		: legs[2].fscRate
            , dist3			: legs[2].dist
            , route3		: legs[2].route
            , exp3			: legs[2].exp
            , minCars3		: legs[2].minCars
            , carOwner3		: legs[2].carOwner
        } AS cacheProperties
        
        MERGE (rc:RailCache{id:cacheProperties.id})
        SET rc = cacheProperties
        SET rc.last_update_date = datetime()

        WITH rc, lpg, dl, ol
        MERGE (lpg)-[:FOR_RAIL_CACHE]->(rc)
        MERGE (rc)-[:HAS_DESTINATION]->(dl)
        MERGE (rc)-[:HAS_ORIGIN]->(ol)
        """, product, product, product, qppMax, product);
    }
}
