package com.kochagenergy.processes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
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
    static final String DB_URI = "neo4j+s://neo4j.data-services-dev.kaes.io";
    static final String DB_USER = "gehad_qaki";
    static final String DB_PASS = "frog-robin-jacket-halt-swim-7015";
    static String product = "UAN";
    static String currency = "USD";
    static String uom = "ST";

    public static void main(String[] args) {
        try (var driver = GraphDatabase.driver(DB_URI, AuthTokens.basic(DB_USER, DB_PASS))) {
            driver.verifyConnectivity();
            System.out.println("Connection established.");

            railCache(driver);
            // truckCache(driver);

            driver.close();
        }
    }
    private static String getRailCacheQueryString(String product, Integer qppMax) {
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

        MATCH (usd:Currency {id:'USD'})-[exch1:HAS_EXCHANGE_RATE]->(cad:Currency {id:'CAD'})
        MATCH (cad)-[exch2:HAS_EXCHANGE_RATE]->(usd)

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
                WHEN r.currency = 'USD' AND $currency = 'CAD' THEN exch1.rate
                WHEN r.currency = 'CAD' AND $currency = 'USD' THEN exch2.rate
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
            WHEN 'MT' THEN 1/1.10231
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
        SET rc.update_date = datetime()

        WITH rc, lpg, dl, ol
        MERGE (lpg)-[:FOR_RAIL_CACHE]->(rc)
        MERGE (rc)-[:HAS_DESTINATION]->(dl)
        MERGE (rc)-[:HAS_ORIGIN]->(ol)
        """, product, product, product, qppMax, product);
    }

    private static String getNewRailCacheQueryString(String product, Integer qppMax) {
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

        MATCH path = SHORTEST 1 (rr2)(
            ()-[:`%s_FROM`]->(stop2)-[:AT_INTERCHANGE]->(interchange)<-[:AT_INTERCHANGE]-(stop1)<-[:`%s_TO`]-()
        ){0, %d}(rr1:RailRoute)-[:`%s_FROM`]->()-[:IN_SPLC]->(s1:SPLC)<-[:IN_SPLC]-(ol)
        WHERE (rr1)-[:HAS_ORIGIN_CARRIER]->()<-[:SERVED_BY]-(ol)

        WITH DISTINCT ol, dl, lpg
        , reverse([r IN nodes(path) WHERE r:RailRoute]) AS routes
        , reverse([s2] + [x IN nodes(path) WHERE x:SPLC]) AS splcList
        WHERE 1=1
        AND all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())
        
        WITH *, size(routes) AS numberOfRoutes
        
        CALL (routes, numberOfRoutes) {
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

        MATCH (usd:Currency {id:'USD'})-[exch1:HAS_EXCHANGE_RATE]->(cad:Currency {id:'CAD'})
        MATCH (cad)-[exch2:HAS_EXCHANGE_RATE]->(usd)

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
                WHEN r.currency = 'USD' AND $currency = 'CAD' THEN exch1.rate
                WHEN r.currency = 'CAD' AND $currency = 'USD' THEN exch2.rate
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
            WHEN 'MT' THEN 1/1.10231
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
        SET rc.update_date = datetime()

        WITH rc, lpg, dl, ol
        MERGE (lpg)-[:FOR_RAIL_CACHE]->(rc)
        MERGE (rc)-[:HAS_DESTINATION]->(dl)
        MERGE (rc)-[:HAS_ORIGIN]->(ol)
        """, product, product, product, qppMax, product);
    }

    private static void railCache(final Driver driver) {
        Map<String, Integer> locationHopsMap = new HashMap<>();
        try (Session session = driver.session()) {
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
        } catch (ClientException e) {
            System.err.println("Error Retrieving Location and QPP Maxes");
            System.err.println(e);
        }
            
        System.out.println("Location Count: " + locationHopsMap.size());
        locationHopsMap.forEach((locationId, qppMax) -> {
            try (Session session = driver.session()) {
                // session.run(getRailCacheQueryString(product, qppMax),
                session.run(getNewRailCacheQueryString(product, qppMax),
                        Values.parameters(
                                "locationId", locationId
                                , "product", product
                                , "uom", uom
                                , "currency", currency)
                );
                counter++;
                System.out.println("Rail Compelted: " + String.valueOf(counter) + "/" + locationHopsMap.size());
            } catch (ClientException e) {
                System.err.println("Error Triggered by Location Id: " + locationId);
                System.err.println("QPP Max Value: " + qppMax);
                System.err.println(e);
            }
        });
    }

    private static void truckCache(final Driver driver) {
        try (Session session = driver.session()) {
            List<String> zips = session.run("""
                    MATCH (:Country{id:'US'})<-[:IN_COUNTRY]-(z:ZipCode)-[:IN_STATE]->(s:StateOrProvince)
                    WHERE (z)-[:TRUCK_DISTANCE_TO]-()
                    AND NOT s.id IN ['PR', 'HI', 'AK']
                    RETURN z.id AS zip ORDER BY zip DESC
                    """)
                .list(record -> record.get(0).asString());
            
            System.out.println("Zip Count: " + zips.size());
            zips.forEach(zip -> {
                session.run("""
                    MATCH (ol:Location)-[:HAS_OCCUPANT]->(occ:Occupant)-[ci:COMPETES_IN]->(ds:StateOrProvince)<-[is2:IN_STATE]-(dz:ZipCode)-[:IN_COUNTRY]->(co:Country)
                    , (ol)-[:IN_GEOGRAPHY]->()<-[:FROM]-(route:TruckRoute)-[:TO]->(co)
                    , (occ)-[:CAN_STORE]->(lpg:LogisticsProductGroup)<-[:FOR_PRODUCTGROUP]-(route)
                    , (ol)-[:HAS_OUTBOUND]->(mo:Mode)

                    WHERE dz.id = $destinationZip
                    AND dz.country = 'US'
                    AND co.id = 'US'
                    AND mo.id = 'TRUCK'
                    AND occ:Koch
                    AND lpg.name = $product

                    MATCH (route)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
                    WHERE cr.shipWindowStartDate <= date() <= cr.shipWindowEndDate

                    MATCH (ol)-[:IN_ZIPCODE]->(oz:ZipCode)-[tdt:TRUCK_DISTANCE_TO]->(dz)
                    WHERE cr.distanceLower <= (CASE WHEN cr.distanceUom = 'KM' THEN tdt.distance * 1.609344 ELSE tdt.distance END) < cr.distanceUpper
                    
                    CALL{
                        WITH lpg, ol
                        MATCH (tFSC:ActiveFSC)-[fp2:FOR_PRODUCTGROUP]->(lpg)
                        , (tFSC)-[:FOR_ORIGIN_COUNTRY]->()<-[:IN_COUNTRY]-(ol)
                        RETURN tFSC
                    }

                    WITH DISTINCT ol, dz, occ, lpg, cr, tFSC
                    , 'MI' AS distUom
                    , tdt.distance AS dist
                    , route.score AS score
                    , route.id AS routeId

                    WITH *
                    ORDER BY score
                    WITH ol, dz, occ, lpg, tFSC, distUom, dist, collect(cr)[0] AS cr

                    WITH ol, dz, occ, lpg, dist, distUom, tFSC.rate AS fsc, cr.currency AS rateCurr, cr.rateUom AS rateUom
                    , cr.ratePerUom AS ratePerUom, cr.loadQuantity AS loadQty, toUpper(cr.rateFactorType) AS rateFactor, cr.shipWindowEndDate AS expirationDate

                    WITH ol, dz, occ, lpg, fsc, dist, distUom, expirationDate, rateCurr, rateUom, ratePerUom, loadQty, rateFactor,
                    CASE rateFactor
                        WHEN 'DISTANCE' THEN (ratePerUom*dist)/(loadQty)
                        ELSE ratePerUom
                    END AS rate,
                    CASE 
                        WHEN rateUom = $uom THEN 1 
                        WHEN rateUom = 'ST' AND $uom = 'MT' THEN 1/1.102311
                        WHEN rateUom = 'ST' AND $uom = 'GAM' THEN 302.114803
                        WHEN rateUom = 'MT' AND $uom = 'ST' THEN 1.102311
                        WHEN rateUom = 'MT' AND $uom = 'GAM' THEN 338.36863
                        WHEN rateUom = 'GAM' AND $uom = 'ST' THEN 0.00331
                        WHEN rateUom = 'GAM' AND $uom = 'MT' THEN 0.00295536
                    END AS convRate

                    WITH ol, dz, occ, lpg
                    , ratePerUom AS originalRate
                    , rateFactor
                    , dist
                    , distUom
                    , loadQty
                    , rate AS ratePerUom
                    , fsc*100 AS fscRate
                    , '%' AS fscRateUom
                    , fsc * rate / convRate AS calculatedFsc
                    , rate * (1+fsc) / convRate AS allInfreight
                    , expirationDate
                    , rateUom AS originalUom
                    , $uom AS uom
                    , rateCurr AS currency
                    ORDER BY allInfreight

                    MATCH (ol)-[:IN_ZIPCODE]->(oz:ZipCode)

                    WITH oz
                    , dz
                    , lpg
                    , {
                        id: oz.id + '|#|' + oz.country + '|#|' + dz.id + '|#|' + dz.country + '|#|' + lpg.name + '|#|' + currency + '|#|' + uom
                        , originalRate: originalRate
                        , rateFactor: rateFactor
                        , dist: dist
                        , distUom: distUom
                        , loadQty: loadQty
                        , ratePerUom: round(ratePerUom, 4)
                        , fscRate: round(fscRate, 4)
                        , fscRateUom: fscRateUom
                        , calculatedFsc: round(calculatedFsc, 4)
                        , allInfreight: round(allInfreight, 4)
                        , expirationDate: expirationDate
                        , uom: uom
                        , currency: currency
                    } AS truckCacheProperties

                    MERGE (tc:TruckCache {id: truckCacheProperties.id})
                    SET tc = truckCacheProperties
                    SET tc.update_date = datetime()
                    MERGE (tc)-[:HAS_DESTINATION]->(dz)
                    MERGE (tc)-[:HAS_ORIGIN]->(oz)
                    MERGE (tc)<-[:FOR_TRUCK_CACHE]-(lpg)
                    """, 
                    Values.parameters(
                        "destinationZip", zip
                        , "product", product
                        , "uom", uom
                    )
                );
                counter++;
                System.out.println("Truck Compelted Destination Zip: " + zip + " - " + String.valueOf(counter) + "/" + zips.size());
            });
        } catch (ClientException e) {
            System.err.println(e);
        }
    }

        private static void newTruckCache(final Driver driver) {
        try (Session session = driver.session()) {
            List<String> zips = session.run("""
                    MATCH (:Country{id:'US'})<-[:IN_COUNTRY]-(z:ZipCode)-[:IN_STATE]->(s:StateOrProvince)
                    WHERE (z)-[:TRUCK_DISTANCE_TO]-()
                    AND NOT s.id IN ['PR', 'HI', 'AK']
                    RETURN z.id AS zip ORDER BY zip DESC
                    """)
                .list(record -> record.get(0).asString());
            
            System.out.println("Zip Count: " + zips.size());
            zips.forEach(zip -> {
                session.run("""
                    MATCH (ol:Location)-[:HAS_OCCUPANT]->(occ:Occupant)-[ci:COMPETES_IN]->(ds:StateOrProvince)<-[is2:IN_STATE]-(dz:ZipCode)-[:IN_COUNTRY]->(co:Country)
                    , (ol)-[:IN_GEOGRAPHY]->()<-[:FROM]-(route:TruckRoute)-[:TO]->(co)
                    , (occ)-[:CAN_STORE]->(lpg:LogisticsProductGroup)<-[:FOR_PRODUCTGROUP]-(route)
                    , (ol)-[:HAS_OUTBOUND]->(mo:Mode)

                    WHERE dz.id = $destinationZip
                    AND dz.country = 'US'
                    AND co.id = 'US'
                    AND mo.id = 'TRUCK'
                    AND occ:Koch
                    AND lpg.name = $product

                    MATCH (route)-[:HAS_CURRENT_RATE]->(cr:TruckRate)
                    WHERE cr.shipWindowStartDate <= date() <= cr.shipWindowEndDate

                    MATCH (ol)-[:IN_ZIPCODE]->(oz:ZipCode)-[tdt:TRUCK_DISTANCE_TO]->(dz)
                    WHERE cr.distanceLower <= (CASE WHEN cr.distanceUom = 'KM' THEN tdt.distance * 1.609344 ELSE tdt.distance END) < cr.distanceUpper
                    
                    CALL (lpg, ol) {
                        MATCH (tFSC:ActiveFSC)-[fp2:FOR_PRODUCTGROUP]->(lpg)
                        , (tFSC)-[:FOR_ORIGIN_COUNTRY]->()<-[:IN_COUNTRY]-(ol)
                        RETURN tFSC
                    }

                    WITH DISTINCT ol, dz, occ, lpg, cr, tFSC
                    , 'MI' AS distUom
                    , tdt.distance AS dist
                    , route.score AS score
                    , route.id AS routeId

                    WITH *
                    ORDER BY score
                    WITH ol, dz, occ, lpg, tFSC, distUom, dist, collect(cr)[0] AS cr

                    WITH ol, dz, occ, lpg, dist, distUom, tFSC.rate AS fsc, cr.currency AS rateCurr, cr.rateUom AS rateUom
                    , cr.ratePerUom AS ratePerUom, cr.loadQuantity AS loadQty, toUpper(cr.rateFactorType) AS rateFactor, cr.shipWindowEndDate AS expirationDate

                    WITH ol, dz, occ, lpg, fsc, dist, distUom, expirationDate, rateCurr, rateUom, ratePerUom, loadQty, rateFactor,
                    CASE rateFactor
                        WHEN 'DISTANCE' THEN (ratePerUom*dist)/(loadQty)
                        ELSE ratePerUom
                    END AS rate,
                    CASE 
                        WHEN rateUom = $uom THEN 1 
                        WHEN rateUom = 'ST' AND $uom = 'MT' THEN 1/1.102311
                        WHEN rateUom = 'ST' AND $uom = 'GAM' THEN 302.114803
                        WHEN rateUom = 'MT' AND $uom = 'ST' THEN 1.102311
                        WHEN rateUom = 'MT' AND $uom = 'GAM' THEN 338.36863
                        WHEN rateUom = 'GAM' AND $uom = 'ST' THEN 0.00331
                        WHEN rateUom = 'GAM' AND $uom = 'MT' THEN 0.00295536
                    END AS convRate

                    WITH ol, dz, occ, lpg
                    , ratePerUom AS originalRate
                    , rateFactor
                    , dist
                    , distUom
                    , loadQty
                    , rate AS ratePerUom
                    , fsc*100 AS fscRate
                    , '%' AS fscRateUom
                    , fsc * rate / convRate AS calculatedFsc
                    , rate * (1+fsc) / convRate AS allInfreight
                    , expirationDate
                    , rateUom AS originalUom
                    , $uom AS uom
                    , rateCurr AS currency
                    ORDER BY allInfreight

                    MATCH (ol)-[:IN_ZIPCODE]->(oz:ZipCode)

                    WITH oz
                    , dz
                    , lpg
                    , {
                        id: oz.id + '|#|' + oz.country + '|#|' + dz.id + '|#|' + dz.country + '|#|' + lpg.name + '|#|' + currency + '|#|' + uom
                        , originalRate: originalRate
                        , rateFactor: rateFactor
                        , dist: dist
                        , distUom: distUom
                        , loadQty: loadQty
                        , ratePerUom: round(ratePerUom, 4)
                        , fscRate: round(fscRate, 4)
                        , fscRateUom: fscRateUom
                        , calculatedFsc: round(calculatedFsc, 4)
                        , allInfreight: round(allInfreight, 4)
                        , expirationDate: expirationDate
                        , uom: uom
                        , currency: currency
                    } AS truckCacheProperties

                    MERGE (tc:TruckCache {id: truckCacheProperties.id})
                    SET tc = truckCacheProperties
                    SET tc.update_date = datetime()
                    MERGE (tc)-[:HAS_DESTINATION]->(dz)
                    MERGE (tc)-[:HAS_ORIGIN]->(oz)
                    MERGE (tc)<-[:FOR_TRUCK_CACHE]-(lpg)
                    """, 
                    Values.parameters(
                        "destinationZip", zip
                        , "product", product
                        , "uom", uom
                    )
                );
                counter++;
                System.out.println("Truck Compelted Destination Zip: " + zip + " - " + String.valueOf(counter) + "/" + zips.size());
            });
        } catch (ClientException e) {
            System.err.println(e);
        }
    }
}
