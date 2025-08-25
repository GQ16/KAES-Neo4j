package com.kochagenergy.processes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
    static String product = "UAN";
    static String currency = "USD";
    static String uom = "ST";

    public static void main(String[] args) {
        try (var driver = GraphDatabase.driver(DB_URI, AuthTokens.basic(DB_USER, DB_PASS))) {
            driver.verifyConnectivity();
            System.out.println("Connection established.");

            // test();
            // var idList = List.of("b03aff81-fc63-4c77-84c1-df7f62050213", "64f4f6a7-d7ff-46a4-8d23-335ed43bc151", "2c7c8747-9cf6-4ea1-9346-cda1de224579", "2fe2d663-e28d-496f-a59a-78203fcf374f", "83a0e4da-37d8-4260-ba31-9222dfb3eaf5", "4b7bc799-35be-420b-9fea-657cde47bf0d", "6893605b-560e-4306-926f-74d6983846e6", "55d2aa63-74d4-4e94-b4f5-9132cbc345d8", "b922ef93-08ef-49c0-995e-804a949ca2db", "8fbd47ee-aafc-4a06-b353-9d94932d5549", "596303fb-6715-4489-be14-a2eddc5309a4", "e9236a6e-a1df-489d-aadb-77aca4bae13f", "a606a5fe-a0e2-406a-9d67-1c1890457db9", "4ddf65e6-9948-4ca3-98de-6512ead3c7da", "2d4cb56c-6885-4915-9a22-963fdd9b6d38", "0c38fd43-007c-49da-b561-b0e592073e06", "1a3ce37d-65f3-4c99-87ed-bcf2a295f1a5", "bdf49050-fd01-4a1c-98c9-eca10ad4ae90", "c4cbd1c6-70f3-4d52-b888-b7de09310eb4", "69d3a37b-9913-4b92-9267-037ad6b39526", "e060bf5c-d22d-456b-85f0-3c7aeba8712a", "f651e19b-fac4-46fa-9144-ef95819c9a60", "80f9bc5a-31f0-4d03-89a5-bcac439d0f6d", "05cc1555-12d1-43ec-8ec9-4c19985de1bf", "307c3dad-4079-4bc0-bc48-c8000662747a", "b9db1d96-3a40-4896-b37d-0ee2bab42396", "eec0d636-d22b-4e53-abfb-f83255fb3092", "1201a968-b6b2-4e9c-8419-451fd5f66f63", "e1be81c2-d2a9-4d5e-99ed-8cde5a4ef6ad", "7c33476f-78a7-4877-a688-223b77fbcca8", "4db2babe-6b25-4756-b365-3280ca047448", "b4dc71df-ee8f-4115-8e8e-5315b21ff5b1", "c5b5715b-6a26-4b1b-af27-f4ee636a9ade", "273150e8-ed48-4e3d-ab59-88f05d2b2e02", "9fcb3ab4-c1ca-4576-b84d-2ad39949e5be", "090d727d-0bf9-43cc-a3d8-32a6602c4643", "bf0b74e8-ab2f-49b1-bc7a-314d89624ab4", "3b4e8ec3-1507-44bd-ad5d-a38b6c31fcd7", "9e2da9da-2b22-4d61-92d6-c34f1c80c5d8", "3e334068-a072-4d2c-9fd4-93eca4b4b154", "7f5349c9-1d3b-4fe0-96fc-2d86b140f228", "5b670d83-7b85-429d-a97f-375cb3a52f2f", "e54caf1d-5195-4ade-80f1-561cffd54381", "98076397-ad3e-4b77-88f3-4117d81b4830", "5450073c-ee61-4f65-8e7e-76eff080655a", "aa46c64b-ce5f-4040-9801-14f962b31a94", "46ea87ea-b22d-4562-8d03-dc7502a97ce1", "09679fcd-685a-4d14-b5a0-8bcbf8fca2bd");
            // idList.forEach(id -> railCache(driver, id));
            railCache(driver, "d671c4a1-c09e-4843-a98d-7fdabf2aef59");
            // railCache(driver);
            // truckCache(driver);

            driver.close();
        }
    }

    private static void test() {
        String cypherQuery = getRailCacheQueryString(product, 2);
        String filePath = "rail_cache_query.cypher";
        
        try {
            Files.writeString(Path.of(filePath), cypherQuery);
            System.out.println("Cypher query written to: " + filePath);
        } catch (IOException e) {
            System.err.println("Error writing cypher query to file: " + e.getMessage());
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
        WHERE ol <> dl

        MATCH path = SHORTEST 3 (rr2)(
            ()-[:`%s_FROM`]->(stop2)-[:AT_INTERCHANGE]->(interchange)<-[:AT_INTERCHANGE]-(stop1)<-[:`%s_TO`]-()
        ){0, %d}(rr1:RailRoute)-[:`%s_FROM`]->()-[:IN_SPLC]->(s1:SPLC)<-[:IN_SPLC]-(ol)
        WHERE (rr1)-[:HAS_ORIGIN_CARRIER]->()<-[:SERVED_BY]-(ol)

        WITH DISTINCT ol, dl, lpg, s1, s2
        , reverse([r IN nodes(path) WHERE r:RailRoute]) AS routes
        , reverse([x IN nodes(path) WHERE x:SPLC AND x <> s1]) AS interchangeList
        WHERE 1=1
        AND all(x IN routes WHERE (x)-[:HAS_CURRENT_RATE]->())

        WITH *, size(routes) AS numberOfRoutes, [s1] + interchangeList + [s2] AS splcList

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

        WITH ol, dl, routes, splcList, interchangeList, rates, lpg, numberOfRoutes

        MATCH (cad:Currency {id:'CAD'})-[exch:HAS_EXCHANGE_RATE]->(usd:Currency {id:'USD'})

        WITH DISTINCT ol, dl, lpg
        , [s IN splcList| s.id] AS splcs
        , [i IN interchangeList| i.r260] AS interchanges
        , [r IN rates| {
            document: r.document,
            baseRate: r.rate,
            baseRateUom: r.uom,
            baseRateCurrency: r.currency,
            exchRate: CASE WHEN r.currency = 'CAD' THEN exch.rate ELSE 1 END,
            usdPerShortTonRate: CASE toLower(r.uom)
                WHEN 'ton' THEN r.rate * CASE WHEN r.currency = 'CAD' THEN exch.rate ELSE 1 END
                WHEN 'car' THEN round(r.rate * CASE WHEN r.currency = 'CAD' THEN exch.rate ELSE 1 END / lpg.railCarVol, 3)
                ELSE 0
            END,
            carrier: r.carrier,
            originCarrier: r.origin_carrier,
            destinationCarrier: r.destination_carrier,
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
            expiration: coalesce(r.rate_expiration, date('2099-01-01'))
        }] AS rateMaps

        WITH ol, dl, rateMaps[0].minCars AS minCars, rateMaps[0].carOwner AS carOwner, rateMaps, lpg, splcs, interchanges
        , round( reduce( price = 0, x IN rateMaps | price + x.usdPerShortTonRate ),4 ) AS rateSum
        ORDER BY carOwner DESC, rateSum

        WITH ol, dl, lpg, minCars
        , collect(carOwner)[0] AS carOwner
        , collect(splcs)[0] AS splcs
        , collect(interchanges)[0] AS interchanges
        , collect(rateMaps)[0] AS legs

        WITH ol
        , dl
        , lpg
        , {
            id: ol.id + '|#|' + dl.id + '|#|' + lpg.name + '|#|' + minCars
            , splcs			        : splcs
            , interchanges          : interchanges
            , carOwner              : carOwner
            , minCars               : minCars
            , expirations           : [x IN legs|x.expiration]
            , documents             : [x IN legs|x.document]
            , baseRates             : [x IN legs|x.baseRate]
            , baseRateUoms          : [x IN legs|x.baseRateUom]
            , baseRateCurrencies    : [x IN legs|x.baseRateCurrency]
            , exchRates             : [x IN legs|x.exchRate]
            , usdPerShortTonRates   : [x IN legs|x.usdPerShortTonRate]
            , carriers		        : [x IN legs|x.carrier]
            , originCarriers        : [x IN legs|x.originCarrier]
            , destinationCarriers   : [x IN legs|x.destinationCarrier]
            , routes                : [x IN legs|x.route]
            , miles                 : []
            , fscs                  : []
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
        railCache(driver, null);
    }

    private static void railCache(final Driver driver, String singleDestinationId) {
        Map<String, Integer> locationHopsMap = getLocationHopsMap(driver, singleDestinationId);
        System.out.println("Location Count: " + locationHopsMap.size());
        
        establishCacheForDestinations(driver, locationHopsMap);
        setFullPathRouteProperties(driver, singleDestinationId);
        setMilesList(driver, singleDestinationId);
        setFscsList(driver, singleDestinationId);
        setFreightAndMileageTotals(driver, singleDestinationId);
    }

    private static Map<String, Integer> getLocationHopsMap(final Driver driver) {
        return getLocationHopsMap(driver, null);
    }

    private static Map<String, Integer> getLocationHopsMap(final Driver driver, String singleDestinationId) {
        Map<String, Integer> locationHopsMap = new HashMap<>();
        try (Session session = driver.session()) {
            // Delete existing cache for product
            String deleteQuery = """
                MATCH (lpg:LogisticsProductGroup)-[:FOR_RAIL_CACHE]->(rc:RailCache)
                WHERE lpg.name = $product
                """ + (singleDestinationId != null ? " AND EXISTS{MATCH (rc)-[:HAS_DESTINATION]->(:Location{id:$locationId})} " : "") + """
                DETACH DELETE rc
                """;
            
            session.run(deleteQuery, 
                singleDestinationId != null 
                    ? Values.parameters("product", product, "locationId", singleDestinationId)
                    : Values.parameters("product", product));
            
            System.out.println(product + " Rail Cache Has been deleted" + 
                (singleDestinationId != null ? " for location " + singleDestinationId : "."));

            // Get list of destinations and their qpp maxes
            String locationQuery = """
                MATCH (mo:Mode)<-[:HAS_INBOUND]-(dl:Location)-[:HAS_OCCUPANT]->()-[cs:CAN_STORE]->(lpg:LogisticsProductGroup)
                WHERE lpg.name = $product
                AND mo.id = 'RAIL'
                """ + (singleDestinationId != null ? " AND dl.id = $locationId " : "") + """
                RETURN DISTINCT dl.id AS locationId, CASE WHEN dl.threeLegsAllowed THEN 2 ELSE 1 END AS qppMax
                ORDER BY locationId
                """;

            session.run(locationQuery,
                singleDestinationId != null 
                    ? Values.parameters("product", product, "locationId", singleDestinationId)
                    : Values.parameters("product", product))
            .forEachRemaining(record -> {
                locationHopsMap.put(
                        record.get("locationId").asString(),
                        record.get("qppMax").asInt()
                );
            });
        } catch (ClientException e) {
            System.err.println("Error Retrieving Location and QPP Maxes");
            System.err.println(e);
        }
        return locationHopsMap;
    }

    private static void establishCacheForDestinations(final Driver driver, Map<String, Integer> locationHopsMap) {
        locationHopsMap.forEach((locationId, qppMax) -> {
            try (Session session = driver.session()) {
                session.run(
                    getRailCacheQueryString(product, qppMax),
                    Values.parameters(
                        "locationId", locationId
                        , "product", product
                        , "uom", uom
                        , "currency", currency
                    )
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

    private static void setFullPathRouteProperties(final Driver driver, String singleDestinationId) {
        try (Session session = driver.session()) {
            String locationFilter = singleDestinationId != null ? " AND EXISTS{MATCH (rc)-[:HAS_DESTINATION]->(:Location{id:$locationId})} " : "";
            
            //Set full path route property for 1-leg moves
            session.run("""
                MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(:LogisticsProductGroup{name:$product})
                WHERE size(rc.splcs) = 2
                """ + locationFilter + """
                SET rc.route = rc.routes[0];
                """, singleDestinationId != null 
                    ? Values.parameters("product", product, "locationId", singleDestinationId)
                    : Values.parameters("product", product));

            //Set full path route property for 2-leg rule 11 moves
            session.run("""
                MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(:LogisticsProductGroup{name:$product})
                WHERE size(rc.splcs) = 3
                """ + locationFilter + """
                SET rc.route = rc.routes[0] + '-' + rc.interchanges[0] + '-' + rc.routes[1];
                """, singleDestinationId != null 
                    ? Values.parameters("product", product, "locationId", singleDestinationId)
                    : Values.parameters("product", product));

            //Set full path route property for 3-leg rule 11 moves
            session.run("""
                MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(:LogisticsProductGroup{name:$product})
                WHERE size(rc.splcs) = 4
                """ + locationFilter + """
                SET rc.route = rc.routes[0] + '-' + rc.interchanges[0] + '-' + rc.routes[1] + '-' + rc.interchanges[1] + '-' + rc.routes[2];
                """, singleDestinationId != null 
                    ? Values.parameters("product", product, "locationId", singleDestinationId)
                    : Values.parameters("product", product));
            System.out.println("RailCache route Property Setting Compelted");
        } catch (ClientException e) {
            System.err.println("Error Triggered by Rail Route Property Setting");
            System.err.println(e);
        }
    }

    private static void setMilesList(final Driver driver, String singleDestinationId) {
        try (Session session = driver.session()) {
            String locationFilter = singleDestinationId != null ? " AND EXISTS{MATCH (rc)-[:HAS_DESTINATION]->(:Location{id:$locationId})} " : "";
            
            for (int i = 0; i < 3; i++) {
                session.run("""
                    MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(:LogisticsProductGroup{name:$product})
                    WHERE size(rc.splcs) > $iterator + 1
                    """ + locationFilter + """
                    WITH rc, rc.splcs[$iterator] AS s1, rc.splcs[$iterator + 1] AS s2, rc.originCarriers[$iterator] AS oc, rc.destinationCarriers[$iterator] AS dc

                    MATCH (ds:SPLC{id:s2})
                    MATCH (os:SPLC{id:s1})
                    OPTIONAL MATCH (os)<-[:FROM_SPLC]-(rm:RailMileage)-[:TO_SPLC]->(ds)
                    WITH rc, coalesce(rm.distance,0) AS dist
                    , EXISTS{
                        (:Carrier{id:oc})<-[:FROM_CARRIER]-(rm)
                    } AS origCarrierMatches
                    , EXISTS{
                        (rm)-[:TO_CARRIER]->(:Carrier{id:dc})
                    } AS destCarrierMatches

                    WITH rc, dist, CASE
                        WHEN origCarrierMatches AND destCarrierMatches THEN 1
                        WHEN origCarrierMatches OR destCarrierMatches THEN 2
                        ELSE 3
                    END AS mileageScore
                    ORDER BY mileageScore

                    WITH rc, collect(dist)[0] AS bestDist
                    SET rc.miles = rc.miles + [bestDist]
                    """, singleDestinationId != null 
                        ? Values.parameters("iterator", i, "product", product, "locationId", singleDestinationId)
                        : Values.parameters("iterator", i, "product", product));
            }
            System.out.println("RailCache miles Property Setting Compelted");
        } catch (ClientException e) {
            System.err.println("Error Triggered by RailCache miles Setting");
            System.err.println(e);
        }
    }

    private static void setFscsList(final Driver driver, String singleDestinationId) {
        try (Session session = driver.session()) {
            String locationFilter = singleDestinationId != null ? " AND EXISTS{MATCH (rc)-[:HAS_DESTINATION]->(:Location{id:$locationId})} " : "";
            
            for (int i = 0; i < 3; i++) {
                session.run("""
                    MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(lpg:LogisticsProductGroup{name:$product})
                    WHERE size(rc.splcs) > $iterator + 1
                    """ + locationFilter + """
                    WITH rc, lpg, rc.carriers[$iterator] AS carrierId, rc.baseRateCurrencies[$iterator] AS currencyId
                    MATCH (ca:Carrier{id:carrierId})
                    MATCH (cu:Currency{id:currencyId})
                    OPTIONAL MATCH (ca)-[:HAS_CURRENT_FSC]->(rFSC:RailFSC)<-[:FOR_RAIL_FSC]-(cu:Currency)
                    , (rFSC)-[:FOR_PRODUCTGROUP]->(lpg)
                    OPTIONAL MATCH (cu)-[exch:HAS_EXCHANGE_RATE]->(usd:Currency{id:'USD'})

                    WITH rc, coalesce(rFSC.rate, 0.0) AS perCarPerMileFuel, coalesce(round(rFSC.rate/lpg.railCarVol, 6), 0.0) AS perShortTonPerMileFuel, coalesce(exch.rate, 1) AS exchangeRate
                    WITH rc, round(perShortTonPerMileFuel * exchangeRate * rc.miles[$iterator], 4) AS usdPerShortTonFuel
                    SET rc.fscs = rc.fscs + [usdPerShortTonFuel]
                    """, singleDestinationId != null 
                        ? Values.parameters("iterator", i, "product", product, "locationId", singleDestinationId)
                        : Values.parameters("iterator", i, "product", product));
            }
            System.out.println("RailCache fscs Property Setting Compelted");
        } catch (ClientException e) {
            System.err.println("Error Triggered by RailCache fscs Setting");
            System.err.println(e);
        }
    }

    private static void setFreightAndMileageTotals(final Driver driver, String singleDestinationId) {
        try (Session session = driver.session()) {
            String locationFilter = singleDestinationId != null ? " WHERE EXISTS{MATCH (rc)-[:HAS_DESTINATION]->(:Location{id:$locationId})} " : "";
            
            session.run("""
                MATCH (rc:RailCache)<-[:FOR_RAIL_CACHE]-(:LogisticsProductGroup{name:$product})
                """ + locationFilter + """
                SET rc.freight = round( reduce( price = 0, x IN range(0,size(rc.splcs)-2) | price + rc.usdPerShortTonRates[x] + rc.fscs[x] ),4 )
                SET rc.totalMiles = reduce( dist = 0, x IN range(0,size(rc.splcs)-2) | dist + rc.miles[x] )
                """, singleDestinationId != null 
                    ? Values.parameters("product", product, "locationId", singleDestinationId)
                    : Values.parameters("product", product));
            System.out.println("RailCache freight and totalMiles Set");
        } catch (ClientException e) {
            System.err.println("Error Triggered by RailCache freight and totalMiles Setting");
            System.err.println(e);
        }
    }

    private static void truckCache(final Driver driver) {
        try (Session session = driver.session()) {
            session.run("""
                CALL apoc.periodic.iterate(
                    "MATCH (lpg:LogisticsProductGroup)-[:FOR_TRUCK_CACHE]->(tc:TruckCache)
                    WHERE lpg.name = $product
                    RETURN tc",
                    "DETACH DELETE tc"
                ,{params:{product:$product}})
                """, Values.parameters("product", product)
            );
            System.out.println(product + " Truck Cache Has been deleted.");

            List<String> zips = session.run("""
                    MATCH (:Country{id:'US'})<-[:IN_COUNTRY]-(z:ZipCode)-[:IN_STATE]->(s:StateOrProvince)
                    WHERE (z)-[:TRUCK_DISTANCE_TO]-()
                    AND NOT s.id IN ['PR', 'HI', 'AK']
                    // AND s.id = 'IL'
                    RETURN z.id AS zip ORDER BY zip DESC
                    """)
                .list(record -> record.get(0).asString()
            );
            
            System.out.println("Zip Count: " + zips.size());
            zips.forEach(zip -> {
                session.run("""
                    MATCH (ol:Location)-[:HAS_OCCUPANT]->(occ:Occupant)-[ci:COMPETES_IN]->(ds:StateOrProvince)<-[is2:IN_STATE]-(dz:ZipCode)-[:IN_COUNTRY]->(co:Country)
                    , (ol)-[:IN_GEOGRAPHY]->()<-[:FROM]-(route:TruckRoute)-[:TO]->(co)
                    , (occ)-[:CAN_STORE]->(lpg:LogisticsProductGroup)<-[:FOR_PRODUCTGROUP]-(route)
                    , (ol)-[:HAS_OUTBOUND]->(mo:Mode)

                    WHERE dz.id = $destinationZip
                    AND dz.country = 'US'
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
