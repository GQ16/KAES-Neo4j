//Here is the article that I get this example from: https://medium.com/neo4j/getting-from-denmark-hill-to-gatwick-airport-with-quantified-path-patterns-bed38da27ca1
//I've nested the query to make each operation understandable
MATCH (gtw:Station {name: 'Gatwick Airport'})
MATCH p = 
    (:Station {name: 'Denmark Hill'}) 
	//start at denmark hill station, juxtaposing a node with QPP anchors the first iteration of that pattern
        (
			(l)-[link:LINK]-(r) //The repeating quantified path pattern
			WHERE point.distance(r.location, gtw.location) - point.distance(l.location, gtw.location) < 1000 //makes sure that between each repition of the qpp, we're getting closer to our final destination
		)+ 	//+ means one or more repitions of the QPP, but if you wanted to specify the min/max hops, use curly braces: {3,5} 
    (gtw) //end at gatwick airport
RETURN reduce(acc = 0.0, l IN link | round(acc + l.distance, 2)) AS total, 
 [n in nodes(p) | n.name] AS stations
ORDER BY total
LIMIT 1;

//To better see how these things must be juxtaposed for the query to run, here is the query with comments and new lines removed:
//in this query to avoid confusion of the + at the end of the QPP I switched it with {2,3}, but it can be a + although it's best 
// practice to limit the hops.
MATCH (gtw:Station {name: 'Gatwick Airport'})
MATCH p = (:Station {name: 'Denmark Hill'}) ((l)-[link:LINK]-(r) WHERE point.distance(r.location, gtw.location) - point.distance(l.location, gtw.location) < 1000){2,3}(gtw)
RETURN reduce(acc = 0.0, l IN link | round(acc + l.distance, 2)) AS total, 
 [n in nodes(p) | n.name] AS stations
ORDER BY total
LIMIT 1;

//In a general sense, this is the query structure for QPP, but it doesn't have to be exactly like this,
// you may remove the anchor nodes, the where clauses, the node aliases, wherever you'd like.
MATCH p=(:StartAnchor)((:StartPatternLabel)-[:RELATIONSHIP]-(:EndPatternLabel)){0,10}(:EndAnchor)
RETURN p;
