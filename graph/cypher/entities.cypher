// (Optional) ensure node lookups are fast
CREATE CONSTRAINT person_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT place_id IF NOT EXISTS
FOR (x:Place) REQUIRE x.id IS UNIQUE;

// Load edges (assumes nodes already exist)
LOAD CSV WITH HEADERS FROM 'file:///edges.csv' AS row
MATCH (a:Person {id: row.start_id})
MATCH (b:Place  {id: row.end_id})
MERGE (a)-[r:MENTIONS]->(b)
  ON CREATE SET r.confidence = toFloat(row.confidence);

