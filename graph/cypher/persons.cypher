USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///verses.csv' AS row
WITH row,
     toInteger(row.verse_num) AS vn
MERGE (v:Verse {id: row.verse_id})
  ON CREATE SET
    v.identifier = row.identifier,
    v.verse_num  = vn,
    v.text       = row.text;

// Optional Sutta nodes
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///verses.csv' AS row
MERGE (s:Sutta {id: row.identifier})
MERGE (v:Verse {id: row.verse_id})
MERGE (v)-[:IN_SUTTA]->(s);