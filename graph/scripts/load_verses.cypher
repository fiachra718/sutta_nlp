LOAD CSV WITH HEADERS 
FROM 'file:///verses.csv' as row

WITH row
MERGE (s:Sutta {identifier: row.identifier})
  ON CREATE SET s.sutta_ref = row.sutta_ref

MERGE (v:Verse {uid: row.identifier + ':' + row.verse_num})
  SET v.verse_num = row.verse_num,   
      v.text      = row.verse_text

MERGE (v)-[:IN_SUTTA]->(s);