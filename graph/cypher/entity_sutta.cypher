LOAD CSV WITH HEADERS
FROM 'file:///sutta_person_counts.csv' AS row

MATCH (s:Sutta  {uid: row.sutta_uid})
MATCH (p:Person {id: toInteger(row.person_id)})

MERGE (s)-[r:MENTIONS {label: 'PERSON'}]->(p)
SET r.count = toInteger(row.mention_count);