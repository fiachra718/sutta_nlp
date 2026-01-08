// 1. Wipe
MATCH (n) DETACH DELETE n;

// 2. Constraints
CREATE CONSTRAINT sutta_uid IF NOT EXISTS
FOR (s:Sutta) REQUIRE s.uid IS UNIQUE;
CREATE CONSTRAINT verse_uid IF NOT EXISTS
FOR (v:Verse) REQUIRE v.uid IS UNIQUE;
CREATE CONSTRAINT person_uid IF NOT EXISTS
FOR (p:Person) REQUIRE p.uid IS UNIQUE;
CREATE CONSTRAINT loc_uid IF NOT EXISTS
FOR (l:LOC) REQUIRE l.uid IS UNIQUE;
CREATE CONSTRAINT gpe_uid IF NOT EXISTS
FOR (g:GPE) REQUIRE g.uid IS UNIQUE;
CREATE CONSTRAINT norp_uid IF NOT EXISTS
FOR (n:NORP) REQUIRE n.uid IS UNIQUE;

// 3. Full-text indexes
CREATE FULLTEXT INDEX person_name_aliases IF NOT EXISTS
FOR (p:Person) ON EACH [p.name, p.aliases];
CREATE FULLTEXT INDEX loc_name_aliases IF NOT EXISTS
FOR (l:LOC) ON EACH [l.name, l.aliases];
CREATE FULLTEXT INDEX gpe_name_aliases IF NOT EXISTS
FOR (g:GPE) ON EACH [g.name, g.aliases];

// 4. Load Suttas
LOAD CSV WITH HEADERS FROM 'file:///suttas.csv' AS row
MERGE (s:Sutta {uid: row.uid})
SET s.nikaya = row.nikaya,
    s.ref    = row.ref,
    s.title  = row.title,
    s.pg_id  = CASE WHEN row.pg_id = '' THEN null ELSE toInteger(row.pg_id) END,
    s.notes  = row.notes;

// 5. Load Verses
LOAD CSV WITH HEADERS FROM 'file:///verses.csv' AS row
MERGE (v:Verse {uid: row.uid})
SET v.verse_num = toInteger(row.verse_num),
    v.text      = row.text,
    v.sutta_uid = row.sutta_uid
WITH v, row
MATCH (s:Sutta {uid: row.sutta_uid})
MERGE (v)-[:IN_SUTTA]->(s);

// 6. People
LOAD CSV WITH HEADERS FROM 'file:///persons.csv' AS row
MERGE (p:Person {uid: row.uid})
SET p.name = row.name,
    p.aliases = CASE
      WHEN row.aliases = '' THEN []
      ELSE split(row.aliases, '|')
    END;

// 7. LOC
LOAD CSV WITH HEADERS FROM 'file:///locs.csv' AS row
MERGE (l:LOC {uid: row.uid})
SET l.name = row.name,
    l.aliases = CASE
      WHEN row.aliases = '' THEN []
      ELSE split(row.aliases, '|')
    END;

// 8. GPE
LOAD CSV WITH HEADERS FROM 'file:///gpes.csv' AS row
MERGE (g:GPE {uid: row.uid})
SET g.name = row.name,
    g.type = CASE WHEN row.type = '' THEN null ELSE row.type END,
    g.aliases = CASE
      WHEN row.aliases = '' THEN []
      ELSE split(row.aliases, '|')
    END;

// 9. NORP
LOAD CSV WITH HEADERS FROM 'file:///norps.csv' AS row
MERGE (n:NORP {uid: row.uid})
SET n.name = row.name,
    n.aliases = CASE
      WHEN row.aliases = '' THEN []
      ELSE split(row.aliases, '|')
    END;

// 10. Mentions
LOAD CSV WITH HEADERS FROM 'file:///verse_person.csv' AS row
MATCH (v:Verse {uid: row.verse_uid})
MATCH (p:Person {uid: row.person_uid})
MERGE (v)-[:MENTIONS_PERSON]->(p);

LOAD CSV WITH HEADERS FROM 'file:///verse_loc.csv' AS row
MATCH (v:Verse {uid: row.verse_uid})
MATCH (l:LOC {uid: row.loc_uid})
MERGE (v)-[:MENTIONS_LOC]->(l);

LOAD CSV WITH HEADERS FROM 'file:///verse_gpe.csv' AS row
MATCH (v:Verse {uid: row.verse_uid})
MATCH (g:GPE {uid: row.gpe_uid})
MERGE (v)-[:MENTIONS_GPE]->(g);

LOAD CSV WITH HEADERS FROM 'file:///verse_norp.csv' AS row
MATCH (v:Verse {uid: row.verse_uid})
MATCH (n:NORP {uid: row.norp_uid})
MERGE (v)-[:MENTIONS_NORP]->(n);

// 11. GPE hierarchy
LOAD CSV WITH HEADERS FROM 'file:///gpe_part_of.csv' AS row
MATCH (child:GPE {uid: row.child_uid})
MATCH (parent:GPE {uid: row.parent_uid})
MERGE (child)-[:PART_OF]->(parent);