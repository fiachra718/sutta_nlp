import psycopg
from psycopg.rows import dict_row
from neo4j import GraphDatabase

PG_DSN = "dbname=tipitaka user=alee"
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "testtest")
DB = "neo4j"

# Prefer DB mapping from identifier -> sutta_ref
MAP_SQL = """
SELECT
  identifier,
  CASE
    WHEN nikaya IN ('AN','SN') THEN nikaya || ' ' || vagga || '.' || book_number
    WHEN nikaya IN ('MN','DN') THEN nikaya || ' ' || book_number
    WHEN nikaya = 'KN'         THEN vagga || ' ' || book_number
    ELSE nikaya || ' ' || book_number
  END AS sutta_ref
FROM ati_suttas
WHERE nikaya IS NOT NULL AND book_number IS NOT NULL
"""

LINK_SQL = """
SELECT from_identifier, to_identifier, source_kind, confidence,
       baseline_jaccard, baseline_weighted_jaccard, baseline_cosine
FROM ati_related_links
WHERE baseline_cosine IS NOT NULL
"""

MERGE_CYPHER = """
MATCH (a:Sutta {sutta_ref: $from_ref})
MATCH (b:Sutta {sutta_ref: $to_ref})
MERGE (a)-[r:RELATED_ATI]->(b)
SET r.source_kind = $source_kind,
    r.confidence = $confidence,
    r.baseline_jaccard = $baseline_jaccard,
    r.baseline_weighted_jaccard = $baseline_weighted_jaccard,
    r.baseline_cosine = $baseline_cosine
"""

with psycopg.connect(PG_DSN, row_factory=dict_row) as pg:
    with pg.cursor() as cur:
        cur.execute(MAP_SQL)
        id2ref = {r["identifier"]: r["sutta_ref"] for r in cur.fetchall()}
        cur.execute(LINK_SQL)
        links = cur.fetchall()

driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()

written = 0
skipped = 0
with driver.session(database=DB) as s:
    for r in links:
        from_ref = id2ref.get(r["from_identifier"])
        to_ref = id2ref.get(r["to_identifier"])
        if not from_ref or not to_ref:
            skipped += 1
            continue
        s.run(
            MERGE_CYPHER,
            from_ref=from_ref,
            to_ref=to_ref,
            source_kind=r["source_kind"],
            confidence=float(r["confidence"] or 0.0),
            baseline_jaccard=float(r["baseline_jaccard"] or 0.0),
            baseline_weighted_jaccard=float(r["baseline_weighted_jaccard"] or 0.0),
            baseline_cosine=float(r["baseline_cosine"] or 0.0),
        ).consume()
        written += 1

driver.close()
print({"written_edges": written, "skipped": skipped})
