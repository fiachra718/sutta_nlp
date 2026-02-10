import psycopg
from psycopg.rows import dict_row
from neo4j import GraphDatabase
import re

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "testtest")
PG_DSN = "dbname=tipitaka user=alee"

ID_RE = re.compile(r"^(an|sn|mn|dn)(\d{2})\.(\d{3})", re.IGNORECASE)

def identifier_to_sutta_ref(identifier: str) -> str | None:
    m = ID_RE.match((identifier or "").strip())
    if not m:
        return None
    nikaya = m.group(1).upper()
    a = int(m.group(2))
    b = int(m.group(3))
    if nikaya in {"AN", "SN"}:
        return f"{nikaya} {a}.{b}"
    return f"{nikaya} {b}"

def get_or_create_sutta(tx, identifier: str):
    sutta_ref = identifier_to_sutta_ref(identifier)
    if not sutta_ref:
        return None

    # Prefer matching by existing sutta_ref; create if missing.
    return tx.run(
        """
        MERGE (s:Sutta {sutta_ref: $sutta_ref})
        ON CREATE SET s.display_name = $sutta_ref
        ON MATCH  SET s.display_name = coalesce(s.display_name, $sutta_ref)
        RETURN elementId(s) AS eid, s.sutta_ref AS sutta_ref
        """,
        sutta_ref=sutta_ref,
    ).single()

def merge_related_edge(tx, from_identifier: str, to_identifier: str, confidence: float, source_kind: str):
    from_row = get_or_create_sutta(tx, from_identifier)
    to_row = get_or_create_sutta(tx, to_identifier)
    if not from_row or not to_row:
        return None

    return tx.run(
        """
        MATCH (a:Sutta {sutta_ref: $from_ref})
        MATCH (b:Sutta {sutta_ref: $to_ref})
        MERGE (a)-[r:RELATED_ATI]->(b)
        ON CREATE SET r.confidence = $confidence, r.source_kind = $source_kind
        ON MATCH  SET r.confidence = coalesce(r.confidence, $confidence), r.source_kind = coalesce(r.source_kind, $source_kind)
        RETURN a.sutta_ref AS from_ref, b.sutta_ref AS to_ref, r.confidence AS confidence
        """,
        from_ref=from_row["sutta_ref"],
        to_ref=to_row["sutta_ref"],
        confidence=float(confidence),
        source_kind=source_kind,
    ).single()

###
# related links generator
###
def related_links(batch_size=1000):
    sql = """
    SELECT from_identifier, to_identifier, confidence, source_kind
    FROM ati_related_links
    ORDER BY id
    """
    conn = psycopg.connect(PG_DSN, row_factory=dict_row)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield row
    finally:
        conn.close()

if __name__ == "__main__":
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        with driver.session(database="neo4j") as session:
            for record in related_links():
                rec = session.execute_write(
                    merge_related_edge,
                    record["from_identifier"],
                    record["to_identifier"],
                    record["confidence"],
                    record["source_kind"],
                )
                print(rec)
    finally:
        driver.close()
