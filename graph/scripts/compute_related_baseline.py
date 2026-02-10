#!/usr/bin/env python3
import argparse
import re
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row
from neo4j import GraphDatabase

PG_DSN_DEFAULT = "dbname=tipitaka user=alee"
NEO4J_URI_DEFAULT = "bolt://localhost:7687"
NEO4J_AUTH_DEFAULT = ("neo4j", "testtest")
NEO4J_DB_DEFAULT = "neo4j"

ID_RE = re.compile(r"^(an|sn)(\d{2})\.(\d{3})", re.IGNORECASE)
MN_DN_RE = re.compile(r"^(mn|dn)\.(\d{3})", re.IGNORECASE)


ALTER_SQL = """
ALTER TABLE ati_related_links
  ADD COLUMN IF NOT EXISTS baseline_jaccard REAL,
  ADD COLUMN IF NOT EXISTS baseline_weighted_jaccard REAL,
  ADD COLUMN IF NOT EXISTS baseline_cosine REAL,
  ADD COLUMN IF NOT EXISTS baseline_person_overlap INTEGER,
  ADD COLUMN IF NOT EXISTS baseline_person_union INTEGER,
  ADD COLUMN IF NOT EXISTS baseline_updated_at TIMESTAMPTZ;
"""

FETCH_LINKS_SQL = """
SELECT id, from_identifier, to_identifier
FROM ati_related_links
ORDER BY id;
"""

FETCH_IDENTIFIER_REFS_SQL = """
SELECT
  identifier,
  CASE
    WHEN nikaya IN ('AN','SN') THEN nikaya || ' ' || vagga || '.' || book_number
    WHEN nikaya IN ('MN','DN') THEN nikaya || ' ' || book_number
    WHEN nikaya = 'KN'         THEN vagga || ' ' || book_number
    ELSE nikaya || ' ' || book_number
  END AS sutta_ref
FROM ati_suttas
WHERE nikaya IS NOT NULL
  AND book_number IS NOT NULL;
"""

UPDATE_SQL = """
UPDATE ati_related_links
SET
  baseline_jaccard = %(baseline_jaccard)s,
  baseline_weighted_jaccard = %(baseline_weighted_jaccard)s,
  baseline_cosine = %(baseline_cosine)s,
  baseline_person_overlap = %(baseline_person_overlap)s,
  baseline_person_union = %(baseline_person_union)s,
  baseline_updated_at = now()
WHERE id = %(id)s;
"""


def fallback_identifier_to_sutta_ref(identifier: str) -> str | None:
    s = (identifier or "").strip()
    m = ID_RE.match(s)
    if m:
        nikaya = m.group(1).upper()
        book = int(m.group(2))
        sutta = int(m.group(3))
        return f"{nikaya} {book}.{sutta}"
    m = MN_DN_RE.match(s)
    if m:
        nikaya = m.group(1).upper()
        sutta = int(m.group(2))
        return f"{nikaya} {sutta}"
    return None


def weighted_jaccard(v1: dict[str, float], v2: dict[str, float]) -> float:
    if not v1 and not v2:
        return 0.0
    keys = set(v1) | set(v2)
    min_sum = 0.0
    max_sum = 0.0
    for k in keys:
        a = float(v1.get(k, 0.0))
        b = float(v2.get(k, 0.0))
        min_sum += min(a, b)
        max_sum += max(a, b)
    return (min_sum / max_sum) if max_sum else 0.0


def binary_jaccard(v1: dict[str, float], v2: dict[str, float]) -> tuple[float, int, int]:
    s1 = set(v1)
    s2 = set(v2)
    inter = s1 & s2
    union = s1 | s2
    overlap = len(inter)
    union_n = len(union)
    score = (overlap / union_n) if union_n else 0.0
    return score, overlap, union_n


def cosine_similarity(v1: dict[str, float], v2: dict[str, float]) -> float:
    if not v1 or not v2:
        return 0.0
    keys = set(v1) | set(v2)
    dot = 0.0
    n1 = 0.0
    n2 = 0.0
    for k in keys:
        a = float(v1.get(k, 0.0))
        b = float(v2.get(k, 0.0))
        dot += a * b
        n1 += a * a
        n2 += b * b
    if n1 == 0.0 or n2 == 0.0:
        return 0.0
    return dot / ((n1 ** 0.5) * (n2 ** 0.5))


def fetch_sutta_person_vectors(driver, neo4j_db: str) -> dict[str, dict[str, float]]:
    cypher = """
    MATCH (s:Sutta)-[:HAS_VERSE]->(:Verse)-[m:MENTIONS]->(e:Entity {entity_type:'PERSON'})
    WITH
      s.sutta_ref AS sutta_ref,
      coalesce(e[$eq_prop], 'PERSON:' + toString(e.id)) AS eq_class,
      sum(coalesce(m.ref_count, 1)) AS weight
    RETURN sutta_ref, eq_class, weight
    """
    vectors: dict[str, dict[str, float]] = defaultdict(dict)
    with driver.session(database=neo4j_db) as session:
        result = session.run(cypher, eq_prop="entity_eq_class")
        for row in result:
            sutta_ref = row["sutta_ref"]
            eq_class = str(row["eq_class"])
            weight = float(row["weight"] or 0.0)
            vectors[sutta_ref][eq_class] = weight
    return vectors


def main():
    parser = argparse.ArgumentParser(description="Compute Jaccard baseline for ati_related_links using Neo4j Sutta->Person mentions.")
    parser.add_argument("--pg-dsn", default=PG_DSN_DEFAULT)
    parser.add_argument("--neo4j-uri", default=NEO4J_URI_DEFAULT)
    parser.add_argument("--neo4j-user", default=NEO4J_AUTH_DEFAULT[0])
    parser.add_argument("--neo4j-password", default=NEO4J_AUTH_DEFAULT[1])
    parser.add_argument("--neo4j-db", default=NEO4J_DB_DEFAULT)
    parser.add_argument("--limit", type=int, default=0, help="Optional limit for testing")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    driver.verify_connectivity()
    vectors = fetch_sutta_person_vectors(driver, args.neo4j_db)
    driver.close()
    print(f"Loaded sutta vectors from Neo4j: {len(vectors)}")

    with psycopg.connect(args.pg_dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(ALTER_SQL)
            conn.commit()

            cur.execute(FETCH_IDENTIFIER_REFS_SQL)
            ident_to_ref = {row["identifier"]: row["sutta_ref"] for row in cur.fetchall()}
            print(f"Loaded identifier->sutta_ref map: {len(ident_to_ref)}")

            cur.execute(FETCH_LINKS_SQL)
            rows = cur.fetchall()

        updates = []
        processed = 0
        missing_ref = 0
        for row in rows:
            if args.limit and processed >= args.limit:
                break
            processed += 1

            from_ident = row["from_identifier"]
            to_ident = row["to_identifier"]
            from_ref = ident_to_ref.get(from_ident) or fallback_identifier_to_sutta_ref(from_ident)
            to_ref = ident_to_ref.get(to_ident) or fallback_identifier_to_sutta_ref(to_ident)
            if not from_ref or not to_ref:
                missing_ref += 1
                continue

            v1 = vectors.get(from_ref, {})
            v2 = vectors.get(to_ref, {})
            j, overlap, union_n = binary_jaccard(v1, v2)
            jw = weighted_jaccard(v1, v2)
            cos = cosine_similarity(v1, v2)

            updates.append(
                {
                    "id": row["id"],
                    "baseline_jaccard": j,
                    "baseline_weighted_jaccard": jw,
                    "baseline_cosine": cos,
                    "baseline_person_overlap": overlap,
                    "baseline_person_union": union_n,
                }
            )

        print(f"Processed links: {processed}")
        print(f"Computed updates: {len(updates)}")
        print(f"Missing refs: {missing_ref}")

        if not args.dry_run and updates:
            with conn.cursor() as cur:
                cur.executemany(UPDATE_SQL, updates)
            conn.commit()
            print("Updated ati_related_links baseline columns.")


if __name__ == "__main__":
    main()
