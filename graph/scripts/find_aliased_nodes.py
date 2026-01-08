# file: graph/scripts/find_aliased_nodes.py
# Simple alias folder for Person nodes.
# - Input: graph/entities/aliases.jsonl  (lines like: {"id": "...", "canonical": "Buddha", "aliases": [...]})
# - Behavior: ensure canonical exists; for each alias:
#     * if alias node exists -> move MENTIONED_IN rels to canonical, union aliases, delete alias node
#     * else -> add alias string to canonical.aliases (dedup)
# - No use of id(), no APOC mergeNodes. Uses apoc.coll.toSet for dedup (APOC must be enabled).

import os, json, sys
from pathlib import Path
from neo4j import GraphDatabase

ALIASES_PATH = Path("graph/entities/aliases.jsonl")

URI      = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
USER     = os.environ.get("NEO4J_USER", "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

# Toggle via CLI: --dry
DRY = "--dry" in sys.argv or "--dry-run" in sys.argv

# Separate statements to avoid “one statement per query” errors
C_CREATE_CONSTRAINT_ID = """
CREATE CONSTRAINT person_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.id IS UNIQUE
"""
C_CREATE_CONSTRAINT_CANON = """
CREATE CONSTRAINT person_canonical IF NOT EXISTS
FOR (p:Person) REQUIRE p.canonical IS UNIQUE
"""

# Ensure canonical node exists; do not overwrite aliases aggressively
C_ENSURE_CANON = """
MERGE (p:Person {canonical:$canonical})
ON CREATE SET p.id = coalesce($canonical_id, p.id), p.aliases = coalesce($aliases, [])
RETURN p.canonical AS canonical
"""

# Add a string alias into canonical.aliases (dedup)
C_ADD_ALIAS_STRING = """
MATCH (canon:Person {canonical:$canonical})
SET canon.aliases = apoc.coll.toSet(coalesce(canon.aliases, []) + [$alias])
RETURN canon.canonical AS canonical
"""

# Full fold of an alias node into the canonical node:
# 1) Move MENTIONED_IN rels
# 2) Union aliases arrays
# 3) Drop alias node
C_FOLD_ALIAS_NODE = """
MATCH (canon:Person {canonical:$canonical})
OPTIONAL MATCH (a:Person {canonical:$alias})
WITH canon, a
CALL {
  WITH canon, a
  WITH canon, a WHERE a IS NOT NULL AND a <> canon
  MATCH (a)-[:MENTIONED_IN]->(v:Verse)
  MERGE (canon)-[:MENTIONED_IN]->(v)
  RETURN count(*) AS moved
}
WITH canon, a
CALL {
  WITH canon, a
  WITH canon, a WHERE a IS NOT NULL AND a <> canon
  // union alias arrays from 'a' into canon
  SET canon.aliases = apoc.coll.toSet(coalesce(canon.aliases, []) + coalesce(a.aliases, []))
  DETACH DELETE a
  RETURN 1 AS done
}
RETURN canon.canonical AS canonical
"""

def ensure_constraints(session):
    session.run(C_CREATE_CONSTRAINT_ID)
    session.run(C_CREATE_CONSTRAINT_CANON)

def alias_node_exists(session, alias):
    rec = session.run(
        "MATCH (a:Person {canonical:$alias}) RETURN a.canonical AS canonical LIMIT 1",
        alias=alias,
    ).single()
    return bool(rec)

def fold_alias(session, canonical, alias, canonical_id=None, bootstrap_aliases=None):
    """
    If alias node exists: fold it into canonical.
    Else: add alias string to canonical.aliases.
    """
    # Make sure canonical exists (and optionally seed its id / initial aliases on first creation)
    session.run(
        C_ENSURE_CANON,
        canonical=canonical,
        canonical_id=canonical_id,
        aliases=bootstrap_aliases or [],
    )

    if alias_node_exists(session, alias):
        # fold the node into the canonical
        session.run(C_FOLD_ALIAS_NODE, canonical=canonical, alias=alias)
    else:
        # just record the alias string on canonical
        session.run(C_ADD_ALIAS_STRING, canonical=canonical, alias=alias)

def main():
    if not ALIASES_PATH.exists():
        print(f"!! Missing {ALIASES_PATH}", file=sys.stderr)
        sys.exit(1)

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    with driver.session() as s:
        # constraints (no-ops if already present)
        if not DRY:
            ensure_constraints(s)

        # stream the file to keep memory low
        with ALIASES_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                canonical = rec["canonical"]
                canonical_id = rec.get("id")
                aliases = [a for a in rec.get("aliases", []) if a and a != canonical]

                print(f"→ {canonical}: {len(aliases)} aliases")
                if DRY:
                    for alias in aliases:
                        exists = alias_node_exists(s, alias)
                        kind = "alias_is_node" if exists else "alias_string_only"
                        print(f"  - {alias}  [{kind}]")
                    continue

                # Bootstrap canonical once (only matters if newly created)
                boot = aliases[:1] if aliases else []
                s.run(C_ENSURE_CANON, canonical=canonical, canonical_id=canonical_id, aliases=boot)

                # Then process all aliases
                for alias in aliases:
                    fold_alias(s, canonical, alias)

    print("✅ Done.")

if __name__ == "__main__":
    main()