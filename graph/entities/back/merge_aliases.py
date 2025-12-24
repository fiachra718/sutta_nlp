#!/usr/bin/env python3
import argparse, json, sys, os
from neo4j import GraphDatabase

MERGE_CYPHER = """
// Ensure canonical node exists
MERGE (canon:Person {canonical:$canonical})
ON CREATE SET canon.id = coalesce(canon.id, $canonical_id),
              canon.aliases = coalesce($aliases, [])
WITH canon, $aliases AS aliases
// Collect any alias nodes that exist as separate Person nodes
UNWIND aliases AS aliasName
OPTIONAL MATCH (alias:Person {canonical: aliasName})
WITH canon, collect(alias) AS aliasNodes, aliases
// Keep only valid nodes and exclude self if present
WITH canon, [x IN aliasNodes WHERE x IS NOT NULL AND x <> canon] AS toMerge, aliases
// Conditionally merge when there are alias nodes to merge
CALL apoc.do.when(
  size(toMerge) > 0,
  '
   CALL apoc.refactor.mergeNodes($nodes, {properties:"combine", mergeRels:true}) YIELD node
   RETURN node AS out
  ',
  '
   RETURN $canon AS out
  ',
  {nodes: [canon] + toMerge, canon: canon}
) YIELD value
WITH value.out AS node, aliases
// Make sure canonical & aliases are set de-duplicated
SET node.canonical = node.canonical, 
    node.aliases   = apoc.coll.toSet(coalesce(node.aliases, []) + aliases)
RETURN node.canonical AS canonical, size(node.aliases) AS alias_count, id(node) AS node_id
"""

CHECK_COUNTS = """
RETURN 
  (SELECT count(p) FROM (:Person) p) AS persons, 
  (SELECT count(v) FROM (:Verse) v)  AS verses,
  (SELECT count(r) FROM ()-[:MENTIONED_IN]->()) AS mentions
"""

ENSURE_CONSTRAINTS = """
CREATE CONSTRAINT person_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT person_canonical IF NOT EXISTS
FOR (p:Person) REQUIRE p.canonical IS UNIQUE;
"""

def md5(s: str) -> str:
    import hashlib
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def main():
    ap = argparse.ArgumentParser(description="Merge Person aliases into canonical nodes using aliases.jsonl")
    ap.add_argument("aliases_jsonl", help="Path to aliases.jsonl (one record per line: {id,canonical,aliases})")
    ap.add_argument("--uri", default=os.environ.get("NEO4J_URI","bolt://localhost:7687"))
    ap.add_argument("--user", default=os.environ.get("NEO4J_USER","neo4j"))
    ap.add_argument("--password", default=os.environ.get("NEO4J_PASSWORD","password"))
    ap.add_argument("--dry-run", action="store_true", help="Parse file and report, but do not write changes")
    args = ap.parse_args()

    drv = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    with drv.session() as sess:
        # Ensure helpful constraints
        if not args.dry_run:
            for stmt in ENSURE_CONSTRAINTS.strip().split(";"):
                s = stmt.strip()
                if s:
                    sess.run(s)

        total = 0
        merged = 0
        with open(args.aliases_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"!! JSON error: {e}: {line[:140]}...", file=sys.stderr)
                    continue
                canonical = rec["canonical"]
                aliases   = [a for a in rec.get("aliases", []) if a and a != canonical]
                canonical_id = rec.get("id") or md5(canonical)

                total += 1
                if args.dry_run:
                    print(f"[DRY] {canonical}  <- {len(aliases)} aliases")
                    continue

                res = sess.run(MERGE_CYPHER, canonical=canonical, aliases=aliases, canonical_id=canonical_id).single()
                merged += 1
                if res:
                    print(f"✓ {res['canonical']}  aliases:{res['alias_count']}  node_id:{res['node_id']}")
                else:
                    print(f"✓ {canonical}  (no result row)")

        print(f"\nDone. Processed {total} canonical entries. {'(DRY RUN)' if args.dry_run else ''}")

if __name__ == "__main__":
    main()
