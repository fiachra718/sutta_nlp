import argparse
import json
from pathlib import Path

from neo4j import GraphDatabase


def fetch_center(tx, center_name: str):
    return tx.run(
        """
        MATCH (e:Entity {entity_type: 'PERSON'})
        WHERE toLower(e.canonical_name) = toLower($center_name)
        RETURN
          e.id AS id,
          e.canonical_name AS canonical_name,
          e.community_person_louvain AS community_id
        LIMIT 1
        """,
        center_name=center_name,
    ).single()


def fetch_nodes(tx, community_id: int):
    return tx.run(
        """
        MATCH (e:Entity {entity_type: 'PERSON', community_person_louvain: $community_id})
        OPTIONAL MATCH (e)-[r:CO_MENTION_PERSON]-(:Entity {entity_type: 'PERSON', community_person_louvain: $community_id})
        WITH e, coalesce(sum(r.weight), 0) AS strength
        RETURN
          e.id AS id,
          e.canonical_name AS label,
          e.pagerank AS pagerank,
          strength
        ORDER BY strength DESC, label
        """,
        community_id=community_id,
    ).data()


def fetch_edges(tx, community_id: int):
    return tx.run(
        """
        MATCH (a:Entity {entity_type: 'PERSON', community_person_louvain: $community_id})
              -[r:CO_MENTION_PERSON]-
              (b:Entity {entity_type: 'PERSON', community_person_louvain: $community_id})
        WHERE a.id < b.id
        RETURN
          a.id AS source,
          b.id AS target,
          r.weight AS weight
        ORDER BY weight DESC
        """,
        community_id=community_id,
    ).data()


def main():
    parser = argparse.ArgumentParser(description="Export a PERSON community subgraph for web visualization.")
    parser.add_argument("--uri", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="testtest")
    parser.add_argument("--database", default="neo4j")
    parser.add_argument("--community", type=int, default=23)
    parser.add_argument("--center", default="Buddha")
    parser.add_argument(
        "--out",
        default="web/app/static/data/community_23_buddha.json",
        help="Output JSON path",
    )
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    driver.verify_connectivity()

    with driver.session(database=args.database) as session:
        center = session.execute_read(fetch_center, args.center)
        if center is None:
            raise RuntimeError(f"Center node '{args.center}' not found.")

        requested_community = args.community
        effective_community = requested_community
        nodes = session.execute_read(fetch_nodes, effective_community)

        if not nodes:
            effective_community = center["community_id"]
            nodes = session.execute_read(fetch_nodes, effective_community)

        edges = session.execute_read(fetch_edges, effective_community)

    driver.close()

    center_id = center["id"]
    max_strength = max((n["strength"] or 0 for n in nodes), default=0)
    max_weight = max((e["weight"] or 0 for e in edges), default=0)

    payload = {
        "meta": {
            "requested_community": requested_community,
            "effective_community": effective_community,
            "center_label": center["canonical_name"],
            "center_id": center_id,
            "fallback_used": requested_community != effective_community,
            "max_strength": max_strength,
            "max_weight": max_weight,
        },
        "nodes": nodes,
        "edges": edges,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {out_path} nodes={len(nodes)} edges={len(edges)}")


if __name__ == "__main__":
    main()
