from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "changeme")
DB = "neo4j"


def create_verse_node(tx, sutta_ref: str, gid: str, verse_num: int, text: str) -> dict:
    """
    verse_id == gid (e.g. 'an11.001.than.html:1')
    Returns both stable verse_id and internal elementId
    """
    cypher = """
    MERGE (v:Verse {verse_id: $gid})
    ON CREATE SET
      v.sutta_ref = $sutta_ref,
      v.number    = $verse_num,
      v.text      = $text
    ON MATCH SET
      v.sutta_ref = $sutta_ref,
      v.number    = $verse_num,
      v.text      = $text
    RETURN v.verse_id AS verse_id, elementId(v) AS neo_id
    """
    rec = tx.run(
        cypher,
        sutta_ref=sutta_ref,
        gid=gid,
        verse_num=int(verse_num),
        text=text,
    ).single()

    # rec is never None if MERGE succeeds
    return {"verse_id": rec["verse_id"], "neo_id": rec["neo_id"]}


def create_mentions_edge(tx, verse_id: str, entity_eid: str, label: str) -> dict:
    """
    Creates ONE MENTIONS edge per span (recommended).
    Uses verse_id property for verse lookup (stable), elementId for entity lookup (fast).
    """
    cypher = """
    MATCH (v:Verse {verse_id: $verse_id})
    MATCH (e) WHERE elementId(e) = $entity_eid
    MERGE (v)-[r:MENTIONS {start: $start, end: $end}]->(e)
    ON CREATE SET
      r.label     = $label,
      r.ref_count = 1
    ON MATCH SET
      r.ref_count = coalesce(r.ref_count, 0) + 1
    RETURN elementId(r) AS rel_id, r.ref_count AS ref_count
    """
    rec = tx.run(
        cypher,
        verse_id=verse_id,
        entity_eid=entity_eid,
        label=label,
    ).single()

    return {"rel_id": rec["rel_id"], "ref_count": rec["ref_count"]}


def resolve_entity_eid(tx, entity_type: str, canonical_name: str, aliases: list[str]) -> list[dict]:
    """
    Returns matching entity candidates (elementId + canonical + normalized).
    You can keep your existing resolver; this is a safe baseline.
    """
    cypher = """
    MATCH (e:Entity)
    WHERE e.entity_type = $entity_type
      AND (
        e.canonical_name = $canonical_name
        OR EXISTS {
          MATCH (e)-[:HAS_ALIAS]->(a:Alias)
          WHERE a.alias_text IN $aliases
        }
      )
    RETURN elementId(e) AS neo_id, e.canonical_name AS canonical, e.normalized AS normalized
    """
    res = tx.run(
        cypher,
        entity_type=entity_type,
        canonical_name=canonical_name,
        aliases=aliases or [],
    )
    return [r.data() for r in res]


def main():
    from create_all_nodes import verse_generator

    driver = GraphDatabase.driver(URI, auth=AUTH, max_connection_pool_size=50)

    # verify once
    driver.verify_connectivity()

    try:
        with driver.session(database=DB) as session:
            for sutta_ref, gid, verse_num, verse_text, ner_ref in verse_generator():

                # Create/update verse in a WRITE tx
                verse_row = session.execute_write(
                    lambda tx: create_verse_node(tx, sutta_ref, gid, verse_num, verse_text)
                )
                verse_id = verse_row["verse_id"]

                # For each NER block, resolve entity and create mention edge(s)
                for ne in ner_ref:
                    label = ne.get("label")
                    if label not in ("PERSON", "GPE", "LOC"):
                        continue

                    surface = ne["text"]

                    # Your resolver logic goes here. For now: try exact canonical match only.
                    candidates = session.execute_read(
                        lambda tx: resolve_entity_eid(tx, label, surface, [])
                    )
                    if not candidates:
                        # If you have create_entity logic, call it here, then set entity_eid
                        # Otherwise skip for now:
                        continue

                    # If multiple, pick first for now (or log for review)
                    entity_eid = candidates[0]["neo_id"]

                    # Create mention edge in a WRITE tx
                    edge_row = session.execute_write(
                        lambda tx: create_mentions_edge(tx, verse_id, entity_eid, label)
                    )

                    # Optional debug:
                    # print("MENTION:", verse_id, surface, "->", entity_eid, edge_row)

    except (ServiceUnavailable, SessionExpired) as e:
        print("Neo4j connection dropped mid-run:", e)
        print("If you're local, confirm URI is bolt:// and Neo4j is running.")
        raise
    finally:
        driver.close()


if __name__ == "__main__":
    main()
