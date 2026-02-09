import psycopg
from psycopg.rows import dict_row
from neo4j import GraphDatabase
from normalize import load_alias_index_pg, resolve_span

'''
This is a NASTY one-off script that will
probably be run a dozen times
Just nab entities and verses from
Posgres and create nodes and edges in
Neo
'''

# CONSTs
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "testtest")
PG_DSN = "dbname=tipitaka user=alee"
CONN = psycopg.connect(PG_DSN, row_factory=dict_row)

driver = GraphDatabase.driver(URI, auth=AUTH, max_connection_pool_size=50)
driver.verify_connectivity()

#
# db_entity_generator
#
def db_entity_generator(entity_type='PERSON'):
    '''
    db_entity_generator runs a select that joins
    ati_entities to ati_entity_aliases on
    entity id.
    The sql builds a json object
    the generator yields that object:
     {
        canonical_name: <string>,
        normalized_name: <string>,
        aliases: [ "canonical_alias": <string>,
            normalized_alias: <string> 
        ]
     }
    :param entity_type: One of PERSON, GPE, LOC, NORP or throws TypeError
    '''
    if entity_type not in ('PERSON', 'GPE', 'LOC', 'NORP'):
        raise TypeError("invalid entity type")

    with CONN.cursor() as cur:
        cur.execute(    
            f"""SELECT
                jsonb_strip_nulls(
                    jsonb_build_object(
                    'canonical_name', e.canonical,
                    'normalized_name', e.normalized,
                    'aliases',
                    CASE
                        WHEN COUNT(ea.alias) FILTER (WHERE ea.alias IS NOT NULL) = 0 THEN NULL
                        ELSE jsonb_agg(
                            jsonb_build_object(
                                'canonical_alias', ea.alias,
                                'normalized_alias', ea.normalized
                            )
                        ) FILTER (WHERE ea.alias IS NOT NULL)
                    END
                    )
                ) AS entity,
                e.id AS entity_id
                FROM ati_entities AS e
                LEFT JOIN ati_entity_aliases ea ON ea.entity_id = e.id
                WHERE e.entity_type = '{entity_type}'
                GROUP BY e.id, e.entity_type, e.canonical, e.normalized
                ORDER BY e.canonical """ 
        )
        for row in cur.fetchall():
            elem = row["entity"]
            entity_id = row["entity_id"]
            aliases = elem.get("aliases")  # list[dict] or None

            # debug
            print(f"id: {entity_id}, element: {elem}, Alliases: {aliases}")
            # yield row.get('entity_id'), row.get('person'), row.get('aliases') # , row.get('canonical_name'), row.get('aliases') 
            yield entity_id, elem, aliases

# def verse_generator():
#     '''
#     SO, we select ALL verses from ati_verse
#     But we number tham so that they look Canonical
#     AN 12.7, SN 23.1, DN 2, etc.
#     To get this we use a SQL CASE statement

#     But the verse number is needed.  THe pair, 
#     (canonical ref, verse number) are unqiue up to 
#     translator
#     ''' 
#     sql = '''
#     SELECT
#         v.id as verse_id,
#         v.gid as global_id,
#         CASE
#         WHEN v.nikaya IN ('AN','SN') THEN v.nikaya || ' ' || v.vagga || '.' || v.book_number
#         WHEN v.nikaya IN ('MN','DN') THEN v.nikaya || ' ' || v.book_number
#         WHEN v.nikaya = 'KN'          THEN v.vagga || ' ' || v.book_number
#         ELSE                               v.nikaya || ' ' || v.book_number
#         END AS sutta_ref,
#         v.verse_num as verse_num,
#         v.text as text,
#         v.ner_span as ner_span
#     FROM ati_verses AS v
#     WHERE length(v.text) > 128
#         AND v.ner_span IS NOT NULL
#         AND jsonb_array_length(v.ner_span) > 0
#         ORDER BY v.nikaya, v.book_number,
#             nullif(regexp_replace(v.verse_num::text, '\D.*$', ''), '')::int
#     '''
#     with CONN.cursor() as cur:
#         cur.execute(sql)
#         for row in cur.fetchall():
#             yield (row.get('verse_id'), 
#                 row.get('global_id'), 
#                 row.get('sutta_ref'), 
#                 row.get('verse_num'), 
#                 row.get('text'), 
#                 row.get('ner_span'))


#
#  verse_generator
#
def verse_generator(batch_size=2000):
    sql = r"""
    SELECT
        v.id   AS verse_id,
        v.gid  AS global_id,
        CASE
          WHEN v.nikaya IN ('AN','SN') THEN v.nikaya || ' ' || v.vagga || '.' || v.book_number
          WHEN v.nikaya IN ('MN','DN') THEN v.nikaya || ' ' || v.book_number
          WHEN v.nikaya = 'KN'         THEN v.vagga  || ' ' || v.book_number
          ELSE                              v.nikaya || ' ' || v.book_number
        END AS sutta_ref,
        v.verse_num,
        v.text,
        v.ner_span
    FROM ati_verses v
    WHERE length(v.text) > 128
      AND v.ner_span IS NOT NULL
      AND jsonb_array_length(v.ner_span) > 0
    ORDER BY
      v.nikaya,
      v.book_number,
      nullif(regexp_replace(v.verse_num::text, E'\\D.*$', ''), '')::int,
      v.verse_num,
      v.id;
    """

    with CONN.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                yield (
                    row["verse_id"],
                    row["global_id"],
                    row["sutta_ref"],
                    row["verse_num"],
                    row["text"],
                    row["ner_span"],
                )


# ------------------
#  create_entity_node
# ------------------
def create_entity_node(driver, entity_type, entity_id, canonical_name, normalized_name, aliases):
    """
    """ 
    assert entity_type in ("PERSON", "GPE", "LOC", "NORP")

    query = """
    MERGE (e:Entity {id: $entity_id})
    ON CREATE SET
      e.entity_id      = $entity_id,
      e.entity_type    = $entity_type,
      e.canonical_name = $canonical_name,
      e.display_name   = $canonical_name,
      e.normalized     = $normalized_name
    ON MATCH SET
      e.entity_id      = coalesce(e.entity_id, $entity_id),
      e.entity_type    = $entity_type,
      e.canonical_name = coalesce(e.canonical_name, $canonical_name),
      e.display_name   = coalesce(e.display_name, $canonical_name),
      e.normalized     = coalesce(e.normalized, $normalized_name)
    RETURN e
    """
    res = driver.execute_query(
        query,
        entity_id=entity_id,
        entity_type=entity_type,
        canonical_name=canonical_name,
        normalized_name=normalized_name,
        database_="neo4j",
    )
    summary = res.summary
    print(
        f"nodes_created={summary.counters.nodes_created}, "
        f"rels_created={summary.counters.relationships_created}, "
        f"props_set={summary.counters.properties_set}, "
        f"time_ms={summary.result_available_after}"
    )

# -------------------
#  create_verse_node
# -------------------
def create_verse_node(driver, verse_id, global_id, sutta_ref, verse_num, text):
    """
    Pass in the verse_id (reference id for ati_verses, e.g. an07.049.than.html:25)
    sutta ref (canonical reference to Nikaya or vagga and sutta number, e.g. AN 7.49)
    verse_num -- the count of the verse (Suttas have multiple verses)
    text is the raw text from postgres
    """
    cypher = """
        MERGE (s:Sutta {sutta_ref: $sutta_ref})
        ON CREATE SET
            s.display_name = $sutta_ref
        ON MATCH SET
            s.display_name = coalesce(s.display_name, $sutta_ref)

        MERGE (v:Verse {id: $verse_id})
        ON CREATE SET
            v.verse_id = $verse_id,
            v.global_id = $global_id,
            v.sutta_ref = $sutta_ref,
            v.number = $verse_num,
            v.text = $text
        ON MATCH SET
            v.verse_id = coalesce(v.verse_id, $verse_id),
            v.sutta_ref = $sutta_ref,
            v.number = $verse_num,
            v.text = $text

        MERGE (s)-[:HAS_VERSE]->(v)
        RETURN v.id as verse_id
    """
    # driver.verify_connectivity()
    res = driver.execute_query(
        cypher,
        verse_id=verse_id,
        global_id=global_id,
        sutta_ref=sutta_ref,
        verse_num=verse_num,
        text=text,
        database_="neo4j",
    )
    print(
        f"nodes_created={res.summary.counters.nodes_created}, "
        f"rels_created={res.summary.counters.relationships_created}, "
        f"props_set={res.summary.counters.properties_set}, "
        f"time_ms={res.summary.result_available_after}"
    )
    verse_id = res.records[0]["verse_id"]
    return verse_id


####################################################
##
##  create_mention_edge
##
####################################################
def create_mention_edge(
    driver,
    verse_id,
    entity_id,
    label,
    surface,
    normalized,
    span_key,
    start=None,
    end=None,
):
    """
    Create one deterministic MENTIONS edge per extracted span.
    Matching is on stable IDs: Verse.verse_id and Entity.entity_id.
    """
    cypher = """
        MERGE (v:Verse {id: $verse_id})
        MERGE (e:Entity {id: $entity_id})
        MERGE (v)-[r:MENTIONS {ner_label: $label}]->(e)
        ON CREATE SET r.ref_count = 1
        ON MATCH SET r.ref_count = r.ref_count + 1
        RETURN v.id as verse_id, r.ref_count AS ref_count
    """
    # driver.verify_connectivity()
    res = driver.execute_query(
        cypher,
        verse_id=verse_id,
        entity_id=entity_id,
        span_key=span_key,
        label=label,
        surface=surface,
        normalized=normalized,
        start=start,
        end=end,
        database_="neo4j",
    )
    print(
        f"nodes_created={res.summary.counters.nodes_created}, "
        f"rels_created={res.summary.counters.relationships_created}, "
        f"props_set={res.summary.counters.properties_set}, "
        f"time_ms={res.summary.result_available_after}"
    )
    return res.records


##########################
##
## main
##
##########################
if __name__ == "__main__":
    alias_index, collisions = load_alias_index_pg(PG_DSN)
    manual_review = []
    stats = {
        "verses_seen": 0,
        "spans_seen": 0,
        "resolved": 0,
        "ambiguous": 0,
        "unresolved": 0,
        "invalid": 0,
        "edges_created_or_matched": 0,
    }

    for e_type in ['PERSON', 'GPE', 'NORP', 'LOC']:
        for (entity_id, elem, aliases) in db_entity_generator(entity_type=e_type):
            print(f"id: {entity_id}, name: {elem['canonical_name']}, norm: {elem['normalized_name']}, Aliases: {aliases}")
            create_entity_node(driver, e_type, entity_id, elem['canonical_name'], elem['normalized_name'], aliases)
    for verse_id, global_id, sutta_ref, verse_num, text, ner_ref in verse_generator():
        stats["verses_seen"] += 1
        print(f"verse_id: {verse_id}, gid: {global_id}, sutta ref: {sutta_ref}, verse number: {verse_num}")
        assert verse_id is not None
        create_verse_node(driver, verse_id, global_id, sutta_ref, verse_num, text)
        for ne_block in ner_ref:
            label = ne_block.get("label")
            if label not in ("PERSON", "GPE", "LOC", "NORP"):
                continue

            stats["spans_seen"] += 1
            surface = ne_block.get("text", "")
            norm = resolve_span(alias_index, collisions, label, surface)["normalized"]
            key = (label, norm)
            entity_id = alias_index.get(key)

            if key in collisions:
                stats["ambiguous"] += 1
                manual_review.append(
                    {
                        "verse_id": verse_id,
                        "label": label,
                        "surface": surface,
                        "normalized": norm,
                        "candidates": collisions[key],
                    }
                )
                continue
            if not entity_id:
                stats["unresolved"] += 1
                continue

            stats["resolved"] += 1

            create_mention_edge(
                driver=driver,
                verse_id=verse_id,
                entity_id=entity_id,
                label=label,
                surface=surface,
                normalized=norm,
                span_key=f"{label}:{ne_block.get('start', '')}:{ne_block.get('end', '')}:{norm}",
                start=ne_block.get("start"),
                end=ne_block.get("end"),
            )
            stats["edges_created_or_matched"] += 1

    print("Done.")
    print(stats)
    if manual_review:
        print(f"manual_review_count={len(manual_review)}")
    driver.close()
