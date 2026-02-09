from neo4j import GraphDatabase, basic_auth
import psycopg
from psycopg.rows import dict_row
from neo4j.exceptions import Neo4jError
from dataclasses import dataclass

@dataclass(frozen=True)
class Entity:
    id: int
    canonical: str
    canonical_norm: str
    entity_type: str


URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "changeme")
CONN = psycopg.connect("dbname=tipitaka user=alee", row_factory=dict_row)


def get_name_ref():
    entities_by_id = {}
    sql = '''
        SELECT 
            ae.id,
            ae.entity_type,
            ae.canonical as canonical, 
            ae.normalized as normalized, 
        COALESCE(
            array_agg(DISTINCT aea.alias) FILTER (WHERE aea.alias IS NOT NULL),
            ARRAY[]::text[]
        ) AS aliases,
        COALESCE(
            array_agg(DISTINCT aea.normalized) FILTER (WHERE aea.normalized IS NOT NULL),
            ARRAY[]::text[]
        ) AS alias_norms
        from ati_entities as ae LEFT JOIN ati_entity_aliases as aea 
        on aea.entity_id = ae.id
        GROUP BY ae.id, ae.canonical
        ORDER BY ae.id
    '''
    with CONN.cursor() as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            entities_by_id[row['id']].append(
                row['type'], row['canonical'], row['normalized'], row['aliases'], row['alias_norms']
            )
    
    return entities_by_id


"""
select verses that are longer than 128 characters 
and have a populated ner_span field
"""

SQL = """
  SELECT
    v.identifier,
    CASE
      WHEN v.nikaya IN ('AN','SN') THEN v.nikaya || ' ' || v.vagga || '.' || v.book_number
      WHEN v.nikaya IN ('MN','DN') THEN v.nikaya || ' ' || v.book_number
      WHEN v.nikaya = 'KN'          THEN v.vagga || ' ' || v.book_number
      ELSE                               v.nikaya || ' ' || v.book_number
    END AS sutta_ref,
    v.verse_num,
    v.text,
    v.ner_span
  FROM ati_verses AS v
  WHERE length(v.text) > 128
    AND v.ner_span IS NOT NULL
    AND jsonb_array_length(v.ner_span) > 0
    ORDER BY v.nikaya, v.book_number,
         nullif(regexp_replace(v.verse_num::text, '\D.*$', ''), '')::int
  """

"""
Loop over these and insert the Verse into the Graph
with label = sutta_ref, text = v.text
Then switch CASE of ner_span.label:
    For node type:  
        MATCH (p:$NodeType {canonical : $s}<-[:MENTIONS]-(v:Verse {canonical: AA nn.nn})
"""

def main():
    names = get_name_ref()
    count = 0
    with CONN.cursor() as cur, GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        cur.execute(SQL)
        for row in cur:
            canonical_id = row.get('sutta_ref')
            verse_num = row.get('verse_num')
            # identifier = row.get('identifier')
            ner_span = row.get('ner_span')
            text = row.get('text')
            print(canonical_id, verse_num, text)
            print(f"Canonical Rep: {canonical_id}, Verse Number: {verse_num}")
            for e in ner_span:
                graph_query = """
                    MERGE (v:Verse {id: $verse_id})
                    ON CREATE SET v.sutta_ref = $sutta_ref, v.verse_num = $verse_num, v.text = $text;
                """
                name = e.get('name')
                check_name(name)
                if e.get('label') == 'NORP':
                    graph_query += """
                        MERGE (p:NORP {name: $name})
                        MERGE (v)-[:MENTIONS]->(p);
                        """
                elif e.get('label') == 'LOC':
                    graph_query += """
                        MERGE (p:LOC {name: $name})
                        MERGE (v)-[:MENTIONS]->(p);
                        """
                elif e.get('label') == 'GPE':
                    graph_query += """
                        MERGE (p:GPE {name: $name})
                        MERGE (v)-[:MENTIONS]->(p);
                        """
                elif e.get('label') == 'PERSON':
                    graph_query += """
                        MERGE (p:Person {canonical: $name})
                        MERGE (v)-[:MENTIONS]->(p);
                        """
                    
            count += 1

            if count == 20:
                break
