import json
from json import JSONDecodeError
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import Neo4jError
import psycopg

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "password")
CONN = psycopg.connect("dbname=tipitaka user=alee")

def load_from_file():
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        with open("./graph/entities/gold_people.jsonl", "r", encoding="utf-8") as f:
            for line in f:
                record = dict()
                try:
                    record = json.loads(line.strip())
                except JSONDecodeError as e:
                    print(e)
                    next
                q_parameters = {
                    "name" : record.get("name"),
                    "aliases": record.get("aliases")
                }
                cypher = "CREATE (p:Person {name: $name, aliases: $aliases, id: randomUUID() }) RETURN p;"
                try:
                    records, summary, keys = driver.execute_query(
                        cypher,
                        **q_parameters,
                    )
                except Neo4jError as e:
                    print(e)
                    next
                print(records)
                    
            driver.close()

def get_person(name, driver):
    query = """
        MATCH (p:Person)
            WHERE toLower(p.name) = toLower($q)
            OR any(a IN coalesce(p.aliases, []) WHERE toLower(a) = toLower($q))
        RETURN p{ .* , id: elementId(p) } AS person
        LIMIT 1
        """
    records, _, _ = driver.execute_query(query, q=name, database_="neo4j")
    return records    

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
records = get_person("Ānanda", driver=driver)

with CONN.cursor() as cur:
    for r in records:
        sql = """
            WITH terms(term) AS (
                SELECT unnest(ARRAY[ 'Ven. Ananda','Ānanda','Ananda' ]) 
            )
            SELECT
            s.identifier,
            CASE
                WHEN s.nikaya IN ('SN','AN')
                THEN format('%s %s.%s', s.nikaya, s.book_number, s.vagga)
                ELSE format('%s %s',     s.nikaya, s.book_number)
            END                                                   AS ref,
            COALESCE(
                NULLIF(v.elem->>'seq',''),
                NULLIF(v.elem->>'verse',''),
                NULLIF(v.elem->>'para',''),
                v.ord::text
            )                                                    AS verse_num,
            COALESCE(v.elem->>'text', v.elem->>'line', v.elem->>'para_text') AS text
                FROM ati_suttas s
            CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS v(elem, ord)
            JOIN terms t
            ON COALESCE(v.elem->>'text', v.elem->>'line', v.elem->>'para_text') ~* (
                '\m' || regexp_replace(t.term, '([.^$*+?()[{\|\\])', '\\\1', 'g') || '\M'
                )
            ORDER BY s.identifier,
                    COALESCE(
                    NULLIF(v.elem->>'seq',''),
                    NULLIF(v.elem->>'verse',''),
                    NULLIF(v.elem->>'para',''),
                    v.ord::text
                    )::int;
        """
        cur.execute(sql)
        rows = cur.fetchall()
        for (row,) in rows:
            cypher = "Match (p:Peron) where p.name = $1"