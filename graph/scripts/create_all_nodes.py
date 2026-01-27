from neo4j import GraphDatabase, basic_auth
import psycopg
from neo4j.exceptions import Neo4jError

'''
This is a naasty one-off script that will
probably be run a dozen times
Just nab entities and verses from
Posgres and create nodes and edges in
Neo
'''

# CONSTs
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "changeme")
CONN = psycopg.connect("dbname=tipitaka user=alee")

def db_entity_generator(entity_type='PERSON'):
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
                        WHEN COUNT(ea.alias) = 0 THEN NULL
                        ELSE jsonb_agg(
                            jsonb_build_object(
                                'canonical_alias', ea.alias,
                                'normalized_alias', ea.normalized
                            )
                        )
                    END
                )
            ) AS person
            FROM ati_entities e
            LEFT JOIN ati_entity_aliases ea
                ON ea.entity_id = e.id
            WHERE e.entity_type = '{entity_type}'
            GROUP BY e.id, e.canonical, e.normalized
            ORDER BY e.canonical """ 
        )
        for row in cur.fetchall():
            for elem in row:
                yield elem['canonical_name'], elem['normalized_name'], elem.get('aliases') or []

def db_verses():
    pass

def create_person_node(canonical, normalized, alias_list):
    print(canonical)
    print(normalized)
    print(alias_list) 

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        if not alias_list:
            alias_list = []
        summary = driver.execute_query(
            """
            MERGE (p:Person {name: $name})
            WITH p
            UNWIND $aliases AS a
            MERGE (al:Alias {canonical: a.canonical_alias, normalized: a.normalized_alias})
            MERGE (p)-[:HAS_ALIAS]->(al)
            """,
            name=canonical,
            aliases=alias_list,   # list of dicts is fine as a *parameter*
            database_="neo4j",
        ).summary
        print("Created {nodes_created} nodes in {time} ms.".format(
            nodes_created=summary.counters.nodes_created,
            time=summary.result_available_after
        ))
        driver.close()

def create_node(canonical, alias_list, node_type):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        assert node_type in ('PERSON', 'GPE', 'LOC', 'NORP')
        q = f"""
            MERGE (p:{node_type} {{name: $name}})
            WITH p
            UNWIND $aliases AS a
            MERGE (al:Alias {{canonical: a.canonical_alias, normalized: a.normalized_alias}})
            MERGE (p)-[:HAS_ALIAS]->(al)
            """
        summary = driver.execute_query(
            q, name=canonical, aliases=alias_list, database_="neo4j",
        ).summary
        print("Created {nodes_created} nodes in {time} ms.".format(
            nodes_created=summary.counters.nodes_created,
            time=summary.result_available_after
        ))
        driver.close()
    
def create_edge():
    pass

if __name__ == "__main__":
    # for name, n, alias_list in db_entity_generator():
    #     create_person_node(name, n, alias_list)
    # for name, normalized, alias_list in db_entity_generator(entity_type='GPE'):
    #     print(f'name: {name}, n: {normalized}, Aliases: {alias_list}')
    #     create_gpe_node(name, alias_list)

    # for name, normalized, alias_list in db_entity_generator(entity_type='LOC'):
    #     print(f'name: {name}, n: {normalized}, Aliases: {alias_list}')
    #     create_node(name, alias_list, node_type='LOC')
    
    for name, normalized, alias_list in db_entity_generator(entity_type='NORP'):
        print(f'name: {name}, n: {normalized}, Aliases: {alias_list}')
        create_node(name, alias_list, node_type='NORP')
    