from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import Neo4jError

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "changeme")

with open("./graph/entities/norp_entities.txt", encoding='utf-8') as f:
    for line in f:
        name = line.strip()    
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
            q = f""" CREATE (p:NORP {{name: $name}}) """
            summary = driver.execute_query(
                q, name=name, database_="neo4j",
            ).summary
            print("Created {nodes_created} nodes in {time} ms.".format(
                    nodes_created=summary.counters.nodes_created,
                    time=summary.result_available_after
                ))
    driver.close()