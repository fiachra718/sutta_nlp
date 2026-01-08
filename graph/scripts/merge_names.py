from neo4j import GraphDatabase
import json
from json import JSONDecodeError
from neo4j.exceptions import Neo4jError


# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "changeme")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Connection established.")
    with open("./graph/entities/people.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            print(line)
            try:
                record = json.loads(line.strip())
            except JSONDecodeError as e:
                print(e)
            with driver.session() as session:
                parameters = {
                    "name": record["name"],
                    "aliases": [],
                    "verses": record["verses"]
                }
                try:
                    result =  session.run(
                            "MERGE (:Person { name: $name } )",
                            **parameters,
                        )
                except Neo4jError as e:
                    print(e)
                summary = result.consume()   
                print(summary.counters.nodes_created, result.data)