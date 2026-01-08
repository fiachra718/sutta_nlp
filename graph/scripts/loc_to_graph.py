import re
import json
from json import JSONDecodeError
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
import csv

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "password")

loc_pattern = re.compile(r'^')
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    # driver.verify_connectivity()
    with open("./graph/entities/loc.csv", "r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            location = row["LOC"]
            aliases = row["aliases"] if row.get("aliases") else []
            try:
                l = [e for e in aliases]
                print(l)
                if len(l):
                    l_al = json.loads(l)
                
            except JSONDecodeError as e:
                print(f"{e}, {aliases}")
                continue
            
            # params = { "name" : location }
            # cypher = "CREATE (l:Location {name: $name, id: apoc.create.uuid() }) RETURN l;"
            # try:
            #     records, summary, keys = driver.execute_query(
            #         cypher,
            #         **params                
            #     )
            # except Neo4jError as e:
            #     print(e)
            #     continue
            # for r in records:
            #     print(r)