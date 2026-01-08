from neo4j import GraphDatabase

URI  = "bolt://localhost:7687"   # or neo4j://localhost:7687
AUTH = ("neo4j", "password")     # you set this in docker run

driver = GraphDatabase.driver(URI, auth=AUTH)

def iter_people(batch=200):
    skip = 0
    with driver.session() as s:
        while True:
            res = s.run("""
                MATCH (p:Person)
                OPTIONAL MATCH (p)-[:MENTIONED_IN]->(v:Verse)
                WITH p, collect(v.id) AS verses
                RETURN p.id AS id, p.canonical AS name, p.aliases AS aliases, verses
                ORDER BY name
                SKIP $skip LIMIT $limit
            """, skip=skip, limit=batch)
            rows = list(res)
            if not rows:
                break
            for r in rows:
                yield r
            skip += batch

for r in iter_people():
    print(r["name"], r["verses"])


with driver.session() as s:
    # counts
    counts = s.run("""
        MATCH (p:Person) RETURN count(p) AS persons;
    """).single()
    print("persons:", counts["persons"])

    counts = s.run("""
        MATCH (v:Verse) RETURN count(v) AS verses;
    """).single()
    print("verses:", counts["verses"])

    # get a few people and their verse ids
    result = s.run("""
        MATCH (p:Person)-[:MENTIONED_IN]->(v:Verse)
        RETURN p.id AS id, p.canonical AS name, p.aliases AS aliases, collect(v.id) AS verses
        ORDER BY name
        LIMIT 10
    """)
    for row in result:
        print(row["name"], row["verses"])

import pandas as pd
with driver.session() as s:
    df = s.run("""
        MATCH (p:Person)-[:MENTIONED_IN]->(v:Verse)
        RETURN p.canonical AS name, v.identifier AS ident, v.verse_num AS vnum
        ORDER BY name, ident, vnum
    """).to_df()
print(df.head())
