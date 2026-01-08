import psycopg
import json
from collections import defaultdict
conn = psycopg.connect("dbname=tipitaka user=alee")

# three tables
# ati_entities
# ati_entity_aliases
# ati_verses

#  run the sql
#  build a list:
#  person[name] = [ aliases ]

SQL = """
    select 
        ae.id, 
        ae.canonical, 
        aea.alias 
    from 
        ati_entities as ae 
    LEFT JOIN 
        ati_entity_aliases as aea 
    on 
        aea.entity_id = ae.id 
    WHERE ae.entity_type = 'PERSON'
"""
select = """
    SELECT v.id, v.identifier, v.text
    FROM ati_verses v
    CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS ent
    WHERE ent->>'label' = 'PERSON'
        AND ent->>'text' IN ( %s );        
"""

graph_upsert = """

 """

aliases = defaultdict(set)
