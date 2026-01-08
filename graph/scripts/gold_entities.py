import psycopg

CONN = psycopg.connect("user=alee dbname=tipitaka")

entities = set()

# for LABEL in ["LOC", "PERSON", "GPE", "NORP" ]:
LABEL = "NORP"    
entity_select = """ 
    SELECT substring
    (gt.text FROM (e->>'start')::int + 1 FOR ((e->>'end')::int - (e->>'start')::int))
    AS span_text FROM gold_training AS gt
    CROSS JOIN LATERAL jsonb_array_elements(gt.spans) AS e WHERE e->>'label' = %(label)s;"""
with CONN.cursor() as cur:
    for entity_label in ["LOC", "GPE", "PERSON", "NORP" ]: 
        print(entity_label) 
        cur.execute(entity_select, {"label": entity_label})
        for row in cur.fetchall():
            (text, ) = row
            entities.add(text)
        print( "\n".join(map(str, [f"\"{e}\", \"aliases\": []" for e in entities])) )
        entities = set()
