import psycopg

conn = psycopg.connect("dbname=tipitaka user=alee")


sql = '''SELECT
    v.identifier,
        ent->>'label' as entity_label,
    ent->>'text' AS entity_text
FROM ati_verses AS v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS ent
WHERE ent->>'label' = 'PERSON' ORDER BY random() LIMIT 1000;'''

people = set()

with conn.cursor() as cur:
    cur.execute(sql)
    for ident, label, text in cur.fetchall():
        people.add(text)

for p in sorted(people):
    print(p)