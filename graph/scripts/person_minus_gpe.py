import psycopg

conn = psycopg.connect("dbname=tipitaka user=alee")

gpe = set()
loc = set()
person = set()

SQL = '''SELECT
    span.label AS ner_label,
    span.text as ner_text
    FROM ati_verses as v
    CROSS JOIN LATERAL jsonb_to_recordset(v.ner_span) AS span
    (
        label text,
        text  text
    )
    WHERE span.label = 'LOC' OR span.label = 'GPE' OR span.label = 'PERSON' '''

with conn.cursor() as cur:
    cur.execute(SQL)
    for ner_label, ner_text in cur.fetchall():
        if ner_label == 'GPE':
            gpe.add(ner_text)
        elif ner_label == 'LOC':
            loc.add(ner_text)
        elif ner_label == 'PERSON':
            person.add(ner_text)

for elem in loc - gpe:
    print(f'in loc but NOT gpe: {elem}')

for elem in gpe - person:
    print(f'in gpe but NOT person: {elem}')

for elem in gpe & loc:
    print(f'{elem} is in BOTH LOC and GPE')

for elem in person & gpe & loc:
    print(f'{elem} is in ALL LOC and GPE and PERSON')
