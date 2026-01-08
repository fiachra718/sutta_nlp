import psycopg

conn = psycopg.connect("dbname=tipitaka user=alee")

gpe = set()
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
    WHERE span.label = 'PERSON' OR span.label = 'GPE'  '''

with conn.cursor() as cur:
    cur.execute(SQL)
    for ner_label, ner_text in cur.fetchall():
        if ner_label == 'GPE':
            gpe.add(ner_text)
        elif ner_label == 'PERSON':
            person.add(ner_text)

for elem in person - gpe:
    print(f'in person but NOT gpe: {elem}')

for elem in gpe - person:
    print(f'in gpe but NOT person: {elem}')

for elem in gpe & person:
    print(f'{elem} is in BOTH')
