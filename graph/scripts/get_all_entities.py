import psycopg
import json

conn = psycopg.connect("user=alee dbname=tipitaka")

SQL = '''SELECT
    v.id,          
    v.identifier,    
    v.text,
    span.text  AS ner_text,
    span.label AS ner_label
    FROM ati_verses AS v
    CROSS JOIN LATERAL jsonb_to_recordset(v.ner_span) AS span
    (
        label text,
        text  text
    )
    WHERE span.text  = %s and span.label = 'PERSON' 
;'''

# wonky_phrases = (
#     "deva-king asked",
#     "Call",
#     "Six Sense-Bases",
#     "Buddhism",
#     "Quickly",
#     "Ambapali's grove",
#     "Subhadda went",
#     "venerable ones",
#     "King Udena's royal",
#     "Soon",
#     "Pāṭimokkha",
#     "Ven. Kimila went",
#     "Rains",
#     "Deva-king,",
#     "Kala-khemaka the Sakyan's dwelling",
#     "Soṇa Koṭikaṇṇa the",
#     "Saccaka the Nigaṇṭha-son,",
#     "deva-king. Gratified",
#     "Punnaka 's Question",
# )
wonky_phrases = [ "deva-king. Gratified" ]

with conn.cursor() as cur:
    for phrase in wonky_phrases:
        print(phrase)
        cur.execute(SQL, (phrase,))  # <- are you fucking kidding me with this syntax!?
        print([c for c in cur.fetchall()])