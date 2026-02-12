# read a verse from ati_verse
# run it through sutta-ner
# write back the span json as a JSONB field

import spacy
import psycopg
import json

conn = psycopg.connect("dbname=tipitaka user=alee")
conn.autocommit = False

nlp = spacy.load("en_sutta_ner")  # should be 1.2.4
assert nlp.meta.get("version") == "1.2.4", "Wrong en_sutta_ner version installed!"


select = "SELECT id, text FROM ati_verses"
update = "UPDATE ati_verses SET ner_span = %s WHERE id = %s" 

batch_size = 100
count = 0

cur = conn.cursor()
cur.execute(select)
rows = cur.fetchall()
for id, text in rows:
    doc = nlp(text)
    spans = []
    for ent in doc.ents:
        spans.append(
            {"start": ent.start_char, "end": ent.end_char, "label": ent.label_, "text": ent.text}
        )
    cur.execute(update, (json.dumps(spans, ensure_ascii=False), id))  
    count += 1
    if count % batch_size == 0:
        conn.commit()
        print(f"Committed {count} rows")
conn.commit()
print(f"Done. Updated {count} rows.")
cur.close()
conn.close()      