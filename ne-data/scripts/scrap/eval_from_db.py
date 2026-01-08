# export_gold.py  — run:  python export_gold.py
import json, psycopg, spacy
from spacy.tokens import DocBin
from spacy.training import Example

conn = psycopg.connect("")              # uses PG* env / .pgpass
cur  = conn.cursor(cursor_factory=DictCursor)
cur.execute("SELECT text, spans FROM gold_training")

nlp = spacy.blank("en")
db  = DocBin(store_user_data=True)

for row in cur:
    text  = row["text"] or ""
    spans = row["spans"]
    if isinstance(spans, str):
        try: spans = json.loads(spans)
        except: spans = []
    ents = []
    n = len(text)
    for s in spans or []:
        try:
            a, b, L = int(s["start"]), int(s["end"]), str(s["label"])
            if 0 <= a < b <= n: ents.append((a,b,L))
        except: pass
    doc = nlp.make_doc(text)
    eg  = Example.from_dict(doc, {"entities": ents})
    db.add(eg.reference)

db.to_disk("gold.spacy")
cur.close(); conn.close()
print("Wrote gold.spacy")