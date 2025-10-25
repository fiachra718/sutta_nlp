import json
from local_settings import load_model
from hashlib import md5
import psycopg
from psycopg.rows import dict_row


conn = psycopg.connect("dbname=tipitaka user=alee")

def random_sutta_paragraph():
    with conn.cursor(row_factory=dict_row) as cur:
        sql = """ 
            SELECT 
                identifier, 
                nikaya, 
                vagga, 
                book_number, 
                title,
                verses
                    FROM ati_suttas WHERE nikaya in ( 'MN', 'DN', 'AN', 'SN' )
                ORDER BY random()
                LIMIT 250
            """
        cur.execute(sql)
        return cur.fetchall()

def ne_tag(text, nlp, tag="ALL"):
    doc = nlp(text)
    results = {}
    entities = []
    for ent in doc.ents:
        # if tag == "ALL":
        entities.append( (ent.label_, ent.text ) )
            # {"start": ent.start_char, "end": ent.end_char,"label": ent.label_, "text": ent.text})
        # else:
        #     if tag == ent.label_:
        #         entities.append({"start": ent.start_char, "end": ent.end_char,"label": ent.label_, "text": ent.text})
        if len(entities):
            results = {"text": text, "entities": [ent for ent in entities]}
       
    return results


nlp = load_model()
verses = random_sutta_paragraph()
# print(verses)
for verse in verses:
    if len(verse.get("verses")):
        # print(json.dumps(verse["verses"][0]["text"].strip(), indent=2, ensure_ascii=False))
        text = verse["verses"][0]["text"]
        jsonl = ne_tag(text.strip(),nlp, tag="ALL")
        if jsonl:
            print(json.dumps(jsonl, indent=2, ensure_ascii=False))
