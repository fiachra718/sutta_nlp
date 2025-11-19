import json
from local_settings import load_model
import psycopg
from psycopg.rows import dict_row


conn = psycopg.connect("dbname=tipitaka user=alee")

import html, unicodedata

def normalize_text(s: str) -> str:
    s = s.encode('utf-8', 'backslashreplace').decode('unicode_escape')
    s = html.unescape(s)
    s = unicodedata.normalize('NFC', s)
    return ' '.join(s.split())

def random_sutta_paragraph():
    with conn.cursor(row_factory=dict_row) as cur:
        sql = """ 
            SELECT s.identifier, s.title, t.ord AS verse_num, t.v->>'text' AS verse_text
            FROM ati_suttas s
            CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS t(v, ord)
            WHERE s.nikaya IN ('MN','DN','AN','SN','KN')
            AND char_length(t.v->>'text') > 200
            ORDER BY gen_random_uuid()
            LIMIT 150;
            """
        cur.execute(sql)
        return cur.fetchall()

def ne_tag(text, nlp, tag="ALL"):
    doc = nlp(text)
    results = {}
    entities = []
    for ent in doc.ents:
        # entities.append( [ent.label_, ent.text] )
        entities.append({"start": ent.start_char, "end":ent.end_char, "label":ent.label_, "text":ent.text})
        if len(entities):
            results = {"text": text, "spans": [ent for ent in entities]}
       
    return results


nlp = load_model()
verses = random_sutta_paragraph()
# print(verses)
for verse in verses:
    text = (verse.get("verse_text") or "").strip()
    if not text:
        continue
    jsonl = ne_tag(text, nlp, tag="ALL")
    if jsonl:
        jsonl["identifier"] = verse.get("identifier")
        jsonl["title"] = verse.get("title")
        jsonl["verse_num"] = verse.get("verse_num")
        print(json.dumps(jsonl, indent=2, ensure_ascii=False))
