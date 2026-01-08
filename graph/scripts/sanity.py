import spacy
from spacy.training import Example
import psycopg
import json
from psycopg.rows import dict_row
import html, unicodedata

nlp = spacy.load("en_sutta_ner")
CONN = psycopg.connect("dbname=tipitaka user=alee")

def normalize_text(s: str) -> str:
    s = s.encode('utf-8', 'backslashreplace').decode('unicode_escape')
    s = html.unescape(s)
    s = unicodedata.normalize('NFC', s)
    return ' '.join(s.split())

def random_sutta_paragraph(conn):
    with conn.cursor(row_factory=dict_row) as cur:
        sql = """ 
            SELECT s.identifier, s.title, t.ord AS verse_num, t.v->>'text' AS verse_text
            FROM ati_suttas s
            CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS t(v, ord)
            WHERE s.nikaya IN ('MN','DN','AN','SN','KN')
            AND char_length(t.v->>'text') > 200
            ORDER BY gen_random_uuid()
            LIMIT 20;
            """
        cur.execute(sql)
        return cur.fetchall()

def ne_tag(text, nlp):
    doc = nlp(text)
    results = {}
    entities = []
    for ent in doc.ents:
        entities.append( [ent.label_, ent.text] )
        # entities.append({"start": ent.start_char, "end":ent.end_char, "label":ent.label_, "text":ent.text})
        if len(entities):
            results = {"text": text, "entities": [ent for ent in entities]}
       
    return results

verses = random_sutta_paragraph(CONN)
for verse in verses:
    if len(verse.get("verse_text")):
        jsonl = ne_tag(verse["verse_text"].strip(), nlp)
        if jsonl:
            print(json.dumps(jsonl, indent=2, ensure_ascii=False))