import psycopg
import json
import html
import unicodedata


def normalize_pali(s: str) -> str:
    s = html.unescape(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = unicodedata.normalize("NFC", s)
    return s.lower().strip()


SQL = ''' INSERT INTO ati_entities (entity_type, canonical, normalized, lang, source) 
            VALUES ('PERSON', %s, %s, 'en', 'manual')
        '''

with open("/Users/alee/sutta_nlp/graph/entities/gold_people.jsonl", "r", encoding="utf-8") as infile:
    for line in infile:
        try:
            record = json.loads(line.strip())
        except json.JSONDecodeError as e:
            print(e)
            continue
        name = record.get("name", "")
        normalizeed_name = normalize_pali(name)
        