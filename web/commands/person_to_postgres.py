import psycopg
import json
import html
import unicodedata

CONN = psycopg.connect("dbname=tipitaka user=alee")

def normalize_pali(s: str) -> str:
    s = html.unescape(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = unicodedata.normalize("NFC", s)
    return s.lower().strip()


SQL_entities = ''' INSERT INTO ati_entities (entity_type, canonical, normalized, lang, source) 
            VALUES ('PERSON', %s, %s, 'en', 'manual')
            RETURNING id; '''
SQL_aliases = ''' INSERT INTO ati_entity_aliases ( entity_id, alias, normalized, source )
                    VALUES(%s, %s, %s, 'manual')'''
 
with open("/Users/alee/sutta_nlp/graph/entities/gold_people.jsonl", "r", encoding="utf-8") as infile ,\
         CONN.cursor() as cur:
    for line in infile.readlines():
        try:
            record = json.loads(line.strip())
            print(record)
        except json.JSONDecodeError as e:
            print(e)
            break
        name = record["name"]
        normalized_name = normalize_pali(name)
        cur.execute(SQL_entities, (name, normalized_name))
        entity_id = cur.fetchone()[0]
        print(entity_id)
        for alias in record["aliases"]:
            norm = normalize_pali(alias)
            cur.execute(SQL_aliases, (entity_id, alias, norm))
CONN.commit()
cur.close()
CONN.close()
