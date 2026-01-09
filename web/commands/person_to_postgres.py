import psycopg
import json
import html
import unicodedata

CONN = psycopg.connect("dbname=tipitaka user=alee")
CONN.autocommit = True

def normalize_pali(s: str) -> str:
    s = html.unescape(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = unicodedata.normalize("NFC", s)
    return s.lower().strip()


SQL_find_entity = """
    SELECT id
    FROM ati_entities
    WHERE entity_type = %s AND normalized = %s
    LIMIT 1
"""
SQL_entities = """
    INSERT INTO ati_entities (entity_type, canonical, normalized, lang, source)
    VALUES ('PERSON', %s, %s, 'en', 'manual')
    RETURNING id
"""
SQL_find_alias = """
    SELECT id
    FROM ati_entity_aliases
    WHERE entity_id = %s AND normalized = %s
    LIMIT 1
"""
SQL_aliases = """
    INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
    VALUES (%s, %s, %s, 'manual')
"""
 
with open("/Users/alee/sutta_nlp/graph/entities/persons.json", "r", encoding="utf-8") as infile, \
        CONN.cursor() as cur:
    records = json.load(infile)
    for record in records:
        name = record["name"].strip()
        normalized_name = normalize_pali(name)
        cur.execute(SQL_find_entity, ("PERSON", normalized_name))
        row = cur.fetchone()
        if row:
            entity_id = row[0]
        else:
            cur.execute(SQL_entities, (name, normalized_name))
            entity_id = cur.fetchone()[0]
        print(entity_id)
        for alias in record.get("aliases", []):
            alias = alias.strip()
            if not alias:
                continue
            norm = normalize_pali(alias)
            cur.execute(SQL_find_alias, (entity_id, norm))
            if cur.fetchone():
                continue
            cur.execute(SQL_aliases, (entity_id, alias, norm))
cur.close()
CONN.close()
