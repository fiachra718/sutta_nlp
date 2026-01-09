import html
import json
import unicodedata

import psycopg

conn = psycopg.connect("dbname=tipitaka user=alee")
conn.autocommit = True


def normalize_pali(s: str) -> str:
    s = html.unescape(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = unicodedata.normalize("NFC", s)
    return s.lower().strip()


path = "./graph/entities/gpe.json"
insert_entity = """
    INSERT INTO ati_entities (entity_type, canonical, normalized, lang, source)
    VALUES (%s, %s, %s, %s, %s)
    RETURNING id
"""
insert_alias = """
    INSERT INTO ati_entity_aliases (entity_id, alias, normalized, source)
    VALUES (%s, %s, %s, %s)
"""
find_entity = "SELECT id FROM ati_entities WHERE entity_type = %s AND normalized = %s LIMIT 1"

with conn.cursor() as cur, open(path, "r", encoding="utf-8") as f:
    records = json.load(f)
    for record in records:
        loc_name = record["name"].strip()
        normalized_name = normalize_pali(loc_name)
        try:
            cur.execute(insert_entity, ("GPE", loc_name, normalized_name, "en", "manual"))
            entity_id = cur.fetchone()[0]
        except psycopg.errors.UniqueViolation as exc:
            print(f"Entity duplicate ({loc_name}): {exc}")
            cur.execute(find_entity, ("GPE", normalized_name))
            row = cur.fetchone()
            if not row:
                continue
            entity_id = row[0]

        for alias in record.get("aliases", []):
            alias = alias.strip()
            if not alias:
                continue
            norm = normalize_pali(alias)
            try:
                cur.execute(insert_alias, (entity_id, alias, norm, "manual"))
            except psycopg.errors.UniqueViolation:
                continue
