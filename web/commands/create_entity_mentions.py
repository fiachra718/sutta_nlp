import html
import unicodedata

import psycopg

conn = psycopg.connect("dbname=tipitaka user=alee")

SQL = """
SELECT id, ner_span
FROM ati_verses
WHERE ner_span IS NOT NULL;
"""

def normalize_name(s: str) -> str:
    s = s.strip()
    # strip common honorifics
    for prefix in ("Ven. ", "Venerable ", "the Venerable ", "Master ", "master "):
        if s.startswith(prefix):
            s = s[len(prefix):]
    s = html.unescape(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = unicodedata.normalize("NFC", s)
    return s.lower().strip()

batch_size = 100
count = 0

with conn.cursor() as cur:
    cur.execute(SQL)
    for verse_id, ner_span in cur.fetchall():
        for ent in ner_span:
            label = ent["label"]
            surface = ent["text"]
            if label not in ("PERSON", "GPE", "NORP", "LOC", "EVENT"):
                continue
            norm = normalize_name(surface)

            cur2 = conn.cursor()
            cur2.execute(
                """
                SELECT entity_id
                FROM v_entity_names
                WHERE normalized_name = %s
                LIMIT 1
                """,
                (norm,)
            )
            row = cur2.fetchone()
            if not row:
                continue
            entity_id = row[0]

            cur2.execute(
                """
                INSERT INTO ati_entity_mentions
                    (entity_id, verse_id, label, surface, start_char, end_char)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (entity_id, verse_id, label, surface,
                 ent.get("start"), ent.get("end"))
            )
            count += 1
            if count % batch_size == 0:
                conn.commit()
                print(f"Committed {count} rows")
conn.commit()
print(f"Done. Updated {count} rows.")
cur.close()
conn.close()      
