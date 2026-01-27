import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

from web.app.db import db


BATCH_SIZE = 500

UPDATE_SQL = """
WITH cleaned AS (
    SELECT id,
           regexp_replace(unaccent(lower(text)), '[^a-z0-9]+', ' ', 'g') AS cleaned_text
    FROM ati_verses
    WHERE id = ANY(%(ids)s)
)
UPDATE ati_verses AS v
SET cleaned_text = c.cleaned_text,
    cleaned_text_hash = md5(c.cleaned_text)
FROM cleaned c
WHERE v.id = c.id
"""


def main():
    conn = psycopg.connect(db.default_dsn())
    total = 0
    with conn.cursor() as select_cur, conn.cursor() as update_cur:
        select_cur.execute("SELECT id FROM ati_verses ORDER BY id")
        while True:
            rows = select_cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            ids = [row[0] for row in rows]
            update_cur.execute(UPDATE_SQL, {"ids": ids})
            total += len(ids)
            conn.commit()
            print(f"Updated {total} verses")
    conn.close()


if __name__ == "__main__":
    main()
