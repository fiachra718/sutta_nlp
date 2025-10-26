#!/usr/bin/env python3
import argparse, json, psycopg
from psycopg.rows import dict_row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="dbname=tipitaka user=alee")
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--min_len", type=int, default=200)
    ap.add_argument("--max_len", type=int, default=1200)
    args = ap.parse_args()

    gazetteer = [
        "Sāriputta","Sariputta","Ānanda","Ananda","Moggallāna","Moggallana",
        "Vesālī","Vesali","Sāvatthī","Savatthi","Rājagaha","Rajagaha","Rajgir",
        "Kosambī","Kosambi","Jetavana","Jeta's Grove","Veluvana","Pubbārāma",
        "Nigrodhārāma","Jīvakambavana","Ambapālivana","Gosinga","Kūṭāgārasālā"
    ]

    sql = """
    WITH para AS (
      SELECT s.identifier, s.title, (e.elem->>'text') AS ptext
      FROM ati_suttas s
      CROSS JOIN LATERAL jsonb_array_elements(s.verses) AS e(elem)
      WHERE s.nikaya IN ('MN','AN','SN')
    )
    SELECT identifier, title, ptext
    FROM para p
    WHERE length(ptext) BETWEEN %s AND %s
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(%s::text[]) AS term(term)
        WHERE p.ptext ILIKE '%%' || term || '%%'
      )
    ORDER BY random()
    LIMIT %s;
    """

    with psycopg.connect(args.dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (args.min_len, args.max_len, gazetteer, args.limit))
        rows = cur.fetchall()

    with open(args.out, "w", encoding="utf-8") as fout:
        for r in rows:
            rec = {
                "text": r["ptext"],
                "spans": [],
                "meta": {"identifier": r["identifier"], "title": r["title"], "source": "neg"}
            }
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"Wrote {len(rows)} negative examples -> {args.out}")

if __name__ == "__main__":
    main()