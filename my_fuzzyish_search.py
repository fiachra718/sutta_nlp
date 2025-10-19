import psycopg
from psycopg.rows import dict_row
import json
import sys


term = sys.argv[1]
if not term:
    sys.exit(-1)

conn = psycopg.connect("dbname=tipitaka user=alee")
sql = """
          WITH q AS (
            SELECT plainto_tsquery('english', %(term)s) AS tsq
            ),
            para AS (
            SELECT
                s.id,
                s.nikaya,
                s.identifier,
                s.title,
                (e.elem->>'text') AS ptext
            FROM ati_suttas s
            CROSS JOIN LATERAL jsonb_array_elements(s.verses) AS e(elem)
            )
            SELECT
            p.nikaya,
            p.identifier,
            p.title,
            COUNT(*) AS matched_paragraphs,
            /* PG14- fallback: (length(h.hl)-length(replace(h.hl,'<<','')))/2 */
            SUM(regexp_count(h.hl, '<<')) AS total_hits
            FROM para p
            JOIN q ON to_tsvector('english', p.ptext) @@ q.tsq
            CROSS JOIN LATERAL (
            SELECT ts_headline(
                    'english',
                    p.ptext,
                    q.tsq,
                    'StartSel=<<, StopSel=>>, HighlightAll=TRUE, MaxFragments=100000, MaxWords=100000, MinWords=1'
                    ) AS hl
            ) AS h
            GROUP BY p.nikaya, p.identifier, p.title
            ORDER BY total_hits DESC, matched_paragraphs DESC, p.identifier;
                   """
params = {"term": term}
with conn.cursor(row_factory=dict_row) as cur:
    cur.execute(sql, params)
    rows = cur.fetchall()
    print(json.dumps(rows, indent=2, ensure_ascii=False))
