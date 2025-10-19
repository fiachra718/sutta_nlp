import psycopg
# from psycopg.rows import dict_row
# import json
import sys
import unicodedata

conn = psycopg.connect("dbname=tipitaka user=alee")

term = sys.argv[1]
if not term:
    sys.exit(-1)


def strip_diacritics(s):
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")



sql = """
    WITH q AS (
    SELECT websearch_to_tsquery('english', %(term)s) AS tsq
    ),
    para AS (
    SELECT
        s.nikaya,
        s.identifier,
        s.title,
        (e.elem->>'text') AS ptext
    FROM ati_suttas s
    CROSS JOIN LATERAL jsonb_array_elements(s.verses) AS e(elem)
    ),
    hits AS (
    SELECT p.nikaya, p.identifier, p.title, p.ptext,
            ts_rank_cd(to_tsvector('english', p.ptext), q.tsq) AS rank
    FROM para p
    CROSS JOIN q
    WHERE to_tsvector('english', p.ptext) @@ q.tsq
    )
    SELECT regexp_replace(trim(ptext), E'[\\t\\n\\r]+', ' ', 'g') AS paragraph
    FROM hits
    ORDER BY rank DESC, length(ptext) DESC LIMIT 100;
"""

params = {"term": term}
with conn.cursor() as cur:
    cur.execute(sql, params)
    for (paragraph,) in cur:          # tuple-unpack the single column
        print(paragraph)

s_term = strip_diacritics(term)
if s_term != term:
    print("++++++++++++")
    params = {"term": s_term}
    with conn.cursor() as cur:
        cur.execute(sql, params)
        for (paragraph,) in cur:          # tuple-unpack the single column
            print(paragraph)

