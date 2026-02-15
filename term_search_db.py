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
    return "".join(
        c
        for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

sql = '''
    WITH q AS (
    SELECT websearch_to_tsquery('english', %(term)s) AS tsq
    ),
    para AS (  -- flatten with ordinality
    SELECT s.nikaya, s.identifier, s.title,
            t.ord AS verse_num, (t.v->>'text') AS ptext
    FROM ati_suttas s
    CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS t(v, ord)
    ),
    sutta_hits AS (
    SELECT p.nikaya, p.identifier, p.title,
            SUM(ts_rank_cd(to_tsvector('english', p.ptext), q.tsq)) AS rank,
            STRING_AGG(p.ptext, E'\n' ORDER BY p.verse_num)        AS paragraph
    FROM para p
    CROSS JOIN q
    WHERE to_tsvector('english', p.ptext) @@ q.tsq
    GROUP BY p.nikaya, p.identifier, p.title
    )
    SELECT sh.identifier, regexp_replace(trim(paragraph), E'[\\t\\n\\r]+', ' ', 'g') AS paragraph
    FROM sutta_hits as sh
    ORDER BY rank DESC, length(paragraph) DESC, gen_random_uuid()
    LIMIT 5;
'''

params = {"term": term}
with conn.cursor() as cur:
    cur.execute(sql, params)
    for (paragraph, identifier, ) in cur:
        print(paragraph, identifier)

s_term = strip_diacritics(term)
if s_term != term:
    params = {"term": s_term}
    with conn.cursor() as cur:
        cur.execute(sql, params)
        for (paragraph, identifier, ) in cur:  # tuple-unpack the columns
            print(f"Stripped diacritics: {paragraph}, {identifier}", flush=True)
