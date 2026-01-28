import spacy
import psycopg

conn = psycopg.connect("dbname=tipitaka user=alee")

SQL = '''
    WITH q AS (
    SELECT websearch_to_tsquery('english', %(term)s) AS tsq
    ),
    para AS (
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
    SELECT regexp_replace(trim(paragraph), E'[\\t\\n\\r]+', ' ', 'g') AS paragraph
    FROM sutta_hits
    LIMIT 1
    '''


def search_db(term):
    params = {"term": term}
    with conn.cursor() as cur:
        cur.execute(SQL, params)
        for (paragraph,) in cur:
            print(f'{term} \t {paragraph}\n\n')
if __name__ == "__main__":
    with open("./graph/entities/not_norp.txt", encoding="utf-8") as f:
        for line in f:
            search_db(line.strip())


