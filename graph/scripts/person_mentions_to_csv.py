# count PERSON per verse 
import psycopg
import json
from psycopg.rows import dict_row
import csv


CONN = psycopg.connect("dbname=tipitaka user=alee")

#####
# for each term in an alias list
#  sum up the number of times that term is mentioned
#  in the verses->>'text' field of the verses
#  field in ati_suttas
#  return the results as nikaya book_number for MN and DN
#  or nikaya book_number,vagga for AN and SN
#  or vagga book_number for KN 
#####
sql = '''
WITH terms(term) AS (
  SELECT unnest(%(terms)s::text[])
),
verse_hits AS (
  SELECT DISTINCT
    s.identifier,
    CASE WHEN s.nikaya IN ('SN','AN') THEN
        s.nikaya || ' ' || s.book_number || '.' || s.vagga
    WHEN s.nikaya = 'KN' THEN
        s.vagga || ' ' || s.book_number 
    ELSE
        s.nikaya || ' ' || s.book_number
    END AS ref,
    (v.elem->>'num')  AS verse_num
  FROM ati_suttas s
    CROSS JOIN LATERAL jsonb_array_elements(s.verses) AS v(elem)
    JOIN terms t
        ON POSITION(t.term IN (v.elem->>'text')) > 0
)
SELECT
  ref,
  count(*) AS verse_count
FROM verse_hits
GROUP BY ref
ORDER BY ref;
'''

# with open("graph/entities/people_mentions.csv", "w", encoding="utf-8") as outfille:
with CONN.cursor() as cur:
    with open("graph/entities/gold_people.jsonl", "r", encoding="utf-8") as f, \
        open("graph/entities/people_mentions.csv", "w", encoding="utf-8") as outfille:
        csv_writer = csv.writer(outfille)
        csv_writer.writerow(["person_name", "aliases", "ref", "verse_count"])

        for line in f:
            record = json.loads(line.strip())
            name = record["name"]
            aliases = record["aliases"]
            # roll up name and aliases into one list
            terms = [name] + aliases
            params = {"terms": terms}
            cur.execute(sql, params)
            rows = cur.fetchall()
            for ref, verse_count in rows:
                csv_writer.writerow([
                    name,
                    "|".join(aliases),   # or json.dumps(aliases) if you prefer
                    ref,
                    verse_count,
                ])