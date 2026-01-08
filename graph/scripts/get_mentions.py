import psycopg
import json

CONN = psycopg.connect("user=alee dbname=tipitaka")

sql = '''
    WITH hits AS (
        SELECT
            s.nikaya,
            NULLIF(regexp_replace(s.book_number::text, '[^0-9].*', ''), '')::int AS book_number,
            COALESCE(s.vagga, '') AS vagga,
            v.ord AS verse_num
        FROM ati_suttas s
        CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS v(vj, ord)
        WHERE v.vj->>'text' ILIKE %(pattern)s
    )
    SELECT
    CASE
        WHEN nikaya IN ('SN', 'AN') THEN nikaya || ' ' || vagga || '.' || book_number
        ELSE nikaya || ' ' || book_number
    END AS ref,
    COUNT(*) AS verse_mentions
    FROM hits
    GROUP BY ref
    ORDER BY ref;
'''

with open("graph/entities/people_aliases.csv", "r", encoding="utf-8") as f:
    people_list = []
    for line in f:
        (name, aliases_list) = line.strip().split(",", 1)
        with CONN.cursor() as cur:
            people_data = {}
            cur.execute(sql, {"pattern": f"%{name}%"})
            rows = cur.fetchall()
            if not rows:
                people_data[name] = []
            else:
                people_data[name] = [
                    {"ref": ref, "count": count}
                    for ref, count in rows
                ]
            people_list.append(people_data)

            print(people_list)
            people_list = []
# print(json.dumps(people_list))