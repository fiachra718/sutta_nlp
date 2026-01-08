import csv
import spacy
import unicodedata
import psycopg
from psycopg.rows import dict_row
from collections import defaultdict
import json

nlp = spacy.load("en_sutta_ner")
CONN = psycopg.connect("dbname=tipitaka user=alee")

def read_people_csv(filename):
    with open("some_characters.csv", encoding="utf-8") as f:
        doc_reader = csv.reader(f, delimiter="\t")
        people = set()
        for row in doc_reader:
            doc = nlp(row[2])
            for ent in doc.ents:
                if ent.label_ == "PERSON":
                    people.add(ent.text)
            print(f"{row[0]}, {row[1]}, {row[2]}, {[p for p in people]}")
            people = set()

        # for person in people:
        #     print (person.strip()) 


def get_gold_persons():
    people = defaultdict(list)

    sql = '''
        WITH spans AS (
        SELECT
            t.text,
            (s->>'start')::int AS s,
            (s->>'end')::int   AS e,
            s->>'label'        AS label
        FROM gold_training t,
            jsonb_array_elements(t.spans) s
        )
        SELECT DISTINCT trim(both ' "'
            FROM substring(text from s+1 for e-s)) AS person
        FROM spans
        WHERE label='PERSON'
        AND substring(text from s+1 for e-s) ~ '[A-Za-z]'
        ORDER BY 1;
    '''
    with CONN.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        for (row,) in rows:
            print(row)
            name = row
            params = {"term": name}
            inner_sql = ''' 
                WITH q AS (
                    SELECT websearch_to_tsquery('english', %(term)s) AS tsq
                    )
                    SELECT s.identifier,
                        t.ord AS verse_num
                    FROM ati_suttas s
                    CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS t(v, ord)
                    CROSS JOIN q
                    WHERE to_tsvector('english', t.v->>'text') @@ q.tsq
                    ORDER BY s.identifier, verse_num limit 16;
                '''
            cur.execute(inner_sql, params)
            for csv_row in cur.fetchall():
                people[name].append(csv_row)
                # print(people)
    return people

if __name__ == "__main__":
    people = get_gold_persons()
    with open("people.jsonl", "w", encoding="utf-8") as f:
        
        for name, matches in people.items():
            record = { "name" : name, "verses" : matches }
            f.write(f"{json.dumps(record, ensure_ascii=False)}\n")
            # line = f"{json.dumps(name, ensure_ascii=False)}, {json.dumps(matches, ensure_ascii=False)}\n"
            # line
