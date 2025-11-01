from app.models.models import CandidateDoc, TrainingDoc
import psycopg
from psycopg.rows import dict_row
from hashlib import md5

conn = psycopg.connect("dbname=tipitaka user=alee", row_factory=dict_row)

sql = ''' SELECT id, text, spans FROM gold_training ORDER BY gen_random_uuid() 
        LIMIT 5
    '''

cur = conn.execute(sql)
docs = []

for row in cur.fetchall():
    try:
        doc = TrainingDoc.model_validate({
            "id": row["id"],
            "text": row["text"],
            "spans": row["spans"],   # jsonb → dict/list already via row_factory
            "spans_hash": md5(row["spans"]).hexdigest()
        })
        docs.append(doc)
        print("Loaded training doc: {}{}".format(doc.id, doc.spans))
    except Exception as e:
        print("VALIDATION FAIL id=", row["id"], "->", e)
    print(docs)

sql = ''' SELECT 
            id, 
            source_identifier, 
            text, 
            text_hash, 
            entities, 
            created_at 
        FROM candidates ORDER BY gen_random_uuid() 
        LIMIT 5
    '''
cur = conn.execute(sql)
docs = []

for row in cur.fetchall():
    try:
        doc = CandidateDoc.model_validate({
            "id": row.get("id"),
            "source_identifier": row.get("source_identier"),
            "text": row.get("text"),
            "text_hash": row.get("text_hash"),
            "entities": row.get("entities"),
            "created_at": row.get("created_at")
        })
        docs.append(doc)
    except Exception as e:
        print("VALIDATION FAIL id=", row["id"], "->", e)
    print("Loaded candidate doc: {}{}".format(doc.id, doc.entities))

######  That was all hideous, is there are prettier way to get Candidate Docs?? 
###### YES!!!
docs = CandidateDoc.objects.sample(5)     # ← descriptor triggers DB
print(docs)
one  = CandidateDoc.objects.get(279)
print(one)
td = TrainingDoc.objects.get("sorted_combined.entities:256")
print(td)
