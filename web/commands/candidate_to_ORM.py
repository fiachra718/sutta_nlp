# Importing docs”
# 	•	Write a tiny importer:
# 	•	reads NDJSON
# 	•	normalizes → hashes
# 	•	converts entities → spans (for the candidate format)
# 	•	validates
# 	•	INSERT ... ON CONFLICT DO NOTHING/UPDATE
# 	•	Keep a manifest (timestamp, counts, label set) per import batch.

import json
from pathlib import Path
from app.models.models import CandidateDoc, TrainingDoc
import psycopg
from psycopg.types.json import Json
from psycopg.rows import dict_row

DEBUG = True

conn = psycopg.connect("dbname=tipitaka user=alee")
conn.autocommit = True

WORK_DIR = Path("/Users/alee/sutta_nlp/ne-data/work")
CANDIDATE_DIR = WORK_DIR / "candidates"
candidate_files = [ CANDIDATE_DIR / "corrected_verses.jsonl", CANDIDATE_DIR / "cleaned_candidates.jsonl" ]


def load_candidate(record: CandidateDoc, conn):
    sql = """
    INSERT INTO candidates (text, text_hash, entities)
    VALUES (%(text)s, %(text_hash)s, %(entities)s)
    ON CONFLICT (text_hash) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "text": doc.text,
            "text_hash": doc.text_hash,
            "entities": Json(doc.entities),            # becomes JSONB
        })
            

for candidate_file in candidate_files:
    with open(candidate_file, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            l = line.rstrip("\n").strip()
            if DEBUG:
                print(idx+1, line)
            
            if l:
                record = json.loads(l)
                doc = CandidateDoc.model_validate(record)
                load_candidate(doc, conn)
