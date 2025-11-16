import psycopg
from psycopg.rows import dict_row
import spacy
from spacy.training import Example
import random
import numpy as np
from local_settings import MODELS_DIR, WORK, load_model
from spacy.util import minibatch

CONN = psycopg.connect("dbname=tipitaka user=alee")
OUTPUT_DIR = WORK / "models" / "1107"

def db_to_examples(conn, nlp):
    sql = """
        select text, spans from gold_training where id like 'manual%' 
    """
    examples = []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            text = row["text"]
            pred_doc = nlp.make_doc(text)
            gold_doc = nlp.make_doc(text)     # reference Doc
            spans = []
            for s in row["spans"]:
                span = gold_doc.char_span(s["start"], s["end"], label=s["label"], alignment_mode="contract")
                if span: 
                    spans.append(span)
            gold_doc.ents = spans
            example = Example(pred_doc, gold_doc)
            examples.append(example)
    if not examples:
        raise ValueError("Candidate records did not yield any training examples.")
    return examples

nlp = load_model()
examples = db_to_examples(CONN, nlp)

test_examples = db_to_examples(CONN, nlp)

scores = nlp.evaluate(test_examples)
print("ents_p:", scores.get("ents_p"))
print("ents_r:", scores.get("ents_r"))
print("ents_f:", scores.get("ents_f"))
print("per-type:", scores.get("ents_per_type"))

