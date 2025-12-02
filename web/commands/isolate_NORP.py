from app.models.models import TrainingDoc
from app.api.ner import run_ner   # your wrapper that calls nlp
from app.db import db             # however you normally talk to Postgres

TARGET_LABEL = "NORP"
MAX_EXAMPLES = 30

# 1. Fetch some gold docs from DB; adjust SQL / Manager as you like.
rows = db.fetch_all("SELECT id, text, spans FROM gold_training LIMIT 500")

count = 0
for row in rows:
    gold_doc = TrainingDoc.model_validate({
        "id": row["id"],
        "text": row["text"],
        "spans": row["spans"],   # list of {start,end,label,text}
    })

    # gold entities as (start,end,label)
    gold_ents = {
        (s["start"], s["end"], s["label"]) for s in row["spans"]
    }

    # 2. Run current NER
    pred = run_ner(gold_doc.text)
    pred_ents = {
        (s["start"], s["end"], s["label"]) for s in pred["spans"]
    }

    fp_ents = pred_ents - gold_ents

    for start, end, label in fp_ents:
        if label != TARGET_LABEL:
            continue
        span_text = gold_doc.text[start:end]
        print("==== NORP FALSE POSITIVE ====")
        print(f"ID:   {gold_doc.id}")
        print(f"TEXT: {gold_doc.text}")
        print(f"PRED: [{span_text!r}] -> {label}")
        print("GOLD:", [(s["text"], s["label"]) for s in row["spans"]])
        print()

        count += 1
        if count >= MAX_EXAMPLES:
            break
    if count >= MAX_EXAMPLES:
        break

print(f"Collected {count} NORP false positives from gold_training.")
