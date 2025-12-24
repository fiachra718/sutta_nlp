from pathlib import Path

import spacy
from psycopg import connect
from psycopg.rows import dict_row
from spacy.tokens import DocBin

DSN = "dbname=tipitaka user=alee"
OUTFILE = Path("ne-data/work/gold_training.spacy")


def span_dicts_to_spacy_spans(doc, span_dicts):
    spans = []
    for spec in span_dicts or []:
        start = spec.get("start")
        end = spec.get("end")
        label = spec.get("label")
        if start is None or end is None or label is None:
            continue
        span = doc.char_span(int(start), int(end), label=str(label), alignment_mode="contract")
        if span:
            spans.append(span)
    return spans



tokenizer = spacy.blank("en")
docbin = DocBin(store_user_data=True)

with connect(DSN, row_factory=dict_row) as cx, cx.cursor() as cur:
    cur.execute("SELECT text, spans FROM gold_training WHERE created_at::date > '2025-11-05'")
    rows = cur.fetchall()

for row in rows:
    text = row.get("text") or ""
    doc = tokenizer.make_doc(text)
    doc.ents = span_dicts_to_spacy_spans(doc, row.get("spans") or [])
    docbin.add(doc)

OUTFILE.parent.mkdir(parents=True, exist_ok=True)
docbin.to_disk(OUTFILE)
print(f"Wrote {len(rows)} docs to {OUTFILE}")

