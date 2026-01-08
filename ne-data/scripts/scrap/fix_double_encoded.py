import spacy
from spacy.training import offsets_to_biluo_tags
from psycopg import connect
from psycopg.rows import dict_row

DSN = "dbname=tipitaka user=alee"
nlp = spacy.blank("en")

with connect(DSN, row_factory=dict_row) as cx, cx.cursor() as cur:
    cur.execute("SELECT text, spans FROM gold_training")
    for row in cur.fetchall():
        text = row["text"]
        raw_spans = row.get("spans",  [])
        span_offsets = [
            (span["start"], span["end"], span["label"])
                for span in raw_spans
        ]
        tags = offsets_to_biluo_tags(nlp.make_doc(text), span_offsets)
        print(tags)

