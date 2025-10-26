# merge_many.py
import spacy
from spacy.tokens import DocBin

nlp = spacy.blank("en")
paths = [
    "./train.spacy",
    "/Users/alee/sutta_nlp/ne-data/scripts/loc_bootstrap.spacy",
    "/Users/alee/sutta_nlp/ne-data/scripts/loc_bootstrap_synth.spacy"  # if you created it
]
docs = []
for p in paths:
    db = DocBin().from_disk(p)
    docs += list(db.get_docs(nlp.vocab))

out = DocBin(store_user_data=False)
seen_texts = set()
for d in docs:
    # lightweight de-dupe on raw text
    raw = d.text.strip()
    if raw in seen_texts: 
        continue
    seen_texts.add(raw)
    out.add(d)
out.to_disk("./train_merged.spacy")
print("Merged:", len(seen_texts))
