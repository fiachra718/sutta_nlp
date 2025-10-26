import spacy
from spacy.tokens import DocBin
from pathlib import Path

paths = [
    Path("ne-data/work/train_merged_v4_no_doctr.spacy"),
    Path("ne-data/work/train_pool_nodoctr.spacy"),
]
seen = set()
out = DocBin(store_user_data=True)

for p in paths:
    db = DocBin().from_disk(p)
    for doc in db.get_docs(spacy.blank("en").vocab):
        key = (doc.text, tuple((ent.start_char, ent.end_char, ent.label_) for ent in doc.ents))
        if key not in seen:
            seen.add(key)
            out.add(doc)

out.to_disk("ne-data/work/train_merged_v5_no_doctr.spacy")
print("Wrote ne-data/work/train_merged_v5_no_doctr.spacy")
