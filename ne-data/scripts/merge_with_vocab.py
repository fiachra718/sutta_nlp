from pathlib import Path
import spacy
from spacy.tokens import DocBin

parts = [
    Path("ne-data/work/train_merged_v2.spacy"),
    Path("ne-data/work/gold_additions.spacy"),
    # add more .spacy files here if needed
]
out_path = Path("ne-data/work/train_merged_v3.spacy")

nlp = spacy.blank("en")             # <-- provide a real vocab
docs = []

for p in parts:
    db = DocBin().from_disk(p)
    docs.extend(list(db.get_docs(nlp.vocab)))  # <-- use nlp.vocab, not {}

DocBin(docs=docs).to_disk(out_path)
print(f"✅ wrote {len(docs)} docs → {out_path}")
