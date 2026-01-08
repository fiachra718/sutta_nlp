from pathlib import Path
import spacy
from spacy.tokens import DocBin

paths = [
    Path("ne-data/work/gold.spacy"),
    Path("ne-data/work/gold_additions.spacy"),
    Path("ne-data/work/gold_additions_v2.spacy"),
    Path("ne-data/work/train_merged_v4.spacy"),
    Path("ne-data/work/latest_gold.spacy"),  # <-- replace with your new file
]

db_out = Path("ne-data/work/train_pool.spacy")
vocab = spacy.blank("en").vocab
out = DocBin()

for p in paths:
    if not p.exists(): 
        continue
    db = DocBin().from_disk(p)
    for doc in db.get_docs(vocab):
        out.add(doc)

out.to_disk(db_out)
print(f"✅ Wrote merged pool → {db_out}")