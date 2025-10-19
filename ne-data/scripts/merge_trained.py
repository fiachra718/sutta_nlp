import spacy
from spacy.tokens import DocBin
from pathlib import Path

work = Path("ne-data/work")
nlp = spacy.blank("en")

bins = [p for p in [
    work/"train.spacy",            # original (if you still keep it)
    work/"train_merged.spacy",     # prior merged set (if exists)
    work/"gold_additions.spacy"    # new stuff from step 1
] if p.exists()]

docs=[]
for p in bins:
    db = DocBin().from_disk(p)
    docs += list(db.get_docs(nlp.vocab))
    print(f"loaded {len(db)} from {p.name}")

seen=set(); out=DocBin(store_user_data=False)
for d in docs:
    t = d.text.strip()
    if t in seen: 
        continue
    seen.add(t)
    out.add(d)

out.to_disk(work/"train_merged.spacy")
print("merged ->", work/"train_merged.spacy", "docs:", len(seen))
