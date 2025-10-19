import spacy
from spacy.tokens import DocBin
from pathlib import Path

WORK = Path("ne-data/work")
nlp = spacy.blank("en")
bins = []
for name in ["train.spacy",
             "loc_bootstrap.spacy",
             "boot_agree.spacy",
             "gold_additions.spacy",
             "neg_suttas.spacy",
             "contrast_places_people.spacy"]:
    p = WORK/name
    if p.exists():
        bins.append(p)

docs=[]
for p in bins:
    db=DocBin().from_disk(p)
    docs += list(db.get_docs(nlp.vocab))
    print("loaded", len(db), "from", p.name)

out = DocBin(store_user_data=False)
seen=set()
for d in docs:
    t=d.text.strip()
    if t in seen: continue
    seen.add(t); out.add(d)
out.to_disk(WORK/"train_merged.spacy")
print("merged ->", WORK/"train_merged.spacy", "docs:", len(seen))
