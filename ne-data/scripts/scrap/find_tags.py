import spacy
from spacy.tokens import DocBin

# Candidate DocBin files to inspect for entity labels.
PATHS = [
    "ne-data/work/train_merged_v3.spacy",
    "ne-data/work/dev.spacy",
    "ne-data/work/train_pool.spacy",
    "ne-data/work/train_merged_v3.spacy",
]

for path in PATHS:
    labels = set()
    db = DocBin().from_disk(path)
    for doc in db.get_docs(spacy.blank("en").vocab):
        for ent in doc.ents:
            labels.add(ent.label_)
    print(path, sorted(labels))
