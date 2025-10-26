import random, spacy
from spacy.tokens import DocBin
from pathlib import Path

random.seed(42)
vocab = spacy.blank("en").vocab
all_db = DocBin().from_disk("ne-data/work/train_pool.spacy")
docs = list(all_db.get_docs(vocab))
random.shuffle(docs)

n_train = max(1, int(0.9 * len(docs)))
train_db = DocBin(docs=docs[:n_train])
dev_db   = DocBin(docs=docs[n_train:])

train_db.to_disk("ne-data/work/train_merged_v4.spacy")
dev_db.to_disk("ne-data/work/dev_v4.spacy")
print(f"✅ Split {len(docs)} docs → train:{n_train} dev:{len(docs)-n_train}")
