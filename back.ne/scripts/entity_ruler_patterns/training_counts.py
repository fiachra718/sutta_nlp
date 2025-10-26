# count_labels.py
import spacy
from spacy.tokens import DocBin
from collections import Counter

nlp = spacy.blank("en")
db = DocBin().from_disk("./train.spacy")
counts = Counter()
for doc in db.get_docs(nlp.vocab):
    counts.update(ent.label_ for ent in doc.ents)
print(counts)
