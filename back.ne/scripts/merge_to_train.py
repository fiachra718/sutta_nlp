# merge_into_train.py
import spacy
from spacy.tokens import DocBin

nlp = spacy.blank("en")
train = DocBin().from_disk("./train.spacy")
boot  = DocBin().from_disk("/Users/alee/sutta_nlp/ne-data/scripts/loc_bootstrap.spacy")

merged = DocBin(store_user_data=False)
for d in list(train.get_docs(nlp.vocab)) + list(boot.get_docs(nlp.vocab)):
    merged.add(d)

merged.to_disk("./train_merged.spacy")
print("Merged -> ./train_merged.spacy")