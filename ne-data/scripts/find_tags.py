from pathlib import Path             
import spacy, srsly
from spacy.tokens import DocBin

'''
ne-data/work/boot_agree.spacy			ne-data/work/gold_additions.spacy		ne-data/work/train_merged_v3_rules.spacy
ne-data/work/candidates.spacy			ne-data/work/gold.spacy				ne-data/work/train_merged_v3.spacy
ne-data/work/dev_nodoctr.spacy			ne-data/work/latest_gold.spacy			ne-data/work/train_merged_v4.spacy
ne-data/work/dev_v4.spacy			ne-data/work/train_clean.spacy			ne-data/work/train_merged.spacy
ne-data/work/dev.spacy				ne-data/work/train_merged_v2.spacy		ne-data/work/train_pool.spacy
ne-data/work/gold_additions_v2.spacy		ne-data/work/train_merged_v3_nodoctr.spacy	ne-data/work/train.spacy
'''

paths = [                  
    "ne-data/work/train_merged_v3.spacy",
    "ne-data/work/dev.spacy",
    "ne-data/work/train_pool.spacy",
    "ne-data/work/train_merged_v3.spacy"

]                                     
for p in paths:                                             
    labels=set()       
    db = DocBin().from_disk(p)
    for doc in db.get_docs(spacy.blank("en").vocab):
        for ent in doc.ents:  
            labels.add(ent.label_)
    print(p, sorted(labels))   