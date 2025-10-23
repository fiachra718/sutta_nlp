from local_settings import load_model, WORK
import spacy
import json
from spacy.scorer import Scorer
from spacy.training import Example


nlp = load_model()
examples = []
with open(WORK / "boot_agree.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        if len(line.strip()) == 0:
            next
        row = json.loads(line)
        doc = nlp.make_doc(row["text"])
        ents = [(s["start"], s["end"], s["label"]) for s in row.get("spans", [])]
        examples.append(Example.from_dict(doc, {"entities": ents}))
    
pred_docs = list(nlp.pipe([ex.x.text for ex in examples]))
scorer = Scorer()
for ex, pred in zip(examples, pred_docs):
    ex.predicted = pred

scorer = Scorer()
scores = scorer.score(examples)
print("ents_p:", scores["ents_p"])
print("ents_r:", scores["ents_r"])
print("ents_f:", scores["ents_f"])
# Optional per-label:
print("per-type:", scores["ents_per_type"])
