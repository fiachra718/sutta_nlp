import spacy
from spacy.tokens import DocBin
from spacy.util import minibatch
from spacy.training import Example
from pathlib import Path
import json
import random, numpy as np


spacy.util.fix_random_seed(42)
random.seed(42); np.random.seed(42)

# ---- paths ----
# --  MUST rerun this code first --
REPO_ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "ne-data")
# then *all* things are good in the world
TRAIN_JSONL = REPO_ROOT / "work/train.jsonl"
DEV_JSONL   = REPO_ROOT / "work/dev.jsonl"
PATTERNS    = REPO_ROOT / "patterns/entity_ruler/patterns.jsonl"
OUTDIR      = REPO_ROOT / "work/model_minimal"

# ---- helper: load JSONL into Examples ----
def jsonl_to_examples(jsonl_path, nlp):
    examples = []
    with open(jsonl_path, "r") as f:
        for line in f:
            item = json.loads(line)
            text = item["text"]
            ents = item.get("entities", [])
            doc = nlp.make_doc(text)
            spans = []
            for start, end, label in ents:
                span = doc.char_span(start, end, label=label, alignment_mode="contract")
                if span is not None:
                    spans.append(span)
            doc.ents = spans
            examples.append(Example.from_dict(doc, {"entities": [(e.start_char, e.end_char, e.label_) for e in spans]}))
    return examples

# ---- blank English pipeline ----
nlp = spacy.blank("en")

# ---- add and load entity_ruler ----
ruler = nlp.add_pipe("entity_ruler", first=True)
ruler.from_disk(PATTERNS)
print(f"Loaded {len(ruler.patterns)} patterns")

# ---- add NER ----
ner = nlp.add_pipe("ner")
examples = jsonl_to_examples(TRAIN_JSONL, nlp)
for eg in examples:
    for span in eg.get_aligned_ner():
        ner.add_label(span)
print("NER labels:", list(ner.labels))

# ---- training loop ----
optimizer = nlp.initialize(lambda: examples)
ruler.from_disk(PATTERNS)
for epoch in range(10):
    random.shuffle(examples)
    losses = {}
    for batch in minibatch(examples, size=32):
        nlp.update(batch, sgd=optimizer, losses=losses, drop=0.1)
    print(f"Epoch {epoch} Losses: {losses}")

# for epoch in range(10):
#     losses = {}
#     for example in examples:
#         nlp.update([example], sgd=optimizer, losses=losses)
#     print(f"Epoch {epoch} Losses: {losses}")

# ---- evaluate ----
dev_examples = jsonl_to_examples(DEV_JSONL, nlp)
scorer = nlp.evaluate(dev_examples)
print("Dev F-score:", scorer["ents_f"])

# ---- save ----
OUTDIR.mkdir(parents=True, exist_ok=True)
nlp.to_disk(OUTDIR)
print("Saved model to", OUTDIR)

# --- test ---
test_examples = jsonl_to_examples(REPO_ROOT/"work/test.jsonl", nlp)
print("Test F1:", nlp.evaluate(test_examples)["ents_f"])
