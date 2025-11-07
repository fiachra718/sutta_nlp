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
TRAIN_JSONL = REPO_ROOT / "work" / "checked" / "corrected_verses.jsonl"
DEV_JSONL   = REPO_ROOT / "work/dev.jsonl"
TRAIN = REPO_ROOT / "work/train.spacy"
DEV   = REPO_ROOT / "work/dev.spacy"
TEST   = REPO_ROOT / "work/test.spacy"
PATTERNS    = REPO_ROOT / "patterns/entity_ruler/patterns.jsonl"
OUTDIR      = REPO_ROOT / "work" / "models" / "1030"

MODELS_DIR  = REPO_ROOT / "work" / "models" / "1030"

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

# --- helper: load DocBin into examples
def docbin_to_examples(docbin_path, nlp):
    docbin = DocBin().from_disk(docbin_path)
    examples = []
    for reference in docbin.get_docs(nlp.vocab):
        predicted = nlp.make_doc(reference.text)
        examples.append(Example(predicted, reference))
    return examples

nlp = spacy.load(MODELS_DIR)

ruler = nlp.add_pipe("entity_ruler", first=True)
ruler.from_disk(PATTERNS)
print(f"Loaded {len(ruler.patterns)} patterns")

# ner = nlp.add_pipe("ner")
ner = nlp.get_pipe("ner")
# examples = docbin_to_examples(TRAIN, nlp)
examples = jsonl_to_examples(TRAIN_JSONL, nlp)
for eg in examples:
    for ent in eg.reference.ents:
    # for span in eg.get_aligned_ner():
        ner.add_label(ent.label_)
print("NER labels:", list(ner.labels))

# ---- training loop ----
optimizer = nlp.resume_training()
ruler.from_disk(PATTERNS)

for epoch in range(15):
    random.shuffle(examples)
    losses = {}
    for batch in minibatch(examples, size=32):
        nlp.update(batch, sgd=optimizer, losses=losses, drop=0.1)
    print(f"Epoch {epoch} Losses: {losses}")

for epoch in range(15):
    losses = {}
    for example in examples:
        nlp.update([example], sgd=optimizer, losses=losses)
    print(f"Epoch {epoch} Losses: {losses}")

# ---- evaluate ----
dev_examples = docbin_to_examples(DEV, nlp)
scorer = nlp.evaluate(dev_examples)
print("Dev F-score:", scorer["ents_f"])

# ---- save ----
OUTDIR.mkdir(parents=True, exist_ok=True)
nlp.to_disk(OUTDIR)
print("Saved model to", OUTDIR)

# --- test ---
test_examples = docbin_to_examples(TEST, nlp)
print("Test F1:", nlp.evaluate(test_examples)["ents_f"])
