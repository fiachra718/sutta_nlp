import spacy, json, random
from spacy.training import Example

# 0) Start from your rules model and ENSURE patterns are present
nlp = spacy.load("ne-data/models/archive/sutta_ner-v2")
ruler = nlp.get_pipe("entity_ruler")
if not getattr(ruler, "patterns", None):
    ruler.from_disk("ne-data/models/archive/sutta_ner-v2/ruler_patterns")

# 1) Prepare training/dev with alignment to token boundaries
def load_and_fix_jsonl(path, tok_nlp):
    fixed = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            j = json.loads(line)
            text = j["text"]
            doc = tok_nlp.make_doc(text)
            ents = []
            for (s, e, label) in j["entities"]:
                span = doc.char_span(s, e, label=label, alignment_mode="expand")
                if span:
                    ents.append((span.start_char, span.end_char, label))
            if ents:
                fixed.append((text, {"entities": ents}))
    return fixed

tok = spacy.blank("en")  # tokenizer-only pipeline for alignment
train = load_and_fix_jsonl("train.jsonl", tok)
dev   = load_and_fix_jsonl("dev.jsonl", tok)

# 2) Ensure an NER pipe and register labels
if "ner" not in nlp.pipe_names:
    ner = nlp.add_pipe("ner", last=True)
else:
    ner = nlp.get_pipe("ner")

labels = set(lbl for _, ann in train for _, _, lbl in ann["entities"])
for lbl in labels:
    ner.add_label(lbl)

# 3) Keep ruler FIRST (high precision)
first = nlp.pipe_names[0]
if first != "entity_ruler":
    # portable move
    name, comp = nlp.remove_pipe("entity_ruler")
    nlp.add_pipe(comp, name=name, first=True)

# 4) Train
examples = [Example.from_dict(nlp.make_doc(t), ann) for t, ann in train]
random.seed(0)
nlp.initialize(lambda: examples)
for epoch in range(12):
    random.shuffle(examples)
    losses = {}
    nlp.update(examples, losses=losses, drop=0.2)
    print(f"epoch {epoch+1:02d} losses={losses}")

# 5) Quick eval
def f1_like(nlp, dev):
    gold_total = got_total = hit = 0
    for t, ann in dev:
        gold = {(s, e, l) for (s, e, l) in ann["entities"]}
        pred = nlp(t)
        got  = {(e.start_char, e.end_char, e.label_) for e in pred.ents}
        hit += len(gold & got); gold_total += len(gold); got_total += len(got)
    prec = hit / max(1, got_total); rec = hit / max(1, gold_total)
    print(f"dev precision={prec:.3f} recall={rec:.3f} f1={2*prec*rec/max(1e-9,prec+rec):.3f}")

dev_fixed = [(t, {"entities": [(s,e,l) for (s,e,l) in ann["entities"]]}) for t, ann in dev]
f1_like(nlp, dev_fixed)

# 6) Save
nlp.to_disk("ne-data/models/archive/sutta_ner-v3")
print("saved ne-data/models/archive/sutta_ner-v3")
