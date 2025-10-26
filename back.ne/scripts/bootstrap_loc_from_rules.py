import spacy, srsly
from spacy.tokens import DocBin
from pathlib import Path

# INPUTS
PARAS_TXT = "/Users/alee/sutta_nlp/ne-data/work/paras.txt"  # one paragraph per line
PATTERNS  = "/Users/alee/sutta_nlp/ne-data/patterns/entity_ruler/patterns.jsonl"
OUT_SPACY = "/Users/alee/sutta_nlp/ne-data/scripts/loc_bootstrap.spacy"

nlp = spacy.blank("en")
ruler = nlp.add_pipe("entity_ruler")
ruler.from_disk(PATTERNS)

db = DocBin(store_user_data=False)
for line in Path(PARAS_TXT).read_text(encoding="utf-8").splitlines():
    if not line.strip():
        continue
    doc = nlp.make_doc(line)
    # apply only the ruler to set ents
    doc = nlp.get_pipe("entity_ruler")(doc)
    # keep only LOC spans (so we don't leak noisy labels)
    locs = [spacy.tokens.Span(doc, s.start, s.end, label="LOC") for s in doc.ents if s.label_=="LOC"]
    doc.ents = locs
    if locs:
        db.add(doc)

db.to_disk(OUT_SPACY)
print(f"Wrote {OUT_SPACY} with {len(list(DocBin().from_disk(OUT_SPACY).get_docs(nlp.vocab)))} docs")
