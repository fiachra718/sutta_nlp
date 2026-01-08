#!/usr/bin/env python3
import spacy
from spacy.tokens import DocBin
from pathlib import Path
import json
from local_settings import WORK, MODELS_DIR, PATTERNS


PARAS_TXT = WORK / "paras.txt"
OUT_SPACY = WORK / "boot_agree.spacy"
OUT_JSONL = WORK / "boot_agree.jsonl"

LABELS = {"PERSON","GPE","LOC","NORP"}  # keep only these

def main():
    nlp = spacy.load(str(MODELS_DIR))
    if "entity_ruler" in nlp.pipe_names:
        nlp.remove_pipe("entity_ruler")
    ruler = nlp.add_pipe("entity_ruler", before="ner", config={"phrase_matcher_attr":"LOWER"})
    ruler.overwrite_ents = False
    ruler.from_disk(str(PATTERNS))

    # 2) rules-only pipeline (blank + ER) to get pure rule ents
    rule_nlp = spacy.blank("en")
    rule_ruler = rule_nlp.add_pipe("entity_ruler", config={"phrase_matcher_attr":"LOWER"})
    rule_ruler.from_disk(str(PATTERNS))
    
    lines = [ln.strip() for ln in PARAS_TXT.read_text(encoding="utf-8").splitlines() if ln.strip()]

    db = DocBin(store_user_data=False)
    n_added = 0

    with OUT_JSONL.open("w", encoding="utf-8") as jf:
        for text in lines:
            doc = nlp(text)
            rule_doc = rule_nlp(text)

            mset = {(e.start_char, e.end_char, e.label_) for e in doc.ents if e.label_ in LABELS}
            rset = {(e.start_char, e.end_char, e.label_) for e in rule_doc.ents if e.label_ in LABELS}

            keep = set()
            keep |= rset            # all rule ents (high precision)
            keep |= (mset & rset)   # model ents that agree with rules

            if not keep:
                continue

            # rebuild ents on the model doc
            spans = []
            for (a,b,label) in sorted(keep):
                span = doc.char_span(a, b, label=label, alignment_mode="contract")
                if span:
                    spans.append(span)

            # enforce non-overlapping for NER training
            spans = spacy.util.filter_spans(spans)
            doc.ents = spans

            if not doc.ents:
                continue

            # write JSONL entry (for visibility or downstream merge)
            jf.write(json.dumps({
                "text": doc.text,
                "spans": [{"start": s.start_char, "end": s.end_char, "label": s.label_} for s in doc.ents]
            }, ensure_ascii=False) + "\n")

            db.add(doc)
            n_added += 1

    OUT_SPACY.parent.mkdir(parents=True, exist_ok=True)
    db.to_disk(OUT_SPACY)
    print(f"Wrote {n_added} docs to {OUT_SPACY}")
    print(f"Also wrote JSONL preview to {OUT_JSONL}")

if __name__ == "__main__":
    main()