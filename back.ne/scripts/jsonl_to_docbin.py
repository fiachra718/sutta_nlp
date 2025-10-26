#!/usr/bin/env python3
import sys, json, argparse
from pathlib import Path
import spacy
from spacy.tokens import DocBin

def load_items(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def extract_triples(rec):
    # Accept "spans" (list of dicts) OR "entities"/"ents" (list of [start,end,label])
    if "spans" in rec and isinstance(rec["spans"], list):
        out = []
        for s in rec["spans"]:
            if all(k in s for k in ("start","end","label")):
                out.append((int(s["start"]), int(s["end"]), str(s["label"])))
        return out
    for key in ("entities", "ents"):
        if key in rec and isinstance(rec[key], list):
            return [(int(s[0]), int(s[1]), str(s[2])) for s in rec[key]]
    return []

def main():
    ap = argparse.ArgumentParser(description="Convert JSONL (text + spans/entities) to .spacy DocBin")
    ap.add_argument("infile", help="Input JSONL with {'text':..., 'spans' or 'entities'/ 'ents': ...}")
    ap.add_argument("outfile", help="Output .spacy path")
    args = ap.parse_args()

    nlp = spacy.blank("en")
    db = DocBin(store_user_data=False)

    n_docs = 0
    for rec in load_items(Path(args.infile)):
        text = rec.get("text", "")
        if not text.strip():
            continue
        doc = nlp.make_doc(text)

        triples = extract_triples(rec)
        spans = []
        for start, end, label in triples:
            if 0 <= start < end <= len(doc.text):
                span = doc.char_span(start, end, label=label, alignment_mode="contract")
                if span is not None:
                    spans.append(span)
        doc.ents = spans
        db.add(doc)
        n_docs += 1

    out = Path(args.outfile)
    out.parent.mkdir(parents=True, exist_ok=True)
    db.to_disk(out)
    print(f"✅ Wrote {n_docs} docs → {out}")

if __name__ == "__main__":
    main()
    