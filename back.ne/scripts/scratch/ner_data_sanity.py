#!/usr/bin/env python3
# ner_pretrain_sanity.py
import argparse, json, hashlib
from collections import Counter

import spacy
from spacy.training.iob_utils import offsets_to_biluo_tags
from json.decoder import JSONDecodeError

def has_overlap(ents):
    ents_sorted = sorted(ents, key=lambda x: (x[0], x[1]))
    for i in range(len(ents_sorted)-1):
        s1,e1,_ = ents_sorted[i]
        s2,e2,_ = ents_sorted[i+1]
        if max(s1,s2) < min(e1,e2):
            return True
    return False

def ensure_ruler(nlp, patterns_dir=None):
    """Ensure an entity_ruler exists, optionally load patterns, keep it first."""
    if "entity_ruler" in nlp.pipe_names:
        ruler = nlp.get_pipe("entity_ruler")
    else:
        ruler = nlp.add_pipe("entity_ruler", first=True, config={"overwrite_ents": True})
    if patterns_dir:
        ruler.from_disk(patterns_dir)
    # keep first
    if nlp.pipe_names[0] != "entity_ruler":
        name, comp = nlp.remove_pipe("entity_ruler")
        nlp.add_pipe(comp, name=name, first=True)
        ruler = nlp.get_pipe("entity_ruler")
    return ruler

def load_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            print("Reading line: {}".format(i))
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
            except JSONDecodeError as e:
                print("JSONDecoderError: {}".format(e))
                continue
            try:
                yield i, j["text"], [(int(s), int(e), str(lbl)) for s, e, lbl in j["entities"]], j.get("meta")
            except KeyError as e:
                print("KeyError: {}".format(e))
                continue

def ctx(text, s, e, pad=40):
    a = max(0, s - pad); b = min(len(text), e + pad)
    return f"{text[a:s]}⟦{text[s:e]}⟧{text[e:b]}"

def main():
    ap = argparse.ArgumentParser(description="Sanity-check NER JSONL & ruler before training.")
    ap.add_argument("--model", required=True, help="spaCy model to use for tokenizer (e.g. ne-data/models/archive/sutta_ner-v2).")
    ap.add_argument("--jsonl", required=True, help="JSONL with {'text','entities'}.")
    ap.add_argument("--ruler", help="(Optional) path to ruler_patterns to load.")
    ap.add_argument("--max-show", type=int, default=8, help="Max examples to print per issue.")
    args = ap.parse_args()

    # Load model for tokenizer + (optional) ruler check
    nlp = spacy.load(args.model)
    n_patterns = 0
    if args.ruler:
        ruler = ensure_ruler(nlp, args.ruler)
        try:
            n_patterns = len(ruler.patterns)
        except Exception:
            n_patterns = 0

    print("=== RULER CHECK ===")
    print(f"entity_ruler in pipeline: {'yes' if 'entity_ruler' in nlp.pipe_names else 'no'}")
    print(f"entity_ruler patterns   : {n_patterns}\n")

    # Use tokenizer-only for alignment checks (don’t run full pipeline)
    tok = spacy.blank(nlp.lang)

    # Scan data
    lines = list(load_jsonl(args.jsonl))
    n_examples = len(lines)
    label_counts = Counter()
    text_hashes = set(); dup_count = 0
    oob = []; empty = []; overlaps = []; misaligned = []

    def overlaps_span(a, b):
        (s1, e1, _), (s2, e2, _) = a, b
        return max(s1, s2) < min(e1, e2)

    for ln, text, ents, meta in lines:
        h = hashlib.md5(text.encode("utf-8")).hexdigest()
        if h in text_hashes: dup_count += 1
        else: text_hashes.add(h)

        doc = tok.make_doc(text)

        # if overlaps exist, record and skip BILUO to avoid exceptions
        if has_overlap(ents):
            overlaps.append((ln, ents, text))
            continue

        # basic span sanity
        for (s, e, lbl) in ents:
            if not (0 <= s < e <= len(text)):
                oob.append((ln, s, e, lbl, text))
            if s == e:
                empty.append((ln, s, e, lbl, text))
            label_counts[lbl] += 1

        # overlap check
        for i in range(len(ents)):
            for j in range(i+1, len(ents)):
                if overlaps_span(ents[i], ents[j]):
                    overlaps.append((ln, ents[i], ents[j], text))

        # strict alignment report (flags true misalignments as '-')
        biluo = offsets_to_biluo_tags(doc, [(s, e, lbl) for (s, e, lbl) in ents])
        for (s, e, lbl), tag in zip(ents, biluo):
            if tag == "-":
                misaligned.append((ln, s, e, lbl, text))

    print("=== DATA SUMMARY ===")
    print(f"Examples          : {n_examples}")
    print(f"Unique texts      : {len(text_hashes)}  (dups: {dup_count})")
    print(f"Label counts      : {dict(label_counts)}")
    print(f"OOB spans        : {len(oob)}")
    print(f"Empty spans      : {len(empty)}")
    print(f"Overlapping spans: {len(overlaps)}")
    print(f"Misaligned spans : {len(misaligned)}\n")

    def show(title, items, fmt):
        if not items: return
        print(f"--- {title} (showing up to {args.max_show}) ---")
        for it in items[:args.max_show]:
            print(fmt(it))
        print()

    show("Out-of-bounds spans", oob,
         lambda x: f"[line {x[0]}] OOB {x[3]} [{x[1]}:{x[2]}] (len={len(x[4])})")
    show("Empty spans", empty,
         lambda x: f"[line {x[0]}] EMPTY {x[3]} [{x[1]}:{x[2]}]  {ctx(x[4], x[1], x[2])}")
    show("Overlapping spans", overlaps,
         lambda x: f"[line {x[0]}] OVERLAP {x[1]} vs {x[2]}  {ctx(x[3], min(x[1][0],x[2][0]), max(x[1][1],x[2][1]))}")
    show("Misaligned spans", misaligned,
         lambda x: f"[line {x[0]}] MISALIGN {x[3]} [{x[1]}:{x[2]}]  {ctx(x[4], x[1], x[2])}")

    print("Tips:")
    print("- Fix OOB/empty spans first; they’re ignored during training.")
    print("- Misaligned spans mean offsets don’t match token boundaries; use doc.char_span(..., alignment_mode='expand') when fixing.")
    print("- Keep entity_ruler FIRST and populated (gazetteers, honorifics, openers) before training.")
    print("- Consider deduping near-identical texts to reduce leakage.")

if __name__ == "__main__":
    main()