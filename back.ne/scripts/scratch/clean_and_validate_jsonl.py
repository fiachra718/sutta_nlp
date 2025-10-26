#!/usr/bin/env python3
import json, argparse, unicodedata, re, sys
from pathlib import Path
from typing import List, Tuple
import spacy
from spacy.tokens import Doc

PREP = {"at","in","of","near","to","from","on","by","into","onto","upon"}
WS_BAD = re.compile(r"[\u00A0\u2000-\u200B\u202F\u205F\u3000]")  # NBSP & friends

def nfc(s: str) -> str:
    if not isinstance(s, str): return s
    s = unicodedata.normalize("NFC", s)
    s = WS_BAD.sub(" ", s)
    return s

def trim_leading_prep(doc: Doc, start: int, end: int) -> Tuple[int,int]:
    """If span starts with a preposition/token like 'At ', shift to the next token that looks like a name.
       Heuristic: skip PREP and punctuation until we hit a Title-case or non-stopword token."""
    span = doc.char_span(start, end, alignment_mode="expand")
    if not span: return start, end
    i, j = span.start, span.end
    # Skip leading preps/quotes
    while i < j and (doc[i].lower_ in PREP or doc[i].is_quote or doc[i].is_punct):
        i += 1
    # Prefer first Title-case token if present
    k = i
    while k < j and not (doc[k].is_title or doc[k].shape_.startswith("X")):
        k += 1
    if k < j: i = k
    if i >= j:  # nothing left
        return start, end
    return doc[i].idx, doc[j-1].idx + len(doc[j-1])

def fix_spans(nlp, text: str, ents: List[Tuple[int,int,str]]):
    """Return repaired, non-overlapping spans aligned to tokens."""
    text = nfc(text)
    doc = nlp.make_doc(text)
    fixed = []
    for (s,e,label) in ents:
        # basic sanity
        if not isinstance(s,int) or not isinstance(e,int) or s < 0 or e <= s or e > len(text): 
            continue
        # align & trim leading preps for LOC/GPE
        if label in {"LOC","GPE"}:
            s,e = trim_leading_prep(doc, s, e)
        # final alignment to token boundaries
        span = doc.char_span(s, e, alignment_mode="expand", label=label)
        if not span: 
            continue
        fixed.append((span.start_char, span.end_char, label))
    # drop overlaps (keep longest first)
    fixed.sort(key=lambda x: (x[0], -(x[1]-x[0])))
    cleaned = []
    last_end = -1
    for s,e,lbl in fixed:
        if s < last_end: 
            continue
        cleaned.append((s,e,lbl))
        last_end = e
    return text, cleaned

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", required=True, help="raw JSONL with {text, entities}")
    ap.add_argument("--out", required=True, help="clean JSONL for training")
    ap.add_argument("--model", default="en_core_web_sm", help="just for tokenization")
    args = ap.parse_args()

    try:
        nlp = spacy.load(args.model)
    except Exception:
        nlp = spacy.blank("en"); nlp.add_pipe("sentencizer")

    src = Path(getattr(args, "in")).expanduser()
    dst = Path(getattr(args, "out")).expanduser()
    total = kept = bad_json = no_text = 0
    misaligned = repaired = 0

    with src.open(encoding="utf-8") as fin, dst.open("w", encoding="utf-8") as fout:
        for ln, line in enumerate(fin, start=1):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                j = json.loads(line)
            except json.JSONDecodeError:
                print(f"Reading line: {ln}\nJSONDecoderError", file=sys.stderr)
                bad_json += 1
                continue

            text = j.get("text") or j.get("paragraph")
            if not text:
                print(f"Reading line: {ln}\nKeyError: 'text'", file=sys.stderr)
                no_text += 1
                continue

            text = nfc(text)
            raw_ents = j.get("entities") or j.get("spans") or []
            ents = []
            for ent in raw_ents:
                # accept [(s,e,lbl)] or [{"start":..,"end":..,"label":..}]
                if isinstance(ent, (list,tuple)) and len(ent)==3:
                    s,e,lbl = ent
                elif isinstance(ent, dict):
                    s,e,lbl = ent.get("start"), ent.get("end"), ent.get("label") or ent.get("ner") or ent.get("type")
                else:
                    continue
                ents.append((s,e,str(lbl).upper()))

            # try to see how many won’t align as-is
            doc = nlp.make_doc(text)
            prelim = 0
            for s,e,lbl in ents:
                span = doc.char_span(s,e)
                if not span: prelim += 1
            if prelim: misaligned += prelim

            text_fixed, ents_fixed = fix_spans(nlp, text, ents)
            if not ents_fixed:
                # keep examples that at least have text? For NER, skip
                continue

            out = {
                "text": text_fixed,
                "entities": ents_fixed,
                "meta": j.get("meta", None) or {k:v for k,v in j.items() if k not in {"text","entities","spans"}}
            }
            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            kept += 1
            repaired += prelim

    print(f"=== SUMMARY ===")
    print(f"read lines         : {total}")
    print(f"kept               : {kept}")
    print(f"bad json           : {bad_json}")
    print(f"missing text       : {no_text}")
    print(f"spans misaligned   : {misaligned} (raw)")
    print(f"spans repaired     : {repaired} (attempted)")

if __name__ == "__main__":
    main()