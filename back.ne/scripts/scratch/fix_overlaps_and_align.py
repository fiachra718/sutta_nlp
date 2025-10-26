#!/usr/bin/env python3
# fix_overlaps_and_align.py
import argparse, json, spacy, re

PUNCT_EDGE = re.compile(r"^[\s\"'“”‘’,.:;!?()\-]+|[\s\"'“”‘’,.:;!?()\-]+$")

# OPTIONAL: prefer certain labels if two spans overlap
LABEL_PRIORITY = {"PERSON": 3, "GPE": 2, "LOC": 1}

def resolve_overlaps(spans):
    """
    Input: list of (start, end, label) possibly overlapping.
    Output: non-overlapping list using this heuristic:
      - keep span with higher LABEL_PRIORITY
      - tie-break by longer length
      - tie-break by earlier start
    """
    spans_sorted = sorted(spans, key=lambda x: (x[0], -(x[1]-x[0])))
    result = []
    for s, e, lbl in spans_sorted:
        keep = True
        for i, (S, E, L) in enumerate(list(result)):
            if max(s, S) < min(e, E):  # overlap
                pr_self = LABEL_PRIORITY.get(lbl, 0)
                pr_that = LABEL_PRIORITY.get(L, 0)
                if (pr_self, e - s, -s) > (pr_that, E - S, -S):
                    # replace existing span
                    result[i] = (s, e, lbl)
                # either way, don't add a second overlapping span
                keep = False
                break
        if keep:
            result.append((s, e, lbl))
    # sort final by start
    return sorted(result, key=lambda x: x[0])

def align_and_trim(text, spans, tok):
    """
    Snap spans to token boundaries and trim stray punctuation.
    Returns list of (start,end,label) valid for spaCy training.
    """
    doc = tok.make_doc(text)
    fixed = []
    for s, e, lbl in spans:
        span = doc.char_span(s, e, label=lbl, alignment_mode="expand")
        if not span:
            continue
        s2, e2 = span.start_char, span.end_char
        surf = text[s2:e2]
        if not surf:
            continue
        lead = len(surf) - len(surf.lstrip(" \t\r\n\"'“”‘’,.:;!?()-"))
        tail = len(surf.rstrip(" \t\r\n\"'“”‘’,.:;!?()-"))
        s3, e3 = s2 + lead, s2 + tail
        if s3 < e3:
            fixed.append((s3, e3, lbl))
    return fixed

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="input JSONL")
    ap.add_argument("--out", dest="out", required=True, help="output JSONL")
    ap.add_argument("--lang", default="en")
    args = ap.parse_args()

    tok = spacy.blank(args.lang)

    kept, dropped_empty = 0, 0
    total_in, total_out = 0, 0
    with open(args.inp, encoding="utf-8") as fin, open(args.out, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip(): continue
            j = json.loads(line)
            text = j["text"]
            spans = [(int(s), int(e), str(lbl)) for s, e, lbl in j["entities"]]
            total_in += len(spans)

            # 1) align + trim first (helps the priority step be consistent)
            spans = align_and_trim(text, spans, tok)
            # 2) resolve overlaps
            spans = resolve_overlaps(spans)

            if spans:
                kept += 1
                total_out += len(spans)
                rec = {"text": text, "entities": spans}
                if "meta" in j: rec["meta"] = j["meta"]
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            else:
                dropped_empty += 1

    print(f"Examples written : {kept}")
    print(f"Examples dropped : {dropped_empty}")
    print(f"Spans in/out     : {total_in} -> {total_out}")

if __name__ == "__main__":
    main()
