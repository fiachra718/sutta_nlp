import sys, json, pathlib, spacy
from spacy.tokens import Doc
from spacy.util import filter_spans

def load_jsonl(p):
    with open(p, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception as e:
                print(f"[line {i}] JSON error: {e}", file=sys.stderr)
                continue
            yield i, rec

def spans_from_charspans(nlp, text, spans):
    doc = nlp.make_doc(text)
    good, bad_none, bad_oob = [], [], []
    for sp in spans:
        start, end, label = sp["start"], sp["end"], sp["label"]
        # bounds check first
        if start < 0 or end > len(text) or start >= end:
            bad_oob.append(sp); continue
        span = doc.char_span(start, end, label=label, alignment_mode="contract")
        if span is None:
            bad_none.append(sp)
        else:
            good.append(span)
    return doc, good, bad_none, bad_oob

def tokenset(span):
    return set(range(span.start, span.end))

def any_overlap(spans):
    # return pairs that overlap at token-level
    overlaps = []
    for i in range(len(spans)):
        for j in range(i+1, len(spans)):
            if tokenset(spans[i]) & tokenset(spans[j]):
                overlaps.append((i, j))
    return overlaps

def main(path):
    nlp = spacy.blank("en")
    had_issue = False
    for lineno, rec in load_jsonl(path):
        text = rec.get("text","")
        spans = rec.get("spans", rec.get("entities", []))
        doc, good, bad_none, bad_oob = spans_from_charspans(nlp, text, spans)
        # duplicate detection
        uniq = {}
        dups = []
        for s in spans:
            key = (s["start"], s["end"], s["label"])
            if key in uniq: dups.append(s)
            else: uniq[key] = 1

        overlaps = any_overlap(good)

        if bad_oob or bad_none or overlaps or dups:
            had_issue = True
            print(f"\n[line {lineno}] -> issues found")
            if bad_oob:
                print("  - OOB spans (check indices / end-exclusive):", bad_oob)
            if bad_none:
                print("  - Unalignable spans (likely wrong indices / quotes):", bad_none)
            if dups:
                print("  - Duplicate spans:", dups)
            if overlaps:
                print("  - Overlapping spans (token-level):")
                for i,j in overlaps:
                    si, sj = good[i], good[j]
                    print(f"      * '{si.text}' ({si.start_char},{si.end_char},{si.label_})"
                          f" OVERLAPS '{sj.text}' ({sj.start_char},{sj.end_char},{sj.label_})")
    if not had_issue:
        print("✅ No issues detected.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python find_span_issues.py <file.jsonl>")
        sys.exit(2)
    main(sys.argv[1])
    