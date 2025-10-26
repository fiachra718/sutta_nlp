# fix_spans.py
import re, json, sys, unicodedata

def nfc(s): return unicodedata.normalize("NFC", s)

def find_spans(text, term, case_sensitive=True):
    # word-boundary search; tweak if you ever want inside-compound matches
    flags = 0 if case_sensitive else re.IGNORECASE
    # Allow diacritics; \b works fine with Unicode letters in Python's regex.
    pat = re.compile(rf"\b{re.escape(term)}\b", flags)
    return [(m.start(), m.end()) for m in pat.finditer(text)]

def rebuild_entities(text, term2label):
    text = nfc(text)
    ents = []
    for term, label in term2label.items():
        for s, e in find_spans(text, nfc(term)):
            ents.append([s, e, label])
    # Ensure no overlaps & sort
    ents.sort(key=lambda x: (x[0], x[1]))
    cleaned = []
    last_e = -1
    for s,e,lbl in ents:
        if s >= last_e:     # keep non-overlapping only
            cleaned.append([s,e,lbl])
            last_e = e
        else:
            # overlap – keep the longer span
            if e - s > cleaned[-1][1] - cleaned[-1][0]:
                cleaned[-1] = [s,e,lbl]
                last_e = e
    return cleaned

if __name__ == "__main__":
    # Example usage on your one line passed via stdin or file
    # Input line is a JSON object with keys: text, entities (ignored), meta...
    # We’ll replace entities with regenerated ones.
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="-")
    ap.add_argument("--out", dest="out", default="-")
    # comma-separated TERM:LABEL pairs, e.g. "Mara:PERSON,Buddha:PERSON"
    ap.add_argument("--terms", required=True)
    args = ap.parse_args()

    term2label = {}
    for pair in args.terms.split(","):
        term, label = pair.split(":", 1)
        term2label[nfc(term.strip())] = label.strip().upper()

    fin = sys.stdin if args.inp == "-" else open(args.inp, "r", encoding="utf-8")
    fout = sys.stdout if args.out == "-" else open(args.out, "w", encoding="utf-8")

    for ln, line in enumerate(fin, 1):
        if not line.strip(): continue
        obj = json.loads(line)
        text = obj["text"]
        obj["entities"] = rebuild_entities(text, term2label)
        fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

    if fin is not sys.stdin: fin.close()
    if fout is not sys.stdout: fout.close()