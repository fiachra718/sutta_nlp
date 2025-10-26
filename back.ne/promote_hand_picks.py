#!/usr/bin/env python3
import json, argparse, unicodedata
from pathlib import Path
from typing import List, Tuple, Dict

def strip_diacritics(s: str) -> str:
    nfkd = unicodedata.normalize("NFD", s)
    base = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return unicodedata.normalize("NFC", base)

def build_norm_map(s: str):
    """Return (norm, idx_map) where norm is diacritic-stripped lowercase and
    idx_map maps positions in norm -> positions in original string."""
    norm_chars = []
    idx_map = []
    for i, ch in enumerate(s):
        nfd = unicodedata.normalize("NFD", ch)
        for sub in nfd:
            if unicodedata.combining(sub):
                continue
            norm_chars.append(sub.lower())
            idx_map.append(i)
    norm = "".join(norm_chars)
    return norm, idx_map

def find_all_spans(text: str, term: str) -> List[Tuple[int,int]]:
    """Find all occurrences of term in text, case- & diacritic-insensitive.
    Returns spans in ORIGINAL text indices."""
    if not term:
        return []
    norm_text, idx_map = build_norm_map(text)
    norm_term, _ = build_norm_map(term)
    if not norm_term:
        return []
    spans = []
    start = 0
    while True:
        pos = norm_text.find(norm_term, start)
        if pos == -1:
            break
        orig_start = idx_map[pos]
        last_norm_idx = pos + len(norm_term) - 1
        orig_end = idx_map[last_norm_idx] + 1  # exclusive
        spans.append((orig_start, orig_end))
        start = pos + 1
    return spans

def merge_overlaps(spans: List[Tuple[int,int,str]]) -> List[Tuple[int,int,str]]:
    """Merge identical spans; for overlaps, keep the longer span."""
    if not spans:
        return []
    spans = sorted(spans, key=lambda x: (x[0], -(x[1]-x[0])))
    out = []
    for s,e,lbl in spans:
        if not out:
            out.append((s,e,lbl))
            continue
        ps,pe,pl = out[-1]
        if s < pe and e > ps:
            if (e-s) > (pe-ps):
                out[-1] = (s,e,lbl)
        else:
            out.append((s,e,lbl))
    uniq = []
    seen = set()
    for s,e,lbl in out:
        key = (s,e)
        if key in seen:
            continue
        uniq.append((s,e,lbl))
        seen.add(key)
    return uniq

def load_terms(path: Path) -> Dict[str, list]:
    data = json.loads(path.read_text(encoding="utf-8"))
    out = {}
    for label, arr in data.items():
        if not isinstance(arr, list):
            continue
        cleaned = [a for a in arr if isinstance(a, str) and a.strip()]
        if cleaned:
            out[label.upper()] = cleaned
    return out

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", required=True, help="hand_picks.txt (one paragraph per line)")
    ap.add_argument("--terms", required=True, help="terms.json (dict of label -> [terms])")
    ap.add_argument("--out",   required=True, help="output JSONL with text + entities")
    ap.add_argument("--label-order", default="", help="comma list pref labels when overlaps (e.g. PERSON,GPE,LOC,NORP)")
    args = ap.parse_args()

    lines_path = Path(args.lines)
    terms_path = Path(args.terms)
    out_path   = Path(args.out)
    label_pref = [x.strip().upper() for x in args.label_order.split(",") if x.strip()]
    label_rank = {lbl: i for i, lbl in enumerate(label_pref)} if label_pref else {}

    terms = load_terms(terms_path)

    count = 0
    with lines_path.open(encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for raw in fin:
            text = raw.rstrip("\n")
            if not text.strip():
                continue

            spans = []
            for label, words in terms.items():
                for w in words:
                    for (s,e) in find_all_spans(text, w):
                        spans.append((s,e,label))

            if label_rank:
                spans.sort(key=lambda x: (x[0], label_rank.get(x[2], 999), -(x[1]-x[0])))
            ents = merge_overlaps(spans)

            rec = {"text": text, "entities": [[s,e,lbl] for (s,e,lbl) in ents]}
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            count += 1

    print(f"Wrote {count} JSONL lines to {out_path}")

if __name__ == "__main__":
    main()
