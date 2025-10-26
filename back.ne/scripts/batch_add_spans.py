#!/usr/bin/env python3
import argparse, json, sys, unicodedata
from pathlib import Path
from typing import List, Tuple

def load_terms(path: Path) -> List[Tuple[str, str]]:
    """Load (phrase, label) pairs from TSV or JSON."""
    txt = path.read_text(encoding="utf-8").strip()
    if not txt:
        return []
    if path.suffix.lower() == ".tsv":
        out = []
        for i, line in enumerate(txt.splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                print(f"[terms] skip line {i}: need <phrase>\\t<label>", file=sys.stderr)
                continue
            phrase, label = parts[0].strip(), parts[1].strip().upper()
            if isinstance(label, (list, tuple)):
                try:
                    label = label[0]  # or skip / choose best
                except:
                    pass
            if phrase:
                out.append((phrase, label))
        return out
    else:
        data = json.loads(txt)
        out = []
        if isinstance(data, dict):
            # {"phrase":"label", ...}
            for k, v in data.items():
                out.append((str(k), str(v).upper()))
        elif isinstance(data, list):
            # [{"phrase":"...", "label":"..."}, ...] or ["Foo", "Bar"] (default label PERSON)
            for item in data:
                if isinstance(item, dict) and "phrase" in item and "label" in item:
                    out.append((str(item["phrase"]), str(item["label"]).upper()))
                elif isinstance(item, str):
                    out.append((item, "PERSON"))
        return out

def normalize(s: str, mode: str) -> str:
    if mode == "none":
        return s
    if mode == "nfc":
        return unicodedata.normalize("NFC", s)
    if mode == "nfd":
        return unicodedata.normalize("NFD", s)
    if mode == "strip":
        # NFD -> strip combining -> NFC
        nfd = unicodedata.normalize("NFD", s)
        base = "".join(ch for ch in nfd if not unicodedata.combining(ch))
        return unicodedata.normalize("NFC", base)
    return s

def find_all(
    text: str,
    needle: str,
    case_sensitive: bool,
    whole_word: bool,
    normalize_mode: str,  # kept for API compatibility; not used for offsets
) -> List[Tuple[int, int]]:
    """
    Find all non-overlapping matches in the RAW text by building a small
    set of robust variants of the needle (NFC, stripped diacritics, apostrophe variants).
    This avoids fragile 'normalized -> raw' offset mapping entirely.
    """
    import unicodedata, re

    def nfc(s: str) -> str:
        return unicodedata.normalize("NFC", s)

    def strip_diacritics(s: str) -> str:
        nfd = unicodedata.normalize("NFD", s)
        base = "".join(ch for ch in nfd if not unicodedata.combining(ch))
        return unicodedata.normalize("NFC", base)

    def apos_variants(s: str) -> set:
        return {s, s.replace("’", "'"), s.replace("'", "’")}

    # Build a compact set of variants to match in RAW text
    variants = set()
    for base in {needle, nfc(needle), strip_diacritics(needle)}:
        variants |= apos_variants(base)

    # Escape each variant for literal matching; combine with alternation
    # Sort longer-first to prefer longer variants when they overlap in regex engine
    parts = sorted((re.escape(v) for v in variants), key=len, reverse=True)
    if not parts:
        return []

    patt = "(?:" + "|".join(parts) + ")"
    if whole_word:
        patt = r"(?<!\w)" + patt + r"(?!\w)"

    flags = 0 if case_sensitive else re.IGNORECASE
    rx = re.compile(patt, flags)

    return [m.span() for m in rx.finditer(text)]

def overlaps(a: Tuple[int,int], b: Tuple[int,int]) -> bool:
    (s1,e1),(s2,e2) = a,b
    return not (e1 <= s2 or e2 <= s1)

def merge_spans(
    text: str,
    existing: List[Tuple[int,int,str]],
    candidates: List[Tuple[int,int,str]],
) -> List[Tuple[int,int,str]]:
    """Prefer longer spans; avoid overlaps with existing and already-accepted."""
    out = list(existing)
    taken = [(s,e) for (s,e,_) in existing]

    # sort candidates: longer first, then earlier
    candidates = sorted(candidates, key=lambda x: (-(x[1]-x[0]), x[0]))
    for s,e,label in candidates:
        if isinstance(label, (list, tuple)):
            try:
                label = label[0]  # or skip / choose best
            except:
                pass
        if s < 0 or e > len(text) or s >= e:
            continue
        if any(overlaps((s,e), pair) for pair in taken):
            continue
        out.append((s,e,label))
        taken.append((s,e))
    # nice order
    out.sort(key=lambda x: x[0])
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="input JSONL")
    ap.add_argument("--terms", required=True, help="terms TSV or JSON")
    ap.add_argument("--out", required=True, help="output JSONL")
    ap.add_argument("--case-sensitive", action="store_true", help="match phrases case-sensitively (default off)")
    ap.add_argument("--whole-word", action="store_true", help="enforce word boundaries (default off)")
    ap.add_argument("--normalize", choices=["none","nfc","nfd","strip"], default="nfc",
                    help="normalization for matching (default nfc)")
    args = ap.parse_args()

    terms = load_terms(Path(args.terms))
    if not terms:
        print("[warn] no terms loaded", file=sys.stderr)

    total_lines = 0
    updated = 0
    out_f = Path(args.out).open("w", encoding="utf-8")
    with Path(args.inp).open(encoding="utf-8") as fin:
        for total_lines, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception as e:
                print(f"[skip line {total_lines}] JSON error: {e}", file=sys.stderr)
                continue

            text = rec.get("text")
            if not isinstance(text, str) or not text:
                print(f"[skip line {total_lines}] no text", file=sys.stderr)
                continue

            # existing entities
            existing = []
            for ent in rec.get("entities", []) or []:
                try:
                    s, e, lbl = int(ent[0]), int(ent[1]), str(ent[2]).upper()
                except Exception:
                    continue
                existing.append((s,e,lbl))

            # gather candidates
            cands = []
            for phrase, label in terms:
                for s,e in find_all(
                    text=text,
                    needle=phrase,
                    case_sensitive=args.case_sensitive,
                    whole_word=args.whole_word,
                    normalize_mode=args.normalize,
                ):
                    cands.append((s,e,label))

            merged = merge_spans(text, existing, cands)
            if len(merged) != len(existing):
                updated += 1

            rec["entities"] = [[s,e,lbl] for (s,e,lbl) in merged]
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    out_f.close()
    print(f"processed lines: {total_lines}")
    print(f"updated records: {updated}")
    print(f"wrote -> {args.out}")

if __name__ == "__main__":
    main()