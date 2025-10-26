#!/usr/bin/env python3
import json, argparse, unicodedata, copy
from pathlib import Path

# set True to also drop generic over-broad patterns
DROP_GENERIC = False
BAN_FIRST_LOWER = {"at","of","in","near","to","from","on","by","into","onto","upon"}
MAX_LEN = 5

DROP_GENERIC = True

def strip_diacritics(s: str) -> str:
    # NFD -> remove combining marks -> NFC
    nfkd = unicodedata.normalize("NFD", s)
    base = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return unicodedata.normalize("NFC", base)

def apostrophe_variants(s: str):
    # Produce both curly and straight versions if needed
    curly = s.replace("'", "’")
    straight = s.replace("’", "'")
    return sorted(set([s, curly, straight]))

def normalize_token_dict(tok):
    t = copy.deepcopy(tok)
    if "LOWER" in t and isinstance(t["LOWER"], str):
        # normalize NFC
        t["LOWER"] = unicodedata.normalize("NFC", t["LOWER"])
    return t

def too_generic(pattern):
    """Return True if a pattern is overly broad.
    Be robust to non-string LOWER values (e.g., dicts for advanced matchers)."""
    if not DROP_GENERIC:
        return False

    # String patterns are always too generic unless they are 2+ chars
    if isinstance(pattern, str):
        txt = pattern.strip()
        return len(txt) <= 1

    # Must be a list of token dicts
    if not isinstance(pattern, list) or not pattern:
        return True

    # Very long sequences are suspicious for gazetteers
    if len(pattern) > MAX_LEN:
        return True

    # Disallow wildcard operators for gazetteer rules
    for t in pattern:
        if isinstance(t, dict) and "OP" in t:
            return True

    # Helper to get a lowercase string from token["LOWER"], else None
    def lower_str(tok):
        if not isinstance(tok, dict):
            return None
        v = tok.get("LOWER")
        return v if isinstance(v, str) else None

    # If ANY token lacks a concrete LOWER (e.g., only IS_TITLE/flags), treat as generic
    if any(lower_str(t) is None for t in pattern):
        return True

    # Nuke preposition-led patterns such as: {LOWER:'at'} + anything
    STOP_LOWER = {"at","of","in","near","to","from","on","by","into","onto","upon","the"}
    t0_lower = lower_str(pattern[0])
    if t0_lower in STOP_LOWER and len(pattern) >= 1:
        return True

    # Single-token punctuation-ish patterns are not useful
    if len(pattern) == 1:
        t_lower = lower_str(pattern[0])
        if isinstance(t_lower, str) and t_lower in {".", ",", ";", ":", "!", "?", "'", "’", "(", ")", "-", "—"}:
            return True

    return False

def expand_pattern(rec):
    """
    Input: {"label": ..., "pattern": either str or list[dict]}
    Output: list of normalized+expanded patterns (list of dict tokens)
    """
    pat = rec["pattern"]
    label = rec["label"]

    # We only support token-pattern lists here (what EntityRuler wants for precise gazetteer).
    if isinstance(pat, str):
        # Convert a plain string to a single-token LOWER pattern
        s = unicodedata.normalize("NFC", pat)
        return [[{"LOWER": s.lower()}]]

    if not isinstance(pat, list):
        return []

    # Normalize each token dict
    toks = [normalize_token_dict(t) for t in pat]

    # Build variant grids for each token: (LOWER with diacritics) x (without diacritics) x (apostrophe variants)
    variant_lists = []
    for t in toks:
        if "LOWER" in t and isinstance(t["LOWER"], str):
            base = t["LOWER"].lower()
            diacs = sorted(set([base, strip_diacritics(base)]))
            apvars = set()
            for d in diacs:
                for v in apostrophe_variants(d):
                    apvars.add(v)
            # make a token dict for each variant
            variant_lists.append([dict({k:v for k,v in t.items() if k!="LOWER"}, LOWER=v) for v in sorted(apvars)])
        else:
            # keep non-LOWER tokens as-is (rare in your gazetteer)
            variant_lists.append([t])

    # Cartesian product
    expanded = [[]]
    for choices in variant_lists:
        expanded = [prev + [c] for prev in expanded for c in choices]

    # Drop generic if requested
    expanded = [p for p in expanded if not too_generic(p)]

    return expanded

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="path to ruler_patterns dir (must contain patterns.jsonl)")
    ap.add_argument("--out", dest="out", required=True, help="output dir for normalized+expanded patterns")
    args = ap.parse_args()

    src = Path(args.inp).resolve()
    dst = Path(args.out).resolve()
    dst.mkdir(parents=True, exist_ok=True)

    src_file = src / "patterns.jsonl"
    if not src_file.exists():
        raise SystemExit(f"patterns.jsonl not found in {src}")

    out_file = dst / "patterns.jsonl"

    kept, wrote = 0, 0
    with src_file.open(encoding="utf-8") as fin, out_file.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if "label" not in rec or "pattern" not in rec:
                continue

            # normalize label just in case
            rec["label"] = rec["label"].strip().upper()

            patterns = expand_pattern(rec)
            for p in patterns:
                out_rec = {"label": rec["label"], "pattern": p}
                # ensure_ascii=False preserves Unicode; spaCy expects UTF-8 here
                fout.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
                wrote += 1
            kept += 1

    print(f"read {kept} rules; wrote {wrote} expanded/normalized patterns to {out_file}")

    # copy cfg if present
    cfg_in = src / "cfg"
    if cfg_in.exists():
        (dst / "cfg").write_bytes(cfg_in.read_bytes())

if __name__ == "__main__":
    main()