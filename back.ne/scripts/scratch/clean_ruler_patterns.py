#!/usr/bin/env python3
import json, argparse
from pathlib import Path

# heuristics to drop overly generic patterns
BAN_FIRST_LOWER = {"at","of","in","near","to","from","on","by","into","onto","upon"}
MAX_LEN = 5  # names are short; avoid sprawling matches
ALLOW_OP = set()  # disallow OP wildcards entirely for gazetteers

def is_punctish(tok):
    return isinstance(tok, str) and tok.strip() in {".", ",", ";", ":", "!", "?", "’", "'", "“", "”", "(", ")", "-", "—"}

def too_generic(pattern):
    # pattern is either a string or a list of token dicts
    if isinstance(pattern, str):
        txt = pattern.strip()
        return (len(txt) <= 1) or is_punctish(txt)

    if not isinstance(pattern, list) or not pattern:
        return True

    # length too long?
    if len(pattern) > MAX_LEN:
        return True

    # any OP usage?
    for t in pattern:
        if isinstance(t, dict) and "OP" in t and t["OP"] not in ALLOW_OP:
            return True

    # first token preposition + more tokens -> generic
    t0 = pattern[0]
    if isinstance(t0, dict) and "LOWER" in t0 and t0["LOWER"] in BAN_FIRST_LOWER and len(pattern) > 1:
        return True

    # patterns that are only casing flags w/out concrete text
    if all(isinstance(t, dict) and ("IS_TITLE" in t or "IS_UPPER" in t or "IS_ALPHA" in t) and "LOWER" not in t for t in pattern):
        return True

    # single-token that's clearly punctuation-ish
    if len(pattern) == 1:
        t = pattern[0]
        if isinstance(t, dict) and "LOWER" in t and is_punctish(t["LOWER"]):
            return True

    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="path to existing ruler_patterns dir")
    ap.add_argument("--out", dest="out", required=True, help="path to cleaned ruler_patterns dir")
    args = ap.parse_args()

    src = Path(args.inp).resolve()
    dst = Path(args.out).resolve()
    dst.mkdir(parents=True, exist_ok=True)

    src_file = src / "patterns.jsonl"
    dst_file = dst / "patterns.jsonl"
    if not src_file.exists():
        raise SystemExit(f"patterns.jsonl not found in {src}")

    kept = 0; dropped = 0
    with src_file.open(encoding="utf-8") as fin, dst_file.open("w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip(): 
                continue
            rec = json.loads(line)
            pat = rec.get("pattern")
            if too_generic(pat):
                dropped += 1
                continue
            # fix some known mislabels (optional): Sāvatthī etc. should be GPE
            # if rec["label"] == "LOC" and isinstance(pat, list) and any(
            #         isinstance(t, dict) and t.get("LOWER") in {"sāvatthī","savatthi","vesālī","vesali","rājagaha","rajagaha","rajgir"} for t in pat):
            #     rec["label"] = "GPE"
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            kept += 1

    print(f"kept {kept} patterns, dropped {dropped}")
    # copy optional config if present
    cfg_in = src / "cfg"
    if cfg_in.exists():
        (dst / "cfg").write_bytes(cfg_in.read_bytes())

if __name__ == "__main__":
    main()