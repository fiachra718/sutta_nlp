#!/usr/bin/env python3
"""Repair mojibake in crawl_pali_names JSONL output.

Common case handled: UTF-8 bytes decoded as latin-1/cp1252, yielding text like
"GosÄla" instead of "Gosāla".
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


MARKERS = ("Ã", "Ä", "Å", "Â", "â", "ï»¿")


def maybe_fix_mojibake(s: str) -> tuple[str, bool]:
    if not s or not any(m in s for m in MARKERS):
        return s, False
    try:
        fixed = s.encode("latin-1", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        return s, False
    # Accept only if it actually improves marker count.
    old_score = sum(s.count(m) for m in MARKERS)
    new_score = sum(fixed.count(m) for m in MARKERS)
    if new_score < old_score:
        return fixed, True
    return s, False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-jsonl", required=True)
    ap.add_argument("--out-jsonl", required=True)
    args = ap.parse_args()

    in_path = Path(args.in_jsonl).resolve()
    out_path = Path(args.out_jsonl).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    changed_records = 0
    changed_fields = 0

    with in_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            rec = json.loads(line)
            rec_changed = False
            for field in ("title", "text"):
                val = rec.get(field)
                if isinstance(val, str):
                    fixed, changed = maybe_fix_mojibake(val)
                    if changed:
                        rec[field] = fixed
                        rec_changed = True
                        changed_fields += 1
            if rec_changed:
                changed_records += 1
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(
        f"done total={total} changed_records={changed_records} changed_fields={changed_fields} "
        f"out={out_path}"
    )


if __name__ == "__main__":
    main()

