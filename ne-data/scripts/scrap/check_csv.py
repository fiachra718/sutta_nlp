import csv, re, sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "./ne-data/gazetteer/master.csv"
rows = list(csv.DictReader(open(path, encoding="utf-8")))
ids = {r["id"] for r in rows}

# 1) parent existence
for r in rows:
    p = r.get("parent_id","").strip()
    if p and p not in ids:
        print(f"[PARENT MISSING] {r['id']} -> {p}")

# 2) duplicate alias collisions across types
alias_to = defaultdict(list)
for r in rows:
    aliases = [a.strip() for a in (r.get("aliases") or "").split(";") if a.strip()]
    aliases += [r["canonical"].strip()]
    for a in aliases:
        alias_to[a.lower()].append((r["id"], r["type"]))
for alias, uses in alias_to.items():
    types = {t for _, t in uses}
    if len(uses) > 1 and len(types) > 1:
        print(f"[ALIAS COLLISION] '{alias}' -> {uses}")

# 3) regex sanity
for r in rows:
    rx = (r.get("variant_regex") or "").strip()
    if rx:
        try:
            re.compile(rx)
        except re.error as e:
            print(f"[BAD REGEX] {r['id']} {e}")

print("Done.")