import json, csv, re, unicodedata
from collections import defaultdict

def ascii_fold(s): 
    return unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode()

rows = {}  # key: (label, slug) -> row dict
def slugify(label, text):
    base = re.sub(r'[^a-z0-9]+','.', ascii_fold(text.lower())).strip('.')
    return f"{label.lower()}.{base}"

with open("ne-data/patterns/entity_ruler/patterns.jsonl", encoding="utf-8") as f:
    for line in f:
        if not line.strip(): continue
        pat = json.loads(line)
        label = pat["label"]
        ent_id = pat.get("id")
        pattern = pat["pattern"]
        text = pattern if isinstance(pattern, str) else pattern.get("REGEX","")
        if not ent_id:
            # make a stable-ish id from first seen surface
            ent_id = slugify(label, text)
        key = ent_id
        row = rows.setdefault(key, {
            "id": ent_id,
            "type": label,
            "canonical": text if isinstance(text, str) else ent_id.split('.',1)[-1],
            "aliases": set(),
            "parent_id": "",
            "variant_regex": ""
        })
        if isinstance(pattern, str):
            row["aliases"].add(pattern)
        else:
            row["variant_regex"] = pattern.get("REGEX","")

with open("ne-data/gazetteer/master.csv","w",newline="",encoding="utf-8") as out:
    w = csv.writer(out)
    w.writerow(["id","type","canonical","aliases","parent_id","variant_regex"])
    for r in rows.values():
        aliases = sorted(a for a in r["aliases"] if a != r["canonical"])
        w.writerow([r["id"], r["type"], r["canonical"], "; ".join(aliases), r["parent_id"], r["variant_regex"]])