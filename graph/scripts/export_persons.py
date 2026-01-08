# file: graph/scripts/export_persons.py
import csv, json, hashlib, unicodedata, pathlib

INFILE  = pathlib.Path("graph/entities/people.jsonl")
OUTFILE = pathlib.Path("graph/entities/persons.csv")

def stable_id(name: str) -> str:
    # MD5 over a normalized canonical name for stability
    key = unicodedata.normalize("NFC", name.strip())
    return hashlib.md5(key.encode("utf-8")).hexdigest()

def to_json(val):
    return json.dumps(val, ensure_ascii=False)

def main():
    with INFILE.open("r", encoding="utf-8") as fin, OUTFILE.open("w", newline="", encoding="utf-8") as fout:
        w = csv.writer(fout)
        w.writerow(["id","canonical","aliases","verse_keys"])
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            name    = obj["name"]
            verses  = obj.get("verses", [])
            aliases = obj.get("aliases", [])
            pid = stable_id(name)
            verse_keys = [f"{ident}#{vnum}" for ident, vnum in verses]
            w.writerow([pid, name, to_json(aliases), to_json(verse_keys)])
    print(f"✅ persons.csv written: {OUTFILE}")

if __name__ == "__main__":
    main()
    