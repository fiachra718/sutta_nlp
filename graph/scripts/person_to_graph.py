import json
import csv
from pathlib import Path

# adjust these
JSONL_PATH = Path("graph/entities/gold_people.jsonl")
IMPORT_DIR = Path("/Users/alee/Library/Application Support/neo4j-desktop/Application/Data/dbmss/dbms-28ea40de-93c0-4ede-b491-dd6fcaf58e82/import")
CSV_PATH = IMPORT_DIR / "people.csv"


def make_uid(name: str) -> str:
    # very dumb but good enough: "Akkosaka Bharadvaja" -> "akkosaka_bharadvaja"
    return (
        name.strip()
        .lower()
        .replace("’", "")
        .replace("'", "")
        .replace(" ", "_")
    )


with JSONL_PATH.open("r", encoding="utf-8") as infile, CSV_PATH.open("w", encoding="utf-8", newline="") as outfile:
    writer = csv.writer(outfile)
    writer.writerow(["uid", "name", "aliases"])

    for line in infile:
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        name = obj["name"]
        aliases = obj.get("aliases", []) or []

        uid = make_uid(name)
        aliases_str = "|".join(aliases)

        writer.writerow([uid, name, aliases_str])

print(f"Wrote {CSV_PATH}")