import ast
import csv
from pathlib import Path

# Path to your raw LOC file (the weird one you just showed)
RAW_LOC_PATH = Path("graph/entities/loc.csv")   # adjust if needed

# Output for Neo4j node load
LOC_NODES_CSV = Path("graph/entities/loc_nodes.csv")
LOC_ALIASES_CSV = Path("graph/entities/loc_aliases.csv")  # optional, for Postgres later


def make_uid(name: str) -> str:
    # Same uid scheme as Person/GPE
    return (
        name.strip()
        .lower()
        .replace("’", "")
        .replace("'", "")
        .replace(" ", "_")
    )


with RAW_LOC_PATH.open("r", encoding="utf-8") as infile, \
     LOC_NODES_CSV.open("w", encoding="utf-8", newline="") as nodes_out, \
     LOC_ALIASES_CSV.open("w", encoding="utf-8", newline="") as aliases_out:

    nodes_writer = csv.writer(nodes_out)
    aliases_writer = csv.writer(aliases_out)

    # loc_nodes.csv header
    nodes_writer.writerow(["uid", "name", "aliases"])

    # loc_aliases.csv header (for future Postgres matching if you want it)
    aliases_writer.writerow(["loc_uid", "loc_name", "alias"])

    for lineno, line in enumerate(infile, start=1):
        line = line.strip()
        if not line:
            continue

        # Your line is like:
        #   "Aggalava Shrine", "aliases": []
        #   "Deer Park at Isipatana", "aliases": ["Deer Park"]
        #
        # Wrap it to look like a Python dict:
        #   {"name": "Aggalava Shrine", "aliases": []}
        try:
            obj = ast.literal_eval('{"name": ' + line + '}')
        except Exception as e:
            print(f"Parse error on line {lineno}: {e}")
            print(f"Offending line: {line!r}")
            break

        name = obj["name"]
        aliases = obj.get("aliases", []) or []

        uid = make_uid(name)

        # loc_nodes.csv: one row per LOC
        aliases_str = "|".join(aliases)
        nodes_writer.writerow([uid, name, aliases_str])

        # loc_aliases.csv: one row per alias, including canonical name
        aliases_writer.writerow([uid, name, name])
        for a in aliases:
            aliases_writer.writerow([uid, name, a])

print(f"Wrote {LOC_NODES_CSV}")
print(f"Wrote {LOC_ALIASES_CSV}")