import ast
import csv
from pathlib import Path

RAW_GPE_PATH = Path("graph/entities/gpe.csv")  #  current NOT-CSV file

# Output for later steps
NODES_CSV = Path("graph/entities/gpe_nodes.csv")
ALIASES_CSV = Path("graph/entities/gpe_aliases.csv")


def make_uid(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace("’", "")
        .replace("'", "")
        .replace(" ", "_")
    )


with RAW_GPE_PATH.open("r", encoding="utf-8") as infile, \
     NODES_CSV.open("w", encoding="utf-8", newline="") as nodes_out, \
     ALIASES_CSV.open("w", encoding="utf-8", newline="") as alias_out:

    nodes_writer = csv.writer(nodes_out)
    alias_writer = csv.writer(alias_out)

    nodes_writer.writerow(["uid", "name", "aliases"])
    alias_writer.writerow(["gpe_uid", "gpe_name", "alias"])

    for lineno, line in enumerate(infile, start=1):
        line = line.strip()
        if not line:
            continue

        # Turn `"Varanasi", ["Bārāṇasī","Vārāṇasī","Benares"]`
        # into ["Varanasi", ["Bārāṇasī","Vārāṇasī","Benares"]]
        try:
            name, aliases = ast.literal_eval(f"[{line}]")
        except Exception as e:
            print(f"Parse error on line {lineno}: {e}")
            print(f"Offending line: {line!r}")
            break

        uid = make_uid(name)
        aliases = aliases or []

        # gpe_nodes.csv: uid,name,aliases joined by |
        aliases_str = "|".join(aliases)
        nodes_writer.writerow([uid, name, aliases_str])

        # gpe_aliases.csv: one row per alias (including canonical name)
        alias_writer.writerow([uid, name, name])
        for a in aliases:
            alias_writer.writerow([uid, name, a])

print(f"Wrote {NODES_CSV}")
print(f"Wrote {ALIASES_CSV}")