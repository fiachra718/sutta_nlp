import ast
import csv
from pathlib import Path

# Adjust this to your real path:
MENTIONS_PATH = Path("graph/entities/people_mentions_dump.jsonl")  # or whatever it's called

IMPORT_DIR = Path(
    "/Users/alee/Library/Application Support/neo4j-desktop/"
    "Application/Data/dbmss/dbms-28ea40de-93c0-4ede-b491-dd6fcaf58e82/import"
)
CSV_PATH = IMPORT_DIR / "person_sutta.csv"


def make_uid(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace("’", "")
        .replace("'", "")
        .replace(" ", "_")
    )


with MENTIONS_PATH.open("r", encoding="utf-8") as infile, CSV_PATH.open(
    "w", encoding="utf-8", newline=""
) as outfile:
    writer = csv.writer(outfile)
    writer.writerow(["person_uid", "person_name", "sutta_ref", "count"])

    for lineno, line in enumerate(infile, start=1):
        line = line.strip()
        if not line:
            continue

        try:
            # Each line looks like: [{'Anathapindika': [{'ref': 'AN 10.48', 'count': 1}, ...]}]
            records = ast.literal_eval(line)
        except Exception as e:
            print(f"Parse error on line {lineno}: {e}")
            print(f"Offending line: {line!r}")
            break

        for mapping in records:           # each mapping is {'Name': [ {ref,count}, ... ]}
            for name, refs in mapping.items():
                uid = make_uid(name)
                for rc in refs:
                    sutta_ref = rc["ref"]     # e.g. "AN 10.48"
                    count = rc["count"]       # integer
                    writer.writerow([uid, name, sutta_ref, count])

print(f"Wrote {CSV_PATH}")
