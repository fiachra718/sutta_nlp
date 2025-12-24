import json

line_count = 0
with open("ne-data/work/test_from_db.jsonl", "r", encoding="utf-8") as f:
    for line in f.readlines():
        line_count += 1
        doc = line.strip()
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            print("Error at line: {}, {}".format(line_count, e))