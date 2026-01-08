import json
from local_settings import PATTERNS

with open(str(PATTERNS)) as f:
    # check THIS line
    lines = f.readlines()
    for i, line in enumerate(lines):
        print("reading line: {}".format(i))
        if line.strip() != "":
            j = json.loads(line.strip())