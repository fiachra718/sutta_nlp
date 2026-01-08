import json
import re
import sys
from pathlib import Path
from typing import TextIO


def find_spans(text, targets):
    spans = []
    pos = 0
    for label, phrase in targets:
        m = re.search(re.escape(phrase), text[pos:])
        if not m:
            # fallback from start (in case duplicates are out of order)
            m = re.search(re.escape(phrase), text)
            if not m:
                print(f"MISS: {phrase}", file=sys.stderr); continue
            start = m.start()
        else:
            start = pos + m.start()
        end = start + len(phrase)
        spans.append({"start": start, "end": end, "label": label})
        pos = end
    return spans


DEFAULT_INPUT = Path("ne-data/work/1025_candidates.jsonl")

input_arg = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_INPUT
output_arg = sys.argv[2] if len(sys.argv) > 2 else "-"

output_handle: TextIO
if output_arg == "-":
    output_handle = sys.stdout
else:
    output_path = Path(output_arg)
    output_handle = output_path.open("w", encoding="utf-8")

try:
    with input_arg.open(encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            data = json.loads(text)
            doc_text = data.get("text", "")
            targets = data.get("entities", [])
            if doc_text and len(targets) > 1:
                spans = find_spans(doc_text, targets)
                print(
                    json.dumps({"text": doc_text, "spans": spans}, ensure_ascii=False),
                    file=output_handle,
                )
finally:
    if output_handle is not sys.stdout:
        output_handle.close()
