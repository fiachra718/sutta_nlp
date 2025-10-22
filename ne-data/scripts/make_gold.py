import json, re, sys
from pathlib import Path

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


TEXT = """The two sages Jali [6] and Atthaka, then Kosala, the enlightened one, then Subahu, Upanemisa, Nemisa, Santacitta, Sacca, Tatha, Viraja, and Pandita."""
TARGETS = [
    ("PERSON", "Jali"),
    ("PERSON", "Atthaka"),
    ("PERSON", "Kosala"),
    ("PERSON", "Subahu"),
    ("PERSON", "Upanemisa"),
    ("PERSON", "Nemisa"),
    ("PERSON", "Santacitta"),
    ("PERSON", "Sacca"),
    ("PERSON", "Tatha"),
    ("PERSON", "Viraja"),
    ("PERSON", "Pandita"),
    # ("PERSON", "Subahu"),
    # ("PERSON", "Subahu"),
]


spans = find_spans(TEXT, TARGETS)
print(json.dumps({"text": TEXT, "spans": spans}, ensure_ascii=False))


TEXT = "Pilgrims visited Isipatana Deer Park."
TARGETS = [
    ("NORP", "Pilgrims"),
    ("LOC", "Isipatana Deer Park"),
]

spans = find_spans(TEXT, TARGETS)
print(json.dumps({"text": TEXT, "spans": spans}, ensure_ascii=False))