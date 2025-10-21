import json, re, sys
from pathlib import Path

TEXT = """What do you think, monks? Which would in fact be the better? If a strong man, having twisted a firm horse-hair rope around both calves, were to rub, so that the rope cut the skin, and having cut the skin it cut the under-skin, and having cut the under-skin it cut the flesh, and having cut the flesh it cut the sinew, and having cut the sinew it cut the bone, and having cut the bone it left the marrow exposed? Or, to derive enjoyment from the homage of rich kshatriyas, or rich brahmans, or rich householders?\" — \"This, venerable Sir, is surely the better: To derive enjoyment from the homage of rich kshatriyas, or rich brahmans, or rich householders. For it would be painful, venerable Sir, if a strong man, having twisted a firm hair-rope around both calves, were to rub, so that the rope cut the skin and so on until it left the marrow exposed. '"""
TARGETS = [
    ("NORP", "kshatriyas"),
    ("NORP", "brahmans"),
    ("PERSON", "venerable Sir"),
    ("NORP", "kshatriyas"),
    ("NORP", "brahmans"),
    ("PERSON", "venerable Sir"),
]

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

spans = find_spans(TEXT, TARGETS)
print(json.dumps({"text": TEXT, "spans": spans}, ensure_ascii=False))