import json, re, sys
from pathlib import Path

TEXT = """And furthermore, with the fading of rapture, he remains equanimous, mindful, & alert, and senses pleasure with the body. He enters & remains in the third jhana, of which the Noble Ones declare, 'Equanimous & mindful, he has a pleasant abiding.'"""
# Add the phrases you want labeled, in order they appear:
TARGETS = [
    ("NORP", "Noble Ones"),
    # ("PERSON", "Vyagghapajja"),
    # ("GPE", "Macchikasanda"),
    # ("LOC", "Wild Mango Grove"),
    # ("PERSON", "Citta"),
    # ("PERSON", "Ratthapala"),

    # ("GPE", "Rājagaha"),
    # ("LOC", "Bamboo Grove"),
    # ("LOC", "Squirrels' Sanctuary"),
    # ("GPE", "Rājagaha"),
    # ("PERSON", "Suppabuddha"),
    # ("PERSON", "Blessed One"),
    # ("PERSON", "Suppabuddha"),
    # ("PERSON", "Blessed One"),
    # ("PERSON", "Gotama"),
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