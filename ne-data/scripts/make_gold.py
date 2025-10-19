import json, re, sys
from pathlib import Path

'''
Find candidate text.  Paste it into this file.  If it passes muster, add it 
to the work/gold.jsonl
work/gold.jsonl will always be loaded for training. 
Want ~ 300 gold examples
 '''

TEXT = """I have heard that on one occasion the Blessed One was staying near Rājagaha at the Bamboo Grove, the Squirrels' Sanctuary. And on that occasion in Rājagaha there was a leper named Suppabuddha, a poor, miserable wretch of a person. And on that occasion the Blessed One was sitting surrounded by a large assembly, teaching the Dhamma. Suppabuddha the leper saw the large gathering of people from afar and thought to himself, Without a doubt, someone must be distributing staple or non-staple food there. Why don't I go over to that large group of people, and maybe there I'll get some staple or non-staple food. So he went over to the large group of people. Then he saw the Blessed One sitting surrounded by a large assembly, teaching the Dhamma. On seeing this, he realized, There's no one distributing staple or non-staple food there. That's Gotama the contemplative (sitting) surrounded, teaching the Dhamma. Why don't I listen to the Dhamma? So he sat down to one side right there, [thinking,] I, too, will listen to the Dhamma.
"""
# Add the phrases you want labeled, in order they appear:
TARGETS = [
    ("PERSON", "Blessed One"),
    ("GPE", "Rājagaha"),
    ("LOC", "Bamboo Grove"),
    ("LOC", "Squirrels' Sanctuary"),
    ("GPE", "Rājagaha"),
    ("PERSON", "Suppabuddha"),
    ("PERSON", "Blessed One"),
    ("PERSON", "Suppabuddha"),
    ("PERSON", "Blessed One"),
    ("PERSON", "Gotama the contemplative"),
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