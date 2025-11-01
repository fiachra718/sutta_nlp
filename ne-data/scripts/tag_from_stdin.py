import json
import sys
import unicodedata
from local_settings import load_model

nlp = load_model()

#  Tag verse passed on STDIN

for raw in sys.stdin:
    line = raw.rstrip("\n")
    if not line.strip():
        continue
    text = unicodedata.normalize("NFC", line)
    doc = nlp(line)
    spans = [
        {"start": ent.start_char, "end": ent.end_char, "label": ent.label_, "text": ent.text}
        for ent in doc.ents
    ]
    out = {"text": text, "spans": spans}
    print(json.dumps(out, ensure_ascii=False))
