import spacy
import config_helpers  # registers custom registry hooks for spaCy
import json
from pathlib import Path
import sys
from local_settings import load_model

nlp = load_model()

#  Tag verse passed on STDIN
#  if not sys.argv[1]: sys.exit(-1)

for raw in sys.stdin:
    line = raw.rstrip("\n").strip()
    # line = sys.stdin.strip()
    doc = nlp(line)
    results = {}
    entities = []
    for ent in doc.ents:
        entities.append( [ent.label_, ent.text, ent.start_char, ent.end_char] )
        # entities.append({"start": ent.start_char, "end":ent.end_char, "label":ent.label_, "text":ent.text})
        if len(entities):
            results = {"text": line, "entities": [ent for ent in entities]}

    print(json.dumps(results, ensure_ascii=False))
