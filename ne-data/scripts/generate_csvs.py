import spacy
import config_helpers  # cargo cult this MOFO
from hashlib import md5

from local_settings import WORK, MODELS_DIR, SPAN_PATTERNS, PATTERNS

def load_model():
    nlp = spacy.load(MODELS_DIR)
    # clear the pipe
    for name in ("entity_ruler", "span_ruler"):
        if name in nlp.pipe_names:
            nlp.remove_pipe(name)

    # add the entity rules
    er = nlp.add_pipe(
        "entity_ruler",
        after="ner",
        config={"overwrite_ents": True}
    )
    er.from_disk(str(PATTERNS))  

    # add the LOC/span patterns
    sr = nlp.add_pipe(
        "span_ruler",
        last=True,
        config={"spans_key": "LOC_PHRASES", "overwrite": True}
    )
    sr.from_disk("ne-data/patterns/span_ruler")  # folder; contains a file named 'patterns'

    return nlp


nlp = load_model()

people = set()
with open(str(WORK / "lines.txt"), encoding="utf-8") as f:
    lines = [ln.strip() for ln in f.readlines() if ln.strip()]
    for l in lines:
        doc = nlp(l.strip())
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                people.add(ent.text)
                # print(ent.text)

# 
#     for line in f.readlines():
#         text = line.strip()
#         doc = nlp(text)
#         for ent in doc.ents:
#             if ent.label_ == "PERSON":
#                 print("{} : {} : {}".format(ent.label, ent.text, ent.
#                 people.add(ent.text)


for i, p in enumerate(people):
    print("{}, {}, {}".format(i, p, md5(p.encode('utf-8')).hexdigest()))
