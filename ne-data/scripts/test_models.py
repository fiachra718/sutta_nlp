import spacy
import config_helpers  # registers custom registry hooks for the exported model
from spacy.pipeline import EntityRuler
import json
from pathlib import Path
from local_settings import MODELS_DIR, WORK  # , PATTERNS, SPAN_PATTERNS
# from local_settings import load_model

ENTITY_PATTERNS = Path("ne-data/patterns/entity_ruler/patterns.jsonl")
SPAN_PATTERNS = Path("ne-data/patterns/span_ruler/loc_phrases.json")

def entity_pos(model, text):
    doc = nlp(text)
    results = {}
    entities = []
    for ent in doc.ents:
        entities.append({"start": ent.start_char, "end": ent.end_char,"label": ent.label_, "text": ent.text})
    if len(entities):
        results = {"text": text, "spans": [ent for ent in entities]}
    return (results)

def load_my_ner():
    ''' becasue SpaCy is finicky, I am going to leave this here '''
    ## load the model
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
    er.from_disk(str(ENTITY_PATTERNS))            # load patterns so no [W036] warning
    # add the LOC/span patterns
    sr = nlp.add_pipe(
        "span_ruler",
        last=True,
        config={"spans_key": "LOC_PHRASES", "overwrite": True}
    )
    sr.from_disk("ne-data/patterns/span_ruler")  # folder; contains a file named 'patterns'
    print("Pipeline:", nlp.pipe_names)
    return (nlp)


sentences = []
with open(WORK / "text" / "lines.txt", "r", encoding="utf-8") as f:
    for line in f:
        sentences.append(line.strip())

nlp = spacy.load("en_sutta_ner")  # should be 1.2.5
assert nlp.meta.get("version") == "1.2.5", "Wrong en_sutta_ner version installed!"

for s in sentences:
    doc = nlp(s)
    training_jsonl = entity_pos(nlp, s.strip())
    if training_jsonl:
        print(json.dumps(training_jsonl, ensure_ascii=False))
    else: # we may have a negative training example
        print("Negative example? : {}\n".format(s))
