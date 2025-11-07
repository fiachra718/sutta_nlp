import spacy
from pathlib import Path
import unicodedata

# need absolute path here.  Hackish, FIXME
NE_DATA = Path("/Users/alee/sutta_nlp/ne-data")

MODELS_DIR = NE_DATA / "work" / "models" / "1106" 
PATTERNS = NE_DATA / "patterns/entity_ruler"
LOC_EVENT_PATTERNS = NE_DATA / "patterns" / "span_ruler"
NORP_PATTERNS = PATTERNS / "ruler_norp.jsonl" 



def _load_nlp_model():
    # copypasta from ner-data/scripts/local_setting.py
    nlp = spacy.load(MODELS_DIR)
    # clear the pipe
    for name in ("norp_head_ruler", "entity_ruler", "span_ruler"):
        if name in nlp.pipe_names:
            nlp.remove_pipe(name)

    # add the NORP patterns from disk
    ruler = nlp.add_pipe(
        "entity_ruler", 
        name="norp_head_ruler", 
        first=True, 
        config={"overwrite_ents": False}
    )
    ruler.from_disk(NORP_PATTERNS) 

    # add the entity rules
    er = nlp.add_pipe(
        "entity_ruler",
        after="ner",
        config={"overwrite_ents": False}
    )
    er.from_disk(str(PATTERNS))  

    # add the LOC/span patterns
    sr = nlp.add_pipe(
        "span_ruler",
        after="ner",
        config={"spans_key": "LOC_PHRASES", "overwrite": False}
    )
    sr.from_disk(LOC_EVENT_PATTERNS)  # folder; contains a file named 'patterns'
    
    return nlp


def run_ner(intext):
    nlp = _load_nlp_model()  # want the LATEST model
    text = unicodedata.normalize("NFC", intext.strip())
    doc = nlp(text)
    spans = [
        {
            "start": ent.start_char,
            "end": ent.end_char,
            "label": ent.label_,
            "text": ent.text,
        }
        for ent in doc.ents
    ]
    print({"text": text, "spans": spans})
    return {"text": text, "spans": spans}
