import spacy
from pathlib import Path
import unicodedata

# need absolute path here.  Hackish, FIXME
NE_DATA = Path("/Users/alee/sutta_nlp/ne-data")

MODELS_DIR = NE_DATA / "ner-model"
PATTERNS = NE_DATA / "patterns/entity_ruler"
LOC_EVENT_PATTERNS = NE_DATA / "patterns" / "span_ruler"



def _load_nlp_model():
    nlp = spacy.load(MODELS_DIR)
    for name in ("entity_ruler", "span_ruler"):
        if name in nlp.pipe_names:
            nlp.remove_pipe(name)

    er = nlp.add_pipe(
            "entity_ruler",
            before="ner",
            config={"overwrite_ents": False}
        )
    er.from_disk(str(PATTERNS))

    sr = nlp.add_pipe(
        "span_ruler",
        after="ner",
        config={"spans_key": "LOC_PHRASES", "overwrite": False}
    )
    sr.from_disk(LOC_EVENT_PATTERNS) 

    return nlp


def run_ner(intext):
    nlp = _load_nlp_model()
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
