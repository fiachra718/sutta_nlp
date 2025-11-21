import logging
from pathlib import Path

import spacy
import unicodedata

# need absolute path here.  Hackish, FIXME
NE_DATA = Path("/Users/alee/sutta_nlp/ne-data")

MODELS_DIR = NE_DATA / "work" / "models" / "1107" 
PATTERNS = NE_DATA / "patterns/entity_ruler"
LOC_EVENT_PATTERNS = NE_DATA / "patterns" / "span_ruler"
NORP_PATTERNS = PATTERNS / "ruler_norp.jsonl" 

logger = logging.getLogger("sutta_nlp.web.api")



def _load_nlp_model():
    # copypasta from ner-data/scripts/local_setting.py
    nlp = spacy.load(MODELS_DIR)
    # clear the pipe
    for name in ("norp_head_ruler", "entity_ruler", "span_ruler"):
        if name in nlp.pipe_names:
            nlp.remove_pipe(name)

    er = nlp.add_pipe("entity_ruler", name="entity_ruler", config={"overwrite_ents": False})
    er.from_disk(str(PATTERNS))

    # Add NORP head rules AFTER ner as well, so they don’t run ahead of model
    ruler = nlp.add_pipe("entity_ruler", name="norp_head_ruler", after="entity_ruler", config={"overwrite_ents": False})
    ruler.from_disk(NORP_PATTERNS)

    # Add span_ruler (LOC/EVENT phrases) AFTER ner too
    sr = nlp.add_pipe("span_ruler", name="span_ruler", after="norp_head_ruler",
                      config={"spans_key": "LOC_PHRASES", "overwrite": False})
    sr.from_disk(LOC_EVENT_PATTERNS)

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
    logger.debug("NER spans=%d text_length=%d", len(spans), len(text))
    return {"text": text, "spans": spans}
