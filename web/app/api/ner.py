import logging
from pathlib import Path

import spacy
import unicodedata

logger = logging.getLogger("sutta_nlp.web.api")


def _load_nlp_model():
    nlp = spacy.load("en_sutta_ner")
    # using en_suttaq_ner 1.1.3
    # pip freeze | grep sutta        
    # en_sutta_ner @ file:///Users/alee/sutta_nlp/dist/en_sutta_ner-1.1.3/dist/en_sutta_ner-1.1.3-py3-none-any.whl#sha256=3fba3db3b4062fd5cf1e66dea79a83b2d18f12a4d0145512d0f3a8933134b517
    # ((venv) ) alee@Mac sutta_nlp % pip show en_sutta_ner          
    # Name: en_sutta_ner
    # Version: 1.1.3
    # Summary: 
    # Home-page: 
    # Author: 
    # Author-email: 
    # License: 
    # Location: /Users/alee/sutta_nlp/venv/lib/python3.12/site-packages
    # Requires: spacy
    # Required-by: 
    assert nlp.meta.get("version") == "1.2.4", "Wrong en_sutta_ner version installed!"
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
