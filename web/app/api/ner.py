import logging
from pathlib import Path

import spacy
import unicodedata

logger = logging.getLogger("sutta_nlp.web.api")


def _load_nlp_model():
    nlp = spacy.load("en_sutta_ner")
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
