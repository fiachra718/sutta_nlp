import spacy
from spacy.pipeline import SpanRuler

nlp = spacy.load("en_core_web_sm")  # or your pipeline

# Patterns for doctrinal/practice terms (grow this list)
doctrine_patterns = [
    {"label": "DOCTRINE", "pattern": "Patimokkha"},
    {"label": "DOCTRINE", "pattern": "Deathless"},
    {"label": "DOCTRINE", "pattern": "Nibbana"},
    {"label": "DOCTRINE", "pattern": "Wheel of Truth"},
    {"label": "DOCTRINE", "pattern": "Unbinding"},
    

    {"label": "DOCTRINE", "pattern": {"REGEX": r"Vinaya|Sutta|Abhidhamma|Dhamma"}},
]
practice_patterns = [
    {"label": "PRACTICE", "pattern": "Jhāna"},
    {"label": "PRACTICE", "pattern": {"REGEX": r"Sam(a|ā)dhi|Satipaṭṭhāna"}},
]

# SpanRuler writes to doc.spans[...] instead of doc.ents
span_ruler_doctrine = nlp.add_pipe(
    "span_ruler", config={"spans_key": "doctrine", "phrase_matcher_attr": "ORTH"}, last=True
)
span_ruler_doctrine.add_patterns(doctrine_patterns)

span_ruler_practice = nlp.add_pipe(
    "span_ruler", config={"spans_key": "practice", "phrase_matcher_attr": "ORTH"}, last=True
)
span_ruler_practice.add_patterns(practice_patterns)

doc = nlp("let the Lord recite the Patimokkha to the bhikkhus.")
print([(e.text, e.label_) for e in doc.ents])                # -> [('the Lord','PERSON')]  (unchanged)
print([(s.text, s.label_) for s in doc.spans["doctrine"]])   # -> [('Patimokkha','DOCTRINE')]
