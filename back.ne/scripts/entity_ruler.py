import spacy
from spacy.pipeline import EntityRuler

nlp = spacy.blank("en")  # or spacy.load("en_core_web_sm")

ruler = nlp.add_pipe(
    "entity_ruler",
    config={
        "overwrite_ents": False,         # True = ruler overwrites model ents
        "phrase_matcher_attr": "LOWER",  # case-insensitive phrase matching
    },
    first=True                           # run before NER if a model exists
)

patterns = [
    {"label": "PERSON", "pattern": "Saccaka"},
    {"label": "PERSON", "pattern": [{"LOWER": "ven."}, {"LOWER": "sariputta"}]},
    {"label": "GPE",    "pattern": "Savatthi"},
    {"label": "GPE",    "pattern": "Rajagaha"},
    {"label": "LOC",    "pattern": "Jeta's Grove"},
    {"label": "LOC",    "pattern": "Squirrels' Sanctuary"},
    # regex example
    {"label": "NORP",   "pattern": [{"TEXT": {"REGEX": "^(B|b)rahman(s)?$"}}]},
]

ruler.add_patterns(patterns)

doc = nlp("Ven. Sariputta went from Rajagaha to Jeta's Grove near Savatthi.")
[(ent.text, ent.label_) for ent in doc.ents]

ruler.to_disk("entity_ruler_patterns")     # directory
# Later:
nlp2 = spacy.blank("en")
nlp2.add_pipe("entity_ruler").from_disk("entity_ruler_patterns")