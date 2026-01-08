import spacy
from local_settings import load_model

nlp = load_model()
print("LOAD_MODEL:", nlp.meta.get("name"), nlp.meta.get("version"), nlp.pipe_names)

# vs

nlp = spacy.load("en_sutta_ner")
print("PACKAGE:", nlp.meta.get("name"), nlp.meta.get("version"), nlp.pipe_names)