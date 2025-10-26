import spacy
nlp = spacy.load("./model_out_more/model-best")

# (Optional) load your rules at inference so they can override/assist
ruler = nlp.add_pipe("entity_ruler", before="ner", config={"overwrite_ents": True})
ruler.from_disk("/Users/alee/sutta_nlp/ne-data/patterns/entity_ruler/patterns.jsonl")

texts = [
    "Ananda was staying near Vesālī at Veluvagamaka.",
    "Dasama the householder came from Atthakanagara to Pāṭaliputta.",
    "He stayed at Kukkata Monastery.",
]
for doc in nlp.pipe(texts):
    print([(e.text, e.label_) for e in doc.ents])

