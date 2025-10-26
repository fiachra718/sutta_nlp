import spacy
nlp = spacy.load("./model_out/model-best")

# overwrite_ents=True makes rules win over the model; False only fills gaps
ruler = nlp.add_pipe("entity_ruler", before="ner", config={"overwrite_ents": True})
ruler.from_disk("/Users/alee/sutta_nlp/ne-data/patterns/entity_ruler/patterns.jsonl")

texts = [
    "Saccaka met the Ven. Sariputta in Vesālī.",
    "Later, Brahmans gathered to hear the discourse."
]
for doc in nlp.pipe(texts):
    print([(ent.text, ent.label_) for ent in doc.ents])