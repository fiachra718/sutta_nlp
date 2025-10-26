import spacy

# 1) Load your trained model (the best checkpoint)
nlp = spacy.load("./model_out/model-best")

# 2) If you ALSO want your EntityRuler patterns active, add them here:
#    (only if you have patterns.jsonl and want rules to run before NER)
# from pathlib import Path
# ruler = nlp.add_pipe("entity_ruler", before="ner")
# ruler.from_disk("/Users/alee/sutta_nlp/ne-data/patterns/entity_ruler/patterns.jsonl")

# 3) Your paragraphs (replace with the ones you nabbed earlier)
paragraphs = [
    "Saccaka met the Ven. Sariputta in Vesālī.",
    "Later, Brahmans gathered to hear the discourse."
]

# 4) Process efficiently in batch
for doc in nlp.pipe(paragraphs, disable=[]):  # you can disable pipes if you add more later
    print("TEXT:", doc.text)
    print("ENTS:", [(ent.text, ent.label_) for ent in doc.ents])
    print("-" * 40)