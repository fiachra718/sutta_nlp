import spacy

from local_settings import MODELS_DIR, PATTERNS

nlp = spacy.load(str(MODELS_DIR))  # load the saved pipeline
# dumb thing about SpaCY version 3.8.7, you have to explicitly delete from pipe
nlp.remove_pipe("entity_ruler") # get rid of the existing ER
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.from_disk(PATTERNS)

tests = [
    "They met at Simsapā Grove.",
    "He meditated in Bamboo Park near Varanasi's Deer Park.",
    "We walked to Jeta’s Grove and later to Anathapindika's Park.",
    "The sermon was given at Blind Man's Grove near Bhesakala Grove.",
    "Pilgrims visited Isipatana Deer Park.",
    "It is true, Kesi, that it's not proper for a Tathagata to take life.",
    "The brahman householders of Icchanangala heard it said, 'Gotama the contemplative — the son of the Sakyans, having gone forth from the Sakyan clan — on a wandering tour among the Kosalans with a large community of monks — has arrived at Icchanangala and is staying in the Icchanangala forest grove.'",
    "I have heard that on one occasion the Blessed One was staying near Rājagaha at the Bamboo Grove, the Squirrels' Sanctuary.",
    "Then Ven. Upasena said, \"Quick, friends, lift this body of mine onto a couch and carry it outside before it is scattered like a fistful of chaff!\"",
]   

for text in tests:
    doc = nlp(text)
    print(text)
    print([(ent.text, ent.label_) for ent in doc.ents])
    print("-" * 40)
