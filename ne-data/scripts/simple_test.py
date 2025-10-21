import spacy
import config_helpers  # registers custom registry hooks for spaCy
from pathlib import Path

from local_settings import MODELS_DIR, PATTERNS

ENTITY_PATTERNS = Path("ne-data/patterns/entity_ruler/patterns.jsonl")
SPAN_PATTERNS = Path("ne-data/patterns/span_ruler/loc_phrases.json")

def load_my_ner():
    ''' becasue SpaCy is finicky, I am going to leave this here '''
    nlp = spacy.load(MODELS_DIR)
    # clear the pipe
    for name in ("entity_ruler", "span_ruler"):
        if name in nlp.pipe_names:
            nlp.remove_pipe(name)

    # add the entity rules
    er = nlp.add_pipe(
        "entity_ruler",
        after="ner",
        config={"overwrite_ents": True}
    )
    er.from_disk(str(ENTITY_PATTERNS))            # load patterns so no [W036] warning

    # add the LOC/span patterns
    sr = nlp.add_pipe(
        "span_ruler",
        last=True,
        config={"spans_key": "LOC_PHRASES", "overwrite": True}
    )
    sr.from_disk("ne-data/patterns/span_ruler")  # folder; contains a file named 'patterns'
    print("Pipeline:", nlp.pipe_names)
    return nlp

nlp = load_my_ner()

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
    "He walked near Squirrels' Refuge in Rajagaha.",
    "The monks gathered in the Deer Park at Isipatana.",
    "Anāthapiṇḍika's Monastery was quiet at dawn.",
    "The brahman householders of Savatthi offered alms.",
    "Ven. Kesi was teaching at Jeta’s Grove near Sāvatthī.",
    "Once, friend, when I was staying in Saketa at the Game Refuge in the Black Forest, the nun Jatila Bhagika went to where I was staying, and on arrival — having bowed to me — stood to one side.",
    "As she was standing there, she said to me: 'The concentration whereby — neither pressed down nor forced back, nor with fabrication kept blocked or suppressed — still as a result of release, contented as a result of standing still, and as a result of contentment one is not agitated: This concentration is said by the Blessed One to be the fruit of what?'",
    "Then Tapussa the householder went to Ven. Ananda and, on arrival, having bowed down to him, sat to one side. As he was sitting there he said to Ven. Ananda: \"Venerable Ananda, sir,"
]   

for text in tests:
    doc = nlp(text)
    print(text)
    print([(ent.text, ent.label_) for ent in doc.ents])
    print("-" * 40)

'''
They met at Simsapā Grove.
[('Simsapā Grove', 'LOC')]
----------------------------------------
He meditated in Bamboo Park near Varanasi's Deer Park.
[('Bamboo Park', 'LOC'), ("Varanasi's Deer Park", 'LOC')]
----------------------------------------
We walked to Jeta’s Grove and later to Anathapindika's Park.
[('Jeta’s Grove', 'LOC'), ("Anathapindika's Park", 'LOC')]
----------------------------------------
The sermon was given at Blind Man's Grove near Bhesakala Grove.
[("Blind Man's Grove", 'LOC'), ('Bhesakala Grove', 'LOC')]
----------------------------------------
Pilgrims visited Isipatana Deer Park.
[('Isipatana Deer Park', 'LOC')]
----------------------------------------
It is true, Kesi, that it's not proper for a Tathagata to take life.
[('Kesi', 'PERSON'), ('Tathagata', 'PERSON')]
----------------------------------------
The brahman householders of Icchanangala heard it said, 'Gotama the contemplative — the son of the Sakyans, having gone forth from the Sakyan clan — on a wandering tour among the Kosalans with a large community of monks — has arrived at Icchanangala and is staying in the Icchanangala forest grove.'
[('brahman householders', 'NORP'), ('Icchanangala', 'GPE'), ('Gotama', 'PERSON'), ('Sakyans', 'NORP'), ('Icchanangala', 'GPE'), ('Icchanangala', 'GPE')]
----------------------------------------
I have heard that on one occasion the Blessed One was staying near Rājagaha at the Bamboo Grove, the Squirrels' Sanctuary.
[('Blessed One', 'PERSON'), ('Rājagaha', 'GPE'), ('at', 'GPE'), ('Bamboo Grove', 'LOC')]
----------------------------------------
Then Ven. Upasena said, "Quick, friends, lift this body of mine onto a couch and carry it outside before it is scattered like a fistful of chaff!"
[('Ven. Upasena', 'PERSON')]
----------------------------------------
He walked near Squirrels' Refuge in Rajagaha.
[('Rajagaha', 'GPE')]
----------------------------------------
The monks gathered in the Deer Park at Isipatana.
[('Deer Park at Isipatana', 'LOC')]
----------------------------------------
Anāthapiṇḍika's Monastery was quiet at dawn.
[("Anāthapiṇḍika's Monastery", 'LOC')]
----------------------------------------
The brahman householders of Savatthi offered alms.
[('brahman householders', 'NORP'), ('Savatthi', 'GPE')]
----------------------------------------
Ven. Kesi was teaching at Jeta’s Grove near Sāvatthī.
[('Kesi', 'PERSON'), ('Jeta’s Grove', 'LOC'), ('near', 'GPE'), ('Sāvatthī', 'GPE')]
----------------------------------------
Once, friend, when I was staying in Saketa at the Game Refuge in the Black Forest, the nun Jatila Bhagika went to where I was staying, and on arrival — having bowed to me — stood to one side.
[('Saketa', 'GPE'), ('Game Refuge', 'LOC'), ('Black Forest', 'LOC'), ('Jatila Bhagika', 'PERSON')]
----------------------------------------
As she was standing there, she said to me: 'The concentration whereby — neither pressed down nor forced back, nor with fabrication kept blocked or suppressed — still as a result of release, contented as a result of standing still, and as a result of contentment one is not agitated: This concentration is said by the Blessed One to be the fruit of what?'
[('Blessed One', 'PERSON')]
'''
