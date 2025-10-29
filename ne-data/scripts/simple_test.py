import spacy
import config_helpers  # registers custom registry hooks for spaCy
from pathlib import Path

from local_settings import MODELS_DIR

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

texts = [
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
    "Then Tapussa the householder went to Ven. Ananda and, on arrival, having bowed down to him, sat to one side. As he was sitting there he said to Ven. Ananda: \"Venerable Ananda, sir,",
    "Now it may be that you are thinking, 'Nakula's mother will not be able to support the children or maintain the household after I'm gone,' but you shouldn't see things in that way.",
    "There is the case, Bharadvaja, where a monk lives in dependence on a certain village or town.",
    "What do you think, monks? Which would in fact be the better? If a strong man were to strike the nether-quarters with a sharp, oil-cleaned sword? Or, to derive enjoyment when rich kshatriyas, brahmans, or householders press the palms together in prayer?",
    "I have heard that on one occasion, while the Blessed One was on a wandering tour among the Kosalans with a large community of monks, he arrived at Salavatika.",
    "On one occasion the Blessed One was staying among the Magadhans at Andhakavinda. Then Ven. Ananda went to him and, having bowed down to him, sat to one side. As he was sitting there the Blessed One said to him, \"Ananda, the new monks — those who have not long gone forth, who are newcomers in this Dhamma & Discipline — should be encouraged, exhorted, and established in these five things. Which five?\"", 
    "Then Mara, the Evil One, knowing with his awareness the train of thought in the Blessed One's awareness, went to him and on arrival said to him: Exercise rulership, Blessed One! Exercise rulership, O One Well-gone! — without killing or causing others to kill, without confiscating or causing others to confiscate, without sorrowing or causing others sorrow — righteously!",
    "I have heard that on one occasion Ven. Maha Kaccana was staying in Avanti at Osprey's Haunt, on Sheer-face Peak. Then Haliddakani the householder went to him and, on arrival, having bowed down to him, sat to one side. As he was sitting there he said to Ven. Maha Kaccana: \"Venerable sir, this was said by the Blessed One in Magandiya's Questions in the Atthaka Vagga\"",
    "Then, having given this exhortation to Ven. Anuruddha, the Blessed One — as a strong man might extend his flexed arm or flex his extended arm — disappeared from the Eastern Bamboo Park of the Cetis and reappeared among the Bhaggas in the Deer Park at Bhesakala Grove, near Crocodile Haunt."
]


for text in texts:
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
