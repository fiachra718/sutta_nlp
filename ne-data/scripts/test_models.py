import spacy
from pathlib import Path
import os

from local_settings import MODELS_DIR, PATTERNS
print(PATTERNS)
print(MODELS_DIR)


sentences = [
    "At that time the Venerable Upavana was standing before the Blessed One, fanning him. And the Blessed One rebuked him, saying: Move aside, bhikkhu, do not stand in front of me.",
    "Then Brahma Sahampati, thinking, The Blessed One has given his consent to teach the Dhamma, bowed down to the Blessed One and, circling him on the right, disappeared right there.",
     "Then King Pasenadi Kosala, delighting in and approving of the Blessed One's words, got up from his seat, bowed down to the Blessed One and — keeping him to his right — departed.",
""" I have heard that on one occasion the Blessed One was living among the Sumbhas . Now there is a Sumbhan town named Sedaka . There the Blessed One addressed the monks, "Monks!" """,
""" I have heard that on one occasion the Blessed One was living among the Sumbhas . Now there is a Sumbhan town named Sedaka . There the Blessed One addressed the monks, "Monks!" """,
""" So King Pasenadi Kosala, delighting in and approving of the Blessed One's words, got up from his seat, bowed down to the Blessed One and — keeping him to his right — departed. """,
""" I have heard that on one occasion the Blessed One was staying among the Angas . Now, the Angas have a town named Assapura . There the Blessed One addressed the monks, "Monks!" """,
""" That is what the Blessed One said. Gratified, the monks delighted in the Blessed One's words. And while this explanation was being given, the ten-thousand fold cosmos quaked. """,
""" "Yes, indeed, friends. I understand the Dhamma taught by the Blessed One, and those acts the Blessed One says are obstructive, when indulged in are not genuine obstructions." """,
""" I have heard that on one occasion the Blessed One was staying at Ukkattha , in the shade of a royal Sal tree in the Very Blessed Grove. There he addressed the monks, "Monks!" """,
"""Thus it was heard by me. At one time the Blessed One was living in the deer park of Isipatana near Benares. There, indeed, the Blessed One addressed the group of five monks.""",
]

nlp = spacy.load(MODELS_DIR)
nlp.remove_pipe("entity_ruler")
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.from_disk(PATTERNS)

for s in sentences:
    doc = nlp(s)
    print(s)
    print([(ent.text, ent.label_) for ent in doc.ents])

nlp = spacy.load(MODELS_DIR)
nlp.remove_pipe("entity_ruler")
ruler = nlp.add_pipe("entity_ruler", after="ner")
ruler.from_disk(PATTERNS)

print("------------------\n\n")
for s in sentences:
    doc = nlp(s)
    print(s)
    print([(ent.text, ent.label_) for ent in doc.ents])

