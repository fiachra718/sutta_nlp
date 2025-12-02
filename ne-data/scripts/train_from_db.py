from __future__ import annotations
from typing import Iterable, List, Sequence, Tuple
import psycopg
from psycopg.rows import dict_row
import spacy
from spacy.training import Example
import random
import numpy as np
from local_settings import MODELS_DIR, WORK, load_model
from spacy.util import fix_random_seed, minibatch


def db_to_examples(conn, nlp):
    sql = """
        select text, spans from gold_training
    """
    examples = []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            text = row["text"]
            pred_doc = nlp.make_doc(text)
            gold_doc = nlp.make_doc(text)     # reference Doc
            spans = []
            for s in row["spans"]:
                span = gold_doc.char_span(s["start"], s["end"], label=s["label"], alignment_mode="contract")
                if span: 
                    spans.append(span)
            gold_doc.ents = spans
            example = Example(pred_doc, gold_doc)
            examples.append(example)
    if not examples:
        raise ValueError("Candidate records did not yield any training examples.")
    return examples

def split_examples(examples: Sequence[Example], *, seed: int, dev_ratio: float ) -> Tuple[List[Example], List[Example]]:
    shuffled = list(examples)
    random.Random(seed).shuffle(shuffled)
    if len(shuffled) < 2 or dev_ratio <= 0:
        return shuffled, []
    dev_size = max(1, int(round(len(shuffled) * dev_ratio)))
    if dev_size >= len(shuffled):
        dev_size = len(shuffled) - 1
    dev_examples = shuffled[:dev_size]
    train_examples = shuffled[dev_size:]
    return train_examples, dev_examples

# -----------

spacy.util.fix_random_seed(42)
random.seed(42)
np.random.seed(42)
SEED = 1029
EPOCHS = 10
BATCH_SIZE = 16
DROPOUT = 0.1
DEV_RATIO = 0.1
CONN = psycopg.connect("dbname=tipitaka user=alee")
OUTPUT_DIR = WORK / "models" / "1202"


nlp = load_model()
optimizer = nlp.resume_training()

examples = db_to_examples(CONN, nlp)

# train/test split of examples
train_examples, dev_examples = split_examples(
        examples, seed=SEED, dev_ratio=DEV_RATIO
    )
# get the NER pipe
ner = nlp.get_pipe("ner")

# add span and entity rules to the NER in TRAIN
for example in train_examples:
	for span in example.reference.ents:
		ner.add_label(span.label_)

# add span and entity rules to the NER in DEV		
for example in dev_examples:
	for span in example.reference.ents:
		ner.add_label(span.label_)

other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]

with nlp.disable_pipes(*other_pipes):
	for epoch in range(1, EPOCHS + 1):
		random.shuffle(train_examples)
		losses: dict[str, float] = {}
		for batch in minibatch(train_examples, size=BATCH_SIZE):
			nlp.update(batch, sgd=optimizer, drop=DROPOUT, losses=losses)
		log = f"Epoch {epoch}/{EPOCHS} Losses: {losses}"
		if dev_examples:
			scores = nlp.evaluate(dev_examples)
			ents_f = scores.get("ents_f")
			if ents_f is not None:
				log += f" | Dev ents_f: {ents_f:.3f}"
		print(log)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
nlp.to_disk(OUTPUT_DIR)
print(f"Saved fine-tuned model to {OUTPUT_DIR}")
