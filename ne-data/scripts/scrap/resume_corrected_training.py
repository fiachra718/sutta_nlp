from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import numpy as np
import spacy
from spacy.training import Example
from spacy.util import fix_random_seed, minibatch

from local_settings import MODELS_DIR, PATTERNS, WORK
from ner_pipeline import candidate_record_to_spans


SEED = 1029
EPOCHS = 10
BATCH_SIZE = 16
DROPOUT = 0.1
DEV_RATIO = 0.1

CANDIDATE_PATH = WORK / "candidates" / "corrected_verses.jsonl"
OUTPUT_DIR = WORK / "models" / "1030"
BASE_MODEL_DIR = MODELS_DIR


def load_candidate_records(
    path: Path, *, tokenizer_nlp: spacy.language.Language
) -> List[dict]:
    records: List[dict] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            text = raw.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON on line {line_no} of {path.name}: {exc.msg} (char {exc.pos})."
                ) from None
            converted = candidate_record_to_spans(record, nlp=tokenizer_nlp)
            records.append(converted)
    if not records:
        raise ValueError(f"No candidate records found in {path}")
    return records


def records_to_examples(
    records: Sequence[dict], *, nlp: spacy.language.Language
) -> List[Example]:
    examples: List[Example] = []
    for record in records:
        text = record.get("text")
        if not text:
            continue
        entities = [tuple(ent) for ent in record.get("entities", [])]
        doc = nlp.make_doc(text)
        example = Example.from_dict(doc, {"entities": entities})
        examples.append(example)
    if not examples:
        raise ValueError("Candidate records did not yield any training examples.")
    return examples


def split_examples(
    examples: Sequence[Example], *, seed: int, dev_ratio: float
) -> Tuple[List[Example], List[Example]]:
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


def reload_entity_ruler(nlp: spacy.language.Language) -> None:
    if PATTERNS.exists() and "entity_ruler" in nlp.pipe_names:
        ruler = nlp.get_pipe("entity_ruler")
        ruler.from_disk(str(PATTERNS))
        print(f"Reloaded {len(ruler.patterns)} entity ruler patterns.")


def main() -> None:
    if not CANDIDATE_PATH.exists():
        raise FileNotFoundError(f"Candidate file not found: {CANDIDATE_PATH}")

    print(f"Loading base model from {BASE_MODEL_DIR}")
    nlp = spacy.load(BASE_MODEL_DIR)
    reload_entity_ruler(nlp)

    tokenizer_nlp = spacy.blank("en")
    records = load_candidate_records(CANDIDATE_PATH, tokenizer_nlp=tokenizer_nlp)
    print(f"Loaded {len(records)} candidate records from {CANDIDATE_PATH.name}")

    examples = records_to_examples(records, nlp=nlp)
    train_examples, dev_examples = split_examples(
        examples, seed=SEED, dev_ratio=DEV_RATIO
    )
    print(f"Training examples: {len(train_examples)} | Dev examples: {len(dev_examples)}")

    fix_random_seed(SEED)
    random.seed(SEED)
    np.random.seed(SEED)

    ner = nlp.get_pipe("ner")
    for example in train_examples:
        for span in example.reference.ents:
            ner.add_label(span.label_)
    for example in dev_examples:
        for span in example.reference.ents:
            ner.add_label(span.label_)

    optimizer = nlp.resume_training()

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


if __name__ == "__main__":
    main()
