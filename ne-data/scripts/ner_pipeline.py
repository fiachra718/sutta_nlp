from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence

import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import DocBin
from spacy.training import Example
from spacy.util import fix_random_seed, minibatch

from local_settings import PATTERNS, WORK


DEFAULT_SEED = 42
TRAIN_RATIO = 0.8
DEV_RATIO = 0.1  # remaining portion becomes test

CANDIDATES_DIR = WORK / "candidates"
SPANS_DIR = WORK / "spans"
TRAIN_JSONL = WORK / "train.jsonl"
DEV_JSONL = WORK / "dev.jsonl"
TEST_JSONL = WORK / "test.jsonl"
TRAIN_DOCBIN = WORK / "train.spacy"
DEV_DOCBIN = WORK / "dev.spacy"
TEST_DOCBIN = WORK / "test.spacy"
MODEL_OUTPUT_DIR = WORK / "models" / "ner_pipeline"


@dataclass
class SplitArtifacts:
    name: str
    records: List[dict]
    jsonl_path: Path
    docbin_path: Path


def jsonl_reader(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            yield json.loads(line)


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def record_entities(record: dict) -> List[tuple[int, int, str]]:
    entities = record.get("entities")
    if entities:
        return [tuple(ent) for ent in entities]

    spans = record.get("spans", [])
    return [(span["start"], span["end"], span["label"]) for span in spans]


def validate_span_record(record: dict) -> List[str]:
    '''
    When there is an entity span record it looks like:
    {"text" : "Thus have I heard ..", "spans" : [
        {"start": 7, "end": 18, "label": "PERSON", "text": "Blessed One"} ... 
    ] }
    * OR *
    {"text" : "Thus have I heard ..",
    "spans": [[10, 19, "PERSON"], [79, 90, "PERSON"], [95, 102, "NORP"]]
    }
    Want to validate that the JSON is well formed and that 
    the start and end offsets are correct.  No NLP here.
    '''
    errors: List[str] = []
    text = record.get("text")
    spans = record.get("spans", [])

    if text is None:
        return ["Missing 'text' field."]

    if not isinstance(spans, list):
        return ["'spans' must be a list."]

    for idx, span in enumerate(spans):
        missing_keys = {"start", "end", "label"} - set(span)
        if missing_keys:
            errors.append(f"Span {idx} missing keys: {sorted(missing_keys)}")
            continue

        start, end = span["start"], span["end"]
        if not isinstance(start, int) or not isinstance(end, int):
            errors.append(f"Span {idx} has non-integer offsets: {start!r}-{end!r}")
            continue

        if start < 0 or end > len(text) or start >= end:
            errors.append(f"Span {idx} has invalid offsets: start={start}, end={end}")
            continue

        extracted = text[start:end]
        expected = span.get("text")
        if expected is not None and extracted != expected:
            errors.append(
                f"Span {idx} text mismatch: expected '{expected}', found '{extracted}'."
            )

    return errors


def candidate_record_to_spans(
    record: dict, *, nlp: spacy.language.Language, allow_possessive: bool = True
) -> dict:
    text = record.get("text")
    if text is None:
        raise ValueError("Candidate record missing 'text'")

    phrases = record.get("spans")
    if phrases is None:
        phrases = record.get("entities", [])

    if not isinstance(phrases, list):
        raise ValueError("Candidate 'spans'/'entities' must be a list.")

    # Some legacy exports wrap an empty list inside another list.
    if len(phrases) == 1 and phrases[0] in ([], None):
        phrases = []

    doc = nlp.make_doc(text)
    matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
    key_to_label: dict[str, str] = {}
    for item in phrases:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError(f"Bad candidate span entry: {item!r}")
        label, phrase = item
        if not phrase:
            continue
        key = f"{label}__{phrase.lower()}"
        matcher.add(key, [nlp.make_doc(phrase)])
        key_to_label[key] = label

    matches = matcher(doc)
    raw_spans: List[tuple[int, int, str]] = []
    seen_keys: set[str] = set()
    for match_id, start, end in matches:
        key = nlp.vocab.strings[match_id]
        seen_keys.add(key)
        label = key_to_label[key]
        span = doc[start:end]
        end_char = span.end_char
        if allow_possessive and end < len(doc):
            nxt = doc[end].text
            if nxt in ("'s", "\u2019s"):
                end_char = doc[end].idx + len(nxt)
        raw_spans.append((span.start_char, end_char, label))

    # Deduplicate overlaps by keeping longer spans first.
    raw_spans.sort(key=lambda item: (item[0], -(item[1] - item[0])))
    merged: List[tuple[int, int, str]] = []
    last_end = -1
    for start_char, end_char, label in raw_spans:
        if start_char >= last_end:
            merged.append((start_char, end_char, label))
            last_end = end_char

    span_dicts: List[dict] = [
        {
            "start": start,
            "end": end,
            "label": label,
            "text": text[start:end],
        }
        for start, end, label in merged
    ]

    missing = set(key_to_label) - seen_keys
    if missing:
        missing_details = ", ".join(
            f"{key_to_label[key]}:{key.split('__', 1)[1]}" for key in sorted(missing)
        )
        print(f"WARNING Missing matches in '{record.get('id', '<unknown>')}': {missing_details}")

    return {
        "text": text,
        "spans": span_dicts,
        "entities": [[span["start"], span["end"], span["label"]] for span in span_dicts],
        "meta": record.get("meta", {}),
        "source": record.get("source"),
    }


def convert_candidates(
    candidate_paths: Sequence[Path],
    *,
    spans_dir: Path,
    nlp: spacy.language.Language,
) -> List[Path]:
    spans_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    for path in candidate_paths:
        output_path = spans_dir / path.name
        converted_records: List[dict] = []
        for record in jsonl_reader(path):
            converted = candidate_record_to_spans(record, nlp=nlp)
            errors = validate_span_record(converted)
            if errors:
                raise ValueError(f"Validation errors in {path.name}: {errors}")
            converted_records.append(converted)
        write_jsonl(output_path, converted_records)
        written.append(output_path)
        print(f"Converted {path.name} -> {output_path.relative_to(spans_dir.parent)}")
    return written


def dedupe_records(records: Iterable[dict]) -> List[dict]:
    seen: set[tuple] = set()
    deduped: List[dict] = []
    for record in records:
        entities = tuple(record_entities(record))
        signature = (record.get("text"), entities)
        if signature not in seen:
            seen.add(signature)
            deduped.append(record)
    return deduped


def split_records(
    records: List[dict],
    *,
    seed: int,
    train_ratio: float,
    dev_ratio: float,
) -> dict[str, List[dict]]:
    if not records:
        raise ValueError("No records available for splitting.")

    random.Random(seed).shuffle(records)
    total = len(records)
    n_train = max(1, int(round(total * train_ratio)))
    remaining = max(0, total - n_train)

    n_dev = int(round(total * dev_ratio))
    if n_dev > remaining:
        n_dev = remaining
    if remaining > 0 and n_dev == 0:
        n_dev = 1
    remaining = max(0, total - n_train - n_dev)

    n_test = remaining
    if n_test < 0:
        n_test = 0

    splits = {
        "train": records[:n_train],
        "dev": records[n_train : n_train + n_dev],
        "test": records[n_train + n_dev :],
    }
    print(
        f"Split {total} records -> train:{len(splits['train'])} dev:{len(splits['dev'])} test:{len(splits['test'])}"
    )
    return splits


def records_to_examples(
    records: Sequence[dict], *, nlp: spacy.language.Language
) -> List[Example]:
    examples: List[Example] = []
    for record in records:
        doc = nlp.make_doc(record["text"])
        example = Example.from_dict(doc, {"entities": record_entities(record)})
        examples.append(example)
    return examples


def records_to_docbin(
    records: Sequence[dict], *, nlp: spacy.language.Language
) -> DocBin:
    docbin = DocBin(store_user_data=True)
    for example in records_to_examples(records, nlp=nlp):
        docbin.add(example.reference)
    return docbin


def jsonl_from_spans(paths: Sequence[Path]) -> List[dict]:
    records: List[dict] = []
    for path in paths:
        records.extend(jsonl_reader(path))
    print(f"Loaded {len(records)} records from {len(paths)} span files.")
    return records


def build_splits(
    span_paths: Sequence[Path],
    *,
    tokenizer_nlp: spacy.language.Language,
    seed: int,
) -> List[SplitArtifacts]:
    records = dedupe_records(jsonl_from_spans(span_paths))
    splits = split_records(records, seed=seed, train_ratio=TRAIN_RATIO, dev_ratio=DEV_RATIO)

    artifacts: List[SplitArtifacts] = []
    mapping = {
        "train": (TRAIN_JSONL, TRAIN_DOCBIN),
        "dev": (DEV_JSONL, DEV_DOCBIN),
        "test": (TEST_JSONL, TEST_DOCBIN),
    }
    for name, split_records_list in splits.items():
        jsonl_path, docbin_path = mapping[name]
        write_jsonl(jsonl_path, split_records_list)
        records_to_docbin(split_records_list, nlp=tokenizer_nlp).to_disk(docbin_path)
        artifacts.append(
            SplitArtifacts(
                name=name,
                records=split_records_list,
                jsonl_path=jsonl_path,
                docbin_path=docbin_path,
            )
        )
        print(f"Wrote {name} split -> {jsonl_path.name}, {docbin_path.name}")

    return artifacts


def train_ner_model(
    train_records: Sequence[dict],
    dev_records: Sequence[dict],
    *,
    output_dir: Path,
    seed: int,
    epochs: int,
    dropout: float,
    batch_size: int,
    include_patterns: bool,
) -> spacy.language.Language:
    fix_random_seed(seed)
    random.seed(seed)

    base_nlp = spacy.blank("en")
    if include_patterns and PATTERNS.exists():
        ruler = base_nlp.add_pipe("entity_ruler", first=True)
        ruler.from_disk(str(PATTERNS))
        print(f"Loaded {len(ruler.patterns)} entity ruler patterns.")

    ner = base_nlp.add_pipe("ner", last=True)
    train_examples = records_to_examples(train_records, nlp=base_nlp)
    dev_examples = records_to_examples(dev_records, nlp=base_nlp)

    for example in train_examples:
        for span in example.reference.ents:
            ner.add_label(span.label_)

    optimizer = base_nlp.initialize(lambda: train_examples)
    if include_patterns and "entity_ruler" in base_nlp.pipe_names:
        base_nlp.get_pipe("entity_ruler").from_disk(str(PATTERNS))

    for epoch in range(epochs):
        random.shuffle(train_examples)
        batches = minibatch(train_examples, size=batch_size)
        losses: dict[str, float] = {}
        for batch in batches:
            base_nlp.update(batch, sgd=optimizer, drop=dropout, losses=losses)
        print(f"Epoch {epoch+1}/{epochs} Losses: {losses}")

        if dev_examples:
            scores = base_nlp.evaluate(dev_examples)
            ents_f = scores["ents_f"]
            print(f"   dev ents_f: {ents_f:.3f}")

    output_dir.mkdir(parents=True, exist_ok=True)
    base_nlp.to_disk(output_dir)
    print(f"Saved trained model to {output_dir}")

    return base_nlp


def evaluate_model(
    nlp: spacy.language.Language,
    records: Sequence[dict],
) -> dict:
    if not records:
        print("No records supplied for evaluation; skipping.")
        return {}
    examples = records_to_examples(records, nlp=nlp)
    scores = nlp.evaluate(examples)
    ents_p = scores["ents_p"]
    ents_r = scores["ents_r"]
    ents_f = scores["ents_f"]
    print(f"Evaluation P: {ents_p:.3f} R: {ents_r:.3f} F: {ents_f:.3f}")
    return scores


def collect_candidate_paths(names: Sequence[str] | None = None) -> List[Path]:
    if not CANDIDATES_DIR.exists():
        raise FileNotFoundError(f"Candidate directory not found: {CANDIDATES_DIR}")

    if names:
        paths = [CANDIDATES_DIR / name for name in names]
    else:
        paths = sorted(CANDIDATES_DIR.glob("*.jsonl"))

    missing = [path for path in paths if not path.exists()]
    if missing:
        missing_str = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"Candidate files not found: {missing_str}")

    if not paths:
        raise ValueError(f"No candidate JSONL files found in {CANDIDATES_DIR}")

    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="End-to-end spaCy NER training pipeline.")
    parser.add_argument(
        "--candidates",
        nargs="*",
        help="Subset of candidate JSONL filenames (defaults to all in work/candidates).",
    )
    parser.add_argument("--epochs", type=int, default=10, help="Number of training epochs.")
    parser.add_argument("--dropout", type=float, default=0.1, help="Training dropout rate.")
    parser.add_argument(
        "--batch-size", type=int, default=32, help="Training batch size for minibatching."
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=MODEL_OUTPUT_DIR,
        help="Directory to write the trained model.",
    )
    parser.add_argument(
        "--no-patterns",
        action="store_true",
        help="Disable loading entity ruler patterns during training.",
    )
    parser.add_argument(
        "--skip-convert",
        action="store_true",
        help="Skip converting candidates if span files already exist.",
    )
    parser.add_argument(
        "--skip-train",
        action="store_true",
        help="Skip model training (still performs conversion and split).",
    )
    return parser.parse_args()


def run_pipeline() -> None:
    args = parse_args()

    tokenizer_nlp = spacy.blank("en")
    candidate_paths = collect_candidate_paths(args.candidates)

    if args.skip_convert:
        span_paths = sorted(SPANS_DIR.glob("*.jsonl"))
        if not span_paths:
            raise ValueError("No span files found; cannot skip conversion.")
        print(f"Skipping conversion. Using {len(span_paths)} existing span files.")
    else:
        span_paths = convert_candidates(candidate_paths, spans_dir=SPANS_DIR, nlp=tokenizer_nlp)

    artifacts = build_splits(span_paths, tokenizer_nlp=tokenizer_nlp, seed=args.seed)
    if args.skip_train:
        return

    split_map = {artifact.name: artifact.records for artifact in artifacts}
    trained_nlp = train_ner_model(
        split_map["train"],
        split_map["dev"],
        output_dir=args.output_dir,
        seed=args.seed,
        epochs=args.epochs,
        dropout=args.dropout,
        batch_size=args.batch_size,
        include_patterns=not args.no_patterns,
    )

    evaluate_model(trained_nlp, split_map.get("dev", []))
    evaluate_model(trained_nlp, split_map.get("test", []))


if __name__ == "__main__":
    run_pipeline()
