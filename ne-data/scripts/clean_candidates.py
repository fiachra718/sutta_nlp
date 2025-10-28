from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable, Iterator, List

SCRIPT_DIR = Path(__file__).resolve().parent
NE_DATA_DIR = SCRIPT_DIR.parent
WORK = NE_DATA_DIR / "work"

DEFAULT_INPUT = WORK / "candidates" / "sorted_combined.entities.jsonl"
DEFAULT_NEGATIVE = WORK / "no_entities.jsonl"

TRAILING_PUNCT = ",.;:!?)}]\"'›”"
LEADING_PUNCT = "\"'“‘(›"
APOSTROPHE_FIX_RE = re.compile(r"\s+'s\b")


def parse_json_stream(text: str) -> Iterator[dict]:
    decoder = json.JSONDecoder()
    idx = 0
    length = len(text)
    while idx < length:
        while idx < length and text[idx].isspace():
            idx += 1
        if idx >= length:
            break
        obj, end = decoder.raw_decode(text, idx)
        yield obj
        idx = end


def clean_phrase(raw: str) -> str:
    phrase = APOSTROPHE_FIX_RE.sub("'s", raw)
    phrase = phrase.strip()

    while phrase and phrase[0] in LEADING_PUNCT:
        phrase = phrase[1:].lstrip()
    while phrase and phrase[-1] in TRAILING_PUNCT:
        phrase = phrase[:-1].rstrip()
    return phrase.strip()


def normalize_entities(record: dict) -> List[List[str]]:
    text = record.get("text", "")
    entities = record.get("entities") or []
    spans = record.get("spans") or []
    normalized: List[List[str]] = []

    def add_entity(label: str, phrase: str) -> None:
        phrase_norm = clean_phrase(phrase)
        if not phrase_norm:
            return
        entry = [label.upper(), phrase_norm]
        if entry not in normalized:
            normalized.append(entry)

    for entry in entities:
        if isinstance(entry, dict):
            label = entry.get("label")
            phrase = entry.get("text")
            if label and phrase:
                add_entity(label, phrase)
            continue

        if isinstance(entry, (list, tuple)):
            if len(entry) == 2:
                label, phrase = entry
                if isinstance(label, str) and isinstance(phrase, str):
                    add_entity(label, phrase)
            elif len(entry) == 3 and isinstance(entry[0], int):
                # Drop offset-based triples per requested cleanup.
                continue
    if not normalized and spans:
        for span in spans:
            label = span.get("label")
            phrase = span.get("text")
            if label and phrase:
                add_entity(label, phrase)

    # Ensure phrases exist in the document text; if they do not, keep anyway but note case issues.
    filtered: List[List[str]] = []
    lower_text = text.lower()
    for label, phrase in normalized:
        if phrase.lower() in lower_text:
            filtered.append([label, phrase])
        else:
            filtered.append([label, phrase])
    return filtered


def sanitize_records(records: Iterable[dict]) -> tuple[List[dict], List[dict]]:
    cleaned: List[dict] = []
    negatives: List[dict] = []

    for record in records:
        base = {k: v for k, v in record.items() if k not in {"entities", "spans"}}
        normalized = normalize_entities(record)
        if normalized:
            base["entities"] = normalized
            cleaned.append(base)
        else:
            negatives.append(base)
    return cleaned, negatives


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False))
            fh.write("\n")


def run(input_path: Path, output_path: Path, negatives_path: Path) -> None:
    text = input_path.read_text(encoding="utf-8")
    records = list(parse_json_stream(text))
    cleaned, negatives = sanitize_records(records)

    write_jsonl(output_path, cleaned)
    write_jsonl(negatives_path, negatives)

    print(
        f"Processed {len(records)} records → kept {len(cleaned)},"
        f" moved {len(negatives)} to {negatives_path.name}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean candidate JSONL annotations.")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to the messy candidates file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_INPUT,
        help="Where to write the cleaned candidates file (defaults to overwriting input).",
    )
    parser.add_argument(
        "--negatives",
        type=Path,
        default=DEFAULT_NEGATIVE,
        help="Where to write records without entities.",
    )
    args = parser.parse_args()
    run(args.input, args.output, args.negatives)


if __name__ == "__main__":
    main()
