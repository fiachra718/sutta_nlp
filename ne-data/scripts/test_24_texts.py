import argparse
import spacy
from spacy.training import Example
import json
from typing import Any

from config_utils import resolve_config_path, testing_model_from_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate model on ne-data/work/random_verses.txt examples."
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to shared train/test TOML config. Defaults to ne-data/config/train_from_db.toml.",
    )
    parser.add_argument(
        "--test-file",
        default="./ne-data/work/random_verses.txt",
        help="Path to pipe-delimited gold eval file.",
    )
    parser.add_argument(
        "--show-disagreements",
        action="store_true",
        help="Print per-example gold vs prediction disagreements.",
    )
    parser.add_argument(
        "--max-disagreements",
        type=int,
        default=0,
        help="Maximum disagreement examples to print (0 = all).",
    )
    return parser.parse_args()


def load_examples(file_name, nlp):
    examples = []
    records: list[dict[str, Any]] = []
    total = bad = bad_lines = 0
    with open(file_name, 'r', encoding='utf-8') as f:
        for lineno, line in enumerate(f, start=1):
            if "|" not in line:
                continue
            text, span = line.split("|", 1)
            text = text.strip()
            pred_doc = nlp.make_doc(text)
            gold_doc = nlp.make_doc(text)     # reference Doc
            try:
                pred = json.loads(span.strip())
            except json.JSONDecodeError as exc:
                bad_lines += 1
                print(f"Skipping malformed JSON at {file_name}:{lineno} ({exc})")
                continue
            # print(pred)
            spans = []
            for e in pred:
                # print(e)
                total += 1
                span = gold_doc.char_span(
                    e["start"], e["end"], label=e["label"], alignment_mode="strict"   # important!
                )
                if span is None:
                    bad += 1
                    continue
                else:
                    spans.append(span)
            gold_doc.ents = spans
            example = Example(pred_doc, gold_doc)
            examples.append(example)
            records.append(
                {
                    "lineno": lineno,
                    "text": text,
                    "gold_spans": [
                        {
                            "start": int(ent.start_char),
                            "end": int(ent.end_char),
                            "label": ent.label_,
                            "text": ent.text,
                        }
                        for ent in gold_doc.ents
                    ],
                }
            )

    print(f"There were {bad} bum records of {total}")
    if bad_lines:
        print(f"Skipped {bad_lines} malformed input lines")
    return examples, records


def _span_key(span: dict[str, Any]) -> tuple[int, int, str, str]:
    return (int(span["start"]), int(span["end"]), str(span["label"]), str(span["text"]))


def print_disagreements(
    nlp,
    records: list[dict[str, Any]],
    *,
    max_disagreements: int = 0,
) -> None:
    shown = 0
    for rec in records:
        doc = nlp(rec["text"])
        pred_spans = [
            {
                "start": int(ent.start_char),
                "end": int(ent.end_char),
                "label": ent.label_,
                "text": ent.text,
            }
            for ent in doc.ents
        ]
        gold_keys = {_span_key(s) for s in rec["gold_spans"]}
        pred_keys = {_span_key(s) for s in pred_spans}

        if gold_keys == pred_keys:
            continue

        only_gold = sorted(gold_keys - pred_keys)
        only_pred = sorted(pred_keys - gold_keys)
        shown += 1
        print(f"\n--- disagreement #{shown} (line {rec['lineno']}) ---")
        print(f"text: {rec['text']}")
        print("gold_only:")
        for start, end, label, text in only_gold:
            print(f"  - [{start},{end}] {label}: {text}")
        print("pred_only:")
        for start, end, label, text in only_pred:
            print(f"  - [{start},{end}] {label}: {text}")
        if max_disagreements > 0 and shown >= max_disagreements:
            break
    print(f"\nTotal disagreement examples shown: {shown}")

def main() -> None:
    args = parse_args()
    cfg_path = resolve_config_path(args.config)
    source, model_target, expected_version = testing_model_from_config(cfg_path)

    nlp = spacy.load(model_target)
    loaded_version = nlp.meta.get("version")
    if expected_version is not None and loaded_version != expected_version:
        raise RuntimeError(
            f"Expected {model_target}=={expected_version}, found {loaded_version}"
        )
    print(
        f"Loaded model ({source}): {model_target} "
        f"(version={loaded_version or 'unknown'})"
    )

    examples, records = load_examples(args.test_file, nlp)
    scores = nlp.evaluate(examples)
    print("ents_p:", scores.get("ents_p"))
    print("ents_r:", scores.get("ents_r"))
    print("ents_f:", scores.get("ents_f"))
    print("per-type:", scores.get("ents_per_type"))
    if args.show_disagreements:
        print_disagreements(nlp, records, max_disagreements=args.max_disagreements)


if __name__ == "__main__":
    main()
