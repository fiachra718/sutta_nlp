import argparse
import spacy
from spacy.training import Example
import json

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
    return parser.parse_args()


def load_examples(file_name, nlp):
    examples = []
    total = bad = 0
    with open(file_name, 'r', encoding='utf-8') as f:
        for line in f:
            (text, span) = line.split('|')
            pred_doc = nlp.make_doc(text.strip())
            gold_doc = nlp.make_doc(text)     # reference Doc
            pred = json.loads(span.strip())
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
                    next
                else:
                    spans.append(span)
            gold_doc.ents = spans
            example = Example(pred_doc, gold_doc)
            examples.append(example)

    print(f"There were {bad} bum records of {total}")
    return examples

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

    examples = load_examples(args.test_file, nlp)
    scores = nlp.evaluate(examples)
    print("ents_p:", scores.get("ents_p"))
    print("ents_r:", scores.get("ents_r"))
    print("ents_f:", scores.get("ents_f"))
    print("per-type:", scores.get("ents_per_type"))


if __name__ == "__main__":
    main()
