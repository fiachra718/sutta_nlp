import argparse
from pathlib import Path

from spacy.tokens import DocBin
from spacy.training import Example
import spacy

from local_settings import WORK
from config_utils import resolve_config_path, testing_model_from_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score NER model on DocBin sources.")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to shared train/test TOML config. Defaults to ne-data/config/train_from_db.toml.",
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        default=None,
        help=(
            "Optional DocBin source paths relative to repo root. "
            "Default: ne-data/work/gold_training.spacy"
        ),
    )
    return parser.parse_args()


def docbin_to_examples(path, nlp):
    if not path.exists():
        print(f"Skipping missing DocBin: {path}")
        return []
    docbin = DocBin().from_disk(path)
    examples = []
    for doc in docbin.get_docs(nlp.vocab):
        pred_doc = nlp.make_doc(doc.text)
        examples.append(Example(pred_doc, doc))
    return examples


def main():
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

    if args.sources:
        sources = [resolve_config_path(raw) for raw in args.sources]
    else:
        sources = [WORK / "gold_training.spacy"]

    for path in sources:
        if "predicted" in path.name.lower():
            print(
                f"Warning: {path} appears to contain model predictions, "
                "not gold labels. Scores may be misleading."
            )

    examples = []
    for path in sources:
        examples.extend(docbin_to_examples(path, nlp))

    if not examples:
        raise SystemExit("No DocBin examples loaded; nothing to score.")

    scores = nlp.evaluate(examples)
    print("\n")
    print("ents_p:", scores.get("ents_p"))
    print("ents_r:", scores.get("ents_r"))
    print("ents_f:", scores.get("ents_f"))
    print("per-type:", scores.get("ents_per_type"))


if __name__ == "__main__":
    main()
