import argparse

import psycopg
from psycopg.rows import dict_row
import spacy
from spacy.training import Example

from config_utils import resolve_config_path, testing_model_from_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate NER against DB gold rows.")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to shared train/test TOML config. Defaults to ne-data/config/train_from_db.toml.",
    )
    return parser.parse_args()

def db_to_examples(conn, nlp):
    sql = """
        select text, spans from gold_training where id like 'manual%' 
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

    conn = psycopg.connect("dbname=tipitaka user=alee")
    test_examples = db_to_examples(conn, nlp)

    scores = nlp.evaluate(test_examples)
    print("ents_p:", scores.get("ents_p"))
    print("ents_r:", scores.get("ents_r"))
    print("ents_f:", scores.get("ents_f"))
    print("per-type:", scores.get("ents_per_type"))


if __name__ == "__main__":
    main()
