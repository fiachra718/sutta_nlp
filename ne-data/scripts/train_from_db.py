from __future__ import annotations
import argparse
import random
import sys
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Sequence, Tuple

import psycopg
from psycopg.rows import dict_row
import spacy
from spacy.training import Example
from spacy.util import fix_random_seed, minibatch

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class TrainConfig:
    db_dsn: str
    created_after: str | None
    model_name: str
    expected_version: str | None
    output_dir: Path
    seed: int
    epochs: int
    batch_size: int
    dropout: float
    dev_ratio: float
    confirm_before_run: bool


def _resolve_path(raw: str) -> Path:
    p = Path(raw).expanduser()
    if p.is_absolute():
        return p
    return (REPO_ROOT / p).resolve()


def load_config(path: Path) -> TrainConfig:
    with path.open("rb") as f:
        cfg = tomllib.load(f)

    db_dsn = cfg["db"]["dsn"]
    created_after = cfg.get("training", {}).get("created_after")
    if created_after is not None:
        date.fromisoformat(created_after)

    model_name = cfg["model"]["name"]
    expected_version = cfg.get("model", {}).get("expected_version")
    output_dir = _resolve_path(cfg["output"]["dir"])

    train_cfg = cfg.get("training", {})
    seed = int(train_cfg.get("seed", 108))
    epochs = int(train_cfg.get("epochs", 20))
    batch_size = int(train_cfg.get("batch_size", 16))
    dropout = float(train_cfg.get("dropout", 0.1))
    dev_ratio = float(train_cfg.get("dev_ratio", 0.1))
    confirm_before_run = bool(train_cfg.get("confirm_before_run", True))

    return TrainConfig(
        db_dsn=db_dsn,
        created_after=created_after,
        model_name=model_name,
        expected_version=expected_version,
        output_dir=output_dir,
        seed=seed,
        epochs=epochs,
        batch_size=batch_size,
        dropout=dropout,
        dev_ratio=dev_ratio,
        confirm_before_run=confirm_before_run,
    )


def wait_for_keypress(enabled: bool) -> None:
    if not enabled:
        return
    print("Hit any key to continue or ^C to abort")
    try:
        import termios
        import tty

        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
        print()
    except Exception:
        input("Press Enter to continue or Ctrl+C to abort: ")


def db_to_examples(conn, nlp, *, created_after: str | None):
    sql = """
        SELECT text, spans
        FROM gold_training
        WHERE (%s::date IS NULL OR created_at::DATE > %s::date)
    """
    examples = []
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (created_after, created_after))
        for row in cur.fetchall():
            text = row["text"]
            pred_doc = nlp.make_doc(text)
            gold_doc = nlp.make_doc(text)
            spans = []
            for s in row["spans"]:
                span = gold_doc.char_span(
                    s["start"], s["end"], label=s["label"], alignment_mode="contract"
                )
                if span:
                    spans.append(span)
            gold_doc.ents = spans
            example = Example(pred_doc, gold_doc)
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

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fine-tune NER from gold_training rows in PostgreSQL."
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Path to TOML config file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config.resolve())

    random.seed(config.seed)
    fix_random_seed(config.seed)

    nlp = spacy.load(config.model_name)
    loaded_version = nlp.meta.get("version")
    if config.expected_version:
        assert (
            loaded_version == config.expected_version
        ), f"Wrong {config.model_name} version installed: {loaded_version}"

    print(
        f"Training off of {config.model_name} == {loaded_version or 'unknown'}"
    )
    print(
        "gold_training more recent than "
        + (config.created_after if config.created_after else "(all rows)")
    )
    print(f"Store newly trained model in {config.output_dir}")
    wait_for_keypress(config.confirm_before_run)

    conn = psycopg.connect(config.db_dsn)
    ner = nlp.get_pipe("ner")

    examples = db_to_examples(conn, nlp, created_after=config.created_after)

    for ex in examples:
        for ent in ex.reference.ents:
            ner.add_label(ent.label_)

    optimizer = nlp.resume_training()

    train_examples, dev_examples = split_examples(
        examples, seed=config.seed, dev_ratio=config.dev_ratio
    )

    for example in train_examples:
        for span in example.reference.ents:
            ner.add_label(span.label_)

    for example in dev_examples:
        for span in example.reference.ents:
            ner.add_label(span.label_)

    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]

    with nlp.disable_pipes(*other_pipes):
        for epoch in range(1, config.epochs + 1):
            random.shuffle(train_examples)
            losses: dict[str, float] = {}
            for batch in minibatch(train_examples, size=config.batch_size):
                nlp.update(batch, sgd=optimizer, drop=config.dropout, losses=losses)
            log = f"Epoch {epoch}/{config.epochs} Losses: {losses}"
            if dev_examples:
                scores = nlp.evaluate(dev_examples)
                ents_f = scores.get("ents_f")
                if ents_f is not None:
                    log += f" | Dev ents_f: {ents_f:.3f}"
            print(log)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    nlp.to_disk(config.output_dir)
    print(f"Saved fine-tuned model to {config.output_dir}")


if __name__ == "__main__":
    main()
