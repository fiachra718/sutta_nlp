#!/usr/bin/env python3
# train_small_ner.py
import argparse
import json
import random
import sys
from pathlib import Path

import spacy
from spacy.training import Example


def load_jsonl(path: Path):
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            j = json.loads(line)
            yield j["text"], {"entities": [tuple(e) for e in j["entities"]]}


def main():
    ap = argparse.ArgumentParser(description="Train a small spaCy NER with rules-first pipeline.")
    ap.add_argument("--base-model", default="ne-data/models/archive/sutta_ner-v2", help="Base model directory.")
    ap.add_argument("--ruler-patterns", default="ne-data/models/archive/sutta_ner-v2/ruler_patterns", help="entity_ruler patterns dir.")
    ap.add_argument("--train", required=True, help="Training JSONL (aligned, non-overlapping).")
    ap.add_argument("--dev", help="Dev JSONL (optional). If omitted, 80/20 split from train.")
    ap.add_argument("--out", default="ne-data/models/archive/sutta_ner-v3", help="Output model directory.")
    ap.add_argument("--epochs", type=int, default=12)
    ap.add_argument("--batch", type=int, default=32)
    ap.add_argument("--dropout", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument(
        "--resume",
        action="store_true",
        help="Resume training from the loaded pipeline instead of reinitializing weights.",
    )
    args = ap.parse_args()

    base_path = Path(args.base_model).resolve()
    pat_path = Path(args.ruler_patterns).expanduser().resolve()
    train_path = Path(args.train).resolve()
    dev_path = Path(args.dev).resolve() if args.dev else None
    out_path = Path(args.out).resolve()

    print("loading base model:", base_path)
    nlp = spacy.load(base_path)
    print("pipeline (initial):", nlp.pipe_names)

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    train_data = list(load_jsonl(train_path))
    if not train_data:
        print("FATAL: no training examples found.")
        sys.exit(1)

    if dev_path and dev_path.exists():
        dev_data = list(load_jsonl(dev_path))
    else:
        cut = max(1, int(0.2 * len(train_data)))
        dev_data, train_data = train_data[:cut], train_data[cut:]

    print(f"train examples: {len(train_data)} | dev examples: {len(dev_data)}")

    # ------------------------------------------------------------------
    # Ensure NER pipe & labels BEFORE initialize
    # ------------------------------------------------------------------
    if "ner" not in nlp.pipe_names:
        ner = nlp.add_pipe("ner", last=True)
    else:
        ner = nlp.get_pipe("ner")

    labels = {lbl for _, ann in train_data for _, _, lbl in ann["entities"]}
    for lbl in labels:
        ner.add_label(lbl)
    print("labels:", sorted(labels))

    # Ensure an entity_ruler component exists (patterns loaded AFTER initialize)
    if "entity_ruler" not in nlp.pipe_names:
        nlp.add_pipe("entity_ruler", first=True, config={"overwrite_ents": True})
    else:
        # keep it first if not already
        if nlp.pipe_names[0] != "entity_ruler":
            name, comp = nlp.remove_pipe("entity_ruler")
            nlp.add_pipe(comp, name=name, first=True)

    # ------------------------------------------------------------------
    # Initialize with examples
    # ------------------------------------------------------------------
    random.seed(args.seed)
    examples = [Example.from_dict(nlp.make_doc(t), ann) for t, ann in train_data]
    if args.resume:
        optimizer = nlp.resume_training()
        print("pipeline (resuming):", nlp.pipe_names)
    else:
        optimizer = nlp.initialize(lambda: examples)
        print("pipeline (post-init):", nlp.pipe_names)

    # ------------------------------------------------------------------
    # NOW load ruler patterns and assert; keep ruler FIRST
    # ------------------------------------------------------------------
    if not pat_path.exists():
        print(f"FATAL: patterns dir not found: {pat_path}")
        sys.exit(1)

    ruler = nlp.get_pipe("entity_ruler")
    try:
        ruler.from_disk(pat_path)
        print("sample patterns:")
        for p in nlp.get_pipe("entity_ruler").patterns[:10]:
            print(p)
    except Exception as e:
        print(f"FATAL: ruler.from_disk failed: {e!r}")
        sys.exit(1)

    pat_count = len(getattr(ruler, "patterns", []))
    print("pattern count (post-load):", pat_count)
    if pat_count == 0:
        print("FATAL: entity_ruler has ZERO patterns after from_disk — check patterns.jsonl")
        sys.exit(1)

    if nlp.pipe_names[0] != "entity_ruler":
        name, comp = nlp.remove_pipe("entity_ruler")
        nlp.add_pipe(comp, name=name, first=True)

    # Probe that rules fire
    probe = "At Sāvatthī in Jeta’s Grove the Blessed One spoke to Ānanda."
    print("probe ents:", [(e.text, e.label_) for e in nlp(probe).ents])

    # ------------------------------------------------------------------
    # Train
    # ------------------------------------------------------------------
    for epoch in range(1, args.epochs + 1):
        random.shuffle(examples)
        losses = {}
        for i in range(0, len(examples), args.batch):
            nlp.update(
                examples[i:i + args.batch],
                drop=args.dropout,
                losses=losses,
                sgd=optimizer,
            )
        if epoch == 1 or epoch % max(1, args.epochs // 6) == 0:
            print(f"epoch {epoch:02d} losses={losses}")

        # quick mid-train check that ruler didn’t get wiped
        if epoch == 1:
            print("pattern count (mid-train):", len(nlp.get_pipe("entity_ruler").patterns))

    # ------------------------------------------------------------------
    # Evaluate (tiny)
    # ------------------------------------------------------------------
    def score(nlp, data):
        gold_total = got_total = hit = 0
        for t, ann in data:
            gold = {(s, e, l) for (s, e, l) in ann["entities"]}
            got = {(e.start_char, e.end_char, e.label_) for e in nlp(t).ents}
            hit += len(gold & got)
            gold_total += len(gold)
            got_total += len(got)
        prec = hit / max(1, got_total)
        rec = hit / max(1, gold_total)
        f1 = 0.0 if prec + rec == 0 else 2 * prec * rec / (prec + rec)
        return prec, rec, f1

    prec, rec, f1 = score(nlp, dev_data)
    print(f"dev precision={prec:.3f} recall={rec:.3f} f1={f1:.3f}")

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------
    out_path.mkdir(parents=True, exist_ok=True)
    nlp.to_disk(out_path)
    try:
        nlp.get_pipe("entity_ruler").to_disk(out_path / "ruler_patterns")
    except Exception:
        pass
    nlp.config.to_disk(out_path / "config.cfg")
    print("saved ->", out_path)


if __name__ == "__main__":
    main()
