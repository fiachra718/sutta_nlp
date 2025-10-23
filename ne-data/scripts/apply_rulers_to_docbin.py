"""
Utility to augment an existing DocBin with entity/span ruler matches.

This is useful when you want to turn rule-based matches into gold annotations
before training, instead of relying on ``training.annotating_components`` which
only runs the rules on the predicted docs during the update step (and can cause
training failures such as [E024]).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List, Sequence

import spacy
import srsly
from spacy.tokens import Doc, DocBin, Span
from spacy.util import filter_spans


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Augment a DocBin with entity/span ruler matches."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to the source DocBin (*.spacy).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Destination for the augmented DocBin (*.spacy).",
    )
    parser.add_argument(
        "--entity-patterns",
        required=True,
        type=Path,
        help="JSONL file containing EntityRuler patterns.",
    )
    parser.add_argument(
        "--span-patterns",
        required=True,
        type=Path,
        help="JSONL file containing SpanRuler patterns.",
    )
    parser.add_argument(
        "--spans-key",
        default="BOOTSTRAP",
        help="SpanRuler spans_key to use (default: BOOTSTRAP).",
    )
    return parser.parse_args()


def load_patterns(path: Path) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Pattern file not found: {path}")
    patterns = list(srsly.read_jsonl(path))
    invalid = [p for p in patterns if "pattern" not in p]
    if invalid:
        raise ValueError(
            f"{path} contains {len(invalid)} entries without a 'pattern' key."
        )
    return patterns


def spans_overlap(span: Span, spans: Sequence[Span]) -> bool:
    for other in spans:
        if span.doc is not other.doc:
            continue
        if other.end <= span.start or span.end <= other.start:
            continue
        return True
    return False


def augment_doc(
    doc: Doc,
    entity_ruler,
    span_ruler,
    spans_key: str,
) -> Doc:
    """Return a copy of ``doc`` with rule-based matches merged into doc.ents."""
    augmented = doc.copy()
    # Remember manual annotations so we can keep them if overlaps occur.
    original_ents = list(augmented.ents)
    original_map = {(ent.start, ent.end): ent for ent in original_ents}

    entity_ruler(augmented)
    span_ruler(augmented)

    merged: List[Span] = list(original_ents)
    for ent in augmented.ents:
        key = (ent.start, ent.end)
        if key in original_map:
            continue  # already present from manual annotations
        if spans_overlap(ent, original_ents):
            # Skip rule-based matches that conflict with human labels.
            continue
        merged.append(ent)

    augmented.ents = tuple(filter_spans(merged))
    # Preserve any span groups produced by the span ruler.
    if spans_key in augmented.spans:
        augmented.spans[spans_key] = list(filter_spans(augmented.spans[spans_key]))
    return augmented


def main() -> None:
    args = parse_args()
    ent_patterns = load_patterns(args.entity_patterns)
    span_patterns = load_patterns(args.span_patterns)

    nlp = spacy.blank("en")
    er = nlp.add_pipe(
        "entity_ruler",
        config={"ent_id_sep": "||", "phrase_matcher_attr": "LOWER", "overwrite_ents": False},
    )
    er.add_patterns(ent_patterns)
    sr = nlp.add_pipe(
        "span_ruler",
        config={"spans_key": args.spans_key, "annotate_ents": True, "overwrite": False},
    )
    sr.add_patterns(span_patterns)

    docbin = DocBin().from_disk(args.input)
    docs = list(docbin.get_docs(nlp.vocab))

    out_bin = DocBin(store_user_data=True)
    for doc in docs:
        aug = augment_doc(doc, er, sr, args.spans_key)
        out_bin.add(aug)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    out_bin.to_disk(args.output)
    print(f"Wrote augmented corpus with {len(docs)} docs to {args.output}")


if __name__ == "__main__":
    main()
