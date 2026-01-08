"""
Audit candidate JSONL records against the active entity_ruler and NER.

For each record we:
  * build expected entity spans from the candidate phrases
  * run the pipeline with only the entity_ruler active
  * run the pipeline with only the NER active
  * run the full pipeline
and report any mismatches.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence, Tuple

from contextlib import nullcontext

import spacy

from local_settings import WORK, load_model
from ner_pipeline import candidate_record_to_spans

SpanTuple = Tuple[int, int, str]
JSON_DECODER = json.JSONDecoder()


@dataclass
class CandidateRecord:
    index: int
    line_no: int
    data: dict


def iter_jsonl(path: Path) -> Iterator[CandidateRecord]:
    """
    Yield JSON objects from a JSONL file, tolerating multiple objects per line.
    """
    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            text = raw.strip()
            if not text:
                continue
            idx = 0
            length = len(text)
            while idx < length:
                while idx < length and text[idx].isspace():
                    idx += 1
                if idx >= length:
                    break
                try:
                    obj, next_idx = JSON_DECODER.raw_decode(text, idx)
                except json.JSONDecodeError as exc:
                    snippet = text[max(0, exc.pos - 40) : exc.pos + 40]
                    raise ValueError(
                        f"Malformed JSON in {path} line {line_no}: {exc.msg} "
                        f"(char {exc.pos}). Near: {snippet!r}"
                    ) from None
                yield CandidateRecord(index=line_no, line_no=line_no, data=obj)
                idx = next_idx


def format_spans(spans: Iterable[SpanTuple], text: str) -> str:
    parts: List[str] = []
    for start, end, label in sorted(spans, key=lambda s: (s[0], s[1], s[2])):
        snippet = text[start:end]
        parts.append(f"{label}:{snippet!r} ({start}-{end})")
    return "; ".join(parts) if parts else "<none>"


def doc_spans(doc) -> List[SpanTuple]:
    return [(ent.start_char, ent.end_char, ent.label_) for ent in doc.ents]


def evaluate_record(
    record: CandidateRecord,
    *,
    matcher_nlp: spacy.language.Language,
    full_nlp: spacy.language.Language,
) -> Tuple[set[SpanTuple], set[SpanTuple], set[SpanTuple], set[SpanTuple]]:
    text = record.data.get("text")
    if not text:
        raise ValueError(f"Record on line {record.line_no} missing 'text'.")

    converted = candidate_record_to_spans(record.data, nlp=matcher_nlp)
    expected = {tuple(item) for item in converted["entities"]}

    disable_patterns = [
        name for name in ("ner", "span_ruler") if name in full_nlp.pipe_names
    ]
    disable_ner = [
        name for name in ("entity_ruler", "span_ruler") if name in full_nlp.pipe_names
    ]

    with (full_nlp.disable_pipes(*disable_patterns) if disable_patterns else nullcontext()):
        patterns_doc = full_nlp(text)
    with (full_nlp.disable_pipes(*disable_ner) if disable_ner else nullcontext()):
        ner_doc = full_nlp(text)
    full_doc = full_nlp(text)

    pattern_spans = set(doc_spans(patterns_doc))
    ner_spans = set(doc_spans(ner_doc))
    full_spans = set(doc_spans(full_doc))

    return expected, pattern_spans, ner_spans, full_spans


def audit_candidates(path: Path, *, show_ok: bool, limit: int | None) -> int:
    matcher_nlp = spacy.blank("en")
    full_nlp = load_model()

    issues = 0
    processed = 0

    for record in iter_jsonl(path):
        if limit is not None and processed >= limit:
            break
        processed += 1

        expected, pattern_spans, ner_spans, full_spans = evaluate_record(
            record, matcher_nlp=matcher_nlp, full_nlp=full_nlp
        )
        text = record.data.get("text", "")
        record_id = record.data.get("id") or f"line-{record.line_no}"

        missing_patterns = expected - pattern_spans
        missing_ner = expected - ner_spans
        missing_full = expected - full_spans
        extra_full = full_spans - expected

        if missing_patterns or missing_ner or missing_full or extra_full:
            issues += 1
            print(f"[{record.line_no}] {record_id}")
            if missing_patterns:
                print("  missing in entity_ruler :", format_spans(missing_patterns, text))
            if missing_ner:
                print("  missing in ner          :", format_spans(missing_ner, text))
            if missing_full:
                print("  missing in combined     :", format_spans(missing_full, text))
            if extra_full:
                print("  extra predicted         :", format_spans(extra_full, text))
        elif show_ok:
            print(f"[{record.line_no}] {record_id} OK")

    print(f"Checked {processed} records, issues found: {issues}")
    return issues


def main(argv: Sequence[str] | None = None) -> int:
    default_path = WORK / "candidates" / "candidate_training.jsonl"
    parser = argparse.ArgumentParser(
        description="Validate candidate JSONL against entity_ruler and NER outputs."
    )
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        default=default_path,
        help=f"Candidate JSONL file (default: {default_path})",
    )
    parser.add_argument("--show-ok", action="store_true", help="Print records with no issues.")
    parser.add_argument(
        "--limit",
        type=int,
        help="Only process the first N records (useful for spot checks).",
    )
    args = parser.parse_args(argv)

    return audit_candidates(args.path, show_ok=args.show_ok, limit=args.limit)


if __name__ == "__main__":
    raise SystemExit(main())
