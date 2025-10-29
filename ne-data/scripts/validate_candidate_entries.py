"""
Validate candidate JSONL entries before turning them into training data.

Checks performed:
  • ensures each record has `text` and a list of (label, phrase) entities
  • verifies each phrase appears in the text (case-sensitive and case-insensitive)
  • notes ambiguous phrases that show up multiple times
  • ensures entity phrases appear in text in the same order they are listed
  • reports the canonical character offsets it infers

The script prints human-friendly warnings and can optionally emit a JSONL file
with resolved offsets for downstream processing.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple


CandidateList = List[Tuple[str, str]]


@dataclass
class CandidateIssue:
    kind: str
    detail: str


@dataclass
class ResolvedEntity:
    label: str
    phrase: str
    start: int
    end: int
    occurrences: int


def load_jsonl(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                yield line_no, json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON on line {line_no}: {exc.msg} (char {exc.pos})"
                ) from None


def normalize_entities(record: dict) -> CandidateList:
    raw_entities = record.get("entities") or record.get("spans") or []
    normalized: CandidateList = []
    if not isinstance(raw_entities, list):
        raise ValueError("entities field must be a list.")

    for item in raw_entities:
        if isinstance(item, dict):
            label = item.get("label")
            phrase = item.get("text")
        else:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                raise ValueError(f"Entity entry must be [label, phrase], got: {item!r}")
            label, phrase = item[0], item[1]
        if not label or not phrase:
            raise ValueError(f"Entity entry missing label/text: {item!r}")
        normalized.append((str(label), str(phrase)))

    return normalized


def find_occurrences(text: str, phrase: str) -> List[int]:
    positions: List[int] = []
    start = 0
    while True:
        idx = text.find(phrase, start)
        if idx == -1:
            break
        positions.append(idx)
        start = idx + 1
    return positions


def find_casefold_occurrences(text: str, phrase: str) -> List[int]:
    lower_text = text.lower()
    lower_phrase = phrase.lower()
    positions: List[int] = []
    start = 0
    while True:
        idx = lower_text.find(lower_phrase, start)
        if idx == -1:
            break
        positions.append(idx)
        start = idx + 1
    return positions


def pick_position(
    positions: Sequence[int], cursor: int
) -> Tuple[Optional[int], bool]:
    for pos in positions:
        if pos >= cursor:
            return pos, True
    if positions:
        return positions[0], False
    return None, False


def resolve_entities(text: str, entities: CandidateList) -> Tuple[List[ResolvedEntity], List[CandidateIssue]]:
    issues: List[CandidateIssue] = []
    resolved: List[ResolvedEntity] = []
    cursor = 0
    order_issue_detected = False

    for label, phrase in entities:
        expected_cursor = cursor

        exact_positions = find_occurrences(text, phrase)
        pos, in_order = pick_position(exact_positions, cursor)
        if pos is not None:
            resolved.append(
                ResolvedEntity(
                    label=label,
                    phrase=phrase,
                    start=pos,
                    end=pos + len(phrase),
                    occurrences=len(exact_positions),
                )
            )
            if len(exact_positions) > 1:
                issues.append(
                    CandidateIssue(
                        "ambiguous",
                        f"{label}:{phrase!r} appears {len(exact_positions)} times; using first at {pos}.",
                    )
                )
            if not in_order:
                order_issue_detected = True
                issues.append(
                    CandidateIssue(
                        "order",
                        f"{label}:{phrase!r} occurs before the previous entity (first at {pos}, expected ≥ {expected_cursor}).",
                    )
                )
            cursor = max(cursor, pos + len(phrase))
            continue

        casefold_positions = find_casefold_occurrences(text, phrase)
        pos, in_order = pick_position(casefold_positions, cursor)
        if pos is not None:
            actual = text[pos : pos + len(phrase)]
            resolved.append(
                ResolvedEntity(
                    label=label,
                    phrase=phrase,
                    start=pos,
                    end=pos + len(phrase),
                    occurrences=len(casefold_positions),
                )
            )
            issues.append(
                CandidateIssue(
                    "case-mismatch",
                    f"{label}:{phrase!r} not found with exact case; matched {actual!r} at {pos}.",
                )
            )
            if len(casefold_positions) > 1:
                issues.append(
                    CandidateIssue(
                        "ambiguous",
                        f"{label}:{phrase!r} (case-insensitive) appears {len(casefold_positions)} times; using first at {pos}.",
                    )
                )
            if not in_order:
                order_issue_detected = True
                issues.append(
                    CandidateIssue(
                        "order",
                        f"{label}:{phrase!r} occurs before the previous entity (first at {pos}, expected ≥ {expected_cursor}).",
                    )
                )
            cursor = max(cursor, pos + len(phrase))
            continue

        issues.append(
            CandidateIssue("missing", f"{label}:{phrase!r} not found in text.")
        )

    ordered = sorted(resolved, key=lambda ent: ent.start)
    if [ent.start for ent in ordered] != [ent.start for ent in resolved]:
        if not order_issue_detected:
            issues.append(
                CandidateIssue(
                    "order",
                    "Entity phrases are not in ascending order by offset.",
                )
            )
        resolved = ordered

    return resolved, issues


def record_summary(resolved: List[ResolvedEntity]) -> str:
    parts: List[str] = []
    for ent in resolved:
        parts.append(
            f"{ent.label}:{ent.phrase!r} ({ent.start}-{ent.end})"
            + (f" x{ent.occurrences}" if ent.occurrences > 1 else "")
        )
    return "; ".join(parts) if parts else "<none>"


def validate_file(
    input_path: Path,
    *,
    output_path: Optional[Path] = None,
    limit: Optional[int] = None,
) -> int:
    results: List[dict] = []
    issues_found = 0
    processed = 0

    for count, (line_no, record) in enumerate(load_jsonl(input_path), start=1):
        if limit is not None and count > limit:
            break

        text = record.get("text")
        if not isinstance(text, str):
            print(f"[{line_no}] Missing or invalid 'text' field.")
            issues_found += 1
            processed += 1
            continue

        try:
            entities = normalize_entities(record)
        except ValueError as exc:
            print(f"[{line_no}] ERROR: {exc}")
            issues_found += 1
            processed += 1
            continue

        resolved, issues = resolve_entities(text, entities)
        processed += 1

        if issues:
            issues_found += 1
            rec_id = record.get("id") or f"line-{line_no}"
            print(f"[{line_no}] {rec_id}")
            for issue in issues:
                print(f"  {issue.kind:12s} {issue.detail}")
            print(f"  resolved: {record_summary(resolved)}")

        if output_path:
            results.append(
                {
                    "text": text,
                    "entities": [[ent.start, ent.end, ent.label] for ent in resolved],
                    "id": record.get("id"),
                    "meta": record.get("meta"),
                    "raw_entities": record.get("entities"),
                }
            )

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as fh:
            for item in results:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        print(f"Wrote {len(results)} resolved records -> {output_path}")

    print(f"Checked {processed} records. Issues: {issues_found}")
    return issues_found


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    default_path = Path("ne-data/work/candidates/cleaned_candidates.jsonl")
    parser = argparse.ArgumentParser(
        description="Validate candidate JSONL entries for training readiness."
    )
    parser.add_argument(
        "path",
        nargs="?",
        type=Path,
        default=default_path,
        help=f"Candidate JSONL file (default: {default_path})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write a resolved JSONL with character offsets.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only process the first N records.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    return validate_file(args.path, output_path=args.output, limit=args.limit)


if __name__ == "__main__":
    raise SystemExit(main())
