#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from collections import Counter, defaultdict
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row


SPACE_RE = re.compile(r"\s+")


@dataclass
class SpanRecord:
    row_id: str
    created_at: str | None
    label: str
    text: str
    norm_text: str
    context: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit gold_training spans for label drift by finding surface forms "
            "that are tagged with multiple labels."
        )
    )
    parser.add_argument(
        "--dsn",
        default="dbname=tipitaka user=alee",
        help="Postgres DSN (default: dbname=tipitaka user=alee)",
    )
    parser.add_argument(
        "--created-after",
        default=None,
        help="Optional YYYY-MM-DD filter on gold_training.created_at::date > value.",
    )
    parser.add_argument(
        "--where",
        default="",
        help=(
            "Optional SQL predicate appended to WHERE clause "
            "(example: \"id like 'manual%%'\")."
        ),
    )
    parser.add_argument(
        "--focus-label",
        default="LOC",
        help=(
            "Only show conflicts that include this label. Use empty string to show all."
        ),
    )
    parser.add_argument(
        "--min-occurrences",
        type=int,
        default=2,
        help="Minimum mentions for a surface form to be considered (default: 2).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=12,
        help="Maximum number of conflicting forms to print (default: 12).",
    )
    parser.add_argument(
        "--examples-per-label",
        type=int,
        default=2,
        help="How many row examples to print for each label (default: 2).",
    )
    parser.add_argument(
        "--terms",
        nargs="*",
        default=None,
        help=(
            "Optional exact normalized entity texts to inspect in detail "
            "(example: --terms yama \"thirty-three\")."
        ),
    )
    return parser.parse_args()


def normalize_entity_text(text: str) -> str:
    return SPACE_RE.sub(" ", text.strip().lower())


def build_context(text: str, start: int, end: int, window: int = 45) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    snippet = text[left:start] + "[[" + text[start:end] + "]]" + text[end:right]
    return SPACE_RE.sub(" ", snippet.strip())


def fetch_rows(
    conn: psycopg.Connection,
    *,
    created_after: str | None,
    where_clause: str,
) -> list[dict]:
    filters: list[str] = []
    params: dict[str, object] = {}
    if created_after:
        filters.append("created_at::date > %(created_after)s::date")
        params["created_after"] = created_after
    if where_clause.strip():
        filters.append(f"({where_clause})")

    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    sql = f"""
        SELECT id, created_at, text, spans
        FROM gold_training
        {where_sql}
        ORDER BY created_at NULLS LAST, id
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def collect_span_records(rows: list[dict]) -> list[SpanRecord]:
    out: list[SpanRecord] = []
    for row in rows:
        text = row.get("text") or ""
        spans = row.get("spans") or []
        row_id = str(row.get("id"))
        created_at_raw = row.get("created_at")
        created_at = str(created_at_raw) if created_at_raw is not None else None

        for spec in spans:
            try:
                start = int(spec["start"])
                end = int(spec["end"])
                label = str(spec["label"])
            except Exception:
                continue
            if start < 0 or end <= start or end > len(text):
                continue

            ent_text = text[start:end]
            norm = normalize_entity_text(ent_text)
            if not norm:
                continue
            out.append(
                SpanRecord(
                    row_id=row_id,
                    created_at=created_at,
                    label=label,
                    text=ent_text,
                    norm_text=norm,
                    context=build_context(text, start, end),
                )
            )
    return out


def print_summary(span_records: list[SpanRecord]) -> None:
    label_counts = Counter(r.label for r in span_records)
    print(f"Span count: {len(span_records)}")
    print("Label counts:", ", ".join(f"{k}:{label_counts[k]}" for k in sorted(label_counts)))


def main() -> None:
    args = parse_args()

    with psycopg.connect(args.dsn) as conn:
        rows = fetch_rows(
            conn, created_after=args.created_after, where_clause=args.where
        )

    span_records = collect_span_records(rows)
    if not span_records:
        raise SystemExit("No spans found for the provided filters.")

    print(f"Rows loaded: {len(rows)}")
    print_summary(span_records)

    by_form: dict[str, list[SpanRecord]] = defaultdict(list)
    for rec in span_records:
        by_form[rec.norm_text].append(rec)

    conflicts = []
    for form, recs in by_form.items():
        if len(recs) < args.min_occurrences:
            continue
        labels = Counter(r.label for r in recs)
        if len(labels) < 2:
            continue
        if args.focus_label and args.focus_label not in labels:
            continue
        dominant = labels.most_common(1)[0][1]
        disagreement = 1.0 - (dominant / len(recs))
        conflicts.append((form, recs, labels, disagreement))

    conflicts.sort(
        key=lambda item: (
            -item[3],  # higher disagreement first
            -len(item[1]),  # then support
            item[0],
        )
    )

    print()
    print(
        f"Conflicting forms found: {len(conflicts)} "
        f"(focus_label={args.focus_label or 'ALL'})"
    )
    if not conflicts:
        return

    for form, recs, labels, disagreement in conflicts[: args.top]:
        total = len(recs)
        breakdown = ", ".join(
            f"{label}:{count}" for label, count in labels.most_common()
        )
        print()
        print(
            f"- {form!r} | mentions={total} | labels={breakdown} "
            f"| disagreement={disagreement:.2f}"
        )

        grouped: dict[str, list[SpanRecord]] = defaultdict(list)
        for rec in recs:
            grouped[rec.label].append(rec)

        for label, examples in sorted(grouped.items()):
            print(f"  {label} examples:")
            seen = 0
            for ex in examples:
                print(
                    "    "
                    f"id={ex.row_id} created_at={ex.created_at or 'NULL'} "
                    f"text={ex.text!r} ctx={ex.context!r}"
                )
                seen += 1
                if seen >= args.examples_per_label:
                    break

    if args.terms:
        wanted = {normalize_entity_text(t) for t in args.terms if t.strip()}
        print()
        print(f"Detailed term audit: {len(wanted)} terms")
        for term in sorted(wanted):
            recs = by_form.get(term, [])
            if not recs:
                print()
                print(f"- {term!r}: no matches")
                continue

            labels = Counter(r.label for r in recs)
            print()
            print(
                f"- {term!r}: mentions={len(recs)} | "
                + ", ".join(f"{k}:{labels[k]}" for k in labels)
            )
            for rec in recs:
                print(
                    "  "
                    f"id={rec.row_id} created_at={rec.created_at or 'NULL'} "
                    f"label={rec.label} text={rec.text!r} ctx={rec.context!r}"
                )


if __name__ == "__main__":
    main()
