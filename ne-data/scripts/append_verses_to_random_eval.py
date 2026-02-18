#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


def parse_pair(token: str) -> tuple[str, int]:
    if ":" not in token:
        raise ValueError(f"Expected IDENTIFIER:VERSE_NUM, got {token!r}")
    ident, verse_s = token.rsplit(":", 1)
    if not ident:
        raise ValueError(f"Missing identifier in {token!r}")
    return ident, int(verse_s)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Append ati_verses rows to random_verses.txt in format: "
            "text | [span_json,...]"
        )
    )
    ap.add_argument(
        "--dsn",
        default="dbname=tipitaka user=alee",
        help="Postgres DSN (default: dbname=tipitaka user=alee)",
    )
    ap.add_argument(
        "--out",
        default="ne-data/work/random_verses.txt",
        help="Output eval file path (default: ne-data/work/random_verses.txt)",
    )
    ap.add_argument(
        "--pair",
        action="append",
        default=[],
        help="Verse ref in IDENTIFIER:VERSE_NUM form. Repeat this flag.",
    )
    ap.add_argument(
        "--pairs-file",
        default="",
        help="Optional file containing one IDENTIFIER:VERSE_NUM per line.",
    )
    ap.add_argument(
        "--skip-empty-spans",
        action="store_true",
        help="Skip verses that have no spans in ati_verses.ner_span.",
    )
    return ap.parse_args()


def load_pairs(args: argparse.Namespace) -> list[tuple[str, int]]:
    items: list[tuple[str, int]] = []
    for token in args.pair:
        items.append(parse_pair(token.strip()))

    if args.pairs_file:
        p = Path(args.pairs_file)
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            items.append(parse_pair(line))
    if not items:
        raise ValueError("Provide at least one --pair or --pairs-file.")
    return items


def main() -> None:
    args = parse_args()
    pairs = load_pairs(args)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sql = """
        SELECT identifier, verse_num, text, ner_span
        FROM ati_verses
        WHERE identifier = %(identifier)s
          AND verse_num = %(verse_num)s
        LIMIT 1
    """

    lines: list[str] = []
    missing: list[str] = []
    skipped_empty = 0
    for ident, vnum in pairs:
        with psycopg.connect(args.dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, {"identifier": ident, "verse_num": vnum})
            row = cur.fetchone()

        if row is None:
            missing.append(f"{ident}:{vnum}")
            continue

        spans = row.get("ner_span") or []
        if args.skip_empty_spans and not spans:
            skipped_empty += 1
            continue

        text = (row.get("text") or "").replace("\n", " ").strip()
        payload = json.dumps(spans, ensure_ascii=False)
        lines.append(f"{text} | {payload}")

    if lines:
        with out_path.open("a", encoding="utf-8") as fh:
            for line in lines:
                fh.write(line + "\n")

    print(f"Requested: {len(pairs)}")
    print(f"Appended : {len(lines)} -> {out_path}")
    print(f"Skipped empty spans: {skipped_empty}")
    if missing:
        print("Missing verses:")
        for item in missing:
            print(f"  {item}")


if __name__ == "__main__":
    main()
