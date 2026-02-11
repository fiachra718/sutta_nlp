#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
import spacy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a spaCy model on gold_training text and export predicted spans as JSONL."
    )
    parser.add_argument(
        "--dsn",
        default="dbname=tipitaka user=alee",
        help="Postgres DSN (default: dbname=tipitaka user=alee)",
    )
    parser.add_argument(
        "--model",
        default="en_sutta_ner",
        help="spaCy model path or package name (default: en_sutta_ner)",
    )
    parser.add_argument(
        "--out",
        default="ne-data/work/gold_training_predicted.jsonl",
        help="Output JSONL path",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row limit for quick checks (0 = no limit)",
    )
    parser.add_argument(
        "--where",
        default="",
        help="Optional SQL WHERE clause suffix for gold_training (e.g. \"id like 'manual%%'\")",
    )
    return parser.parse_args()


def fetch_rows(conn: psycopg.Connection, where_clause: str, limit: int) -> list[dict]:
    where_sql = f"WHERE {where_clause}" if where_clause.strip() else ""
    limit_sql = "LIMIT %(limit)s" if limit and limit > 0 else ""
    sql = f"""
        SELECT id, text, source
        FROM gold_training
        {where_sql}
        ORDER BY created_at NULLS LAST, id
        {limit_sql}
    """
    params = {"limit": limit} if limit and limit > 0 else {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def doc_to_spans(doc) -> list[dict]:
    return [
        {
            "start": ent.start_char,
            "end": ent.end_char,
            "label": ent.label_,
            "text": ent.text,
        }
        for ent in doc.ents
    ]


def main() -> None:
    args = parse_args()
    nlp = spacy.load(args.model)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(args.dsn) as conn:
        rows = fetch_rows(conn, args.where, args.limit)

    with out_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            text = row["text"] or ""
            doc = nlp(text)
            payload = {
                "text": text,
                "spans": doc_to_spans(doc),
                "meta": {"gold_id": row["id"]},
                "source": row.get("source"),
            }
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()
