from __future__ import annotations

import argparse
import psycopg
from psycopg.rows import dict_row


def make_context(text: str, start: int, end: int, window: int = 60) -> str:
    left = max(0, start - window)
    right = min(len(text), end + window)
    return text[left:start] + "[[" + text[start:end] + "]]" + text[end:right]


def search_spans(
    conn: psycopg.Connection,
    search_term: str,
    *,
    verbose: bool = False,
    contains: bool = False,
) -> int:
    comparator = "ILIKE" if contains else "="
    count = 0
    SQL = f"""
        SELECT
            g.id,
            g.text,
            elem->>'label' AS label,
            elem->>'text' AS span_text,
            (elem->>'start')::int AS start_char,
            (elem->>'end')::int AS end_char
        FROM gold_training g
        CROSS JOIN LATERAL jsonb_array_elements(g.spans::jsonb) AS elem
        WHERE elem->>'text' {comparator} %s
        ORDER BY g.created_at NULLS LAST, g.id
    """
    needle = f"%{search_term}%" if contains else search_term

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(SQL, (needle,))
        for row in cur.fetchall():
            print(
                f"{row['id']}\t{row['label']}\t{row['span_text']}"
                f"\t({row['start_char']}-{row['end_char']})"
            )
            if verbose:
                print(f"\t{make_context(row['text'] or '', row['start_char'], row['end_char'])}")
            count += 1
    return count


def main() -> None:
    ap = argparse.ArgumentParser(description="search gold_training spans")
    ap.add_argument("--search-term", required=True, help="Tagged entity text to search for.")
    ap.add_argument(
        "--verbose",
        type=int,
        default=0,
        help="Set to 1 to print text context around each match.",
    )
    ap.add_argument(
        "--contains",
        action="store_true",
        help="Use case-insensitive substring matching instead of exact match.",
    )

    args = ap.parse_args()
    with psycopg.connect("dbname=tipitaka user=alee") as conn:
        count = search_spans(
            conn,
            args.search_term,
            verbose=bool(args.verbose),
            contains=args.contains,
        )
    if count == 0:
        mode = "contains" if args.contains else "exact"
        print(f"No rows matched ({mode}) text={args.search_term!r}.")


if __name__ == "__main__":
    main()
