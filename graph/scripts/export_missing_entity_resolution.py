#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


SUMMARY_SQL = """
WITH missing AS (
    WITH ner_mentions AS (
        SELECT
            v.id AS verse_id,
            v.identifier,
            v.verse_num,
            (ent->>'label')::text AS label,
            (ent->>'text')::text AS surface,
            CASE
                WHEN (ent->>'label')::text = 'PERSON' THEN
                    regexp_replace(
                        lower(unaccent(trim(ent->>'text'))),
                        '^(ven\\.?-?|venerable|the venerable|master)\\s*',
                        '',
                        'i'
                    )
                ELSE lower(unaccent(trim(ent->>'text')))
            END AS norm
        FROM ati_verses v
        CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) ent
        WHERE v.ner_span IS NOT NULL
          AND (ent->>'label') IN ('PERSON', 'LOC', 'GPE')
          AND coalesce(ent->>'text', '') <> ''
    ),
    entity_names AS (
        SELECT e.id AS entity_id, e.entity_type AS label, e.canonical, e.normalized
        FROM ati_entities e
        UNION ALL
        SELECT e.id AS entity_id, e.entity_type AS label, e.canonical, a.normalized
        FROM ati_entity_aliases a
        JOIN ati_entities e ON e.id = a.entity_id
    ),
    entity_keys AS (
        SELECT
            label,
            canonical,
            normalized,
            regexp_replace(normalized, '[^a-z0-9]+', '', 'g') AS norm_key
        FROM entity_names
    )
    SELECT
        nm.verse_id,
        nm.identifier,
        nm.verse_num,
        nm.label,
        nm.surface,
        nm.norm,
        regexp_replace(nm.norm, '[^a-z0-9]+', '', 'g') AS norm_key,
        (
          SELECT min(ek.canonical)
          FROM entity_keys ek
          WHERE ek.label = nm.label
            AND ek.norm_key = regexp_replace(nm.norm, '[^a-z0-9]+', '', 'g')
        ) AS suggested_canonical,
        CASE
          WHEN nm.label = 'PERSON'
               AND nm.norm ~ '^(arahant|tathagata|tathagatas|noble one|fully enlightened one|fully self-enlightened one|consummate one|perfect one|supreme|sugata)$'
            THEN 'likely_noise_or_bad_ner'
          WHEN nm.norm ~ '^(one|sir|elder|\\.\\.\\.|.*\\&.*)$'
            THEN 'likely_noise_or_bad_ner'
          WHEN EXISTS (
              SELECT 1
              FROM entity_keys ek
              WHERE ek.label = nm.label
                AND ek.norm_key = regexp_replace(nm.norm, '[^a-z0-9]+', '', 'g')
          )
            THEN 'likely_missing_alias_for_existing'
          ELSE 'likely_missing_canonical_entity'
        END AS bucket
    FROM ner_mentions nm
    LEFT JOIN entity_names en
      ON en.label = nm.label
     AND en.normalized = nm.norm
    WHERE en.entity_id IS NULL
)
SELECT bucket, count(*) AS mentions, count(DISTINCT (label, norm)) AS distinct_norms
FROM missing
GROUP BY bucket
ORDER BY mentions DESC, bucket
"""


TOP_SQL = """
WITH missing AS (
    WITH ner_mentions AS (
        SELECT
            v.id AS verse_id,
            v.identifier,
            v.verse_num,
            (ent->>'label')::text AS label,
            (ent->>'text')::text AS surface,
            CASE
                WHEN (ent->>'label')::text = 'PERSON' THEN
                    regexp_replace(
                        lower(unaccent(trim(ent->>'text'))),
                        '^(ven\\.?-?|venerable|the venerable|master)\\s*',
                        '',
                        'i'
                    )
                ELSE lower(unaccent(trim(ent->>'text')))
            END AS norm
        FROM ati_verses v
        CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) ent
        WHERE v.ner_span IS NOT NULL
          AND (ent->>'label') IN ('PERSON', 'LOC', 'GPE')
          AND coalesce(ent->>'text', '') <> ''
    ),
    entity_names AS (
        SELECT e.id AS entity_id, e.entity_type AS label, e.canonical, e.normalized
        FROM ati_entities e
        UNION ALL
        SELECT e.id AS entity_id, e.entity_type AS label, e.canonical, a.normalized
        FROM ati_entity_aliases a
        JOIN ati_entities e ON e.id = a.entity_id
    ),
    entity_keys AS (
        SELECT
            label,
            canonical,
            normalized,
            regexp_replace(normalized, '[^a-z0-9]+', '', 'g') AS norm_key
        FROM entity_names
    )
    SELECT
        nm.label,
        nm.surface,
        nm.norm,
        (
          SELECT min(ek.canonical)
          FROM entity_keys ek
          WHERE ek.label = nm.label
            AND ek.norm_key = regexp_replace(nm.norm, '[^a-z0-9]+', '', 'g')
        ) AS suggested_canonical,
        CASE
          WHEN nm.label = 'PERSON'
               AND nm.norm ~ '^(arahant|tathagata|tathagatas|noble one|fully enlightened one|fully self-enlightened one|consummate one|perfect one|supreme|sugata)$'
            THEN 'likely_noise_or_bad_ner'
          WHEN nm.norm ~ '^(one|sir|elder|\\.\\.\\.|.*\\&.*)$'
            THEN 'likely_noise_or_bad_ner'
          WHEN EXISTS (
              SELECT 1
              FROM entity_keys ek
              WHERE ek.label = nm.label
                AND ek.norm_key = regexp_replace(nm.norm, '[^a-z0-9]+', '', 'g')
          )
            THEN 'likely_missing_alias_for_existing'
          ELSE 'likely_missing_canonical_entity'
        END AS bucket
    FROM ner_mentions nm
    LEFT JOIN entity_names en
      ON en.label = nm.label
     AND en.normalized = nm.norm
    WHERE en.entity_id IS NULL
)
SELECT
    label,
    norm,
    min(surface) AS example_surface,
    min(suggested_canonical) AS suggested_canonical,
    bucket,
    count(*) AS freq
FROM missing
GROUP BY label, norm, bucket
ORDER BY freq DESC, label, norm
LIMIT %(limit)s
"""


DETAIL_SQL = """
WITH missing AS (
    WITH ner_mentions AS (
        SELECT
            v.id AS verse_id,
            v.identifier,
            v.verse_num,
            (ent->>'label')::text AS label,
            (ent->>'text')::text AS surface,
            CASE
                WHEN (ent->>'label')::text = 'PERSON' THEN
                    regexp_replace(
                        lower(unaccent(trim(ent->>'text'))),
                        '^(ven\\.?-?|venerable|the venerable|master)\\s*',
                        '',
                        'i'
                    )
                ELSE lower(unaccent(trim(ent->>'text')))
            END AS norm
        FROM ati_verses v
        CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) ent
        WHERE v.ner_span IS NOT NULL
          AND (ent->>'label') IN ('PERSON', 'LOC', 'GPE')
          AND coalesce(ent->>'text', '') <> ''
    ),
    entity_names AS (
        SELECT e.id AS entity_id, e.entity_type AS label, e.canonical, e.normalized
        FROM ati_entities e
        UNION ALL
        SELECT e.id AS entity_id, e.entity_type AS label, e.canonical, a.normalized
        FROM ati_entity_aliases a
        JOIN ati_entities e ON e.id = a.entity_id
    ),
    entity_keys AS (
        SELECT
            label,
            canonical,
            normalized,
            regexp_replace(normalized, '[^a-z0-9]+', '', 'g') AS norm_key
        FROM entity_names
    )
    SELECT
        nm.verse_id,
        nm.identifier,
        nm.verse_num,
        nm.label,
        nm.surface,
        nm.norm,
        (
          SELECT min(ek.canonical)
          FROM entity_keys ek
          WHERE ek.label = nm.label
            AND ek.norm_key = regexp_replace(nm.norm, '[^a-z0-9]+', '', 'g')
        ) AS suggested_canonical,
        CASE
          WHEN nm.label = 'PERSON'
               AND nm.norm ~ '^(arahant|tathagata|tathagatas|noble one|fully enlightened one|fully self-enlightened one|consummate one|perfect one|supreme|sugata)$'
            THEN 'likely_noise_or_bad_ner'
          WHEN nm.norm ~ '^(one|sir|elder|\\.\\.\\.|.*\\&.*)$'
            THEN 'likely_noise_or_bad_ner'
          WHEN EXISTS (
              SELECT 1
              FROM entity_keys ek
              WHERE ek.label = nm.label
                AND ek.norm_key = regexp_replace(nm.norm, '[^a-z0-9]+', '', 'g')
          )
            THEN 'likely_missing_alias_for_existing'
          ELSE 'likely_missing_canonical_entity'
        END AS bucket
    FROM ner_mentions nm
    LEFT JOIN entity_names en
      ON en.label = nm.label
     AND en.normalized = nm.norm
    WHERE en.entity_id IS NULL
)
SELECT *
FROM missing
ORDER BY identifier, verse_num, label, norm
"""


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def run_query(conn: psycopg.Connection, sql: str, params: dict | None = None) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(description="Export missing graph-entity resolution audit reports.")
    parser.add_argument("--dsn", default="dbname=tipitaka user=alee", help="Postgres DSN")
    parser.add_argument(
        "--out-dir",
        default="graph/entities/reports",
        help="Directory for output CSV files",
    )
    parser.add_argument("--top-limit", type=int, default=500, help="Max rows in top_missing.csv")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    with psycopg.connect(args.dsn) as conn:
        summary = run_query(conn, SUMMARY_SQL)
        top_missing = run_query(conn, TOP_SQL, {"limit": args.top_limit})
        details = run_query(conn, DETAIL_SQL)

    write_csv(out_dir / "missing_resolution_summary.csv", summary)
    write_csv(out_dir / "missing_resolution_top.csv", top_missing)
    write_csv(out_dir / "missing_resolution_details.csv", details)

    print(f"Wrote {out_dir / 'missing_resolution_summary.csv'} ({len(summary)} rows)")
    print(f"Wrote {out_dir / 'missing_resolution_top.csv'} ({len(top_missing)} rows)")
    print(f"Wrote {out_dir / 'missing_resolution_details.csv'} ({len(details)} rows)")


if __name__ == "__main__":
    main()
