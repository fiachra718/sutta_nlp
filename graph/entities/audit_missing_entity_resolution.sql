-- Graph-focused entity resolution audit (PERSON / LOC / GPE only).
--
-- Purpose:
-- 1) Find NER spans in ati_verses.ner_span that do not resolve to ati_entities/aliases.
-- 2) Classify misses into actionable buckets:
--    - likely_missing_alias_for_existing
--    - likely_missing_canonical_entity
--    - likely_noise_or_bad_ner
-- 3) Produce verse-level rows for targeted correction.
--
-- Usage:
--   psql -d tipitaka -f graph/entities/audit_missing_entity_resolution.sql

DROP TABLE IF EXISTS tmp_missing_entity_resolution;

CREATE TEMP TABLE tmp_missing_entity_resolution AS
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
),
missing AS (
    SELECT
        nm.verse_id,
        nm.identifier,
        nm.verse_num,
        nm.label,
        nm.surface,
        nm.norm,
        regexp_replace(nm.norm, '[^a-z0-9]+', '', 'g') AS norm_key
    FROM ner_mentions nm
    LEFT JOIN entity_names en
      ON en.label = nm.label
     AND en.normalized = nm.norm
    WHERE en.entity_id IS NULL
)
SELECT
    m.*,
    (
      SELECT min(ek.canonical)
      FROM entity_keys ek
      WHERE ek.label = m.label
        AND ek.norm_key = m.norm_key
    ) AS suggested_canonical,
    CASE
      WHEN m.label = 'PERSON'
           AND m.norm ~ '^(arahant|tathagata|tathagatas|noble one|fully enlightened one|fully self-enlightened one|consummate one|perfect one|supreme|sugata)$'
        THEN 'likely_noise_or_bad_ner'
      WHEN m.norm ~ '^(one|sir|elder|\\.\\.\\.|.*\\&.*)$'
        THEN 'likely_noise_or_bad_ner'
      WHEN EXISTS (
          SELECT 1
          FROM entity_keys ek
          WHERE ek.label = m.label
            AND ek.norm_key = m.norm_key
      )
        THEN 'likely_missing_alias_for_existing'
      ELSE 'likely_missing_canonical_entity'
    END AS bucket
FROM missing m;

-- 1) High-level totals
SELECT
    (SELECT count(*)
     FROM ati_verses v
     CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) ent
     WHERE v.ner_span IS NOT NULL
       AND (ent->>'label') IN ('PERSON', 'LOC', 'GPE')
       AND coalesce(ent->>'text', '') <> '') AS total_graph_mentions,
    (SELECT count(*) FROM tmp_missing_entity_resolution) AS missing_mentions,
    (SELECT count(DISTINCT (label, norm)) FROM tmp_missing_entity_resolution) AS missing_distinct_label_norm;

-- 2) Bucket summary
SELECT
    bucket,
    count(*) AS mentions,
    count(DISTINCT (label, norm)) AS distinct_norms
FROM tmp_missing_entity_resolution
GROUP BY bucket
ORDER BY mentions DESC, bucket;

-- 3) Top unresolved label+norm by frequency
SELECT
    label,
    norm,
    min(surface) AS example_surface,
    min(suggested_canonical) AS suggested_canonical,
    bucket,
    count(*) AS freq
FROM tmp_missing_entity_resolution
GROUP BY label, norm, bucket
ORDER BY freq DESC, label, norm
LIMIT 500;

-- 4) Verse-level rows for direct correction/export
SELECT
    verse_id,
    identifier,
    verse_num,
    label,
    surface,
    norm,
    suggested_canonical,
    bucket
FROM tmp_missing_entity_resolution
ORDER BY identifier, verse_num, label, norm;
