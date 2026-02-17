-- Compact queue for entity cleanup.
-- Produces one row per unresolved (label, norm), with frequency and sample verse refs.
--
-- Usage:
--   psql -d tipitaka -f graph/entities/entity_cleanup_queue.sql

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
),
classified AS (
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
    FROM missing m
)
SELECT
    c.bucket,
    c.label,
    c.norm,
    min(c.surface) AS example_surface,
    min(c.suggested_canonical) AS suggested_canonical,
    count(*) AS freq,
    string_agg(DISTINCT (c.identifier || ':' || c.verse_num::text), ', ' ORDER BY (c.identifier || ':' || c.verse_num::text)) FILTER (
      WHERE c.identifier IS NOT NULL
    ) AS sample_refs
FROM classified c
GROUP BY c.bucket, c.label, c.norm
ORDER BY freq DESC, c.bucket, c.label, c.norm
LIMIT 300;
