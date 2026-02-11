-- Periodic audit: NER mentions in ati_verses.ner_span that do not resolve
-- to ati_entities / ati_entity_aliases by normalized form.
--
-- Usage:
--   psql -d tipitaka -f graph/entities/audit_missing_ner.sql
--
-- Notes:
-- - This follows the same normalization style used elsewhere in this repo:
--   unaccent + lower + trim.
-- - PERSON mentions also strip common honorific prefixes.

DROP TABLE IF EXISTS tmp_missing_ner_audit;

CREATE TEMP TABLE tmp_missing_ner_audit AS
WITH ner_mentions AS (
    SELECT
        v.id AS verse_id,
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
      AND (ent->>'label') IN ('PERSON', 'NORP', 'GPE', 'LOC')
      AND coalesce(ent->>'text', '') <> ''
),
entity_names AS (
    SELECT e.entity_type AS label, e.normalized
    FROM ati_entities e
    UNION
    SELECT e.entity_type AS label, a.normalized
    FROM ati_entity_aliases a
    JOIN ati_entities e ON e.id = a.entity_id
)
SELECT
    nm.verse_id,
    nm.label,
    nm.surface,
    nm.norm
FROM ner_mentions nm
LEFT JOIN entity_names en
    ON en.label = nm.label
   AND en.normalized = nm.norm
WHERE en.normalized IS NULL;

-- 1) High-level counts
SELECT
    (SELECT count(*)
     FROM ati_verses v
     CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) ent
     WHERE v.ner_span IS NOT NULL
       AND (ent->>'label') IN ('PERSON', 'NORP', 'GPE', 'LOC')
       AND coalesce(ent->>'text', '') <> '') AS total_ner_mentions,
    (SELECT count(*) FROM tmp_missing_ner_audit) AS missing_ner_mentions,
    (SELECT count(DISTINCT (label, norm)) FROM tmp_missing_ner_audit) AS missing_distinct_label_norm;

-- 2) Missing counts by label
SELECT
    label,
    count(*) AS missing_mentions,
    count(DISTINCT norm) AS missing_distinct_norms
FROM tmp_missing_ner_audit
GROUP BY label
ORDER BY missing_mentions DESC, label;

-- 3) Top missing normalized names by frequency
SELECT
    label,
    norm,
    min(surface) AS example_surface,
    count(*) AS freq
FROM tmp_missing_ner_audit
GROUP BY label, norm
ORDER BY freq DESC, label, norm
LIMIT 300;

-- 4) Quick quality buckets (heuristic)
SELECT
    CASE
        WHEN norm ~ '^(arahant|tathagata|tathagatas|noble one|fully enlightened one|consummate one|perfect one|supreme|sugata)$'
            THEN 'title_or_epithet'
        WHEN norm ~ '^(one|sir|\\.\\.\\.|.*\\&.*)$'
            THEN 'noise_or_artifact'
        WHEN label = 'PERSON'
            THEN 'likely_missing_person_entity'
        ELSE 'other'
    END AS bucket,
    count(DISTINCT (label, norm)) AS distinct_norms,
    count(*) AS mentions
FROM tmp_missing_ner_audit
GROUP BY bucket
ORDER BY mentions DESC, bucket;
