-- Compare gold_training spans to ati_verses.ner_span by verse reference,
-- ignoring character offsets.
--
-- Match key:
--   identifier + verse_num + label + normalized span text
--
-- Usage:
--   psql -d tipitaka -f graph/scripts/compare_gold_to_ati_by_ref.sql

WITH gold_mentions AS (
    SELECT DISTINCT
        split_part(gt.id, ':', 1) AS source_tag,
        split_part(gt.id, ':', 2) AS source_ref,
        split_part(split_part(gt.id, ':', 2), '#', 1) AS identifier,
        NULLIF(split_part(split_part(gt.id, ':', 2), '#', 2), '')::int AS verse_num,
        (s->>'label')::text AS label,
        lower(btrim(s->>'text')) AS span_text
    FROM gold_training gt
    CROSS JOIN LATERAL jsonb_array_elements(gt.spans) AS s
    WHERE gt.id LIKE 'manual:%#%'
      AND btrim(s->>'text') <> ''
),
ati_mentions AS (
    SELECT DISTINCT
        av.identifier,
        av.verse_num,
        (s->>'label')::text AS label,
        lower(btrim(s->>'text')) AS span_text
    FROM ati_verses av
    CROSS JOIN LATERAL jsonb_array_elements(av.ner_span) AS s
    WHERE av.ner_span IS NOT NULL
      AND btrim(s->>'text') <> ''
),
diffs AS (
    SELECT
        'ati_not_in_gold'::text AS diff_type,
        a.identifier,
        a.verse_num,
        a.label,
        a.span_text
    FROM ati_mentions a
    LEFT JOIN gold_mentions g
      ON a.identifier = g.identifier
     AND a.verse_num = g.verse_num
     AND a.label = g.label
     AND a.span_text = g.span_text
    WHERE g.identifier IS NULL

    UNION ALL

    SELECT
        'gold_not_in_ati'::text AS diff_type,
        g.identifier,
        g.verse_num,
        g.label,
        g.span_text
    FROM gold_mentions g
    LEFT JOIN ati_mentions a
      ON g.identifier = a.identifier
     AND g.verse_num = a.verse_num
     AND g.label = a.label
     AND g.span_text = a.span_text
    WHERE a.identifier IS NULL
)
SELECT
    diff_type,
    count(*) AS mention_count
FROM diffs
GROUP BY diff_type
ORDER BY diff_type;


WITH gold_mentions AS (
    SELECT DISTINCT
        split_part(split_part(gt.id, ':', 2), '#', 1) AS identifier,
        NULLIF(split_part(split_part(gt.id, ':', 2), '#', 2), '')::int AS verse_num,
        (s->>'label')::text AS label,
        lower(btrim(s->>'text')) AS span_text
    FROM gold_training gt
    CROSS JOIN LATERAL jsonb_array_elements(gt.spans) AS s
    WHERE gt.id LIKE 'manual:%#%'
      AND btrim(s->>'text') <> ''
),
ati_mentions AS (
    SELECT DISTINCT
        av.identifier,
        av.verse_num,
        (s->>'label')::text AS label,
        lower(btrim(s->>'text')) AS span_text
    FROM ati_verses av
    CROSS JOIN LATERAL jsonb_array_elements(av.ner_span) AS s
    WHERE av.ner_span IS NOT NULL
      AND btrim(s->>'text') <> ''
)
SELECT
    'ati_not_in_gold' AS diff_type,
    a.identifier,
    a.verse_num,
    a.label,
    a.span_text
FROM ati_mentions a
LEFT JOIN gold_mentions g
  ON a.identifier = g.identifier
 AND a.verse_num = g.verse_num
 AND a.label = g.label
 AND a.span_text = g.span_text
WHERE g.identifier IS NULL

UNION ALL

SELECT
    'gold_not_in_ati' AS diff_type,
    g.identifier,
    g.verse_num,
    g.label,
    g.span_text
FROM gold_mentions g
LEFT JOIN ati_mentions a
  ON g.identifier = a.identifier
 AND g.verse_num = a.verse_num
 AND g.label = a.label
 AND g.span_text = a.span_text
WHERE a.identifier IS NULL

ORDER BY diff_type, identifier, verse_num, label, span_text;
