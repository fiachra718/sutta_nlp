WITH gold_spans AS (
    SELECT DISTINCT
        lower(btrim(s->>'text')) AS span,
        s->>'label'              AS label
    FROM gold_training gt
    CROSS JOIN LATERAL jsonb_array_elements(gt.spans) AS s
    WHERE btrim(s->>'text') <> ''
),
ati_spans AS (
    SELECT DISTINCT
        lower(btrim(s->>'text')) AS span,
        s->>'label'              AS label
    FROM ati_verses av
    CROSS JOIN LATERAL jsonb_array_elements(av.ner_span) AS s
    WHERE btrim(s->>'text') <> ''
)
SELECT
    'ati_not_in_gold' AS diff_type,
    a.label,
    a.span
FROM ati_spans a
LEFT JOIN gold_spans g
  ON a.span = g.span AND a.label = g.label
WHERE g.span IS NULL

UNION ALL

SELECT
    'gold_not_in_ati' AS diff_type,
    g.label,
    g.span
FROM gold_spans g
LEFT JOIN ati_spans a
  ON g.span = a.span AND g.label = a.label
WHERE a.span IS NULL

ORDER BY diff_type, label, span;


WITH gold_norp AS (
    SELECT
        lower(btrim(s->>'text')) AS norp,
        COUNT(*) AS gold_freq
    FROM gold_training gt
    CROSS JOIN LATERAL jsonb_array_elements(gt.spans) AS s
    WHERE s->>'label' = 'NORP'
      AND btrim(s->>'text') <> ''
    GROUP BY 1
),
ati_norp AS (
    SELECT
        lower(btrim(s->>'text')) AS norp,
        COUNT(*) AS ati_freq
    FROM ati_verses av
    CROSS JOIN LATERAL jsonb_array_elements(av.ner_span) AS s
    WHERE s->>'label' = 'NORP'
      AND btrim(s->>'text') <> ''
    GROUP BY 1
)
SELECT
    COALESCE(a.norp, g.norp) AS norp,
    g.gold_freq,
    a.ati_freq,
    CASE
      WHEN g.norp IS NULL THEN 'ati_not_in_gold'
      WHEN a.norp IS NULL THEN 'gold_not_in_ati'
    END AS diff_type
FROM gold_norp g
FULL OUTER JOIN ati_norp a USING (norp)
WHERE g.norp IS NULL OR a.norp IS NULL
ORDER BY COALESCE(a.ati_freq, 0) DESC,
         COALESCE(g.gold_freq, 0) DESC,
         norp;
