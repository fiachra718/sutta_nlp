-- Build a focused NER regression set from known trouble patterns.
--
-- Usage:
--   psql -d tipitaka -f graph/entities/select_ner_regression_verses.sql \
--     > graph/entities/ner_regression_verses.txt
--
-- Output columns:
--   reason, identifier, verse_num, route_verse_num, predict_url

WITH targets(label, needle) AS (
  VALUES
    ('LOC', 'knower ('),
    ('LOC', 'mango grove.'),
    ('PERSON', 'apparently'),
    ('PERSON', 'void'),
    ('PERSON', 'master''s'),
    ('PERSON', 'long'),
    ('LOC', 'stream-winner'),
    ('GPE', 'yama'),
    ('PERSON', 'ratthapala''s'),
    ('PERSON', 'nalanda'),
    ('PERSON', 'elder'),
    ('LOC', 'tathagatha'),
    ('LOC', 'supreme'),
    ('PERSON', 'awakened'),
    ('LOC', 'squirrels'' sanctuary.'),
    ('LOC', '... &'),
    ('LOC', 'brahma'),
    ('PERSON', 'arahant')
),
bad_target_hits AS (
  SELECT DISTINCT
    v.id AS verse_id,
    'target_mismatch:' || t.label || ':' || t.needle AS reason
  FROM ati_verses v
  CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) ent
  JOIN targets t
    ON t.label = ent->>'label'
   AND lower(unaccent(trim(ent->>'text'))) = lower(unaccent(t.needle))
),
explicit_focus AS (
  -- Handpicked verses from recent debugging threads.
  SELECT v.id AS verse_id, 'focus:sn12_011_012_related'::text AS reason
  FROM ati_verses v
  WHERE v.identifier IN ('sn12.011.than.html', 'sn12.012.than.html', 'sn12.011.nypo.html', 'sn12.012.nypo.html')

  UNION ALL

  SELECT v.id AS verse_id, 'focus:dn16_vaji_route_index'::text AS reason
  FROM ati_verses v
  WHERE v.identifier = 'dn.16.1-6.vaji.html'
    AND v.verse_num IN (1, 7, 9)

  UNION ALL

  SELECT v.id AS verse_id, 'focus:dhp_route_index'::text AS reason
  FROM ati_verses v
  WHERE v.identifier = 'dhp.19.budd.html'
),
candidate_set AS (
  SELECT * FROM bad_target_hits
  UNION ALL
  SELECT * FROM explicit_focus
),
candidate_ids AS (
  SELECT DISTINCT verse_id FROM candidate_set
),
candidate_identifiers AS (
  SELECT DISTINCT v.identifier
  FROM ati_verses v
  JOIN candidate_ids c ON c.verse_id = v.id
),
verse_keys AS (
  SELECT
    v.id AS verse_id,
    v.identifier,
    v.verse_num,
    md5(regexp_replace(unaccent(lower(v.text)), '[^a-z0-9]+', ' ', 'g')) AS cleaned_hash,
    row_number() OVER (
      PARTITION BY v.identifier, md5(regexp_replace(unaccent(lower(v.text)), '[^a-z0-9]+', ' ', 'g'))
      ORDER BY v.verse_num, v.id
    ) AS occ_idx
  FROM ati_verses v
  JOIN candidate_identifiers ci ON ci.identifier = v.identifier
),
sutta_ordinals AS (
  SELECT
    s.identifier,
    ordinality - 1 AS route_verse_num,
    md5(regexp_replace(unaccent(lower(verse_elem->>'text')), '[^a-z0-9]+', ' ', 'g')) AS cleaned_hash,
    row_number() OVER (
      PARTITION BY s.identifier, md5(regexp_replace(unaccent(lower(verse_elem->>'text')), '[^a-z0-9]+', ' ', 'g'))
      ORDER BY ordinality
    ) AS occ_idx
  FROM ati_suttas s
  JOIN candidate_identifiers ci ON ci.identifier = s.identifier
  CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS t(verse_elem, ordinality)
),
route_map AS (
  SELECT
    vk.verse_id,
    vk.identifier,
    vk.verse_num,
    so.route_verse_num
  FROM verse_keys vk
  JOIN sutta_ordinals so
    ON so.identifier = vk.identifier
   AND so.cleaned_hash = vk.cleaned_hash
   AND so.occ_idx = vk.occ_idx
)
SELECT DISTINCT
  c.reason,
  rm.identifier,
  rm.verse_num,
  rm.route_verse_num,
  'http://127.0.0.1:5000/predict/verse/' || rm.identifier || '/' || rm.route_verse_num AS predict_url
FROM candidate_set c
JOIN route_map rm
  ON rm.verse_id = c.verse_id
ORDER BY rm.identifier, rm.verse_num, c.reason;
