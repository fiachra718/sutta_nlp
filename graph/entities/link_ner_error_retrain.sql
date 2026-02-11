WITH verse_keys AS (
  SELECT
    v.id AS verse_id,
    v.identifier,
    md5(regexp_replace(unaccent(lower(v.text)), '[^a-z0-9]+', ' ', 'g')) AS cleaned_hash,
    row_number() OVER (
      PARTITION BY v.identifier, md5(regexp_replace(unaccent(lower(v.text)), '[^a-z0-9]+', ' ', 'g'))
      ORDER BY v.verse_num, v.id
    ) AS occ_idx
  FROM ati_verses v
),
verse_ordinals AS (
  SELECT
    s.identifier,
    ordinality - 1 AS route_verse_num,
    md5(regexp_replace(unaccent(lower(verse_elem->>'text')), '[^a-z0-9]+', ' ', 'g')) AS cleaned_hash,
    row_number() OVER (
      PARTITION BY s.identifier, md5(regexp_replace(unaccent(lower(verse_elem->>'text')), '[^a-z0-9]+', ' ', 'g'))
      ORDER BY ordinality
    ) AS occ_idx
  FROM ati_suttas s
  CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS t(verse_elem, ordinality)
),
bad AS (
  -- One row per matched bad NER span for manual cleanup.
  SELECT
    v.id AS verse_id,
    v.identifier,
    v.verse_num,
    t.label AS target_label,
    t.needle AS target_text,
    ent->>'label' AS found_label,
    ent->>'text' AS found_text
  FROM ati_verses v
  CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) ent
  JOIN (
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
  ) AS t(label, needle)
    ON t.label = ent->>'label'
   AND lower(unaccent(trim(ent->>'text'))) = lower(unaccent(t.needle))
)
SELECT
  bad.verse_id,
  bad.identifier,
  bad.verse_num,
  bad.target_label,
  bad.target_text,
  bad.found_label,
  bad.found_text,
  vo.route_verse_num,
  'http://127.0.0.1:5000/predict/verse/' || bad.identifier || '/' || vo.route_verse_num AS predict_url
FROM bad
JOIN verse_keys vk
  ON vk.verse_id = bad.verse_id
JOIN verse_ordinals vo
  ON vo.identifier = vk.identifier
 AND vo.cleaned_hash = vk.cleaned_hash
 AND vo.occ_idx = vk.occ_idx
ORDER BY bad.identifier, bad.verse_num, bad.found_label, bad.found_text;
