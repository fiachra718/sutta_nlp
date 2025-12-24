-- missing DN entities
SELECT
  v.id,
  ve.mention,
  e->>'label' AS label,
  e->>'text'  AS text
FROM ati_verses v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS e
LEFT JOIN ati_verse_person ve
  ON ve.verse_id = v.id
WHERE v.nikaya = 'DN'
  AND v.book_number = 2
  AND e->>'label' = 'PERSON';

-- actual DN entities
  SELECT
  v.id,
  v.verse_num,
  e->>'text'  AS span_text,
  e->>'label' AS span_label,
  (e->>'start')::int AS span_start,
  (e->>'end')::int   AS span_end,
  ve.mention         AS ve_mention,
  ve.normalized      AS ve_normalized,
  ve.start_pos       AS ve_start,
  ve.end_pos         AS ve_end
FROM ati_verses v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS e
LEFT JOIN ati_verse_person ve
  ON  ve.verse_id = v.id
  AND ve.start_pos = (e->>'start')::int
  AND ve.end_pos   = (e->>'end')::int
WHERE v.nikaya = 'DN'
  AND v.book_number = 2
  AND e->>'label' = 'PERSON'
ORDER BY v.id, v.verse_num, span_start;