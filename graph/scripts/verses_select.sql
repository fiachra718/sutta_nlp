\copy (
  SELECT
    v.identifier,
    CASE
      WHEN v.nikaya IN ('AN','SN') THEN
        v.nikaya || ' ' || v.book_number || '.' || v.vagga
      WHEN v.nikaya IN ('MN','DN') THEN
        v.nikaya || ' ' || v.book_number
      WHEN v.nikaya = 'KN' THEN
        v.vagga || ' ' || v.book_number
      ELSE
        v.nikaya || ' ' || v.book_number
    END AS sutta_ref,
    v.verse_num,
    v.text
  FROM ati_verses AS v
  CROSS JOIN LATERAL (
    SELECT
      t.elem->>'text' AS ner_text, t.elem->>'label' as ner_label
    FROM jsonb_array_elements(v.ner_span) WITH ORDINALITY AS t(elem, ord)
  ) AS n
  WHERE length(v.text) > 128 AND v.ner_span <> '[]'::jsonb
  ORDER BY v.nikaya, v.book_number, v.verse_num 
) TO '/tmp/verses.csv'
  WITH (FORMAT csv, HEADER true)