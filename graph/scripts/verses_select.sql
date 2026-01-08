\copy (
  SELECT
    s.identifier,
    CASE
      WHEN s.nikaya IN ('AN','SN') THEN
        s.nikaya || ' ' || s.book_number || '.' || s.vagga
      WHEN s.nikaya IN ('MN','DN') THEN
        s.nikaya || ' ' || s.book_number
      WHEN s.nikaya = 'KN' THEN
        s.vagga || ' ' || s.book_number
      ELSE
        s.nikaya || ' ' || s.book_number
    END AS sutta_ref,
    v.verse_num,
    v.verse_text
  FROM ati_suttas AS s
  CROSS JOIN LATERAL (
    SELECT
      t.elem->>'text' AS verse_text,
      t.ord::int      AS verse_num
    FROM jsonb_array_elements(s.verses) WITH ORDINALITY AS t(elem, ord)
  ) AS v
  WHERE s.doc_type = 'sutta'
  ORDER BY s.nikaya, s.book_number, v.verse_num
) TO '/tmp/verses.csv'
  WITH (FORMAT csv, HEADER true)