\copy (
  SELECT
    v.identifier,
    CASE
      WHEN v.nikaya IN ('AN','SN') THEN v.nikaya || ' ' || v.book_number || '.' || v.vagga
      WHEN v.nikaya IN ('MN','DN') THEN v.nikaya || ' ' || v.book_number
      WHEN v.nikaya = 'KN'          THEN v.vagga || ' ' || v.book_number
      ELSE                               v.nikaya || ' ' || v.book_number
    END AS sutta_ref,
    v.verse_num,
    v.text,
    v.ner_span
  FROM ati_verses AS v
  WHERE length(v.text) > 128
    AND v.ner_span IS NOT NULL
    AND jsonb_array_length(v.ner_span) > 0
    ORDER BY v.nikaya, v.book_number,
         nullif(regexp_replace(v.verse_num::text, '\D.*$', ''), '')::int
) TO '/tmp/verses.csv' WITH CSV HEADER;



SELECT
  v.gid,
  v.verse_num,
CASE
  WHEN v.nikaya IN ('AN','SN')
    THEN COALESCE(v.nikaya,'') || ' ' || COALESCE(v.book_number::text,'') || '.' || COALESCE(v.vagga::text,'')
  WHEN v.nikaya IN ('MN','DN')
    THEN COALESCE(v.nikaya,'') || ' ' || COALESCE(v.book_number::text,'')
  WHEN v.gid LIKE 'dhp.%'
    THEN 'Dhp ' || COALESCE(v.book_number::text, split_part(v.gid, '.', 2))
  WHEN v.nikaya = 'KN'
    THEN concat_ws(' ', v.vagga, v.book_number::text)
  ELSE
    COALESCE(v.nikaya,'') || ' ' || COALESCE(v.book_number::text,'')
END 
  as sutta_ref,
  v.verse_num,
    v.text,
    v.ner_span
FROM ati_verses AS v
  WHERE length(v.text) > 128
    AND v.ner_span IS NOT NULL
    AND jsonb_array_length(v.ner_span) > 0
    ORDER BY v.nikaya, v.book_number,
         nullif(regexp_replace(v.verse_num::text, '\D.*$', ''), '')::int