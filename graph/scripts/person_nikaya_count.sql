WITH hits AS (
  SELECT
    s.nikaya,
    s.book_number::int AS book_number,
    COALESCE(s.vagga, '') AS vagga,
    v.ord AS verse_num
  FROM ati_suttas s
  CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS v(vj, ord)
  WHERE v.vj->>'text' ILIKE '%' || 'Ajita Kesakambalin' || '%'
)

SELECT
  CASE
    WHEN nikaya IN ('SN', 'AN') THEN nikaya || ' ' || vagga || ',' || book_number
    ELSE nikaya || ' ' || book_number
  END AS ref,
  COUNT(*) AS verse_mentions
FROM hits
GROUP BY ref
ORDER BY ref;