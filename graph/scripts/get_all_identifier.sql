SELECT  CASE WHEN s.nikaya IN ('SN','AN') THEN format('%s %s.%s,%s', s.nikaya, s.vagga, s.book_number, s.title) ELSE format('%s %s,%s', s.nikaya, s.book_number, s.title) END AS canonical_title FROM ati_suttas s ORDER BY s.nikaya, s.book_number ;

SELECT
  CASE
    WHEN s.nikaya IN ('SN','AN') THEN
      format('%s %s.%s,%s', s.nikaya, s.vagga, s.book_number, s.title)
    WHEN s.nikaya IN ('MN','DN') THEN
      format('%s %s,%s', s.nikaya, s.book_number, s.title)
    WHEN s.nikaya = 'KN' THEN
      -- KN needs identifier + collection (stored in vagga)
      format('%s — %s %s', s.identifier, s.vagga, s.book_number, s.title)
    ELSE
      -- fallback
      format('%s %s,%s', s.nikaya, coalesce(s.book_number, ''), s.title)
  END AS canonical_title
FROM ati_suttas s
ORDER BY
  CASE s.nikaya                  -- enforce Nikaya order
    WHEN 'DN' THEN 1
    WHEN 'MN' THEN 2
    WHEN 'SN' THEN 3
    WHEN 'AN' THEN 4
    WHEN 'KN' THEN 5
    ELSE 9
  END,
  -- For SN/AN, vagga is numeric: sort numerically when it is
  CASE
    WHEN s.nikaya IN ('SN','AN') AND s.vagga ~ '^\d+$' THEN s.vagga::int
    ELSE NULL
  END NULLS LAST,
  -- book_number numeric order when possible
  CASE
    WHEN s.book_number ~ '^\d+$' THEN s.book_number::int
    ELSE NULL
  END NULLS LAST,
  -- tie-breakers
  s.vagga,
  s.book_number,
  s.identifier;