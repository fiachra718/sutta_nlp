-- 1) Get the exact, case-preserved terms: name + aliases
WITH terms AS (
  SELECT DISTINCT t AS term
  FROM people p,
       unnest(ARRAY[p.name] || coalesce(p.aliases, '{}')) AS t
  WHERE p.name = $1             -- e.g. 'King Ajatasattu'
),

-- 2) For each verse & each term, count matches in verse text
hits AS (
  SELECT
    s.identifier,
    CASE
      WHEN s.nikaya IN ('SN','AN')
        THEN format('%s %s.%s', s.nikaya, s.book_number, s.vagga)
      ELSE format('%s %s',     s.nikaya, s.book_number)
    END               AS ref,
    (v.elem->>'num')  AS verse_num,
    t.term            AS alias,
    -- count exact, case-sensitive, whole-word matches of alias in verse text
    (
      SELECT count(*)
      FROM regexp_matches(
        v.elem->>'text',
        '\m' || regexp_replace(t.term, '([.^$*+?()[{\|\\])', '\\\1', 'g') || '\M',
        'g'
      )
    ) AS count
  FROM ati_suttas s
  CROSS JOIN LATERAL jsonb_array_elements(s.verses) AS v(elem)
  JOIN terms t ON TRUE
)
SELECT
  identifier,
  ref,
  verse_num,
  alias,
  count
FROM hits
WHERE count > 0                      -- only verses where he actually appears
ORDER BY identifier, (verse_num)::int, alias;