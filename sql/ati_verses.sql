WITH exploded AS (
  SELECT
    s.identifier,
    s.nikaya,
    s.book_number,
    s.vagga,
    -- Safely convert num like '1', '1.', '1a' → 1; skip if no leading digits
    CASE
      WHEN v.elem->>'num' ~ '^[0-9]+' THEN
        substring(v.elem->>'num' from '^[0-9]+')::int
      ELSE
        NULL
    END                       AS verse_num,
    v.elem->>'text'           AS verse_text,
    v.ord
  FROM ati_suttas AS s
  CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS v(elem, ord)
  WHERE s.nikaya IN ('MN', 'DN', 'SN', 'AN')
),
grouped AS (
  SELECT
    identifier,
    nikaya,
    book_number,
    vagga,
    verse_num,
    string_agg(verse_text, E'\n\n' ORDER BY ord) AS full_text
  FROM exploded
  WHERE verse_num IS NOT NULL          -- drop any weird ones with no leading digits
  GROUP BY identifier, nikaya, book_number, vagga, verse_num
)
INSERT INTO ati_verses (
  gid,
  identifier,
  nikaya,
  book_number,
  vagga,
  canon_ref,
  verse_num,
  text
)
SELECT
  identifier || ':' || verse_num                 AS gid,
  identifier,
  nikaya,
  CASE
    WHEN book_number ~ '^[0-9]+$' THEN book_number::int
    ELSE NULL
  END                                            AS book_number,
  vagga,
  CASE
    WHEN nikaya IN ('MN', 'DN') THEN
      format('%s.%s.%s', nikaya, book_number, verse_num)
    WHEN nikaya IN ('SN', 'AN') THEN
      format('%s.%s.%s.%s', nikaya, book_number, vagga, verse_num)
    ELSE
      NULL
  END                                            AS canon_ref,
  verse_num,
  full_text                                      AS text
FROM grouped;