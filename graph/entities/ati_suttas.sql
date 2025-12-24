COPY (
    SELECT
      s.id,
      s.identifier,
      s.nikaya,
      s.vagga,
      s.book_number,
      s.translator,
      s.title,
      s.subtitle,
      CASE
        -- SN / AN: e.g. "SN 12.1: Connected Discourses on ..."
        WHEN s.nikaya IN ('SN','AN') THEN
          format('%s %s.%s: %s', s.nikaya, s.vagga, s.book_number, s.title)

        -- KN: use collection name from vagga as you’d asked before
        WHEN s.nikaya = 'KN' THEN
          format('%s %s: %s', s.vagga, coalesce(s.book_number::text, ''), s.title)

        -- DN, MN, etc: e.g. "MN 143: The Full-Moon Night..."
        ELSE
          format('%s %s: %s', s.nikaya, s.vagga, s.title)
      END AS canonical_title
    FROM ati_suttas s
    ORDER BY s.nikaya, s.vagga, s.book_number
) TO '/Users/alee/sutta_nlp/graph/entities/suttas.csv'
  WITH (FORMAT csv, HEADER true);