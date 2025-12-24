LOAD CSV WITH HEADERS
FROM 'file:///suttas.csv' AS row

MERGE (s:Sutta {uid: row.id})
SET
  s.nikaya          = row.nikaya,
  s.vagga           = row.vagga,
  s.book_number     = CASE
                        WHEN row.book_number IS NULL OR row.book_number = '' THEN null
                        ELSE toInteger(row.book_number)
                      END,
  s.translator      = row.translator,
  s.title           = row.title,
  s.subtitle        = row.subtitle,
  s.canonical_title = row.canonical_title;