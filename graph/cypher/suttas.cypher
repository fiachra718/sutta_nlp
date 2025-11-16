LOAD CSV WITH HEADERS
FROM 'file:///Users/alee/sutta_nlp/graph/entities/suttas.csv' AS row
FIELDTERMINATOR ','
MERGE (s:Sutta {identifier: trim(row.identifier)})
SET  s.nikaya      = nullIf(trim(row.nikaya), ''),
     s.vagga       = nullIf(trim(row.vagga), ''),
     s.book_number = toIntegerOrNull(trim(row.book_number)),
     s.title       = nullIf(trim(row.title), ''),
     s.subtitle    = nullIf(trim(row.subtitle), '');