COPY (
  SELECT
    v.identifier        AS sutta_uid,       -- sutta-level ID
    v.nikaya,
    v.book_number,
    v.vagga,
    vp.entity_id        AS person_id,
    e.canonical         AS person_name,
    COUNT(*)            AS mention_count
  FROM ati_verse_person vp
  JOIN ati_verses   v ON v.id = vp.verse_id
  JOIN ati_entities e ON e.id = vp.entity_id
  WHERE e.entity_type = 'PERSON'
  GROUP BY
    v.identifier,
    v.nikaya,
    v.book_number,
    v.vagga,
    vp.entity_id,
    e.canonical
  ORDER BY
    v.identifier,
    person_name
) TO '/Users/alee/sutta_nlp/graph/entities/sutta_person_counts.csv'
  WITH (FORMAT csv, HEADER true);