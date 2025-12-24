COPY (
  SELECT
    p.id,
    p.canonical,
    coalesce(
      json_agg(a.alias) FILTER (WHERE a.alias IS NOT NULL),
      '[]'::json
    ) AS aliases
  FROM ati_person AS p
  LEFT JOIN ati_entity_aliases AS a
    ON a.entity_id = p.id
  GROUP BY
    p.id,
    p.canonical
  ORDER BY
    p.canonical
) TO '/Users/alee/sutta_nlp/graph/entities/people.csv'
  WITH (FORMAT csv, HEADER true);