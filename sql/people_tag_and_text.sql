 select identifier, 
 	nikaya, 
 	book_number, 
 	vagga, 
 	text, 
 	ner_span, 
 	ent->>'label' as label, 
 	ent->>'text' as entity 
 from ati_verses 
 	cross join lateral 
 	jsonb_array_elements(ner_span) as ent 
 where ent->>'label' = 'PERSON' and ent->>'text' = 'Absorbed';


SELECT
    v.identifier,
    v.nikaya,
    v.book_number,
    v.vagga,
    ent_gpe->>'label' AS label,
    ent_gpe->>'text'  AS entity
FROM ati_verses v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS ent_gpe
WHERE ent_gpe->>'label' = 'GPE'
  AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(v.ner_span) AS ent_p
      WHERE ent_p->>'label' = 'PERSON'
        AND ent_p->>'text' IN (
          'Ven. Mahā Kassapa',
          'Ven. Maha Kassapa',
          'Kassapa'
        )
  )
ORDER BY v.identifier, entity;
 

 select identifier, 
 	nikaya, 
 	book_number, 
 	vagga, 
 	text, 
 	ner_span, 
 	ent->>'label' as label, 
 	ent->>'text' as entity 
 from ati_verses 
 	cross join lateral 
 	jsonb_array_elements(ner_span) as ent 
 where ent->>'label' = 'LOC' and ent->>'text' = 'Qualities of the';



SELECT
  ent->>'text'  AS text,
  ent->>'label' AS label,
  COUNT(*)      AS freq
FROM ati_verses v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS ent
WHERE ent->>'label' = 'GPE'
GROUP BY ent->>'text', ent->>'label'
ORDER BY freq DESC;