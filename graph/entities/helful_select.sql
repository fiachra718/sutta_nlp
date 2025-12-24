SELECT 
    v.id AS verse_id, v.ner_span, v.identifier, 
    ent->>'text' AS mention,
    CASE 
        WHEN s.nikaya IN ('SN','AN') 
            THEN s.nikaya || ' ' || s.vagga || '.' || s.book_number
        WHEN s.nikaya = 'KN'
            THEN s.nikaya || ' ' || s.vagga
        ELSE 
            s.nikaya || ' ' || s.book_number
    END AS canonical_sutta,
    v.text
FROM ati_verses v
JOIN ati_suttas s ON s.identifier = v.identifier
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS ent
WHERE ent->>'label' = 'GPE'
    AND ent->>'text' = 'Kammasadhamma'
  ORDER BY canonical_sutta, v.verse_num
  ;


  --------


SELECT 
    v.id, v.identifier, 
    ent->>'text' AS mention
    FROM ati_verses v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS ent
 WHERE ent->>'label' = 'GPE'
  AND ent->>'text' = 'Kammasadhamma'


 ++++++++++++++++
 

SELECT
    v.id, v.identifier, 
    ent->>'text' AS mention
    FROM ati_verses v
CROSS JOIN LATERAL jsonb_array_elements(v.ner_span) AS ent
 WHERE ent->>'label' = 'GPE'
  AND ent->>'text' IN ( 'Sāvatthī', 'Savatthi') ;
 