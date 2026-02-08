SELECT
    jsonb_strip_nulls(
        jsonb_build_object(
            'type', e.entity_type,
            'canonical_name', e.canonical,
            'normalized_name', e.normalized,
            'aliases',
            CASE
            WHEN COUNT(ea.alias) = 0 THEN NULL
            ELSE jsonb_agg(
                jsonb_build_object(
                    'canonical_alias', ea.alias,
                    'normalized_alias', ea.normalized
                    )
                )
            END
        )
    ) AS entity
FROM ati_entities e
    LEFT JOIN ati_entity_aliases ea
    ON ea.entity_id = e.id
WHERE e.entity_type IN ('GPE', 'NORP', 'LOC', 'PERSON')
GROUP BY e.id, e.canonical, e.normalized
ORDER BY e.canonical 