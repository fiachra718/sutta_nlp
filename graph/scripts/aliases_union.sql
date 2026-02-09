WITH base AS (
    SELECT
        e.id AS entity_id,
        e.entity_type,
        e.canonical AS alias_raw,
        e.normalized AS alias_norm
    FROM ati_entities e

    UNION ALL

    SELECT
        e.id AS entity_id,
        e.entity_type,
        e.normalized AS alias_raw,
        e.normalized AS alias_norm
    FROM ati_entities e

    UNION ALL

    SELECT
        ea.entity_id,
        e.entity_type,
        ea.alias AS alias_raw,
        COALESCE(ea.normalized, ea.alias) AS alias_norm
    FROM ati_entity_aliases ea
    JOIN ati_entities e ON e.id = ea.entity_id
)
SELECT
    entity_id,
    entity_type,
    alias_raw,
    alias_norm
FROM base
WHERE alias_norm IS NOT NULL
  AND btrim(alias_norm) <> '';