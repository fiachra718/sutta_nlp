CREATE TABLE ati_entity_aliases (
    id          bigserial PRIMARY KEY,
    entity_id   bigint NOT NULL REFERENCES ati_entities(id) ON DELETE CASCADE,
    alias       text   NOT NULL,
    normalized  text   NOT NULL,    -- normalize_pali(alias)
    source      text   NOT NULL,    -- 'manual', 'ner_seed', etc.
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_alias_norm
    ON ati_entity_aliases (normalized);