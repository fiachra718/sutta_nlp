-- Enable optional fuzzy matching extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Helper to concatenate verse text for FTS
CREATE OR REPLACE FUNCTION jsonb_verses_concat(j jsonb)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
AS $$
  SELECT COALESCE(string_agg(elem->>'text', ' '), '')
  FROM jsonb_array_elements(j) AS elem
$$;

-- Enum type for document categories
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'doc_type') THEN
    CREATE TYPE doc_type AS ENUM ('sutta','vinaya','abhidhamma','commentary','other');
  END IF;
END $$;

-- -------------------------------------------------------------------
-- Main table: ati_suttas
-- -------------------------------------------------------------------
CREATE TABLE ati_suttas (
  id                  BIGSERIAL PRIMARY KEY,
  identifier          TEXT NOT NULL UNIQUE,          -- e.g. 'mn.020.than.html'
  raw_path            TEXT NOT NULL,                 -- e.g. 'ati/tipitaka/mn/mn.020.than.html'

  nikaya              TEXT,                          -- MN, SN, DN, AN, KN, etc.
  vagga               TEXT,                          -- chapter/vagga
  book_number         TEXT,                          -- '1.2', 'III', etc.
  doc_type            doc_type NOT NULL DEFAULT 'sutta',

  translator          TEXT,
  copyright           TEXT,

  title               TEXT NOT NULL,
  subtitle            TEXT,

  alternative_translations JSONB DEFAULT '[]'::jsonb,
  verses              JSONB NOT NULL,                -- array of {num,text}

  notes               TEXT,

  -- Full-text search vector
  tsv TSVECTOR GENERATED ALWAYS AS (
    to_tsvector(
      'english',
      COALESCE(title,'') || ' ' ||
      COALESCE(subtitle,'') || ' ' ||
      jsonb_verses_concat(verses)
    )
  ) STORED,

  created_at          TIMESTAMPTZ DEFAULT now(),
  updated_at          TIMESTAMPTZ DEFAULT now()
);

-- -------------------------------------------------------------------
-- Triggers: maintain updated_at
-- -------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ati_suttas_touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER ati_suttas_set_updated_at
BEFORE UPDATE ON ati_suttas
FOR EACH ROW EXECUTE FUNCTION ati_suttas_touch_updated_at();

-- -------------------------------------------------------------------
-- Indexes
-- -------------------------------------------------------------------
CREATE INDEX ati_suttas_tsv_idx           ON ati_suttas USING gin (tsv);
CREATE INDEX ati_suttas_nikaya_idx        ON ati_suttas (nikaya);
CREATE INDEX ati_suttas_doc_type_idx      ON ati_suttas (doc_type);
CREATE INDEX ati_suttas_book_number_idx   ON ati_suttas (book_number);
CREATE INDEX ati_suttas_raw_path_trgm_idx ON ati_suttas USING gin (raw_path gin_trgm_ops);

-- -------------------------------------------------------------------
-- Notes table
-- -------------------------------------------------------------------
CREATE TABLE ati_notes (
  id          BIGSERIAL PRIMARY KEY,
  sutta_id    BIGINT NOT NULL REFERENCES ati_suttas(id) ON DELETE CASCADE,
  body        TEXT NOT NULL,
  created_by  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ati_notes_sutta_id_idx ON ati_notes (sutta_id);
