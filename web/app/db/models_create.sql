-- -- Canonical verse registry (or materialize from ati_suttas + ordinality)
-- CREATE TABLE IF NOT EXISTS verses (
--   identifier   text NOT NULL,
--   verse_num    int  NOT NULL,
--   text         text NOT NULL,
--   text_hash    text NOT NULL,
--   PRIMARY KEY (identifier, verse_num),
--   UNIQUE (text_hash)  -- optional, if you want global dedupe by text
-- );

-- -- “Seen” registry to avoid repeats in UI pulls
-- CREATE TABLE IF NOT EXISTS seen_verses (
--   text_hash     text PRIMARY KEY,
--   first_seen_at timestamptz DEFAULT now(),
--   last_seen_at  timestamptz DEFAULT now(),
--   times_seen    int DEFAULT 1
-- );

-- Candidates (machine suggestions captured for later editing)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS candidates (
  id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_identifier  text,
  source_verse_num   int,
  text               text NOT NULL,
  text_hash          text NOT NULL UNIQUE,
  entities           jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at         timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_candidates_entities_gin ON candidates USING GIN (entities);
CREATE INDEX IF NOT EXISTS idx_candidates_text_hash ON candidates(text_hash);


-- Gold training examples
CREATE TABLE IF NOT EXISTS gold_training (
  id              text PRIMARY KEY,
  text            text NOT NULL,
  text_hash       text NOT NULL,
  spans           jsonb NOT NULL,        -- validated spans
  spans_hash     text      NOT NULL,     -- hash of canonical span list
  source         text,                   -- "candidate" | "manual" | "import"
  source_identifier text,
  source_verse_num  int,
  source_meta       jsonb,
  from_file      text,
  created_at      timestamptz DEFAULT now(),
  UNIQUE (text_hash, spans_hash)
);

CREATE INDEX IF NOT EXISTS idx_gold_text_hash       ON gold_training(text_hash);
CREATE INDEX IF NOT EXISTS idx_gold_spans_gin ON gold_training USING GIN (spans);
CREATE INDEX IF NOT EXISTS idx_gold_source_ref ON gold_training(source_identifier, source_verse_num);

ALTER TABLE gold_training
  ADD COLUMN IF NOT EXISTS source_identifier text;
ALTER TABLE gold_training
  ADD COLUMN IF NOT EXISTS source_verse_num int;
ALTER TABLE gold_training
  ADD COLUMN IF NOT EXISTS source_meta jsonb;


