-- Pali Names crawl + parsed entry storage
-- Safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS pali_names_pages (
  id BIGSERIAL PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  path TEXT NOT NULL,
  title TEXT,
  text TEXT NOT NULL,
  text_sha256 TEXT,
  fetched_at_utc TIMESTAMPTZ,
  visited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_run TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pali_names_pages_path_idx
  ON pali_names_pages (path);

CREATE INDEX IF NOT EXISTS pali_names_pages_title_trgm_idx
  ON pali_names_pages USING gin (title gin_trgm_ops);

CREATE TABLE IF NOT EXISTS pali_names_entries (
  id BIGSERIAL PRIMARY KEY,
  page_id BIGINT NOT NULL REFERENCES pali_names_pages(id) ON DELETE CASCADE,
  entry_ord INTEGER NOT NULL,
  entry_name TEXT NOT NULL,
  entry_name_norm TEXT NOT NULL,
  entry_text TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (page_id, entry_ord)
);

CREATE INDEX IF NOT EXISTS pali_names_entries_name_norm_idx
  ON pali_names_entries (entry_name_norm);

CREATE INDEX IF NOT EXISTS pali_names_entries_name_trgm_idx
  ON pali_names_entries USING gin (entry_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS pali_names_entries_text_trgm_idx
  ON pali_names_entries USING gin (entry_text gin_trgm_ops);

CREATE TABLE IF NOT EXISTS pali_names_see_also (
  id BIGSERIAL PRIMARY KEY,
  entry_id BIGINT NOT NULL REFERENCES pali_names_entries(id) ON DELETE CASCADE,
  raw_ref TEXT NOT NULL,
  raw_ref_norm TEXT NOT NULL,
  resolved_page_id BIGINT REFERENCES pali_names_pages(id) ON DELETE SET NULL,
  resolved_url TEXT,
  resolution_method TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (entry_id, raw_ref_norm)
);

CREATE INDEX IF NOT EXISTS pali_names_see_also_ref_norm_idx
  ON pali_names_see_also (raw_ref_norm);

CREATE INDEX IF NOT EXISTS pali_names_see_also_resolved_page_idx
  ON pali_names_see_also (resolved_page_id);

CREATE TABLE IF NOT EXISTS pali_names_citations (
  id BIGSERIAL PRIMARY KEY,
  entry_id BIGINT NOT NULL REFERENCES pali_names_entries(id) ON DELETE CASCADE,
  raw_citation TEXT NOT NULL,
  citation_norm TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (entry_id, citation_norm)
);

CREATE INDEX IF NOT EXISTS pali_names_citations_norm_idx
  ON pali_names_citations (citation_norm);

CREATE OR REPLACE VIEW v_pali_names_lookup AS
SELECT
  e.id AS entry_id,
  p.url AS page_url,
  p.title AS page_title,
  e.entry_ord,
  e.entry_name,
  e.entry_name_norm,
  e.entry_text,
  sa.raw_ref AS see_also_raw,
  sa.resolved_url AS see_also_url,
  sa.resolution_method AS see_also_resolution
FROM pali_names_entries e
JOIN pali_names_pages p ON p.id = e.page_id
LEFT JOIN pali_names_see_also sa ON sa.entry_id = e.id;

CREATE OR REPLACE VIEW v_pali_names_citations AS
SELECT
  e.id AS entry_id,
  e.entry_name,
  e.entry_name_norm,
  c.raw_citation,
  c.citation_norm,
  p.url AS page_url
FROM pali_names_citations c
JOIN pali_names_entries e ON e.id = c.entry_id
JOIN pali_names_pages p ON p.id = e.page_id;
