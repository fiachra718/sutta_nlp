CREATE TABLE IF NOT EXISTS ati_related_links (
  id BIGSERIAL PRIMARY KEY,
  from_identifier TEXT NOT NULL,
  from_path TEXT NOT NULL,
  to_identifier TEXT NOT NULL,
  to_href TEXT NOT NULL DEFAULT '',
  to_ref_label TEXT NOT NULL DEFAULT '',
  source_kind TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0.5,
  baseline_jaccard REAL,
  baseline_weighted_jaccard REAL,
  baseline_cosine REAL,
  baseline_person_overlap INTEGER,
  baseline_person_union INTEGER,
  baseline_updated_at TIMESTAMPTZ,
  context TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ati_related_links_uni
ON ati_related_links (from_identifier, to_identifier, to_href, to_ref_label, source_kind);
