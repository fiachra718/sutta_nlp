-- Prefer verse-level pool to avoid re-sampling paragraphs you already tagged
WITH pool AS (
  SELECT v.identifier, v.verse_num, v.text, v.text_hash
  FROM verses v
  WHERE NOT EXISTS (SELECT 1 FROM seen_verses s WHERE s.text_hash = v.text_hash)
    AND NOT EXISTS (SELECT 1 FROM gold_training g WHERE g.text_hash = v.text_hash)
)
SELECT *
FROM pool
ORDER BY gen_random_uuid()
LIMIT 50;

INSERT INTO seen_verses(text_hash)
SELECT text_hash FROM (VALUES (...),(...)) AS t(text_hash)
ON CONFLICT (text_hash)
DO UPDATE SET last_seen_at = now(), times_seen = seen_verses.times_seen + 1;
