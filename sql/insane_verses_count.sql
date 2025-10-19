WITH q AS (
  SELECT plainto_tsquery('english', 'contemplatives') AS tsq
)
SELECT
  s.identifier,
  s.title,
  SUM(regexp_count(h.hl, '<<')) AS hits
FROM ati_suttas s
JOIN verses p ON p.sutta_id = s.id
JOIN q ON to_tsvector('english', p.body) @@ q.tsq
CROSS JOIN LATERAL (
  SELECT ts_headline(
           'english',
           p.body,
           q.tsq,
           'StartSel=<<, StopSel=>>, HighlightAll==TRUE, MaxFragments=100000, MaxWords=100000, MinWords=1'
         ) AS hl
) AS h
GROUP BY s.identifier, s.title
ORDER BY hits DESC, s.identifier;



WITH q AS (
  SELECT plainto_tsquery('english', 'contemplatives') AS tsq
)
SELECT
  s.identifier,
  s.title,
  /* PG15+: simpler with regexp_count */
  regexp_count(h.hl, '<<') AS hits
FROM ati_suttas s
JOIN q ON s.tsv @@ q.tsq
CROSS JOIN LATERAL (
  SELECT ts_headline(
           'english',
           /* replace this with your body field or a concat helper */
           jsonb_texts_concat(s.verses),
           q.tsq,
           /* show the whole text and mark every hit */
           'StartSel=<<, StopSel=>>, HighlightAll=TRUE, MaxFragments=100000, MaxWords=100000, MinWords=1'
         ) AS hl
) AS h
ORDER BY hits DESC, s.identifier;


WITH q AS (
  SELECT plainto_tsquery('english', 'contemplatives') AS tsq
)
SELECT
  s.nikaya,
  s.identifier,
  s.title,
  COUNT(*) AS matched_paragraphs,
  SUM((length(h.hl) - length(replace(h.hl, '<<', ''))) / 2) AS total_hits
FROM ati_suttas s
JOIN ati_sutta_body b ON b.identifier = s.identifier
JOIN q ON to_tsvector('english', b.body) @@ q.tsq
CROSS JOIN LATERAL (
  SELECT ts_headline(
           'english',
           b.body,
           q.tsq,
           'StartSel=<<, StopSel=>>, HighlightAll=TRUE, MaxFragments=100000'
         ) AS hl
) AS h
GROUP BY s.nikaya, s.identifier, s.title
ORDER BY total_hits DESC;