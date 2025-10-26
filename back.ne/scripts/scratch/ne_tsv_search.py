#!/usr/bin/env python3
# multi_term_hits.py
import sys, json, re, argparse
import psycopg
from psycopg.rows import dict_row

# --- Try to import your gazetteers to build a label map ---
LABEL_MAP = {}
try:
    # adjust module path if needed; this assumes you put the gazetteers here
    from ne_trainer_v2 import PERSON_GAZETTEER, GPE_GAZETTEER, LOC_GAZETTEER
    for lbl, phrase in PERSON_GAZETTEER:
        LABEL_MAP[phrase.lower()] = "PERSON"
    for lbl, phrase in GPE_GAZETTEER:
        LABEL_MAP[phrase.lower()] = "GPE"
    for lbl, phrase in LOC_GAZETTEER:
        LABEL_MAP[phrase.lower()] = "LOC"
except Exception:
    # okay to proceed without; we'll use a heuristic/default
    PERSON_GAZETTEER = GPE_GAZETTEER = LOC_GAZETTEER = []  # noqa


def spans_from_headline(hl: str):
    """Return list of (start,end) offsets in the ORIGINAL text given an
    hl string that is the original text with << and >> inserted around hits."""
    spans, i, pos, start = [], 0, 0, None
    while i < len(hl):
        if hl.startswith("<<", i):
            start = pos; i += 2
        elif hl.startswith(">>", i):
            if start is not None:
                spans.append((start, pos))
                start = None
            i += 2
        else:
            i += 1; pos += 1
    return spans


def merge_close_spans(spans, text, max_gap=3):
    """
    Merge spans if only a tiny separator (e.g., "'s ", whitespace, punctuation)
    lies between them. Useful for "Jeta's Grove" where headline marks Jeta and Grove separately.
    """
    if not spans:
        return []
    merged = [list(spans[0])]
    for s, e in spans[1:]:
        last_s, last_e = merged[-1]
        gap = s - last_e
        sep = text[last_e:s]
        if gap <= max_gap and re.fullmatch(r"[^\w]{,3}", sep or ""):
            merged[-1][1] = e
        else:
            merged.append([s, e])
    return [tuple(x) for x in merged]


def infer_label(term: str, default_label: str = "PERSON") -> str:
    """Pick a label for the term using gazetteer map or a light heuristic."""
    t = term.lower()
    if t in LABEL_MAP:
        return LABEL_MAP[t]
    # lightweight heuristic: common location words → LOC
    if any(k in t for k in [" grove", " park", "monaster", "wood", "forest", "peak", "hall", "river", "mango"]):
        return "LOC"
    # some city/region hints → GPE
    if any(k in t for k in ["savatthi", "sāvatthī", "rajagaha", "vesali", "kosala", "magadha", "videha", "kuru", "vajji"]):
        return "GPE"
    return default_label


SQL_MULTI = """
WITH terms(term) AS (
  SELECT unnest(%(terms)s::text[])
),
q AS (
  SELECT term,
         /* use websearch_to_tsquery w/ quotes for phrase mode, else plainto_tsquery */
         CASE WHEN %(phrase)s THEN websearch_to_tsquery(%(cfg)s::regconfig, '"' || term || '"')
              ELSE plainto_tsquery(%(cfg)s::regconfig, term)
         END AS tsq
  FROM terms
),
para AS (
  SELECT
    s.id              AS sutta_id,
    s.nikaya,
    s.identifier,
    s.title,
    e.ord::int        AS para_seq,
    (e.elem->>'text') AS ptext
  FROM ati_suttas s
  CROSS JOIN LATERAL jsonb_array_elements(s.verses) WITH ORDINALITY AS e(elem, ord)
  WHERE s.nikaya = ANY(%(nikayas)s)
    AND char_length(e.elem->>'text') >= %(min_chars)s
),
hits AS (
  SELECT
    q.term,
    p.sutta_id, p.nikaya, p.identifier, p.title, p.para_seq, p.ptext,
    h.hl,
    regexp_count(h.hl, '<<')          AS hits,
    NULLIF(strpos(h.hl, '<<'), 0) - 1 AS first_offset,
    row_number() OVER (
      PARTITION BY q.term
      ORDER BY regexp_count(h.hl, '<<') DESC, p.nikaya, p.identifier, p.para_seq
    ) AS rn
  FROM q
  JOIN para p
    ON to_tsvector(%(cfg)s::regconfig, p.ptext) @@ q.tsq
  CROSS JOIN LATERAL (
    SELECT ts_headline(
             %(cfg)s::regconfig,
             p.ptext,
             q.tsq,
             'StartSel=<<, StopSel=>>, HighlightAll=TRUE, MaxFragments=100000, MaxWords=100000, MinWords=1'
           ) AS hl
  ) AS h
)
SELECT term, sutta_id, nikaya, identifier, title, para_seq, ptext, hl, hits, first_offset
FROM hits
WHERE rn <= %(k)s
ORDER BY term, hits DESC, nikaya, identifier, para_seq;
"""


def main():
    ap = argparse.ArgumentParser(description="Fetch per-term paragraph hits and emit spaCy JSONL with merged spans.")
    ap.add_argument("terms", nargs="+", help="Search terms (names/places).")
    ap.add_argument("--dsn", default="postgresql://localhost/tipitaka?user=alee", help="Postgres DSN")
    ap.add_argument("-k", type=int, default=12, help="Samples per term (top K).")
    ap.add_argument("--nikayas", nargs="+", default=["MN", "SN", "AN"], help="Filter nikayas (default MN SN AN).")
    ap.add_argument("--min-chars", type=int, default=300, help="Min paragraph length.")
    ap.add_argument("--cfg", default="english", help="TSVector config (english/simple/etc).")
    ap.add_argument("--phrase", action="store_true", help="Treat each term as a quoted phrase query.")
    ap.add_argument("--out", default="gold_samples.jsonl", help="Output JSONL path.")
    ap.add_argument("--default-label", default="PERSON", help="Fallback label if term not in gazetteer.")
    ap.add_argument("--merge-gap", type=int, default=3, help="Max chars between hits to merge (for multiword names).")
    args = ap.parse_args()

    params = {
        "terms": args.terms,
        "k": args.k,
        "nikayas": args.nikayas,
        "min_chars": args.min_chars,
        "cfg": args.cfg,
        "phrase": True if args.phrase else False,
    }

    with psycopg.connect(args.dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(SQL_MULTI, params)
            rows = cur.fetchall()

    # Build JSONL for spaCy training
    n_examples = 0
    with open(args.out, "w", encoding="utf-8") as f:
        for r in rows:
            text = r["ptext"]
            hl = r["hl"]
            if not hl:
                continue

            # raw spans from << >>
            raw_spans = spans_from_headline(hl)

            # merge spans only if the term looks like a phrase (space/apostrophe/hyphen) or if multiple spans appear
            is_phrase = bool(re.search(r"[ '\-]", r["term"]))
            spans = merge_close_spans(raw_spans, text, max_gap=args.merge_gap) if is_phrase else raw_spans

            # assign a label for THIS term
            label = infer_label(r["term"], default_label=args.default_label)

            ents = [(s, e, label) for (s, e) in spans if 0 <= s < e <= len(text)]
            if not ents:
                continue

            rec = {"text": text, "entities": ents,
                   "meta": {
                       "term": r["term"],
                       "nikaya": r["nikaya"],
                       "identifier": r["identifier"],
                       "title": r["title"],
                       "para_seq": r["para_seq"],
                       "hits": r["hits"],
                       "first_offset": r["first_offset"],
                   }}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n_examples += 1

    print(f"Wrote {n_examples} examples to {args.out}")


if __name__ == "__main__":
    main()