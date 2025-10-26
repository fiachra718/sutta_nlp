#!/usr/bin/env python3
import argparse, json, re, unicodedata
from pathlib import Path
from collections import defaultdict
import spacy
from spacy.training.iob_utils import offsets_to_biluo_tags

def nfc(s): return unicodedata.normalize("NFC", s or "")
def nfd(s): return unicodedata.normalize("NFD", s or "")

def strip_diacritics(s: str) -> str:
    nfkd = unicodedata.normalize("NFD", s)
    base = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return unicodedata.normalize("NFC", base)

def apostrophe_variants(s: str):
    # straight ↔ curly
    return sorted(set([s, s.replace("'", "’"), s.replace("’", "'")]))

def find_all_occurrences(hay: str, needle: str, case_sensitive=False, allow_diacritic_variants=True):
    """
    Return list of (start, end) for all non-overlapping needle hits in hay.
    If allow_diacritic_variants=True, also try diacritic-stripped + apostrophe variants.
    """
    spans = []

    hay_nfc = nfc(hay)
    candidates = [nfc(needle)]
    if allow_diacritic_variants:
        b = strip_diacritics(needle)
        candidates.extend(apostrophe_variants(nfc(needle)))
        candidates.extend(apostrophe_variants(b))

    # de-dup candidates, longer first to reduce overlaps
    cands = sorted(set([c for c in candidates if c]), key=len, reverse=True)

    for cand in cands:
        if not cand:
            continue
        if case_sensitive:
            start = 0
            while True:
                i = hay_nfc.find(cand, start)
                if i == -1: break
                spans.append((i, i + len(cand)))
                start = i + len(cand)
        else:
            # case-insensitive search with re, escaping cand
            pat = re.compile(re.escape(cand), flags=re.IGNORECASE)
            for m in pat.finditer(hay_nfc):
                spans.append((m.start(), m.end()))

    # merge duplicate/contained spans
    spans = sorted(set(spans))
    merged = []
    for s,e in spans:
        if merged and s >= merged[-1][0] and e <= merged[-1][1]:
            continue
        if merged and s < merged[-1][1] and e > merged[-1][1]:
            # overlapping; keep the longer one
            (ps,pe) = merged[-1]
            if (e - s) > (pe - ps):
                merged[-1] = (s,e)
            continue
        merged.append((s,e))
    return merged

def load_candidates_lines(path: Path):
    """
    Reads a file where each logical item is ONE line of text you want to annotate.
    If your file has 'Matched: ... at Line 18' style lines, you should instead
    feed a plain file with just the paragraph lines (use your earlier script’s raw paragraphs).
    """
    lines = []
    with path.open(encoding="utf-8") as f:
        for ln, raw in enumerate(f, start=1):
            text = raw.rstrip("\n")
            lines.append((ln, text))
    return lines

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidates", required=True, help="text file: one paragraph per line (NFC recommended)")
    ap.add_argument("--gold", required=True, help="gold manifest (json or yaml): per line number, phrases+labels")
    ap.add_argument("--out", required=True, help="output JSONL of gold examples")
    ap.add_argument("--label-default", default=None, help="optional default label if none provided for an item")
    ap.add_argument("--case-sensitive", action="store_true", help="case-sensitive matching (default: case-insensitive)")
    ap.add_argument("--no-diacritics", action="store_true", help="disable diacritic/apostrophe variants")
    ap.add_argument("--model", default="en_core_web_sm", help="spaCy model for alignment sanity checks")
    args = ap.parse_args()

    # lazy YAML load if available, else JSON
    def load_gold(p: Path):
        try:
            import yaml  # optional
            with p.open(encoding="utf-8") as fh:
                return yaml.safe_load(fh)
        except Exception:
            with p.open(encoding="utf-8") as fh:
                return json.load(fh)

    cand_path = Path(args.candidates)
    gold_path = Path(args.gold)
    out_path  = Path(args.out)

    # load
    lines = load_candidates_lines(cand_path)
    gold = load_gold(gold_path)
    # gold format:
    # {
    #   "18": {"items":[{"text":"Aggivessana","label":"PERSON"}], "meta":{"id":"MN x", "title":"..."}},
    #   "38": {"items":[{"text":"Tissa","label":"PERSON"}]},
    #   ...
    # }
    # line keys can be str or int

    nlp = spacy.load(args.model, exclude=["parser","lemmatizer","textcat","attribute_ruler","tagger"])
    if "sentencizer" not in nlp.pipe_names:
        nlp.add_pipe("sentencizer")

    wrote, skipped = 0, 0
    with out_path.open("w", encoding="utf-8") as fout:
        for ln, text in lines:
            key = str(ln)
            if key not in gold:
                continue
            spec = gold[key] or {}
            items = spec.get("items") or []
            meta  = spec.get("meta") or {}

            ents = []
            for it in items:
                label = (it.get("label") or args.label_default or "").strip().upper()
                phrase = it.get("text") or it.get("phrase") or ""
                phrase = phrase.strip()
                if not label or not phrase:
                    continue

                hits = find_all_occurrences(
                    hay=text,
                    needle=phrase,
                    case_sensitive=args.case_sensitive,
                    allow_diacritic_variants=not args.no_diacritics
                )
                if not hits:
                    # no match; carry on
                    continue
                for s,e in hits:
                    ents.append((s,e,label))

            # de-dup and remove overlaps within-label preference (longer span wins)
            ents = sorted(ents, key=lambda x:(x[0], -x[1]))
            final = []
            taken = []
            for s,e,lbl in ents:
                overlap = False
                for (ps,pe,pl) in final:
                    if not (e <= ps or s >= pe):
                        # overlapping; keep the longer span if same label, else skip shorter
                        if (e - s) <= (pe - ps):
                            overlap = True
                            break
                if not overlap:
                    final.append((s,e,lbl))
                    taken.append((s,e))

            if not final:
                skipped += 1
                continue

            # alignment sanity; drop misaligned spans (rare, but safe)
            doc = nlp(text)
            try:
                biluo = offsets_to_biluo_tags(doc, [(s,e,l) for (s,e,l) in final])
            except Exception:
                # try expand alignment
                ok = []
                for (s,e,l) in final:
                    span = doc.char_span(s,e, label=l, alignment_mode="expand")
                    if span is not None:
                        ok.append((span.start_char, span.end_char, l))
                final = ok

            if not final:
                skipped += 1
                continue

            rec = {"text": text, "entities": final}
            if meta:
                rec["meta"] = meta
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            wrote += 1

    print(f"wrote {wrote} gold examples to {out_path} (skipped {skipped})")

if __name__ == "__main__":
    main()