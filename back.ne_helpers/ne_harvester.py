# harvest_proper_names.py
from __future__ import annotations
import re, unicodedata
from collections import defaultdict, Counter
from pathlib import Path



# ---------- helpers ----------
def strip_diacritics(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

# Capitalized token (Unicode-friendly). Allow internal hyphen/’/dot.
UC = r"[A-ZĀĪŪṄÑṬḌṆḶ][\w\-\.’']+"
# A sequence of 1–4 such tokens (multi-word names)
CAP_SEQ = fr"(?:{UC})(?:\s+{UC}){{0,3}}"

# crude sentence splitter for the “don’t count sentence-start capitalizations” heuristic
SPLIT_SENT = re.compile(r"(?<=[.!?])\s+")

# words to ignore when they appear alone (keep short list; you can expand)
COMMON_CAPS = {
    "The","A","And","But","So","He","She","They","It","This","That","Thus",
    "Brahman","Monk","Nun","Wanderer","Householder","Bhikkhu","Bhikkhus",
    "One","On","In","At","Of","With","To","From","For","As","By","Yes","No",
    "Master","Blessed","Buddha","Dhamma","Sangha","Sutta","MN","SN","AN","DN",
}

# blacklist patterns (headers, numbers, transliteration artefacts, etc.)
BLACKLIST_RX = [
    re.compile(r"^\d+(\.\d+)*$"),
    re.compile(r"^(Chapter|Section|Verse)\b", re.I),
]

def is_blacklisted(span: str) -> bool:
    return any(rx.search(span) for rx in BLACKLIST_RX)

def get_from_text(text: str) -> list[tuple[str, str]]:
    """
    Return [(span_text, sent_context), ...]
    Only collect spans not at sentence start.
    """
    out = []
    for sent in SPLIT_SENT.split(text):
        s = sent.strip()
        if not s:
            continue
        # find all capitalized sequences in this sentence
        for m in re.finditer(CAP_SEQ, s):
            span = m.group(0).strip(" ,.;:–—()[]")
            # skip sentence-initial match (very likely just grammar)
            if m.start() == 0:
                continue
            # ignore trivial/common/blacklisted
            if span in COMMON_CAPS or is_blacklisted(span):
                continue
            # weed out ALLCAPS
            if span.isupper() and len(span) > 1:
                continue
            # keep short context
            start = max(0, m.start() - 40)
            end   = min(len(s), m.end() + 40)
            ctx = s[start:end].strip()
            out.append((span, ctx))
    return out

# ---------- main ----------
def main():
    import psycopg
    counts = Counter()
    ctx_map = defaultdict(list)
    
    # build your MN slice (adjust to your DB plumbing)

    sql = """
        select doc_id, identifier, title, raw_text
        from suttas
        where nikaya = 'Majjhima'
    """
    conn = psycopg.connect("dbname=tipitaka user=alee")
    cur = conn.execute(sql)
    for _, _, title, raw_text in cur.fetchall():
        text = f"{title}\n\n{raw_text}"

        for span, ctx in get_from_text(text):
            counts[span] += 1
            if len(ctx_map[span]) < 3:
                ctx_map[span].append(ctx)
    
    # rank by frequency then alphabetically
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))

    # write TSV for manual curation
    out = Path("gazetteer_candidates.tsv")
    with out.open("w", encoding="utf-8") as f:
        f.write("span\tfreq\tascii_variant\tcontexts\n")
        for span, freq in ranked:
            ascii_variant = strip_diacritics(span)
            ctxs = " | ".join(ctx_map[span])
            f.write(f"{span}\t{freq}\t{ascii_variant}\t{ctxs}\n")

    # quick console preview (top 40)
    print("Top candidates:")
    for span, freq in ranked[:40]:
        print(f"{freq:4d}  {span}")

    print(f"\nWrote {len(ranked)} candidates to {out}")

if __name__ == "__main__":
    main()
