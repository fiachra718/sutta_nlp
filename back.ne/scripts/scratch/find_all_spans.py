import re
from typing import List, Tuple

def find_all_spans(text: str, terms: List[str]) -> List[Tuple[int,int,str]]:
    """Return [(start, end, label)] for every exact term occurrence.
       Keeps duplicates and only merges true overlaps if requested."""
    spans = []
    for label, term in terms:  # e.g. [("PERSON","Sakka"), ("PERSON","Vasavassa")]
        # \b handles punctuation like (Sakka) correctly
        pat = re.compile(rf'\b{re.escape(term)}\b', flags=re.UNICODE)
        for m in pat.finditer(text):
            spans.append((m.start(), m.end(), label))
    # sort by start, then -end
    spans.sort(key=lambda x: (x[0], -x[1]))
    return spans

def merge_overlaps(spans: List[Tuple[int,int,str]]) -> List[Tuple[int,int,str]]:
    """Only merge if spans overlap and have identical label; duplicates at different
       offsets are preserved."""
    if not spans:
        return spans
    spans = sorted(spans, key=lambda x: (x[0], x[1]))
    merged = [spans[0]]
    for s,e,lbl in spans[1:]:
        ps,pe,pl = merged[-1]
        if s < pe and lbl == pl:  # true overlap
            merged[-1] = (ps, max(pe,e), pl)
        else:
            merged.append((s,e,lbl))
    return merged


if __name__ == "__main__":
    text = '. "The Asuras dwelling in the ocean were defeated by Vajirahattha (Sakka). They are brethren of Vasavassa (Sakka) [10] possessed of iddhi power, and are followed by a retinue of attendants.'
    terms = [("PERSON","Vajirahattha"), ("PERSON","Sakka"), ("PERSON","Vasavassa")]
    spans = find_all_spans(text, terms)
    print(spans)
    # spans -> [(53,65,'PERSON'), (67,72,'PERSON'), (96,105,'PERSON'), (107,112,'PERSON')]