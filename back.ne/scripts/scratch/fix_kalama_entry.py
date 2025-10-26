# fix_kalama_entry.py
import json, re, sys

REC = {
  "text": ". \"I heard thus. Once the Blessed One, while wandering in the Kosala country with a large community of bhikkhus, entered a town of the Kalama people called Kesaputta . The Kalamas who were inhabitants of Kesaputta: \"Reverend Gotama, the monk, the son of the Sakyans, has, while wandering in the Kosala country, entered Kesaputta. The good repute of the Reverend Gotama has been spread in this way: Indeed, the Blessed One is thus consummate, fully enlightened, endowed with knowledge and practice, sublime, knower of the worlds, peerless, guide of tamable men, teacher of divine and human beings, which he by himself has through direct knowledge understood clearly. He set forth the Dhamma, good in the beginning, good in the middle, good in the end, possessed of meaning and the letter, and complete in everything; and he proclaims the holy life that is perfectly pure. Seeing such consummate ones is good indeed.\"",
  "entities": [],
  "meta": {
    "term": "Kosala",
    "nikaya": "AN",
    "identifier": "an03.065.soma.html",
    "title": "Kalama Sutta",
    "para_seq": 1,
    "hits": 2,
    "first_offset": 57
  }
}

# Strings to tag (no overlaps for inner substrings of longer ones)
TAGS = [
  ("Reverend Gotama", "PERSON"),
  ("Blessed One", "PERSON"),
  ("Buddha", "PERSON"),
  ("Kesaputta", "LOC"),
  ("Kalama people", "NORP"),
  ("Kalamas", "NORP"),
  ("Sakyans", "NORP"),
  ("Kosala", "GPE"),
]

def find_all_spans(text, needle):
    # Find all non-overlapping literal matches, case-sensitive
    out = []
    start = 0
    while True:
        i = text.find(needle, start)
        if i == -1: break
        out.append((i, i+len(needle)))
        start = i + len(needle)
    return out

def spans_overlap(a, b):
    (s1,e1),(s2,e2) = a,b
    return not (e1 <= s2 or e2 <= s1)

def main():
    text = REC["text"]

    # Collect candidate spans
    cand = []
    for needle, label in TAGS:
        for (s,e) in find_all_spans(text, needle):
            cand.append((s,e,label,needle))

    # Sort by length descending, so longer phrases (e.g. "Reverend Gotama")
    # win over their substrings (e.g. "Gotama")
    cand.sort(key=lambda x: (-(x[1]-x[0]), x[0]))

    accepted = []
    for s,e,label,needle in cand:
        if any(spans_overlap((s,e), (S,E)) for (S,E,_) in accepted):
            continue
        accepted.append((s,e,label))

    # Sort by start for nice output
    accepted.sort(key=lambda x: x[0])

    out = dict(REC)
    out["entities"] = [[s,e,label] for (s,e,label) in accepted]
    print(json.dumps(out, ensure_ascii=False))

if __name__ == "__main__":
    main()