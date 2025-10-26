import re
from spacy.language import Language
import json
from json import JSONDecodeError
from pathlib import Path
from typing import List

from spacy.tokens import Doc, Span
from spacy.util import filter_spans

from local_settings import load_model

WORK_DIR = Path("ne-data") / "work"
# TEXT_PATH = WORK_DIR / "test_verses.txt"
TEXT_PATH = WORK_DIR / "1022_candidates.jsonl"
CANDIDATES_PATH = WORK_DIR / "1022_candidates.jsonl"

BAD_TAIL = re.compile(r'[^A-Za-zÀ-ÖØ-öø-ÿ’\-]$')  # non-letter at the very end

@Language.component("clean_spans")
def clean_spans(doc):
    keep = []
    for ent in doc.ents:
        if ent.label_ != "PERSON":
            keep.append(ent); continue
        txt = ent.text
        has_title = any(t.is_title for t in ent)
        has_backslash = "\\" in txt
        bad_tail = bool(BAD_TAIL.search(txt))
        # prune obviously bogus “resolute.\” style PERSONS
        if not has_title or has_backslash or bad_tail:
            continue
        keep.append(ent)
    doc.ents = keep
    return doc



def _read_texts(path: Path) -> List[str]:
    lines = []
    with path.open("r", encoding="utf-8") as fh:
        for idx, line in enumerate(fh):
            if line.strip():
                try:
                    data = json.loads(line.strip())
                except JSONDecodeError as ej:
                    print("{} at line: {}".format(ej, idx+1))
                    next
                lines.append(data.get("text"))
    return lines
    # return [line.strip() for line in fh if line.strip()]


def _read_candidates(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8") as fh:
        return [json.loads(line.strip()) for line in fh if line.strip()]


def _collect_spans(doc: Doc) -> List[List[str]]:
    spans: List[Span] = list(doc.ents)
    loc_spans = doc.spans.get("LOC_PHRASES")
    if loc_spans:
        spans.extend(loc_spans)
    spans = sorted(filter_spans(spans), key=lambda span: span.start)
    return [[span.label_, span.text] for span in spans]


def main() -> None:
    texts = _read_texts(TEXT_PATH)
    candidates = _read_candidates(CANDIDATES_PATH)

    if len(texts) != len(candidates):
        raise ValueError(
            f"Mismatched data: {len(texts)} texts vs {len(candidates)} candidate rows"
        )
    nlp = load_model()
    nlp.add_pipe("clean_spans", last=True)
    mismatches = 0

    for idx, (text, record) in enumerate(zip(texts, candidates), start=1):
        doc = nlp(text)
        nlp_result = _collect_spans(doc)
        train_data = record["entities"]

        if nlp_result == train_data:
            print(f"[{idx}] match")
            continue

        mismatches += 1
        print(f"[{idx}] mismatch")
        print(f"  text: {text}")
        print(f"  nlp_result: {nlp_result}")
        print(f"  train_data:  {train_data}")
        if doc.spans.get("LOC_PHRASES"):
            loc_spans = [[span.label_, span.text] for span in doc.spans["LOC_PHRASES"]]
            print(f"  loc_spans: {loc_spans}")
        print(f"  ents_raw:  {[[ent.label_, ent.text] for ent in doc.ents]}")

        print("\n\n")
        # rerun with features disabled
        with nlp.select_pipes(enable=["entity_ruler"]):
            print("ER:", [(e.text, e.label_) for e in nlp(text).ents])
        with nlp.select_pipes(enable=["span_ruler"]):
            doc = nlp(text)
            print("SR:", [(s.text, s.label_) for s in doc.spans.get("BOOTSTRAP", [])])
        with nlp.select_pipes(enable=["ner"]):
            doc = nlp(text)
            print("NER:", [(e.text, e.label_) for e in doc.ents])
            print(repr(text[-80:]))            # see literal chars (look for "\\")
            with nlp.select_pipes(enable=["ner"]):
                doc = nlp(text)
            for t in doc[-12:]:
                print(t.i, repr(t.text), t.is_punct, t.is_title)

        print("\n\n")

    total = len(texts)
    print(f"\nSummary: {total - mismatches}/{total} matched.")




if __name__ == "__main__":
    main()
