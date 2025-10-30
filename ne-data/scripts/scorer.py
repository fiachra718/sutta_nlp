from local_settings import load_model, WORK
import json
from spacy.training import Example
from typing import List, Tuple

import spacy


SpanTuple = Tuple[int, int, str]


def extract_entities(record: dict) -> List[SpanTuple]:
    """Support both span dicts and [start, end, label] triples."""
    spans = record.get("spans")
    if spans:
        first = spans[0] if spans else None
        if isinstance(first, dict):
            return [
                (int(span["start"]), int(span["end"]), span["label"])
                for span in spans
                if "start" in span and "end" in span and "label" in span
            ]
        if isinstance(first, (list, tuple)) and len(first) >= 3:
            return [
                (int(span[0]), int(span[1]), str(span[2]))
                for span in spans
                if len(span) >= 3
            ]

    entities = record.get("entities", [])
    if entities:
        first = entities[0]
        if isinstance(first, dict):
            return [
                (int(ent["start"]), int(ent["end"]), ent["label"])
                for ent in entities
                if "start" in ent and "end" in ent and "label" in ent
            ]
        if isinstance(first, (list, tuple)) and len(first) >= 3:
            return [
                (int(ent[0]), int(ent[1]), str(ent[2]))
                for ent in entities
                if len(ent) >= 3
            ]

    return []


nlp = load_model()
examples = []
with open(WORK / "test.jsonl", "r", encoding="utf-8") as f:
    for line_no, line in enumerate(f, start=1):
        if not line.strip():
            continue
        row = json.loads(line)
        text = row.get("text")
        if text is None:
            raise ValueError(f"Missing 'text' field on line {line_no}")
        ents = extract_entities(row)
        doc = nlp.make_doc(text)
        examples.append(Example.from_dict(doc, {"entities": ents}))

scores = nlp.evaluate(examples)
print("ents_p:", scores["ents_p"])
print("ents_r:", scores["ents_r"])
print("ents_f:", scores["ents_f"])
# Optional per-label:
print("per-type:", scores["ents_per_type"])
