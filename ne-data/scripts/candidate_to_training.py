import spacy
from spacy.matcher import PhraseMatcher
import json
from json import JSONDecodeError

def candidates_to_training(record, nlp=None, allow_possessive=True):
    """
    record: {"text": str, "entities": [[label, phrase], ...]}
    returns: {"text": str, "entities": [(start, end, label), ...]}
    """
    # Use your model's tokenizer if you customized it; otherwise blank is fine.
    if nlp is None:
        nlp = spacy.blank("en")

    text = record["text"]
    doc  = nlp.make_doc(text)

    # Build a matcher for THIS sentence
    matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
    key_to_label = {}
    for label, phrase in record["entities"]:
        key = f"{label}__{phrase}"
        matcher.add(key, [nlp.make_doc(phrase)])
        key_to_label[key] = label

    matches = matcher(doc)

    # Collect raw spans
    raw = []
    for match_id, start, end in matches:
        key = nlp.vocab.strings[match_id]
        label = key_to_label[key]
        span = doc[start:end]

        # Optional: include possessive right after the match ("'s" or "’s")
        end_char = span.end_char
        if allow_possessive and end < len(doc):
            nxt = doc[end].text
            if nxt in ("'s", "’s"):
                end_char = doc[end].idx + len(nxt)

        raw.append((span.start_char, end_char, label))

    # Deduplicate & resolve overlaps: keep longer first
    raw.sort(key=lambda t: (t[0], -(t[1]-t[0])))
    entities = []
    last_end = -1
    for start_char, end_char, label in raw:
        if start_char >= last_end:   # non-overlapping
            entities.append((start_char, end_char, label))
            last_end = end_char
        # else: skip the shorter overlapping one

    return {"text": text, "entities": entities}


if __name__ == "__main__":
    for filename in ["./ne-data/work/1021.candidates.jsonl",
                     "./ne-data/work/1022_candidates.jsonl"]:
        with open(filename, "r", encoding="utf-8") as f:
            input_json = {}
            for i, line in enumerate(f):
                # print("Line: {}".format(i+1))
                try:
                    # print("line: {}".format(line))
                    input_json = json.loads(line.strip())
                except JSONDecodeError as e:
                    print(e)

                training_span = candidates_to_training(input_json)
                print(json.dumps(training_span))
