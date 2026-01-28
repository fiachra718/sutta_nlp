import spacy
from spacy.training import Example
import json



def load_examples(file_name, nlp):
    examples = []
    total = bad = 0
    with open(file_name, 'r', encoding='utf-8') as f:
        for line in f:
            (text, span) = line.split('|')
            pred_doc = nlp.make_doc(text.strip())
            gold_doc = nlp.make_doc(text)     # reference Doc
            pred = json.loads(span.strip())
            # print(pred)
            spans = []
            for e in pred:
                # print(e)
                total += 1
                span = gold_doc.char_span(
                    e["start"], e["end"], label=e["label"], alignment_mode="strict"   # important!
                )
                if span is None:
                    bad += 1
                    next
                else:
                    spans.append(span)
            gold_doc.ents = spans
            example = Example(pred_doc, gold_doc)
            examples.append(example)

    print(f"There were {bad} bum records of {total}")
    return examples

nlp = spacy.load("en_sutta_ner")
test_file = "./ne-data/work/random_verses.txt"

examples = load_examples(test_file, nlp)
scores = nlp.evaluate(examples)
print("ents_p:", scores.get("ents_p"))
print("ents_r:", scores.get("ents_r"))
print("ents_f:", scores.get("ents_f"))
print("per-type:", scores.get("ents_per_type"))
