from spacy.tokens import DocBin
from spacy.training import Example
import spacy

from local_settings import WORK, load_model

def docbin_to_examples(path, nlp):
    if not path.exists():
        print(f"Skipping missing DocBin: {path}")
        return []
    docbin = DocBin().from_disk(path)
    examples = []
    for doc in docbin.get_docs(nlp.vocab):
        pred_doc = nlp.make_doc(doc.text)
        examples.append(Example(pred_doc, doc))
    return examples


def main():
    nlp = load_model()

    sources = [
        WORK / "test.spacy",
        WORK / "test_from_db.spacy"
    ]

    examples = []
    for path in sources:
        examples.extend(docbin_to_examples(path, nlp))

    if not examples:
        raise SystemExit("No DocBin examples loaded; nothing to score.")

    scores = nlp.evaluate(examples)
    
    print("ents_p:", scores.get("ents_p"))
    print("ents_r:", scores.get("ents_r"))
    print("ents_f:", scores.get("ents_f"))
    print("per-type:", scores.get("ents_per_type"))


if __name__ == "__main__":
    main()
