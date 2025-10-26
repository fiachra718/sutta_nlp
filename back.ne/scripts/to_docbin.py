import json, sys, spacy
from spacy.tokens import DocBin

def make_docbin(in_path, out_path, lang="en"):
    nlp = spacy.blank(lang)
    db = DocBin(store_user_data=True)
    with open(in_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            text = rec["text"]
            doc = nlp.make_doc(text)
            ents = []
            for start, end, label in rec.get("entities", []):
                # Use 'contract' so spans that fall between token boundaries shrink to valid spans
                span = doc.char_span(start, end, label=label, alignment_mode="contract")
                if span is not None:
                    ents.append(span)
            doc.ents = ents
            db.add(doc)
    db.to_disk(out_path)
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    make_docbin(sys.argv[1], sys.argv[2])