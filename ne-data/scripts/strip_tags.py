from spacy.tokens import DocBin, Span
import spacy

def strip_label(in_path, out_path, kill={"DOCTRINE"}):
    nlp = spacy.blank("en")
    db = DocBin().from_disk(in_path)
    out = DocBin(store_user_data=True)
    for doc in db.get_docs(nlp.vocab):
        ents = [e for e in doc.ents if e.label_ not in kill]
        doc.ents = ents
        out.add(doc)
    out.to_disk(out_path)
    print(f"Wrote {out_path}")

strip_label("ne-data/work/train_merged_v3.spacy", "ne-data/work/train_merged_v3_nodoctr.spacy")
strip_label("ne-data/work/dev.spacy", "ne-data/work/dev_nodoctr.spacy")
strip_label("ne-data/work/train_pool.spacy", "ne-data/work/train_pool_nodoctr.spacy")
strip_label("ne-data/work/train_merged_v3.spacy", "ne-data/work/train_merged_v3_nodoctr.spacy")
strip_label("ne-data/work/latest_gold.spacy", "ne-data/work/latest_gold_nodoctr.spacy")
strip_label("ne-data/work/train_merged_v4.spacy", "ne-data/work/train_merged_v4_no_doctr.spacy")

