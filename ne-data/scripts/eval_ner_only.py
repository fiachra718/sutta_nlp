# eval_ner_only.py
import spacy
from spacy.scorer import Scorer
from spacy.training import Example
from spacy.tokens import DocBin
from local_settings import MODELS_DIR

nlp = spacy.load(str(MODELS_DIR))
# drop rulers for a clean NER-only signal
for name in [n for n in ("entity_ruler","span_ruler") if n in nlp.pipe_names]:
    nlp.remove_pipe(name)

docs = DocBin().from_disk("ne-data/work/dev.spacy").get_docs(nlp.vocab)
examples = [Example.from_dict(nlp.make_doc(doc.text), {"entities":[(e.start_char,e.end_char,e.label_) for e in doc.ents]})
            for doc in docs]  # dev.spacy is gold, so this reconstructs gold spans

scorer = Scorer()
pred = [nlp(ex.x) for ex in examples]
sc = scorer.score([Example(p, ex.y) for p, ex in zip(pred, examples)])
#  f"result: {value:{width}.{precision}}"
p = sc["ents_p"]
r = sc["ents_r"]
f = sc["ents_f"]
ept = sc["ents_per_type"]
print(f'results: p={sc["ents_p"]:{5}.{4}}, r={sc["ents_r"]:{5}.{4}}, f={sc["ents_f"]:{5}.{4}}') # , {r:5}, {f:5}, {ept:5}\n")
print(f'entities per type: {sc["ents_per_type"]}')
