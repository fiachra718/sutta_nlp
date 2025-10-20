# Sutta NLP pipeline

- Build corpus from ATI zip file
- Load Postgres using TSVector type via sql/ati_suttas.sql
- TF-IDF vectorizer wrapper
- Optional SVD + clustering
- Train SpaCy NER


## Setup
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   
edit local_settings.py

## Run NER training
```
Directory layout (suggested)
----------------------------
sutta_nlp/
├─ data/
│  ├─ seeds/
│  │  ├─ terms.json                # gazetteer-ish lists by label (PERSON/LOC/GPE/NORP…)
│  │  └─ hand_picks.txt            # your hand-selected gold lines (one paragraph per line)
│  ├─ raw/
│  │  ├─ positive_candidates.jsonl # multi_term_hits output etc.
│  │  └─ negative_candidates.jsonl # generate_negatives output (pre-pruning)
│  ├─ work/
│  │  ├─ training_set.jsonl        # current combined candidates (pos + neg)
│  │  ├─ training_set.cleaned.jsonl
│  │  ├─ training_plus_neg.jsonl
│  │  ├─ train.jsonl               # final train split
│  │  └─ dev.jsonl                 # final dev split
│  └─ gold/
│     └─ hand_picks.jsonl          # hand picks promoted to JSONL with spans
├─ models/
│  ├─ model-more/
│  │  ├─ model-best/
│  │  └─ model-last/
│  └─ archive/
│     ├─ sutta_ner-v1/
│     ├─ sutta_ner-v2/
│     │  └─ ruler_patterns(_nfc|_nfc_clean)/
│     └─ sutta_ner-v3/              # new outputs landed here previously
├─ patterns/
│  └─ entity_ruler/
│     ├─ cfg
│     └─ patterns.jsonl
├─ scripts/
│  ├─ batch_add_spans.py
│  ├─ clean_and_validate_jsonl.py
│  ├─ normalize_and_expand_patterns.py
│  ├─ ner_data_sanity.py
│  ├─ train_small_ner.py
│  └─ generate_negatives.py
└─ Makefile (optional)

```

------
## Copyrights: Creative Commons Attribution-NonCommercial 4.0 International License

(You may copy, reformat, reprint, republish, and redistribute this work in any medium whatsoever, provided that: (1) you only make such copies, etc. available free of charge and, in the case of reprinting, only in quantities of no more than 50 copies; (2) you clearly indicate that any derivatives of this work (including translations) are derived from this source document; and (3) you include the full text of this license in any copies or derivatives of this work. Otherwise, all rights reserved. Documents linked from this page may be subject to other restrictions)

# plan:
```
- train NER
- build graphs based on
-- interlocuter
-- audience
-- town or city
-- location (e.g. Jeta's Grove, Vulture Peak)
- cluster sutta paragraphs
-- using NMF/TF-IDF/SVD
-- embeded person/place/location
-- transaltor's "see also" notes
```
