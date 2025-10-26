Sutta NER — Lean Workflow (pin me)

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

terms.json (example)
--------------------
{
  "PERSON": ["Ānanda","Sāriputta","Moggallāna","Gotama","Sahampati","Sakka"],
  "GPE": ["Sāvatthī","Rājagaha","Vesālī","Kosambī","Kapilavatthu","Magadha","Kosala"],
  "LOC": ["Jetavana","Veḷuvana","Jīvakambavana","Gosiṅga Sālavana","Deer Park"],
  "NORP": ["Sakyans","Kālamas","Devas","Asuras"]
}

Daily pipeline
--------------
(A) Normalize/clean the ruler patterns (NFC + drop over-generic)
python scripts/normalize_and_expand_patterns.py \
  --in  models/archive/sutta_ner-v2/ruler_patterns \
  --out models/archive/sutta_ner-v2/ruler_patterns_nfc_clean

(B) Clean the main candidate JSONL (tolerant loader, drops bad rows)
python scripts/clean_and_validate_jsonl.py \
  --in  data/work/training_set.jsonl \
  --out data/work/training_set.cleaned.jsonl \
  --model en_core_web_sm

(C) (Optional) Generate & prune negatives, then merge
python scripts/generate_negatives.py \
  --out   data/raw/negative_candidates.jsonl \
  --limit 300
# manual prune pass, then:
cat data/work/training_set.cleaned.jsonl data/raw/negative_candidates.jsonl \
  > data/work/training_plus_neg.jsonl

(D) Promote hand-picked lines to JSONL with spans (see promote_hand_picks.py below)
python scripts/promote_hand_picks.py \
  --lines data/seeds/hand_picks.txt \
  --terms data/seeds/terms.json \
  --out   data/gold/hand_picks.jsonl

(E) Assemble train/dev & sanity-check
# simple split example (adjust counts as needed)
head -n 256 data/work/training_plus_neg.jsonl > data/work/train.jsonl
tail -n 64  data/work/training_plus_neg.jsonl > data/work/dev.jsonl
# add curated gold to train (or split some into dev)
cat data/gold/hand_picks.jsonl >> data/work/train.jsonl

# Sanity check with ruler
python scripts/ner_data_sanity.py \
  --model  models/archive/sutta_ner-v2 \
  --jsonl  data/work/train.jsonl \
  --ruler  models/archive/sutta_ner-v2/ruler_patterns_nfc_clean

(F) Train (resume from v2; keep ruler FIRST)
python scripts/train_small_ner.py \
  --base-model     models/archive/sutta_ner-v2 \
  --ruler-patterns models/archive/sutta_ner-v2/ruler_patterns_nfc_clean \
  --train          data/work/train.jsonl \
  --dev            data/work/dev.jsonl \
  --out            models/archive/sutta_ner-v3 \
  --resume

Practical tips
--------------
• Drop generic ruler rules (e.g., “at” + IS_TITLE) — they inflate false positives.
• Normalize diacritics (NFC) consistently; include diacritic-free variants for recall.
• Balance classes: aim ~1:1 or 1:2 pos:neg to avoid crushing recall or precision.
• Include “hard negatives” (capitalized non-entities) to teach boundaries.
• Consider NORP for groups/peoples (Sakyans, Kālamas, Devas, Asuras).
• Keep dev genuinely unseen (different suttas/paragraphs) to measure generalization.


What is promote_hand_picks.py?
------------------------------
A tiny helper that reads your hand-picked lines (plain text) and a terms.json dictionary
of labeled names/places. It finds every occurrence (case- & diacritic-insensitive) of those
terms in each line and emits JSONL records with proper character offsets:

{"text": "...", "entities": [[start, end, "LABEL"], ...]}

Use it to turn “lines I cut aside to be gold” into ready-to-train JSONL without hand-computing offsets.


Disagreement mining: run your text twice—once with rulers ON and once with NER-only—and surface sentences where the sets of spans differ. Label only those. That’s where the model is learning the most.
	•	Lexicon recall probes (silver recall): for a list of canonical names (places/people), search random sentences that contain them and measure found / total. This gives a proxy for recall without full gold.
	•	Precision@K samples: randomly sample K predictions per label and quickly mark correct/incorrect to estimate precision without exhaustive gold.

  	•	Every label your rulers can inject into doc.ents must also exist at least once in training, or disable annotate_ents during training/eval.

