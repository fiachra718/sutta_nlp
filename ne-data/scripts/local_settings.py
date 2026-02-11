# local_settings.py
from pathlib import Path
import os
import spacy
import config_helpers


# ----- choose anchor -----
REPO_ROOT = Path(__file__).resolve().parents[2]  # scripts -> ne-data -> <repo root>

# B) OR allow override via env var (optional)
REPO_ROOT = Path(os.environ.get("REPO_ROOT", REPO_ROOT))

# ----- common paths -----
NE_DATA     = REPO_ROOT / "ne-data"
WORK        = NE_DATA / "work"
SCRIPTS     = NE_DATA / "scripts"

MODELS_DIR  = WORK / "models" / "2026_02_10"
PATTERNS    = NE_DATA / "patterns" / "entity_ruler" / "patterns.jsonl"
NORP_PATTERNS    = NE_DATA / "patterns" / "entity_ruler" / "ruler_norp.jsonl"
LOC_EVENT_PATTERNS    = NE_DATA / "patterns" / "span_ruler"

# frequently used files
SPAN_PATTERNS = NE_DATA / "patterns" / "span_ruler"
TRAIN_SPACY = WORK / "train.spacy"
DEV_SPACY   = WORK / "dev.spacy"
MERGED_SPACY= WORK / "train_merged.spacy"
PARAS_TXT   = WORK / "paras.txt"

# helpful: ensure dirs exist (no-ops if they already do)
for p in ( NE_DATA, WORK, SCRIPTS,
    MODELS_DIR, PATTERNS.parent, ):
    p.mkdir(parents=True, exist_ok=True)

##################################
#### load_model 
##################################

def load_model():
    print(f"Loading local NER model from {MODELS_DIR}")
    nlp = spacy.load(MODELS_DIR)

    # clear the pipe
    for name in ("norp_head_ruler", "entity_ruler", "span_ruler"):
        if name in nlp.pipe_names:
            print("remove", name)
            nlp.remove_pipe(name)

    # 1) Main entity_ruler AFTER <!BEFORE> ner
    er = nlp.add_pipe(
        "entity_ruler",
        name="entity_ruler",
        after="ner",
        config={"overwrite_ents": False},
    )
    print("add entity ruler (before ner)")
    er.from_disk(str(PATTERNS))

    # 2) NORP head ruler AFTER ner
    norp_ruler = nlp.add_pipe(
        "entity_ruler",
        name="norp_head_ruler",
        after="entity_ruler",
        config={"overwrite_ents": False},
    )
    print("add NORP ruler (after ner)")
    norp_ruler.from_disk(NORP_PATTERNS)

    # 3) span_ruler at the end
    sr = nlp.add_pipe(
        "span_ruler",
        name="span_ruler",
        last=True,
        config={"spans_key": "LOC_PHRASES", "overwrite": False},
    )
    print("add span ruler (LOC, EVENT)")
    sr.from_disk(LOC_EVENT_PATTERNS)

    print("Final pipeline:", nlp.pipe_names)
    return nlp
