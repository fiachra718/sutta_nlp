# local_settings.py
from pathlib import Path
import os
import spacy
import config_helpers


# ----- choose your anchor -----
# A) anchor at this file's location (recommended)
# REPO_ROOT = Path(__file__).resolve().parent  # move this file to your repo root
# If you place this file in ne-data/scripts/, then do:
REPO_ROOT = Path(__file__).resolve().parents[2]  # scripts -> ne-data -> <repo root>

# B) OR allow override via env var (optional)
REPO_ROOT = Path(os.environ.get("REPO_ROOT", REPO_ROOT))

# ----- common paths -----
NE_DATA     = REPO_ROOT / "ne-data"
WORK        = NE_DATA / "work"
SCRIPTS     = NE_DATA / "scripts"

MODELS_DIR  = NE_DATA / "work" / "models" / "1106"
CURRENT_MODEL = NE_DATA / "work" / "models" / "1106"
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
for p in (
    NE_DATA,
    WORK,
    SCRIPTS,
    MODELS_DIR,
    PATTERNS.parent,
):
    p.mkdir(parents=True, exist_ok=True)

def load_model():
    nlp = spacy.load(CURRENT_MODEL)
    # clear the pipe
    for name in ("norp_head_ruler", "entity_ruler", "span_ruler"):
        if name in nlp.pipe_names:
            nlp.remove_pipe(name)

    # add the NORP patterns from disk
    ruler = nlp.add_pipe(
        "entity_ruler", 
        name="norp_head_ruler", 
        first=True, 
        config={"overwrite_ents": False}
    )
    ruler.from_disk(NORP_PATTERNS) 

    # add the entity rules
    er = nlp.add_pipe(
        "entity_ruler",
        after="ner",
        config={"overwrite_ents": False}
    )
    er.from_disk(str(PATTERNS))  

    # add the LOC/span patterns
    sr = nlp.add_pipe(
        "span_ruler",
        after="ner",
        config={"spans_key": "LOC_PHRASES", "overwrite": False}
    )
    sr.from_disk(LOC_EVENT_PATTERNS)  # folder; contains a file named 'patterns'
    
    return nlp