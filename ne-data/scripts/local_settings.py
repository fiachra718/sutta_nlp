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

MODELS_DIR  = NE_DATA / "models" / "1023_merged_v4" / "model-best"
PATTERNS    = NE_DATA / "patterns" / "entity_ruler" / "patterns.jsonl"

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
    nlp = spacy.load(MODELS_DIR)
    # clear the pipe
    for name in ("entity_ruler", "span_ruler"):
        if name in nlp.pipe_names:
            nlp.remove_pipe(name)

    # add the entity rules
    er = nlp.add_pipe(
        "entity_ruler",
        after="ner",
        config={"overwrite_ents": True}
    )
    er.from_disk(str(PATTERNS))  

    # add the LOC/span patterns
    sr = nlp.add_pipe(
        "span_ruler",
        last=True,
        config={"spans_key": "LOC_PHRASES", "overwrite": True}
    )
    sr.from_disk("ne-data/patterns/span_ruler")  # folder; contains a file named 'patterns'

    return nlp