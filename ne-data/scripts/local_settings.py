# local_settings.py
from pathlib import Path
import os

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

MODELS_DIR  = NE_DATA / "models" / "model-more" / "model-last"
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
