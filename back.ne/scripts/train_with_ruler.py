# train_with_ruler.py  (drop-in)
import sys
from pathlib import Path
import spacy
from spacy.util import load_config
from spacy.training.loop import train as train_nlp

# ---- paths (adjust if needed) ----
CONFIG = Path("filled_resolved.cfg")
OUTDIR = Path("../work/model_out")
TRAIN = Path("../work/train_merged.spacy")   # must exist
DEV   = Path("../work/dev.spacy")            # must exist
RULER_PATH = Path("/Users/alee/sutta_nlp/ne-data/patterns/entity_ruler/patterns.jsonl")  # must exist

# ---- sanity checks ----
assert TRAIN.exists(), f"Missing train DocBin: {TRAIN.resolve()}"
assert DEV.exists(),   f"Missing dev DocBin:   {DEV.resolve()}"
assert RULER_PATH.exists(), f"Missing patterns file: {RULER_PATH.resolve()}"

# ---- load config with path overrides baked in ----
cfg = load_config(CONFIG, overrides={
    "paths.train": str(TRAIN),
    "paths.dev":   str(DEV),
})

# ---- build nlp from *this cfg* ----
nlp = spacy.util.load_model_from_config(cfg)

# ---- inject entity_ruler patterns BEFORE training ----
if "entity_ruler" in nlp.pipe_names:
    nlp.remove_pipe("entity_ruler")
ruler = nlp.add_pipe("entity_ruler", first=True, config={"overwrite_ents": False})
ruler.from_disk(RULER_PATH)
print(f"Loaded {len(ruler.patterns)} patterns from {RULER_PATH}")

# (optional) freeze the ruler if your config expects that
if "training" in nlp.config and "frozen_components" in nlp.config["training"]:
    # make sure ruler is in the frozen list
    frozen = set(nlp.config["training"].get("frozen_components", []))
    if "entity_ruler" not in frozen:
        frozen.add("entity_ruler")
        nlp.config["training"]["frozen_components"] = list(frozen)

# ---- train THIS nlp (no config re-parsing) ----
OUTDIR.mkdir(parents=True, exist_ok=True)
train_nlp(nlp, OUTDIR)