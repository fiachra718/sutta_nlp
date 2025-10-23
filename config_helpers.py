from pathlib import Path
from typing import Dict, Any, List

import srsly
from spacy import Language, registry


@registry.misc("load_jsonl_patterns.v1")
def load_jsonl_patterns(path: str) -> List[Dict[str, Any]]:
    """Load patterns from a JSONL file into memory."""
    data_path = Path(path)
    if not data_path.exists():
        raise FileNotFoundError(f"Entity ruler patterns file not found: {path}")
    return list(srsly.read_jsonl(data_path))


@registry.callbacks("set_ruler_patterns.v1")
def set_ruler_patterns():
    """Return a callback that loads entity ruler patterns after the pipeline is created."""

    def after_pipeline_creation(nlp: Language) -> Language:
        paths = nlp.config.get("paths", {})
        patterns_root = paths.get("ruler_patterns_dir")
        if patterns_root is None:
            raise ValueError("Expected 'paths.ruler_patterns_dir' in config to load patterns.")
        root_path = Path(patterns_root)
        if not root_path.exists():
            raise FileNotFoundError(f"Entity ruler pattern directory not found: {root_path}")
        ruler = nlp.get_pipe("entity_ruler")
        ruler.from_disk(root_path)
        return nlp

    return after_pipeline_creation
