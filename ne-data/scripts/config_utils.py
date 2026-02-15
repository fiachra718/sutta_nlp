from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "ne-data" / "config" / "train_from_db.toml"


def resolve_config_path(raw: str | None) -> Path:
    if raw is None:
        return DEFAULT_CONFIG
    p = Path(raw).expanduser()
    if p.is_absolute():
        return p
    return (REPO_ROOT / p).resolve()


def load_toml(path: Path) -> dict:
    with path.open("rb") as f:
        return tomllib.load(f)


def model_from_config(path: Path) -> tuple[str, str | None]:
    cfg = load_toml(path)
    model_cfg = cfg.get("model", {})
    model_name = model_cfg["name"]
    expected_version = model_cfg.get("expected_version")
    return model_name, expected_version


def testing_model_from_config(
    path: Path,
) -> tuple[Literal["system", "path"], str, str | None]:
    cfg = load_toml(path)
    model_cfg = cfg.get("model", {})
    testing_cfg = cfg.get("testing", {})

    default_model = model_cfg["name"]
    default_expected = model_cfg.get("expected_version")
    expected_version = testing_cfg.get("expected_version", default_expected)

    source = testing_cfg.get("source")
    if source is None:
        # Back-compat convenience:
        # testing.model = "system" means use installed package.
        model_token = testing_cfg.get("model")
        if model_token == "system":
            source = "system"
        elif testing_cfg.get("path") or testing_cfg.get("model_path"):
            source = "path"
        else:
            source = "system"

    if source == "system":
        model_name = testing_cfg.get("system_model", default_model)
        return "system", model_name, expected_version

    if source == "path":
        model_path_raw = (
            testing_cfg.get("path")
            or testing_cfg.get("model_path")
            or testing_cfg.get("model")
        )
        if not model_path_raw or model_path_raw == "system":
            raise ValueError(
                "testing.source='path' requires testing.path (or testing.model_path)."
            )
        model_path = str(resolve_config_path(model_path_raw))
        return "path", model_path, expected_version

    raise ValueError(f"Unsupported testing.source={source!r} (expected 'system' or 'path').")
