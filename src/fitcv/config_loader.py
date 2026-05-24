"""@meta
name: config_loader
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Loader utilities for config file discovery and YAML ingestion.
inputs:
  - Config file paths and loader callbacks
outputs:
  - Parsed config dictionaries and resolved config paths
lifecycle:
  - status: active
"""

from pathlib import Path
from typing import Any

import yaml


def load_yaml_file(path: Path, *, logger: Any) -> dict[str, Any]:
    if not path.exists():
        logger.warning("Config file not found (skipping): %s", path)
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except PermissionError:
        logger.warning("Config file not readable (skipping): %s", path)
        return {}
    return data if isinstance(data, dict) else {}


def load_policy_file(
    config_dir: Path,
    rel_paths: tuple[str, ...],
    *,
    load_yaml_file_fn: Any,
    logger: Any,
) -> tuple[dict[str, Any], Path]:
    for rel_path in rel_paths:
        candidate = config_dir / rel_path
        if candidate.exists():
            return load_yaml_file_fn(candidate), candidate
    preferred_path = config_dir / rel_paths[0]
    logger.warning("Config file not found (skipping): %s", preferred_path)
    return {}, preferred_path


def find_config_dir(base_path: Path) -> Path:
    candidate = base_path.parent
    for _ in range(4):
        config_dir = candidate / "config"
        if config_dir.is_dir():
            return config_dir
        candidate = candidate.parent
    return base_path.parent / "config"


def resolve_env_path(path: str | Path | None, *, default_env_candidates: tuple[str, ...]) -> Path:
    if path is not None:
        return Path(path)
    for candidate in default_env_candidates:
        candidate_path = Path(candidate)
        if candidate_path.exists():
            return candidate_path
    return Path(default_env_candidates[0])


def is_legacy_env_path(path: Path) -> bool:
    return path.name == "env.yaml" and path.parent.name == "config"


def merge_missing_keys(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    for key, value in extra.items():
        if key not in base:
            base[key] = value
    return base
