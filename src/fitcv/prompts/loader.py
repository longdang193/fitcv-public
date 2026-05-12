"""@meta
name: loader
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.prompts.loader.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

from pathlib import Path


def load_prompt_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")
