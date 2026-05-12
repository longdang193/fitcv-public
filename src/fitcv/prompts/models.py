"""@meta
name: models
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.prompts.models.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PromptDefinition:
    prompt_id: str
    stage_id: str
    version: str
    template_path: Path
    summary: str


@dataclass(frozen=True)
class RenderedPrompt:
    prompt_id: str
    stage_id: str
    version: str
    template_path: Path
    text: str
