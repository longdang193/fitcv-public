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
