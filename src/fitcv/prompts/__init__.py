"""@meta
name: __init__
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.prompts.__init__.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from fitcv.prompts.models import PromptDefinition, RenderedPrompt
from fitcv.prompts.registry import get_prompt_definition
from fitcv.prompts.renderer import render_prompt

__all__ = [
    "PromptDefinition",
    "RenderedPrompt",
    "get_prompt_definition",
    "render_prompt",
]
