"""@meta
name: renderer
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.prompts.renderer.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

from string import Template
from typing import Any

from fitcv.prompts.loader import load_prompt_template
from fitcv.prompts.models import RenderedPrompt
from fitcv.prompts.registry import get_prompt_definition


def _required_template_variables(template_text: str) -> set[str]:
    required: set[str] = set()
    for match in Template.pattern.finditer(template_text):
        named = match.group("named")
        braced = match.group("braced")
        if named:
            required.add(named)
        elif braced:
            required.add(braced)
    return required


def render_prompt(prompt_id: str, context: dict[str, Any]) -> RenderedPrompt:
    definition = get_prompt_definition(prompt_id)
    template_text = load_prompt_template(definition.template_path)
    required_variables = _required_template_variables(template_text)
    missing_variables = sorted(
        variable_name
        for variable_name in required_variables
        if variable_name not in context
    )
    if missing_variables:
        raise ValueError(
            "Prompt render missing template variables: "
            + ", ".join(missing_variables)
        )
    rendered_text = Template(template_text).substitute(
        {key: str(value) for key, value in context.items()}
    )
    return RenderedPrompt(
        prompt_id=definition.prompt_id,
        stage_id=definition.stage_id,
        version=definition.version,
        template_path=definition.template_path,
        text=rendered_text,
    )
