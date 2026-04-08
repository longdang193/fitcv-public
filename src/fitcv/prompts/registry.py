from __future__ import annotations

from pathlib import Path

from fitcv.prompts.models import PromptDefinition

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

_PROMPT_REGISTRY: dict[str, PromptDefinition] = {
    "enrich.extraction.v1": PromptDefinition(
        prompt_id="enrich.extraction.v1",
        stage_id="enrich",
        version="v1",
        template_path=_TEMPLATES_DIR / "enrich_extraction_v1.md",
        summary="Structured JD extraction prompt for the enrich stage.",
    ),
    "ranking.ai_score.v1": PromptDefinition(
        prompt_id="ranking.ai_score.v1",
        stage_id="ranking",
        version="v1",
        template_path=_TEMPLATES_DIR / "ranking_ai_score_v1.md",
        summary="Structured AI reranking prompt for shortlist scoring.",
    ),
    "cv_generation.write.v1": PromptDefinition(
        prompt_id="cv_generation.write.v1",
        stage_id="cv_generation",
        version="v1",
        template_path=_TEMPLATES_DIR / "cv_generation_write_v1.md",
        summary="Primary CV generation writer prompt for markdown CV output.",
    ),
    "cv_generation.structured_write.v1": PromptDefinition(
        prompt_id="cv_generation.structured_write.v1",
        stage_id="cv_generation",
        version="v1",
        template_path=_TEMPLATES_DIR / "cv_generation_structured_write_v1.md",
        summary="Primary CV generation writer prompt for structured JSON CV output.",
    ),
}


def get_prompt_definition(prompt_id: str) -> PromptDefinition:
    try:
        return _PROMPT_REGISTRY[prompt_id]
    except KeyError as exc:
        raise KeyError(f"Unknown prompt_id: {prompt_id}") from exc
