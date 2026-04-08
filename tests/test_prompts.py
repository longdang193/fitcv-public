"""Tests for the shared prompt registry and renderer."""

import pytest

from fitcv.prompts import get_prompt_definition, render_prompt


def test_get_prompt_definition_returns_enrich_extraction_metadata() -> None:
    definition = get_prompt_definition("enrich.extraction.v1")

    assert definition.prompt_id == "enrich.extraction.v1"
    assert definition.stage_id == "enrich"
    assert definition.version == "v1"
    assert definition.template_path.name == "enrich_extraction_v1.md"


def test_get_prompt_definition_returns_ranking_ai_score_metadata() -> None:
    definition = get_prompt_definition("ranking.ai_score.v1")

    assert definition.prompt_id == "ranking.ai_score.v1"
    assert definition.stage_id == "ranking"
    assert definition.version == "v1"
    assert definition.template_path.name == "ranking_ai_score_v1.md"


def test_get_prompt_definition_returns_cv_generation_write_metadata() -> None:
    definition = get_prompt_definition("cv_generation.write.v1")

    assert definition.prompt_id == "cv_generation.write.v1"
    assert definition.stage_id == "cv_generation"
    assert definition.version == "v1"
    assert definition.template_path.name == "cv_generation_write_v1.md"


def test_get_prompt_definition_returns_cv_generation_structured_write_metadata() -> None:
    definition = get_prompt_definition("cv_generation.structured_write.v1")

    assert definition.prompt_id == "cv_generation.structured_write.v1"
    assert definition.stage_id == "cv_generation"
    assert definition.version == "v1"
    assert definition.template_path.name == "cv_generation_structured_write_v1.md"


def test_render_prompt_includes_expected_runtime_context() -> None:
    rendered = render_prompt(
        "enrich.extraction.v1",
        {
            "metadata_block": '{"title": "Data Analyst"}',
            "extraction_schema": '{"required_skills": []}',
            "description": "Need SQL and Python skills.",
        },
    )

    assert rendered.prompt_id == "enrich.extraction.v1"
    assert "Data Analyst" in rendered.text
    assert "required_skills" in rendered.text
    assert "Need SQL and Python skills." in rendered.text


def test_render_prompt_ranking_ai_score_includes_thresholds() -> None:
    rendered = render_prompt(
        "ranking.ai_score.v1",
        {
            "jd_summary": "Data Analyst",
            "candidate_summary": "SQL, Python",
            "evidence_section": "",
            "strong_threshold": "0.7",
            "stretch_threshold": "0.4",
        },
    )

    assert "Data Analyst" in rendered.text
    assert "0.7" in rendered.text
    assert "0.4" in rendered.text


def test_render_prompt_cv_generation_write_includes_sections() -> None:
    rendered = render_prompt(
        "cv_generation.write.v1",
        {
            "title": "Data Analyst",
            "required_skills": "SQL, Python",
            "selected_evidence": "- Experience",
            "evidence_usage_guidance": "- Use evidence",
            "analysis_summary": "Selected evidence count: 2",
            "constraints": "Do not invent claims.",
            "section_evidence": "(none)",
            "output_template": "## Summary",
            "output_instruction": "Write only the completed CV markdown. Do not add commentary.",
        },
    )

    assert "Data Analyst" in rendered.text
    assert "Do not invent claims." in rendered.text
    assert "## Summary" in rendered.text


def test_render_prompt_cv_generation_structured_write_includes_schema() -> None:
    rendered = render_prompt(
        "cv_generation.structured_write.v1",
        {
            "title": "Data Analyst",
            "required_skills": "SQL, Python",
            "selected_evidence": "- Experience",
            "evidence_usage_guidance": "- Use evidence",
            "analysis_summary": "Selected evidence count: 2",
            "constraints": "Do not invent claims.",
            "section_evidence": "(none)",
            "output_template": "## Summary",
            "structured_schema": '{"sections": {}}',
            "output_instruction": "Write only valid JSON matching the schema below.",
        },
    )

    assert "structured JSON document" in rendered.text
    assert "## Structured JSON Schema" in rendered.text
    assert '{"sections": {}}' in rendered.text


def test_render_prompt_raises_for_missing_required_variables() -> None:
    with pytest.raises(ValueError, match="missing template variables"):
        render_prompt("enrich.extraction.v1", {"description": "Only description"})


def test_get_prompt_definition_rejects_unknown_prompt_id() -> None:
    with pytest.raises(KeyError):
        get_prompt_definition("enrich.extraction.v999")
