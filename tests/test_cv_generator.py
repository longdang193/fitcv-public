"""
@meta
type: test
scope: unit
domain: cv_generator
covers:
  - build_generation_prompt: evidence and gap constraints appear in prompt
  - select_template_variant: reads job_family from enriched JD, no new classification
  - render_cv_template: Jinja2 rendering with selected evidence slots
excludes:
  - LLM call (generate_cv requires live model — not tested here)
  - CV validation (owned by Task 14, validator.py)
tags:
  - fast
  - ci-safe
"""

import sys
import types
import json
from pathlib import Path

import pytest

from fitcv.cv_generator import (
    _normalize_cv_markdown,
    build_empty_structured_cv,
    build_generation_prompt,
    build_structured_generation_prompt,
    generate_cv,
    render_cv_markdown,
    _normalize_structured_cv,
    render_cv_template,
    select_template_variant,
    validate_structured_cv,
)


# ── build_generation_prompt ───────────────────────────────────────────────────

def test_build_generation_prompt_contains_evidence() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[{"name": "GA4 Project", "skills": ["SQL"]}],
        gap={"matched": ["SQL"], "missing": []},
        template="# {{ candidate.name }}",
    )
    assert "GA4 Project" in prompt
    assert "SQL" in prompt


def test_build_generation_prompt_includes_missing_skills() -> None:
    """Missing skills from the gap should appear in the prompt as constraints."""
    prompt = build_generation_prompt(
        jd={"title": "DE", "required_skills": ["SQL", "Terraform"]},
        evidence=[{"name": "X", "skills": ["SQL"]}],
        gap={"matched": ["SQL"], "missing": ["Terraform"]},
        template="",
    )
    assert "Terraform" in prompt


def test_build_generation_prompt_includes_jd_title() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Analytics Engineer", "required_skills": []},
        evidence=[],
        gap={"matched": [], "missing": []},
        template="",
    )
    assert "Analytics Engineer" in prompt


def test_build_generation_prompt_empty_evidence_no_crash() -> None:
    """Empty evidence list must not crash."""
    prompt = build_generation_prompt(
        jd={"title": "DE", "required_skills": []},
        evidence=[],
        gap={"matched": [], "missing": []},
        template="",
    )
    assert isinstance(prompt, str)


def test_build_generation_prompt_includes_grounding_constraints_from_profile() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[{"name": "GA4 Project", "skills": ["SQL"]}],
        gap={"matched": ["SQL"], "missing": []},
        template="",
        profile={
            "experiences": [{"company": "Acme Analytics GmbH"}],
            "projects": [{"name": "FitCV Pipeline"}],
        },
    )
    assert "Acme Analytics GmbH" in prompt
    assert "FitCV Pipeline" in prompt
    assert "Do not invent employer names" in prompt
    assert "Do not invent project names" in prompt


def test_build_generation_prompt_requires_exact_candidate_name_in_header() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[{"name": "GA4 Project", "skills": ["SQL"]}],
        gap={"matched": ["SQL"], "missing": []},
        template="",
        profile={
            "name": "Jane Doe",
            "experiences": [],
            "projects": [],
            "skills": [],
        },
    )
    assert "Use this exact candidate name in the header: Jane Doe" in prompt
    assert "Do not output placeholder names such as Candidate Name" in prompt


def test_build_generation_prompt_restricts_skills_section_to_candidate_skill_whitelist() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[{"name": "GA4 Project", "skills": ["SQL"]}],
        gap={"matched": ["SQL"], "missing": []},
        template="",
        profile={
            "skills": [{"name": "SQL"}, {"name": "Python"}],
            "experiences": [],
            "projects": [],
        },
    )
    assert "In the Skills section, only use skills from this approved list" in prompt
    assert "SQL, Python" in prompt


def test_build_generation_prompt_includes_grouped_experience_blocks() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL", "BigQuery"]},
        evidence=[
            {
                "evidence_type": "experience_entry",
                "role": "Senior Data Engineer",
                "company": "Acme Analytics GmbH",
                "start": "2023-01",
                "end": "present",
                "bullets": [
                    "Built GA4 to BigQuery pipeline processing 2M daily events.",
                    "Designed Pub/Sub to Dataflow streaming architecture.",
                ],
                "skills": ["BigQuery", "SQL", "Python"],
                "name": "Senior Data Engineer — Acme Analytics GmbH",
            }
        ],
        gap={"matched": ["SQL"], "missing": []},
        template="",
    )
    assert "Experience Entry" in prompt
    assert "Role: Senior Data Engineer" in prompt
    assert "Company: Acme Analytics GmbH" in prompt
    assert "Dates: 2023-01 to present" in prompt
    assert "Built GA4 to BigQuery pipeline processing 2M daily events." in prompt


def test_build_generation_prompt_keeps_project_evidence_available() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL", "Python"]},
        evidence=[
            {
                "evidence_type": "project",
                "name": "FitCV",
                "skills": ["Python", "SQL"],
                "business_value": "End-to-end CV generation pipeline",
            }
        ],
        gap={"matched": ["SQL"], "missing": []},
        template="",
    )
    assert "Selected Evidence" in prompt
    assert "- FitCV: Python, SQL" in prompt


def test_build_generation_prompt_includes_grouped_project_blocks() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["Python", "BigQuery"]},
        evidence=[
            {
                "evidence_type": "project_entry",
                "name": "FitCV — AI-Powered CV Generation Pipeline",
                "duration": "2024-01 — present",
                "skills": ["Python", "BigQuery", "Gemini"],
                "tech_stack": [
                    "Backend: Python, FastAPI",
                    "Data: BigQuery",
                    "AI: Gemini",
                ],
                "business_value": "Reduced manual CV tailoring time from 2 hours to under 5 minutes.",
                "highlights": [
                    "Ingested 5000+ postings",
                    "Achieved 89% relevance score",
                ],
            }
        ],
        gap={"matched": ["Python"], "missing": []},
        template="",
    )
    assert "Project Entry" in prompt
    assert "Name: FitCV — AI-Powered CV Generation Pipeline" in prompt
    assert "Duration: 2024-01 — present" in prompt
    assert "Business value: Reduced manual CV tailoring time from 2 hours to under 5 minutes." in prompt
    assert "Relevant stack:" in prompt
    assert "- Backend: Python, FastAPI" in prompt
    assert "Relevant highlights:" in prompt
    assert "- Ingested 5000+ postings" in prompt


def test_build_generation_prompt_sparse_project_entry_degrades_gracefully() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Analytics Engineer", "required_skills": ["Python"]},
        evidence=[
            {
                "evidence_type": "project_entry",
                "name": "Internal Reporting Tool",
                "duration": "2022",
                "skills": ["Python", "SQL"],
                "tech_stack": [],
                "business_value": "",
                "highlights": [],
            }
        ],
        gap={"matched": ["Python"], "missing": []},
        template="",
    )
    assert "Project Entry" in prompt
    assert "Name: Internal Reporting Tool" in prompt
    assert "Duration: 2022" in prompt
    assert "Relevant stack:" not in prompt
    assert "Relevant highlights:" not in prompt
    assert "Business value:" not in prompt


def test_build_generation_prompt_includes_role_level_supporting_evidence_and_adaptive_guidance() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Analytics Engineer", "required_skills": ["Analytics", "Python"]},
        evidence=[
            {
                "evidence_type": "experience_entry",
                "role": "Data Engineer",
                "company": "Fintech Startup GmbH",
                "start": "2021-06",
                "end": "2022-12",
                "bullets": [
                    "Built self-service Looker dashboards for KPI monitoring.",
                    "Automated KPI reporting workflows for analytics stakeholders.",
                ],
                "skills": ["Analytics", "Python", "Looker"],
            },
            {
                "evidence_type": "achievement",
                "name": "Reduced ad-hoc reporting requests by 60%",
                "skills": ["Analytics"],
                "business_value": "",
            },
        ],
        gap={"matched": ["Analytics"], "missing": []},
        template="",
    )
    assert "Supporting evidence:" in prompt
    assert "- Achievement: Reduced ad-hoc reporting requests by 60%" in prompt
    assert "summarize or combine grounded facts where helpful" in prompt
    assert "emphasize the bullets most relevant to the target JD" in prompt


def test_build_generation_prompt_requires_selected_evidence_grounding() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Analyst", "required_skills": ["SQL"]},
        evidence=[
            {
                "evidence_type": "experience_entry",
                "role": "Data Analyst",
                "company": "ACME",
                "skills": ["SQL"],
                "matched_channels": ["required_skill_support", "responsibility_alignment"],
                "selection_reasons": ["required_skill_support", "responsibility_alignment"],
            }
        ],
        gap={"matched": ["SQL"], "missing": []},
        template="",
        evidence_selection_summary={"selected_evidence_ids": ["exp-1"], "selected_evidence_count": 1},
    )
    assert "Stay within the selected evidence bundle for this job." in prompt
    assert "If a responsibility, domain, or role-positioning claim is not supported by the selected evidence, omit it." in prompt


# ── select_template_variant ───────────────────────────────────────────────────

def test_select_template_variant_returns_known_string() -> None:
    """select_template_variant reads job_family from enriched JD — no new classification."""
    jd = {"job_family": "data_engineering"}
    variant = select_template_variant(jd)
    assert isinstance(variant, str)
    assert len(variant) > 0


def test_select_template_variant_known_families() -> None:
    """Each documented job_family returns a non-empty variant string."""
    families = ["data_engineering", "analytics", "data_science", "ml_engineering"]
    for family in families:
        variant = select_template_variant({"job_family": family})
        assert isinstance(variant, str) and len(variant) > 0


def test_select_template_variant_unknown_family_returns_default() -> None:
    """Unknown or missing job_family → a safe default (not a crash)."""
    assert isinstance(select_template_variant({}), str)
    assert isinstance(select_template_variant({"job_family": None}), str)
    assert isinstance(select_template_variant({"job_family": "unknown_role"}), str)


# ── render_cv_template ────────────────────────────────────────────────────────

def test_render_cv_template_fills_slots() -> None:
    """Jinja2 template renders with selected_skills, selected_experiences, selected_projects."""
    template_str = "Skills: {{ selected_skills | join(', ') }}"
    rendered = render_cv_template(
        template_str=template_str,
        selected_skills=["SQL", "Python"],
        selected_experiences=[],
        selected_projects=[],
        candidate={"name": "Jane Doe"},
        headline="Senior Data Engineer",
        summary="Experienced DE.",
    )
    assert "SQL" in rendered
    assert "Python" in rendered


def test_render_cv_template_candidate_name() -> None:
    template_str = "# {{ candidate.name }}"
    rendered = render_cv_template(
        template_str=template_str,
        selected_skills=[],
        selected_experiences=[],
        selected_projects=[],
        candidate={"name": "Alice"},
        headline="",
        summary="",
    )
    assert "Alice" in rendered


def test_render_cv_template_experience_bullets() -> None:
    template_str = (
        "{% for exp in selected_experiences %}"
        "{{ exp.role }} at {{ exp.company }}"
        "{% endfor %}"
    )
    rendered = render_cv_template(
        template_str=template_str,
        selected_skills=[],
        selected_experiences=[
            {"role": "DE", "company": "Acme", "start": "2021", "end": "2023", "bullets": []}
        ],
        selected_projects=[],
        candidate={"name": "Bob"},
        headline="",
        summary="",
    )
    assert "DE" in rendered
    assert "Acme" in rendered


def test_build_empty_structured_cv_preserves_empty_section_defaults() -> None:
    doc = build_empty_structured_cv(
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )
    assert doc["schema_version"] == "cv_doc_v1"
    assert doc["sections"]["experience"] == []
    assert doc["sections"]["projects"] == []
    assert doc["sections"]["education"] == []
    assert doc["sections"]["skills"]["groups"] == []
    assert doc["sections"]["certifications"] == []
    assert doc["sections"]["languages"] == []


def test_validate_structured_cv_accepts_valid_shape() -> None:
    doc = build_empty_structured_cv(
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )
    doc["sections"]["summary"] = {"text": "Experienced analyst."}
    doc["sections"]["skills"] = {
        "groups": [{"label": "Core", "items": ["SQL", "Python"]}],
    }
    validate_structured_cv(doc)


def test_validate_structured_cv_rejects_missing_required_sections() -> None:
    doc = build_empty_structured_cv(
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )
    del doc["sections"]["summary"]
    with pytest.raises(ValueError, match="summary"):
        validate_structured_cv(doc)


def test_validate_structured_cv_respects_config_required_sections_when_provided() -> None:
    doc = build_empty_structured_cv(
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )
    del doc["sections"]["summary"]

    config = {
        "cv": {
            "composition": {
                "summary": {"enabled": False},
                "experience": {"enabled": True, "required": True},
                "skills": {"enabled": True, "required": True},
            }
        },
        "required_cv_sections": ["Experience", "Skills"],
    }

    validate_structured_cv(doc, config=config)


def test_validate_structured_cv_rejects_malformed_skills_groups() -> None:
    doc = build_empty_structured_cv(
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )
    doc["sections"]["skills"] = {"groups": [{"label": "Core", "items": "SQL"}]}
    with pytest.raises(ValueError, match="skills.groups"):
        validate_structured_cv(doc)


def test_render_cv_markdown_consumes_structured_cv(tmp_path: Path) -> None:
    """@proves cv_system.structured-cv-generation"""
    template_path = tmp_path / "cv_template.md"
    template_path.write_text(
        "# {{ candidate.name }}\n"
        "**{{ headline }}**\n\n"
        "## Summary\n"
        "{{ summary }}\n\n"
        "## Skills\n"
        "{{ selected_skills | join(', ') }}\n\n"
        "## Experience\n"
        "{% for exp in selected_experiences %}- {{ exp.role }} at {{ exp.company }}\n{% endfor %}",
        encoding="utf-8",
    )
    structured_cv = build_empty_structured_cv(
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )
    structured_cv["sections"]["header"] = {
        "name": "Jane Doe",
        "title": "Senior Data Analyst",
        "location": "Berlin",
        "contact": {"email": None, "phone": None, "linkedin": None},
    }
    structured_cv["sections"]["summary"] = {"text": "Builds decision-grade analytics."}
    structured_cv["sections"]["skills"] = {
        "groups": [{"label": "Core", "items": ["SQL", "Python"]}],
    }
    structured_cv["sections"]["experience"] = [
        {
            "role": "Data Analyst",
            "company": "Acme",
            "start": "2022-01",
            "end": "2025-03",
            "location": None,
            "bullets": ["Built KPI reporting."],
        }
    ]

    rendered = render_cv_markdown(
        structured_cv,
        {"cv": {"preset": "europass"}, "_template_path": str(template_path)},
    )

    assert "Jane Doe" in rendered
    assert "Senior Data Analyst" in rendered
    assert "Builds decision-grade analytics." in rendered
    assert "SQL, Python" in rendered
    assert "Data Analyst at Acme" in rendered


def test_render_cv_markdown_omits_disabled_sections_from_final_markdown(tmp_path: Path) -> None:
    template_path = tmp_path / "cv_template.md"
    template_path.write_text(
        "# {{ candidate.name }}\n"
        "**{{ headline }}**\n\n"
        "## Summary\n"
        "{{ summary }}\n\n"
        "## Skills\n"
        "{{ selected_skills | join(', ') }}\n\n"
        "## Experience\n"
        "{% for exp in selected_experiences %}- {{ exp.role }} at {{ exp.company }}\n{% endfor %}\n"
        "## Education\n"
        "{% for edu in selected_education %}- {{ edu.degree }}\n{% endfor %}\n",
        encoding="utf-8",
    )
    structured_cv = build_empty_structured_cv(
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )
    structured_cv["sections"]["header"] = {
        "name": "Jane Doe",
        "title": "Senior Data Analyst",
        "location": "Berlin",
        "contact": {"email": None, "phone": None, "linkedin": None},
    }
    structured_cv["sections"]["summary"] = {"text": "Builds decision-grade analytics."}
    structured_cv["sections"]["skills"] = {
        "groups": [{"label": "Core", "items": ["SQL", "Python"]}],
    }
    structured_cv["sections"]["experience"] = [
        {
            "role": "Data Analyst",
            "company": "Acme",
            "start": "2022-01",
            "end": "2025-03",
            "location": None,
            "bullets": ["Built KPI reporting."],
        }
    ]
    structured_cv["sections"]["education"] = [
        {
            "degree": "MSc Data Science",
            "institution": "TU Berlin",
            "field": None,
            "start": "2019",
            "end": "2021",
        }
    ]

    rendered = render_cv_markdown(
        structured_cv,
        {
            "cv": {
                "preset": "europass",
                "composition": {
                    "summary": {"enabled": False},
                    "skills": {"enabled": False},
                    "experience": {"enabled": True},
                    "education": {"enabled": False},
                },
            },
            "_template_path": str(template_path),
        },
    )

    assert "## Experience" in rendered
    assert "Data Analyst at Acme" in rendered
    assert "## Summary" not in rendered
    assert "Builds decision-grade analytics." not in rendered
    assert "## Skills" not in rendered
    assert "SQL, Python" not in rendered
    assert "## Education" not in rendered
    assert "MSc Data Science" not in rendered


def test_normalize_structured_cv_coerces_null_section_lists() -> None:
    normalized = _normalize_structured_cv(
        {
            "sections": {
                "header": {"name": "Jane Doe", "title": "Data Analyst"},
                "summary": {"text": "Grounded summary."},
                "experience": None,
                "projects": None,
                "education": None,
                "skills": {"groups": None},
                "certifications": None,
                "publications": None,
                "languages": None,
            }
        },
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )

    assert normalized["sections"]["experience"] == []
    assert normalized["sections"]["projects"] == []
    assert normalized["sections"]["education"] == []
    assert normalized["sections"]["skills"]["groups"] == []
    assert normalized["sections"]["certifications"] == []
    assert normalized["sections"]["publications"] == []
    assert normalized["sections"]["languages"] == []


def test_normalize_structured_cv_filters_synthetic_education_rows() -> None:
    normalized = _normalize_structured_cv(
        {
            "sections": {
                "header": {"name": "Jane Doe", "title": "Data Analyst"},
                "summary": {"text": "Grounded summary."},
                "education": [
                    {
                        "degree": "Not specified",
                        "institution": "None",
                        "field": None,
                        "start": "None",
                        "end": "None",
                    },
                    {
                        "degree": "MSc Data Science",
                        "institution": "TU Berlin",
                        "field": None,
                        "start": "2019",
                        "end": "2021",
                    },
                ],
            }
        },
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass", "composition": {"education": {"enabled": True}}}},
        fit_classification="strong",
    )

    assert len(normalized["sections"]["education"]) == 1
    assert normalized["sections"]["education"][0]["degree"] == "MSc Data Science"

def test_normalize_structured_cv_filters_synthetic_rows_in_other_sections() -> None:
    normalized = _normalize_structured_cv(
        {
            "sections": {
                "header": {"name": "Jane Doe", "title": "Data Analyst"},
                "summary": {"text": "Grounded summary."},
                "projects": [
                    {"name": "Not specified", "context": "None", "bullets": ["None"]},
                    {"name": "FitCV", "context": "Pipeline", "bullets": ["Shipped release"]},
                ],
                "certifications": [
                    {"name": "None", "issuer": "None", "year": "None"},
                    {"name": "AWS SA", "issuer": "Amazon", "year": "2024"},
                ],
                "publications": [
                    {"title": "Not provided", "publisher": "None", "year": "None"},
                    {"title": "Paper A", "publisher": "ACM", "year": "2023"},
                ],
                "languages": [
                    {"name": "None", "level": "None"},
                    {"name": "English", "level": "C2"},
                ],
            }
        },
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )

    assert len(normalized["sections"]["projects"]) == 1
    assert normalized["sections"]["projects"][0]["name"] == "FitCV"
    assert len(normalized["sections"]["certifications"]) == 1
    assert normalized["sections"]["certifications"][0]["name"] == "AWS SA"
    assert len(normalized["sections"]["publications"]) == 1
    assert normalized["sections"]["publications"][0]["title"] == "Paper A"
    assert len(normalized["sections"]["languages"]) == 1
    assert normalized["sections"]["languages"][0]["name"] == "English"

def test_normalize_cv_markdown_is_deterministic() -> None:
    raw = "# Jane Doe\r\n\r\n## Experience\r\n* built pipelines\r\n\r\n\r\n## Projects\r\n• shipped feature\r\n"
    once = _normalize_cv_markdown(raw)
    twice = _normalize_cv_markdown(once)
    assert once == twice
    assert "\r" not in once
    assert "* built pipelines" not in once
    assert "• shipped feature" not in once
    assert "- built pipelines" in once
    assert "- shipped feature" in once

def test_normalize_cv_markdown_compacts_blank_lines_between_bullets() -> None:
    raw = (
        "# Jane Doe\n"
        "## Experience\n"
        "- Built ETL\n"
        "\n"
        "- Improved latency\n"
    )
    normalized = _normalize_cv_markdown(raw)
    assert "- Built ETL\n- Improved latency" in normalized

def test_render_cv_markdown_applies_normalization(tmp_path: Path) -> None:
    template_path = tmp_path / "cv_template.md"
    template_path.write_text(
        "# {{ candidate.name }}\n"
        "**{{ headline }}**\n"
        "## Summary\n"
        "{{ summary }}\n"
        "## Experience\n"
        "{% for exp in selected_experiences %}\n"
        "### {{ exp.role }}\n"
        "{% for bullet in exp.bullets %}\n"
        "* {{ bullet }}\n"
        "{% endfor %}\n"
        "{% endfor %}\n"
        "## Projects\n"
        "{% for proj in selected_projects %}\n"
        "### {{ proj.name }}\n"
        "{{ proj.description }}\n"
        "{% endfor %}\n",
        encoding="utf-8",
    )
    structured_cv = build_empty_structured_cv(
        jd={"title": "Data Engineer"},
        profile={"name": "Jane Doe"},
        config={},
        fit_classification="strong",
    )
    structured_cv["sections"]["summary"] = {"text": "Grounded summary"}
    structured_cv["sections"]["experience"] = [
        {
            "role": "Data Engineer",
            "company": "Acme",
            "start": "2020-01",
            "end": "2024-01",
            "location": None,
            "bullets": ["Built ETL"],
        }
    ]
    structured_cv["sections"]["projects"] = [
        {
            "name": "FitCV",
            "context": "Pipeline project",
            "bullets": ["Shipped release"],
        }
    ]
    rendered = render_cv_markdown(
        structured_cv,
        {
            "cv": {"preset": "europass"},
            "_template_path": str(template_path),
        },
    )
    assert "* Built ETL" not in rendered
    assert "- Built ETL" in rendered
    assert "\r" not in rendered


def test_generate_cv_uses_openai_compatible_routed_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "fitcv.cv_generator.resolve_model_routing_part",
        lambda *args, **kwargs: {
            "provider": "openai_compatible",
            "model": "cx/gpt-5.2",
            "base_url": "http://localhost:20128/v1",
        },
    )
    template_path = tmp_path / "cv_template.md"
    template_path.write_text(
        "# {{ candidate.name }}\n"
        "**{{ headline }}**\n\n"
        "## Summary\n"
        "{{ summary }}\n",
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "output_text": json.dumps(
                    {
                        "sections": {
                            "header": {"name": "Jane Doe", "title": "Senior Data Engineer"},
                            "summary": {"text": "Designs reliable data platforms."},
                        }
                    }
                )
            }

    class FakeHTTPClient:
        def __enter__(self) -> "FakeHTTPClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def post(self, url: str, headers: dict[str, str], json: dict[str, object]) -> FakeResponse:
            captured["url"] = url
            captured["model"] = json.get("model")
            return FakeResponse()

    fake_httpx = types.SimpleNamespace(Client=lambda timeout=None: FakeHTTPClient(), HTTPStatusError=Exception)
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("FITCV_LANGGRAPH_WIRE_API", "responses")
    monkeypatch.setenv("FITCV_LANGGRAPH_MODEL", "cx/gpt-5.2")

    result = generate_cv(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[{"name": "GA4 Project", "skills": ["SQL"]}],
        gap={"matched": ["SQL"], "missing": []},
        profile={"name": "Jane Doe"},
        config={
            "gcp_project": "fitcv-491123",
            "vertex_location": "us-central1",
            "cv": {
                "generation": {
                    "model": "gemini-2.5-flash",
                    "prompt_version": "v1",
                },
                "preset": "europass",
                "composition": {"summary": {"enabled": True}},
                "content_rules": {"evidence_grounded_only": True},
                "validation": {"max_pages": 2},
            },
            "_template_path": str(template_path),
        },
        fit_classification="strong",
    )

    assert result["structured_cv"]["schema_version"] == "cv_doc_v1"
    assert result["structured_cv"]["sections"]["header"]["name"] == "Jane Doe"
    assert "Designs reliable data platforms." in result["markdown"]
    assert captured["model"] == "cx/gpt-5.2"
    assert str(captured["url"]).endswith("/responses")


# ── preset-based config reads ──────────────────────────────────────────────────

def test_generate_cv_reads_model_from_nested_cv_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """generate_cv must read cv.generation.model, not flat cv_generation_model."""
    monkeypatch.setattr(
        "fitcv.cv_generator.resolve_model_routing_part",
        lambda *args, **kwargs: {
            "provider": "openai_compatible",
            "model": "cx/gpt-5.2",
            "base_url": "http://localhost:20128/v1",
        },
    )

    template_path_str = "templates/cv_template.md"
    captured_model: list[str] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "output_text": json.dumps(
                    {
                        "sections": {
                            "header": {"name": "Jane Doe", "title": "Data Engineer"},
                            "summary": {"text": "Grounded summary."},
                        }
                    }
                )
            }

    class FakeHTTPClient:
        def __enter__(self) -> "FakeHTTPClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def post(self, url: str, headers: dict[str, str], json: dict[str, object]) -> FakeResponse:
            captured_model.append(str(json.get("model") or ""))
            return FakeResponse()

    fake_httpx = types.SimpleNamespace(Client=lambda timeout=None: FakeHTTPClient(), HTTPStatusError=Exception)
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("FITCV_LANGGRAPH_WIRE_API", "responses")
    monkeypatch.setenv("FITCV_LANGGRAPH_MODEL", "cx/gpt-5.2")

    nested_config = {
        "gcp_project": "fitcv-491123",
        "vertex_location": "us-central1",
        "cv": {
            "generation": {
                "model": "gemini-3-pro",
                "prompt_version": "v2",
            },
            "preset": "europass",
            "composition": {
                "summary": {"enabled": True},
                "experience": {"enabled": True},
                "skills": {"enabled": True},
            },
            "content_rules": {
                "evidence_grounded_only": True,
                "align_jd_terminology": True,
            },
            "validation": {"max_pages": 2},
        },
        # Compatibility: flat key should NOT be used by generate_cv directly
        "cv_generation_model": "WRONG_MODEL",
    }

    template_path = tmp_path / "cv_template.md"
    template_path.write_text("# Template", encoding="utf-8")
    nested_config["_template_path"] = str(template_path)

    generate_cv(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[{"name": "Project", "skills": ["SQL"]}],
        gap={"matched": ["SQL"], "missing": []},
        profile={"name": "Jane Doe"},
        config=nested_config,
        fit_classification="strong",
    )

    assert captured_model == ["cx/gpt-5.2"]


def test_get_template_path_for_preset() -> None:
    """cv_generator can resolve template path from preset via cv_presets registry."""
    from fitcv.cv_presets import get_template_path

    preset_path = get_template_path("europass")
    # Both paths should resolve to the same template
    resolved = preset_path
    assert resolved == "templates/cv_template.md"


# ── disabled-section constraints via config ───────────────────────────────────


def test_build_generation_prompt_excludes_disabled_sections() -> None:
    """When config has a section with enabled:false, prompt must contain a 'Do NOT include' constraint."""
    config = {
        "cv": {
            "composition": {
                "education": {"enabled": False},
                "publications": {"enabled": False},
                "experience": {"enabled": True},
                "summary": {"enabled": True},
            }
        }
    }
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[],
        gap={"matched": [], "missing": []},
        template="",
        config=config,
    )
    assert "Do NOT include a 'Education' section" in prompt
    assert "Do NOT include a 'Publications' section" in prompt
    # Enabled sections must NOT have a negative constraint
    assert "Do NOT include a 'Experience' section" not in prompt
    assert "Do NOT include a 'Summary' section" not in prompt


def test_build_generation_prompt_omits_constraint_for_enabled_sections() -> None:
    """When all sections are enabled, no 'Do NOT include' constraint should appear."""
    config = {
        "cv": {
            "composition": {
                "education": {"enabled": True},
                "experience": {"enabled": True},
                "skills": {"enabled": True},
                "summary": {"enabled": True},
            }
        }
    }
    prompt = build_generation_prompt(
        jd={"title": "DE", "required_skills": []},
        evidence=[],
        gap={"matched": [], "missing": []},
        template="",
        config=config,
    )
    assert "Do NOT include" not in prompt


def test_build_generation_prompt_requires_enabled_sections_and_filters_template() -> None:
    """Prompt should explicitly require enabled sections and show only enabled template sections."""
    config = {
        "cv": {
            "composition": {
                "summary": {"enabled": True},
                "education": {"enabled": False},
                "experience": {"enabled": True},
                "skills": {"enabled": False},
                "certifications": {"enabled": True},
                "projects": {"enabled": True},
                "publications": {"enabled": False},
                "languages": {"enabled": True},
            }
        }
    }
    template = """# {{ candidate.name }}

## Summary
{{ summary }}

## Experience
...

## Education
...

## Skills
...

## Certifications
...

## Projects
...

## Publications
...

## Languages
...
"""
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[],
        gap={"matched": [], "missing": []},
        template=template,
        config=config,
    )
    assert "The generated CV MUST include these sections in this order: Summary, Experience, Certifications, Projects, Languages" in prompt
    assert "Use markdown headings exactly: '# {Candidate Name}', optional one-line subtitle, then each required section as '## Section'." in prompt
    assert "Use '- ' as the only bullet marker and keep exactly one blank line between top-level sections." in prompt
    assert "## Summary" in prompt
    assert "## Experience" in prompt
    assert "## Certifications" in prompt
    assert "## Projects" in prompt
    assert "## Languages" in prompt
    assert "## Education" not in prompt
    assert "## Skills" not in prompt
    assert "## Publications" not in prompt


def test_build_generation_prompt_includes_certification_and_language_evidence() -> None:
    config = {
        "cv": {
            "composition": {
                "certifications": {"enabled": True},
                "languages": {"enabled": True},
            }
        }
    }
    profile = {
        "certifications": [
            {"name": "Google Professional Data Engineer", "issuer": "Google Cloud", "year": 2023},
        ],
        "languages": [
            {"name": "English", "read": "C2", "write": "C2", "speak": "C2"},
        ],
    }
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[],
        gap={"matched": [], "missing": []},
        template="## Certifications\n...\n## Languages\n...",
        profile=profile,
        config=config,
    )
    assert "Use these candidate certifications when filling the Certifications section" in prompt
    assert "Google Professional Data Engineer — Google Cloud (2023)" in prompt
    assert "Use these candidate languages when filling the Languages section" in prompt
    assert "English (read: C2, write: C2, speak: C2)" in prompt


def test_build_generation_prompt_includes_education_evidence() -> None:
    config = {
        "cv": {
            "composition": {
                "education": {"enabled": True},
            }
        }
    }
    profile = {
        "education": [
            {
                "degree": "M.Sc. Data Engineering",
                "field": "Data Engineering",
                "institution": "Technical University Berlin",
                "start": "2017-10",
                "end": "2019-09",
            },
        ],
    }
    prompt = build_generation_prompt(
        jd={"title": "Data Engineer", "required_skills": ["SQL"]},
        evidence=[],
        gap={"matched": [], "missing": []},
        template="## Education\n...",
        profile=profile,
        config=config,
    )
    assert "Use these candidate education entries when filling the Education section" in prompt
    assert "M.Sc. Data Engineering — Technical University Berlin (Data Engineering) [2017-10–2019-09]" in prompt

def test_render_cv_markdown_education_dates_do_not_render_none_tokens(tmp_path: Path) -> None:
    template_path = tmp_path / "cv_template.md"
    template_path.write_text(
        "# {{ candidate.name }}\n"
        "**{{ headline }}**\n\n"
        "## Summary\n"
        "{{ summary }}\n\n"
        "## Education\n"
        "{% for edu in selected_education %}\n"
        "### {{ edu.degree }} — {{ edu.institution }}{% if edu.start or edu.end %} ({% if edu.start %}{{ edu.start }}{% endif %}{% if edu.start and edu.end %}–{% endif %}{% if edu.end %}{{ edu.end }}{% endif %}){% endif %}\n"
        "{% endfor %}\n",
        encoding="utf-8",
    )
    structured_cv = build_empty_structured_cv(
        jd={"job_url": "https://example.com/jobs/1", "title": "Data Analyst"},
        profile={"name": "Jane Doe"},
        config={"cv": {"preset": "europass"}},
        fit_classification="strong",
    )
    structured_cv["sections"]["education"] = [
        {"degree": "MSc Data Science", "institution": "TU Berlin", "field": None, "start": None, "end": None}
    ]
    rendered = render_cv_markdown(
        structured_cv,
        {"cv": {"preset": "europass"}, "_template_path": str(template_path)},
    )
    assert "None–None" not in rendered
    assert "(None" not in rendered

def test_build_generation_prompt_includes_analysis_aware_evidence_guidance() -> None:
    config = {
        "cv": {
            "composition": {
                "summary": {"enabled": True},
                "experience": {"enabled": True},
                "projects": {"enabled": True},
            }
        }
    }
    prompt = build_generation_prompt(
        jd={"title": "Data Analyst", "required_skills": ["SQL", "Python"]},
        evidence=[
            {
                "evidence_type": "experience_entry",
                "role": "Data Analyst",
                "company": "Bank Corp",
                "skills": ["SQL", "Python"],
                "bullets": ["Built KPI dashboards for retail banking stakeholders"],
                "matched_channels": ["required_skill_support", "responsibility_alignment"],
                "selection_reasons": ["required_skill_support", "responsibility_alignment"],
            },
            {
                "evidence_type": "project_entry",
                "name": "Retail Banking KPI Dashboard",
                "tech_stack": ["SQL", "Looker"],
                "highlights": ["Created stakeholder dashboards for monthly KPI reviews"],
                "matched_channels": ["domain_alignment", "role_alignment"],
                "selection_reasons": ["domain_alignment", "role_alignment"],
            },
        ],
        gap={"matched": ["SQL"], "missing": ["dbt"]},
        template="## Summary\n...\n## Experience\n...\n## Projects\n...",
        config=config,
    )
    assert "## Evidence Usage Guidance" in prompt
    assert "Use evidence tagged `required_skill_support` to justify concrete technical and skills claims." in prompt
    assert "Use evidence tagged `responsibility_alignment` to craft experience bullets around similar work and outcomes." in prompt
    assert "Matched channels: required_skill_support, responsibility_alignment" in prompt
    assert "Selection reasons: domain_alignment, role_alignment" in prompt


def test_build_structured_generation_prompt_uses_dedicated_structured_template() -> None:
    prompt = build_structured_generation_prompt(
        jd={"title": "Data Analyst", "required_skills": ["SQL", "Python"]},
        evidence=[],
        gap={"matched": ["SQL"], "missing": ["dbt"]},
        template="## Summary\n...",
        config={
            "prompts": {
                "cv_generation": {
                    "structured_write": {"prompt_id": "cv_generation.structured_write.v1"},
                }
            }
        },
    )

    assert "Generate a tailored CV as a structured JSON document." in prompt
    assert "## Markdown Output Standard" in prompt
    assert "## Structured JSON Schema" in prompt
    assert "Write only valid JSON matching the schema below." in prompt
    assert "Write only the completed CV markdown." not in prompt

def test_build_generation_prompt_consumes_extended_analysis_hints() -> None:
    prompt = build_generation_prompt(
        jd={"title": "Data Analyst", "required_skills": ["SQL", "Python"]},
        evidence=[{"evidence_type": "experience_entry", "role": "Analyst", "company": "Acme", "skills": ["SQL"]}],
        gap={
            "matched": ["SQL"],
            "missing": ["Python"],
            "do_not_claim": ["Python", "Data Vault"],
            "requirement_coverage": [
                {"requirement": "SQL", "support_strength": "supported"},
                {"requirement": "Python", "support_strength": "unsupported"},
            ],
            "section_confidence_hints": {"experience": "high", "projects": "medium"},
        },
        template="## Summary\n...\n## Experience\n...",
    )
    assert "Do NOT claim the following unsupported items: Python, Data Vault" in prompt
    assert "Prioritize these strongly supported requirements: SQL" in prompt
    assert "Treat these requirements as unsupported unless explicit evidence is present: Python" in prompt
    assert "Section confidence hints: experience=high, projects=medium" in prompt


def test_europass_template_includes_publications_section() -> None:
    template = Path("templates/cv_template.md").read_text(encoding="utf-8")
    assert "## Publications" in template
