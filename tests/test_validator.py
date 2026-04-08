"""
@meta
type: test
scope: unit
domain: validator
covers:
  - validate_output: missing section detection, valid flag
  - check_length_constraints: character/line-based page estimate
  - check_chronology: date ordering in source profile experiences
  - check_employer_grounding: invented employer detection in CV text
  - check_project_existence: unknown project detection in CV text
  - check_skill_provenance: skill section validation against candidate knowledge base
  - run_all_validations: aggregated output schema
excludes:
  - Subtle factual errors in bullet text (out of scope for basic grounding)
tags:
  - fast
  - ci-safe
"""

from fitcv.validator import (
    check_chronology,
    check_employer_grounding,
    check_length_constraints,
    check_project_existence,
    check_skill_provenance,
    run_all_validations,
    validate_output,
)


# ── validate_output ───────────────────────────────────────────────────────────

def test_validate_output_catches_missing_sections() -> None:
    cv = "# Name\n## Summary\nHello"
    required_sections = ["Summary", "Skills", "Experience"]
    result = validate_output(cv, required_sections)
    assert result["valid"] is False
    assert "Skills" in result["missing_sections"]
    assert "Experience" in result["missing_sections"]


def test_validate_output_passes_complete_cv() -> None:
    cv = "# Name\n## Summary\nX\n## Skills\nY\n## Experience\nZ"
    required_sections = ["Summary", "Skills", "Experience"]
    result = validate_output(cv, required_sections)
    assert result["valid"] is True
    assert result["missing_sections"] == []


def test_validate_output_rejects_empty_required_list_sections() -> None:
    cv = "# Name\n## Summary\nX\n## Certifications\n\n## Languages\n"
    required_sections = ["Summary", "Certifications", "Languages"]
    result = validate_output(cv, required_sections)
    assert result["valid"] is False
    assert "Certifications" in result["missing_sections"]
    assert "Languages" in result["missing_sections"]


def test_validate_output_accepts_non_empty_required_list_sections() -> None:
    cv = "# Name\n## Summary\nX\n## Certifications\n- Cert A\n## Languages\n- English (C2)"
    required_sections = ["Summary", "Certifications", "Languages"]
    result = validate_output(cv, required_sections)
    assert result["valid"] is True
    assert result["missing_sections"] == []


def test_validate_output_empty_required_sections() -> None:
    """No required sections → always valid."""
    result = validate_output("any text", [])
    assert result["valid"] is True


def test_validate_output_returns_full_schema() -> None:
    """validate_output result must include all schema keys."""
    result = validate_output("# CV\n## Skills\nSQL", ["Skills"])
    for key in ("valid", "missing_sections", "grounding_violations", "skill_violations", "warnings"):
        assert key in result


# ── check_length_constraints ──────────────────────────────────────────────────

def test_check_length_constraints_short_cv_passes() -> None:
    """A short CV (< 2 pages) must pass."""
    cv = "# Name\n## Summary\nShort text."
    assert check_length_constraints(cv, max_pages=2) is True


def test_check_length_constraints_very_long_cv_fails() -> None:
    """A very long CV (>> 2 pages) should fail."""
    cv = "\n".join(f"Line {i}: some content here" for i in range(200))
    assert check_length_constraints(cv, max_pages=2) is False


# ── check_chronology ──────────────────────────────────────────────────────────

def test_check_chronology_ordered_returns_empty() -> None:
    """Chronologically ordered experiences → no violations."""
    experiences = [
        {"start": "2022", "end": "2024"},
        {"start": "2019", "end": "2022"},
    ]
    assert check_chronology(experiences) == []


def test_check_chronology_overlap_returns_violation() -> None:
    """Overlapping dates in source profile experiences → violation message."""
    experiences = [
        {"start": "2019", "end": "2024"},
        {"start": "2022", "end": "2024"},
    ]
    violations = check_chronology(experiences)
    assert len(violations) > 0


def test_check_chronology_missing_dates_no_violation() -> None:
    """Missing start/end → skip (cannot determine ordering)."""
    experiences = [{"role": "DE"}]
    assert check_chronology(experiences) == []


# ── check_employer_grounding ──────────────────────────────────────────────────

def test_check_employer_grounding_catches_invented_employer() -> None:
    cv_text = "Worked at InventedCorp from 2020"
    violations = check_employer_grounding(cv_text, known_employers=["ACME", "TechCo"])
    assert len(violations) > 0
    assert any("InventedCorp" in v for v in violations)


def test_check_employer_grounding_passes_known_employer() -> None:
    cv_text = "Engineer at ACME (2019–2022)"
    violations = check_employer_grounding(cv_text, known_employers=["ACME"])
    assert violations == []


def test_check_employer_grounding_empty_known_list_returns_no_violations() -> None:
    """Empty known_employers → no grounding check possible → no violations."""
    violations = check_employer_grounding("Worked at Foo", known_employers=[])
    assert violations == []


def test_check_employer_grounding_ignores_great_expectations_tool_name() -> None:
    cv_text = "Used Great Expectations, dbt, and SQL for data quality workflows."
    violations = check_employer_grounding(cv_text, known_employers=["ACME", "TechCo"])
    assert violations == []


def test_check_employer_grounding_skips_project_name_with_em_dash() -> None:
    """Em-dash–separated project titles must not be flagged as employer names."""
    cv_text = (
        "## Projects\n"
        "### FitCV — AI-Powered CV Generation Pipeline\n"
        "Built an end-to-end CV generation system using Python and Vertex AI.\n"
        "## Experience\n"
        "### Data Engineer — ACME (2019–2022)\n"
        "- Built data pipelines"
    )
    violations = check_employer_grounding(cv_text, known_employers=["ACME"])
    assert violations == []


def test_check_employer_grounding_catches_unknown_in_experience_heading() -> None:
    """Unknown employer in Experience heading must be caught."""
    cv_text = (
        "## Experience\n"
        "### Data Engineer — InventedCorp (2020–2022)\n"
        "- Built things\n"
        "### Analyst — ACME (2018–2020)\n"
        "- Did stuff"
    )
    violations = check_employer_grounding(cv_text, known_employers=["ACME"])
    assert len(violations) == 1
    assert "InventedCorp" in violations[0]


# ── check_project_existence ───────────────────────────────────────────────────

def test_check_project_existence_catches_unknown_project() -> None:
    cv_text = "Built the Phantom Pipeline project"
    violations = check_project_existence(cv_text, known_projects=["GA4 Pipeline", "ETL System"])
    assert len(violations) > 0


def test_check_project_existence_passes_known_project() -> None:
    cv_text = "Led the GA4 Pipeline initiative"
    violations = check_project_existence(cv_text, known_projects=["GA4 Pipeline", "ETL System"])
    assert violations == []


def test_check_project_existence_empty_projects_no_violations() -> None:
    """No known_projects → nothing to check → no violations."""
    assert check_project_existence("any text", known_projects=[]) == []


def test_check_project_existence_ignores_generic_pipeline_phrase() -> None:
    cv_text = "Built a sophisticated data pipeline on Google Cloud Platform for analytics."
    violations = check_project_existence(cv_text, known_projects=["GA4 Pipeline", "ETL System"])
    assert violations == []


def test_check_project_existence_ignores_non_project_heading() -> None:
    cv_text = "## Projects\n### Self-Service Product KPI Dashboards\nBuilt analytics assets."
    violations = check_project_existence(cv_text, known_projects=["GA4 Pipeline", "ETL System"])
    assert violations == []


# ── check_skill_provenance ────────────────────────────────────────────────────

def test_check_skill_provenance_catches_unsupported_skill() -> None:
    """Skills section is validated; skill not in candidate knowledge base flagged."""
    cv_text = "## Skills\nSQL, Rust, Python"
    violations = check_skill_provenance(cv_text, candidate_skills=["SQL", "Python"])
    assert any("Rust" in v for v in violations)


def test_check_skill_provenance_passes_known_skills() -> None:
    cv_text = "## Skills\nSQL, Python"
    violations = check_skill_provenance(cv_text, candidate_skills=["SQL", "Python", "BigQuery"])
    assert violations == []


def test_check_skill_provenance_ignores_bullet_text() -> None:
    """Skill-like words in bullet text outside the Skills section must not be flagged."""
    cv_text = "## Experience\n- Built Rust-based tools\n## Skills\nSQL, Python"
    violations = check_skill_provenance(cv_text, candidate_skills=["SQL", "Python"])
    # Rust in bullet text should NOT trigger a violation (Skills section only)
    assert violations == []


def test_check_skill_provenance_accepts_synonym_equivalent_skill() -> None:
    cv_text = "## Skills\nGoogle Analytics"
    violations = check_skill_provenance(cv_text, candidate_skills=["GA4"])
    assert violations == []


# ── run_all_validations ───────────────────────────────────────────────────────

# Minimal CV config satisfying the centralized cv.yaml contract.
_CV_CONFIG: dict = {
    "required_cv_sections": ["Summary", "Skills", "Experience"],
    "cv_max_pages": 2,
}


def test_run_all_validations_output_schema() -> None:
    """run_all_validations must return the full schema."""
    profile = {
        "experiences": [{"role": "DE", "company": "ACME", "start": "2020", "end": "2022"}],
        "projects": [{"name": "GA4 Pipeline"}],
        "skills": ["SQL", "Python"],
    }
    cv_text = "# Name\n## Summary\nX\n## Skills\nSQL, Python\n## Experience\nACME"
    result = run_all_validations(cv_text, profile=profile, config=_CV_CONFIG)
    for key in ("valid", "missing_sections", "grounding_violations", "skill_violations", "warnings"):
        assert key in result


def test_run_all_validations_length_warning() -> None:
    """Overly long CV adds a warning but does not flip valid=False on its own."""
    profile: dict = {"experiences": [], "projects": [], "skills": ["SQL"]}
    long_cv = "# CV\n## Summary\nX\n## Skills\nSQL\n## Experience\nACME\n" + "\n".join(
        f"- Bullet {i}" for i in range(200)
    )
    result = run_all_validations(long_cv, profile=profile, config=_CV_CONFIG)
    assert any("length" in w.lower() for w in result["warnings"])


def test_run_all_validations_rejects_candidate_name_placeholder() -> None:
    profile = {
        "name": "Real Candidate",
        "experiences": [{"company": "ACME"}],
        "projects": [],
        "skills": ["SQL"],
    }
    cv_text = (
        "# [Candidate Name]\n"
        "## Summary\nGrounded summary\n"
        "## Skills\nSQL\n"
        "## Experience\nEngineer at ACME"
    )

    result = run_all_validations(cv_text, profile=profile, config=_CV_CONFIG)

    assert result["valid"] is False
    assert any("Candidate Name" in violation for violation in result["grounding_violations"])


def test_run_all_validations_rejects_plain_candidate_name_header() -> None:
    profile = {
        "name": "Real Candidate",
        "experiences": [{"company": "ACME"}],
        "projects": [],
        "skills": ["SQL"],
    }
    cv_text = (
        "# Candidate Name\n"
        "## Summary\nGrounded summary\n"
        "## Skills\nSQL\n"
        "## Experience\nEngineer at ACME"
    )

    result = run_all_validations(cv_text, profile=profile, config=_CV_CONFIG)

    assert result["valid"] is False
    assert any("Candidate Name" in violation for violation in result["grounding_violations"])


def test_run_all_validations_rejects_candidate_name_in_structured_header() -> None:
    profile = {
        "name": "Real Candidate",
        "experiences": [{"company": "ACME"}],
        "projects": [],
        "skills": ["SQL"],
    }
    cv_text = (
        "# Real Candidate\n"
        "## Summary\nGrounded summary\n"
        "## Skills\nSQL\n"
        "## Experience\nEngineer at ACME"
    )
    structured_cv = {
        "schema_version": "cv_doc_v1",
        "preset": "europass",
        "locale": "en",
        "sections": {
            "header": {"name": "Candidate Name", "title": "Data Analyst", "location": None, "contact": {}},
            "summary": {"text": "Grounded summary"},
            "experience": [{"role": "Engineer", "company": "ACME"}],
            "projects": [],
            "education": [],
            "skills": {"groups": [{"label": "Core", "items": ["SQL"]}]},
            "certifications": [],
            "publications": [],
            "languages": [],
        },
    }

    result = run_all_validations(
        cv_text,
        profile=profile,
        config=_CV_CONFIG,
        structured_cv=structured_cv,
    )

    assert result["valid"] is False
    assert any("Candidate Name" in violation for violation in result["grounding_violations"])


def test_run_all_validations_accepts_real_candidate_name_header() -> None:
    profile = {
        "name": "Jane Doe",
        "experiences": [{"company": "ACME"}],
        "projects": [],
        "skills": ["SQL"],
    }
    cv_text = (
        "# Jane Doe\n"
        "## Summary\nGrounded summary\n"
        "## Skills\nSQL\n"
        "## Experience\nEngineer at ACME"
    )
    structured_cv = {
        "schema_version": "cv_doc_v1",
        "preset": "europass",
        "locale": "en",
        "sections": {
            "header": {"name": "Jane Doe", "title": "Data Analyst", "location": None, "contact": {}},
            "summary": {"text": "Grounded summary"},
            "experience": [{"role": "Engineer", "company": "ACME"}],
            "projects": [],
            "education": [],
            "skills": {"groups": [{"label": "Core", "items": ["SQL"]}]},
            "certifications": [],
            "publications": [],
            "languages": [],
        },
    }

    result = run_all_validations(
        cv_text,
        profile=profile,
        config=_CV_CONFIG,
        structured_cv=structured_cv,
    )

    assert result["valid"] is True
    assert result["grounding_violations"] == []


def test_run_all_validations_accepts_skill_dicts_in_profile() -> None:
    profile = {
        "experiences": [{"role": "DE", "company": "ACME", "start": "2020", "end": "2022"}],
        "projects": [{"name": "GA4 Pipeline"}],
        "skills": [
            {"name": "SQL", "level": "advanced"},
            {"name": "Python", "level": "advanced"},
        ],
    }
    cv_text = "# Name\n## Summary\nX\n## Skills\nSQL, Python\n## Experience\nEngineer at ACME"

    result = run_all_validations(cv_text, profile=profile, config=_CV_CONFIG)

    assert result["skill_violations"] == []


def test_run_all_validations_uses_flattened_profile_skills() -> None:
    profile = {
        "experiences": [{
            "role": "DE",
            "company": "ACME",
            "start": "2020",
            "end": "2022",
            "bullets": [{"text": "Built pipelines", "skills": ["ETL", "CI/CD"]}],
        }],
        "projects": [{"name": "FitCV Pipeline", "skills": ["Gemini", "Great Expectations"]}],
        "skills": [{"name": "SQL", "level": "advanced"}],
    }
    cv_text = "# Name\n## Summary\nX\n## Skills\nSQL, ETL, CI/CD, Gemini, Great Expectations\n## Experience\nEngineer at ACME"

    result = run_all_validations(cv_text, profile=profile, config=_CV_CONFIG)

    assert result["skill_violations"] == []


# ── preset-based config reads ──────────────────────────────────────────────────

def test_run_all_validations_reads_required_sections_from_nested_cv() -> None:
    """run_all_validations must derive required sections from cv.composition."""
    cv_text = "# Name\n## Summary\nX\n## Skills\nSQL"
    profile = {"experiences": [], "projects": [], "skills": ["SQL"]}
    # Composition: summary and skills are enabled, only skills is required
    nested_config = {
        "cv": {
            "preset": "europass",
            "composition": {
                "summary": {"enabled": True},       # enabled but not required
                "skills": {"enabled": True, "required": True},  # required
            },
            "content_rules": {"evidence_grounded_only": True},
            "validation": {"max_pages": 2},
        },
        # Compatibility projection should make flat key available
        "required_cv_sections": ["Skills"],
        "cv_max_pages": 2,
    }
    result = run_all_validations(cv_text, profile=profile, config=nested_config)
    # Missing Summary is OK (it's enabled but not required), Skills is present
    assert result["valid"] is True
    assert "Summary" not in result["missing_sections"]


def test_run_all_validations_flags_empty_required_summary_from_structured_cv() -> None:
    cv_text = "# Name\n## Summary\n\n## Skills\nSQL\n## Experience\nACME"
    profile = {"experiences": [], "projects": [], "skills": ["SQL"]}
    structured_cv = {
        "schema_version": "cv_doc_v1",
        "preset": "europass",
        "locale": "en",
        "job_url": "https://example.com/jobs/1",
        "fit_classification": "strong",
        "target_role": "Data Analyst",
        "sections": {
            "header": {"name": "Jane Doe", "title": "Data Analyst", "location": None, "contact": {}},
            "summary": {"text": ""},
            "experience": [{"role": "Analyst", "company": "ACME"}],
            "projects": [],
            "education": [],
            "skills": {"groups": [{"label": "Core", "items": ["SQL"]}]},
            "certifications": [],
            "publications": [],
            "languages": [],
        },
    }
    nested_config = {
        "cv": {
            "preset": "europass",
            "composition": {
                "summary": {"enabled": True, "required": True},
                "skills": {"enabled": True, "required": True},
                "experience": {"enabled": True, "required": True},
            },
            "content_rules": {"evidence_grounded_only": True},
            "validation": {"max_pages": 2},
        },
        "required_cv_sections": ["Summary", "Skills", "Experience"],
        "cv_max_pages": 2,
    }

    result = run_all_validations(
        cv_text,
        profile=profile,
        config=nested_config,
        structured_cv=structured_cv,
    )

    assert result["valid"] is False
    assert "Summary" in result["missing_sections"]


def test_run_all_validations_reads_max_pages_from_nested_cv() -> None:
    """run_all_validations must read cv.validation.max_pages from nested config.

    Verifies that when only the nested key is provided (flat key absent),
    length checking still functions correctly.
    """
    # A CV with 200 lines — exceeds both max_pages=2 (110 lines) and max_pages=1 (55 lines)
    long_cv = "# Name\n" + "\n".join(f"Line {i}" for i in range(200))
    profile = {"experiences": [], "projects": [], "skills": []}
    # Only nested key — no flat cv_max_pages in this config
    nested_only_config = {
        "cv": {
            "validation": {"max_pages": 2},
        },
        "required_cv_sections": [],
    }
    result = run_all_validations(long_cv, profile=profile, config=nested_only_config)
    # Should have a length warning (200 lines > 2*55=110 lines)
    assert any("length" in w.lower() for w in result["warnings"]), (
        f"Expected length warning for 200-line CV with max_pages=2, got: {result['warnings']}"
    )


def test_run_all_validations_uses_content_rules_from_nested_cv() -> None:
    """run_all_validations should accept the content_rules block from nested cv config."""
    cv_text = "# Name\n## Summary\nX\n## Skills\nSQL"
    profile = {"experiences": [], "projects": [], "skills": ["SQL"]}
    nested_config = {
        "cv": {
            "composition": {
                "summary": {"enabled": True},
                "skills": {"enabled": True, "required": True},
            },
            "content_rules": {
                "evidence_grounded_only": True,
                "align_jd_terminology": True,
            },
            "validation": {"max_pages": 2},
        },
        "required_cv_sections": ["Skills"],
        "cv_max_pages": 2,
    }
    result = run_all_validations(cv_text, profile=profile, config=nested_config)
    # content_rules are informational in this validator pass;
    # the key check is that nested cv is accepted without error
    assert "content_rules" not in result.get("errors", [])


def test_run_all_validations_flags_profile_true_employer_not_selected_by_analysis_evidence() -> None:
    profile = {
        "experiences": [
            {"role": "Data Analyst", "company": "ACME"},
            {"role": "Data Engineer", "company": "FintechCo"},
        ],
        "projects": [],
        "skills": ["SQL", "Python"],
    }
    cv_text = (
        "# Name\n"
        "## Summary\nGrounded summary\n"
        "## Skills\nSQL\n"
        "## Experience\n"
        "### Data Engineer — FintechCo (2022–2024)\n"
        "- Built reporting workflows\n"
    )
    analysis_grounding = {
        "evidence_payload": [
            {
                "evidence_id": "exp-acme",
                "evidence_type": "experience_entry",
                "company": "ACME",
                "role": "Data Analyst",
                "skills": ["SQL"],
            }
        ],
        "evidence_selection_summary": {"selected_evidence_ids": ["exp-acme"]},
        "analysis_input_summary": {"job_family": "analytics"},
    }

    result = run_all_validations(
        cv_text,
        profile=profile,
        config=_CV_CONFIG,
        analysis_grounding=analysis_grounding,
    )

    assert result["valid"] is False
    assert any("selected evidence" in message.lower() for message in result["grounding_violations"])
    assert any("FintechCo" in message for message in result["deterministic_grounding_violations"])


def test_run_all_validations_supports_semantically_close_soft_claim_from_selected_evidence() -> None:
    profile = {
        "experiences": [{"role": "Data Analyst", "company": "ACME"}],
        "projects": [],
        "skills": ["SQL", "Python", "Power BI"],
    }
    cv_text = (
        "# Name\n"
        "## Summary\nDelivered stakeholder-facing dashboards and reporting workflows for retail analytics teams.\n"
        "## Skills\nSQL, Power BI\n"
        "## Experience\n"
        "### Data Analyst — ACME (2022–2024)\n"
        "- Built reporting dashboards for business stakeholders.\n"
    )
    analysis_grounding = {
        "evidence_payload": [
                {
                    "evidence_id": "exp-acme",
                    "evidence_type": "experience_entry",
                    "company": "ACME",
                    "role": "",
                    "skills": ["SQL", "Power BI"],
                    "bullets": [
                        "Maintained Power BI dashboards for sales and inventory reporting.",
                    ],
                }
            ],
        "evidence_selection_summary": {"selected_evidence_ids": ["exp-acme"]},
        "analysis_input_summary": {"job_family": "analytics"},
    }

    result = run_all_validations(
        cv_text,
        profile=profile,
        config=_CV_CONFIG,
        analysis_grounding=analysis_grounding,
    )

    assert result["valid"] is True
    assert result["semantic_grounding_violations"] == []
    assert result["support_source_summary"]["semantic_supported_soft_claims"] >= 1


def test_run_all_validations_rejects_unresolved_template_placeholders() -> None:
    profile = {
        "experiences": [{"role": "Data Analyst", "company": "ACME"}],
        "projects": [],
        "skills": ["SQL", "Python"],
    }
    cv_text = (
        "# [Your Name]\n"
        "## Summary\nAnalyst with SQL and Python experience.\n"
        "## Skills\nSQL, Python\n"
        "## Experience\n"
        "### Data Analyst — ACME (2022–2024)\n"
        "- Built reporting workflows.\n"
    )

    result = run_all_validations(cv_text, profile=profile, config=_CV_CONFIG)

    assert result["valid"] is False
    assert any("[Your Name]" in message for message in result["grounding_violations"])
