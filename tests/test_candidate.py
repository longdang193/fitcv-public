"""Tests for fitcv.candidate — all pure unit tests."""

import uuid
from pathlib import Path

import pytest

from fitcv.candidate import (
    flatten_skills,
    infer_effective_preferences,
    load_profile_json_text,
    load_profile_yaml,
    prepare_profile_rows,
    validate_profile,
)


_VALID_PROFILE_DICT: dict = {
    "experiences": [{"id": "exp_1", "role": "DE", "company": "X", "bullets": []}],
    "skills": [{"name": "SQL"}],
    "projects": [],
    "achievements": [],
    "preferences": {"seniority_target": "mid", "location_types": ["remote"]},
}


# ── load_profile_yaml ─────────────────────────────────────────────────────────

def test_load_profile_yaml_returns_dict(sample_profile_path: Path) -> None:
    profile = load_profile_yaml(sample_profile_path)
    assert isinstance(profile, dict)


def test_load_profile_yaml_has_required_sections(sample_profile_path: Path) -> None:
    profile = load_profile_yaml(sample_profile_path)
    for section in ("experiences", "skills", "projects", "achievements", "preferences"):
        assert section in profile, f"Missing section: {section}"


def test_load_profile_yaml_raises_for_missing_file() -> None:
    with pytest.raises(FileNotFoundError):
        load_profile_yaml(Path("/nonexistent/profile.yaml"))


# ── validate_profile ────────────────────────────────────────────────────────────

def test_validate_profile_accepts_valid_profile(sample_profile_path: Path) -> None:
    profile = load_profile_yaml(sample_profile_path)
    errors = validate_profile(profile)
    assert errors == [], f"Unexpected validation errors: {errors}"


def test_validate_profile_rejects_missing_experiences() -> None:
    errors = validate_profile({"skills": [], "projects": [], "achievements": [], "preferences": {}})
    assert any("experiences" in e for e in errors)


def test_validate_profile_rejects_missing_preferences() -> None:
    errors = validate_profile({"experiences": [], "skills": [], "projects": [], "achievements": {}})
    assert any("preferences" in e for e in errors)


def test_validate_profile_rejects_duplicate_ids() -> None:
    profile = {
        "experiences": [{"id": "exp_1", "bullets": []}],
        "projects": [{"id": "exp_1", "name": "clash"}],  # duplicate
        "achievements": [],
        "skills": [],
        "preferences": {},
    }
    errors = validate_profile(profile)
    assert any("Duplicate" in e for e in errors)


def test_validate_profile_rejects_dangling_evidence_ref() -> None:
    profile = {
        "experiences": [{"id": "exp_1", "bullets": []}],
        "projects": [],
        "achievements": [],
        "skills": [{"name": "SQL", "evidence_refs": ["proj_99"]}],  # dangling
        "preferences": {},
    }
    errors = validate_profile(profile)
    assert any("proj_99" in e for e in errors)


def test_validate_profile_accepts_education_evidence_ref() -> None:
    """Skills may reference education IDs without being flagged as dangling."""
    profile = {
        "experiences": [],
        "projects": [],
        "achievements": [],
        "education": [{"id": "edu_1", "degree": "M.Sc.", "institution": "TU Berlin"}],
        "skills": [{"name": "Apache Spark", "evidence_refs": ["edu_1"]}],
        "preferences": {},
    }
    errors = validate_profile(profile)
    assert errors == [], f"Unexpected errors: {errors}"


# ── flatten_skills ────────────────────────────────────────────────────────────

def test_flatten_skills_extracts_unique() -> None:
    profile = {
        "experiences": [{"bullets": [{"skills": ["SQL", "Python"]}]}],
        "projects": [{"skills": ["SQL", "BigQuery"]}],
    }
    skills = flatten_skills(profile)
    assert "SQL" in skills
    assert len(skills) == len(set(skills))  # no duplicates


def test_flatten_skills_includes_skills_section() -> None:
    profile = {
        "skills": [{"name": "dbt"}, {"name": "Airflow"}],
        "experiences": [],
        "projects": [],
    }
    skills = flatten_skills(profile)
    assert "dbt" in skills
    assert "Airflow" in skills


def test_flatten_skills_empty_profile() -> None:
    profile: dict = {"experiences": [], "projects": [], "skills": []}
    assert flatten_skills(profile) == []


def test_infer_effective_preferences_fills_missing_values_from_recent_profile_evidence() -> None:
    profile = {
        "preferences": {"location_types": ["remote", "hybrid"]},
        "experiences": [
            {"role": "Senior Data Analyst", "role_family": "analytics", "domain_tags": ["banking"], "bullets": []},
            {"role": "BI Analyst", "domain_tags": ["retail"], "bullets": []},
        ],
        "projects": [{"name": "KPI Dashboard", "domain_tags": ["retail"]}],
        "skills": [],
        "achievements": [],
    }
    config = {
        "role_taxonomy": {
            "canonical_role_by_alias": {
                "data analyst": "data analyst",
                "senior data analyst": "data analyst",
                "bi analyst": "data analyst",
            },
            "role_family_by_role": {
                "data analyst": "analytics",
            },
        }
    }

    result = infer_effective_preferences(profile, config)

    assert result["effective_preferences"] == {
        "target_role": "Data Analyst",
        "role_families": ["analytics"],
        "domains": ["banking", "retail"],
        "location_types": ["remote", "hybrid"],
    }
    assert result["preference_sources"]["target_role"] == "inferred_recent_experience"
    assert result["preference_sources"]["role_families"] == "inferred_role_family_map"
    assert result["preference_sources"]["domains"] == "inferred_profile_domain_tags"
    assert result["preference_sources"]["location_types"] == "explicit_yaml"


def test_infer_effective_preferences_preserves_explicit_preferences() -> None:
    profile = {
        "preferences": {
            "target_role": "Analytics Engineer",
            "role_families": ["data_engineering"],
            "domains": ["fintech"],
        },
        "experiences": [
            {"role": "Senior Data Analyst", "role_family": "analytics", "domain_tags": ["banking"], "bullets": []},
        ],
        "projects": [],
        "skills": [],
        "achievements": [],
    }
    config = {
        "role_taxonomy": {
            "canonical_role_by_alias": {
                "senior data analyst": "data analyst",
                "data analyst": "data analyst",
            },
            "role_family_by_role": {
                "data analyst": "analytics",
            },
        }
    }

    result = infer_effective_preferences(profile, config)

    assert result["effective_preferences"]["target_role"] == "Analytics Engineer"
    assert result["effective_preferences"]["role_families"] == ["data_engineering"]
    assert result["effective_preferences"]["domains"] == ["fintech"]
    assert result["preference_sources"]["target_role"] == "explicit_yaml"
    assert result["preference_sources"]["role_families"] == "explicit_yaml"
    assert result["preference_sources"]["domains"] == "explicit_yaml"


# ── prepare_profile_rows ──────────────────────────────────────────────────────

def test_prepare_profile_rows_returns_all_tables(sample_profile_path: Path) -> None:
    profile = load_profile_yaml(sample_profile_path)
    rows = prepare_profile_rows(profile)
    expected_tables = {"profile", "experiences", "projects", "skills", "achievements"}
    assert expected_tables == set(rows.keys())


def test_prepare_profile_rows_profile_has_one_row(sample_profile_path: Path) -> None:
    profile = load_profile_yaml(sample_profile_path)
    rows = prepare_profile_rows(profile)
    assert len(rows["profile"]) == 1
    assert "profile_id" in rows["profile"][0]
    assert "preferences" not in rows["profile"][0]  # preferences flattened into columns


def test_prepare_profile_rows_experiences_one_per_bullet(sample_profile_path: Path) -> None:
    profile = load_profile_yaml(sample_profile_path)
    total_bullets = sum(
        len(exp.get("bullets", [])) for exp in profile.get("experiences", [])
    )
    rows = prepare_profile_rows(profile)
    assert len(rows["experiences"]) == total_bullets


def test_prepare_profile_rows_skills_has_skill_name(sample_profile_path: Path) -> None:
    profile = load_profile_yaml(sample_profile_path)
    rows = prepare_profile_rows(profile)
    assert all("skill_name" in row for row in rows["skills"])


def test_prepare_profile_rows_achievements_has_text(sample_profile_path: Path) -> None:
    profile = load_profile_yaml(sample_profile_path)
    rows = prepare_profile_rows(profile)
    assert all("text" in row for row in rows["achievements"])


# ── integration ────────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_load_candidate_to_bigquery(sample_profile_path: Path, config: dict) -> None:
    """Integration test — requires GOOGLE_APPLICATION_CREDENTIALS."""
    from fitcv.candidate import load_candidate_to_bigquery
    profile = load_profile_yaml(sample_profile_path)
    load_candidate_to_bigquery(profile, config)  # should not raise


# ── Task 3: load_profile_json_text ───────────────────────────────────────────

import json as _json


def test_load_profile_json_text_valid_returns_dict() -> None:
    payload = _json.dumps(_VALID_PROFILE_DICT)
    result = load_profile_json_text(payload)
    assert isinstance(result, dict)
    assert "experiences" in result


def test_load_profile_json_text_invalid_json_raises() -> None:
    with pytest.raises(ValueError, match="Invalid JSON"):
        load_profile_json_text("{not json}")


def test_load_profile_json_text_array_raises() -> None:
    """Top-level array is not a valid candidate profile (must be an object)."""
    with pytest.raises(ValueError, match="JSON object"):
        load_profile_json_text("[]")


def test_load_profile_json_text_missing_section_raises() -> None:
    """Profile missing required sections fails validate_profile."""
    incomplete = {"experiences": [], "skills": []}
    with pytest.raises(ValueError, match="validation failed"):
        load_profile_json_text(_json.dumps(incomplete))


def test_load_profile_json_text_preserves_all_required_sections() -> None:
    result = load_profile_json_text(_json.dumps(_VALID_PROFILE_DICT))
    for section in ("experiences", "skills", "projects", "achievements", "preferences"):
        assert section in result


def test_load_profile_json_text_normalizes_additive_alignment_metadata() -> None:
    payload = _json.dumps(
        {
            "experiences": [
                {
                    "id": "exp_1",
                    "role": "Data Analyst",
                    "company": "Acme",
                    "role_family": " analytics ",
                    "domain_tags": [" banking ", "", None, "fintech"],
                    "responsibility_themes": [" dashboarding ", " ", "kpi_reporting"],
                    "bullets": [],
                }
            ],
            "skills": [{"name": "SQL"}],
            "projects": [
                {
                    "id": "proj_1",
                    "name": "Dashboards",
                    "domain_tags": [" banking "],
                    "responsibility_themes": ["reporting_automation", ""],
                }
            ],
            "achievements": [
                {
                    "id": "ach_1",
                    "text": "Improved reporting",
                    "domain_tags": [" banking ", ""],
                }
            ],
            "preferences": {
                "target_role": " Data Analyst ",
                "role_families": [" analytics ", "", "data_science"],
                "domains": [" banking ", "", "fintech"],
            },
        }
    )

    result = load_profile_json_text(payload)

    assert result["preferences"]["target_role"] == "Data Analyst"
    assert result["preferences"]["role_families"] == ["analytics", "data_science"]
    assert result["preferences"]["domains"] == ["banking", "fintech"]
    assert result["experiences"][0]["role_family"] == "analytics"
    assert result["experiences"][0]["domain_tags"] == ["banking", "fintech"]
    assert result["experiences"][0]["responsibility_themes"] == ["dashboarding", "kpi_reporting"]
    assert result["projects"][0]["domain_tags"] == ["banking"]
    assert result["projects"][0]["responsibility_themes"] == ["reporting_automation"]
    assert result["achievements"][0]["domain_tags"] == ["banking"]
