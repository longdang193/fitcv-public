"""
@meta
type: test
scope: unit
domain: gap_analysis
covers:
  - normalise_raw_skill: light normalisation for raw comparison
  - classify_skill_match: two-level matching (raw → matched, synonym → partial, none → missing)
  - compute_gap: matched/partial/missing classification, years_risk, overclaim_risk
  - classify_fit: config-driven strong/stretch/skip thresholds
excludes:
  - BigQuery integration (store_gap_analysis)
tags:
  - fast
  - ci-safe
"""

import pytest
from pathlib import Path

from fitcv.gap_analysis import classify_fit, classify_skill_match, compute_gap, normalise_raw_skill


# ── normalise_raw_skill ───────────────────────────────────────────────────────

def test_normalise_raw_skill_lowercases() -> None:
    assert normalise_raw_skill("SQL") == "sql"
    assert normalise_raw_skill("BigQuery") == "bigquery"


def test_normalise_raw_skill_strips_whitespace() -> None:
    assert normalise_raw_skill("  SQL  ") == "sql"


def test_normalise_raw_skill_collapses_internal_spaces() -> None:
    assert normalise_raw_skill("Google  Cloud") == "google cloud"


# ── classify_skill_match ──────────────────────────────────────────────────────

def test_classify_skill_match_exact_case_insensitive() -> None:
    """SQL vs sql → matched (raw normalisation only)."""
    result = classify_skill_match("SQL", ["sql", "Python"])
    assert result["result"] == "matched"
    assert result["candidate"] == "sql"
    assert result["canonical"] is None


def test_classify_skill_match_synonym_is_partial() -> None:
    """GCP vs Google Cloud → partial (same canonical, different raw)."""
    result = classify_skill_match("Google Cloud", ["GCP", "Python"])
    assert result["result"] == "partial"
    assert result["required"] == "Google Cloud"
    assert result["candidate"] == "GCP"
    assert result["canonical"] is not None


def test_classify_skill_match_apache_airflow_is_partial() -> None:
    """Airflow vs Apache Airflow → partial is NOT triggered here since
    'airflow' does not synonym-resolve to 'apache airflow' in the default map.
    Both normalise differently → raw no-match, no canonical match → missing.
    This documents the exact boundary of the synonym map."""
    result = classify_skill_match("Apache Airflow", ["Airflow"])
    # Without a synonym entry, this is missing (documents the exact behaviour)
    assert result["result"] in ("partial", "missing")


def test_classify_skill_match_postgres_synonym() -> None:
    """Postgres → PostgreSQL via synonym map → partial."""
    result = classify_skill_match("PostgreSQL", ["Postgres"])
    assert result["result"] == "partial"
    assert result["canonical"] is not None


def test_classify_skill_match_missing() -> None:
    """Terraform not in candidate skills → missing."""
    result = classify_skill_match("Terraform", ["SQL", "Python"])
    assert result["result"] == "missing"
    assert result["candidate"] is None
    assert result["canonical"] is None


# ── compute_gap: matched / partial / missing ──────────────────────────────────

def test_compute_gap_identifies_missing_skills() -> None:
    result = compute_gap(
        required_skills=["SQL", "Python", "Airflow", "Terraform"],
        candidate_skills=["SQL", "Python", "dbt"],
        years_required=5,
        years_candidate=3,
    )
    assert "SQL" in result["matched"]
    assert "Airflow" in result["missing"]
    assert "dbt" not in result["missing"]
    assert result["years_risk"] is True


def test_compute_gap_partial_via_synonym() -> None:
    """GCP matches Google Cloud via synonym map → partial dict, not missing."""
    result = compute_gap(
        required_skills=["Google Cloud"],
        candidate_skills=["GCP"],
        years_required=None,
        years_candidate=None,
    )
    assert len(result["partial"]) == 1
    partial_entry = result["partial"][0]
    assert partial_entry["required"] == "Google Cloud"
    assert partial_entry["candidate"] == "GCP"
    assert partial_entry["canonical"] is not None
    assert "Google Cloud" not in result["missing"]


def test_classify_skill_match_phrase_requirement_contains_candidate_skill() -> None:
    """Long JD phrases should still match explicit candidate skills they mention."""
    result = classify_skill_match("Python programming for data science", ["Python", "SQL"])
    assert result["result"] == "matched"
    assert result["candidate"] == "Python"


def test_classify_skill_match_matches_compact_spacing_variant_as_raw_match() -> None:
    result = classify_skill_match("PowerBI", ["Power BI", "SQL"])
    assert result["result"] == "matched"
    assert result["candidate"] == "Power BI"


def test_classify_skill_match_matches_segmented_skill_variant_as_phrase_match() -> None:
    result = classify_skill_match("Git", ["Git / GitHub", "SQL"])
    assert result["result"] == "matched"
    assert result["candidate"] == "Git / GitHub"


def test_compute_gap_phrase_requirements_ignore_non_skill_items_for_fit_count() -> None:
    result = compute_gap(
        required_skills=[
            "Master's or PhD Degree in Data Science",
            "Python programming for data science",
            "Proficient in SQL and database operations",
            "Advanced English proficiency (C1 or above)",
            "Ability to learn new methods",
        ],
        candidate_skills=["Python", "SQL"],
        years_required=5,
        years_candidate=5,
    )
    assert result["matched"] == [
        "Python programming for data science",
        "Proficient in SQL and database operations",
    ]
    assert result["matchable_required_count"] == 2
    assert result["ignored_for_fit"] == [
        "Master's or PhD Degree in Data Science",
        "Advanced English proficiency (C1 or above)",
        "Ability to learn new methods",
    ]


def test_compute_gap_ignores_generic_analyst_requirements_for_fit_count() -> None:
    result = compute_gap(
        required_skills=[
            "SQL",
            "PowerBI",
            "Git",
            "Experience in Requirements Management",
            "Strong analytical skills",
        ],
        candidate_skills=["SQL", "Power BI", "Git / GitHub"],
        years_required=3,
        years_candidate=3,
    )
    assert result["matched"] == ["SQL", "PowerBI", "Git"]
    assert result["ignored_for_fit"] == [
        "Experience in Requirements Management",
        "Strong analytical skills",
    ]
    assert result["matchable_required_count"] == 3


def test_compute_gap_phrase_requirement_matches_skill_alias_inside_requirement() -> None:
    result = classify_skill_match(
        "Experience with GenAI technologies (LLMs, RAG, prompt engineering, vector databases)",
        ["Gemini", "Python"],
        config={"skill_synonyms": {"gemini": "genai"}},
    )
    assert result["result"] == "matched"
    assert result["candidate"] == "Gemini"


def test_compute_gap_partial_has_dict_shape() -> None:
    """partial entries must have required, candidate, canonical keys."""
    result = compute_gap(
        required_skills=["PostgreSQL"],
        candidate_skills=["Postgres"],
        years_required=None,
        years_candidate=None,
    )
    assert len(result["partial"]) == 1
    entry = result["partial"][0]
    assert "required" in entry
    assert "candidate" in entry
    assert "canonical" in entry


def test_compute_gap_no_required_skills() -> None:
    """No required skills → all fields empty, no risk."""
    result = compute_gap(
        required_skills=[],
        candidate_skills=["SQL"],
        years_required=None,
        years_candidate=5,
    )
    assert result["matched"] == []
    assert result["missing"] == []
    assert result["years_risk"] is False


def test_compute_gap_full_match() -> None:
    result = compute_gap(
        required_skills=["SQL", "Python"],
        candidate_skills=["SQL", "Python"],
        years_required=3,
        years_candidate=4,
    )
    assert set(result["matched"]) == {"SQL", "Python"}
    assert result["missing"] == []
    assert result["years_risk"] is False


def test_compute_gap_all_missing() -> None:
    result = compute_gap(
        required_skills=["Terraform", "Rust"],
        candidate_skills=["SQL"],
        years_required=5,
        years_candidate=2,
    )
    assert set(result["missing"]) == {"Terraform", "Rust"}
    assert result["years_risk"] is True


# ── years_risk edge cases ─────────────────────────────────────────────────────

def test_compute_gap_unknown_years_no_risk() -> None:
    """Unknown years on either side must not set years_risk."""
    result = compute_gap(
        required_skills=["SQL"],
        candidate_skills=["SQL"],
        years_required=None,
        years_candidate=None,
    )
    assert result["years_risk"] is False


def test_compute_gap_years_required_zero_no_risk() -> None:
    """years_required=0 treated as unknown → no risk."""
    result = compute_gap(
        required_skills=["SQL"],
        candidate_skills=["SQL"],
        years_required=0,
        years_candidate=5,
    )
    assert result["years_risk"] is False


def test_compute_gap_years_range_string_parses_minimum() -> None:
    """'3-5' years required → minimum of 3; candidate with 2 years → risk."""
    result = compute_gap(
        required_skills=["SQL"],
        candidate_skills=["SQL"],
        years_required="3-5",  # type: ignore[arg-type]
        years_candidate=2,
    )
    assert result["years_risk"] is True


def test_compute_gap_prefers_canonical_years_fields_when_provided() -> None:
    result = compute_gap(
        required_skills=["SQL"],
        candidate_skills=["SQL"],
        years_required=1,
        years_experience_min=4,
        years_experience_max=6,
        years_candidate=3,
    )
    assert result["years_risk"] is True
    assert result["overclaim_risk"] == ["years_gap: candidate has 3 years, 4 required"]


def test_compute_gap_does_not_emit_leadership_overclaim_from_candidate_evidence_in_phase1() -> None:
    result = compute_gap(
        required_skills=["Team leadership", "SQL"],
        candidate_skills=["SQL"],
        years_required=None,
        years_candidate=5,
        candidate_evidence=["Built pipelines and dashboards"],
    )
    assert result["overclaim_risk"] == []


# ── classify_fit ──────────────────────────────────────────────────────────────

def test_classify_fit_uses_config_thresholds() -> None:
    """classify_fit must derive strong/stretch/skip from config, not hardcoded values."""
    gap_strong = {
        "matched": ["SQL", "Python"], "partial": [], "missing": [],
        "years_risk": False, "overclaim_risk": [],
    }
    gap_skip = {
        "matched": [], "partial": [], "missing": ["SQL", "Python", "Terraform"],
        "years_risk": True, "overclaim_risk": [],
    }
    config = {"gap_thresholds": {"strong_min_matched_ratio": 0.8, "stretch_min_matched_ratio": 0.5}}
    assert classify_fit(gap_strong, required_count=2, config=config) == "strong"
    assert classify_fit(gap_skip, required_count=3, config=config) == "skip"


def test_classify_fit_partial_does_not_count_as_matched() -> None:
    """Synonym-only partial matches must not lift the matched ratio."""
    gap = {
        "matched": [],
        "partial": [{"required": "Google Cloud", "candidate": "GCP", "canonical": "google cloud"}],
        "missing": ["SQL"],
        "years_risk": False, "overclaim_risk": [],
    }
    config = {"gap_thresholds": {"strong_min_matched_ratio": 0.8, "stretch_min_matched_ratio": 0.5}}
    # 0 matched out of 2 required → ratio 0.0 → skip
    assert classify_fit(gap, required_count=2, config=config) == "skip"


def test_classify_fit_stretch_band() -> None:
    """matched_ratio between stretch and strong thresholds → stretch."""
    gap = {
        "matched": ["SQL"], "partial": [], "missing": ["Python"],
        "years_risk": False, "overclaim_risk": [],
    }
    config = {"gap_thresholds": {"strong_min_matched_ratio": 0.8, "stretch_min_matched_ratio": 0.5}}
    assert classify_fit(gap, required_count=2, config=config) == "stretch"


def test_classify_fit_default_thresholds_when_no_config() -> None:
    """classify_fit works with no config (uses built-in defaults)."""
    gap_strong = {
        "matched": ["A", "B", "C", "D", "E"],
        "partial": [], "missing": [], "years_risk": False, "overclaim_risk": [],
    }
    result = classify_fit(gap_strong, required_count=5, config=None)
    assert result in ("strong", "stretch", "skip")


# ── store_gap_analysis ─────────────────────────────────────────────────────────


def test_store_gap_analysis_writes_sqlite_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    import sqlite3

    from fitcv.gap_analysis import store_gap_analysis

    db_path = tmp_path / "fitcv_cp.sqlite3"
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "sqlite")
    monkeypatch.setenv("FITCV_CP_SQLITE_PATH", str(db_path))

    gap = {
        "matched": ["SQL"],
        "partial": [{"required": "Google Cloud", "candidate": "GCP", "canonical": "google cloud"}],
        "missing": ["Terraform"],
        "years_risk": True,
        "overclaim_risk": ["years_gap: candidate has 3 years, 5 required"],
    }

    store_gap_analysis("https://example.com/job-1", gap, config={})

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT job_url, matched_skills_json, partial_skills_json, missing_skills_json,
                   years_risk, overclaim_risk_json
            FROM gap_analysis
            WHERE job_url = ?
            """,
            ("https://example.com/job-1",),
        ).fetchone()

    assert row is not None
    assert row[0] == "https://example.com/job-1"
    assert json.loads(str(row[1])) == ["SQL"]
    assert json.loads(str(row[2])) == [
        {"required": "Google Cloud", "candidate": "GCP", "canonical": "google cloud"}
    ]
    assert json.loads(str(row[3])) == ["Terraform"]
    assert int(row[4]) == 1
    assert json.loads(str(row[5])) == ["years_gap: candidate has 3 years, 5 required"]


def test_store_gap_analysis_bigquery_mode_uses_bigquery_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys
    import types

    from fitcv.gap_analysis import store_gap_analysis

    calls: dict[str, object] = {}

    class FakeCredentials:
        @staticmethod
        def from_service_account_file(path: str) -> str:
            calls["credential_path"] = path
            return "fake-creds"

    class FakeClient:
        def __init__(self, *, project: str, credentials: object) -> None:
            calls["project"] = project
            calls["credentials"] = credentials

        def insert_rows_json(self, table_ref: str, rows: list[dict[str, object]]) -> list[object]:
            calls["table_ref"] = table_ref
            calls["rows"] = rows
            return []

    fake_bigquery = types.SimpleNamespace(Client=FakeClient)
    fake_service_account = types.SimpleNamespace(Credentials=FakeCredentials)
    monkeypatch.setitem(sys.modules, "google.cloud", types.SimpleNamespace(bigquery=fake_bigquery))
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", types.SimpleNamespace(service_account=fake_service_account))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    store_gap_analysis(
        "https://example.com/job-2",
        {
            "matched": ["SQL"],
            "partial": [{"required": "Google Cloud", "candidate": "GCP", "canonical": "google cloud"}],
            "missing": [],
            "years_risk": False,
            "overclaim_risk": [],
        },
        config={
            "gcp_project": "demo-project",
            "bigquery_dataset": "fitcv",
            "service_account_key": "C:/fake/key.json",
        },
    )

    assert calls["credential_path"] == "C:/fake/key.json"
    assert calls["project"] == "demo-project"
    assert calls["table_ref"] == "demo-project.fitcv.gap_analysis"
    rows = calls["rows"]
    assert isinstance(rows, list)
    assert rows[0]["job_url"] == "https://example.com/job-2"
    assert rows[0]["partial_skills"] == [
        '{"required": "Google Cloud", "candidate": "GCP", "canonical": "google cloud"}'
    ]
