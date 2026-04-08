"""Tests for fitcv.rule_filter — all pure unit tests (no cloud calls)."""

import pytest

from fitcv.rule_filter import (
    apply_pre_enrichment_global_filters,
    apply_rule_filters,
    check_applicant_count,
    check_contract_type,
    check_domain_preference,
    check_experience_level,
    check_freshness,
    check_location_type,
    check_must_have_skills,
    check_seniority,
)

# ── helpers ───────────────────────────────────────────────────────────────────

def _prefs(**kwargs) -> dict:
    """Build a preferences dict with sensible defaults."""
    defaults = {
        "seniority_target": "mid",
        "location_types": ["remote"],
        "contract_types": ["Full-time"],
        "exclude_experience_levels": ["Internship"],
        "must_have_skills": [],
        "preferred_domains": [],
        # max_age_days intentionally omitted — freshness now uses global admin settings
    }
    return {**defaults, **kwargs}


def _job(**kwargs) -> dict:
    """Build a job dict with sensible defaults."""
    defaults = {
        "job_url": "http://example.com/job/1",
        "seniority": "mid",
        "location_type": "remote",
        "contract_type": "Full-time",
        "experience_level": "Entry level",
        "required_skills": ["SQL"],
        "published_at": "2026-03-10",  # recent enough for 30-day window
        "domain": "data_engineering",
    }
    return {**defaults, **kwargs}


# ── return structure ──────────────────────────────────────────────────────────

def test_apply_rule_filters_returns_passed_and_rejected() -> None:
    """Return value must always be {passed, rejected} dicts, never a flat list."""
    result = apply_rule_filters([_job(seniority="senior")], _prefs())
    assert "passed" in result and "rejected" in result
    assert isinstance(result["passed"], list)
    assert isinstance(result["rejected"], list)


def test_rejected_jobs_include_reasons() -> None:
    """Each rejected job must include a non-empty reasons list."""
    # lead is 2 steps above mid → must be rejected by seniority check
    result = apply_rule_filters([_job(seniority="lead")], _prefs())
    assert len(result["rejected"]) > 0
    assert all(len(r["reasons"]) > 0 for r in result["rejected"])
    assert all("job_url" in r for r in result["rejected"])


def test_passes_when_no_filters_violated() -> None:
    result = apply_rule_filters([_job()], _prefs(must_have_skills=["SQL"]))
    assert len(result["passed"]) == 1
    assert len(result["rejected"]) == 0


def test_unselected_must_have_skill_missing_emits_mark_not_reject_by_default() -> None:
    result = apply_rule_filters(
        [_job(job_url="http://mark-only", required_skills=["Java"])],
        _prefs(must_have_skills=["SQL"]),
        config={"rule_filter": {"selected_filters": [
            "seniority_mismatch",
            "location_type_excluded",
            "contract_type_excluded",
            "experience_level_excluded",
        ]}},
    )

    assert result["rejected"] == []
    assert result["passed"] == ["http://mark-only"]
    assert result["passed_records"] == [
        {
            "job_url": "http://mark-only",
            "marks": [
                {
                    "code": "must_have_skill_missing",
                    "message": "Missing must-have skills",
                    "details": {"missing_skills": ["sql"], "missing_count": 1},
                }
            ],
        }
    ]


def test_selected_must_have_skill_missing_rejects() -> None:
    result = apply_rule_filters(
        [_job(job_url="http://reject-me", required_skills=["Java"])],
        _prefs(must_have_skills=["SQL"]),
        config={"rule_filter": {"selected_filters": [
            "seniority_mismatch",
            "location_type_excluded",
            "contract_type_excluded",
            "experience_level_excluded",
            "must_have_skill_missing",
        ]}},
    )

    assert result["passed"] == []
    assert result["rejected"] == [
        {
            "job_url": "http://reject-me",
            "reasons": ["must_have_skill_missing"],
            "marks": [],
        }
    ]


def test_selected_and_unselected_failures_split_between_reasons_and_marks() -> None:
    result = apply_rule_filters(
        [_job(job_url="http://mixed", seniority="lead", domain="fintech", required_skills=["Java"])],
        _prefs(must_have_skills=["SQL"], preferred_domains=["analytics"]),
        config={"rule_filter": {"selected_filters": [
            "seniority_mismatch",
            "location_type_excluded",
            "contract_type_excluded",
            "experience_level_excluded",
        ]}},
    )

    assert result["passed"] == []
    assert result["rejected"] == [
        {
            "job_url": "http://mixed",
            "reasons": ["seniority_mismatch"],
            "marks": [
                {
                    "code": "must_have_skill_missing",
                    "message": "Missing must-have skills",
                    "details": {"missing_skills": ["sql"], "missing_count": 1},
                },
                {
                    "code": "domain_not_preferred",
                    "message": "Job domain is outside preferred domains",
                    "details": {
                        "job_domain": "fintech",
                        "preferred_domains": ["analytics"],
                    },
                },
            ],
        }
    ]


def test_multiple_rejection_reasons_accumulated() -> None:
    """A job that fails two checks should accumulate both reasons."""
    # lead is 2+ above mid (seniority_mismatch) AND Internship is excluded (contract_type_excluded)
    job = _job(seniority="lead", contract_type="Internship")
    result = apply_rule_filters([job], _prefs())
    assert len(result["rejected"]) == 1
    assert len(result["rejected"][0]["reasons"]) >= 2


# ── seniority ladder ──────────────────────────────────────────────────────────

def test_seniority_accepts_exact_match() -> None:
    assert check_seniority(_job(seniority="mid"), _prefs(seniority_target="mid"))


def test_seniority_accepts_one_step_below() -> None:
    """target=mid, job=associate (one below) → pass."""
    assert check_seniority(_job(seniority="associate"), _prefs(seniority_target="mid"))


def test_seniority_accepts_one_step_above() -> None:
    """target=mid, job=senior (one above) → pass (stretch)."""
    assert check_seniority(_job(seniority="senior"), _prefs(seniority_target="mid"))


def test_seniority_rejects_two_steps_above() -> None:
    """target=mid, job=lead (two above) → reject."""
    assert not check_seniority(_job(seniority="lead"), _prefs(seniority_target="mid"))


def test_seniority_rejects_two_steps_below() -> None:
    """target=mid, job=entry (two below) → reject."""
    assert not check_seniority(_job(seniority="entry"), _prefs(seniority_target="mid"))


def test_seniority_rejects_three_steps_above() -> None:
    """target=mid, job=manager → reject."""
    assert not check_seniority(_job(seniority="manager"), _prefs(seniority_target="mid"))


def test_seniority_unknown_passes() -> None:
    """None/unknown seniority → keep (do not hard-reject)."""
    assert check_seniority(_job(seniority=None), _prefs(seniority_target="mid"))


def test_seniority_unknown_string_passes() -> None:
    assert check_seniority(_job(seniority=""), _prefs(seniority_target="mid"))


# ── location type ─────────────────────────────────────────────────────────────

def test_location_accepts_matching() -> None:
    assert check_location_type(_job(location_type="remote"), _prefs(location_types=["remote"]))


def test_location_rejects_non_matching() -> None:
    assert not check_location_type(_job(location_type="onsite"), _prefs(location_types=["remote"]))


def test_location_passes_when_prefs_empty() -> None:
    """Empty location_types = no preference = accept everything."""
    assert check_location_type(_job(location_type="onsite"), _prefs(location_types=[]))


def test_location_unknown_passes_when_preferences_exist() -> None:
    assert check_location_type(_job(location_type=None), _prefs(location_types=["remote", "hybrid"]))


# ── contract type ─────────────────────────────────────────────────────────────

def test_contract_type_accepts_matching() -> None:
    assert check_contract_type(_job(contract_type="Full-time"), _prefs(contract_types=["Full-time"]))


def test_contract_type_rejects_internship() -> None:
    assert not check_contract_type(_job(contract_type="Internship"), _prefs(contract_types=["Full-time"]))


def test_contract_type_reason_code_contains_contract() -> None:
    result = apply_rule_filters([_job(contract_type="Internship")], _prefs())
    rejected = result["rejected"]
    assert any("contract_type" in r for reason in rejected for r in reason["reasons"])


def test_contract_type_rejects_profile_excluded_contract_type() -> None:
    assert not check_contract_type(
        _job(contract_type="Internship"),
        _prefs(contract_types=[], exclude_contract_types=["Internship"]),
    )


# ── experience level ──────────────────────────────────────────────────────────

def test_experience_level_excludes_internship() -> None:
    assert not check_experience_level(
        _job(experience_level="Internship"),
        _prefs(exclude_experience_levels=["Internship"]),
    )


def test_experience_level_passes_entry_level() -> None:
    assert check_experience_level(
        _job(experience_level="Entry level"),
        _prefs(exclude_experience_levels=["Internship"]),
    )


def test_experience_level_does_not_exclude_entry_level() -> None:
    assert check_experience_level(
        _job(experience_level="Entry level"),
        _prefs(exclude_experience_levels=["Internship", "Entry level"]),
    )


# ── must-have skills ──────────────────────────────────────────────────────────

def test_must_have_skills_exact_match() -> None:
    assert check_must_have_skills(_job(required_skills=["SQL", "Python"]), _prefs(must_have_skills=["SQL"]))


def test_must_have_skills_missing_skill_fails() -> None:
    assert not check_must_have_skills(_job(required_skills=["Java"]), _prefs(must_have_skills=["SQL"]))


def test_must_have_skills_empty_prefs_passes() -> None:
    """No must-have skills = always pass."""
    assert check_must_have_skills(_job(required_skills=[]), _prefs(must_have_skills=[]))


def test_must_have_skills_synonym_gcp_matches_google_cloud() -> None:
    """GCP (canonical) must match 'Google Cloud' in JD via synonym map."""
    assert check_must_have_skills(
        _job(required_skills=["Google Cloud"]),
        _prefs(must_have_skills=["GCP"]),
    )


def test_must_have_skills_synonym_k8s_matches_kubernetes() -> None:
    assert check_must_have_skills(
        _job(required_skills=["Kubernetes"]),
        _prefs(must_have_skills=["K8s"]),
    )


def test_must_have_skills_case_insensitive() -> None:
    assert check_must_have_skills(
        _job(required_skills=["bigquery"]),
        _prefs(must_have_skills=["BigQuery"]),
    )


def test_must_have_skills_prefers_canonical_skill_list_when_present() -> None:
    assert check_must_have_skills(
        _job(
            required_skills=["Python programming for data science"],
            required_skills_canonical=["python"],
        ),
        _prefs(must_have_skills=["Python"]),
    )


# ── freshness (now reads from global_settings) ───────────────────────────────

def _gs(**kwargs) -> dict:
    """Build a global_settings dict for freshness/applicant checks."""
    return {f"global_job_filters.{k}": v for k, v in kwargs.items()}


def test_freshness_accepts_recent_job() -> None:
    assert check_freshness(_job(published_at="2026-03-20"), _gs(max_age_days=30))


def test_freshness_rejects_stale_job() -> None:
    assert not check_freshness(_job(published_at="2025-01-01"), _gs(max_age_days=30))


def test_freshness_passes_when_no_published_at() -> None:
    """Missing published_at → keep (fail open)."""
    assert check_freshness(_job(published_at=None), _gs(max_age_days=30))


def test_freshness_uses_global_settings_not_prefs() -> None:
    """Candidate profile max_age_days is ignored — admin setting takes precedence."""
    # Admin says 7 days; stale job should be rejected even if prefs say 365
    old_job = _job(published_at="2025-01-01")
    # With global_settings=7 days → reject
    assert not check_freshness(old_job, _gs(max_age_days=7))


def test_freshness_falls_back_to_30_days_when_no_global_settings() -> None:
    """global_settings=None → hard-coded default of 30 days applies."""
    recent = _job(published_at="2026-03-20")
    assert check_freshness(recent, global_settings=None)
    stale = _job(published_at="2025-01-01")
    assert not check_freshness(stale, global_settings=None)



# ── domain preference ─────────────────────────────────────────────────────────

def test_domain_passes_when_no_preference() -> None:
    """Empty preferred_domains = no preference = accept all."""
    assert check_domain_preference(_job(domain="fintech"), _prefs(preferred_domains=[]))


def test_domain_accepts_matching_domain() -> None:
    assert check_domain_preference(
        _job(domain="data_engineering"),
        _prefs(preferred_domains=["data_engineering", "analytics"]),
    )


def test_domain_rejects_non_matching_domain() -> None:
    assert not check_domain_preference(
        _job(domain="fintech"),
        _prefs(preferred_domains=["data_engineering"]),
    )


def test_domain_profile_domains_key_rejects_non_matching_domain() -> None:
    assert not check_domain_preference(
        _job(domain="fintech"),
        _prefs(preferred_domains=[], domains=["analytics", "data_engineering"]),
    )


def test_domain_unknown_passes_when_preferences_exist() -> None:
    assert check_domain_preference(
        _job(domain=None),
        _prefs(preferred_domains=["analytics"]),
    )


def test_domain_preference_matches_job_family_when_domains_use_role_taxonomy() -> None:
    assert check_domain_preference(
        _job(domain="finance", job_family="data_science"),
        _prefs(preferred_domains=[], domains=["data_science", "analytics"]),
    )


# ── integration ────────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_store_filter_results_integration(config: dict) -> None:
    """Integration — inserts filter results into BigQuery."""
    from fitcv.rule_filter import store_filter_results
    result = {
        "passed": ["http://example.com/job/1"],
        "rejected": [{"job_url": "http://example.com/job/2", "reasons": ["seniority_mismatch"]}],
    }
    store_filter_results(result, "run-integration-test", config)  # should not raise


# ── Task 2: run-scoped filter results (unit) ─────────────────────────────────

def test_store_filter_results_row_includes_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """store_filter_results includes run_id in every inserted row."""
    from unittest.mock import MagicMock, patch
    captured: dict = {}

    mock_client = MagicMock()
    def _fake_insert_1(table: str, rows: list) -> list:
        captured["rows"] = rows
        return []  # no errors
    mock_client.insert_rows_json.side_effect = _fake_insert_1

    with patch("google.oauth2.service_account.Credentials") as mock_creds, \
         patch("google.cloud.bigquery.Client") as mock_bq_client:
        mock_creds.from_service_account_file.return_value = MagicMock()
        mock_bq_client.return_value = mock_client
        from fitcv.rule_filter import store_filter_results
        result = {
            "passed": ["http://example.com/job/1"],
            "rejected": [{"job_url": "http://example.com/job/2", "reasons": ["location_type_excluded"]}],
        }
        store_filter_results(result, "run-abc", {
            "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "/tmp/key.json",
        })

    rows = captured.get("rows", [])
    assert len(rows) == 2
    assert all(r.get("run_id") == "run-abc" for r in rows), "All rows must include run_id"


def test_store_filter_results_run_id_in_rejected_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rejected rows include run_id and reasons unchanged."""
    from unittest.mock import MagicMock, patch
    captured: dict = {}

    mock_client = MagicMock()
    def _fake_insert_2(table: str, rows: list) -> list:
        captured["rows"] = rows
        return []  # no errors
    mock_client.insert_rows_json.side_effect = _fake_insert_2

    with patch("google.oauth2.service_account.Credentials"), \
         patch("google.cloud.bigquery.Client") as mock_bq_client:
        mock_bq_client.return_value = mock_client
        from fitcv.rule_filter import store_filter_results
        result = {
            "passed": [],
            "rejected": [{"job_url": "https://x.com/1", "reasons": ["seniority_mismatch"]}],
        }
        store_filter_results(result, "run-xyz", {
            "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "/key.json",
        })

    rows = captured.get("rows", [])
    assert rows[0]["run_id"] == "run-xyz"
    assert rows[0]["reasons"] == ["seniority_mismatch"]
    assert rows[0]["passed"] is False


def test_store_filter_results_serializes_marks_json_as_string() -> None:
    from unittest.mock import MagicMock, patch
    import json as _json

    captured: dict = {}
    mock_client = MagicMock()

    def _fake_insert(table: str, rows: list) -> list:
        captured["rows"] = rows
        return []

    mock_client.insert_rows_json.side_effect = _fake_insert

    with patch("google.oauth2.service_account.Credentials"), \
         patch("google.cloud.bigquery.Client") as mock_bq_client:
        mock_bq_client.return_value = mock_client
        from fitcv.rule_filter import store_filter_results

        result = {
            "passed": ["https://x.com/1"],
            "passed_records": [
                {
                    "job_url": "https://x.com/1",
                    "marks": [
                        {
                            "code": "must_have_skill_missing",
                            "message": "Missing must-have skills",
                            "details": {"missing_count": 1, "missing_skills": ["dbt"]},
                        }
                    ],
                }
            ],
            "rejected": [],
        }
        store_filter_results(result, "run-marks", {
            "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "/key.json",
        })

    rows = captured["rows"]
    assert isinstance(rows[0]["marks_json"], str)
    assert _json.loads(rows[0]["marks_json"]) == [
        {
            "code": "must_have_skill_missing",
            "message": "Missing must-have skills",
            "details": {"missing_count": 1, "missing_skills": ["dbt"]},
        }
    ]


# ── check_applicant_count ─────────────────────────────────────────────────────

def test_applicant_count_passes_when_below_threshold() -> None:
    gs = _gs(applications_count_max=100)
    assert check_applicant_count(_job(applications_count=50), gs)


def test_applicant_count_passes_at_threshold() -> None:
    gs = _gs(applications_count_max=100)
    assert check_applicant_count(_job(applications_count=100), gs)


def test_applicant_count_rejects_above_threshold() -> None:
    gs = _gs(applications_count_max=100)
    assert not check_applicant_count(_job(applications_count=101), gs)


def test_applicant_count_passes_when_null() -> None:
    """NULL applications_count → fail open."""
    gs = _gs(applications_count_max=100)
    assert check_applicant_count(_job(applications_count=None), gs)


def test_applicant_count_passes_when_no_setting() -> None:
    """No configured threshold → filter disabled."""
    assert check_applicant_count(_job(applications_count=9999), {})


# ── apply_rule_filters with global_settings ───────────────────────────────────

def test_apply_rule_filters_global_settings_none_skips_applicant_check() -> None:
    """global_settings=None → applications_count_exceeded never appears."""
    jobs = [{"job_url": "http://a", "applications_count": 9999}]
    result = apply_rule_filters(jobs, prefs={}, global_settings=None)
    assert "http://a" in result["passed"]
    all_reasons = [r for item in result["rejected"] for r in item["reasons"]]
    assert "applications_count_exceeded" not in all_reasons


def test_apply_rule_filters_global_settings_rejects_high_count() -> None:
    """High-count rejection is now handled by apply_pre_enrichment_global_filters."""
    jobs = [
        {"job_url": "http://pass", "applications_count_int": 10},
        {"job_url": "http://fail", "applications_count_int": 500},
    ]
    gs = _gs(applications_count_max=50)
    result = apply_pre_enrichment_global_filters(jobs, gs)
    assert "http://pass" in result["passed"]
    rejected_urls = {r["job_url"] for r in result["rejected"]}
    assert "http://fail" in rejected_urls
    reasons = next(r["reasons"] for r in result["rejected"] if r["job_url"] == "http://fail")
    assert "applications_count_exceeded" in reasons


# ── end-to-end: apply_settings_to_config → apply_rule_filters ────────────────

def test_admin_setting_reaches_filter_via_apply_settings_to_config() -> None:
    """Proves the full settings→config→filter chain: an admin setting reaches the filter."""
    from fitcv_cp.settings_schema import apply_settings_to_config

    config: dict = {}
    apply_settings_to_config(config, {
        "global_job_filters.applications_count_max": 50,
    })

    raw_global = config.get("global_job_filters", {})
    global_settings = {f"global_job_filters.{k}": v for k, v in raw_global.items()}

    jobs = [
        {"job_url": "http://a", "applications_count_int": 10},   # pass
        {"job_url": "http://b", "applications_count_int": 200},  # reject
    ]
    result = apply_pre_enrichment_global_filters(jobs, global_settings)

    assert "http://a" in result["passed"]
    rejected_urls = {r["job_url"] for r in result["rejected"]}
    assert "http://b" in rejected_urls
    reasons = next(r["reasons"] for r in result["rejected"] if r["job_url"] == "http://b")
    assert "applications_count_exceeded" in reasons


# ── apply_pre_enrichment_global_filters ───────────────────────────────────────

def _normalized_job(job_url="http://j", **kw):
    base = {"job_url": job_url, "published_at": None, "applications_count_int": None}
    base.update(kw)
    return base


def test_pre_filter_no_global_settings_passes_all():
    jobs = [_normalized_job("http://a"), _normalized_job("http://b")]
    result = apply_pre_enrichment_global_filters(jobs, None)
    assert set(result["passed"]) == {"http://a", "http://b"}
    assert result["rejected"] == []


def test_pre_filter_rejects_stale_job():
    from datetime import datetime, timedelta, timezone
    old = (datetime.now(tz=timezone.utc) - timedelta(days=60)).date().isoformat()
    jobs = [_normalized_job("http://stale", published_at=old)]
    result = apply_pre_enrichment_global_filters(jobs, _gs(max_age_days=30))
    assert result["passed"] == []
    assert result["rejected"][0]["reasons"] == ["job_too_stale"]


def test_pre_filter_rejects_high_count_job_using_applications_count_int():
    jobs = [_normalized_job("http://busy", applications_count_int=500)]
    result = apply_pre_enrichment_global_filters(jobs, _gs(applications_count_max=100))
    assert result["passed"] == []
    assert "applications_count_exceeded" in result["rejected"][0]["reasons"]


def test_pre_filter_null_count_passes():
    jobs = [_normalized_job("http://unkn", applications_count_int=None)]
    result = apply_pre_enrichment_global_filters(jobs, _gs(applications_count_max=50))
    assert "http://unkn" in result["passed"]


def test_check_applicant_count_prefers_count_int_over_raw_field():
    """applications_count_int=10 (within limit) wins over raw applications_count='9999'."""
    job = {"applications_count_int": 10, "applications_count": "9999"}
    gs = _gs(applications_count_max=100)
    assert check_applicant_count(job, gs) is True  # 10 <= 100


def test_check_applicant_count_falls_back_to_raw_field():
    """When applications_count_int absent, raw applications_count is used."""
    job = {"applications_count": 200}
    gs = _gs(applications_count_max=100)
    assert check_applicant_count(job, gs) is False  # 200 > 100


def test_apply_rule_filters_no_longer_rejects_stale_jobs():
    """After cheapest-first refactor, apply_rule_filters must NOT filter on freshness."""
    from datetime import datetime, timedelta, timezone
    old = (datetime.now(tz=timezone.utc) - timedelta(days=90)).date().isoformat()
    stale_job = _job(job_url="http://stale", published_at=old)
    result = apply_rule_filters([stale_job], prefs={})
    assert "http://stale" in result["passed"]


def test_apply_rule_filters_ignores_prefs_max_age_days():
    """Candidate profile max_age_days must have no effect on freshness (migration complete)."""
    from datetime import datetime, timedelta, timezone
    old = (datetime.now(tz=timezone.utc) - timedelta(days=90)).date().isoformat()
    stale_job = _job(job_url="http://stale2", published_at=old)
    result = apply_rule_filters([stale_job], prefs={"max_age_days": 1})
    assert "http://stale2" in result["passed"]
