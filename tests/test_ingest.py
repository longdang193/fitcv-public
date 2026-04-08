"""Tests for fitcv.ingest — all tests here are pure unit tests (no BigQuery)."""

import json
from pathlib import Path

import pytest

from fitcv.ingest import parse_jobs_file, prepare_raw_rows, snake_case_keys, validate_linkedin_schema


def test_parse_jobs_file_returns_list(sample_jobs_path: Path) -> None:
    jobs = parse_jobs_file(sample_jobs_path)
    assert isinstance(jobs, list)
    assert len(jobs) > 0


def test_parse_jobs_file_has_required_fields(sample_jobs_path: Path) -> None:
    jobs = parse_jobs_file(sample_jobs_path)
    required = ["title", "jobUrl", "companyName", "description", "contractType", "experienceLevel"]
    for job in jobs:
        for field in required:
            assert field in job, f"Missing field '{field}' in job: {job.get('title', '?')}"


def test_sample_data_engineer_jobs_contains_two_explicitly_flexible_data_engineer_roles() -> None:
    sample_path = Path(__file__).parent.parent / "data" / "sample_data_engineer_jobs.json"
    jobs = json.loads(sample_path.read_text())

    flexible_data_engineer_jobs = []
    for job in jobs:
        text = f"{job.get('title', '')}\n{job.get('description', '')}".lower()
        if "data engineer" not in text:
            continue
        if "hybrid" in text or "remote" in text:
            flexible_data_engineer_jobs.append(job)

    assert len(flexible_data_engineer_jobs) == 2


def test_parse_jobs_file_raises_for_missing_file() -> None:
    with pytest.raises(FileNotFoundError):
        parse_jobs_file(Path("/nonexistent/path/jobs.json"))


def test_parse_jobs_file_raises_for_non_list(tmp_path: Path) -> None:
    bad_file = tmp_path / "jobs.json"
    bad_file.write_text(json.dumps({"title": "Not a list"}))
    with pytest.raises(ValueError, match="expected a JSON array"):
        parse_jobs_file(bad_file)


def test_validate_linkedin_schema_rejects_malformed() -> None:
    bad_job = {"title": "Test"}  # missing jobUrl
    errors = validate_linkedin_schema(bad_job)
    assert len(errors) > 0
    assert any("jobUrl" in e for e in errors)


def test_validate_linkedin_schema_accepts_valid_job() -> None:
    valid_job = {
        "title": "Data Engineer",
        "jobUrl": "https://linkedin.com/jobs/view/1",
        "companyName": "ACME",
        "description": "We need SQL skills.",
        "contractType": "Full-time",
        "experienceLevel": "Entry level",
    }
    errors = validate_linkedin_schema(valid_job)
    assert errors == []


def test_snake_case_keys_converts_camel() -> None:
    job = {
        "jobUrl": "https://example.com",
        "companyName": "ACME",
        "publishedAt": "2026-01-01",
        "applicationsCount": "42 applicants",
        "contractType": "Full-time",
        "experienceLevel": "Entry level",
        "workType": "IT",
        "companyUrl": "https://linkedin.com/company/acme",
        "companyId": "12345",
        "posterFullName": "",
        "posterProfileUrl": "",
        "applyUrl": "https://apply.example.com",
        "applyType": "EASY_APPLY",
    }
    result = snake_case_keys(job)
    assert result["job_url"] == "https://example.com"
    assert result["company_name"] == "ACME"
    assert result["published_at"] == "2026-01-01"
    assert result["applications_count"] == "42 applicants"
    assert result["contract_type"] == "Full-time"
    assert result["experience_level"] == "Entry level"


def test_prepare_raw_rows_maps_schema(sample_jobs_path: Path) -> None:
    jobs = parse_jobs_file(sample_jobs_path)
    rows = prepare_raw_rows(jobs)
    assert len(rows) == len(jobs)
    required_columns = [
        "job_url", "title", "location", "company_name", "description",
        "contract_type", "experience_level", "raw_json", "ingested_at",
    ]
    for row in rows:
        for col in required_columns:
            assert col in row, f"Missing column '{col}' in row"


def test_prepare_raw_rows_preserves_raw_json(sample_jobs_path: Path) -> None:
    jobs = parse_jobs_file(sample_jobs_path)
    rows = prepare_raw_rows(jobs)
    for original, row in zip(jobs, rows):
        raw = json.loads(row["raw_json"]) if isinstance(row["raw_json"], str) else row["raw_json"]
        assert raw["jobUrl"] == original["jobUrl"]


@pytest.mark.integration
def test_load_to_bigquery_inserts_rows(sample_jobs_path: Path, config: dict) -> None:
    """Integration test — requires GOOGLE_APPLICATION_CREDENTIALS."""
    from fitcv.ingest import load_to_bigquery
    jobs = parse_jobs_file(sample_jobs_path)
    rows = prepare_raw_rows(jobs)
    inserted = load_to_bigquery(rows, config)
    assert inserted == len(rows)


def test_fetch_from_apify_returns_list(sample_jobs_path: Path) -> None:
    """Unit test — mocks urllib so no real HTTP call is made."""
    import json
    from unittest.mock import MagicMock, patch

    from fitcv.ingest import fetch_from_apify

    real_jobs = json.loads(sample_jobs_path.read_text())
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(real_jobs).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_resp):
        result = fetch_from_apify({"apify_dataset_id": "abc123", "apify_token": "fake-tok"})

    assert isinstance(result, list)
    assert len(result) == len(real_jobs)
    assert result[0]["jobUrl"] == real_jobs[0]["jobUrl"]


def test_fetch_from_apify_raises_on_non_list(sample_jobs_path: Path) -> None:
    """Unit test — API returning a non-list should raise ValueError."""
    import json
    from unittest.mock import MagicMock, patch

    from fitcv.ingest import fetch_from_apify

    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps({"error": "not a list"}).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_resp):
        with pytest.raises(ValueError, match="JSON array"):
            fetch_from_apify({"apify_dataset_id": "abc123", "apify_token": "fake-tok"})
