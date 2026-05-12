"""
@meta
type: test
scope: unit
domain: tracker
covers:
  - create_cv_version_record: UUID4 version_id, ISO timestamp, full field presence
  - update_application_status: valid status accepted, notes preserved, invalid status raises ValueError
excludes:
  - BigQuery integration (store_cv_version, store_application_status)
tags:
  - fast
  - ci-safe
"""

import json
import uuid
from unittest.mock import MagicMock, patch

import pytest

from fitcv.tracker import create_cv_version_record, store_cv_version, update_application_status


# ── create_cv_version_record ──────────────────────────────────────────────────

def test_create_cv_version_record() -> None:
    record = create_cv_version_record(
        job_url="https://linkedin.com/jobs/view/123",
        run_id="test-run-123",
        enrichment_version="v1",
        vector_rank=5,
        ai_score=0.85,
        final_score=0.78,
        evidence_ids=["e1", "e2"],
        prompt_version="v1",
        cv_markdown="# CV",
        gap_summary={"matched": ["SQL"]},
        fit_classification="strong",
    )
    assert record["job_url"] == "https://linkedin.com/jobs/view/123"
    assert "version_id" in record
    assert "generated_at" in record


def test_create_cv_version_record_uuid_format() -> None:
    """version_id must be a valid UUID string."""
    record = create_cv_version_record(
        job_url="https://linkedin.com/jobs/view/123",
        run_id="test-run-123",
        enrichment_version="v1", vector_rank=1, ai_score=0.9,
        final_score=0.85, evidence_ids=[], prompt_version="v1",
        cv_markdown="# CV", gap_summary={}, fit_classification="strong",
    )
    uuid.UUID(record["version_id"])  # raises ValueError if not valid UUID


def test_create_cv_version_record_has_timestamp() -> None:
    record = create_cv_version_record(
        job_url="https://linkedin.com/jobs/view/123",
        run_id="test-run-123",
        enrichment_version="v1", vector_rank=1, ai_score=0.9,
        final_score=0.85, evidence_ids=[], prompt_version="v1",
        cv_markdown="# CV", gap_summary={}, fit_classification="strong",
    )
    assert record["generated_at"] is not None


def test_create_cv_version_record_contains_all_fields() -> None:
    """Record must contain all schema fields."""
    record = create_cv_version_record(
        job_url="https://linkedin.com/jobs/view/123",
        run_id="test-run-123",
        enrichment_version="v2", vector_rank=3, ai_score=0.7,
        final_score=0.65, evidence_ids=["e1"], prompt_version="v2",
        cv_markdown="# Jane Doe\n## Skills\nSQL",
        gap_summary={"matched": ["SQL"], "missing": []},
        fit_classification="stretch",
    )
    expected_fields = {
        "version_id", "job_url", "enrichment_version", "vector_rank",
        "ai_score", "final_score", "evidence_ids", "prompt_version",
        "cv_markdown", "gap_summary", "fit_classification", "generated_at",
    }
    assert expected_fields.issubset(record.keys())


def test_create_cv_version_record_evidence_ids_preserved() -> None:
    """evidence_ids list must be stored as-is."""
    evidence = ["ev-001", "ev-002", "ev-003"]
    record = create_cv_version_record(
        job_url="u", run_id="rid", enrichment_version="v1", vector_rank=1,
        ai_score=0.8, final_score=0.7, evidence_ids=evidence,
        prompt_version="v1", cv_markdown="# CV", gap_summary={},
        fit_classification="strong",
    )
    assert record["evidence_ids"] == evidence


def test_create_cv_version_record_includes_structured_cv_and_generation_metadata() -> None:
    structured_cv = {
        "schema_version": "cv_doc_v1",
        "sections": {"summary": {"text": "Grounded summary."}},
    }
    record = create_cv_version_record(
        job_url="u",
        run_id="rid",
        enrichment_version="v1",
        vector_rank=1,
        ai_score=0.8,
        final_score=0.7,
        evidence_ids=["ev-001"],
        prompt_version="v1",
        cv_markdown="# CV",
        gap_summary={},
        fit_classification="strong",
        cv_structured=structured_cv,
        cv_generation_model="gemini-2.5-pro",
        cv_prompt_version="cv_prompt_v3",
    )
    assert json.loads(record["cv_structured_json"]) == structured_cv
    assert record["cv_schema_version"] == "cv_doc_v1"
    assert record["cv_generation_model"] == "gemini-2.5-pro"
    assert record["cv_prompt_version"] == "cv_prompt_v3"


def test_store_cv_version_falls_back_to_legacy_schema_when_structured_columns_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")
    record = create_cv_version_record(
        job_url="u",
        run_id="rid",
        enrichment_version="v1",
        vector_rank=1,
        ai_score=0.8,
        final_score=0.7,
        evidence_ids=["ev-001"],
        prompt_version="v1",
        cv_markdown="# CV",
        gap_summary={},
        fit_classification="strong",
        cv_structured={"schema_version": "cv_doc_v1", "sections": {"summary": {"text": "x"}}},
        cv_generation_model="gemini-2.5-pro",
        cv_prompt_version="cv_prompt_v3",
    )
    fake_client = MagicMock()
    fake_client.insert_rows_json.side_effect = [
        [{"index": 0, "errors": [{"reason": "invalid", "location": "cv_prompt_version", "message": "no such field: cv_prompt_version"}]}],
        [],
    ]

    with patch("google.oauth2.service_account.Credentials.from_service_account_file", return_value=object()), patch(
        "google.cloud.bigquery.Client",
        return_value=fake_client,
    ):
        store_cv_version(
            record,
            {
                "gcp_project": "fitcv-491123",
                "bigquery_dataset": "fitcv",
                "service_account_key": "sa_key.json",
            },
        )

    assert fake_client.insert_rows_json.call_count == 2
    retried_record = fake_client.insert_rows_json.call_args_list[1][0][1][0]
    assert "cv_prompt_version" not in retried_record
    assert "cv_generation_model" not in retried_record
    assert "cv_schema_version" not in retried_record
    assert "cv_structured_json" not in retried_record

def test_store_cv_version_sqlite_mode_uses_control_plane_store(tmp_path, monkeypatch) -> None:
    record = create_cv_version_record(
        job_url="https://example.com/job/1",
        run_id="rid-sqlite",
        enrichment_version="v1",
        vector_rank=1,
        ai_score=0.9,
        final_score=0.8,
        evidence_ids=["e1"],
        prompt_version="v1",
        cv_markdown="# CV",
        gap_summary={},
        fit_classification="strong",
        cv_structured={"schema_version": "cv_doc_v1", "sections": {"summary": {"text": "x"}}},
    )
    sqlite_path = tmp_path / "fitcv_cp.sqlite3"
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "sqlite")
    monkeypatch.setenv("FITCV_CP_SQLITE_PATH", str(sqlite_path))
    store_cv_version(record, {})

    from fitcv_cp import bq_store

    rows = bq_store.list_cvs_for_run("rid-sqlite", None, project="", dataset="")
    assert len(rows) == 1
    assert rows[0]["version_id"] == record["version_id"]


# ── update_application_status ─────────────────────────────────────────────────

def test_update_application_status_valid() -> None:
    record = update_application_status(
        job_url="https://linkedin.com/jobs/view/123", status="applied"
    )
    assert record["status"] == "applied"


def test_update_application_status_preserves_notes() -> None:
    record = update_application_status(
        job_url="https://linkedin.com/jobs/view/123",
        status="interview",
        notes="Phone screen scheduled",
    )
    assert record["notes"] == "Phone screen scheduled"


def test_update_application_status_rejects_invalid_status() -> None:
    """Status not in the configured enum must raise ValueError."""
    with pytest.raises(ValueError):
        update_application_status(
            job_url="https://linkedin.com/jobs/view/123", status="ghosted"
        )


def test_update_application_status_all_valid_statuses() -> None:
    """All documented status values must be accepted without error."""
    valid_statuses = ["applied", "not_applied", "interview", "rejected", "no_response"]
    for status in valid_statuses:
        record = update_application_status(
            job_url="https://linkedin.com/jobs/view/123", status=status
        )
        assert record["status"] == status


def test_update_application_status_has_tracker_id_and_timestamp() -> None:
    """Record must have a UUID tracker_id and an updated_at timestamp."""
    record = update_application_status(
        job_url="https://linkedin.com/jobs/view/123", status="applied"
    )
    assert "tracker_id" in record
    assert "updated_at" in record
    uuid.UUID(record["tracker_id"])  # raises if invalid UUID
