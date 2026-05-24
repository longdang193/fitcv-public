"""@meta
name: tracker
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.tracker.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from fitcv.config import sqlite_mode_enabled
from fitcv.contracts import DEFAULT_APPLICATION_STATUSES
from fitcv.persistence import build_bigquery_client


# ── default status enum ───────────────────────────────────────────────────────

_DEFAULT_APPLICATION_STATUSES: tuple[str, ...] = DEFAULT_APPLICATION_STATUSES

def _get_valid_statuses(config: dict[str, Any] | None) -> list[str]:
    """Return the list of valid application statuses from config, or the built-in default."""
    if config:
        statuses = config.get("application_statuses")
        if isinstance(statuses, list) and statuses:
            return [str(s) for s in statuses]
    return list(_DEFAULT_APPLICATION_STATUSES)


# ── cv version record ─────────────────────────────────────────────────────────

def create_cv_version_record(
    job_url: str,
    run_id: str,
    enrichment_version: str,
    vector_rank: int,
    ai_score: float,
    final_score: float,
    evidence_ids: list[str],
    prompt_version: str,
    cv_markdown: str,
    gap_summary: dict[str, Any],
    fit_classification: str,
    cv_structured: dict[str, Any] | None = None,
    cv_generation_model: str | None = None,
    cv_prompt_version: str | None = None,
    cv_generation_input_fingerprint: str | None = None,
    cv_generation_reuse_status: str | None = None,
) -> dict[str, Any]:
    """Build a cv_versions record in memory.

    Fields:
    - version_id     : UUID4 string (PK)
    - generated_at   : UTC ISO-8601 timestamp
    - All kwargs passed through verbatim.
    - gap_summary    : stored as a JSON string.

    Does not write to BigQuery — call store_cv_version() for persistence.
    """
    return {
        "version_id": str(uuid.uuid4()),
        "run_id": str(run_id) if run_id else None,
        "job_url": str(job_url),
        "enrichment_version": str(enrichment_version),
        "vector_rank": int(vector_rank),
        "ai_score": float(ai_score),
        "final_score": float(final_score),
        "evidence_ids": list(evidence_ids),
        "prompt_version": str(prompt_version),
        "cv_prompt_version": str(cv_prompt_version or prompt_version),
        "cv_generation_model": str(cv_generation_model or "") or None,
        "cv_schema_version": (
            str(cv_structured.get("schema_version") or "").strip()
            if isinstance(cv_structured, dict)
            else None
        ),
        "cv_structured_json": json.dumps(cv_structured) if isinstance(cv_structured, dict) else None,
        "cv_markdown": str(cv_markdown),
        "cv_generation_input_fingerprint": str(cv_generation_input_fingerprint or "") or None,
        "cv_generation_reuse_status": str(cv_generation_reuse_status or "") or None,
        "gap_summary": json.dumps(gap_summary),
        "fit_classification": str(fit_classification),
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def _is_missing_structured_cv_column_error(errors: list[dict[str, Any]]) -> bool:
    structured_fields = {
        "cv_prompt_version",
        "cv_generation_model",
        "cv_schema_version",
        "cv_structured_json",
        "cv_generation_input_fingerprint",
        "cv_generation_reuse_status",
    }
    for error in errors:
        for item in error.get("errors") or []:
            message = str(item.get("message") or "").lower()
            location = str(item.get("location") or "")
            if "no such field" in message and any(field in message for field in structured_fields):
                return True
            if location in structured_fields and str(item.get("reason") or "").lower() == "invalid":
                return True
    return False


def _legacy_cv_version_record(record: dict[str, Any]) -> dict[str, Any]:
    legacy_record = dict(record)
    for field in (
        "cv_prompt_version",
        "cv_generation_model",
        "cv_schema_version",
        "cv_structured_json",
        "cv_generation_input_fingerprint",
        "cv_generation_reuse_status",
    ):
        legacy_record.pop(field, None)
    return legacy_record

def lookup_reusable_cv_versions(
    fingerprints: list[str],
    config: dict[str, Any],
    *,
    limit: int = 500,
) -> dict[str, dict[str, Any]]:
    normalized = [str(item or "").strip() for item in fingerprints if str(item or "").strip()]
    if not normalized:
        return {}
    if sqlite_mode_enabled(config):
        from fitcv_cp import bq_store as cp_bq_store

        return cp_bq_store.lookup_reusable_cv_versions(
            normalized,
            bq=None,
            project="",
            dataset="",
            limit=limit,
        )

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)
    from fitcv_cp import bq_store as cp_bq_store

    return cp_bq_store.lookup_reusable_cv_versions(
        normalized,
        bq=client,
        project=project,
        dataset=dataset,
        limit=limit,
    )


def store_cv_version(record: dict[str, Any], config: dict[str, Any]) -> None:
    """Insert a cv_version record into fitcv.cv_versions.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.
    """
    if sqlite_mode_enabled(config):
        from fitcv_cp import bq_store as cp_bq_store

        errors = cp_bq_store.insert_cv_version_row(
            record,
            bq=None,
            project="",
            dataset="",
        )
        if errors:
            raise RuntimeError(f"SQLite insert errors for cv_versions: {errors}")
        return

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)
    table_ref = f"{project}.{dataset}.cv_versions"

    errors = client.insert_rows_json(table_ref, [record])
    if errors and _is_missing_structured_cv_column_error(errors):
        legacy_record = _legacy_cv_version_record(record)
        errors = client.insert_rows_json(table_ref, [legacy_record])
    if errors:
        raise RuntimeError(f"BigQuery insert errors for cv_versions: {errors}")


# ── application status record ─────────────────────────────────────────────────

def update_application_status(
    job_url: str,
    status: str,
    notes: str = "",
    cv_version_id: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate status and build an application_tracker record in memory.

    Raises ValueError if status is not in the configured enum.

    Fields:
    - tracker_id    : UUID4 string (PK of this tracker row)
    - cv_version_id : FK → cv_versions.version_id (may be None)
    - updated_at    : UTC ISO-8601 timestamp
    """
    valid_statuses = _get_valid_statuses(config)
    if status not in valid_statuses:
        raise ValueError(
            f"Invalid application status '{status}'. "
            f"Must be one of: {', '.join(valid_statuses)}"
        )

    return {
        "tracker_id": str(uuid.uuid4()),
        "job_url": str(job_url),
        "cv_version_id": str(cv_version_id) if cv_version_id else None,
        "status": str(status),
        "notes": str(notes),
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def store_application_status(record: dict[str, Any], config: dict[str, Any]) -> None:
    """Insert an application_tracker record into fitcv.application_tracker.

    Requires GOOGLE_APPLICATION_CREDENTIALS for BigQuery mode.
    Decorated with @pytest.mark.integration in tests.
    """
    if sqlite_mode_enabled(config):
        from fitcv_cp import bq_store as cp_bq_store

        errors = cp_bq_store.insert_application_tracker_row(
            record,
            bq=None,
            project="",
            dataset="",
        )
        if errors:
            raise RuntimeError(f"SQLite insert errors for application_tracker: {errors}")
        return

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)
    table_ref = f"{project}.{dataset}.application_tracker"

    errors = client.insert_rows_json(table_ref, [record])
    if errors:
        raise RuntimeError(f"BigQuery insert errors for application_tracker: {errors}")







