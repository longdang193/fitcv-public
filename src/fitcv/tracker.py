"""Versioning and application tracking — record generated CVs and track job applications.

Public API
----------
create_cv_version_record    : build a cv_versions record (UUID4 PK + ISO timestamp)
store_cv_version            : insert into fitcv.cv_versions (integration)
update_application_status   : validate status against enum; build tracker record
store_application_status    : insert into fitcv.application_tracker (integration)

Schema relationships
--------------------
cv_versions.version_id   (UUID4 PK)
    ↑
application_tracker.cv_version_id  (FK)
application_tracker.tracker_id     (UUID4 PK of the tracker row)

CV markdown is stored directly in cv_versions.cv_markdown — no separate generated_cvs table.

Status enum
-----------
Defined in config["application_statuses"]. Default:
  applied | not_applied | interview | rejected | no_response
update_application_status() raises ValueError for any status not in this list.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any


# ── default status enum ───────────────────────────────────────────────────────

_DEFAULT_APPLICATION_STATUSES: list[str] = [
    "applied",
    "not_applied",
    "interview",
    "rejected",
    "no_response",
]


def _get_valid_statuses(config: dict[str, Any] | None) -> list[str]:
    """Return the list of valid application statuses from config, or the built-in default."""
    if config:
        statuses = config.get("application_statuses")
        if isinstance(statuses, list) and statuses:
            return [str(s) for s in statuses]
    return _DEFAULT_APPLICATION_STATUSES


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
    ):
        legacy_record.pop(field, None)
    return legacy_record


def store_cv_version(record: dict[str, Any], config: dict[str, Any]) -> None:
    """Insert a cv_version record into fitcv.cv_versions.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.
    """
    from google.cloud import bigquery  # type: ignore[import-not-found]
    from google.oauth2 import service_account  # type: ignore[import-not-found]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
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

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.
    """
    from google.cloud import bigquery  # type: ignore[import-not-found]
    from google.oauth2 import service_account  # type: ignore[import-not-found]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
    table_ref = f"{project}.{dataset}.application_tracker"

    errors = client.insert_rows_json(table_ref, [record])
    if errors:
        raise RuntimeError(f"BigQuery insert errors for application_tracker: {errors}")
