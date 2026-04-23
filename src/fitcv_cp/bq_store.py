"""
@meta
name: control_plane_bq_store
type: utility
domain: admin_ui
responsibility:
  - Persist and read control-plane run, event, snapshot, and artifact records.
  - Keep mutating BigQuery operations parameterized.
inputs:
  - control-plane model objects
  - BigQuery table rows and query results
outputs:
  - pipeline_runs, run_events, and run-scoped snapshot updates
capabilities:
  - admin_control_plane_core.pipeline-runs-bigquery-table
  - admin_control_plane_core.pipeline-run-events-bigquery-table
  - multi_file_job_input.one-immutable-snapshot-stored-per-run
  - run_lifecycle_controls.archive-and-unarchive-terminal-runs
  - run_lifecycle_controls.full-audit-trail-in-pipeline-run-events
  - inspection_debugging.settings-used-export
  - inspection_debugging.results-ledger-inspection
  - inspection_debugging.stage-transition-diagnostics
  - inspection_debugging.enriched-job-debug-export
  - pipeline_performance.operator-facing-enriched-job-exports-now-keep-canonical-semantic-fields-and-fingerprint-reuse-provenance-while-omitting-retired-raw-duplicate-classification-baggage
  - settings_system.trigger-time-effective-settings-snapshot
  - trigger_run_management.runs-list-management
  - trigger_run_management.run-detail-actions
  - trigger_run_management.run-owned-artifact-exports
  - trigger_run_management.run-results-export
tags:
  - bigquery
  - control-plane
lifecycle:
  status: active
"""
import datetime
import json
import logging
import time
from typing import Any, Optional

from google.cloud import bigquery as bq_module

from fitcv_cp.models import PipelineRun, RunEvent, RunStatus

logger = logging.getLogger(__name__)

_PIPELINE_RUNS_UPDATE_RETRY_ATTEMPTS = 3
_PIPELINE_RUNS_UPDATE_RETRY_DELAY_SECONDS = 0.25


def _is_concurrent_pipeline_runs_update_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "pipeline_runs" in message
        and "concurrent update" in message
        and "could not serialize access" in message
    )


def _execute_query_with_pipeline_runs_retry(
    bq: Any,
    sql: str,
    *,
    job_config: bq_module.QueryJobConfig,
) -> None:
    for attempt in range(1, _PIPELINE_RUNS_UPDATE_RETRY_ATTEMPTS + 1):
        try:
            bq.query(sql, job_config=job_config).result()
            return
        except Exception as exc:
            if (
                attempt >= _PIPELINE_RUNS_UPDATE_RETRY_ATTEMPTS
                or not _is_concurrent_pipeline_runs_update_error(exc)
            ):
                raise
            logger.warning(
                "Retrying pipeline_runs concurrent update after attempt %s/%s: %s",
                attempt,
                _PIPELINE_RUNS_UPDATE_RETRY_ATTEMPTS,
                exc,
            )
            time.sleep(_PIPELINE_RUNS_UPDATE_RETRY_DELAY_SECONDS * attempt)


def insert_run(run: PipelineRun, bq: Any, *, project: str, dataset: str) -> None:
    table = f"{project}.{dataset}.pipeline_runs"
    sql = f"""
        INSERT INTO `{table}` (
            run_id, status, triggered_by, trigger_source,
            jobs_path, config_path, created_at, effective_settings_json,
            run_mode, checkpoint_status, next_stage, last_completed_stage,
            completed_stages_json, checkpoint_payload_json,
            jobs_input_source, jobs_input_json,
            candidate_profile_source, candidate_profile_json,
            queue_job_id
        )
        VALUES (
            @run_id, @status, @triggered_by, @trigger_source,
            @jobs_path, @config_path, @created_at, @effective_settings_json,
            @run_mode, @checkpoint_status, @next_stage, @last_completed_stage,
            @completed_stages_json, @checkpoint_payload_json,
            @jobs_input_source, @jobs_input_json,
            @candidate_profile_source, @candidate_profile_json,
            @queue_job_id
        )
    """
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("run_id", "STRING", run.run_id),
            bq_module.ScalarQueryParameter("status", "STRING", run.status.value),
            bq_module.ScalarQueryParameter("triggered_by", "STRING", run.triggered_by),
            bq_module.ScalarQueryParameter("trigger_source", "STRING", run.trigger_source),
            bq_module.ScalarQueryParameter("jobs_path", "STRING", run.jobs_path),
            bq_module.ScalarQueryParameter("config_path", "STRING", run.config_path),
            bq_module.ScalarQueryParameter("created_at", "TIMESTAMP", run.created_at),
            bq_module.ScalarQueryParameter("effective_settings_json", "STRING", run.effective_settings_json),
            bq_module.ScalarQueryParameter("run_mode", "STRING", run.run_mode),
            bq_module.ScalarQueryParameter("checkpoint_status", "STRING", run.checkpoint_status),
            bq_module.ScalarQueryParameter("next_stage", "STRING", run.next_stage),
            bq_module.ScalarQueryParameter(
                "last_completed_stage", "STRING", run.last_completed_stage
            ),
            bq_module.ScalarQueryParameter(
                "completed_stages_json",
                "STRING",
                json.dumps(run.completed_stages) if run.completed_stages is not None else None,
            ),
            bq_module.ScalarQueryParameter(
                "checkpoint_payload_json", "STRING", run.checkpoint_payload_json
            ),
            bq_module.ScalarQueryParameter("jobs_input_source", "STRING", run.jobs_input_source),
            bq_module.ScalarQueryParameter("jobs_input_json", "STRING", run.jobs_input_json),
            bq_module.ScalarQueryParameter("candidate_profile_source", "STRING", run.candidate_profile_source),
            bq_module.ScalarQueryParameter("candidate_profile_json", "STRING", run.candidate_profile_json),
            bq_module.ScalarQueryParameter("queue_job_id", "STRING", run.queue_job_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)



def update_run_status(
    run_id: str,
    status: RunStatus,
    bq: Any,
    *,
    project: str,
    dataset: str,
    started_at: Optional[datetime.datetime] = None,
    finished_at: Optional[datetime.datetime] = None,
    summary: Optional[dict] = None,
    error_message: Optional[str] = None,
    error_stage: Optional[str] = None,
) -> None:
    set_clauses = ["status = @status"]
    params: list[bq_module.ScalarQueryParameter] = [
        bq_module.ScalarQueryParameter("status", "STRING", status.value),
        bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
    ]
    if started_at:
        set_clauses.append("started_at = @started_at")
        params.append(bq_module.ScalarQueryParameter("started_at", "TIMESTAMP", started_at))
    if finished_at:
        set_clauses.append("finished_at = @finished_at")
        params.append(bq_module.ScalarQueryParameter("finished_at", "TIMESTAMP", finished_at))
    if error_message:
        set_clauses.append("error_message = @error_message")
        params.append(bq_module.ScalarQueryParameter("error_message", "STRING", error_message))
    if error_stage:
        set_clauses.append("error_stage = @error_stage")
        params.append(bq_module.ScalarQueryParameter("error_stage", "STRING", error_stage))
    if summary:
        for k in ("total_jobs", "passed_filter", "ranked", "cvs_generated"):
            if k in summary:
                set_clauses.append(f"{k} = @{k}")
                params.append(bq_module.ScalarQueryParameter(k, "INT64", int(summary[k])))

    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET {', '.join(set_clauses)} WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(query_parameters=params)
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def update_run_checkpoint(
    run_id: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
    checkpoint_status: Optional[str] = None,
    next_stage: Optional[str] = None,
    last_completed_stage: Optional[str] = None,
    completed_stages: Optional[list[str]] = None,
    checkpoint_payload_json: Optional[str] = None,
) -> None:
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        "SET checkpoint_status = @checkpoint_status, "
        "    next_stage = @next_stage, "
        "    last_completed_stage = @last_completed_stage, "
        "    completed_stages_json = @completed_stages_json, "
        "    checkpoint_payload_json = @checkpoint_payload_json "
        "WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("checkpoint_status", "STRING", checkpoint_status),
            bq_module.ScalarQueryParameter("next_stage", "STRING", next_stage),
            bq_module.ScalarQueryParameter("last_completed_stage", "STRING", last_completed_stage),
            bq_module.ScalarQueryParameter(
                "completed_stages_json",
                "STRING",
                json.dumps(completed_stages) if completed_stages is not None else None,
            ),
            bq_module.ScalarQueryParameter(
                "checkpoint_payload_json", "STRING", checkpoint_payload_json
            ),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def update_run_progress(
    run_id: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
    last_completed_stage: Optional[str] = None,
    completed_stages: Optional[list[str]] = None,
) -> None:
    """Persist shared stage progress without implying resumability."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        "SET checkpoint_status = NULL, "
        "    next_stage = NULL, "
        "    last_completed_stage = @last_completed_stage, "
        "    completed_stages_json = @completed_stages_json, "
        "    checkpoint_payload_json = NULL "
        "WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("last_completed_stage", "STRING", last_completed_stage),
            bq_module.ScalarQueryParameter(
                "completed_stages_json",
                "STRING",
                json.dumps(completed_stages) if completed_stages is not None else None,
            ),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def append_event(event: RunEvent, bq: Any, *, project: str, dataset: str) -> None:
    table = f"{project}.{dataset}.pipeline_run_events"
    row = {
        "run_id": event.run_id,
        "event_id": event.event_id,
        "stage": event.stage,
        "level": event.level,
        "message": event.message,
        "payload_json": event.payload_json,
        "created_at": event.created_at.isoformat(),
    }
    errors = bq.insert_rows_json(table, [row])
    if errors:
        logger.warning("BQ append_event errors: %s", errors)


def get_run(run_id: str, bq: Any, *, project: str, dataset: str) -> Optional[PipelineRun]:
    sql = f"SELECT * FROM `{project}.{dataset}.pipeline_runs` WHERE run_id = @run_id LIMIT 1"
    job_config = bq_module.QueryJobConfig(
        query_parameters=[bq_module.ScalarQueryParameter("run_id", "STRING", run_id)]
    )
    rows = list(bq.query(sql, job_config=job_config).result())
    return _row_to_run(rows[0]) if rows else None


def list_runs(
    bq: Any,
    *,
    project: str,
    dataset: str,
    limit: int = 50,
    include_archived: bool = False,
    archived_only: bool = False,
) -> list[PipelineRun]:
    """List pipeline runs with archive visibility control.

    - include_archived=False (default): active runs only (archived_at IS NULL)
    - archived_only=True: archived runs only (archived_at IS NOT NULL)
    - include_archived=True: all runs, no archive filter

    DEPLOY NOTE: migration must be applied before this code is deployed.
    """
    if archived_only:
        where = "WHERE archived_at IS NOT NULL"
    elif not include_archived:
        where = "WHERE archived_at IS NULL"
    else:
        where = ""
    sql = (
        f"SELECT * FROM `{project}.{dataset}.pipeline_runs` "
        f"{where} ORDER BY created_at DESC LIMIT {int(limit)}"
    )
    return [_row_to_run(r) for r in bq.query(sql).result()]


def update_run_queue_job_id(
    run_id: str,
    queue_job_id: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the RQ job id onto the run row immediately after enqueue."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET queue_job_id = @queue_job_id WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("queue_job_id", "STRING", queue_job_id),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def update_run_results_export(
    run_id: str,
    results_export_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the immutable run-results export snapshot for a completed run."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET results_export_json = @results_export_json WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("results_export_json", "STRING", results_export_json),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def update_run_cv_generation_debug(
    run_id: str,
    cv_generation_debug_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the immutable run-scoped CV-generation debug snapshot."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET cv_generation_debug_json = @cv_generation_debug_json WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter(
                "cv_generation_debug_json", "STRING", cv_generation_debug_json
            ),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def update_run_stage_transition_artifacts(
    run_id: str,
    stage_transition_artifacts_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the immutable run-scoped stage transition artifacts snapshot."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET stage_transition_artifacts_json = @stage_transition_artifacts_json WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter(
                "stage_transition_artifacts_json", "STRING", stage_transition_artifacts_json
            ),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def update_run_settings_used(
    run_id: str,
    settings_used_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the immutable run-scoped settings-used snapshot."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET settings_used_json = @settings_used_json WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("settings_used_json", "STRING", settings_used_json),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def update_run_mapping_suggestions(
    run_id: str,
    mapping_suggestions_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the immutable run-scoped mapping suggestions snapshot."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET mapping_suggestions_json = @mapping_suggestions_json WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter(
                "mapping_suggestions_json", "STRING", mapping_suggestions_json
            ),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def update_run_effective_settings(
    run_id: str,
    effective_settings_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the mutable run-scoped effective settings snapshot."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET effective_settings_json = @effective_settings_json WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter(
                "effective_settings_json", "STRING", effective_settings_json
            ),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    bq.query(sql, job_config=job_config).result()


def request_run_cancel(
    run_id: str,
    requested_by: str,
    new_status: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Set cancel_requested_at/by and update status (running→cancelling, queued→cancelled)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET cancel_requested_at = @cancel_requested_at, "
        f"    cancel_requested_by = @cancel_requested_by, "
        f"    status = @status "
        f"WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("cancel_requested_at", "TIMESTAMP", now),
            bq_module.ScalarQueryParameter("cancel_requested_by", "STRING", requested_by),
            bq_module.ScalarQueryParameter("status", "STRING", new_status),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def archive_run(
    run_id: str,
    archived_by: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist archive state on the run record (non-destructive)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET archived_at = @archived_at, archived_by = @archived_by "
        f"WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("archived_at", "TIMESTAMP", now),
            bq_module.ScalarQueryParameter("archived_by", "STRING", archived_by),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def unarchive_run(
    run_id: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Clear archive state, returning run to the active list."""
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET archived_at = NULL, archived_by = NULL "
        f"WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)


def get_events(run_id: str, bq: Any, *, project: str, dataset: str) -> list[RunEvent]:
    sql = (
        f"SELECT * FROM `{project}.{dataset}.pipeline_run_events` "
        f"WHERE run_id = @run_id ORDER BY created_at ASC"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[bq_module.ScalarQueryParameter("run_id", "STRING", run_id)]
    )
    return [_row_to_event(r) for r in bq.query(sql, job_config=job_config).result()]


def _row_to_run(row: Any) -> PipelineRun:
    r = dict(row)
    raw_status = str(r.get("status") or "").strip().lower()
    try:
        status = RunStatus(raw_status)
    except ValueError:
        logger.warning(
            "Unknown pipeline run status %r for run_id=%s; coercing to failed for admin compatibility",
            raw_status,
            r.get("run_id"),
        )
        status = RunStatus.FAILED
    completed_stages_raw = r.get("completed_stages_json")
    completed_stages: list[str] | None = None
    if isinstance(completed_stages_raw, str) and completed_stages_raw.strip():
        try:
            parsed_completed_stages = json.loads(completed_stages_raw)
        except json.JSONDecodeError:
            parsed_completed_stages = None
        if isinstance(parsed_completed_stages, list):
            completed_stages = [str(item) for item in parsed_completed_stages]
    elif isinstance(completed_stages_raw, list):
        completed_stages = [str(item) for item in completed_stages_raw]
    return PipelineRun(
        run_id=r["run_id"],
        status=status,
        triggered_by=r.get("triggered_by") or "",
        trigger_source=r.get("trigger_source") or "",
        jobs_path=r.get("jobs_path") or "",
        config_path=r.get("config_path") or "",
        created_at=r["created_at"],
        started_at=r.get("started_at"),
        finished_at=r.get("finished_at"),
        total_jobs=r.get("total_jobs"),
        passed_filter=r.get("passed_filter"),
        ranked=r.get("ranked"),
        cvs_generated=r.get("cvs_generated"),
        error_message=r.get("error_message"),
        error_stage=r.get("error_stage"),
        effective_settings_json=r.get("effective_settings_json"),
        results_export_json=r.get("results_export_json"),
        cv_generation_debug_json=r.get("cv_generation_debug_json"),
        stage_transition_artifacts_json=r.get("stage_transition_artifacts_json"),
        settings_used_json=r.get("settings_used_json"),
        mapping_suggestions_json=r.get("mapping_suggestions_json"),
        run_mode=r.get("run_mode") or "run_all",
        checkpoint_status=r.get("checkpoint_status"),
        next_stage=r.get("next_stage"),
        last_completed_stage=r.get("last_completed_stage"),
        completed_stages=completed_stages,
        checkpoint_payload_json=r.get("checkpoint_payload_json"),
        jobs_input_source=r.get("jobs_input_source"),
        jobs_input_json=r.get("jobs_input_json"),
        candidate_profile_source=r.get("candidate_profile_source"),
        candidate_profile_json=r.get("candidate_profile_json"),
        queue_job_id=r.get("queue_job_id"),
        cancel_requested_at=r.get("cancel_requested_at"),
        cancel_requested_by=r.get("cancel_requested_by"),
        archived_at=r.get("archived_at"),
        archived_by=r.get("archived_by"),
    )


def _row_to_event(row: Any) -> RunEvent:
    r = dict(row)
    return RunEvent(
        run_id=r["run_id"],
        event_id=r["event_id"],
        stage=r["stage"],
        level=r["level"],
        message=r["message"],
        created_at=r["created_at"],
        payload_json=r.get("payload_json"),
    )


def list_cvs_for_run(run_id: str, bq: Any, *, project: str, dataset: str) -> list[dict[str, Any]]:
    table = f"{project}.{dataset}.cv_versions"
    sql = f"""
        SELECT
            version_id,
            job_url,
            fit_classification,
            generated_at,
            cv_generation_model,
            cv_prompt_version,
            cv_schema_version,
            cv_structured_json
        FROM `{table}`
        WHERE run_id = @run_id
        ORDER BY generated_at DESC
    """
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ],
        use_query_cache=False,
    )
    legacy_sql = f"""
        SELECT
            version_id,
            job_url,
            fit_classification,
            generated_at,
            NULL AS cv_generation_model,
            NULL AS cv_prompt_version,
            NULL AS cv_schema_version,
            NULL AS cv_structured_json
        FROM `{table}`
        WHERE run_id = @run_id
        ORDER BY generated_at DESC
    """
    try:
        rows = bq.query(sql, job_config=job_config).result()
    except Exception as exc:
        if "Unrecognized name:" not in str(exc):
            raise
        logger.warning(
            "cv_versions structured CV columns missing; falling back to legacy read path: %s",
            exc,
        )
        rows = bq.query(legacy_sql, job_config=job_config).result()
    results: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row.items())
        row_dict.setdefault("cv_generation_model", None)
        row_dict.setdefault("cv_prompt_version", None)
        row_dict.setdefault("cv_schema_version", None)
        row_dict.setdefault("cv_structured_json", None)
        structured_raw = row_dict.get("cv_structured_json")
        if isinstance(structured_raw, str) and structured_raw.strip():
            try:
                row_dict["cv_structured"] = json.loads(structured_raw)
            except json.JSONDecodeError:
                row_dict["cv_structured"] = None
        else:
            row_dict["cv_structured"] = None
        results.append(row_dict)
    return results


def get_cv_markdown(version_id: str, bq: Any, *, project: str, dataset: str) -> Optional[str]:
    table = f"{project}.{dataset}.cv_versions"
    sql = f"""
        SELECT cv_markdown
        FROM `{table}`
        WHERE version_id = @version_id
        LIMIT 1
    """
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("version_id", "STRING", version_id),
        ],
        use_query_cache=False,
    )
    rows = list(bq.query(sql, job_config=job_config).result())
    if not rows:
        return None
    return rows[0]["cv_markdown"]


def list_run_structured_jobs(
    run_id: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> list[dict[str, Any]]:
    """Return run-scoped enriched job rows for the given run_id.

    Rows are returned as plain dicts and ordered by title, job_url for
    deterministic display. Uses parameterized SQL to avoid injection.
    """
    table = f"{project}.{dataset}.run_structured_jobs"
    sql = f"""
        SELECT *
        FROM `{table}`
        WHERE run_id = @run_id
        ORDER BY title, job_url
    """
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ],
        use_query_cache=False,
    )
    rows = bq.query(sql, job_config=job_config).result()
    results: list[dict[str, Any]] = []
    json_fields = (
        "required_skill_entities_json",
        "preferred_skill_entities_json",
        "mapping_suggestions_json",
    )
    for row in rows:
        row_dict = dict(row.items())
        for field_name in json_fields:
            raw_value = row_dict.get(field_name)
            if isinstance(raw_value, str) and raw_value.strip():
                try:
                    parsed_value = json.loads(raw_value)
                except json.JSONDecodeError:
                    parsed_value = None
            else:
                parsed_value = None
            row_dict[field_name.removesuffix("_json")] = parsed_value
        results.append(row_dict)
    return results


def list_filter_results_for_run(
    run_id: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> list[dict[str, Any]]:
    """Return run-scoped filter results for a given run_id.

    Rows include job_url, passed (bool), reasons, marks, and run_id.
    Ordered by job_url for deterministic display. Uses parameterized SQL.
    """
    table = f"{project}.{dataset}.rule_filter_results"
    sql = f"""
        SELECT job_url, passed, reasons, marks_json, run_id, filtered_at
        FROM `{table}`
        WHERE run_id = @run_id
        ORDER BY job_url
    """
    legacy_sql = f"""
        SELECT job_url, passed, reasons, run_id, filtered_at
        FROM `{table}`
        WHERE run_id = @run_id
        ORDER BY job_url
    """
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ],
        use_query_cache=False,
    )
    try:
        rows = bq.query(sql, job_config=job_config).result()
    except Exception as exc:
        if "Unrecognized name:" not in str(exc):
            raise
        logger.warning(
            "rule_filter_results marks_json column missing; falling back to legacy read path: %s",
            exc,
        )
        rows = bq.query(legacy_sql, job_config=job_config).result()
    results: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row.items())
        marks_raw = row_dict.get("marks_json")
        if isinstance(marks_raw, str) and marks_raw.strip():
            try:
                row_dict["marks"] = json.loads(marks_raw)
            except json.JSONDecodeError:
                row_dict["marks"] = []
        else:
            row_dict["marks"] = []
        results.append(row_dict)
    return results
