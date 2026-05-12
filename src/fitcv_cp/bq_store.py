"""@meta
name: bq_store
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.bq_store.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import datetime
import dataclasses
import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from google.cloud import bigquery as bq_module

from fitcv_cp.models import PipelineRun, RunEvent, RunStatus

logger = logging.getLogger(__name__)
_LOCAL_RUNS: dict[str, PipelineRun] = {}
_LOCAL_EVENTS: dict[str, list[RunEvent]] = {}

_PIPELINE_RUNS_UPDATE_RETRY_ATTEMPTS = 3
_PIPELINE_RUNS_UPDATE_RETRY_DELAY_SECONDS = 0.25
_EVENT_APPEND_RETRY_ATTEMPTS = 3
_EVENT_APPEND_RETRY_DELAY_SECONDS = 0.2

def _local_sqlite_path() -> str:
    return str(os.environ.get("FITCV_CP_SQLITE_PATH") or "data/fitcv_cp.sqlite3").strip() or "data/fitcv_cp.sqlite3"

def _sqlite_mode_enabled() -> bool:
    return str(os.environ.get("FITCV_CP_DATA_BACKEND") or "").strip().lower() == "sqlite"

def _ensure_local_cv_versions_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cv_versions (
            version_id TEXT PRIMARY KEY,
            run_id TEXT,
            job_url TEXT,
            fit_classification TEXT,
            generated_at TEXT,
            cv_generation_model TEXT,
            cv_prompt_version TEXT,
            cv_schema_version TEXT,
            cv_structured_json TEXT,
            cv_markdown TEXT
        )
        """
    )
    conn.commit()

def _ensure_local_pipeline_runs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS local_pipeline_runs (
            run_id TEXT PRIMARY KEY,
            run_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

def _pipeline_run_to_json(run: PipelineRun) -> str:
    payload = dataclasses.asdict(run)
    payload["status"] = run.status.value
    for field_name in (
        "created_at",
        "started_at",
        "finished_at",
        "cancel_requested_at",
        "archived_at",
    ):
        value = payload.get(field_name)
        if isinstance(value, datetime.datetime):
            payload[field_name] = value.isoformat()
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)

def _parse_dt(value: Any) -> Optional[datetime.datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        return None

def _pipeline_run_from_json(run_json: str) -> Optional[PipelineRun]:
    try:
        payload = json.loads(run_json)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        status = RunStatus(str(payload.get("status") or "").strip().lower())
    except ValueError:
        status = RunStatus.FAILED
    return PipelineRun(
        run_id=str(payload.get("run_id") or ""),
        status=status,
        triggered_by=str(payload.get("triggered_by") or ""),
        trigger_source=str(payload.get("trigger_source") or ""),
        jobs_path=str(payload.get("jobs_path") or ""),
        config_path=str(payload.get("config_path") or ""),
        created_at=_parse_dt(payload.get("created_at")) or datetime.datetime.now(datetime.timezone.utc),
        started_at=_parse_dt(payload.get("started_at")),
        finished_at=_parse_dt(payload.get("finished_at")),
        total_jobs=payload.get("total_jobs"),
        passed_filter=payload.get("passed_filter"),
        ranked=payload.get("ranked"),
        cvs_generated=payload.get("cvs_generated"),
        error_message=payload.get("error_message"),
        error_stage=payload.get("error_stage"),
        effective_settings_json=payload.get("effective_settings_json"),
        results_export_json=payload.get("results_export_json"),
        cv_generation_debug_json=payload.get("cv_generation_debug_json"),
        stage_transition_artifacts_json=payload.get("stage_transition_artifacts_json"),
        settings_used_json=payload.get("settings_used_json"),
        mapping_suggestions_json=payload.get("mapping_suggestions_json"),
        synonym_proposals_json=payload.get("synonym_proposals_json"),
        run_mode=str(payload.get("run_mode") or "run_all"),
        checkpoint_status=payload.get("checkpoint_status"),
        next_stage=payload.get("next_stage"),
        last_completed_stage=payload.get("last_completed_stage"),
        completed_stages=list(payload.get("completed_stages") or []) or None,
        checkpoint_payload_json=payload.get("checkpoint_payload_json"),
        jobs_input_source=payload.get("jobs_input_source"),
        jobs_input_json=payload.get("jobs_input_json"),
        candidate_profile_source=payload.get("candidate_profile_source"),
        candidate_profile_json=payload.get("candidate_profile_json"),
        queue_job_id=payload.get("queue_job_id"),
        orchestration_backend=payload.get("orchestration_backend"),
        orchestration_run_id=payload.get("orchestration_run_id"),
        cancel_requested_at=_parse_dt(payload.get("cancel_requested_at")),
        cancel_requested_by=payload.get("cancel_requested_by"),
        archived_at=_parse_dt(payload.get("archived_at")),
        archived_by=payload.get("archived_by"),
    )

def _upsert_local_pipeline_run(run: PipelineRun) -> None:
    db_path = Path(_local_sqlite_path())
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        _ensure_local_pipeline_runs_table(conn)
        conn.execute(
            """
            INSERT INTO local_pipeline_runs(run_id, run_json, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
              run_json = excluded.run_json,
              created_at = excluded.created_at
            """,
            (run.run_id, _pipeline_run_to_json(run), run.created_at.isoformat()),
        )
        conn.commit()

def _load_local_pipeline_run(run_id: str) -> Optional[PipelineRun]:
    db_path = Path(_local_sqlite_path())
    if not db_path.exists():
        return None
    with sqlite3.connect(db_path) as conn:
        _ensure_local_pipeline_runs_table(conn)
        row = conn.execute(
            "SELECT run_json FROM local_pipeline_runs WHERE run_id = ? LIMIT 1",
            (run_id,),
        ).fetchone()
    if not row:
        return None
    run = _pipeline_run_from_json(str(row[0] or ""))
    if run is not None and run.run_id:
        _LOCAL_RUNS[run.run_id] = dataclasses.replace(run)
    return run

def _list_local_pipeline_runs() -> list[PipelineRun]:
    db_path = Path(_local_sqlite_path())
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        _ensure_local_pipeline_runs_table(conn)
        rows = conn.execute(
            "SELECT run_json FROM local_pipeline_runs ORDER BY created_at DESC"
        ).fetchall()
    runs: list[PipelineRun] = []
    for row in rows:
        run = _pipeline_run_from_json(str(row[0] or ""))
        if run is not None and run.run_id:
            _LOCAL_RUNS[run.run_id] = dataclasses.replace(run)
            runs.append(run)
    return runs

def _local_get_run(run_id: str) -> Optional[PipelineRun]:
    cached = _LOCAL_RUNS.get(run_id)
    if cached is not None:
        return dataclasses.replace(cached)
    run = _load_local_pipeline_run(run_id)
    return dataclasses.replace(run) if run is not None else None

def _local_save_run(run: PipelineRun) -> None:
    _LOCAL_RUNS[run.run_id] = dataclasses.replace(run)
    _upsert_local_pipeline_run(run)


def _local_event_history_dir() -> Path:
    raw = str(
        os.environ.get("FITCV_CP_LOCAL_EVENT_HISTORY_DIR")
        or "data/fitcv_cp_event_history"
    ).strip()
    return Path(raw)


def _local_event_history_file(run_id: str) -> Path:
    safe_run_id = "".join(ch for ch in str(run_id) if ch.isalnum() or ch in {"-", "_"})
    return _local_event_history_dir() / f"{safe_run_id}.jsonl"


def _is_concurrent_pipeline_runs_update_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "pipeline_runs" in message
        and "concurrent update" in message
        and "could not serialize access" in message
    )


def _is_unrecognized_column_error(exc: Exception, column_name: str) -> bool:
    message = str(exc).lower()
    return "unrecognized name:" in message and column_name.lower() in message


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
    if bq is None:
        _local_save_run(dataclasses.replace(run))
        return
    table = f"{project}.{dataset}.pipeline_runs"
    sql = f"""
        INSERT INTO `{table}` (
            run_id, status, triggered_by, trigger_source,
            jobs_path, config_path, created_at, effective_settings_json,
            run_mode, checkpoint_status, next_stage, last_completed_stage,
            completed_stages_json, checkpoint_payload_json,
            jobs_input_source, jobs_input_json,
            candidate_profile_source, candidate_profile_json,
            queue_job_id, orchestration_backend, orchestration_run_id
        )
        VALUES (
            @run_id, @status, @triggered_by, @trigger_source,
            @jobs_path, @config_path, @created_at, @effective_settings_json,
            @run_mode, @checkpoint_status, @next_stage, @last_completed_stage,
            @completed_stages_json, @checkpoint_payload_json,
            @jobs_input_source, @jobs_input_json,
            @candidate_profile_source, @candidate_profile_json,
            @queue_job_id, @orchestration_backend, @orchestration_run_id
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
            bq_module.ScalarQueryParameter("orchestration_backend", "STRING", run.orchestration_backend),
            bq_module.ScalarQueryParameter("orchestration_run_id", "STRING", run.orchestration_run_id),
        ]
    )
    try:
        _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)
    except Exception as exc:
        if not (
            _is_unrecognized_column_error(exc, "orchestration_backend")
            or _is_unrecognized_column_error(exc, "orchestration_run_id")
        ):
            raise
        # Backward-compatible insert path before migration lands.
        legacy_sql = f"""
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
        legacy_params = [
            param for param in job_config.query_parameters
            if param.name not in {"orchestration_backend", "orchestration_run_id"}
        ]
        legacy_job_config = bq_module.QueryJobConfig(query_parameters=legacy_params)
        _execute_query_with_pipeline_runs_retry(bq, legacy_sql, job_config=legacy_job_config)



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
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        updated = dataclasses.replace(existing, status=status)
        if started_at:
            updated.started_at = started_at
        if finished_at:
            updated.finished_at = finished_at
        if error_message:
            updated.error_message = error_message
        if error_stage:
            updated.error_stage = error_stage
        if summary:
            updated.total_jobs = int(summary.get("total_jobs")) if summary.get("total_jobs") is not None else updated.total_jobs
            updated.passed_filter = int(summary.get("passed_filter")) if summary.get("passed_filter") is not None else updated.passed_filter
            updated.ranked = int(summary.get("ranked")) if summary.get("ranked") is not None else updated.ranked
            updated.cvs_generated = int(summary.get("cvs_generated")) if summary.get("cvs_generated") is not None else updated.cvs_generated
        _local_save_run(updated)
        return
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
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(
            existing,
            checkpoint_status=checkpoint_status,
            next_stage=next_stage,
            last_completed_stage=last_completed_stage,
            completed_stages=completed_stages,
            checkpoint_payload_json=checkpoint_payload_json,
        ))
        return
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
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(
            existing,
            checkpoint_status=None,
            next_stage=None,
            last_completed_stage=last_completed_stage,
            completed_stages=completed_stages,
            checkpoint_payload_json=None,
        ))
        return
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


def _append_event_dead_letter(row: dict[str, Any]) -> str:
    dead_letter_path = str(
        os.environ.get("FITCV_EVENT_DEAD_LETTER_PATH")
        or "tmp/fitcv_pipeline_run_events_dead_letter.jsonl"
    )
    dead_letter_file = os.path.abspath(dead_letter_path)
    os.makedirs(os.path.dirname(dead_letter_file), exist_ok=True)
    with open(dead_letter_file, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return dead_letter_file

def append_event(event: RunEvent, bq: Any, *, project: str, dataset: str) -> dict[str, str]:
    if bq is None:
        _LOCAL_EVENTS.setdefault(event.run_id, []).append(dataclasses.replace(event))
        try:
            event_file = _local_event_history_file(event.run_id)
            event_file.parent.mkdir(parents=True, exist_ok=True)
            record = {
                "run_id": event.run_id,
                "event_id": event.event_id,
                "stage": event.stage,
                "level": event.level,
                "message": event.message,
                "payload_json": event.payload_json,
                "created_at": event.created_at.isoformat(),
            }
            with event_file.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.warning(
                "local append_event file persistence degraded for run_id=%s: %s",
                event.run_id,
                exc,
            )
        return {"persistence_status": "persisted", "degradation_reason": "none"}
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
    last_errors: Any = None
    for attempt in range(1, _EVENT_APPEND_RETRY_ATTEMPTS + 1):
        errors = bq.insert_rows_json(table, [row])
        if not errors:
            return {"persistence_status": "persisted", "degradation_reason": ""}
        last_errors = errors
        logger.warning(
            "BQ append_event errors [attempt=%s/%s]: %s",
            attempt,
            _EVENT_APPEND_RETRY_ATTEMPTS,
            errors,
        )
        if attempt < _EVENT_APPEND_RETRY_ATTEMPTS:
            time.sleep(_EVENT_APPEND_RETRY_DELAY_SECONDS * attempt)
    try:
        dead_letter_file = _append_event_dead_letter(
            {
                "table": table,
                "row": row,
                "bq_errors": last_errors,
                "retry_attempts": _EVENT_APPEND_RETRY_ATTEMPTS,
                "degradation_reason": "event_insert_failed_dead_lettered",
                "failed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
        )
        logger.warning("append_event dead-lettered row to %s", dead_letter_file)
        return {
            "persistence_status": "dead_lettered",
            "degradation_reason": "event_insert_failed_dead_lettered",
        }
    except Exception as exc:
        logger.warning("append_event dead-letter fallback failed: %s", exc)
        return {
            "persistence_status": "failed",
            "degradation_reason": "event_insert_failed_no_dead_letter",
        }


def get_run(run_id: str, bq: Any, *, project: str, dataset: str) -> Optional[PipelineRun]:
    if bq is None:
        return _local_get_run(run_id)
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
    if bq is None:
        runs = _list_local_pipeline_runs()
        if archived_only:
            runs = [r for r in runs if r.archived_at is not None]
        elif not include_archived:
            runs = [r for r in runs if r.archived_at is None]
        runs.sort(key=lambda r: r.created_at, reverse=True)
        return [dataclasses.replace(r) for r in runs[: int(limit)]]
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

def get_pipeline_runs_schema_status(
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> dict[str, Any]:
    """Report whether orchestration-binding columns exist on pipeline_runs."""
    if bq is None:
        return {
            "status": "unknown",
            "missing_columns": [],
            "warning": "sqlite_mode_no_bigquery_schema_check",
        }
    required_columns = {"orchestration_backend", "orchestration_run_id"}
    sql = (
        f"SELECT column_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        "WHERE table_name = 'pipeline_runs'"
    )
    try:
        rows = list(bq.query(sql).result())
        present = {str(dict(row).get("column_name") or "").strip() for row in rows}
    except Exception as exc:
        return {
            "status": "unknown",
            "missing_columns": sorted(required_columns),
            "warning": f"schema_check_failed:{exc}",
        }
    missing = sorted(col for col in required_columns if col not in present)
    if not missing:
        return {"status": "complete", "missing_columns": [], "warning": None}
    return {
        "status": "fallback",
        "missing_columns": missing,
        "warning": "orchestration_binding_columns_missing",
    }


def update_run_queue_job_id(
    run_id: str,
    queue_job_id: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the RQ job id onto the run row immediately after enqueue."""
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(existing, queue_job_id=queue_job_id))
        return
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

def update_run_orchestration_binding(
    run_id: str,
    *,
    queue_job_id: str | None,
    orchestration_backend: str | None,
    orchestration_run_id: str | None,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(
            existing,
            queue_job_id=queue_job_id,
            orchestration_backend=orchestration_backend,
            orchestration_run_id=orchestration_run_id,
        ))
        return
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        "SET queue_job_id = @queue_job_id, "
        "    orchestration_backend = @orchestration_backend, "
        "    orchestration_run_id = @orchestration_run_id "
        "WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter("queue_job_id", "STRING", queue_job_id),
            bq_module.ScalarQueryParameter("orchestration_backend", "STRING", orchestration_backend),
            bq_module.ScalarQueryParameter("orchestration_run_id", "STRING", orchestration_run_id),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    try:
        _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)
    except Exception as exc:
        if not (
            _is_unrecognized_column_error(exc, "orchestration_backend")
            or _is_unrecognized_column_error(exc, "orchestration_run_id")
        ):
            raise
        # Legacy compatibility before schema migration.
        update_run_queue_job_id(
            run_id,
            str(queue_job_id or ""),
            bq,
            project=project,
            dataset=dataset,
        )


def update_run_results_export(
    run_id: str,
    results_export_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the immutable run-results export snapshot for a completed run."""
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(existing, results_export_json=results_export_json))
        return
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
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(existing, cv_generation_debug_json=cv_generation_debug_json))
        return
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
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(existing, stage_transition_artifacts_json=stage_transition_artifacts_json))
        return
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
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(existing, settings_used_json=settings_used_json))
        return
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
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(existing, mapping_suggestions_json=mapping_suggestions_json))
        return
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


def update_run_synonym_proposals(
    run_id: str,
    synonym_proposals_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> dict[str, str]:
    """Persist the mutable run-scoped synonym proposal review snapshot."""
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return {"persistence_status": "degraded", "degradation_reason": "run_not_found"}
        _local_save_run(dataclasses.replace(existing, synonym_proposals_json=synonym_proposals_json))
        return {"persistence_status": "persisted", "degradation_reason": "none"}
    sql = (
        f"UPDATE `{project}.{dataset}.pipeline_runs` "
        f"SET synonym_proposals_json = @synonym_proposals_json WHERE run_id = @run_id"
    )
    job_config = bq_module.QueryJobConfig(
        query_parameters=[
            bq_module.ScalarQueryParameter(
                "synonym_proposals_json", "STRING", synonym_proposals_json
            ),
            bq_module.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    try:
        _execute_query_with_pipeline_runs_retry(bq, sql, job_config=job_config)
        return {"persistence_status": "persisted", "degradation_reason": ""}
    except Exception as exc:
        if not _is_unrecognized_column_error(exc, "synonym_proposals_json"):
            raise
        logger.warning(
            "pipeline_runs.synonym_proposals_json missing in live schema; "
            "skipping synonym proposal snapshot persistence until migration is applied: %s",
            exc,
        )
        return {
            "persistence_status": "bundle_only_degraded",
            "degradation_reason": "missing_synonym_proposals_json_column",
        }


def update_run_effective_settings(
    run_id: str,
    effective_settings_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> None:
    """Persist the mutable run-scoped effective settings snapshot."""
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return
        _local_save_run(dataclasses.replace(existing, effective_settings_json=effective_settings_json))
        return
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
) -> bool:
    """Set cancel_requested_at/by and update status (running→cancelling, queued→cancelled)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    if bq is None:
        existing = _local_get_run(run_id)
        if existing is None:
            return False
        updated = dataclasses.replace(
            existing,
            cancel_requested_at=now,
            cancel_requested_by=requested_by,
            status=RunStatus(new_status),
        )
        _local_save_run(updated)
        return True
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
    return True


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
    if bq is None:
        event_file = _local_event_history_file(run_id)
        if event_file.exists():
            file_events: list[RunEvent] = []
            try:
                with event_file.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        raw = line.strip()
                        if not raw:
                            continue
                        try:
                            record = json.loads(raw)
                            created_raw = str(record.get("created_at") or "").strip()
                            created_at = (
                                datetime.datetime.fromisoformat(created_raw)
                                if created_raw
                                else datetime.datetime.now(datetime.timezone.utc)
                            )
                            file_events.append(
                                RunEvent(
                                    run_id=str(record.get("run_id") or run_id),
                                    event_id=str(record.get("event_id") or ""),
                                    stage=str(record.get("stage") or ""),
                                    level=str(record.get("level") or ""),
                                    message=str(record.get("message") or ""),
                                    created_at=created_at,
                                    payload_json=record.get("payload_json"),
                                )
                            )
                        except Exception:
                            continue
            except Exception as exc:
                logger.warning(
                    "local get_events file read degraded for run_id=%s: %s",
                    run_id,
                    exc,
                )
            file_events.sort(key=lambda ev: ev.created_at)
            return file_events
        events = _LOCAL_EVENTS.get(run_id) or []
        return [dataclasses.replace(event) for event in events]
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
        synonym_proposals_json=r.get("synonym_proposals_json"),
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
        orchestration_backend=r.get("orchestration_backend"),
        orchestration_run_id=r.get("orchestration_run_id"),
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
    if _sqlite_mode_enabled():
        db_path = Path(_local_sqlite_path())
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            _ensure_local_cv_versions_table(conn)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    version_id,
                    job_url,
                    fit_classification,
                    generated_at,
                    cv_generation_model,
                    cv_prompt_version,
                    cv_schema_version,
                    cv_structured_json
                FROM cv_versions
                WHERE run_id = ?
                ORDER BY generated_at DESC
                """,
                (run_id,),
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            row_dict = dict(row)
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

    if bq is None:
        return []
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
    if _sqlite_mode_enabled():
        db_path = Path(_local_sqlite_path())
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            _ensure_local_cv_versions_table(conn)
            row = conn.execute(
                "SELECT cv_markdown FROM cv_versions WHERE version_id = ? LIMIT 1",
                (version_id,),
            ).fetchone()
        if row is None:
            return None
        return str(row[0] or "")

    if bq is None:
        return None
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
    if bq is None:
        return []
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
    if bq is None:
        return []
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


def insert_cv_version_row(row: dict[str, Any], bq: Any, *, project: str, dataset: str) -> list[Any]:
    if _sqlite_mode_enabled():
        db_path = Path(_local_sqlite_path())
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            _ensure_local_cv_versions_table(conn)
            conn.execute(
                """
                INSERT OR REPLACE INTO cv_versions (
                    version_id,
                    run_id,
                    job_url,
                    fit_classification,
                    generated_at,
                    cv_generation_model,
                    cv_prompt_version,
                    cv_schema_version,
                    cv_structured_json,
                    cv_markdown
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(row.get("version_id") or ""),
                    str(row.get("run_id") or ""),
                    str(row.get("job_url") or ""),
                    str(row.get("fit_classification") or ""),
                    str(row.get("generated_at") or ""),
                    str(row.get("cv_generation_model") or ""),
                    str(row.get("cv_prompt_version") or ""),
                    str(row.get("cv_schema_version") or ""),
                    str(row.get("cv_structured_json") or ""),
                    str(row.get("cv_markdown") or ""),
                ),
            )
            conn.commit()
        return []

    table = f"{project}.{dataset}.cv_versions"
    return bq.insert_rows_json(table, [row])
