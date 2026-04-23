"""
@meta
name: control_plane_worker_job
type: script
domain: run_orchestration
responsibility:
  - Execute one queued pipeline run and persist lifecycle state.
  - Persist run-scoped settings-used and compact results snapshots.
inputs:
  - queued run id and pipeline paths
  - control-plane BigQuery run state
outputs:
  - run lifecycle updates
  - settings-used and results-export snapshots
capabilities:
  - admin_control_plane_core.pipelinereporter-integration
  - run_lifecycle_controls.cooperative-cancellation-at-safe-checkpoints-for-running-jobs
  - run_lifecycle_controls.full-audit-trail-in-pipeline-run-events
  - inspection_debugging.settings-used-export
  - inspection_debugging.results-ledger-inspection
  - inspection_debugging.stage-transition-diagnostics
  - inspection_debugging.prompt-provenance-diagnostics
  - inspection_debugging.reuse-diagnostics
  - inspection_debugging.quality-metrics-diagnostics
  - settings_system.settings-used-exports
  - pipeline_performance.large-runs-avoid-some-row-scaled-layer-4-event-noise-by-relying-more-on-aggregate-stage-summaries-plus-stage-owned-artifacts
  - pipeline_performance.results-json-now-keeps-only-compact-job-ledger-fields-instead-of-repeating-full-job-snapshots-heavy-score-explanation-internals-and-full-cv-bodies-already-represented-elsewhere
  - trigger_run_management.shared-stage-progress
  - trigger_run_management.run-owned-artifact-exports
  - trigger_run_management.run-results-export
  - trigger_run_management.reranker-fit-authority
tags:
  - worker
  - control-plane
lifecycle:
  status: active
"""
import datetime
import json
import logging
import os
import uuid
from typing import Any

from google.cloud import bigquery

from fitcv.pipeline import PipelineCancelled, run_pipeline
from fitcv_cp.bq_store import (
    append_event,
    get_run,
    list_runs,
    update_run_checkpoint,
    update_run_progress,
    update_run_cv_generation_debug,
    update_run_mapping_suggestions,
    update_run_results_export,
    update_run_settings_used,
    update_run_stage_transition_artifacts,
    update_run_status,
)
from fitcv_cp.models import RunEvent, RunStatus

logger = logging.getLogger(__name__)
_MAX_DEBUG_MARKDOWN_CHARS = 4000
_LATE_STAGE_REUSE_RUN_SCAN_LIMIT = 50
_SETTINGS_COMPATIBILITY_KEYS = {
    "vector_top_n",
    "rerank_top_n",
    "cv_generation_model",
    "prompt_version",
    "cv_max_pages",
    "required_cv_sections",
}
_CV_GENERATION_ATTEMPTED_STATUSES = {
    "accepted",
    "validation_failed",
    "generation_failed",
    "persistence_failed",
}
_CV_DEBUG_ANALYSIS_OMISSION_STATUSES = {
    "blocked_by_reranker_fit",
    "skipped_fit_gate",
    "analysis_failed",
}
_RUN_MODE_LABELS = {
    "run_all": "Run All",
    "manual_staged": "Stage by Stage",
}


def _get_bq() -> bigquery.Client:
    return bigquery.Client()


def _run_cancelled_event(run_id: str, message: str) -> RunEvent:
    return RunEvent(
        run_id=run_id,
        event_id=str(uuid.uuid4()),
        stage="run_cancelled",
        level="warning",
        message=message,
        created_at=datetime.datetime.now(datetime.timezone.utc),
    )


def _snapshot_persist_failed_event(run_id: str, snapshot_name: str, message: str) -> RunEvent:
    return RunEvent(
        run_id=run_id,
        event_id=str(uuid.uuid4()),
        stage="snapshot_persist_failed",
        level="warning",
        message=f"{snapshot_name} snapshot persistence failed: {message}",
        created_at=datetime.datetime.now(datetime.timezone.utc),
    )


def _build_results_export_payload(
    *,
    run_id: str,
    run_record: Any,
    summary: dict[str, Any],
    export_results: list[dict[str, Any]],
    finished_at: datetime.datetime,
) -> str:
    def _json_safe(value: Any) -> Any:
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        if isinstance(value, datetime.date):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_json_safe(item) for item in value]
        if isinstance(value, tuple):
            return [_json_safe(item) for item in value]
        if isinstance(value, set):
            return [_json_safe(item) for item in sorted(value)]
        return value

    def _string_or_none(value: Any) -> str | None:
        return value if isinstance(value, str) else None

    def _normalized_run_mode(value: Any) -> str:
        run_mode = _string_or_none(value)
        if run_mode in _RUN_MODE_LABELS:
            return run_mode
        return "run_all"

    def _iso_or_none(value: Any) -> str | None:
        return value.isoformat() if isinstance(value, datetime.datetime) else None

    diagnostic_support = {
        "late_stage_reuse_snapshots": _json_safe(summary.get("late_stage_reuse_snapshots") or {}),
    }
    payload = {
        "run_id": run_id,
        "results_schema_version": "results_job_ledger_v3",
        "status": RunStatus.SUCCEEDED.value,
        "triggered_by": _string_or_none(getattr(run_record, "triggered_by", "")) or "",
        "run_mode": _normalized_run_mode(getattr(run_record, "run_mode", None)),
        "run_mode_label": _RUN_MODE_LABELS[_normalized_run_mode(getattr(run_record, "run_mode", None))],
        "created_at": _iso_or_none(getattr(run_record, "created_at", None)),
        "started_at": _iso_or_none(getattr(run_record, "started_at", None)),
        "finished_at": finished_at.isoformat(),
        "jobs_path": _string_or_none(getattr(run_record, "jobs_path", "")) or "",
        "jobs_input_source": _string_or_none(getattr(run_record, "jobs_input_source", None)),
        "candidate_profile_source": _string_or_none(getattr(run_record, "candidate_profile_source", None)),
        "summary": {
            "total_jobs": int(summary.get("total_jobs", 0)),
            "passed_filter": int(summary.get("passed_filter", 0)),
            "ranked": int(summary.get("ranked", 0)),
            "cvs_generated": int(summary.get("cvs_generated", 0)),
        },
        "results": _json_safe(export_results),
    }
    if diagnostic_support["late_stage_reuse_snapshots"]:
        payload["diagnostic_support"] = diagnostic_support
    return json.dumps(payload, ensure_ascii=False)


def _collect_late_stage_reuse_snapshots(
    *,
    current_run_id: str,
    bq: Any,
    project: str,
    dataset: str,
) -> dict[str, Any]:
    snapshots: dict[str, Any] = {
        "schema_version": "late_stage_reuse_v1",
        "ranking_ai_scores": [],
        "cv_analysis_records": [],
    }
    try:
        prior_runs = list_runs(
            bq,
            project=project,
            dataset=dataset,
            limit=_LATE_STAGE_REUSE_RUN_SCAN_LIMIT,
            include_archived=True,
        )
    except Exception as exc:
        logger.warning("[run_id=%s] Failed to list prior runs for reuse lookup: %s", current_run_id, exc)
        return snapshots

    for prior_run in prior_runs:
        if prior_run.run_id == current_run_id:
            continue
        if prior_run.status != RunStatus.SUCCEEDED or not prior_run.results_export_json:
            continue
        try:
            payload = json.loads(prior_run.results_export_json)
        except Exception as exc:
            logger.warning(
                "[run_id=%s] Failed to parse prior results_export_json for reuse lookup [source_run_id=%s]: %s",
                current_run_id,
                prior_run.run_id,
                exc,
            )
            continue
        diagnostic_support = dict(payload.get("diagnostic_support") or {})
        reuse_payload = dict(
            diagnostic_support.get("late_stage_reuse_snapshots")
            or payload.get("late_stage_reuse_snapshots")
            or {}
        )
        snapshots["ranking_ai_scores"].extend(
            [
                dict(item)
                for item in list(reuse_payload.get("ranking_ai_scores") or [])
                if isinstance(item, dict)
            ]
        )
        snapshots["cv_analysis_records"].extend(
            [
                dict(item)
                for item in list(reuse_payload.get("cv_analysis_records") or [])
                if isinstance(item, dict)
            ]
        )
    return snapshots


def _build_cv_generation_debug_payload(
    *,
    run_id: str,
    run_record: Any,
    summary: dict[str, Any],
    finished_at: datetime.datetime,
) -> str:
    def _string_or_none(value: Any) -> str | None:
        return value if isinstance(value, str) else None

    def _normalized_run_mode(value: Any) -> str:
        run_mode = _string_or_none(value)
        if run_mode in _RUN_MODE_LABELS:
            return run_mode
        return "run_all"

    def _truncate_large_fields(record: dict[str, Any]) -> dict[str, Any]:
        truncated = dict(record)
        markdown_final = truncated.get("markdown_final")
        if isinstance(markdown_final, str) and len(markdown_final) > _MAX_DEBUG_MARKDOWN_CHARS:
            truncated["markdown_final"] = markdown_final[:_MAX_DEBUG_MARKDOWN_CHARS] + "\n...[truncated]"
        return truncated

    debug_records = [
        _truncate_large_fields(record)
        for record in list(summary.get("cv_generation_debug_records") or [])
    ]
    ranked_jobs_total = int(summary.get("ranked", 0))
    attempted_generation_jobs_total = sum(
        1
        for record in debug_records
        if str(record.get("status") or "") in _CV_GENERATION_ATTEMPTED_STATUSES
    )
    debug_record_job_urls = {
        str(record.get("job_url") or "")
        for record in debug_records
        if str(record.get("job_url") or "")
    }
    omission_reason_counts: dict[str, int] = {}
    for record in debug_records:
        status = str(record.get("status") or "")
        if status in _CV_GENERATION_ATTEMPTED_STATUSES:
            continue
        omission_reason_counts[status] = omission_reason_counts.get(status, 0) + 1
    for record in list(summary.get("cv_analysis_results") or []):
        if not isinstance(record, dict):
            continue
        status = str(record.get("status") or "")
        if status not in _CV_DEBUG_ANALYSIS_OMISSION_STATUSES:
            continue
        job_url = str(record.get("job_url") or "")
        if job_url and job_url in debug_record_job_urls:
            continue
        omission_reason_counts[status] = omission_reason_counts.get(status, 0) + 1
    non_attempted_ranked_jobs_total = sum(omission_reason_counts.values())
    payload = {
        "run_id": run_id,
        "status": RunStatus.SUCCEEDED.value,
        "debug_schema_version": "cv_generation_debug_v3",
        "run_mode": _normalized_run_mode(getattr(run_record, "run_mode", None)),
        "run_mode_label": _RUN_MODE_LABELS[_normalized_run_mode(getattr(run_record, "run_mode", None))],
        "created_at": finished_at.isoformat(),
        "ranked_jobs_total": ranked_jobs_total,
        "debug_records_captured": len(debug_records),
        "attempted_generation_jobs_total": attempted_generation_jobs_total,
        "non_attempted_ranked_jobs_total": non_attempted_ranked_jobs_total,
        "omission_reason_counts": omission_reason_counts,
        "snapshot_complete": len(debug_records) == ranked_jobs_total,
        "debug_records": debug_records,
    }
    return json.dumps(payload, ensure_ascii=False)


def _build_stage_transition_artifacts_payload(
    *,
    run_id: str,
    summary: dict[str, Any],
    finished_at: datetime.datetime,
) -> str:
    stage_artifacts = dict(summary.get("stage_transition_artifacts") or {})
    payload = {
        "run_id": run_id,
        "status": RunStatus.SUCCEEDED.value,
        "artifact_schema_version": "stage_transition_artifacts_run_v1",
        "created_at": finished_at.isoformat(),
        "snapshot_complete": bool(stage_artifacts),
        "artifacts": stage_artifacts,
    }
    return json.dumps(payload, ensure_ascii=False)


def _build_manual_checkpoint_payload(
    *,
    run_id: str,
    summary: dict[str, Any],
    created_at: datetime.datetime,
) -> str:
    payload = {
        "run_id": run_id,
        "checkpoint_schema_version": "manual_checkpoint_v1",
        "created_at": created_at.isoformat(),
        "paused_after_stage": summary.get("paused_after_stage"),
        "next_stage": summary.get("next_stage"),
        "completed_stages": list(summary.get("completed_stages") or []),
        "checkpoint_payload": summary.get("checkpoint_payload") or {},
    }
    return json.dumps(payload, ensure_ascii=False)


def _build_settings_used_payload(
    *,
    run_id: str,
    run_record: Any,
    effective_config: dict[str, Any] | None,
    config_path: str,
    finished_at: datetime.datetime,
) -> str:
    effective_settings = dict(effective_config or {})
    compatibility_projection = {
        key: effective_settings.pop(key)
        for key in list(effective_settings.keys())
        if key in _SETTINGS_COMPATIBILITY_KEYS
    }
    payload = {
        "run_id": run_id,
        "settings_schema_version": "settings_used_v2",
        "created_at": finished_at.isoformat(),
        "effective_settings": effective_settings,
        "sources": {
            "config_path": str(config_path or getattr(run_record, "config_path", "") or ""),
            "effective_settings_snapshot_present": effective_config is not None,
            "jobs_input_source": getattr(run_record, "jobs_input_source", None),
            "candidate_profile_source": getattr(run_record, "candidate_profile_source", None),
            "skill_synonyms_runtime": (
                dict((effective_config or {}).get("skill_synonyms_runtime") or {})
                if isinstance((effective_config or {}).get("skill_synonyms_runtime"), dict)
                else None
            ),
            "prompts_runtime": (
                dict((effective_config or {}).get("prompts_runtime") or {})
                if isinstance((effective_config or {}).get("prompts_runtime"), dict)
                else None
            ),
        },
    }
    if compatibility_projection:
        payload["compatibility_projection"] = compatibility_projection
    return json.dumps(payload, ensure_ascii=False)


def _build_mapping_suggestions_payload(
    *,
    run_id: str,
    summary: dict[str, Any],
    created_at: datetime.datetime,
) -> str:
    payload = {
        "run_id": run_id,
        "mapping_suggestions_schema_version": "mapping_suggestions_v1",
        "created_at": created_at.isoformat(),
        "suggestions": list(summary.get("mapping_suggestions") or []),
    }
    return json.dumps(payload, ensure_ascii=False)


def _summary_has_reached_stage(summary: dict[str, Any], stage_id: str) -> bool:
    normalized_stage_id = str(stage_id or "").strip()
    if not normalized_stage_id:
        return False
    completed_stages = [
        str(item).strip()
        for item in list(summary.get("completed_stages") or [])
        if str(item).strip()
    ]
    if normalized_stage_id in completed_stages:
        return True
    if str(summary.get("last_completed_stage") or "").strip() == normalized_stage_id:
        return True
    stage_transition_artifacts = summary.get("stage_transition_artifacts")
    if not isinstance(stage_transition_artifacts, dict):
        return False
    artifacts = stage_transition_artifacts.get("artifacts")
    stage_root = artifacts if isinstance(artifacts, dict) else stage_transition_artifacts
    if not isinstance(stage_root, dict):
        return False
    stages = stage_root.get("stages")
    if not isinstance(stages, dict):
        return False
    stage_block = stages.get(normalized_stage_id)
    if not isinstance(stage_block, dict):
        return False
    return str(stage_block.get("status") or "").strip().lower() not in {"", "not_reached"}


def _persist_shared_progress_snapshot(
    *,
    run_id: str,
    summary: dict[str, Any],
    snapshot_at: datetime.datetime,
    bq: Any,
    project: str,
    dataset: str,
    run_status: RunStatus,
) -> None:
    update_run_status(
        run_id,
        run_status,
        bq,
        project=project,
        dataset=dataset,
        summary=summary,
    )
    update_run_progress(
        run_id,
        bq,
        project=project,
        dataset=dataset,
        last_completed_stage=str(summary.get("last_completed_stage") or "").strip() or None,
        completed_stages=list(summary.get("completed_stages") or []),
    )
    update_run_stage_transition_artifacts(
        run_id,
        _build_stage_transition_artifacts_payload(
            run_id=run_id,
            summary=summary,
            finished_at=snapshot_at,
        ),
        bq,
        project=project,
        dataset=dataset,
    )
    if _summary_has_reached_stage(summary, "enrich"):
        update_run_mapping_suggestions(
            run_id,
            _build_mapping_suggestions_payload(
                run_id=run_id,
                summary=summary,
                created_at=snapshot_at,
            ),
            bq,
            project=project,
            dataset=dataset,
        )


def execute_pipeline_run(run_id: str, jobs_path: str, config_path: str) -> None:
    project = os.environ.get("GCP_PROJECT", "")
    dataset = os.environ.get("BIGQUERY_DATASET", "fitcv")
    bq = _get_bq()

    # Fall back to config file if GCP_PROJECT env var is not set — the worker
    # may be started without env vars even though the config file is always present.
    if not project:
        try:
            from fitcv.config import load_config as _load_config
            _cfg = _load_config(config_path)
            project = str(_cfg.get("gcp_project", ""))
            dataset = str(_cfg.get("bigquery_dataset", dataset))
        except Exception as exc:
            logger.warning("Could not load config for project/dataset fallback: %s", exc)

    # Import here to avoid circular deps at module load time
    from fitcv_cp.reporter import PipelineReporter

    try:
        current_run_record = get_run(run_id, bq, project=project, dataset=dataset)
        # ── Step 1: Mark running ──────────────────────────────────────────────
        update_run_status(
            run_id, RunStatus.RUNNING, bq, project=project, dataset=dataset,
            started_at=(
                datetime.datetime.now(datetime.timezone.utc)
                if current_run_record is None or getattr(current_run_record, "started_at", None) is None
                else None
            ),
        )

        # ── Step 2: Read current row (reads cancel_requested_at + config snapshot)
        run_record = get_run(run_id, bq, project=project, dataset=dataset)
        effective_config: dict | None = None
        if run_record and run_record.effective_settings_json:
            try:
                effective_config = json.loads(run_record.effective_settings_json)
            except Exception as exc:
                logger.warning("[run_id=%s] Failed to parse effective_settings_json: %s", run_id, exc)
        run_mode = str(getattr(run_record, "run_mode", "run_all") or "run_all")
        next_stage = getattr(run_record, "next_stage", None) or "normalize"
        checkpoint_payload: dict[str, Any] | None = None
        checkpoint_payload_json = getattr(run_record, "checkpoint_payload_json", None)
        if checkpoint_payload_json:
            try:
                checkpoint_container = json.loads(checkpoint_payload_json)
                checkpoint_payload_candidate = checkpoint_container.get("checkpoint_payload")
                if isinstance(checkpoint_payload_candidate, dict):
                    checkpoint_payload = checkpoint_payload_candidate
            except Exception as exc:
                logger.warning("[run_id=%s] Failed to parse checkpoint payload: %s", run_id, exc)

        # ── Step 3: Early-exit if cancellation already requested ──────────────
        if run_record and run_record.cancel_requested_at is not None:
            logger.info("[run_id=%s] Cancellation already requested — exiting early", run_id)
            update_run_status(
                run_id, RunStatus.CANCELLED, bq, project=project, dataset=dataset,
                finished_at=datetime.datetime.now(datetime.timezone.utc),
            )
            append_event(
                _run_cancelled_event(run_id, "Run cancelled before pipeline execution started"),
                bq, project=project, dataset=dataset,
            )
            return

        # ── Step 4: Run pipeline with cooperative cancellation check ──────────
        reporter = PipelineReporter(run_id=run_id, bq=bq, project=project, dataset=dataset)

        def _cancellation_check() -> bool:
            """Lightweight re-read to check if cancel was requested mid-flight."""
            current = get_run(run_id, bq, project=project, dataset=dataset)
            return current is not None and current.cancel_requested_at is not None

        late_stage_reuse_snapshots = _collect_late_stage_reuse_snapshots(
            current_run_id=run_id,
            bq=bq,
            project=project,
            dataset=dataset,
        )

        def _stage_progress_callback(progress_summary: dict[str, Any]) -> None:
            if run_mode != "run_all":
                return
            snapshot_time = datetime.datetime.now(datetime.timezone.utc)
            try:
                _persist_shared_progress_snapshot(
                    run_id=run_id,
                    summary=progress_summary,
                    snapshot_at=snapshot_time,
                    bq=bq,
                    project=project,
                    dataset=dataset,
                    run_status=RunStatus.RUNNING,
                )
            except Exception as exc:
                logger.warning(
                    "[run_id=%s] Failed to persist run-all progress snapshot after %s: %s",
                    run_id,
                    progress_summary.get("last_completed_stage"),
                    exc,
                )
                try:
                    append_event(
                        _snapshot_persist_failed_event(run_id, "stage_progress", str(exc)),
                        bq,
                        project=project,
                        dataset=dataset,
                    )
                except Exception as inner:
                    logger.warning(
                        "[run_id=%s] Failed to append stage progress persistence warning event: %s",
                        run_id,
                        inner,
                    )

        summary = run_pipeline(
            jobs_path=jobs_path,
            config_path=config_path,
            reporter=reporter,
            config=effective_config,
            run_id=run_id,
            cancellation_check=_cancellation_check,
            start_stage=next_stage if run_mode == "manual_staged" else None,
            stop_after_stage=next_stage if run_mode == "manual_staged" else None,
            checkpoint_payload=checkpoint_payload,
            reuse_snapshots=late_stage_reuse_snapshots,
            stage_progress_callback=_stage_progress_callback if run_mode == "run_all" else None,
        )

        paused_after_stage = str(summary.get("paused_after_stage") or "").strip() or None
        if paused_after_stage is not None:
            checkpoint_time = datetime.datetime.now(datetime.timezone.utc)
            update_run_status(
                run_id,
                RunStatus.AWAITING_CONTINUE,
                bq,
                project=project,
                dataset=dataset,
                summary=summary,
            )
            update_run_checkpoint(
                run_id,
                bq,
                project=project,
                dataset=dataset,
                checkpoint_status="awaiting_continue",
                next_stage=summary.get("next_stage"),
                last_completed_stage=paused_after_stage,
                completed_stages=list(summary.get("completed_stages") or []),
                checkpoint_payload_json=_build_manual_checkpoint_payload(
                    run_id=run_id,
                    summary=summary,
                    created_at=checkpoint_time,
                ),
            )
            try:
                update_run_stage_transition_artifacts(
                    run_id,
                    _build_stage_transition_artifacts_payload(
                        run_id=run_id,
                        summary=summary,
                        finished_at=checkpoint_time,
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
            except Exception as exc:
                logger.warning(
                    "[run_id=%s] Failed to persist stage transition artifacts snapshot at checkpoint: %s",
                    run_id,
                    exc,
                )
            if _summary_has_reached_stage(summary, "enrich"):
                try:
                    update_run_mapping_suggestions(
                        run_id,
                        _build_mapping_suggestions_payload(
                            run_id=run_id,
                            summary=summary,
                            created_at=checkpoint_time,
                        ),
                        bq,
                        project=project,
                        dataset=dataset,
                    )
                except Exception as exc:
                    logger.warning(
                        "[run_id=%s] Failed to persist mapping suggestions snapshot at checkpoint: %s",
                        run_id,
                        exc,
                    )
                    try:
                        append_event(
                            _snapshot_persist_failed_event(run_id, "mapping_suggestions", str(exc)),
                            bq,
                            project=project,
                            dataset=dataset,
                        )
                    except Exception as inner:
                        logger.warning(
                            "[run_id=%s] Failed to append mapping suggestions persistence warning event: %s",
                            run_id,
                            inner,
                        )
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="stage_checkpoint",
                    level="info",
                    message=f"Paused after {paused_after_stage}; next stage: {summary.get('next_stage') or 'complete'}",
                    created_at=checkpoint_time,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            return

        # ── Step 5: Success ───────────────────────────────────────────────────
        finished_at = datetime.datetime.now(datetime.timezone.utc)
        update_run_status(
            run_id, RunStatus.SUCCEEDED, bq, project=project, dataset=dataset,
            finished_at=finished_at, summary=summary,
        )
        completed_stages = [
            "normalize",
            "enrich",
            "rule_filter",
            "shortlist",
            "ranking",
            "cv_analysis",
            "cv_generation",
        ]
        if run_mode == "manual_staged":
            update_run_checkpoint(
                run_id,
                bq,
                project=project,
                dataset=dataset,
                checkpoint_status="completed",
                next_stage=None,
                last_completed_stage="cv_generation",
                completed_stages=completed_stages,
                checkpoint_payload_json=None,
            )
        else:
            update_run_progress(
                run_id,
                bq,
                project=project,
                dataset=dataset,
                last_completed_stage="cv_generation",
                completed_stages=completed_stages,
            )
        export_results = list(summary.get("export_results") or [])
        try:
            update_run_results_export(
                run_id,
                _build_results_export_payload(
                    run_id=run_id,
                    run_record=run_record,
                    summary=summary,
                    export_results=export_results,
                    finished_at=finished_at,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        except Exception as exc:
            logger.warning("[run_id=%s] Failed to persist results export snapshot: %s", run_id, exc)
        try:
            update_run_cv_generation_debug(
                run_id,
                _build_cv_generation_debug_payload(
                    run_id=run_id,
                    run_record=run_record,
                    summary=summary,
                    finished_at=finished_at,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        except Exception as exc:
            logger.warning("[run_id=%s] Failed to persist CV generation debug snapshot: %s", run_id, exc)
        try:
            update_run_stage_transition_artifacts(
                run_id,
                _build_stage_transition_artifacts_payload(
                    run_id=run_id,
                    summary=summary,
                    finished_at=finished_at,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        except Exception as exc:
            logger.warning("[run_id=%s] Failed to persist stage transition artifacts snapshot: %s", run_id, exc)
        try:
            update_run_settings_used(
                run_id,
                _build_settings_used_payload(
                    run_id=run_id,
                    run_record=run_record,
                    effective_config=effective_config,
                    config_path=config_path,
                    finished_at=finished_at,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        except Exception as exc:
            logger.warning("[run_id=%s] Failed to persist settings-used snapshot: %s", run_id, exc)
        if _summary_has_reached_stage(summary, "enrich"):
            try:
                update_run_mapping_suggestions(
                    run_id,
                    _build_mapping_suggestions_payload(
                        run_id=run_id,
                        summary=summary,
                        created_at=finished_at,
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
            except Exception as exc:
                logger.warning("[run_id=%s] Failed to persist mapping suggestions snapshot: %s", run_id, exc)
                try:
                    append_event(
                        _snapshot_persist_failed_event(run_id, "mapping_suggestions", str(exc)),
                        bq,
                        project=project,
                        dataset=dataset,
                    )
                except Exception as inner:
                    logger.warning(
                        "[run_id=%s] Failed to append mapping suggestions persistence warning event: %s",
                        run_id,
                        inner,
                    )

    except PipelineCancelled as exc:
        # ── Step 5 (alt): Pipeline was cancelled at a checkpoint ──────────────
        logger.info("[run_id=%s] Pipeline cancelled at checkpoint: %s", run_id, exc)
        update_run_status(
            run_id, RunStatus.CANCELLED, bq, project=project, dataset=dataset,
            finished_at=datetime.datetime.now(datetime.timezone.utc),
        )
        try:
            append_event(
                _run_cancelled_event(run_id, f"Run cancelled at pipeline checkpoint: {exc}"),
                bq, project=project, dataset=dataset,
            )
        except Exception as inner:
            logger.warning("[run_id=%s] Failed to write cancellation event: %s", run_id, inner)

    except Exception as exc:
        # ── Step 7: Unexpected pipeline failure ───────────────────────────────
        logger.error("[run_id=%s] Pipeline failed: %s", run_id, exc)
        update_run_status(
            run_id, RunStatus.FAILED, bq, project=project, dataset=dataset,
            finished_at=datetime.datetime.now(datetime.timezone.utc), error_message=str(exc),
        )
        try:
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="pipeline_failed",
                    level="error",
                    message=str(exc),
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        except Exception as inner:
            logger.warning("[run_id=%s] Failed to write failure event: %s", run_id, inner)
