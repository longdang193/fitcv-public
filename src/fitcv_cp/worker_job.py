"""@meta
name: worker_job
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.worker_job.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import datetime
import hashlib
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from fitcv.config import apply_runtime_skill_synonym_overlay, parse_skill_synonym_overlay_yaml
from fitcv.pipeline import PipelineCancelled, run_pipeline
from fitcv.telemetry import (
    build_langfuse_trace_attributes,
    observe_span,
    set_span_attributes,
)
from fitcv_cp.backend_runtime import resolve_backend_runtime
from fitcv_cp.bq_store import (
    append_event,
    get_events,
    get_run,
    list_runs,
    update_run_checkpoint,
    update_run_progress,
    update_run_cv_generation_debug,
    update_run_mapping_suggestions,
    update_run_effective_settings,
    update_run_synonym_proposals,
    update_run_results_export,
    update_run_settings_used,
    update_run_stage_transition_artifacts,
    update_run_status,
)
from fitcv_cp.models import RunEvent, RunStatus
from fitcv_cp.data_plane import data_plane_contract_payload

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
    "review_required",
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
_NON_SKILL_MIN_SUPPORT_FOR_PROPOSAL = 2
_WINDOWS_ABSOLUTE_PATH_PATTERN = re.compile(r"^[A-Za-z]:[\\/]")
LOW_RISK_AUTO_ACCEPT_REASON_CODES = {
    "provider_response_unusable",
}


def _stage_deterministic_summary(
    *,
    stage_id: str,
    output_counts: dict[str, Any] | None,
) -> dict[str, Any]:
    counts = dict(output_counts or {})
    if stage_id == "cv_analysis":
        return {
            "source_stage": "cv_analysis",
            "stage_owned_subreason": "stage_summary",
            "deterministic_outcome": None,
            "outcome_counts": {
                "ready_for_generation": int(counts.get("ready_for_generation") or 0),
                "blocked_by_reranker_fit": int(counts.get("blocked_by_reranker_fit") or 0),
                "skipped_fit_gate": int(counts.get("skipped_fit_gate") or 0),
                "analysis_failed": int(counts.get("analysis_failed") or 0),
            },
        }
    if stage_id == "cv_generation":
        return {
            "source_stage": "cv_generation",
            "stage_owned_subreason": "stage_summary",
            "deterministic_outcome": None,
            "outcome_counts": {
                "accepted": int(counts.get("accepted") or 0),
                "review_required": int(counts.get("review_required") or 0),
                "validation_failed": int(counts.get("validation_failed") or 0),
                "generation_failed": int(counts.get("generation_failed") or 0),
                "persistence_failed": int(counts.get("persistence_failed") or 0),
            },
        }
    return {
        "source_stage": None,
        "stage_owned_subreason": None,
        "deterministic_outcome": None,
        "outcome_counts": {},
    }

def _policy_registry_version(config_payload: dict[str, Any] | None) -> str:
    cfg = dict(config_payload or {})
    block = dict(cfg.get("policy_registry") or {})
    return str(block.get("version") or "policy_registry.v1")

def _policy_envelope_signature(config_payload: dict[str, Any] | None) -> str:
    cfg = dict(config_payload or {})
    envelope = {
        "ranking_weights": dict(cfg.get("ranking_weights") or {}),
        "preference_fit_weights": dict(cfg.get("preference_fit_weights") or {}),
        "missing_value_defaults": dict(cfg.get("missing_value_defaults") or {}),
        "fit_label_thresholds": dict(cfg.get("fit_label_thresholds") or {}),
        "cv": dict(cfg.get("cv") or {}),
        "pipeline": dict(cfg.get("pipeline") or {}),
        "prompts_runtime": dict(cfg.get("prompts_runtime") or {}),
    }
    raw = json.dumps(envelope, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _resolve_run_replay_context(
    *,
    effective_config: dict[str, Any] | None,
    run_id: str,
) -> dict[str, Any]:
    cfg = dict(effective_config or {})
    runtime_inputs = dict(cfg.get("runtime_inputs") or {})
    replay = dict(runtime_inputs.get("replay_context") or {})
    replay_mode = str(replay.get("replay_mode") or "strict").strip().lower() or "strict"
    if replay_mode not in {"strict", "policy_replay"}:
        replay_mode = "strict"
    return {
        "replay_mode": replay_mode,
        "replay_source_run_id": str(replay.get("replay_source_run_id") or run_id),
        "policy_registry_version": str(replay.get("policy_registry_version") or _policy_registry_version(cfg)),
        "policy_envelope_signature": str(replay.get("policy_envelope_signature") or _policy_envelope_signature(cfg)),
    }


def _get_bq() -> bigquery.Client:
    return bigquery.Client()


def _normalize_runtime_service_account_key(
    effective_config: dict[str, Any] | None,
    *,
    run_id: str,
) -> dict[str, Any] | None:
    """Normalize service_account_key for Linux/container runtime safety."""
    if not isinstance(effective_config, dict):
        return effective_config
    key_path = str(effective_config.get("service_account_key") or "").strip()
    if not key_path:
        return effective_config
    if os.name == "nt":
        return effective_config
    if not _WINDOWS_ABSOLUTE_PATH_PATTERN.match(key_path):
        return effective_config

    normalized = dict(effective_config)
    env_key_path = str(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if env_key_path and Path(env_key_path).exists():
        normalized["service_account_key"] = env_key_path
        logger.warning(
            "[run_id=%s] Normalized Windows service_account_key %r to runtime credential path %r",
            run_id,
            key_path,
            env_key_path,
        )
        return normalized

    fallback_path = "/app/sa_key.json"
    if Path(fallback_path).exists():
        normalized["service_account_key"] = fallback_path
        logger.warning(
            "[run_id=%s] Normalized Windows service_account_key %r to container fallback %r",
            run_id,
            key_path,
            fallback_path,
        )
    else:
        logger.warning(
            "[run_id=%s] Windows service_account_key %r detected in non-Windows runtime and no fallback key file was found.",
            run_id,
            key_path,
        )
    return normalized


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


def _append_degraded_snapshot_persistence_warning(
    *,
    run_id: str,
    snapshot_name: str,
    persistence_status: dict[str, str] | None,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    status = dict(persistence_status or {})
    if status.get("persistence_status") in {"persisted", "not_applicable", ""}:
        return
    append_event(
        _snapshot_persist_failed_event(
            run_id,
            snapshot_name,
            str(status.get("degradation_reason") or status.get("persistence_status") or "unknown_degradation"),
        ),
        bq,
        project=project,
        dataset=dataset,
    )


def _estimate_jobs_count_from_input(jobs_path: str) -> int:
    try:
        payload = json.loads(Path(jobs_path).read_text(encoding="utf-8"))
    except Exception:
        return 0
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        jobs = payload.get("jobs")
        if isinstance(jobs, list):
            return len(jobs)
    return 0


def _config_agentic_late_stage_enabled(config: dict[str, Any] | None) -> bool:
    cv_block = dict((config or {}).get("cv") or {})
    late_stage_block = dict(cv_block.get("agentic_late_stage") or {})
    return bool(late_stage_block.get("enabled"))


def _build_late_stage_mode_payload(
    *,
    summary: dict[str, Any],
    effective_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    existing_payload = summary.get("late_stage_mode")
    if isinstance(existing_payload, dict):
        return dict(existing_payload)
    agentic_enabled = _config_agentic_late_stage_enabled(effective_config)
    return {
        "late_stage_mode": "agentic" if agentic_enabled else "non_agentic",
        "agentic_late_stage_enabled": agentic_enabled,
        "mode_source": "cv.agentic_late_stage.enabled",
        "agentic_status": "completed" if agentic_enabled else "not_applicable",
    }


def _build_results_export_payload(
    *,
    run_id: str,
    run_record: Any,
    effective_config: dict[str, Any] | None,
    summary: dict[str, Any],
    export_results: list[dict[str, Any]],
    finished_at: datetime.datetime,
    replay_context: dict[str, Any],
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
    stage_result_summary: dict[str, Any] = {}
    stage_transition_artifacts = dict(summary.get("stage_transition_artifacts") or {})
    stage_blocks = dict(stage_transition_artifacts.get("stages") or {})
    for stage_id, block in stage_blocks.items():
        if not isinstance(block, dict):
            continue
        stage_result = dict(block.get("stage_result") or {})
        trace_context = dict(stage_result.get("trace_context") or {})
        deterministic_summary = _stage_deterministic_summary(
            stage_id=str(stage_id),
            output_counts=dict(block.get("output_counts") or {}),
        )
        stage_result_summary[str(stage_id)] = {
            "status": str(block.get("status") or ""),
            "decision": _json_safe(stage_result.get("decision") or {}),
            "policy_version": str(stage_result.get("policy_version") or ""),
            "trace_context": {
                "trace_id": str(trace_context.get("trace_id") or ""),
                "span_id": str(trace_context.get("span_id") or ""),
                "parent_span_id": str(trace_context.get("parent_span_id") or ""),
            },
            "source_stage": deterministic_summary["source_stage"],
            "deterministic_outcome": deterministic_summary["deterministic_outcome"],
            "stage_owned_subreason": deterministic_summary["stage_owned_subreason"],
            "outcome_counts": deterministic_summary["outcome_counts"],
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
        "late_stage_mode": _build_late_stage_mode_payload(summary=summary),
        "stage_result_summary": stage_result_summary,
        "data_plane": data_plane_contract_payload(effective_config),
        "replay_context": {
            "replay_mode": str(replay_context.get("replay_mode") or "strict"),
            "replay_source_run_id": str(replay_context.get("replay_source_run_id") or run_id),
            "policy_registry_version": str(replay_context.get("policy_registry_version") or "policy_registry.v1"),
            "policy_envelope_signature": str(replay_context.get("policy_envelope_signature") or ""),
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
    for record in debug_records:
        if not isinstance(record, dict):
            continue
        ranking_fit_label = record.get("ranking_fit_label")
        reranker_fit_label = record.get("reranker_fit_label")
        if ranking_fit_label is None and reranker_fit_label is not None:
            record["ranking_fit_label"] = reranker_fit_label
        if reranker_fit_label is None and ranking_fit_label is not None:
            record["reranker_fit_label"] = ranking_fit_label
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
    if isinstance(summary.get("agentic_live_trace"), dict):
        payload["agentic_live_trace"] = dict(summary["agentic_live_trace"])
    if isinstance(summary.get("cv_analysis_trace"), dict):
        payload["cv_analysis_trace"] = dict(summary["cv_analysis_trace"])
    return json.dumps(payload, ensure_ascii=False)


def _build_stage_transition_artifacts_payload(
    *,
    run_id: str,
    summary: dict[str, Any],
    finished_at: datetime.datetime,
    run_status: RunStatus = RunStatus.SUCCEEDED,
    degradation_reason: str | None = None,
) -> str:
    stage_artifacts = dict(summary.get("stage_transition_artifacts") or {})
    snapshot_complete = bool(stage_artifacts) and run_status == RunStatus.SUCCEEDED
    resolved_reason = (
        str(degradation_reason or "").strip()
        or ("partial_snapshot_non_terminal_success" if run_status != RunStatus.SUCCEEDED else "")
    )
    payload = {
        "run_id": run_id,
        "status": run_status.value,
        "artifact_schema_version": "stage_transition_artifacts_run_v1",
        "created_at": finished_at.isoformat(),
        "snapshot_complete": snapshot_complete,
        "degradation_reason": resolved_reason,
        "artifacts": stage_artifacts,
    }
    return json.dumps(payload, ensure_ascii=False)


def _build_manual_checkpoint_payload(
    *,
    run_id: str,
    summary: dict[str, Any],
    created_at: datetime.datetime,
    replay_context: dict[str, Any],
) -> str:
    payload = {
        "run_id": run_id,
        "checkpoint_schema_version": "manual_checkpoint_v1",
        "created_at": created_at.isoformat(),
        "paused_after_stage": summary.get("paused_after_stage"),
        "next_stage": summary.get("next_stage"),
        "completed_stages": list(summary.get("completed_stages") or []),
        "checkpoint_payload": summary.get("checkpoint_payload") or {},
        "replay_context": {
            "replay_mode": str(replay_context.get("replay_mode") or "strict"),
            "replay_source_run_id": str(replay_context.get("replay_source_run_id") or run_id),
            "policy_registry_version": str(replay_context.get("policy_registry_version") or "policy_registry.v1"),
            "policy_envelope_signature": str(replay_context.get("policy_envelope_signature") or ""),
        },
    }
    return json.dumps(payload, ensure_ascii=False)


def _build_settings_used_payload(
    *,
    run_id: str,
    run_record: Any,
    effective_config: dict[str, Any] | None,
    config_path: str,
    finished_at: datetime.datetime,
    replay_context: dict[str, Any],
) -> str:
    effective_settings = dict(effective_config or {})
    sqlite_mode = resolve_backend_runtime().backend_type == "sqlite"
    if sqlite_mode:
        effective_settings.pop("service_account_key", None)
    compatibility_projection = {
        key: effective_settings.pop(key)
        for key in list(effective_settings.keys())
        if key in _SETTINGS_COMPATIBILITY_KEYS
    }
    if sqlite_mode and isinstance(compatibility_projection, dict):
        compatibility_projection.pop("service_account_key", None)
    payload = {
        "run_id": run_id,
        "settings_schema_version": "settings_used_v2",
        "created_at": finished_at.isoformat(),
        "late_stage_mode": _build_late_stage_mode_payload(
            summary={},
            effective_config=effective_config,
        ),
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
        "data_plane": data_plane_contract_payload(effective_config),
        "replay_context": {
            "replay_mode": str(replay_context.get("replay_mode") or "strict"),
            "replay_source_run_id": str(replay_context.get("replay_source_run_id") or run_id),
            "policy_registry_version": str(replay_context.get("policy_registry_version") or "policy_registry.v1"),
            "policy_envelope_signature": str(replay_context.get("policy_envelope_signature") or ""),
        },
    }
    if sqlite_mode:
        data_plane = dict(payload.get("data_plane") or {})
        data_plane["state_backend"] = "sqlite"
        if str(data_plane.get("artifact_backend") or "").strip().lower() in {"", "bigquery_json"}:
            data_plane["artifact_backend"] = "sqlite_json"
        payload["data_plane"] = data_plane
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


def _build_synonym_proposals_payload(
    *,
    run_id: str,
    summary: dict[str, Any],
    created_at: datetime.datetime,
    existing_payload_json: str | None = None,
    global_synonyms: dict[str, str] | None = None,
) -> str:
    existing_proposals_by_id: dict[str, dict[str, Any]] = {}
    if existing_payload_json:
        try:
            existing_payload = json.loads(existing_payload_json)
        except (TypeError, json.JSONDecodeError):
            existing_payload = None
        if isinstance(existing_payload, dict):
            for existing_proposal in list(existing_payload.get("proposals") or []):
                if not isinstance(existing_proposal, dict):
                    continue
                proposal_id = str(existing_proposal.get("proposal_id") or "").strip()
                if proposal_id:
                    existing_proposals_by_id[proposal_id] = existing_proposal

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for suggestion in list(summary.get("mapping_suggestions") or []):
        if not isinstance(suggestion, dict):
            continue
        field = str(suggestion.get("field") or "skill").strip().lower() or "skill"
        alias = str(suggestion.get("alias") or "").strip().lower()
        canonical = str(suggestion.get("canonical") or "").strip().lower()
        if not alias or not canonical:
            continue
        bucket = grouped.setdefault(
            (field, alias),
            {
                "field": field,
                "alias": alias,
                "candidate_canonicals": {},
                "must_have_skills": set(),
                "job_refs": [],
                "occurrence_count": 0,
                "confidence_sum": 0.0,
            },
        )
        bucket["occurrence_count"] += 1
        confidence = float(suggestion.get("confidence") or 0.0)
        bucket["confidence_sum"] += confidence
        bucket["candidate_canonicals"][canonical] = (
            bucket["candidate_canonicals"].get(canonical, 0) + 1
        )
        must_have_skill = str(suggestion.get("must_have_skill") or "").strip().lower()
        if must_have_skill:
            bucket["must_have_skills"].add(must_have_skill)
        job_url = str(suggestion.get("job_url") or "").strip()
        if job_url and len(bucket["job_refs"]) < 5:
            bucket["job_refs"].append(
                {
                    "job_url": job_url,
                    "job_title": str(suggestion.get("job_title") or "").strip(),
                    "confidence": confidence,
                }
            )

    proposals: list[dict[str, Any]] = []
    normalized_global_synonyms: dict[str, str] = {}
    if isinstance(global_synonyms, dict):
        normalized_global_synonyms = {
            str(alias).strip().lower(): str(canonical).strip().lower()
            for alias, canonical in global_synonyms.items()
            if str(alias).strip() and str(canonical).strip()
        }
    suppressed_as_already_global_count = 0
    suppressed_count_by_field: dict[str, int] = {}
    suppressed_reason_counts_by_field: dict[str, dict[str, int]] = {}
    suppressed_examples: list[dict[str, str]] = []
    for (_field_alias_key, bucket) in grouped.items():
        field = str(bucket.get("field") or "skill")
        alias = str(bucket.get("alias") or "")
        ranked_canonicals = sorted(
            bucket["candidate_canonicals"].items(),
            key=lambda item: (-int(item[1]), item[0]),
        )
        candidate_canonicals = [canonical for canonical, _count in ranked_canonicals]
        primary_canonical = candidate_canonicals[0]
        has_conflict = len(candidate_canonicals) > 1
        proposal_family = "conflict_bundle" if has_conflict else "alias_to_canonical_mapping"
        occurrence_count = int(bucket["occurrence_count"])
        avg_confidence = (
            float(bucket["confidence_sum"]) / occurrence_count if occurrence_count else 0.0
        )
        if field in {"domain", "role_family"} and occurrence_count < _NON_SKILL_MIN_SUPPORT_FOR_PROPOSAL:
            suppressed_count_by_field[field] = suppressed_count_by_field.get(field, 0) + 1
            reason_bucket = suppressed_reason_counts_by_field.setdefault(field, {})
            reason_bucket["insufficient_non_skill_support"] = (
                reason_bucket.get("insufficient_non_skill_support", 0) + 1
            )
            if len(suppressed_examples) < 10:
                suppressed_examples.append(
                    {
                        "field": field,
                        "alias": alias,
                        "canonical": primary_canonical,
                    }
                )
            continue
        identity_seed = f"{run_id}:{field}:{alias}:{'|'.join(candidate_canonicals)}:{proposal_family}"
        proposal_id = f"synprop-{hashlib.sha1(identity_seed.encode('utf-8')).hexdigest()[:12]}"
        existing_proposal = existing_proposals_by_id.get(proposal_id) or {}
        global_canonical = normalized_global_synonyms.get(alias) if field == "skill" else None
        if global_canonical and global_canonical == primary_canonical:
            suppressed_as_already_global_count += 1
            suppressed_count_by_field[field] = suppressed_count_by_field.get(field, 0) + 1
            reason_bucket = suppressed_reason_counts_by_field.setdefault(field, {})
            reason_bucket["already_global_exact"] = reason_bucket.get("already_global_exact", 0) + 1
            if len(suppressed_examples) < 10:
                suppressed_examples.append({"field": field, "alias": alias, "canonical": primary_canonical})
            continue
        proposals.append(
            {
                "proposal_id": proposal_id,
                "run_id": run_id,
                "field": field,
                "alias": alias,
                "canonical": primary_canonical,
                "candidate_aliases": [alias],
                "candidate_canonicals": candidate_canonicals,
                "confidence": round(avg_confidence, 6),
                "rationale": {
                    "kind": "alias_conflict" if has_conflict else "repeated_alias_mapping",
                    "occurrence_count": occurrence_count,
                    "distinct_canonical_count": len(candidate_canonicals),
                },
                "evidence_summary": {
                    "occurrence_count": occurrence_count,
                    "average_confidence": round(avg_confidence, 6),
                    "must_have_skills": sorted(bucket["must_have_skills"]),
                    "sample_job_refs": list(bucket["job_refs"]),
                },
                "conflict_summary": {
                    "has_conflict": has_conflict,
                    "conflicting_canonicals": candidate_canonicals[1:],
                },
                "proposal_status": str(existing_proposal.get("proposal_status") or "proposed_unreviewed"),
                "proposal_scope": "run_scoped_overlay_candidate",
                "proposal_family": proposal_family,
                "source_artifact_refs": {
                    "run_id": run_id,
                    "artifact_type": "mapping_suggestions",
                },
                "review_history": list(existing_proposal.get("review_history") or []),
            }
        )
    proposals.sort(key=lambda item: (-float(item["confidence"]), str(item["alias"])))
    payload = {
        "run_id": run_id,
        "synonym_proposals_schema_version": "synonym_proposals_v1",
        "created_at": created_at.isoformat(),
        "proposal_generation_status": "generated" if proposals else "not_applicable",
        "persistence_status": "persisted" if proposals else "not_applicable",
        "proposals": proposals,
    }
    payload["synonym_proposals_trace"] = _build_synonym_proposals_trace_payload(
        run_id=run_id,
        created_at=created_at,
        proposal_generation_status=str(payload["proposal_generation_status"] or ""),
        persistence_status=str(payload["persistence_status"] or ""),
        proposals=proposals,
        suppression_summary={
            "suppressed_as_already_global_count": suppressed_as_already_global_count,
            "generated_for_review_count": len(proposals),
            "suppressed_count_by_field": suppressed_count_by_field,
            "suppressed_reason_counts_by_field": suppressed_reason_counts_by_field,
            "suppressed_examples": suppressed_examples,
            "suppression_source": (
                "run_effective_skill_synonyms"
                if normalized_global_synonyms
                else "none"
            ),
        },
    )
    return json.dumps(payload, ensure_ascii=False)

def _effective_skill_synonyms_from_run_record(run_record: Any) -> dict[str, str]:
    if run_record is None:
        return {}
    raw_payload = getattr(run_record, "effective_settings_json", None)
    if not raw_payload:
        return {}
    try:
        settings_payload = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(settings_payload, dict):
        return {}
    raw_synonyms = settings_payload.get("skill_synonyms")
    if not isinstance(raw_synonyms, dict):
        return {}
    return {
        str(alias).strip().lower(): str(canonical).strip().lower()
        for alias, canonical in raw_synonyms.items()
        if str(alias).strip() and str(canonical).strip()
    }

def _synonym_propose_enabled_from_run_record(run_record: Any) -> bool:
    if run_record is None:
        return True
    raw_payload = getattr(run_record, "effective_settings_json", None)
    if not raw_payload:
        return True
    try:
        settings_payload = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError):
        return True
    if not isinstance(settings_payload, dict):
        return True
    block = dict(settings_payload.get("synonym_management") or {})
    return bool(block.get("propose_enabled", True))


def _auto_accept_ai_action_enabled_from_run_record(run_record: Any) -> bool:
    if run_record is None:
        return True
    raw_payload = getattr(run_record, "effective_settings_json", None)
    if not raw_payload:
        return True
    try:
        settings_payload = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError):
        return True
    if not isinstance(settings_payload, dict):
        return True
    block = dict(settings_payload.get("synonym_management") or {})
    return bool(block.get("auto_accept_ai_action_enabled", True))

def _synonym_management_mode_from_run_record(run_record: Any) -> dict[str, bool]:
    if run_record is None:
        return {
            "propose_enabled": True,
            "apply_to_run_enabled": True,
            "promote_global_enabled": True,
            "auto_triage_recommendation_enabled": True,
            "triage_recommendation_reuse_enabled": True,
            "auto_apply_recommendation_enabled": False,
            "auto_promote_global_enabled": False,
        }
    raw_payload = getattr(run_record, "effective_settings_json", None)
    if not raw_payload:
        return {
            "propose_enabled": True,
            "apply_to_run_enabled": True,
            "promote_global_enabled": True,
            "auto_triage_recommendation_enabled": True,
            "triage_recommendation_reuse_enabled": True,
            "auto_apply_recommendation_enabled": False,
            "auto_promote_global_enabled": False,
        }
    try:
        settings_payload = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError):
        settings_payload = {}
    if not isinstance(settings_payload, dict):
        settings_payload = {}
    block = dict(settings_payload.get("synonym_management") or {})
    return {
        "propose_enabled": bool(block.get("propose_enabled", True)),
        "apply_to_run_enabled": bool(block.get("apply_to_run_enabled", True)),
        "promote_global_enabled": bool(block.get("promote_global_enabled", True)),
        "auto_triage_recommendation_enabled": bool(block.get("auto_triage_recommendation_enabled", True)),
        "triage_recommendation_reuse_enabled": bool(block.get("triage_recommendation_reuse_enabled", True)),
        "auto_apply_recommendation_enabled": bool(block.get("auto_apply_recommendation_enabled", False)),
        "auto_promote_global_enabled": bool(block.get("auto_promote_global_enabled", False)),
    }

def _stable_sha256_json(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _transition_synonym_proposal_status(current_status: str, action: str) -> str | None:
    transitions = {
        "approve_for_run_overlay": {
            "proposed_unreviewed": "approved_for_run_overlay",
            "in_review": "approved_for_run_overlay",
            "deferred": "approved_for_run_overlay",
        },
        "reject": {
            "proposed_unreviewed": "rejected",
            "in_review": "rejected",
            "deferred": "rejected",
        },
        "defer": {
            "proposed_unreviewed": "deferred",
            "in_review": "deferred",
        },
    }
    return transitions.get(action, {}).get(str(current_status or "").strip())

def _triage_synonym_proposal_recommendation_builtin(proposal: dict[str, Any], *, now_iso: str) -> dict[str, Any]:
    alias = str(proposal.get("alias") or "").strip().lower()
    canonical = str(proposal.get("canonical") or "").strip().lower()
    confidence = float(proposal.get("confidence") or 0.0)
    candidate_canonicals = [
        str(item).strip().lower()
        for item in list(proposal.get("candidate_canonicals") or [])
        if str(item).strip()
    ]
    risk_flags: list[str] = []
    rationale = "Alias/canonical pair appears stable for run-scoped overlay."
    recommended_action = "approve"
    recommendation_confidence = min(max(confidence, 0.0), 1.0)

    if not alias or not canonical:
        recommended_action = "reject"
        recommendation_confidence = 0.98
        rationale = "Alias or canonical is empty after normalization."
        risk_flags.append("invalid_mapping_shape")
    elif len(set(candidate_canonicals)) > 1:
        recommended_action = "defer"
        recommendation_confidence = max(0.55, min(confidence, 0.85))
        rationale = "Alias maps to multiple canonical candidates; review conflict manually."
        risk_flags.append("alias_canonical_conflict")
    elif confidence < 0.50:
        recommended_action = "reject"
        recommendation_confidence = min(0.95, 1.0 - confidence + 0.2)
        rationale = "Low confidence mapping is likely noisy and should be rejected."
        risk_flags.append("low_confidence")
    elif confidence < 0.75:
        recommended_action = "defer"
        recommendation_confidence = min(0.85, confidence + 0.1)
        rationale = "Moderate confidence mapping should be deferred for review."
        risk_flags.append("moderate_confidence")

    return {
        "recommended_action": recommended_action,
        "recommendation_confidence": round(float(recommendation_confidence), 3),
        "recommendation_rationale": rationale,
        "recommendation_risk_flags": risk_flags,
        "recommendation_runtime": {
            "provider": "fitcv_builtin",
            "model": "synonym_triage_v1",
            "wire_api": "builtin",
            "triage_at": now_iso,
            "triage_version": "synonym_triage_v1",
        },
    }

def _global_skill_synonyms_path() -> Path:
    return Path("config") / "taxonomy" / "skill_synonyms.yaml"

def _load_global_skill_synonyms_map() -> dict[str, str]:
    path = _global_skill_synonyms_path()
    if not path.exists():
        return {}
    return parse_skill_synonym_overlay_yaml(path.read_text(encoding="utf-8"))

def _build_synonym_overlay_yaml(overlay: dict[str, str]) -> str:
    if not overlay:
        return ""
    lines = ["skill_synonyms:"]
    for alias, canonical in sorted(overlay.items()):
        lines.append(f"  {alias}: {canonical}")
    return "\n".join(lines) + "\n"

def _persist_global_skill_synonyms_map(mappings: dict[str, str]) -> None:
    path = _global_skill_synonyms_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_build_synonym_overlay_yaml(mappings), encoding="utf-8")


def _map_review_required_reason_code(record: dict[str, Any]) -> str:
    explicit_code = str(record.get("review_required_reason_code") or "").strip()
    if explicit_code and explicit_code != "unknown":
        return explicit_code
    error = dict(record.get("error") or {})
    stage = str(error.get("stage") or "").strip().lower()
    message = str(error.get("message") or record.get("operator_note") or "").strip().lower()
    if "unsupported requirements require review" in message:
        return "unsupported_requirement_gap"
    if stage == "markdown_quality_review" or "markdown quality" in message:
        return "quality_gate_failed"
    if stage == "validation" or "validation failed" in message or "guardrail" in message:
        return "validation_guardrail_failed"
    if "insufficient evidence" in message or "evidence coverage" in message:
        return "evidence_coverage_insufficient"
    if stage in {"provider", "llm"} or "provider" in message or "response unusable" in message:
        return "provider_response_unusable"
    return "manual_review_other"


def _build_synonym_proposals_trace_payload(
    *,
    run_id: str,
    created_at: datetime.datetime,
    proposal_generation_status: str,
    persistence_status: str,
    proposals: list[dict[str, Any]],
    suppression_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if proposal_generation_status == "not_applicable":
        return {
            "run_id": run_id,
            "trace_schema_version": "agentic_step_trace_run_v1",
            "trace_family": "agentic_step_trace",
            "step_id": "synonym_proposals",
            "created_at": created_at.isoformat(),
            "trace_status": "not_applicable",
            "trace_summary": {
                "records_total": 0,
                "present_records": 0,
                "proposal_count": 0,
                "suppressed_as_already_global_count": int(
                    (suppression_summary or {}).get("suppressed_as_already_global_count") or 0
                ),
                "generated_for_review_count": int(
                    (suppression_summary or {}).get("generated_for_review_count") or 0
                ),
                "suppression_source": str((suppression_summary or {}).get("suppression_source") or "none"),
                "suppressed_count_by_field": dict((suppression_summary or {}).get("suppressed_count_by_field") or {}),
                "suppressed_reason_counts_by_field": dict((suppression_summary or {}).get("suppressed_reason_counts_by_field") or {}),
            },
            "records": [],
            "degradation": {},
            "artifact_refs": {},
            "suppression_examples": list((suppression_summary or {}).get("suppressed_examples") or []),
        }

    trace_records: list[dict[str, Any]] = []
    for proposal in proposals:
        if not isinstance(proposal, dict):
            continue
        proposal_id = str(proposal.get("proposal_id") or "").strip()
        alias = str(proposal.get("alias") or "").strip()
        trace_records.append(
            {
                "trace_schema_version": "agentic_step_trace_record_v1",
                "trace_family": "agentic_step_trace",
                "step_id": "synonym_proposals",
                "trace_status": "completed",
                "record_id": proposal_id or alias,
                "scope_type": "alias",
                "scope_key": alias,
                "status": str(proposal.get("proposal_status") or "proposed_unreviewed"),
                "runtime_provenance": {
                    "runtime_path": "fitcv_synonym_proposal_builder_builtin",
                    "provider": "fitcv_builtin",
                    "mode_source": "mapping_suggestions_to_synonym_proposals",
                },
                "attempts": [
                    {
                        "attempt_index": 1,
                        "attempt_type": "proposal_generation",
                        "attempt_status": "completed",
                        "provider_status": "completed",
                    }
                ],
                "input_summary": {
                    "alias": alias,
                    "candidate_canonicals_count": len(list(proposal.get("candidate_canonicals") or [])),
                },
                "output_summary": {
                    "proposal_family": str(proposal.get("proposal_family") or ""),
                    "proposal_scope": str(proposal.get("proposal_scope") or ""),
                    "confidence": float(proposal.get("confidence") or 0.0),
                },
                "validation_summary": {"status": "not_run"},
                "repair_summary": {"repair_attempted": False, "repair_attempts": 0},
                "error_summary": None,
            }
        )

    trace_status = "completed"
    degradation: dict[str, Any] = {}
    if persistence_status == "bundle_only_degraded":
        trace_status = "degraded"
        degradation = {"reason": "synonym_proposals_bundle_only_degraded"}
    elif persistence_status == "failed":
        trace_status = "degraded"
        degradation = {"reason": "synonym_proposals_persistence_failed"}
    elif not trace_records:
        trace_status = "partial"
        degradation = {"reason": "proposal_generation_without_trace_records"}

    return {
        "run_id": run_id,
        "trace_schema_version": "agentic_step_trace_run_v1",
        "trace_family": "agentic_step_trace",
        "step_id": "synonym_proposals",
        "created_at": created_at.isoformat(),
        "trace_status": trace_status,
        "trace_summary": {
            "records_total": len(proposals),
            "present_records": len(trace_records),
            "proposal_count": len(proposals),
            "suppressed_as_already_global_count": int(
                (suppression_summary or {}).get("suppressed_as_already_global_count") or 0
            ),
            "generated_for_review_count": int(
                (suppression_summary or {}).get("generated_for_review_count") or len(proposals)
            ),
            "suppression_source": str((suppression_summary or {}).get("suppression_source") or "none"),
            "suppressed_count_by_field": dict((suppression_summary or {}).get("suppressed_count_by_field") or {}),
            "suppressed_reason_counts_by_field": dict((suppression_summary or {}).get("suppressed_reason_counts_by_field") or {}),
        },
        "records": trace_records,
        "degradation": degradation,
        "artifact_refs": {
            "proposal_artifact": "synonym-proposals.json",
            "stage_artifact": "enrich.json",
        },
        "suppression_examples": list((suppression_summary or {}).get("suppressed_examples") or []),
    }


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

def _append_synonym_suppression_summary_event(
    *,
    run_id: str,
    synonym_payload_json: str,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    try:
        payload = json.loads(synonym_payload_json)
    except (TypeError, json.JSONDecodeError):
        return
    if not isinstance(payload, dict):
        return
    trace_payload = payload.get("synonym_proposals_trace")
    if not isinstance(trace_payload, dict):
        return
    trace_summary = trace_payload.get("trace_summary")
    if not isinstance(trace_summary, dict):
        return
    suppressed_count = int(trace_summary.get("suppressed_as_already_global_count") or 0)
    if suppressed_count <= 0:
        return
    suppression_payload = {
        "suppressed_as_already_global_count": suppressed_count,
        "generated_for_review_count": int(trace_summary.get("generated_for_review_count") or 0),
        "suppression_source": str(trace_summary.get("suppression_source") or "none"),
        "suppression_examples": list(trace_payload.get("suppression_examples") or []),
    }
    suppression_fingerprint = hashlib.sha1(
        json.dumps(suppression_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    try:
        prior_events = get_events(run_id, bq, project=project, dataset=dataset)
    except Exception:
        prior_events = []
    for prior in reversed(prior_events):
        if str(getattr(prior, "stage", "") or "") != "synonym_proposal_suppression_summary":
            continue
        try:
            prior_payload = json.loads(str(getattr(prior, "payload_json", "") or "{}"))
        except (TypeError, json.JSONDecodeError):
            prior_payload = {}
        if str(prior_payload.get("suppression_fingerprint") or "") == suppression_fingerprint:
            return
        break
    append_event(
        RunEvent(
            run_id=run_id,
            event_id=str(uuid.uuid4()),
            stage="synonym_proposal_suppression_summary",
            level="info",
            message=(
                "Suppressed synonym proposals already covered by global map: "
                f"{suppressed_count}"
            ),
            created_at=datetime.datetime.now(datetime.timezone.utc),
            payload_json=json.dumps(
                {**suppression_payload, "suppression_fingerprint": suppression_fingerprint},
                ensure_ascii=False,
            ),
        ),
        bq,
        project=project,
        dataset=dataset,
    )

def _run_synonym_automation_for_payload(
    *,
    run_id: str,
    run_record: Any,
    payload: dict[str, Any],
    run_status: RunStatus,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    mode = _synonym_management_mode_from_run_record(run_record)
    proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
    if not proposals:
        return
    trace_payload = dict(payload.get("synonym_proposals_trace") or {})
    trace_summary = dict(trace_payload.get("trace_summary") or {})
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    triaged_count = 0
    reused_count = 0
    fresh_count = 0
    skipped_count = 0
    failed_count = 0
    fallback_count = 0
    reuse_reason = "reuse_enabled"
    if not bool(mode.get("auto_triage_recommendation_enabled")):
        reuse_reason = "auto_triage_disabled"
    elif not bool(mode.get("triage_recommendation_reuse_enabled")):
        reuse_reason = "reuse_disabled"

    if bool(mode.get("auto_triage_recommendation_enabled")):
        for idx, proposal in enumerate(proposals):
            status = str(proposal.get("proposal_status") or "").strip() or "proposed_unreviewed"
            if status not in {"proposed_unreviewed", "in_review", "deferred"}:
                skipped_count += 1
                continue
            runtime_meta = dict(proposal.get("recommendation_runtime") or {})
            triage_fp = _stable_sha256_json(
                {
                    "proposal_id": str(proposal.get("proposal_id") or "").strip(),
                    "alias": str(proposal.get("alias") or "").strip().lower(),
                    "canonical": str(proposal.get("canonical") or "").strip().lower(),
                    "confidence": round(float(proposal.get("confidence") or 0.0), 6),
                    "candidate_canonicals": sorted(
                        {
                            str(item).strip().lower()
                            for item in list(proposal.get("candidate_canonicals") or [])
                            if str(item).strip()
                        }
                    ),
                    "provider": "fitcv_builtin",
                    "model": "synonym_triage_v1",
                    "wire_api": "builtin",
                }
            )
            reuse_enabled = bool(mode.get("triage_recommendation_reuse_enabled"))
            if reuse_enabled and str(runtime_meta.get("triage_fingerprint") or "").strip() == triage_fp:
                reused_count += 1
                triaged_count += 1
                continue
            try:
                recommendation = _triage_synonym_proposal_recommendation_builtin(proposal, now_iso=now_iso)
            except Exception:
                failed_count += 1
                fallback_count += 1
                continue
            recommendation_runtime = dict(recommendation.get("recommendation_runtime") or {})
            recommendation_runtime["triage_fingerprint"] = triage_fp
            updated = dict(proposal)
            updated.update(
                {
                    "recommended_action": str(recommendation.get("recommended_action") or "").strip() or None,
                    "recommendation_confidence": round(float(recommendation.get("recommendation_confidence") or 0.0), 3),
                    "recommendation_rationale": str(recommendation.get("recommendation_rationale") or "").strip() or None,
                    "recommendation_risk_flags": [
                        str(flag).strip()
                        for flag in list(recommendation.get("recommendation_risk_flags") or [])
                        if str(flag).strip()
                    ],
                    "recommendation_runtime": recommendation_runtime,
                }
            )
            proposals[idx] = updated
            fresh_count += 1
            triaged_count += 1

        append_event(
            RunEvent(
                run_id=run_id,
                event_id=str(uuid.uuid4()),
                stage="synonym_proposal_triage_completed",
                level="info",
                message=(
                    "Synonym triage refresh completed: "
                    f"triaged={triaged_count}, reused={reused_count}, "
                    f"fallback={fallback_count}, skipped={skipped_count}, failed={failed_count}"
                ),
                created_at=datetime.datetime.now(datetime.timezone.utc),
                payload_json=json.dumps(
                    {
                        "triaged_count": triaged_count,
                        "reused_count": reused_count,
                        "fresh_count": fresh_count,
                        "fallback_count": fallback_count,
                        "skipped_count": skipped_count,
                        "failed_count": failed_count,
                        "reuse_reason": reuse_reason,
                        "auto_triage_recommendation_enabled": bool(mode.get("auto_triage_recommendation_enabled")),
                        "triage_recommendation_reuse_enabled": bool(mode.get("triage_recommendation_reuse_enabled")),
                        "provider": "fitcv_builtin",
                        "model": "synonym_triage_v1",
                        "wire_api": "builtin",
                    },
                    ensure_ascii=False,
                ),
            ),
            bq,
            project=project,
            dataset=dataset,
        )

    auto_apply_counts = {"applied": 0, "skipped": 0, "failed": 0, "reason_counts": {}}
    if bool(mode.get("auto_apply_recommendation_enabled")) and bool(mode.get("apply_to_run_enabled")):
        action_map = {"approve": "approve_for_run_overlay", "reject": "reject", "defer": "defer"}
        for idx, proposal in enumerate(proposals):
            status = str(proposal.get("proposal_status") or "").strip() or "proposed_unreviewed"
            if status not in {"proposed_unreviewed", "in_review", "deferred"}:
                auto_apply_counts["skipped"] += 1
                auto_apply_counts["reason_counts"]["not_pending"] = int(auto_apply_counts["reason_counts"].get("not_pending", 0)) + 1
                continue
            recommendation = str(proposal.get("recommended_action") or "").strip().lower()
            action = action_map.get(recommendation)
            if not action:
                auto_apply_counts["skipped"] += 1
                auto_apply_counts["reason_counts"]["missing_recommendation"] = int(auto_apply_counts["reason_counts"].get("missing_recommendation", 0)) + 1
                continue
            next_status = _transition_synonym_proposal_status(status, action)
            if not next_status:
                auto_apply_counts["failed"] += 1
                auto_apply_counts["reason_counts"]["invalid_transition"] = int(auto_apply_counts["reason_counts"].get("invalid_transition", 0)) + 1
                continue
            updated = dict(proposal)
            history = [item for item in list(updated.get("review_history") or []) if isinstance(item, dict)]
            history.append(
                {
                    "action": action,
                    "from_status": status,
                    "to_status": next_status,
                    "acted_by": "system",
                    "acted_at": now_iso,
                    "note": "auto:run-execution",
                }
            )
            updated["proposal_status"] = next_status
            updated["review_history"] = history
            proposals[idx] = updated
            auto_apply_counts["applied"] += 1
        if int(auto_apply_counts.get("applied") or 0) > 0:
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="synonym_proposal_auto_apply_completed",
                    level="info",
                    message=(
                        "Synonym auto-apply completed: "
                        f"applied={auto_apply_counts['applied']}, "
                        f"skipped={auto_apply_counts['skipped']}, "
                        f"failed={auto_apply_counts['failed']}"
                    ),
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                    payload_json=json.dumps(
                        {
                            "applied_count": int(auto_apply_counts.get("applied") or 0),
                            "skipped_count": int(auto_apply_counts.get("skipped") or 0),
                            "failed_count": int(auto_apply_counts.get("failed") or 0),
                            "reason_counts": dict(auto_apply_counts.get("reason_counts") or {}),
                            "acted_by": "system",
                            "note": "auto:run-execution",
                        },
                        ensure_ascii=False,
                    ),
                ),
                bq,
                project=project,
                dataset=dataset,
            )

    promote_counts = {
        "applied": 0,
        "skipped": 0,
        "failed": 0,
        "new_aliases": 0,
        "unchanged_aliases": 0,
        "overridden_aliases": 0,
    }
    promote_skip_reason = "disabled"
    if bool(mode.get("auto_promote_global_enabled")) and bool(mode.get("promote_global_enabled")):
        if run_status != RunStatus.SUCCEEDED:
            promote_skip_reason = "validation_not_eligible"
        else:
            approved = [
                item for item in proposals
                if str(item.get("proposal_status") or "").strip() == "approved_for_run_overlay"
            ]
            if not approved:
                promote_skip_reason = "no_approved_proposals"
            else:
                global_map = _load_global_skill_synonyms_map()
                alias_to_canonicals: dict[str, set[str]] = {}
                for item in approved:
                    alias = str(item.get("alias") or "").strip().lower()
                    canonical = str(item.get("canonical") or "").strip().lower()
                    if alias and canonical:
                        alias_to_canonicals.setdefault(alias, set()).add(canonical)
                conflict_aliases = {k for k, v in alias_to_canonicals.items() if len(v) > 1}
                if conflict_aliases:
                    promote_skip_reason = "conflicts_present"
                    promote_counts["failed"] = len(conflict_aliases)
                else:
                    updated_ids: list[str] = []
                    for idx, item in enumerate(proposals):
                        if str(item.get("proposal_status") or "").strip() != "approved_for_run_overlay":
                            continue
                        alias = str(item.get("alias") or "").strip().lower()
                        canonical = str(item.get("canonical") or "").strip().lower()
                        if not alias or not canonical:
                            promote_counts["skipped"] += 1
                            continue
                        current = str(global_map.get(alias) or "").strip().lower()
                        if not current:
                            promote_counts["new_aliases"] += 1
                        elif current == canonical:
                            promote_counts["unchanged_aliases"] += 1
                            promote_counts["skipped"] += 1
                            continue
                        else:
                            promote_counts["overridden_aliases"] += 1
                        global_map[alias] = canonical
                        promote_counts["applied"] += 1
                        proposal_id = str(item.get("proposal_id") or "").strip()
                        if proposal_id:
                            updated_ids.append(proposal_id)
                        history = [entry for entry in list(item.get("global_promotion_history") or []) if isinstance(entry, dict)]
                        history.append(
                            {
                                "action": "promote_to_global",
                                "acted_by": "system",
                                "acted_at": now_iso,
                                "note": "auto:run-execution",
                                "run_id": run_id,
                            }
                        )
                        updated = dict(item)
                        updated["global_promotion_history"] = history
                        proposals[idx] = updated
                    _persist_global_skill_synonyms_map(global_map)
                    if promote_counts["applied"] > 0:
                        append_event(
                            RunEvent(
                                run_id=run_id,
                                event_id=str(uuid.uuid4()),
                                stage="synonym_proposal_promoted_global",
                                level="info",
                                message=f"Promoted {promote_counts['applied']} synonym proposal mapping(s) to global policy",
                                created_at=datetime.datetime.now(datetime.timezone.utc),
                                payload_json=json.dumps(
                                    {
                                        "applied_count": promote_counts["applied"],
                                        "skipped_count": promote_counts["skipped"],
                                        "new_aliases_count": promote_counts["new_aliases"],
                                        "unchanged_aliases_count": promote_counts["unchanged_aliases"],
                                        "overridden_aliases_count": promote_counts["overridden_aliases"],
                                        "acted_by": "system",
                                        "note": "auto:run-execution",
                                    },
                                    ensure_ascii=False,
                                ),
                            ),
                            bq,
                            project=project,
                            dataset=dataset,
                        )
                    promote_skip_reason = "applied"
                    if int(auto_apply_counts.get("applied") or 0) > 0:
                        effective = json.loads(getattr(run_record, "effective_settings_json", "{}") or "{}")
                        if not isinstance(effective, dict):
                            effective = {}
                        overlay = {
                            str(item.get("alias") or "").strip().lower(): str(item.get("canonical") or "").strip().lower()
                            for item in proposals
                            if str(item.get("proposal_status") or "").strip() == "approved_for_run_overlay"
                            and str(item.get("alias") or "").strip()
                            and str(item.get("canonical") or "").strip()
                        }
                        if overlay:
                            overlay_yaml = _build_synonym_overlay_yaml(overlay)
                            updated_cfg = apply_runtime_skill_synonym_overlay(
                                effective,
                                overlay,
                                source="proposal_review",
                                filename="approved-synonym-proposals.yaml",
                                uploaded_at=now_iso,
                                raw_yaml=overlay_yaml,
                            )
                            update_run_effective_settings(
                                run_id,
                                json.dumps(updated_cfg, ensure_ascii=False),
                                bq,
                                project=project,
                                dataset=dataset,
                            )
    trace_summary["triage_recommendation_generated_total"] = int(triaged_count)
    trace_summary["triage_recommendation_reused_total"] = int(reused_count)
    trace_summary["triage_recommendation_fresh_total"] = int(fresh_count)
    trace_summary["triage_recommendation_suppressed_total"] = 0
    trace_summary["triage_recommendation_reuse_reason"] = reuse_reason
    trace_summary["triage_recommendation_fingerprint"] = _stable_sha256_json(
        {"provider": "fitcv_builtin", "model": "synonym_triage_v1", "wire_api": "builtin"}
    )
    trace_summary["auto_apply_recommendation_applied"] = int(auto_apply_counts.get("applied") or 0)
    trace_summary["auto_apply_recommendation_skipped"] = int(auto_apply_counts.get("skipped") or 0)
    trace_summary["auto_apply_recommendation_failed"] = int(auto_apply_counts.get("failed") or 0)
    trace_summary["auto_apply_recommendation_reason_counts"] = dict(auto_apply_counts.get("reason_counts") or {})
    trace_summary["auto_promote_global_applied"] = int(promote_counts.get("applied") or 0)
    trace_summary["auto_promote_global_skipped"] = int(promote_counts.get("skipped") or 0)
    trace_summary["auto_promote_global_failed"] = int(promote_counts.get("failed") or 0)
    trace_summary["auto_promote_global_skip_reason"] = promote_skip_reason

    trace_payload["trace_summary"] = trace_summary
    payload["proposals"] = proposals
    payload["synonym_proposals_trace"] = trace_payload


def _persist_shared_progress_snapshot(
    *,
    run_id: str,
    run_record: Any,
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
            run_status=run_status,
            degradation_reason="partial_snapshot",
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
        if _synonym_propose_enabled_from_run_record(run_record):
            synonym_payload_json = _build_synonym_proposals_payload(
                run_id=run_id,
                summary=summary,
                created_at=snapshot_at,
                existing_payload_json=getattr(run_record, "synonym_proposals_json", None),
                global_synonyms=_effective_skill_synonyms_from_run_record(run_record),
            )
            synonym_status = update_run_synonym_proposals(
                run_id,
                synonym_payload_json,
                bq,
                project=project,
                dataset=dataset,
            )
            _append_synonym_suppression_summary_event(
                run_id=run_id,
                synonym_payload_json=synonym_payload_json,
                bq=bq,
                project=project,
                dataset=dataset,
            )
            _append_degraded_snapshot_persistence_warning(
                run_id=run_id,
                snapshot_name="synonym_proposals",
                persistence_status=synonym_status,
                bq=bq,
                project=project,
                dataset=dataset,
            )


def execute_pipeline_run(run_id: str, jobs_path: str, config_path: str) -> None:
    runtime = resolve_backend_runtime()
    previous_backend_env = os.environ.get("FITCV_CP_DATA_BACKEND")
    previous_sqlite_path_env = os.environ.get("FITCV_CP_SQLITE_PATH")
    os.environ["FITCV_CP_DATA_BACKEND"] = str(runtime.backend_type)
    if runtime.backend_type == "sqlite":
        os.environ["FITCV_CP_SQLITE_PATH"] = str(runtime.sqlite_path)
    elif "FITCV_CP_SQLITE_PATH" in os.environ:
        del os.environ["FITCV_CP_SQLITE_PATH"]
    project = runtime.project
    dataset = runtime.dataset
    bq = _get_bq() if runtime.backend_type == "bigquery" else None

    # Import here to avoid circular deps at module load time
    from fitcv_cp.reporter import PipelineReporter

    summary: dict[str, Any] = {}
    run_record: Any | None = None

    with observe_span(
        "fitcv.worker_job",
        attributes={
            "run_id": run_id,
            "backend_type": str(runtime.backend_type),
            **build_langfuse_trace_attributes(
                trace_name="fitcv.worker_job",
                session_id=run_id,
                input_payload={
                    "run_id": run_id,
                    "jobs_path": jobs_path,
                    "config_path": config_path,
                    "backend_type": str(runtime.backend_type),
                },
                metadata={
                    "scope": "control_plane_worker",
                    "backend_type": str(runtime.backend_type),
                    "project": str(project or ""),
                    "dataset": str(dataset or ""),
                },
            ),
        },
    ):
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
            with observe_span(
                "run.resolve_context",
                attributes={
                    "run_id": run_id,
                    "backend_type": str(runtime.backend_type),
                },
            ):
                run_record = get_run(run_id, bq, project=project, dataset=dataset)
                effective_config: dict[str, Any] | None = None
                if run_record and run_record.effective_settings_json:
                    try:
                        effective_config = json.loads(run_record.effective_settings_json)
                    except Exception as exc:
                        logger.warning("[run_id=%s] Failed to parse effective_settings_json: %s", run_id, exc)
                effective_config = _normalize_runtime_service_account_key(effective_config, run_id=run_id)
                replay_context = _resolve_run_replay_context(
                    effective_config=effective_config,
                    run_id=run_id,
                )
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

                set_span_attributes(
                    {
                        "run_mode": run_mode,
                        "next_stage": next_stage,
                        "has_effective_config": effective_config is not None,
                        "has_replay_context": replay_context is not None,
                        "has_checkpoint_payload": checkpoint_payload is not None,
                        **build_langfuse_trace_attributes(
                            session_id=run_id,
                            user_id=str(getattr(run_record, "triggered_by", "") or "").strip() or None,
                            input_payload={
                                "run_id": run_id,
                                "jobs_path": str(getattr(run_record, "jobs_path", "") or jobs_path or ""),
                                "config_path": str(getattr(run_record, "config_path", "") or config_path or ""),
                                "run_mode": run_mode,
                                "next_stage": next_stage if run_mode == "manual_staged" else None,
                                "jobs_input_source": getattr(run_record, "jobs_input_source", None),
                                "candidate_profile_source": getattr(run_record, "candidate_profile_source", None),
                            },
                            metadata={
                                "scope": "control_plane_worker",
                                "backend_type": str(runtime.backend_type),
                                "trigger_source": getattr(run_record, "trigger_source", None),
                                "run_mode": run_mode,
                                "next_stage": next_stage,
                                "has_effective_config": effective_config is not None,
                                "has_replay_context": replay_context is not None,
                                "has_checkpoint_payload": checkpoint_payload is not None,
                                "jobs_input_source": getattr(run_record, "jobs_input_source", None),
                                "candidate_profile_source": getattr(run_record, "candidate_profile_source", None),
                            },
                            extra_attributes={
                                "langfuse.user.id": str(getattr(run_record, "triggered_by", "") or "").strip() or None,
                            },
                        ),
                    }
                )

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
                set_span_attributes({"run_terminal_status": str(RunStatus.CANCELLED)})
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
                        run_record=run_record,
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

            with observe_span(
                "run.execute_pipeline",
                attributes={
                    "run_id": run_id,
                    "run_mode": run_mode,
                    "start_stage": next_stage if run_mode == "manual_staged" else None,
                    "stop_after_stage": next_stage if run_mode == "manual_staged" else None,
                },
            ):
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
                        replay_context=replay_context,
                    ),
                )
                try:
                    update_run_stage_transition_artifacts(
                        run_id,
                        _build_stage_transition_artifacts_payload(
                            run_id=run_id,
                            summary=summary,
                            finished_at=checkpoint_time,
                            run_status=RunStatus.AWAITING_CONTINUE,
                            degradation_reason="checkpoint_partial_snapshot",
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
                    if _synonym_propose_enabled_from_run_record(run_record):
                        try:
                            synonym_payload_json = _build_synonym_proposals_payload(
                                run_id=run_id,
                                summary=summary,
                                created_at=checkpoint_time,
                                existing_payload_json=getattr(run_record, "synonym_proposals_json", None),
                                global_synonyms=_effective_skill_synonyms_from_run_record(run_record),
                            )
                            synonym_payload = json.loads(synonym_payload_json)
                            if isinstance(synonym_payload, dict):
                                _run_synonym_automation_for_payload(
                                    run_id=run_id,
                                    run_record=run_record,
                                    payload=synonym_payload,
                                    run_status=RunStatus.AWAITING_CONTINUE,
                                    bq=bq,
                                    project=project,
                                    dataset=dataset,
                                )
                                synonym_payload_json = json.dumps(synonym_payload, ensure_ascii=False)
                            synonym_status = update_run_synonym_proposals(
                                run_id,
                                synonym_payload_json,
                                bq,
                                project=project,
                                dataset=dataset,
                            )
                            _append_synonym_suppression_summary_event(
                                run_id=run_id,
                                synonym_payload_json=synonym_payload_json,
                                bq=bq,
                                project=project,
                                dataset=dataset,
                            )
                            _append_degraded_snapshot_persistence_warning(
                                run_id=run_id,
                                snapshot_name="synonym_proposals",
                                persistence_status=synonym_status,
                                bq=bq,
                                project=project,
                                dataset=dataset,
                            )
                        except Exception as exc:
                            logger.warning(
                                "[run_id=%s] Failed to persist synonym proposals snapshot at checkpoint: %s",
                                run_id,
                                exc,
                            )
                            try:
                                append_event(
                                    _snapshot_persist_failed_event(run_id, "synonym_proposals", str(exc)),
                                    bq,
                                    project=project,
                                    dataset=dataset,
                                )
                            except Exception as inner:
                                logger.warning(
                                    "[run_id=%s] Failed to append synonym proposals persistence warning event: %s",
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
                set_span_attributes({
                    "run_terminal_status": str(RunStatus.AWAITING_CONTINUE),
                    "paused_after_stage": paused_after_stage,
                })
                return

            # ── Step 5: Terminalize or park for review ───────────────────────────
            with observe_span(
                "run.finalize_status",
                attributes={
                    "run_id": run_id,
                    "run_mode": run_mode,
                },
            ):
                cv_debug_records = [
                    item for item in list(summary.get("cv_generation_debug_records") or [])
                    if isinstance(item, dict)
                ]
                auto_accept_enabled = _auto_accept_ai_action_enabled_from_run_record(run_record)
                auto_accepted_count = 0
                pending_review_required = 0
                review_reason_counts: dict[str, int] = {}
                for record in cv_debug_records:
                    if str(record.get("status") or "").strip() != "review_required":
                        continue
                    reason_code = _map_review_required_reason_code(record)
                    review_reason_counts[reason_code] = int(review_reason_counts.get(reason_code, 0)) + 1
                    if run_mode == "run_all" and auto_accept_enabled and reason_code in LOW_RISK_AUTO_ACCEPT_REASON_CODES:
                        auto_accepted_count += 1
                        continue
                    pending_review_required += 1
                summary["review_required_total"] = int(sum(review_reason_counts.values()))
                summary["review_required_auto_accepted"] = int(auto_accepted_count)
                summary["review_required_remaining"] = int(pending_review_required)
                summary["review_required_reason_counts"] = dict(review_reason_counts)
                finished_at = datetime.datetime.now(datetime.timezone.utc) if pending_review_required == 0 else None
                terminal_status = RunStatus.SUCCEEDED if pending_review_required == 0 else RunStatus.AWAITING_CONTINUE
                set_span_attributes(
                    {
                        "review_required_total": int(sum(review_reason_counts.values())),
                        "review_required_remaining": int(pending_review_required),
                        "run_terminal_status": str(terminal_status),
                    }
                )
                update_run_status(
                    run_id,
                    terminal_status,
                    bq,
                    project=project,
                    dataset=dataset,
                    finished_at=finished_at,
                    summary=summary,
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
                if pending_review_required > 0:
                    update_run_checkpoint(
                        run_id,
                        bq,
                        project=project,
                        dataset=dataset,
                        checkpoint_status="awaiting_review",
                        next_stage=None,
                        last_completed_stage="cv_generation",
                        completed_stages=completed_stages,
                        checkpoint_payload_json=None,
                    )
                    append_event(
                        RunEvent(
                            run_id=run_id,
                            event_id=str(uuid.uuid4()),
                            stage="cv_review_required",
                            level="warning",
                            message=(
                                f"Run paused: {pending_review_required} review-required CV item(s) pending operator action. "
                                f"Auto-accepted={auto_accepted_count}."
                            ),
                            created_at=datetime.datetime.now(datetime.timezone.utc),
                            payload_json=json.dumps(
                                {
                                    "review_required_total": int(sum(review_reason_counts.values())),
                                    "auto_accepted": int(auto_accepted_count),
                                    "remaining": int(pending_review_required),
                                    "reason_counts": dict(review_reason_counts),
                                },
                                ensure_ascii=False,
                            ),
                        ),
                        bq,
                        project=project,
                        dataset=dataset,
                    )
                elif run_mode == "manual_staged":
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
                                effective_config=effective_config,
                                summary=summary,
                                export_results=export_results,
                                finished_at=finished_at,
                                replay_context=replay_context,
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
                            finished_at=finished_at or datetime.datetime.now(datetime.timezone.utc),
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
                            run_status=RunStatus.SUCCEEDED,
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
                            replay_context=replay_context,
                        ),
                        bq,
                        project=project,
                        dataset=dataset,
                    )
                except Exception as exc:
                    logger.warning("[run_id=%s] Failed to persist settings-used snapshot: %s", run_id, exc)
                if _summary_has_reached_stage(summary, "enrich"):
                    snapshot_created_at = finished_at or datetime.datetime.now(datetime.timezone.utc)
                    try:
                        update_run_mapping_suggestions(
                            run_id,
                            _build_mapping_suggestions_payload(
                                run_id=run_id,
                                summary=summary,
                                created_at=snapshot_created_at,
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
                    if _synonym_propose_enabled_from_run_record(run_record):
                        try:
                            synonym_payload_json = _build_synonym_proposals_payload(
                                run_id=run_id,
                                summary=summary,
                                created_at=snapshot_created_at,
                                existing_payload_json=getattr(run_record, "synonym_proposals_json", None),
                                global_synonyms=_effective_skill_synonyms_from_run_record(run_record),
                            )
                            synonym_payload = json.loads(synonym_payload_json)
                            if isinstance(synonym_payload, dict):
                                _run_synonym_automation_for_payload(
                                    run_id=run_id,
                                    run_record=run_record,
                                    payload=synonym_payload,
                                    run_status=terminal_status,
                                    bq=bq,
                                    project=project,
                                    dataset=dataset,
                                )
                                synonym_payload_json = json.dumps(synonym_payload, ensure_ascii=False)
                            synonym_status = update_run_synonym_proposals(
                                run_id,
                                synonym_payload_json,
                                bq,
                                project=project,
                                dataset=dataset,
                            )
                            _append_synonym_suppression_summary_event(
                                run_id=run_id,
                                synonym_payload_json=synonym_payload_json,
                                bq=bq,
                                project=project,
                                dataset=dataset,
                            )
                            _append_degraded_snapshot_persistence_warning(
                                run_id=run_id,
                                snapshot_name="synonym_proposals",
                                persistence_status=synonym_status,
                                bq=bq,
                                project=project,
                                dataset=dataset,
                            )
                        except Exception as exc:
                            logger.warning("[run_id=%s] Failed to persist synonym proposals snapshot: %s", run_id, exc)
                            try:
                                append_event(
                                    _snapshot_persist_failed_event(run_id, "synonym_proposals", str(exc)),
                                    bq,
                                    project=project,
                                    dataset=dataset,
                                )
                            except Exception as inner:
                                logger.warning(
                                    "[run_id=%s] Failed to append synonym proposals persistence warning event: %s",
                                    run_id,
                                    inner,
                                )

        except PipelineCancelled as exc:
            # ── Step 5 (alt): Pipeline was cancelled at a checkpoint ──────────────
            logger.info("[run_id=%s] Pipeline cancelled at checkpoint: %s", run_id, exc)
            cancelled_at = datetime.datetime.now(datetime.timezone.utc)
            set_span_attributes({"run_terminal_status": str(RunStatus.CANCELLED)})
            update_run_status(
                run_id, RunStatus.CANCELLED, bq, project=project, dataset=dataset,
                finished_at=cancelled_at,
                summary=summary if isinstance(summary, dict) else None,
            )
            if isinstance(summary, dict) and summary:
                try:
                    _persist_shared_progress_snapshot(
                        run_id=run_id,
                        run_record=run_record,
                        summary=summary,
                        snapshot_at=cancelled_at,
                        bq=bq,
                        project=project,
                        dataset=dataset,
                        run_status=RunStatus.CANCELLED,
                    )
                except Exception as persist_exc:
                    logger.warning(
                        "[run_id=%s] Failed to persist partial progress snapshot for cancelled run: %s",
                        run_id,
                        persist_exc,
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
            logger.exception("[run_id=%s] Pipeline failed: %s", run_id, exc)
            failed_at = datetime.datetime.now(datetime.timezone.utc)
            set_span_attributes({
                "run_terminal_status": str(RunStatus.FAILED),
                "error.message": str(exc),
            })
            update_run_status(
                run_id, RunStatus.FAILED, bq, project=project, dataset=dataset,
                finished_at=failed_at,
                summary=summary if isinstance(summary, dict) else None,
                error_message=str(exc),
            )
            if isinstance(summary, dict) and summary:
                try:
                    _persist_shared_progress_snapshot(
                        run_id=run_id,
                        run_record=run_record,
                        summary=summary,
                        snapshot_at=failed_at,
                        bq=bq,
                        project=project,
                        dataset=dataset,
                        run_status=RunStatus.FAILED,
                    )
                except Exception as persist_exc:
                    logger.warning(
                        "[run_id=%s] Failed to persist partial progress snapshot for failed run: %s",
                        run_id,
                        persist_exc,
                    )
            try:
                append_event(
                    RunEvent(
                        run_id=run_id,
                        event_id=str(uuid.uuid4()),
                        stage="pipeline_failed",
                        level="error",
                        message=str(exc),
                        created_at=failed_at,
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
            except Exception as inner:
                logger.warning("[run_id=%s] Failed to write failure event: %s", run_id, inner)
        finally:
            if previous_backend_env is None:
                os.environ.pop("FITCV_CP_DATA_BACKEND", None)
            else:
                os.environ["FITCV_CP_DATA_BACKEND"] = previous_backend_env
            if previous_sqlite_path_env is None:
                os.environ.pop("FITCV_CP_SQLITE_PATH", None)
            else:
                os.environ["FITCV_CP_SQLITE_PATH"] = previous_sqlite_path_env

