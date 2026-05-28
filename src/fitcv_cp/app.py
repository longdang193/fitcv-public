"""@meta
name: app
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.app.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import dataclasses
import datetime
import hashlib
import html
import io
import json as _json
import logging
import os
import re
import time
import uuid
import zipfile
from collections import OrderedDict
from pathlib import Path
from typing import Any, Literal, TypedDict
from urllib.parse import unquote, urlencode, urlparse
from zoneinfo import ZoneInfo

import httpx
import yaml
from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, field_validator

from fitcv.config import (
    apply_cv_compatibility_projection,
    apply_runtime_skill_synonym_overlay,
    apply_runtime_synonym_overlay,
    load_config,
    load_control_plane_config,
    parse_runtime_synonym_overlay_yaml,
    parse_skill_synonym_overlay_yaml,
    resolve_langgraph_runtime_expectation,
)
from fitcv.contracts import (
    MAPPING_SUGGESTIONS_AGGREGATE_SCHEMA_VERSION,
    STAGE_TRANSITION_ARTIFACTS_STAGE_SCHEMA_VERSION,
    SYNONYM_PROPOSALS_QUEUE_SCHEMA_VERSION,
)
from fitcv.prompts import render_prompt
from fitcv.pipeline import (
    _infer_last_completed_stage_from_state,
    _restore_pipeline_state,
    next_pipeline_stage,
)
from fitcv.tracker import create_cv_version_record
import fitcv_cp.bq_store as bq_store_module
from fitcv_cp.models import PipelineRun, RunEvent, RunStatus
from fitcv_cp.orchestrator import RunSubmission, get_orchestration_adapter
from fitcv_cp.queue import (
    enqueue_cv_regenerate_once_with_job_id,
)
from fitcv_cp.run_artifact_contracts import (
    decode_json_object_or_none,
    pretty_json_string,
    pretty_json_string_or_fallback,
    run_mode_label,
)
from fitcv_cp.settings_schema import (
    AGENTIC_SETTINGS_SECTIONS,
    ALL_GROUP_REGISTRIES,
    CV_GROUPS,
    DECISION_STATUS_ADVANCED,
    DECISION_STATUS_CONFIGURED,
    DECISION_STATUS_NEEDS_REVIEW,
    DECISION_STATUS_RECOMMENDED,
    RANKING_GROUPS,
    SETTINGS_SCHEMA,
    SETTINGS_SECTIONS,
    ValidationError,
    apply_settings_to_config,
    coerce_value,
    decision_status_sort_key,
    derive_settings_decision_state,
    danger_zone_settings_keys,
    editable_settings_keys,
    hidden_deprecated_settings_keys,
    metadata_only_settings_keys,
    settings_ia_contract_for_key,
    settings_ia_metadata_by_key,
    settings_keys_for_control_surface,
    settings_keys_for_domain,
    settings_keys_for_stage,
    settings_keys_for_workflow_stage,
    settings_schema_with_runtime_defaults,
    validate_settings,
)

# Back-compat module-level store symbols (tests + patch surfaces).
insert_run = bq_store_module.insert_run
update_run_queue_job_id = bq_store_module.update_run_queue_job_id
update_run_orchestration_binding = bq_store_module.update_run_orchestration_binding
from fitcv_cp.settings_store import (
    bookmark_key_for_job,
    delete_bookmarked_job,
    is_job_bookmarked,
    list_bookmarked_jobs,
    load_active_settings,
    set_bookmarked_job_status,
    save_setting,
    save_settings_group,
    upsert_bookmarked_job,
)
from fitcv_cp.synonym_proposals import (
    apply_synonym_management_defaults,
    build_synonym_proposals_payload,
    build_synonym_triage_fingerprint,
    evaluate_synonym_triage_reuse,
    resolve_synonym_management_mode,
    transition_synonym_proposal_status,
)
from fitcv_cp.data_plane import data_plane_contract_payload
from fitcv_cp.observability import emit_observability_event
from fitcv_cp.store import ControlPlaneStore
from fitcv_cp.review_identity import ensure_review_item_id
TEMPLATES_DIR = Path(__file__).parent / "templates"
ORCHESTRATION_ADAPTER = get_orchestration_adapter()
_RUN_SUBMISSION_CACHE_TTL_SECS = float(os.environ.get("FITCV_CP_SUBMISSION_CACHE_TTL_SECS") or 15 * 60)
_RUN_SUBMISSION_CACHE_MAX = int(os.environ.get("FITCV_CP_SUBMISSION_CACHE_MAX") or 256)
_RUN_SUBMISSION_CACHE: "OrderedDict[str, tuple[float, RunSubmission]]" = OrderedDict()


def _prune_run_submission_cache(now: float) -> None:
    ttl = max(0.0, float(_RUN_SUBMISSION_CACHE_TTL_SECS))
    while _RUN_SUBMISSION_CACHE:
        oldest_key = next(iter(_RUN_SUBMISSION_CACHE))
        cached_at, _submission = _RUN_SUBMISSION_CACHE[oldest_key]
        if ttl <= 0.0 or now - float(cached_at) <= ttl:
            break
        _RUN_SUBMISSION_CACHE.popitem(last=False)
    max_size = max(0, int(_RUN_SUBMISSION_CACHE_MAX))
    while max_size and len(_RUN_SUBMISSION_CACHE) > max_size:
        _RUN_SUBMISSION_CACHE.popitem(last=False)


def _cache_run_submission(submission: RunSubmission) -> None:
    now = time.time()
    _RUN_SUBMISSION_CACHE[submission.run_id] = (now, submission)
    _RUN_SUBMISSION_CACHE.move_to_end(submission.run_id)
    _prune_run_submission_cache(now)


def _pop_cached_run_submission(run_id: str) -> RunSubmission | None:
    now = time.time()
    entry = _RUN_SUBMISSION_CACHE.pop(run_id, None)
    if entry is None:
        return None
    cached_at, submission = entry
    ttl = max(0.0, float(_RUN_SUBMISSION_CACHE_TTL_SECS))
    if ttl > 0.0 and now - float(cached_at) > ttl:
        return None
    _prune_run_submission_cache(now)
    return submission
_CP_STORE: ControlPlaneStore | None = None
logger = logging.getLogger(__name__)
GERMANY_TZ = ZoneInfo("Europe/Berlin")


class OutputAvailability(TypedDict):
    generated_count: int
    version_row_count: int
    downloadable_count: int
    state: Literal["available", "not_ready", "none_generated", "mismatch"]


def _count_dict_leaf_differences(baseline: dict[str, Any], effective: dict[str, Any]) -> int:
    """Count leaf-value differences between two nested dict payloads."""
    count = 0
    keys = set(baseline.keys()) | set(effective.keys())
    for key in keys:
        left = baseline.get(key)
        right = effective.get(key)
        if isinstance(left, dict) and isinstance(right, dict):
            count += _count_dict_leaf_differences(left, right)
            continue
        if left != right:
            count += 1
    return count


def _run_detail_visibility_registry() -> dict[str, list[dict[str, str]]]:
    """Visibility contract for run-detail sections."""
    return {
        "core": [
            {"name": "status", "owner": "run_overview"},
            {"name": "run_mode", "owner": "run_overview"},
            {"name": "next_stage", "owner": "run_overview"},
        ],
        "advanced": [
            {"name": "stage_result_policy_trace_summary", "owner": "advanced_diagnostics"},
        ],
        "diagnostic": [
            {"name": "synonym_fingerprints", "owner": "advanced_diagnostics"},
            {"name": "event_delivery_health", "owner": "advanced_diagnostics"},
        ],
    }


def _run_overview_consistency_summary(
    run: PipelineRun,
    *,
    stage_result_summary_rows: list[dict[str, Any]] | None,
    event_delivery_health: dict[str, Any] | None,
) -> dict[str, Any]:
    """Core consistency projection used by run overview contract tests."""
    return {
        "status": run.status.value,
        "stage_count": len(stage_result_summary_rows or []),
        "dead_letter_events": int((event_delivery_health or {}).get("count") or 0),
    }


def _build_output_availability(
    run: PipelineRun,
    cv_versions: list[dict[str, Any]],
) -> OutputAvailability:
    generated_count = int(run.cvs_generated or 0)
    version_row_count = len(cv_versions)
    downloadable_count = sum(
        1
        for row in cv_versions
        if isinstance(row, dict) and str(row.get("version_id") or "").strip()
    )

    if downloadable_count > 0:
        state: Literal["available", "not_ready", "none_generated", "mismatch"] = "available"
    elif run.status != RunStatus.SUCCEEDED:
        state = "not_ready"
    elif generated_count > 0:
        state = "mismatch"
    else:
        state = "none_generated"

    return {
        "generated_count": generated_count,
        "version_row_count": version_row_count,
        "downloadable_count": downloadable_count,
        "state": state,
    }


def _resolve_run_store(bq: Any, *, project: str, dataset: str) -> ControlPlaneStore:
    if _CP_STORE is not None:
        return _CP_STORE
    return ControlPlaneStore(bq=bq, project=project, dataset=dataset)


def get_run(run_id: str, bq: Any, *, project: str, dataset: str) -> PipelineRun | None:
    return _resolve_run_store(bq, project=project, dataset=dataset).get_run(run_id)


def list_runs(
    bq: Any,
    *,
    project: str,
    dataset: str,
    limit: int = 50,
    include_archived: bool = False,
    archived_only: bool = False,
) -> list[PipelineRun]:
    return _resolve_run_store(bq, project=project, dataset=dataset).list_runs(
        limit=limit,
        include_archived=include_archived,
        archived_only=archived_only,
    )


def get_events(run_id: str, bq: Any, *, project: str, dataset: str) -> list[RunEvent]:
    return _resolve_run_store(bq, project=project, dataset=dataset).get_events(run_id)


def update_run_status(run_id: str, status: RunStatus, bq: Any, *, project: str, dataset: str, **kwargs: Any) -> dict[str, str]:
    return dict(
        _resolve_run_store(bq, project=project, dataset=dataset).update_run_status(run_id, status, **kwargs)
    )


def update_run_checkpoint(run_id: str, bq: Any, *, project: str, dataset: str, **kwargs: Any) -> dict[str, str]:
    return dict(_resolve_run_store(bq, project=project, dataset=dataset).update_run_checkpoint(run_id, **kwargs))

def update_run_stage_transition_artifacts(
    run_id: str,
    stage_transition_artifacts_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> dict[str, str]:
    return dict(
        _resolve_run_store(bq, project=project, dataset=dataset).update_run_stage_transition_artifacts(
            run_id, stage_transition_artifacts_json
        )
    )


def request_run_cancel(
    run_id: str,
    requested_by: str,
    target_status: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> bool:
    return bool(
        _resolve_run_store(bq, project=project, dataset=dataset).request_run_cancel(
            run_id, requested_by, target_status
        )
    )


def archive_run(run_id: str, archived_by: str, bq: Any, *, project: str, dataset: str) -> None:
    _resolve_run_store(bq, project=project, dataset=dataset).archive_run(run_id, archived_by)


def unarchive_run(run_id: str, bq: Any, *, project: str, dataset: str) -> None:
    _resolve_run_store(bq, project=project, dataset=dataset).unarchive_run(run_id)


def list_cvs_for_run(run_id: str, bq: Any, *, project: str, dataset: str) -> list[dict[str, Any]]:
    return _resolve_run_store(bq, project=project, dataset=dataset).list_cvs_for_run(run_id)


def get_cv_markdown(version_id: str, bq: Any, *, project: str, dataset: str) -> str | None:
    return _resolve_run_store(bq, project=project, dataset=dataset).get_cv_markdown(version_id)


def list_run_structured_jobs(run_id: str, bq: Any, *, project: str, dataset: str) -> list[dict[str, Any]]:
    return _resolve_run_store(bq, project=project, dataset=dataset).list_run_structured_jobs(run_id)


def list_filter_results_for_run(run_id: str, bq: Any, *, project: str, dataset: str) -> list[dict[str, Any]]:
    return _resolve_run_store(bq, project=project, dataset=dataset).list_filter_results_for_run(run_id)

def get_pipeline_runs_schema_status(bq: Any, *, project: str, dataset: str) -> dict[str, Any]:
    return dict(_resolve_run_store(bq, project=project, dataset=dataset).get_pipeline_runs_schema_status())

def append_event(event: RunEvent, bq: Any, *, project: str, dataset: str) -> dict[str, str]:
    return dict(_resolve_run_store(bq, project=project, dataset=dataset).append_event(event))

def update_run_effective_settings(run_id: str, effective_settings_json: str, bq: Any, *, project: str, dataset: str) -> dict[str, str]:
    return dict(
        _resolve_run_store(bq, project=project, dataset=dataset).update_run_effective_settings(
            run_id, effective_settings_json
        )
    )

def update_run_synonym_proposals(
    run_id: str,
    synonym_proposals_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> dict[str, str]:
    return dict(
        _resolve_run_store(bq, project=project, dataset=dataset).update_run_synonym_proposals(
            run_id, synonym_proposals_json
        )
    )

def update_run_cv_generation_debug(
    run_id: str,
    cv_generation_debug_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> dict[str, str]:
    return dict(
        _resolve_run_store(bq, project=project, dataset=dataset).update_run_cv_generation_debug(
            run_id, cv_generation_debug_json
        )
    )

def update_run_results_export_snapshot(
    run_id: str,
    results_export_json: str,
    bq: Any,
    *,
    project: str,
    dataset: str,
) -> dict[str, str]:
    # ControlPlaneStore has no dedicated results_export mutator yet.
    # Persist via canonical store module path.
    return dict(bq_store_module.update_run_results_export(
        run_id,
        results_export_json,
        bq,
        project=project,
        dataset=dataset,
    ))

def insert_cv_version_row(row: dict[str, Any], bq: Any, *, project: str, dataset: str) -> list[Any]:
    return list(_resolve_run_store(bq, project=project, dataset=dataset).insert_cv_version_row(row))


def _observability_toggles() -> tuple[bool, bool]:
    cfg = load_control_plane_config()
    obs = dict(cfg.get("observability") or {})
    return bool(obs.get("emit_model_routing_diagnostics", False)), bool(
        obs.get("emit_backend_capability_diagnostics", False)
    )


def _persist_run_initial(run: PipelineRun, *, bq: Any, project: str, dataset: str) -> None:
    if _CP_STORE is not None:
        _CP_STORE.insert_run(run)
        return
    insert_run(run, bq, project=project, dataset=dataset)


def _persist_run_orchestration_binding(
    run_id: str,
    *,
    queue_job_id: str,
    orchestration_backend: str,
    orchestration_run_id: str | None,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    if _CP_STORE is not None:
        _CP_STORE.update_run_orchestration_binding(
            run_id,
            queue_job_id=queue_job_id,
            orchestration_backend=orchestration_backend,
            orchestration_run_id=orchestration_run_id,
        )
        return
    update_run_orchestration_binding(
        run_id,
        queue_job_id=queue_job_id,
        orchestration_backend=orchestration_backend,
        orchestration_run_id=orchestration_run_id,
        bq=bq,
        project=project,
        dataset=dataset,
    )


def _persist_run_queue_job_id(
    run_id: str,
    queue_job_id: str,
    *,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    if _CP_STORE is not None:
        _CP_STORE.update_run_queue_job_id(run_id, queue_job_id)
        return
    update_run_queue_job_id(run_id, queue_job_id, bq, project=project, dataset=dataset)


def enqueue_run_with_job_id(
    jobs_path: str,
    config_path: str,
    triggered_by: str,
    redis_url: str = "redis://redis:6379/0",
    run_id: str | None = None,
) -> tuple[str, str]:
    submission = submit_run(
        jobs_path=jobs_path,
        config_path=config_path,
        triggered_by=triggered_by,
        redis_url=redis_url,
        run_id=run_id,
    )
    _cache_run_submission(submission)
    return submission.run_id, submission.queue_job_id

def submit_run(
    *,
    jobs_path: str,
    config_path: str,
    triggered_by: str,
    redis_url: str = "redis://redis:6379/0",
    run_id: str | None = None,
) -> RunSubmission:
    submission = ORCHESTRATION_ADAPTER.submit(
        jobs_path=jobs_path,
        config_path=config_path,
        triggered_by=triggered_by,
        redis_url=redis_url,
        run_id=run_id,
    )
    emit_routing, emit_backend = _observability_toggles()
    if emit_backend:
        emit_observability_event(
            "control_plane.backend_execution",
            {
                "run_id": submission.run_id,
                "trace_id": submission.run_id,
                "stage": "submit_run",
                "task_part": "enqueue_run",
                "backend": submission.backend,
                "backend_run_id": submission.backend_run_id or submission.queue_job_id,
                "queue_job_id": submission.queue_job_id,
            },
        )
    if emit_routing:
        emit_observability_event(
            "control_plane.model_routing",
            {
                "run_id": submission.run_id,
                "trace_id": submission.run_id,
                "stage": "submit_run",
                "task_part": "enqueue_run",
                "provider": "orchestration_adapter",
                "model": str(ORCHESTRATION_ADAPTER.name or "default_queue"),
            },
        )
    return submission


def enqueue_run(
    jobs_path: str,
    config_path: str,
    triggered_by: str,
    redis_url: str = "redis://redis:6379/0",
    run_id: str | None = None,
) -> str:
    return ORCHESTRATION_ADAPTER.enqueue_run(
        jobs_path=jobs_path,
        config_path=config_path,
        triggered_by=triggered_by,
        redis_url=redis_url,
        run_id=run_id,
    )


def cancel_queued_run(queue_job_id: str, redis_url: str = "redis://redis:6379/0") -> bool:
    return ORCHESTRATION_ADAPTER.cancel_queued_run(queue_job_id=queue_job_id, redis_url=redis_url)

def get_queue_job_status(queue_job_id: str, redis_url: str = "redis://redis:6379/0") -> str:
    return ORCHESTRATION_ADAPTER.status(queue_job_id=queue_job_id, redis_url=redis_url)

def continue_run_with_job_id(
    *,
    run_id: str,
    jobs_path: str,
    config_path: str,
    triggered_by: str,
    redis_url: str = "redis://redis:6379/0",
) -> tuple[str, str]:
    submission = continue_run_submission(
        run_id=run_id,
        jobs_path=jobs_path,
        config_path=config_path,
        triggered_by=triggered_by,
        redis_url=redis_url,
    )
    _cache_run_submission(submission)
    return submission.run_id, submission.queue_job_id

def _resolve_submission_binding(run_id: str, queue_job_id: str) -> RunSubmission:
    cached = _pop_cached_run_submission(run_id)
    if cached is not None:
        return cached
    requested_backend = str(ORCHESTRATION_ADAPTER.name or "default_queue")
    execution_backend = "prefect" if requested_backend == "prefect" else "queue"
    submission = RunSubmission(
        run_id=run_id,
        queue_job_id=queue_job_id,
        backend_run_id=queue_job_id,
        requested_backend=requested_backend,
        execution_backend=execution_backend,
    )
    _, emit_backend = _observability_toggles()
    if emit_backend:
        emit_observability_event(
            "control_plane.backend_fallback_binding",
            {
                "run_id": submission.run_id,
                "trace_id": submission.run_id,
                "stage": "resolve_submission_binding",
                "task_part": "binding_fallback",
                "backend": submission.backend,
                "backend_run_id": submission.backend_run_id,
                "queue_job_id": submission.queue_job_id,
            },
        )
    return submission

def continue_run_submission(
    *,
    run_id: str,
    jobs_path: str,
    config_path: str,
    triggered_by: str,
    redis_url: str = "redis://redis:6379/0",
) -> RunSubmission:
    return ORCHESTRATION_ADAPTER.continue_run(
        run_id=run_id,
        jobs_path=jobs_path,
        config_path=config_path,
        triggered_by=triggered_by,
        redis_url=redis_url,
    )

def orchestration_job_status(queue_job_id: str, redis_url: str = "redis://redis:6379/0") -> str:
    return get_queue_job_status(queue_job_id=queue_job_id, redis_url=redis_url)

def _build_orchestration_diagnostics(run: PipelineRun) -> dict[str, Any]:
    backend = str(run.orchestration_backend or "").strip() or str(ORCHESTRATION_ADAPTER.name or "default_queue")
    backend_run_id = str(run.orchestration_run_id or "").strip() or str(run.queue_job_id or "").strip() or None
    status = "not_available"
    status_checked = False
    if backend_run_id and run.status in {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.CANCELLING}:
        status_checked = True
        try:
            status = str(orchestration_job_status(backend_run_id, redis_url=redis_url) or "unknown").strip() or "unknown"
        except Exception:
            status = "unknown"
    return {
        "backend": backend,
        "backend_run_id": backend_run_id,
        "status": status,
        "status_checked": status_checked,
    }
PIPELINE_OUTCOME_META: dict[str, dict[str, str]] = {
    "ranked_with_cv": {
        "label": "CV created",
        "badge_class": "badge-success",
    },
    "ranked_blocked_by_reranker_fit": {
        "label": "Ranked, blocked by reranker fit",
        "badge_class": "badge-warning",
    },
    "ranked_skipped_fit_gate": {
        "label": "Skipped after CV analysis",
        "badge_class": "badge-warning",
    },
    "ranked_no_cv": {
        "label": "Ranked, CV failed",
        "badge_class": "badge-warning",
    },
    "not_shortlisted": {
        "label": "Passed filter, not shortlisted",
        "badge_class": "badge-info",
    },
    "shortlisted_not_scored": {
        "label": "Shortlisted, not AI scored",
        "badge_class": "badge-info",
    },
    "scored_not_ranked": {
        "label": "Scored, not final top-N",
        "badge_class": "badge-info",
    },
    "rejected_after_enrichment": {
        "label": "Rejected after enrichment",
        "badge_class": "badge-error",
    },
    "rejected_before_enrichment": {
        "label": "Rejected before enrichment",
        "badge_class": "badge-error",
    },
    "deduplicated_before_enrichment": {
        "label": "Deduplicated before enrichment",
        "badge_class": "badge-warning",
    },
}
DECISION_CHAIN_LABELS: dict[str, str] = {
    "returned_by_vector_search": "returned by vector search",
    "backfilled_for_scoring": "backfilled for scoring",
    "advanced_to_scoring": "advanced to scoring",
    "not_returned_by_vector_search": "not returned by vector search",
    "accepted": "accepted",
    "ready_for_generation": "ready for CV generation",
    "validation_failed": "validation failed",
    "generation_failed": "generation failed",
    "persistence_failed": "persistence failed",
    "blocked_by_reranker_fit": "blocked by reranker fit",
    "skipped_fit_gate": "skipped after CV analysis",
    "not_attempted": "not attempted",
    "not_run": "not run",
    "failed": "failed",
}
TIMELINE_STAGE_DOWNLOADS: dict[str, str] = {
    "layer1_normalize": "normalize",
    "layer1_jobs": "enrich",
    "layer3_filter": "rule_filter",
    "layer3_shortlist": "shortlist",
    "layer3_ranking": "ranking",
    "layer4_cv_analysis": "cv_analysis",
    "layer4_cv_analysis_skip": "cv_analysis",
    "pipeline_complete": "cv_generation",
    "pipeline_compute_complete": "cv_generation",
    "layer4_cv_skip": "cv_analysis",
    "layer4_cv_validation_failed": "cv_generation",
}
STAGE_DOWNLOAD_LABELS: dict[str, str] = {
    "normalize": "Download Normalize JSON",
    "enrich": "Download Enrich JSON",
    "rule_filter": "Download Rule Filter JSON",
    "shortlist": "Download Shortlist JSON",
    "ranking": "Download Ranking JSON",
    "cv_analysis": "Download CV Analysis JSON",
    "cv_generation": "Download CV Generation JSON",
}
RUN_STATUS_GROUPS = {
    "active": {RunStatus.QUEUED.value, RunStatus.RUNNING.value, RunStatus.CANCELLING.value},
    "terminal": {RunStatus.SUCCEEDED.value, RunStatus.FAILED.value, RunStatus.CANCELLED.value},
    "awaiting_continue": {RunStatus.AWAITING_CONTINUE.value},
}
REPLAY_MODES = {"strict", "policy_replay"}

def _run_status_projection(run: PipelineRun) -> dict[str, Any]:
    status_value = run.status.value
    return {
        "status": status_value,
        "is_active": status_value in RUN_STATUS_GROUPS["active"],
        "is_terminal": status_value in RUN_STATUS_GROUPS["terminal"],
        "is_awaiting_continue": status_value in RUN_STATUS_GROUPS["awaiting_continue"],
        "is_archived": bool(run.archived_at),
    }

def _policy_registry_version_from_config(config_payload: dict[str, Any] | None) -> str:
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
    payload = _json.dumps(envelope, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _checkpoint_replay_context(run: PipelineRun) -> dict[str, Any]:
    payload = _load_json_object(run.checkpoint_payload_json) or {}
    replay = payload.get("replay_context")
    if isinstance(replay, dict):
        return dict(replay)
    return {}

def _run_replay_context_summary(run: PipelineRun) -> dict[str, Any]:
    for raw_payload, source in (
        (run.results_export_json, "results_export"),
        (run.settings_used_json, "settings_used"),
        (run.checkpoint_payload_json, "checkpoint"),
    ):
        payload = _load_json_object(raw_payload)
        if not isinstance(payload, dict):
            continue
        replay_context = payload.get("replay_context")
        if isinstance(replay_context, dict):
            summary = dict(replay_context)
            summary["source"] = source
            return summary
    return {"replay_mode": "strict", "source": "default"}

def _run_data_plane_summary(run: PipelineRun) -> dict[str, Any]:
    for raw_payload in (run.settings_used_json, run.results_export_json):
        payload = _load_json_object(raw_payload)
        if not isinstance(payload, dict):
            continue
        block = payload.get("data_plane")
        if isinstance(block, dict):
            return dict(block)
    effective = _load_run_effective_config_snapshot(run, fallback_to_runtime_config=False)
    return data_plane_contract_payload(effective)

def _resolve_replay_mode(request: Request) -> str:
    candidate = str(request.query_params.get("replay_mode") or "").strip().lower()
    if candidate in REPLAY_MODES:
        return candidate
    return "strict"


def _apply_trigger_runtime_envelope(
    effective_config: dict[str, Any],
    *,
    jobs_input_source: str | None,
    jobs_input_json: str | None,
    jobs_input_manifest_json: str | None,
    candidate_profile_source: str | None,
    candidate_profile_json: str | None,
    run_mode: str,
    reuse_precheck_warning: dict[str, Any] | None = None,
) -> dict[str, Any]:
    synonym_management = dict(effective_config.get("synonym_management") or {})
    synonym_management.setdefault("auto_apply_recommendation_enabled", False)
    synonym_management.setdefault("auto_promote_global_enabled", False)
    synonym_management.setdefault("auto_accept_ai_action_enabled", True)
    effective_config["synonym_management"] = synonym_management
    effective_config = apply_synonym_management_defaults(effective_config)

    runtime_inputs = effective_config.setdefault("runtime_inputs", {})
    if jobs_input_json:
        runtime_inputs["jobs_input_json"] = jobs_input_json
    if jobs_input_manifest_json:
        runtime_inputs["jobs_input_manifest_json"] = jobs_input_manifest_json
    if candidate_profile_json:
        runtime_inputs["candidate_profile_json"] = candidate_profile_json
    # Capture trigger-time agentic runtime expectation to avoid later interpretation drift.
    runtime_inputs["agentic_runtime_expectation"] = resolve_langgraph_runtime_expectation()
    effective_config["trigger_runtime_envelope"] = {
        "jobs_input_source": jobs_input_source,
        "candidate_profile_source": candidate_profile_source,
        "run_mode": run_mode,
        "has_jobs_input_snapshot": bool(jobs_input_json),
        "has_candidate_profile_snapshot": bool(candidate_profile_json),
        "reuse_precheck_warning": dict(reuse_precheck_warning or {}),
    }
    return effective_config

def _stable_synonym_hash(config: dict[str, Any]) -> str:
    synonyms = dict(config.get("skill_synonyms") or {})
    normalized: dict[str, str] = {}
    for raw_alias, raw_canonical in synonyms.items():
        alias = str(raw_alias or "").strip().casefold()
        canonical = str(raw_canonical or "").strip().casefold()
        if not alias or not canonical:
            continue
        normalized[alias] = canonical
    seed = _json.dumps(dict(sorted(normalized.items())), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _resolve_jobs_path_snapshot(jobs_path: str) -> tuple[str, str]:
    normalized_jobs_path = str(jobs_path or "").strip()
    if not normalized_jobs_path:
        raise HTTPException(status_code=422, detail="jobs_path required for path mode")
    path_file = Path(normalized_jobs_path)
    if not path_file.exists():
        raise HTTPException(
            status_code=422,
            detail=(
                f"Jobs file not found: {normalized_jobs_path}. "
                "In Docker use container-visible paths (for example: data/<file>.json), "
                "not host-absolute paths."
            ),
        )
    try:
        raw_text = path_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(status_code=422, detail=f"Cannot read jobs file {normalized_jobs_path}: {exc}")
    try:
        parsed_jobs = _json.loads(raw_text)
    except _json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid jobs JSON at {normalized_jobs_path}: {exc}")
    if not isinstance(parsed_jobs, list):
        raise HTTPException(
            status_code=422,
            detail=f"Invalid jobs JSON at {normalized_jobs_path}: top-level value must be a JSON array",
        )
    return normalized_jobs_path, _json.dumps(parsed_jobs, ensure_ascii=False, indent=2)


def _resolve_default_candidate_profile_snapshot(config_path: str) -> str:
    from fitcv.candidate import load_profile_yaml as _load_profile_yaml, validate_profile as _validate_profile

    base_cfg_for_profile = load_config(config_path)
    profile_path_str = str(base_cfg_for_profile.get("paths", {}).get("candidate_profile", "") or "").strip()
    if not profile_path_str:
        raise HTTPException(status_code=422, detail="No candidate_profile path configured")
    try:
        resolved_profile = _load_profile_yaml(profile_path_str)
    except FileNotFoundError:
        raise HTTPException(
            status_code=422,
            detail=f"Candidate profile not found: {profile_path_str}",
        )
    profile_errors = _validate_profile(resolved_profile)
    if profile_errors:
        raise HTTPException(
            status_code=422,
            detail=f"Candidate profile validation failed: {'; '.join(profile_errors)}",
        )
    return _json.dumps(resolved_profile, ensure_ascii=False, indent=2)

def _resolve_candidate_profile_snapshot_from_text(raw_text: str) -> str:
    from fitcv.candidate import load_profile_text as _load_profile_text

    try:
        resolved_profile = _load_profile_text(raw_text, format_hint="auto")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return _json.dumps(resolved_profile, ensure_ascii=False, indent=2)

def _load_jobs_input_manifest(raw_manifest_json: str | None) -> dict[str, Any]:
    if not raw_manifest_json:
        return {}
    return decode_json_object_or_none(raw_manifest_json) or {}

def _format_jobs_path_display(run: PipelineRun) -> str:
    raw_jobs_path = str(getattr(run, "jobs_path", "") or "").strip()
    manifest = _load_jobs_input_manifest(getattr(run, "jobs_input_manifest_json", None))
    source = str(getattr(run, "jobs_input_source", "") or "").strip().lower()
    if source != "upload":
        return raw_jobs_path
    merged_from = manifest.get("source_filenames")
    if not isinstance(merged_from, list):
        return raw_jobs_path
    names = [str(item).strip() for item in merged_from if str(item).strip()]
    if not names:
        return raw_jobs_path
    return f"{raw_jobs_path} (merged from: {', '.join(names)})"

def _attach_jobs_path_display(run: PipelineRun) -> PipelineRun:
    run.jobs_path_display = _format_jobs_path_display(run)  # type: ignore[attr-defined]
    return run


def _canonical_continue_next_stage(run: PipelineRun) -> str | None:
    def _safe_next_stage(stage_name: str | None) -> str | None:
        normalized_stage = str(stage_name or "").strip()
        if not normalized_stage:
            return None
        try:
            return next_pipeline_stage(normalized_stage)
        except ValueError:
            return None

    checkpoint_next_stage: str | None = None
    checkpoint_payload_json = str(run.checkpoint_payload_json or "").strip()
    if checkpoint_payload_json:
        raw_payload = decode_json_object_or_none(checkpoint_payload_json)
        if raw_payload:
            checkpoint_payload = raw_payload.get("checkpoint_payload")
            if not isinstance(checkpoint_payload, dict):
                checkpoint_payload = raw_payload
            restored_state = _restore_pipeline_state(
                run_id=str(run.run_id or ""),
                checkpoint_payload=checkpoint_payload,
            )
            checkpoint_last_completed_stage = _infer_last_completed_stage_from_state(restored_state)
            checkpoint_next_stage = _safe_next_stage(checkpoint_last_completed_stage)

    last_completed_stage = str(run.last_completed_stage or "").strip()
    if last_completed_stage:
        canonical_next_stage = _safe_next_stage(last_completed_stage)
        if canonical_next_stage:
            if checkpoint_next_stage and checkpoint_next_stage != canonical_next_stage:
                return None
            return canonical_next_stage

    completed_stages = list(run.completed_stages or [])
    if completed_stages:
        canonical_next_stage = _safe_next_stage(str(completed_stages[-1]))
        if canonical_next_stage:
            if checkpoint_next_stage and checkpoint_next_stage != canonical_next_stage:
                return None
            return canonical_next_stage

    return None
STAGE_SEQUENCE: tuple[str, ...] = (
    "normalize",
    "enrich",
    "rule_filter",
    "shortlist",
    "ranking",
    "cv_analysis",
    "cv_generation",
)
TIMELINE_STAGE_LABELS: dict[str, str] = {
    "pipeline_start": "Pipeline",
    "layer1_normalize": "Normalize",
    "layer1b_pre_filter": "Pre-Enrichment Filter",
    "enrich_heartbeat": "Enrich In Progress",
    "layer1_jobs": "Enrich",
    "layer2_candidate": "Candidate Profile",
    "layer3_filter": "Rule Filter",
    "layer3_shortlist": "Shortlist",
    "layer3_ai_score": "Ranking",
    "layer3_ranking": "Ranking",
    "layer4_cv_analysis": "CV Analysis",
    "layer4_cv_analysis_invoked": "CV Analysis",
    "layer4_cv_analysis_skip": "CV Analysis",
    "layer4_cv_skip": "CV Analysis",
    "layer4_cv_generation_invoked": "CV Generation",
    "layer4_cv_generation_reused": "CV Generation",
    "layer4_cv_validation_failed": "CV Generation",
    "pipeline_complete": "CV Generation",
    "pipeline_compute_complete": "CV Generation",
    "layer4_cv_analysis_blocked_details": "CV Analysis Blocked Details",
    "stage_checkpoint": "Checkpoint",
    "manual_continue_requested": "Manual Continue",
    "pipeline_failed": "Pipeline",
    "synonym_overlay_uploaded": "Synonym Overlay",
}
TIMELINE_STAGE_DOWNLOADABLE_EVENTS: set[str] = {
    "layer1_normalize",
    "layer1_jobs",
    "layer3_filter",
    "layer3_shortlist",
    "layer3_ranking",
    "layer4_cv_analysis",
    "layer4_cv_validation_failed",
    "pipeline_complete",
    "pipeline_compute_complete",
}
NEGATIVE_METRIC_LABEL_MARKERS = (
    "Backfill Rate",
    "Skip Rate",
    "Failure Rate",
    "Validation-Fail Rate",
    "Persistence-Fail Rate",
)
POSITIVE_METRIC_LABEL_MARKERS = (
    "Ready Rate",
    "Accepted Rate",
    "Strong Rate",
)
BUNDLE_STAGE_IDS: tuple[str, ...] = (
    "normalize",
    "enrich",
    "rule_filter",
    "shortlist",
    "ranking",
    "cv_analysis",
    "cv_generation",
)
BUNDLE_ARTIFACT_FILENAMES: tuple[str, ...] = (
    "results.json",
    "hitl-review-audit.json",
    "stage-artifacts.json",
    "normalize.json",
    "enrich.json",
    "rule_filter.json",
    "shortlist.json",
    "ranking.json",
    "cv_analysis.json",
    "cv_generation.json",
    "settings-used.json",
    "cv-debug.json",
    "cv-generation-review-required.json",
    "cv-analysis-trace.json",
    "agentic-live-trace.json",
    "mapping-suggestions.json",
    "synonym-proposals.json",
    "synonym-proposals-trace.json",
    "synonym-suppression-diff.json",
    "approved-synonym-proposals.yaml",
    "synonym-overlay-used.yaml",
)


@dataclasses.dataclass(frozen=True)
class RunArtifactFile:
    filename: str
    label: str
    href: str
    content: str
    show_in_exports: bool = True


def _load_stage_transition_artifacts_payload(run: PipelineRun) -> dict[str, Any]:
    payload = decode_json_object_or_none(run.stage_transition_artifacts_json)
    return payload or {}

def _persist_stage_artifacts_terminal_snapshot(
    *,
    run_id: str,
    bq: Any,
    project: str,
    dataset: str,
    terminal_status: RunStatus,
    snapshot_at: datetime.datetime,
    snapshot_complete: bool,
    degradation_reason: str,
) -> None:
    run = get_run(run_id, bq, project=project, dataset=dataset)
    if run is None or not run.stage_transition_artifacts_json:
        return
    payload = _load_stage_transition_artifacts_payload(run)
    if not payload:
        return
    payload["status"] = terminal_status.value
    payload["created_at"] = snapshot_at.isoformat()
    payload["snapshot_complete"] = bool(snapshot_complete)
    payload["degradation_reason"] = str(degradation_reason or "").strip()
    update_run_stage_transition_artifacts(
        run_id,
        _json.dumps(payload, ensure_ascii=False),
        bq,
        project=project,
        dataset=dataset,
    )


def _load_json_object(raw_payload: str | None) -> dict[str, Any] | None:
    return decode_json_object_or_none(raw_payload)


def _stage_artifacts_by_id(run: PipelineRun) -> dict[str, dict[str, Any]]:
    payload = _load_stage_transition_artifacts_payload(run)
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        return {}
    stages = artifacts.get("stages")
    if not isinstance(stages, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for stage_id, stage_payload in stages.items():
        if isinstance(stage_payload, dict):
            result[str(stage_id)] = stage_payload
    return result


def _stage_quality_metrics_from_stage_artifacts(
    stage_artifacts_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    metrics_by_stage: dict[str, Any] = {}
    for stage_id, stage_payload in stage_artifacts_by_id.items():
        decision_summary = dict(stage_payload.get("decision_summary") or {})
        metrics = decision_summary.get("quality_metrics")
        if isinstance(metrics, dict) and metrics:
            metrics_by_stage[stage_id] = metrics
    return metrics_by_stage


def _late_stage_reuse_metrics_from_stage_artifacts(
    stage_artifacts_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    metrics_by_stage: dict[str, Any] = {}
    for stage_id in ("enrich", "ranking", "cv_analysis", "cv_generation"):
        stage_payload = dict(stage_artifacts_by_id.get(stage_id) or {})
        decision_summary = dict(stage_payload.get("decision_summary") or {})
        reuse_metrics = decision_summary.get("reuse_metrics")
        if isinstance(reuse_metrics, dict) and reuse_metrics:
            metrics_by_stage[stage_id] = reuse_metrics
    return metrics_by_stage


def _run_has_stage_artifact(run: PipelineRun, stage_id: str) -> bool:
    return stage_id in _stage_artifacts_by_id(run)


def _run_has_reached_stage(run: PipelineRun, stage_id: str) -> bool:
    normalized_stage_id = str(stage_id or "").strip()
    if not normalized_stage_id:
        return False
    completed_stages = [str(item).strip() for item in list(run.completed_stages or []) if str(item).strip()]
    if normalized_stage_id in completed_stages:
        return True
    last_completed_stage = str(run.last_completed_stage or "").strip()
    if not last_completed_stage:
        return False
    try:
        target_index = STAGE_SEQUENCE.index(normalized_stage_id)
        last_index = STAGE_SEQUENCE.index(last_completed_stage)
    except ValueError:
        return normalized_stage_id == last_completed_stage
    return target_index <= last_index


def _stage_status_by_id(stage_artifacts_by_id: dict[str, Any]) -> dict[str, str]:
    statuses: dict[str, str] = {}
    for stage_id, block in dict(stage_artifacts_by_id or {}).items():
        if not isinstance(block, dict):
            continue
        normalized_stage_id = str(stage_id or "").strip()
        if not normalized_stage_id:
            continue
        statuses[normalized_stage_id] = str(block.get("status") or "").strip().lower()
    return statuses


def _metric_severity(metric: dict[str, Any], stage_statuses: dict[str, str] | None = None) -> tuple[str, str]:
    rate = float(metric.get("rate") or 0.0)
    denominator = int(metric.get("denominator") or 0)
    label = str(metric.get("label") or "")
    category = str(metric.get("category") or "")
    stage_id = str(metric.get("stage_id") or "").strip()
    stage_status = str((stage_statuses or {}).get(stage_id) or "").strip().lower()
    if stage_status in {"", "not_reached"}:
        return "muted", "Pending"
    if denominator <= 0:
        return "info", "N/A"
    if category == "reuse":
        if rate >= 0.5:
            return "success", "Healthy"
        if rate > 0:
            return "info", "Observed"
        return "muted", "Fresh only"
    if any(marker in label for marker in NEGATIVE_METRIC_LABEL_MARKERS):
        if rate >= 0.5:
            return "error", "Needs attention"
        if rate >= 0.2:
            return "warning", "Watch"
        return "success", "Healthy"
    if any(marker in label for marker in POSITIVE_METRIC_LABEL_MARKERS):
        if rate >= 0.75:
            return "success", "Healthy"
        if rate >= 0.4:
            return "warning", "Watch"
        return "error", "Needs attention"
    if "Stretch Rate" in label:
        if rate >= 0.6:
            return "warning", "Stretch-heavy"
        return "info", "Observed"
    return "info", "Observed"


def _stage_quality_metric_row(
    *,
    stage_id: str,
    label: str,
    rate: float | int | None,
    numerator: int | None,
    denominator: int | None,
    hint: str,
) -> dict[str, Any] | None:
    if rate is None or numerator is None or denominator is None:
        return None
    return {
        "stage_id": stage_id,
        "label": label,
        "rate": float(rate),
        "rate_percent": int(round(float(rate) * 100)),
        "numerator": int(numerator),
        "denominator": int(denominator),
        "hint": hint,
    }


def _build_stage_quality_metric_rows(stage_quality_metrics: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    shortlist = dict(stage_quality_metrics.get("shortlist") or {})
    shortlist_row = _stage_quality_metric_row(
        stage_id="shortlist",
        label="Shortlist Backfill Rate",
        rate=shortlist.get("backfill_rate"),
        numerator=shortlist.get("backfilled_jobs_total"),
        denominator=shortlist.get("scoring_shortlisted_jobs_total"),
        hint="High values usually mean shortlist recall is relying on backfill.",
    )
    if shortlist_row:
        rows.append(shortlist_row)

    ranking_distribution = dict((stage_quality_metrics.get("ranking") or {}).get("label_distribution") or {})
    ranking_specs = (
        ("Ranking Strong Rate", "strong_rate", "strong_count", "Strong labels among scored ranking inputs."),
        ("Ranking Stretch Rate", "stretch_rate", "stretch_count", "Stretch labels among scored ranking inputs."),
        ("Ranking Skip Rate", "skip_rate", "skip_count", "Skip labels among scored ranking inputs."),
    )
    for label, rate_key, count_key, hint in ranking_specs:
        row = _stage_quality_metric_row(
            stage_id="ranking",
            label=label,
            rate=ranking_distribution.get(rate_key),
            numerator=ranking_distribution.get(count_key),
            denominator=ranking_distribution.get("total_scored"),
            hint=hint,
        )
        if row:
            rows.append(row)

    cv_analysis = dict(stage_quality_metrics.get("cv_analysis") or {})
    cv_analysis_specs = (
        (
            "CV Analysis Reranker Block Rate",
            "blocked_by_reranker_fit_rate",
            "blocked_by_reranker_fit",
            "Ranked jobs stopped before CV analysis because reranker fit was skip.",
        ),
        ("CV Analysis Skip Rate", "skip_rate", "skipped_fit_gate", "Jobs blocked by the fit gate after ranking."),
        (
            "CV Analysis Ready Rate",
            "ready_for_generation_rate",
            "ready_for_generation",
            "Jobs that are ready to hand off to CV generation.",
        ),
        (
            "CV Analysis Failure Rate",
            "analysis_failed_rate",
            "analysis_failed",
            "Analysis records that failed before generation handoff.",
        ),
    )
    for label, rate_key, count_key, hint in cv_analysis_specs:
        row = _stage_quality_metric_row(
            stage_id="cv_analysis",
            label=label,
            rate=cv_analysis.get(rate_key),
            numerator=cv_analysis.get(count_key),
            denominator=cv_analysis.get("total_processed"),
            hint=hint,
        )
        if row:
            rows.append(row)

    cv_generation = dict(stage_quality_metrics.get("cv_generation") or {})
    cv_generation_specs = (
        (
            "CV Generation Accepted Rate",
            "accepted_rate",
            "accepted",
            "Accepted CV outputs among attempted generation jobs.",
        ),
        (
            "CV Generation Validation-Fail Rate",
            "validation_fail_rate",
            "validation_failed",
            "Generated CVs rejected by validation.",
        ),
        (
            "CV Generation Failure Rate",
            "generation_failed_rate",
            "generation_failed",
            "Runtime failures during CV generation.",
        ),
        (
            "CV Generation Persistence-Fail Rate",
            "persistence_failed_rate",
            "persistence_failed",
            "Generated CVs that failed while saving.",
        ),
    )
    for label, rate_key, count_key, hint in cv_generation_specs:
        row = _stage_quality_metric_row(
            stage_id="cv_generation",
            label=label,
            rate=cv_generation.get(rate_key),
            numerator=cv_generation.get(count_key),
            denominator=cv_generation.get("total_attempted"),
            hint=hint,
        )
        if row:
            rows.append(row)

    return rows


def _build_late_stage_reuse_metric_rows(late_stage_reuse_metrics: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    enrich = dict(late_stage_reuse_metrics.get("enrich") or {})
    enrich_row = _stage_quality_metric_row(
        stage_id="enrich",
        label="Enrich Reuse Rate",
        rate=enrich.get("reuse_rate"),
        numerator=enrich.get("reused_rows"),
        denominator=enrich.get("total_rows"),
        hint="Exact-match enrich rows reused from prior runs with matching fingerprints.",
    )
    if enrich_row:
        enrich_row["fresh_count"] = int(enrich.get("fresh_rows") or 0)
        rows.append(enrich_row)

    ranking = dict(late_stage_reuse_metrics.get("ranking") or {})
    ranking_row = _stage_quality_metric_row(
        stage_id="ranking",
        label="Ranking AI-Score Reuse Rate",
        rate=ranking.get("reuse_rate"),
        numerator=ranking.get("reused_ai_scores"),
        denominator=ranking.get("total_ai_scores"),
        hint="Exact-match AI-score rows reused from previous successful runs.",
    )
    if ranking_row:
        ranking_row["fresh_count"] = int(ranking.get("fresh_ai_scores") or 0)
        rows.append(ranking_row)

    cv_analysis = dict(late_stage_reuse_metrics.get("cv_analysis") or {})
    cv_analysis_row = _stage_quality_metric_row(
        stage_id="cv_analysis",
        label="CV Analysis Reuse Rate",
        rate=cv_analysis.get("analysis_reuse_rate"),
        numerator=cv_analysis.get("reused_analysis_rows"),
        denominator=cv_analysis.get("analysis_rows_executed"),
        hint="Exact-match analysis records reused from previous successful runs.",
    )
    if cv_analysis_row:
        cv_analysis_row["fresh_count"] = int(cv_analysis.get("fresh_analysis_rows") or 0)
        rows.append(cv_analysis_row)

    cv_generation = dict(late_stage_reuse_metrics.get("cv_generation") or {})
    cv_generation_row = _stage_quality_metric_row(
        stage_id="cv_generation",
        label="CV Generation Reuse Rate",
        rate=cv_generation.get("reuse_rate"),
        numerator=cv_generation.get("reused_rows"),
        denominator=cv_generation.get("total_rows"),
        hint="Exact-match CV generation outputs reused from prior runs.",
    )
    if cv_generation_row:
        cv_generation_row["fresh_count"] = int(cv_generation.get("fresh_rows") or 0)
        rows.append(cv_generation_row)

    return rows


def _build_run_health_rows(
    stage_quality_metric_rows: list[dict[str, Any]],
    late_stage_reuse_metric_rows: list[dict[str, Any]],
    stage_artifacts_by_id: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    stage_statuses = _stage_status_by_id(dict(stage_artifacts_by_id or {}))
    for row in stage_quality_metric_rows:
        item = dict(row)
        item["category"] = "quality"
        severity, severity_label = _metric_severity(item, stage_statuses)
        item["severity"] = severity
        item["severity_label"] = severity_label
        rows.append(item)
    for row in late_stage_reuse_metric_rows:
        item = dict(row)
        item["category"] = "reuse"
        severity, severity_label = _metric_severity(item, stage_statuses)
        item["severity"] = severity
        item["severity_label"] = severity_label
        rows.append(item)
    return rows


def _run_event_delivery_health(run_id: str) -> dict[str, Any]:
    dead_letter_path = str(
        os.environ.get("FITCV_EVENT_DEAD_LETTER_PATH")
        or "tmp/fitcv_pipeline_run_events_dead_letter.jsonl"
    ).strip()
    dead_letter_file = Path(dead_letter_path)
    if not dead_letter_file.exists():
        return {
            "status": "healthy",
            "count": 0,
            "last_failed_at": None,
            "last_failed_at_display": None,
            "dead_letter_path": str(dead_letter_file),
        }
    count = 0
    last_failed_at: str | None = None
    last_degradation_reason: str | None = None
    max_retry_attempts = 0
    try:
        with dead_letter_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                record = decode_json_object_or_none(raw)
                if record is None:
                    continue
                row = dict(record.get("row") or {})
                if str(row.get("run_id") or "").strip() != str(run_id):
                    continue
                count += 1
                failed_at_candidate = str(record.get("failed_at") or "").strip() or None
                if failed_at_candidate:
                    last_failed_at = failed_at_candidate
                degradation_reason_candidate = str(record.get("degradation_reason") or "").strip() or None
                if degradation_reason_candidate:
                    last_degradation_reason = degradation_reason_candidate
                retry_attempts_value = int(record.get("retry_attempts") or 0)
                if retry_attempts_value > max_retry_attempts:
                    max_retry_attempts = retry_attempts_value
    except Exception:
        return {
            "status": "unknown",
            "count": 0,
            "last_failed_at": None,
            "last_failed_at_display": None,
            "last_degradation_reason": None,
            "max_retry_attempts": 0,
            "dead_letter_path": str(dead_letter_file),
        }
    return {
        "status": "degraded" if count > 0 else "healthy",
        "count": count,
        "last_failed_at": last_failed_at,
        "last_failed_at_display": _format_compact_utc_timestamp(last_failed_at),
        "last_degradation_reason": last_degradation_reason,
        "max_retry_attempts": max_retry_attempts,
        "dead_letter_path": str(dead_letter_file),
    }

def _run_telemetry_export_health(events: list[RunEvent]) -> dict[str, Any]:
    degraded_count = 0
    last_degraded_stage: str | None = None
    for event in events:
        payload_json = str(getattr(event, "payload_json", "") or "").strip()
        if not payload_json:
            continue
        payload = decode_json_object_or_none(payload_json)
        if payload is None:
            continue
        telemetry = dict(payload.get("telemetry_export") or {})
        if str(telemetry.get("status") or "") != "degraded":
            continue
        if str(telemetry.get("degradation_reason") or "").strip() == "otel_disabled":
            continue
        degraded_count += 1
        stage = str(getattr(event, "stage", "") or "").strip()
        if stage:
            last_degraded_stage = stage
    return {
        "status": "degraded" if degraded_count > 0 else "healthy",
        "degraded_count": degraded_count,
        "last_degraded_stage": last_degraded_stage,
    }


def _run_langfuse_link_health(events: list[RunEvent]) -> dict[str, Any]:
    unverified_count = 0
    degraded_count = 0
    disabled_count = 0
    last_degraded_stage: str | None = None
    last_trace_url: str | None = None
    for ev in events:
        payload_json = str(getattr(ev, "payload_json", "") or "").strip()
        if not payload_json:
            continue
        payload = decode_json_object_or_none(payload_json)
        if payload is None:
            continue
        stage = str(ev.stage or "")
        langfuse = dict(payload.get("langfuse_link") or {})
        status = str(langfuse.get("status") or "")
        if status == "unverified":
            unverified_count += 1
            trace_url = str(langfuse.get("trace_url") or "").strip()
            if trace_url:
                last_trace_url = trace_url
            continue
        if status == "degraded":
            degraded_count += 1
            if stage:
                last_degraded_stage = stage
            continue
        if status == "disabled":
            disabled_count += 1
    if degraded_count > 0:
        overall = "degraded"
    elif unverified_count > 0:
        overall = "unverified"
    else:
        overall = "disabled"
    return {
        "status": overall,
        "unverified_count": unverified_count,
        "degraded_count": degraded_count,
        "disabled_count": disabled_count,
        "last_degraded_stage": last_degraded_stage,
        "last_trace_url": last_trace_url,
    }

def _latest_dead_letter_replay_summary(events: list[RunEvent]) -> dict[str, Any]:
    latest: dict[str, Any] | None = None
    for event in events:
        if str(getattr(event, "stage", "") or "").strip() != "event_dead_letter_replay":
            continue
        payload_json = str(getattr(event, "payload_json", "") or "").strip()
        if not payload_json:
            continue
        payload = decode_json_object_or_none(payload_json)
        if payload is None:
            continue
        latest = {
            "replay_candidates": int(payload.get("replay_candidates") or 0),
            "replayed": int(payload.get("replayed") or 0),
            "failed": int(payload.get("failed") or 0),
            "replay_success_ratio": float(payload.get("replay_success_ratio") or 0.0),
            "remaining_dead_letter_total": int(payload.get("remaining_dead_letter_total") or 0),
            "occurred_at": (
                event.created_at.isoformat()
                if getattr(event, "created_at", None) is not None
                else None
            ),
            "occurred_at_display": _format_compact_utc_timestamp(
                event.created_at.isoformat() if getattr(event, "created_at", None) is not None else None
            ),
        }
    if latest is not None:
        return latest
    return {
        "replay_candidates": 0,
        "replayed": 0,
        "failed": 0,
        "replay_success_ratio": 0.0,
        "remaining_dead_letter_total": 0,
        "occurred_at": None,
        "occurred_at_display": None,
    }

def _latest_reuse_anomaly_summary(events: list[RunEvent]) -> dict[str, Any]:
    latest: dict[str, Any] | None = None
    for event in events:
        if str(getattr(event, "stage", "") or "").strip() != "reuse_anomaly":
            continue
        payload = _event_payload(event)
        output_snapshot = dict(payload.get("output_snapshot") or {})
        stages = [item for item in list(output_snapshot.get("stages") or []) if isinstance(item, dict)]
        if not stages:
            continue
        latest = {
            "status": str(output_snapshot.get("status") or "breached"),
            "min_overlap": int(output_snapshot.get("min_overlap") or 0),
            "reuse_rate_floor": float(output_snapshot.get("reuse_rate_floor") or 0.0),
            "stages": [
                {
                    "stage_id": str(stage.get("stage_id") or ""),
                    "total": int(stage.get("total") or 0),
                    "reused": int(stage.get("reused") or 0),
                    "fresh": int(stage.get("fresh") or 0),
                    "reuse_rate": float(stage.get("reuse_rate") or 0.0),
                    "reuse_rate_percent": int(round(float(stage.get("reuse_rate") or 0.0) * 100)),
                }
                for stage in stages
            ],
            "occurred_at": (
                event.created_at.isoformat()
                if getattr(event, "created_at", None) is not None
                else None
            ),
            "occurred_at_display": _format_compact_utc_timestamp(
                event.created_at.isoformat() if getattr(event, "created_at", None) is not None else None
            ),
        }
    return latest or {
        "status": "healthy",
        "min_overlap": 0,
        "reuse_rate_floor": 0.0,
        "stages": [],
        "occurred_at": None,
        "occurred_at_display": None,
    }

def _aggregate_dead_letter_replay_health(
    runs: list[PipelineRun],
    *,
    bq: Any,
    project: str,
    dataset: str,
) -> dict[str, Any]:
    run_ids = {str(run.run_id or "").strip() for run in runs if str(run.run_id or "").strip()}
    dead_letter_records = _load_event_dead_letter_records(_event_dead_letter_path())
    dead_letter_total = 0
    impacted_run_ids: set[str] = set()
    for record in dead_letter_records:
        row = dict(record.get("row") or {})
        run_id = str(row.get("run_id") or "").strip()
        if not run_id:
            continue
        if run_ids and run_id not in run_ids:
            continue
        dead_letter_total += 1
        impacted_run_ids.add(run_id)

    replay_candidates = 0
    replayed = 0
    failed = 0
    replay_event_count = 0
    for run_id in sorted(run_ids):
        try:
            events = get_events(run_id, bq, project=project, dataset=dataset)
        except Exception as exc:
            logger.warning("dead-letter health aggregation skipped events for run_id=%s due to read error: %s", run_id, exc)
            continue
        summary = _latest_dead_letter_replay_summary(events)
        if summary["replay_candidates"] <= 0:
            continue
        replay_event_count += 1
        replay_candidates += int(summary["replay_candidates"] or 0)
        replayed += int(summary["replayed"] or 0)
        failed += int(summary["failed"] or 0)

    replay_success_ratio = float(replayed / replay_candidates) if replay_candidates else 0.0
    return {
        "dead_letter_total": dead_letter_total,
        "impacted_runs": len(impacted_run_ids),
        "replay_event_count": replay_event_count,
        "replay_candidates": replay_candidates,
        "replayed": replayed,
        "failed": failed,
        "replay_success_ratio": replay_success_ratio,
        "status": "degraded" if dead_letter_total > 0 else "healthy",
    }

def _default_outbox_replay_min_success_ratio() -> float:
    try:
        cfg = load_config()
    except Exception:
        return 0.95
    replay_cfg = dict(cfg.get("outbox_replay_health") or {})
    raw = replay_cfg.get("min_replay_success_ratio")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.95

def _event_dead_letter_path() -> Path:
    return Path(
        str(
            os.environ.get("FITCV_EVENT_DEAD_LETTER_PATH")
            or "tmp/fitcv_pipeline_run_events_dead_letter.jsonl"
        ).strip()
    )

def _load_event_dead_letter_records(dead_letter_file: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not dead_letter_file.exists():
        return records
    with dead_letter_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            parsed = decode_json_object_or_none(raw)
            if parsed is not None:
                records.append(parsed)
    return records

def _persist_event_dead_letter_records(dead_letter_file: Path, records: list[dict[str, Any]]) -> None:
    dead_letter_file.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        if dead_letter_file.exists():
            dead_letter_file.unlink()
        return
    with dead_letter_file.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(_json.dumps(record, ensure_ascii=False) + "\n")

def _build_stage_slice_payload(run: PipelineRun, stage_id: str) -> dict[str, Any] | None:
    if stage_id not in BUNDLE_STAGE_IDS:
        return None
    artifact_payload = _load_stage_transition_artifacts_payload(run)
    if not artifact_payload:
        return None
    artifacts = artifact_payload.get("artifacts") or {}
    stages = artifacts.get("stages") or {}
    stage_artifact = stages.get(stage_id)
    if not isinstance(stage_artifact, dict):
        return None
    return {
        "run_id": run.run_id,
        "stage_id": stage_id,
        "artifact_schema_version": STAGE_TRANSITION_ARTIFACTS_STAGE_SCHEMA_VERSION,
        "created_at": artifact_payload.get("created_at"),
        "stage_artifact": stage_artifact,
    }


def _build_available_run_artifact_files(run: PipelineRun) -> list[RunArtifactFile]:
    files: list[RunArtifactFile] = []
    stage_artifacts_by_id = _stage_artifacts_by_id(run)
    stage_transition_artifact_payload = _load_stage_transition_artifacts_payload(run)

    if run.status == RunStatus.SUCCEEDED and run.results_export_json:
        results_payload = {
            "run_id": run.run_id,
            "results": _results_export_rows_with_hitl_audit(run),
        }
        files.append(
            RunArtifactFile(
                filename="results.json",
                label="Results JSON (Job Ledger)",
                href=f"/admin/runs/{run.run_id}/export.json",
                content=_json.dumps(results_payload, ensure_ascii=False, indent=2),
            )
        )
    if run.status == RunStatus.SUCCEEDED and run.cv_generation_debug_json:
        files.append(
            RunArtifactFile(
                filename="hitl-review-audit.json",
                label="HITL Review Audit JSON",
                href=f"/admin/runs/{run.run_id}/hitl-review-audit.json",
                content=_json.dumps(_build_hitl_review_audit_payload(run), ensure_ascii=False, indent=2),
            )
        )
    if run.status == RunStatus.SUCCEEDED and run.cv_generation_debug_json:
        files.append(
            RunArtifactFile(
                filename="cv-debug.json",
                label="CV Debug JSON",
                href=f"/admin/runs/{run.run_id}/cv-debug.json",
                content=pretty_json_string_or_fallback(run.cv_generation_debug_json),
            )
        )
        review_required_payload = _build_cv_generation_review_required_payload(run)
        if isinstance(review_required_payload, dict):
            files.append(
                RunArtifactFile(
                    filename="cv-generation-review-required.json",
                    label="CV Generation Review-Required JSON",
                    href=f"/admin/runs/{run.run_id}/cv-generation-review-required.json",
                    content=_json.dumps(review_required_payload, ensure_ascii=False, indent=2),
                )
            )
    cv_analysis_trace_payload = _load_run_cv_analysis_trace_payload(run)
    if run.status == RunStatus.SUCCEEDED:
        if not isinstance(cv_analysis_trace_payload, dict):
            cv_analysis_trace_payload = _default_not_applicable_trace_payload(
                run=run,
                trace_name="cv_analysis_trace",
            )
        files.append(
            RunArtifactFile(
                filename="cv-analysis-trace.json",
                label="CV Analysis Trace JSON",
                href=f"/admin/runs/{run.run_id}/cv-analysis-trace.json",
                content=_json.dumps(cv_analysis_trace_payload, ensure_ascii=False, indent=2),
            )
        )
    agentic_live_trace_payload = _load_run_agentic_live_trace_payload(run)
    if run.status == RunStatus.SUCCEEDED:
        if not isinstance(agentic_live_trace_payload, dict):
            agentic_live_trace_payload = _default_not_applicable_trace_payload(
                run=run,
                trace_name="agentic_live_trace",
            )
        files.append(
            RunArtifactFile(
                filename="agentic-live-trace.json",
                label="Agentic Live Trace JSON",
                href=f"/admin/runs/{run.run_id}/agentic-live-trace.json",
                content=_json.dumps(agentic_live_trace_payload, ensure_ascii=False, indent=2),
            )
        )
    if run.status == RunStatus.SUCCEEDED and run.settings_used_json:
        files.append(
            RunArtifactFile(
                filename="settings-used.json",
                label="Settings Used JSON",
                href=f"/admin/runs/{run.run_id}/settings-used.json",
                content=pretty_json_string_or_fallback(run.settings_used_json),
            )
        )
    if run.mapping_suggestions_json and _run_has_reached_stage(run, "enrich") and "enrich" in stage_artifacts_by_id:
        files.append(
            RunArtifactFile(
                filename="mapping-suggestions.json",
                label="Mapping Suggestions JSON",
                href=f"/admin/runs/{run.run_id}/mapping-suggestions.json",
                content=pretty_json_string_or_fallback(run.mapping_suggestions_json),
            )
        )
    if run.synonym_proposals_json and (
        _run_has_reached_stage(run, "enrich") or _run_has_stage_artifact(run, "enrich")
    ):
        files.append(
            RunArtifactFile(
                filename="synonym-proposals.json",
                label="Synonym Proposals JSON",
                href=f"/admin/runs/{run.run_id}/synonym-proposals.json",
                content=pretty_json_string_or_fallback(run.synonym_proposals_json),
            )
        )
        approved_overlay_yaml = _build_run_approved_synonym_overlay_yaml(run)
        if approved_overlay_yaml:
            files.append(
                RunArtifactFile(
                    filename="approved-synonym-proposals.yaml",
                    label="Approved Synonym Overlay Delta YAML",
                    href=f"/admin/runs/{run.run_id}/approved-synonym-proposals.yaml",
                    content=approved_overlay_yaml,
                )
            )
    synonym_overlay_info = _extract_run_synonym_overlay_info(run)
    runtime_overlay_yaml = str(synonym_overlay_info.get("run_overlay_yaml") or "").strip()
    if runtime_overlay_yaml:
        files.append(
            RunArtifactFile(
                filename="synonym-overlay-used.yaml",
                label="Synonym Overlay Delta Used YAML",
                href=f"/admin/runs/{run.run_id}/settings-used.json",
                content=runtime_overlay_yaml if runtime_overlay_yaml.endswith("\n") else runtime_overlay_yaml + "\n",
            )
        )
    synonym_proposals_trace_payload = _load_run_synonym_proposals_trace_payload(run)
    if (
        run.status == RunStatus.SUCCEEDED
        and _run_has_reached_stage(run, "enrich")
        and isinstance(synonym_proposals_trace_payload, dict)
        and str(synonym_proposals_trace_payload.get("trace_status") or "").strip() != "not_applicable"
    ):
        files.append(
            RunArtifactFile(
                filename="synonym-proposals-trace.json",
                label="Synonym Proposals Trace JSON",
                href=f"/admin/runs/{run.run_id}/synonym-proposals-trace.json",
                content=_json.dumps(synonym_proposals_trace_payload, ensure_ascii=False, indent=2),
            )
        )
        files.append(
            RunArtifactFile(
                filename="synonym-suppression-diff.json",
                label="Synonym Suppression Diff JSON",
                href=f"/admin/runs/{run.run_id}/synonym-suppression-diff.json",
                content=_json.dumps(_build_synonym_suppression_diff_payload(run), ensure_ascii=False, indent=2),
            )
        )
    if stage_transition_artifact_payload and run.status != RunStatus.QUEUED:
        files.append(
            RunArtifactFile(
                filename="stage-artifacts.json",
                label="Stage Artifacts JSON (Diagnostics)",
                href=f"/admin/runs/{run.run_id}/stage-artifacts.json",
                content=_json.dumps(stage_transition_artifact_payload, ensure_ascii=False, indent=2),
            )
        )
        for stage_id in BUNDLE_STAGE_IDS:
            payload = _build_stage_slice_payload(run, stage_id)
            if payload is None:
                continue
            files.append(
                RunArtifactFile(
                    filename=f"{stage_id}.json",
                    label=_stage_download_label(stage_id),
                    href=f"/admin/runs/{run.run_id}/stage-artifacts/{stage_id}.json",
                    content=_json.dumps(payload, ensure_ascii=False, indent=2),
                    show_in_exports=False,
                )
            )
    return files


def _default_late_stage_mode_payload() -> dict[str, Any]:
    return {
        "late_stage_mode": "non_agentic",
        "agentic_late_stage_enabled": False,
        "mode_source": "artifact_bundle_default",
        "agentic_status": "not_applicable",
    }


def _load_run_late_stage_mode_payload(run: PipelineRun) -> dict[str, Any]:
    for raw_payload in (run.settings_used_json, run.results_export_json):
        payload = _load_json_object(raw_payload)
        if isinstance(payload, dict) and isinstance(payload.get("late_stage_mode"), dict):
            return dict(payload["late_stage_mode"])
    for stage_id in ("cv_generation", "cv_analysis"):
        stage_payload = dict(_stage_artifacts_by_id(run).get(stage_id) or {})
        if isinstance(stage_payload.get("late_stage_mode"), dict):
            return dict(stage_payload["late_stage_mode"])
    return _default_late_stage_mode_payload()


def _load_run_agentic_live_trace_payload(run: PipelineRun) -> dict[str, Any] | None:
    payload = _load_json_object(run.cv_generation_debug_json)
    trace_payload = payload.get("agentic_live_trace") if isinstance(payload, dict) else None
    if isinstance(trace_payload, dict):
        return dict(trace_payload)
    return None

def _load_run_cv_generation_debug_payload(run: PipelineRun) -> dict[str, Any] | None:
    payload = _load_json_object(run.cv_generation_debug_json)
    if not isinstance(payload, dict):
        return None
    copied = dict(payload)
    records = [
        item
        for item in list(copied.get("debug_records") or copied.get("cv_generation_debug_records") or [])
        if isinstance(item, dict)
    ]
    normalized_records: list[dict[str, Any]] = []
    run_id_value = str(getattr(run, "run_id", "") or payload.get("run_id") or "")
    for index, record in enumerate(records):
        row = dict(record)
        if str(row.get("status") or "").strip() == "review_required":
            ensure_review_item_id(
                run_id=run_id_value,
                record=row,
                fallback_index=index + 1,
            )
        ranking_fit_label = row.get("ranking_fit_label")
        reranker_fit_label = row.get("reranker_fit_label")
        if ranking_fit_label is None and reranker_fit_label is not None:
            row["ranking_fit_label"] = reranker_fit_label
        if reranker_fit_label is None and ranking_fit_label is not None:
            row["reranker_fit_label"] = ranking_fit_label
        normalized_records.append(row)
    if "debug_records" in copied:
        copied["debug_records"] = normalized_records
    if "cv_generation_debug_records" in copied:
        copied["cv_generation_debug_records"] = normalized_records
    return copied

def _run_status_allows_export(run: PipelineRun) -> bool:
    if run.status == RunStatus.SUCCEEDED:
        return True
    return run.status == RunStatus.AWAITING_CONTINUE and str(run.checkpoint_status or "").strip() == "awaiting_review"


def _is_hitl_review_pending_state(run: PipelineRun) -> bool:
    return (
        run.status in {RunStatus.AWAITING_CONTINUE, RunStatus.SUCCEEDED}
        and str(run.checkpoint_status or "").strip() == "awaiting_review"
    )


def _map_review_required_reason_code(record: dict[str, Any]) -> str:
    explicit_code = str(record.get("review_required_reason_code") or "").strip()
    from fitcv.pipeline_contracts import ReviewRequiredReasonCode, is_review_required_reason_code

    if is_review_required_reason_code(explicit_code):
        return explicit_code
    error = dict(record.get("error") or {})
    stage = str(error.get("stage") or "").strip().lower()
    message = str(error.get("message") or record.get("operator_note") or "").strip().lower()
    if "unsupported requirements require review" in message:
        return ReviewRequiredReasonCode.UNSUPPORTED_REQUIREMENT_GAP.value
    if stage == "markdown_quality_review" or "markdown quality" in message:
        return ReviewRequiredReasonCode.QUALITY_GATE_FAILED.value
    if stage == "validation" or "validation failed" in message or "guardrail" in message:
        return ReviewRequiredReasonCode.VALIDATION_GUARDRAIL_FAILED.value
    if "insufficient evidence" in message or "evidence coverage" in message:
        return ReviewRequiredReasonCode.EVIDENCE_COVERAGE_INSUFFICIENT.value
    if stage in {"provider", "llm"} or "provider" in message or "response unusable" in message:
        return ReviewRequiredReasonCode.PROVIDER_RESPONSE_UNUSABLE.value
    return ReviewRequiredReasonCode.MANUAL_REVIEW_OTHER.value

def _extract_unsupported_requirements(record: dict[str, Any]) -> list[str]:
    gap_summary = dict(record.get("gap_summary") or {})
    structured_missing = [
        str(item).strip()
        for item in list(gap_summary.get("missing") or [])
        if str(item).strip()
    ]
    if structured_missing:
        return structured_missing

    message = str((dict(record.get("error") or {})).get("message") or "").strip()
    marker = "Unsupported requirements require review:"
    if marker not in message:
        return []
    suffix = message.split(marker, 1)[1].strip()
    if not suffix:
        return []
    guidance_marker = ". Review the generated CV output"
    if guidance_marker in suffix:
        suffix = suffix.split(guidance_marker, 1)[0].strip()
    parsed = [item.strip() for item in suffix.split(",") if item.strip()]
    banned_tokens = ("approve", "regenerate", "reject", "review the generated cv output")
    cleaned: list[str] = []
    for token in parsed:
        lowered = token.lower()
        if any(banned in lowered for banned in banned_tokens):
            continue
        cleaned.append(token)
    return cleaned

def _review_target_for_reason_code(reason_code: str) -> str:
    if reason_code == "unsupported_requirement_gap":
        return "requirements_alignment"
    if reason_code in {"quality_gate_failed", "validation_guardrail_failed"}:
        return "cv_output"
    if reason_code == "evidence_coverage_insufficient":
        return "cv_output"
    if reason_code == "provider_response_unusable":
        return "other"
    return "other"

def _operator_prompt_for_review_required(
    *,
    reason_code: str,
    unsupported_requirements: list[str],
) -> str:
    if reason_code == "unsupported_requirement_gap":
        missing = ", ".join(unsupported_requirements[:6]) if unsupported_requirements else "listed requirements"
        return (
            "Review the generated CV output against required stack coverage "
            f"({missing}), then choose approve as-is, regenerate once, or reject."
        )
    if reason_code in {"quality_gate_failed", "validation_guardrail_failed"}:
        return "Review CV output quality/guardrail issues, then choose approve as-is, regenerate once, or reject."
    if reason_code == "evidence_coverage_insufficient":
        return "Review whether evidence coverage is acceptable, then choose approve as-is, regenerate once, or reject."
    return "Review this CV outcome and choose approve as-is, regenerate once, or reject."

def _extract_review_required_request_id(record: dict[str, Any]) -> str | None:
    runtime_provenance = dict(record.get("runtime_provenance") or {})
    for key in ("request_id", "response_id"):
        value = str(runtime_provenance.get(key) or "").strip()
        if value:
            return value
    live_trace = dict(record.get("agentic_live_trace") or {})
    for attempt in list(live_trace.get("attempts") or []):
        if not isinstance(attempt, dict):
            continue
        for key in ("request_id", "response_id"):
            value = str(attempt.get(key) or "").strip()
            if value:
                return value
    return None

def _normalized_cv_debug_payload_for_export(run: PipelineRun) -> dict[str, Any] | None:
    payload = _load_run_cv_generation_debug_payload(run)
    if not isinstance(payload, dict):
        return None
    copied = dict(payload)
    records = [item for item in list(copied.get("debug_records") or copied.get("cv_generation_debug_records") or []) if isinstance(item, dict)]
    normalized_records: list[dict[str, Any]] = []
    run_id_value = str(getattr(run, "run_id", "") or payload.get("run_id") or "")
    for index, record in enumerate(records):
        row = dict(record)
        if str(row.get("status") or "").strip() == "review_required":
            ensure_review_item_id(
                run_id=run_id_value,
                record=row,
                fallback_index=index + 1,
            )
        ranking_fit_label = row.get("ranking_fit_label")
        reranker_fit_label = row.get("reranker_fit_label")
        if ranking_fit_label is None and reranker_fit_label is not None:
            row["ranking_fit_label"] = reranker_fit_label
        if reranker_fit_label is None and ranking_fit_label is not None:
            row["reranker_fit_label"] = ranking_fit_label
        if str(row.get("status") or "").strip() == "review_required":
            row["review_required_reason_code"] = _map_review_required_reason_code(row)
            if _extract_review_required_request_id(row) is not None:
                runtime = dict(row.get("runtime_provenance") or {})
                runtime["request_id"] = _extract_review_required_request_id(row)
                row["runtime_provenance"] = runtime
        normalized_records.append(row)
    if "debug_records" in copied:
        copied["debug_records"] = normalized_records
    if "cv_generation_debug_records" in copied:
        copied["cv_generation_debug_records"] = normalized_records
    return copied

def _build_cv_generation_review_required_payload(run: PipelineRun) -> dict[str, Any] | None:
    payload = _load_run_cv_generation_debug_payload(run)
    if not isinstance(payload, dict):
        return None
    records = [item for item in list(payload.get("debug_records") or payload.get("cv_generation_debug_records") or []) if isinstance(item, dict)]

    rows: list[dict[str, Any]] = []
    for record in records:
        if str(record.get("status") or "").strip() != "review_required":
            continue
        reason_code = _map_review_required_reason_code(record)
        unsupported_requirements = _extract_unsupported_requirements(record)
        rows.append(
            {
                "job_url": str(record.get("job_url") or ""),
                "job_title": str(record.get("job_title") or ""),
                "reason_code": reason_code,
                "review_target": _review_target_for_reason_code(reason_code),
                "operator_prompt": _operator_prompt_for_review_required(
                    reason_code=reason_code,
                    unsupported_requirements=unsupported_requirements,
                ),
                "unsupported_requirements": unsupported_requirements,
                "generated_draft_present": bool(str(record.get("markdown_final") or "").strip()),
                "accepted_cv_artifact_present": False,
                "attempt_count": int(record.get("attempt_count") or 1),
                "failed_rule_ids": list(record.get("failed_rule_ids") or []),
                "first_failing_section_key": record.get("first_failing_section_key"),
                "operator_note": record.get("operator_note"),
                "provider_name": str((record.get("runtime_provenance") or {}).get("provider") or ""),
                "model_name": str(record.get("cv_generation_model") or ""),
                "request_id": _extract_review_required_request_id(record),
            }
        )
    if not rows:
        return None
    return {
        "run_id": run.run_id,
        "schema_version": "cv_generation_review_required_v1",
        "rows": rows,
    }


def _build_ranked_cv_outcome_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "ranked_total": 0,
        "ranked_cv_created_count": 0,
        "ranked_fit_gated_count": 0,
        "ranked_review_required_count": 0,
        "ranked_generation_failed_count": 0,
        "ranked_other_no_cv_count": 0,
    }
    for row in rows:
        if row.get("rank") is None:
            continue
        summary["ranked_total"] += 1
        pipeline_status = str(row.get("pipeline_status") or "").strip()
        stage_owned_subreason = str(row.get("stage_owned_subreason") or "").strip()
        cv_gen_status = str((((row.get("decision_chain") or {}).get("cv_generation") or {}).get("status")) or "").strip()
        if pipeline_status == "ranked_with_cv":
            summary["ranked_cv_created_count"] += 1
        elif pipeline_status in {"ranked_blocked_by_reranker_fit", "ranked_skipped_fit_gate"}:
            summary["ranked_fit_gated_count"] += 1
        elif pipeline_status == "ranked_no_cv" and (
            stage_owned_subreason == "review_required" or cv_gen_status == "review_required"
        ):
            summary["ranked_review_required_count"] += 1
        elif pipeline_status == "ranked_no_cv" and (
            stage_owned_subreason in {"validation_failed", "generation_failed", "persistence_failed"}
            or cv_gen_status in {"validation_failed", "generation_failed", "persistence_failed"}
        ):
            summary["ranked_generation_failed_count"] += 1
        elif pipeline_status == "ranked_no_cv":
            # Preserve stage-owned "no CV yet" versus "CV generation failed"
            # truth in summary counters for run detail.
            summary["ranked_other_no_cv_count"] += 1
        else:
            summary["ranked_other_no_cv_count"] += 1
    return summary


def _build_cv_generation_failure_reason_summary(run: PipelineRun) -> dict[str, Any]:
    payload = _load_run_cv_generation_debug_payload(run)
    if not isinstance(payload, dict):
        return {"total_failed": 0, "reason_rows": []}
    records = [
        item
        for item in list(payload.get("debug_records") or payload.get("cv_generation_debug_records") or [])
        if isinstance(item, dict)
    ]
    reason_counts: dict[str, int] = {}
    for record in records:
        status = str(record.get("status") or "").strip()
        if status not in {"generation_failed", "persistence_failed", "validation_failed"}:
            continue
        error_payload = record.get("error") if isinstance(record.get("error"), dict) else {}
        error_stage = str(error_payload.get("stage") or "").strip()
        error_message = str(error_payload.get("message") or "").strip()
        if error_stage or error_message:
            reason_label = f"{error_stage}: {error_message}".strip(": ").strip()
        else:
            reason_label = status
        reason_counts[reason_label] = int(reason_counts.get(reason_label, 0)) + 1
    reason_rows = [
        {"reason": reason, "count": count}
        for reason, count in reason_counts.items()
    ]
    reason_rows.sort(key=lambda row: (-int(row["count"]), str(row["reason"])))
    return {
        "total_failed": int(sum(reason_counts.values())),
        "reason_rows": reason_rows,
    }

def _format_compact_utc_timestamp(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        germany_value = parsed.astimezone(GERMANY_TZ)
        return germany_value.strftime("%d.%m.%Y %H:%M %Z")
    except ValueError:
        return raw

def _fit_classification_badge_class(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"stretch", "strong"}:
        return "badge-info"
    if normalized in {"target", "good"}:
        return "badge-success"
    return "badge-neutral"

_HITL_TERMINAL_RESOLUTION_STATUSES = {
    "approved_as_is",
    "rejected",
    "regenerated_and_accepted",
    "regenerated_and_rejected",
}
_TRUNCATED_MARKDOWN_SENTINELS = (
    "...[truncated]",
    "...[truncated in review queue]",
)


def _normalize_hitl_resolution_status(action_name: str | None, explicit_status: str | None) -> str:
    normalized_explicit = str(explicit_status or "").strip().lower()
    if normalized_explicit:
        return normalized_explicit
    normalized_action = str(action_name or "").strip().lower()
    if normalized_action in {"approve", "approve_as_is"}:
        return "approved_as_is"
    if normalized_action == "reject":
        return "rejected"
    if normalized_action == "regenerate_once":
        return "regeneration_requested"
    return "pending"

def _is_hitl_resolution_pending(resolution_status: str | None) -> bool:
    normalized = str(resolution_status or "").strip().lower() or "pending"
    return normalized not in _HITL_TERMINAL_RESOLUTION_STATUSES


def _build_hitl_review_queue(run: PipelineRun) -> dict[str, Any]:
    payload = _load_run_cv_generation_debug_payload(run)
    if not isinstance(payload, dict):
        return {"queue_items": [], "pending_count": 0, "total_review_required": 0, "actions_count": 0}
    records = list(payload.get("debug_records") or payload.get("cv_generation_debug_records") or [])
    actions = [item for item in list(payload.get("hitl_review_actions") or []) if isinstance(item, dict)]
    latest_action_by_review_item_id: dict[str, dict[str, Any]] = {}
    latest_action_by_job: dict[str, dict[str, Any]] = {}
    for action in actions:
        review_item_id = str(action.get("review_item_id") or "").strip()
        if review_item_id:
            latest_action_by_review_item_id[review_item_id] = action
        job_url = str(action.get("job_url") or "").strip()
        if not job_url:
            continue
        latest_action_by_job[job_url] = action
    queue_items: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        if str(record.get("status") or "").strip() != "review_required":
            continue
        review_item_id = str(record.get("review_item_id") or "").strip()
        job_url = str(record.get("job_url") or "").strip()
        action = latest_action_by_review_item_id.get(review_item_id) if review_item_id else None
        if action is None and job_url:
            action = latest_action_by_job.get(job_url)
        action_name = str((action or {}).get("action") or "").strip() or None
        resolution_status = _normalize_hitl_resolution_status(
            action_name,
            (action or {}).get("resolution_status"),
        )
        markdown_preview = str(record.get("markdown_preview") or "").strip()
        if not markdown_preview:
            markdown_preview = str(record.get("markdown_full") or "").strip()
        if not markdown_preview:
            markdown_preview = str(record.get("markdown_final") or "").strip()
        if markdown_preview and len(markdown_preview) > 2400:
            markdown_preview = markdown_preview[:2400] + "\n...[truncated in review queue]"
        queue_items.append(
            {
                "job_url": job_url,
                "review_item_id": review_item_id or None,
                "missing_job_url": not bool(job_url),
                "job_title": str(record.get("job_title") or "").strip() or "Unknown title",
                "fit_classification": str(record.get("fit_classification") or "").strip() or "unknown",
                "reason": str((record.get("error") or {}).get("message") or "").strip() or "Manual review required.",
                "action": action_name,
                "action_at": str((action or {}).get("created_at") or "").strip() or None,
                "action_at_display": _format_compact_utc_timestamp((action or {}).get("created_at")),
                "action_by": str((action or {}).get("actor") or "").strip() or None,
                "resolution_status": resolution_status,
                "pending": _is_hitl_resolution_pending(resolution_status),
                "cv_markdown_preview": markdown_preview or None,
                "cv_preview_available": bool(markdown_preview),
                "last_regenerated_at": _format_compact_utc_timestamp(record.get("last_regenerated_at")),
                "regenerated_draft_fingerprint": str(record.get("regenerated_draft_fingerprint") or "").strip() or None,
                "regeneration_job_id": str((action or {}).get("regeneration_job_id") or "").strip() or None,
            }
        )
    queue_items.sort(key=lambda item: (not item["pending"], item["job_title"].lower(), item["job_url"]))
    pending_count = sum(1 for item in queue_items if item["pending"])
    return {
        "queue_items": queue_items,
        "pending_count": pending_count,
        "total_review_required": len(queue_items),
        "actions_count": len(actions),
    }


HITL_REVIEW_QUEUE_INLINE_THRESHOLD = 5

def _build_hitl_closure_summary(run: PipelineRun, queue: dict[str, Any] | None = None) -> dict[str, Any]:
    queue_payload = queue or _build_hitl_review_queue(run)
    queue_items = [item for item in list(queue_payload.get("queue_items") or []) if isinstance(item, dict)]
    resolution_totals: dict[str, int] = {}
    for item in queue_items:
        resolution = str(item.get("resolution_status") or "pending").strip() or "pending"
        resolution_totals[resolution] = resolution_totals.get(resolution, 0) + 1
    pending_total = int(queue_payload.get("pending_count") or 0)
    review_required_total = int(queue_payload.get("total_review_required") or 0)
    all_terminal = bool(queue_items) and pending_total == 0
    accepted_cv_total = int(run.cvs_generated or 0)
    requires_no_accepted_ack = all_terminal and accepted_cv_total <= 0
    closure_mode = "incomplete"
    if all_terminal:
        closure_mode = "all_review_rows_terminal"
        if accepted_cv_total <= 0:
            closure_mode = "all_review_rows_terminal_no_accepted_cv"
    return {
        "review_required_total": review_required_total,
        "pending_total": pending_total,
        "resolution_totals": resolution_totals,
        "all_terminal": all_terminal,
        "accepted_cv_total": accepted_cv_total,
        "requires_no_accepted_ack": requires_no_accepted_ack,
        "closure_mode": closure_mode,
    }

def _checkpoint_truth_for_review_closure(run: PipelineRun) -> tuple[str | None, list[str], str | None]:
    """Preserve stage-owned checkpoint truth when lifecycle closes review."""
    completed_stages = [str(item).strip() for item in list(run.completed_stages or []) if str(item).strip()]
    last_completed_stage = str(run.last_completed_stage or "").strip() or None
    if last_completed_stage is None and completed_stages:
        last_completed_stage = completed_stages[-1]
    checkpoint_payload_json = run.checkpoint_payload_json
    return (last_completed_stage, completed_stages, checkpoint_payload_json)

def _effective_settings_dict(run: PipelineRun) -> dict[str, Any]:
    payload = _load_json_object(run.effective_settings_json)
    return payload if isinstance(payload, dict) else {}

def _review_record_for_job(run: PipelineRun, job_url: str) -> dict[str, Any] | None:
    payload = _load_run_cv_generation_debug_payload(run)
    if not isinstance(payload, dict):
        return None
    normalized_job_url = str(job_url or "").strip()
    for record in list(payload.get("debug_records") or payload.get("cv_generation_debug_records") or []):
        if not isinstance(record, dict):
            continue
        if str(record.get("status") or "").strip() != "review_required":
            continue
        if str(record.get("job_url") or "").strip() == normalized_job_url:
            return record
    return None

def _finalize_review_draft_as_cv_artifact(
    *,
    run: PipelineRun,
    job_url: str,
    record: dict[str, Any] | None,
    bq: Any,
    project: str,
    dataset: str,
) -> tuple[bool, str, str | None]:
    if not isinstance(record, dict):
        return (False, "not_review_required", None)
    markdown_full = str(record.get("markdown_full") or "").strip()
    markdown_legacy = str(record.get("markdown_final") or "").strip()
    markdown = markdown_full or markdown_legacy
    if not markdown:
        return (False, "missing_draft_for_approve", None)
    lowered_markdown = markdown.lower()
    if any(lowered_markdown.endswith(sentinel) for sentinel in _TRUNCATED_MARKDOWN_SENTINELS):
        return (False, "truncated_draft_blocked", None)
    rows = _results_export_rows(run)
    row = next((item for item in rows if str(item.get("job_url") or "").strip() == str(job_url or "").strip()), {})
    effective_settings = _effective_settings_dict(run)
    fit_classification = str(record.get("fit_classification") or row.get("fit_classification") or "unknown").strip() or "unknown"
    version_record = create_cv_version_record(
        job_url=str(job_url),
        run_id=str(run.run_id),
        enrichment_version=str(row.get("enrichment_version") or record.get("enrichment_version") or "review_finalize"),
        vector_rank=int(row.get("vector_rank") or row.get("rank") or 0),
        ai_score=float(row.get("ai_score") or 0.0),
        final_score=float(row.get("final_score") or 0.0),
        evidence_ids=list(row.get("evidence_ids") or []),
        prompt_version=str(
            row.get("prompt_version")
            or effective_settings.get("cv_generation_prompt_version")
            or effective_settings.get("cv_prompt_version")
            or "review_finalize_v1"
        ),
        cv_markdown=markdown,
        gap_summary=dict(record.get("gap_summary") or {}),
        fit_classification=fit_classification,
        cv_structured=(dict(record.get("structured_cv_final") or {}) or None),
        cv_generation_model=str(record.get("model") or effective_settings.get("cv_generation_model") or "") or None,
        cv_prompt_version=str(
            effective_settings.get("cv_generation_prompt_version")
            or effective_settings.get("cv_prompt_version")
            or row.get("prompt_version")
            or ""
        ) or None,
        cv_generation_input_fingerprint=str(record.get("cv_generation_input_fingerprint") or "") or None,
        cv_generation_reuse_status=str(record.get("cv_generation_reuse_status") or "") or None,
    )
    errors = insert_cv_version_row(version_record, bq, project=project, dataset=dataset)
    if errors:
        return (False, "persist_failed", None)
    return (True, "finalized", str(version_record.get("version_id") or ""))

def _build_hitl_review_audit_payload(run: PipelineRun) -> dict[str, Any]:
    queue = _build_hitl_review_queue(run)
    payload = _load_run_cv_generation_debug_payload(run)
    actions = [item for item in list((payload or {}).get("hitl_review_actions") or []) if isinstance(item, dict)]
    queue_items = [item for item in list(queue.get("queue_items") or []) if isinstance(item, dict)]
    closure_summary = _build_hitl_closure_summary(run, queue=queue)
    return {
        "schema_version": "hitl_review_audit_v1",
        "run_id": run.run_id,
        "status": run.status.value,
        "summary": {
            "review_required_total": closure_summary.get("review_required_total"),
            "pending_total": closure_summary.get("pending_total"),
            "actions_total": len(actions),
            "closure_mode": closure_summary.get("closure_mode"),
            "resolution_totals": closure_summary.get("resolution_totals"),
            "accepted_cv_total": closure_summary.get("accepted_cv_total"),
            "requires_no_accepted_ack": closure_summary.get("requires_no_accepted_ack"),
        },
        "queue_items": queue_items,
        "actions": actions,
    }

def _build_synonym_proposal_review_queue(run: PipelineRun) -> dict[str, Any]:
    payload = _load_run_synonym_proposals_payload(run)
    if not isinstance(payload, dict):
        return {"items": [], "pending_count": 0, "total_count": 0}
    trace_payload = _load_run_synonym_proposals_trace_payload(run) or {}
    trace_summary = dict(trace_payload.get("trace_summary") or {})
    global_synonyms = _global_synonyms_for_proposal_evaluation(run)
    items: list[dict[str, Any]] = []
    lane_keys = ("skill", "domain", "role_family")
    lane_summary: dict[str, dict[str, Any]] = {
        key: {
            "field": key,
            "label": "Skills" if key == "skill" else ("Domain" if key == "domain" else "Role Family"),
            "total": 0,
            "pending": 0,
            "approved": 0,
            "suppressed": 0,
            "generated": 0,
            "zero_state_reason": "no_suggestions",
        }
        for key in lane_keys
    }
    filtered_as_already_global_count = 0
    for proposal in list(payload.get("proposals") or []):
        if not isinstance(proposal, dict):
            continue
        field = str(proposal.get("field") or "skill").strip().lower() or "skill"
        status = str(proposal.get("proposal_status") or "proposed_unreviewed").strip() or "proposed_unreviewed"
        pending = status in {"proposed_unreviewed", "in_review", "deferred"}
        review_history = [entry for entry in list(proposal.get("review_history") or []) if isinstance(entry, dict)]
        global_promotion_history = [
            entry for entry in list(proposal.get("global_promotion_history") or []) if isinstance(entry, dict)
        ]
        alias = str(proposal.get("alias") or "").strip().lower()
        canonical = str(proposal.get("canonical") or "").strip().lower()
        already_global = field == "skill" and bool(alias) and bool(canonical) and global_synonyms.get(alias) == canonical
        if already_global:
            filtered_as_already_global_count += 1
            lane_summary.setdefault(field, {"field": field, "label": field, "total": 0, "pending": 0, "approved": 0, "suppressed": 0, "generated": 0, "zero_state_reason": "no_suggestions"})
            lane_summary[field]["suppressed"] = int(lane_summary[field].get("suppressed", 0)) + 1
            continue
        lane_summary.setdefault(field, {"field": field, "label": field, "total": 0, "pending": 0, "approved": 0, "suppressed": 0, "generated": 0, "zero_state_reason": "no_suggestions"})
        lane_summary[field]["generated"] = int(lane_summary[field].get("generated", 0)) + 1
        latest_action = review_history[-1] if review_history else {}
        item = {
            "proposal_id": str(proposal.get("proposal_id") or "").strip(),
            "field": field,
            "alias": str(proposal.get("alias") or "").strip() or "—",
            "canonical": str(proposal.get("canonical") or "").strip() or "—",
            "confidence": float(proposal.get("confidence") or 0.0),
            "status": status,
            "pending": pending,
            "latest_action": str(latest_action.get("action") or "").strip() or None,
            "latest_action_at": str(latest_action.get("acted_at") or "").strip() or None,
            "latest_action_at_display": _format_compact_utc_timestamp(latest_action.get("acted_at")),
            "latest_action_by": str(latest_action.get("acted_by") or "").strip() or None,
            "recommended_action": str(proposal.get("recommended_action") or "").strip() or None,
            "recommendation_confidence": float(proposal.get("recommendation_confidence") or 0.0),
            "recommendation_rationale": str(proposal.get("recommendation_rationale") or "").strip() or None,
            "recommendation_risk_flags": [
                str(flag).strip()
                for flag in list(proposal.get("recommendation_risk_flags") or [])
                if str(flag).strip()
            ],
            "globally_promoted": bool(global_promotion_history) or already_global,
        }
        items.append(item)
        lane_summary[field]["total"] = int(lane_summary[field].get("total", 0)) + 1
        if pending:
            lane_summary[field]["pending"] = int(lane_summary[field].get("pending", 0)) + 1
        if status == "approved_for_run_overlay":
            lane_summary[field]["approved"] = int(lane_summary[field].get("approved", 0)) + 1
    items.sort(key=lambda item: (not item["pending"], -item["confidence"], item["alias"], item["canonical"]))
    triage_status = "not_generated"
    if items:
        pending_items = [item for item in items if bool(item.get("pending"))]
        pending_with_reco = [
            item
            for item in pending_items
            if str(item.get("recommended_action") or "").strip()
        ]
        if pending_items and len(pending_with_reco) == len(pending_items):
            triage_status = "fresh"
        elif pending_with_reco:
            triage_status = "partial"
        else:
            triage_status = "stale"
    suppressed_by_field_from_trace = {
        str(field).strip().lower(): int(count or 0)
        for field, count in dict(trace_summary.get("suppressed_count_by_field") or {}).items()
        if str(field).strip()
    }
    for field_key, suppressed_count in suppressed_by_field_from_trace.items():
        lane_summary.setdefault(
            field_key,
            {
                "field": field_key,
                "label": "Skills" if field_key == "skill" else ("Domain" if field_key == "domain" else "Role Family"),
                "total": 0,
                "pending": 0,
                "approved": 0,
                "suppressed": 0,
                "generated": 0,
                "zero_state_reason": "no_suggestions",
            },
        )
        lane_summary[field_key]["suppressed"] = max(
            int(lane_summary[field_key].get("suppressed", 0)),
            suppressed_count,
        )
    for field_key, lane in lane_summary.items():
        total = int(lane.get("total", 0))
        suppressed = int(lane.get("suppressed", 0))
        generated = int(lane.get("generated", 0))
        if total > 0:
            lane["zero_state_reason"] = None
        elif generated == 0 and suppressed > 0:
            lane["zero_state_reason"] = "all_suppressed"
        elif generated == 0:
            lane["zero_state_reason"] = "no_suggestions"
        else:
            lane["zero_state_reason"] = None
    triage_summary = {
        "generated_total": int(trace_summary.get("triage_recommendation_generated_total") or 0),
        "reused_total": int(trace_summary.get("triage_recommendation_reused_total") or 0),
        "fresh_total": int(trace_summary.get("triage_recommendation_fresh_total") or 0),
        "suppressed_total": int(trace_summary.get("triage_recommendation_suppressed_total") or 0),
        "reuse_reason": str(trace_summary.get("triage_recommendation_reuse_reason") or "not_available"),
        "fingerprint": str(trace_summary.get("triage_recommendation_fingerprint") or "").strip() or None,
    }
    return {
        "items": items,
        "items_by_field": {
            "skill": [item for item in items if str(item.get("field") or "").strip() == "skill"],
            "domain": [item for item in items if str(item.get("field") or "").strip() == "domain"],
            "role_family": [item for item in items if str(item.get("field") or "").strip() == "role_family"],
        },
        "pending_count": sum(1 for item in items if item["pending"]),
        "total_count": len(items),
        "approved_count": sum(1 for item in items if item["status"] == "approved_for_run_overlay"),
        "filtered_as_already_global_count": filtered_as_already_global_count,
        "triage_status": triage_status,
        "triage_summary": triage_summary,
        "field_lanes": [lane_summary[key] for key in lane_keys if key in lane_summary],
    }

def _build_markdown_quality_summary(run: PipelineRun) -> dict[str, Any]:
    payload = _load_run_cv_generation_debug_payload(run)
    if not isinstance(payload, dict):
        return {
            "attempted_total": 0,
            "review_required_total": 0,
            "blocking_total": 0,
            "review_reasons_sample": [],
            "blocking_reasons_sample": [],
        }
    records = [item for item in list(payload.get("cv_generation_debug_records") or []) if isinstance(item, dict)]
    attempted_statuses = {"accepted", "review_required", "validation_failed", "generation_failed", "persistence_failed"}
    attempted_records = [record for record in records if str(record.get("status") or "").strip() in attempted_statuses]
    review_required_records = [
        record for record in attempted_records
        if str(record.get("status") or "").strip() == "review_required"
        and str((record.get("error") or {}).get("stage") or "").strip() == "markdown_quality_review"
    ]
    blocking_records = [
        record for record in attempted_records
        if str(record.get("status") or "").strip() == "validation_failed"
        and any(
            str(item).strip()
            for item in list((record.get("validation_initial") or {}).get("markdown_quality_blocking_issues") or [])
        )
    ]
    review_reasons = [
        str((record.get("error") or {}).get("message") or "").strip()
        for record in review_required_records
        if str((record.get("error") or {}).get("message") or "").strip()
    ]
    blocking_reasons: list[str] = []
    for record in blocking_records:
        for issue in list((record.get("validation_initial") or {}).get("markdown_quality_blocking_issues") or []):
            message = str(issue or "").strip()
            if message:
                blocking_reasons.append(message)
    return {
        "attempted_total": len(attempted_records),
        "review_required_total": len(review_required_records),
        "blocking_total": len(blocking_records),
        "review_reasons_sample": review_reasons[:3],
        "blocking_reasons_sample": list(dict.fromkeys(blocking_reasons))[:3],
    }

def _results_export_rows_with_hitl_audit(run: PipelineRun) -> list[dict[str, Any]]:
    rows = _results_export_rows(run)
    if not rows:
        return rows
    payload = _load_run_cv_generation_debug_payload(run)
    if not isinstance(payload, dict):
        return rows
    actions = [item for item in list(payload.get("hitl_review_actions") or []) if isinstance(item, dict)]
    latest_action_by_review_item_id: dict[str, dict[str, Any]] = {}
    latest_action_by_job: dict[str, dict[str, Any]] = {}
    for action in actions:
        review_item_id = str(action.get("review_item_id") or "").strip()
        if review_item_id:
            latest_action_by_review_item_id[review_item_id] = action
        job_url = str(action.get("job_url") or "").strip()
        if job_url:
            latest_action_by_job[job_url] = action
    queue = _build_hitl_review_queue(run)
    review_item_by_id = {
        str(item.get("review_item_id") or "").strip(): item
        for item in list(queue.get("queue_items") or [])
        if isinstance(item, dict) and str(item.get("review_item_id") or "").strip()
    }
    review_item_by_job = {
        str(item.get("job_url") or "").strip(): item
        for item in list(queue.get("queue_items") or [])
        if isinstance(item, dict) and str(item.get("job_url") or "").strip()
    }
    debug_records_by_id = {
        str(item.get("review_item_id") or "").strip(): item
        for item in list(payload.get("debug_records") or payload.get("cv_generation_debug_records") or [])
        if isinstance(item, dict) and str(item.get("review_item_id") or "").strip()
    }
    debug_records_by_job = {
        str(item.get("job_url") or "").strip(): item
        for item in list(payload.get("debug_records") or payload.get("cv_generation_debug_records") or [])
        if isinstance(item, dict) and str(item.get("job_url") or "").strip()
    }
    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        row_copy = dict(row)
        job_url = str(row_copy.get("job_url") or "").strip()
        review_item_id = str(row_copy.get("review_item_id") or "").strip()
        review_item = review_item_by_id.get(review_item_id) if review_item_id else None
        if review_item is None:
            review_item = review_item_by_job.get(job_url)
        latest_action = latest_action_by_review_item_id.get(review_item_id) if review_item_id else None
        if latest_action is None and job_url:
            latest_action = latest_action_by_job.get(job_url)
        debug_record = debug_records_by_id.get(review_item_id) if review_item_id else None
        if debug_record is None and job_url:
            debug_record = debug_records_by_job.get(job_url)
        if review_item is not None:
            row_copy["hitl_review_required"] = True
            row_copy["hitl_review_reason"] = str(review_item.get("reason") or "").strip() or None
            row_copy["hitl_review_pending"] = bool(review_item.get("pending"))
            row_copy["generated_draft_present"] = bool(str((debug_record or {}).get("markdown_final") or "").strip())
            row_copy["accepted_cv_artifact_present"] = bool(row_copy.get("cv"))
            row_copy["hitl_review_category"] = (
                "markdown_quality"
                if "markdown quality" in str(review_item.get("reason") or "").strip().lower()
                else "general"
            )
        if latest_action is not None:
            row_copy["hitl_review_action"] = str(latest_action.get("action") or "").strip() or None
            row_copy["hitl_review_actor"] = str(latest_action.get("actor") or "").strip() or None
            row_copy["hitl_review_action_at"] = str(latest_action.get("created_at") or "").strip() or None
            row_copy["hitl_review_note"] = str(latest_action.get("note") or "").strip() or None
            row_copy["hitl_review_artifact_version_id"] = str(latest_action.get("artifact_version_id") or "").strip() or None
        enriched_rows.append(row_copy)
    return enriched_rows

def _persist_post_hitl_closure_artifact_reconciliation(
    *,
    run_id: str,
    run: PipelineRun,
    closure_payload: dict[str, Any],
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    summary = dict(closure_payload.get("summary") or {})
    accepted_total = int(summary.get("accepted_cv_total") or 0)
    pending_total = int(summary.get("pending_total") or 0)
    fresh_run = get_run(run_id, bq, project=project, dataset=dataset) or run

    export_payload = _load_json_object(fresh_run.results_export_json)
    rows = list((export_payload or {}).get("results") or [])
    actions = [
        item
        for item in list(closure_payload.get("actions") or [])
        if isinstance(item, dict)
    ]
    approved_by_job_url: dict[str, str | None] = {}
    for action in actions:
        action_name = str(action.get("action") or "").strip().lower()
        if action_name not in {"approve", "approve_as_is"}:
            continue
        job_url = str(action.get("job_url") or "").strip()
        if not job_url:
            continue
        artifact_version_id = str(action.get("artifact_version_id") or "").strip() or None
        approved_by_job_url[job_url] = artifact_version_id

    if rows and approved_by_job_url:
        reconciled_rows: list[dict[str, Any]] = []
        for row in rows:
            row_copy = dict(row)
            job_url = str(row_copy.get("job_url") or "").strip()
            if job_url in approved_by_job_url:
                cv_payload = row_copy.get("cv")
                if not isinstance(cv_payload, dict):
                    cv_payload = {}
                cv_payload = dict(cv_payload)
                cv_payload["status"] = "accepted"
                version_id = str(approved_by_job_url.get(job_url) or "").strip()
                if version_id:
                    cv_payload["version_id"] = version_id
                row_copy["cv"] = cv_payload
                row_copy["pipeline_status"] = "ranked_with_cv"
                row_copy["deterministic_outcome"] = "accepted"
                decision_chain = row_copy.get("decision_chain")
                if not isinstance(decision_chain, dict):
                    decision_chain = {}
                cv_generation_chain = decision_chain.get("cv_generation")
                if not isinstance(cv_generation_chain, dict):
                    cv_generation_chain = {}
                cv_generation_chain["status"] = "accepted"
                cv_generation_chain["attempted"] = True
                decision_chain["cv_generation"] = cv_generation_chain
                row_copy["decision_chain"] = decision_chain
            reconciled_rows.append(row_copy)
        payload = dict(export_payload or {})
        payload["run_id"] = run_id
        payload["results"] = reconciled_rows
        update_run_results_export_snapshot(
            run_id,
            _json.dumps(payload, ensure_ascii=False),
            bq,
            project=project,
            dataset=dataset,
        )

    stage_payload = _load_stage_transition_artifacts_payload(fresh_run)
    if not stage_payload:
        return
    artifacts = stage_payload.get("artifacts")
    if not isinstance(artifacts, dict):
        return
    stages = artifacts.get("stages")
    if not isinstance(stages, dict):
        return
    cv_generation_stage = stages.get("cv_generation")
    if not isinstance(cv_generation_stage, dict):
        return

    output_counts = cv_generation_stage.get("output_counts")
    if isinstance(output_counts, dict):
        output_counts["accepted"] = accepted_total
        output_counts["review_required"] = pending_total
    stage_result = cv_generation_stage.get("stage_result")
    if isinstance(stage_result, dict):
        out = stage_result.get("output")
        if isinstance(out, dict):
            out["accepted"] = accepted_total
            out["review_required"] = pending_total

    update_run_stage_transition_artifacts(
        run_id,
        _json.dumps(stage_payload, ensure_ascii=False),
        bq,
        project=project,
        dataset=dataset,
    )

def _run_agentic_runtime_drift_summary(run: PipelineRun) -> dict[str, Any]:
    late_stage_mode = _load_run_late_stage_mode_payload(run)
    if str(late_stage_mode.get("late_stage_mode") or "").strip() != "agentic":
        return {
            "status": "not_applicable",
            "expected_provider": None,
            "expected_model": None,
            "actual_provider": None,
            "actual_model": None,
            "message": "Agentic late-stage mode was not active for this run.",
        }
    expected_provider: str | None = None
    expected_model: str | None = None
    effective_settings = _load_json_object(run.effective_settings_json)
    if isinstance(effective_settings, dict):
        runtime_inputs = dict(effective_settings.get("runtime_inputs") or {})
        runtime_expectation = dict(runtime_inputs.get("agentic_runtime_expectation") or {})
        expected_provider = str(runtime_expectation.get("provider") or "").strip() or None
        expected_model = str(runtime_expectation.get("model") or "").strip() or None
        if expected_model is None:
            expected_model = str(effective_settings.get("cv_generation_model") or "").strip() or None

    trace_payload = _load_run_agentic_live_trace_payload(run) or {}
    records = list(trace_payload.get("records") or [])
    actual_provider: str | None = None
    actual_model: str | None = None
    for record in records:
        if not isinstance(record, dict):
            continue
        runtime_provenance = dict(record.get("runtime_provenance") or {})
        provider = str(runtime_provenance.get("provider") or "").strip() or None
        model = str(runtime_provenance.get("model") or "").strip() or None
        if provider and actual_provider is None:
            actual_provider = provider
        if model and actual_model is None:
            actual_model = model
        if actual_provider and actual_model:
            break
    if expected_provider is None:
        expected_provider = actual_provider
    if expected_model is None:
        expected_model = actual_model
    aligned = (
        bool(actual_provider and expected_provider and actual_provider == expected_provider)
        and bool(actual_model and expected_model and actual_model == expected_model)
    )
    if actual_provider is None and actual_model is None:
        return {
            "status": "not_applicable",
            "expected_provider": expected_provider,
            "expected_model": expected_model,
            "actual_provider": None,
            "actual_model": None,
            "message": "No attempted live-generation provenance was captured for this run.",
        }
    return {
        "status": "aligned" if aligned else "drifted",
        "expected_provider": expected_provider,
        "expected_model": expected_model,
        "actual_provider": actual_provider,
        "actual_model": actual_model,
        "message": (
            "Runtime provider/model match current run settings expectations."
            if aligned else
            "Runtime provider/model differ from current run settings expectations."
        ),
    }


def _load_run_cv_analysis_trace_payload(run: PipelineRun) -> dict[str, Any] | None:
    payload = _load_json_object(run.cv_generation_debug_json)
    trace_payload = payload.get("cv_analysis_trace") if isinstance(payload, dict) else None
    if isinstance(trace_payload, dict):
        return dict(trace_payload)
    return None


def _default_not_applicable_trace_payload(*, run: PipelineRun, trace_name: str) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "trace_schema_version": f"{trace_name}_run_v1",
        "trace_family": trace_name,
        "created_at": (run.finished_at or run.created_at).isoformat() if (run.finished_at or run.created_at) else None,
        "trace_status": "not_applicable",
        "trace_summary": {},
        "records": [],
    }


def _load_run_synonym_proposals_trace_payload(run: PipelineRun) -> dict[str, Any] | None:
    payload = _load_json_object(run.synonym_proposals_json)
    trace_payload = payload.get("synonym_proposals_trace") if isinstance(payload, dict) else None
    if isinstance(trace_payload, dict):
        return dict(trace_payload)
    return None

def _stable_sha256_json(payload: Any) -> str:
    canonical = _json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

def _synonym_observability_fingerprints(run: PipelineRun) -> dict[str, str | None]:
    effective_config = _load_run_effective_config_snapshot(run)
    runtime = dict(effective_config.get("skill_synonyms_runtime") or {})
    pre_run = dict(runtime.get("pre_run_overlay_skill_synonyms") or {})
    overlay_yaml = str(runtime.get("run_overlay_yaml") or "").strip()
    mapping_payload = _load_json_object(run.mapping_suggestions_json) or {}
    proposal_input_bundle = {
        "mapping_suggestions": list(mapping_payload.get("suggestions") or []),
        "global_synonyms": _global_synonyms_for_proposal_evaluation(run),
    }
    return {
        "pre_run_global_map_fingerprint": _stable_sha256_json(pre_run) if pre_run else None,
        "run_overlay_fingerprint": hashlib.sha256(overlay_yaml.encode("utf-8")).hexdigest() if overlay_yaml else None,
        "mapping_suggestions_fingerprint": _stable_sha256_json(mapping_payload) if mapping_payload else None,
        "proposal_input_bundle_fingerprint": _stable_sha256_json(proposal_input_bundle),
    }

def _build_synonym_proposal_decision_ledger(run: PipelineRun) -> list[dict[str, Any]]:
    def _decision_from_status(status: str) -> str:
        value = str(status or "").strip()
        if value in {"proposed_unreviewed", "in_review"}:
            return "pending"
        if value == "deferred":
            return "defer"
        if value == "rejected":
            return "reject"
        if value == "approved_for_run_overlay":
            return "approve"
        return value or "pending"

    def _decision_source_from_status_and_history(
        *,
        status: str,
        review_history: list[dict[str, Any]],
    ) -> str:
        status_value = str(status or "").strip()
        if status_value in {"proposed_unreviewed", "in_review"}:
            return "generated_for_review"
        if review_history:
            return "review_decision_applied"
        return "status_transitioned"

    def _source_display(value: str) -> str:
        mapping = {
            "generated_for_review": "Auto generated",
            "review_decision_applied": "Reviewed",
            "suppressed_as_already_global": "Suppressed (already global)",
            "status_transitioned": "Status transitioned",
        }
        return mapping.get(value, value.replace("_", " ").strip().title() or "—")

    def _reason_display(value: str) -> str:
        mapping = {
            "repeated_alias_mapping": "Seen repeatedly with same meaning",
            "already_global_exact_match": "Already exists in global synonyms",
            "insufficient_non_skill_support": "Insufficient non-skill evidence",
            "generated": "Generated from mapping suggestions",
        }
        return mapping.get(value, value.replace("_", " ").strip() or "—")

    def _decision_display(value: str) -> str:
        mapping = {
            "approve": "Approved",
            "reject": "Rejected",
            "defer": "Deferred",
            "pending": "Pending",
            "suppressed": "Suppressed",
        }
        return mapping.get(value, value.replace("_", " ").strip().title() or "Pending")

    def _recommendation_display(value: str) -> str:
        if not value or value == "—":
            return "—"
        mapping = {
            "approve": "Recommend Approve",
            "reject": "Recommend Reject",
            "defer": "Recommend Defer",
        }
        lowered = value.strip().lower()
        return mapping.get(lowered, f"Recommend {value.strip().title()}")

    payload = _load_run_synonym_proposals_payload(run) or {}
    proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
    rows: list[dict[str, Any]] = []
    for proposal in proposals:
        status = str(proposal.get("proposal_status") or "").strip()
        review_history = [
            item for item in list(proposal.get("review_history") or [])
            if isinstance(item, dict)
        ]
        decision_source = _decision_source_from_status_and_history(
            status=status,
            review_history=review_history,
        )
        raw_reason = str((proposal.get("rationale") or {}).get("kind") or "generated")
        decision = _decision_from_status(status)
        recommendation = str(proposal.get("recommended_action") or "").strip() or "—"
        last_action_display = "—"
        if review_history:
            last = review_history[-1]
            action = str(last.get("action") or "").strip()
            actor = str(last.get("acted_by") or "").strip()
            acted_at = str(last.get("acted_at") or "").strip()
            acted_at_display = _format_compact_utc_timestamp(acted_at) if acted_at else None
            parts = [part for part in [action or None, (f"by {actor}" if actor else None), acted_at_display or None] if part]
            if parts:
                last_action_display = " ".join(parts)
        is_overridden = (
            recommendation != "—"
            and recommendation.lower() in {"approve", "reject", "defer"}
            and decision in {"approve", "reject", "defer"}
            and recommendation.lower() != decision
        )
        rows.append(
            {
                "alias": str(proposal.get("alias") or "").strip(),
                "canonical": str(proposal.get("canonical") or "").strip(),
                "decision_source": decision_source,
                "decision_source_display": _source_display(decision_source),
                "decision_reason": raw_reason,
                "decision_reason_display": _reason_display(raw_reason),
                "recommendation": recommendation,
                "recommendation_display": _recommendation_display(recommendation),
                "decision": decision,
                "decision_display": _decision_display(decision),
                "is_overridden": is_overridden,
                "last_action_display": last_action_display,
                "confidence": float(proposal.get("confidence") or 0.0),
                "conflict": bool((proposal.get("conflict_summary") or {}).get("has_conflict")),
            }
        )
    trace_payload = _load_run_synonym_proposals_trace_payload(run) or {}
    for example in list(trace_payload.get("suppression_examples") or []):
        if not isinstance(example, dict):
            continue
        rows.append(
            {
                "alias": str(example.get("alias") or "").strip(),
                "canonical": str(example.get("canonical") or "").strip(),
                "decision_source": "suppressed_as_already_global",
                "decision_source_display": _source_display("suppressed_as_already_global"),
                "decision_reason": "already_global_exact_match",
                "decision_reason_display": _reason_display("already_global_exact_match"),
                "recommendation": "—",
                "recommendation_display": "—",
                "decision": "suppressed",
                "decision_display": _decision_display("suppressed"),
                "is_overridden": False,
                "last_action_display": "—",
                "confidence": None,
                "conflict": False,
            }
        )
    rows.sort(key=lambda item: (str(item.get("decision_source")), str(item.get("alias"))))
    return rows

def _build_synonym_suppression_diff_payload(run: PipelineRun) -> dict[str, Any]:
    mapping_payload = _load_json_object(run.mapping_suggestions_json) or {}
    suggestions = [item for item in list(mapping_payload.get("suggestions") or []) if isinstance(item, dict)]
    trace_payload = _load_run_synonym_proposals_trace_payload(run) or {}
    trace_summary = dict(trace_payload.get("trace_summary") or {})
    suppressed_examples = [item for item in list(trace_payload.get("suppression_examples") or []) if isinstance(item, dict)]
    generated_payload = _load_run_synonym_proposals_payload(run) or {}
    generated_proposals = [item for item in list(generated_payload.get("proposals") or []) if isinstance(item, dict)]
    fps = _synonym_observability_fingerprints(run)
    suppressed_count_by_field = {
        str(field).strip(): int(count or 0)
        for field, count in dict(trace_summary.get("suppressed_count_by_field") or {}).items()
        if str(field).strip()
    }
    suppressed_pairs_total = int(sum(suppressed_count_by_field.values()))
    if suppressed_pairs_total <= 0:
        suppressed_pairs_total = int(
            trace_summary.get("suppressed_as_already_global_count") or len(suppressed_examples)
        )
    return {
        "run_id": run.run_id,
        "schema_version": "synonym_suppression_diff_v1",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "suggested_pairs_total": len(suggestions),
        "suppressed_pairs_total": suppressed_pairs_total,
        "generated_pairs_total": int(trace_summary.get("generated_for_review_count") or len(generated_proposals)),
        "suppressed_pairs": suppressed_examples[:200],
        "generated_pairs": [
            {"alias": str(p.get("alias") or ""), "canonical": str(p.get("canonical") or "")}
            for p in generated_proposals[:200]
        ],
        "suppression_source": str(trace_summary.get("suppression_source") or "none"),
        **fps,
    }


def _artifact_applicability_state(run: PipelineRun, filename: str, included_files: set[str]) -> str:
    if filename in included_files:
        return "present"
    if filename in {f"{stage_id}.json" for stage_id in BUNDLE_STAGE_IDS}:
        stage_id = filename[:-5]
        if _run_has_stage_artifact(run, stage_id):
            return "missing"
        return "not_applicable" if not _run_has_reached_stage(run, stage_id) else "missing"
    if filename == "mapping-suggestions.json":
        return (
            "missing"
            if _run_has_reached_stage(run, "enrich") and _run_has_stage_artifact(run, "enrich")
            else "not_applicable"
        )
    if filename == "synonym-proposals.json":
        return "missing" if _run_has_reached_stage(run, "enrich") else "not_applicable"
    if filename == "synonym-proposals-trace.json":
        if not _run_has_reached_stage(run, "enrich"):
            return "not_applicable"
        trace_payload = _load_run_synonym_proposals_trace_payload(run)
        if isinstance(trace_payload, dict) and str(trace_payload.get("trace_status") or "").strip():
            trace_status = str(trace_payload.get("trace_status") or "").strip()
            if trace_status == "not_applicable":
                return "not_applicable"
            return "degraded" if trace_status == "degraded" else "present"
        return "missing"
    if filename == "synonym-suppression-diff.json":
        if not _run_has_reached_stage(run, "enrich"):
            return "not_applicable"
        trace_payload = _load_run_synonym_proposals_trace_payload(run)
        if isinstance(trace_payload, dict) and str(trace_payload.get("trace_status") or "").strip():
            trace_status = str(trace_payload.get("trace_status") or "").strip()
            if trace_status == "not_applicable":
                return "not_applicable"
            return "degraded" if trace_status == "degraded" else "present"
        return "missing"
    if filename == "approved-synonym-proposals.yaml":
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            return "not_applicable"
        proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
        overlay_synonyms, _proposal_ids = _approved_synonym_overlay_payload(proposals)
        return "missing" if overlay_synonyms else "not_applicable"
    if filename == "synonym-overlay-used.yaml":
        overlay_info = _extract_run_synonym_overlay_info(run)
        return "missing" if str(overlay_info.get("run_overlay_yaml") or "").strip() else "not_applicable"
    if filename == "results.json":
        return "missing" if run.status == RunStatus.SUCCEEDED else "not_applicable"
    if filename == "hitl-review-audit.json":
        return "missing" if run.status == RunStatus.SUCCEEDED and bool(run.cv_generation_debug_json) else "not_applicable"
    if filename == "settings-used.json":
        return "missing" if run.status == RunStatus.SUCCEEDED else "not_applicable"
    if filename == "cv-debug.json":
        return "missing" if run.status == RunStatus.SUCCEEDED else "not_applicable"
    if filename == "cv-generation-review-required.json":
        if run.status != RunStatus.SUCCEEDED:
            return "not_applicable"
        return "present" if isinstance(_build_cv_generation_review_required_payload(run), dict) else "not_applicable"
    if filename == "cv-analysis-trace.json":
        late_stage_mode = _load_run_late_stage_mode_payload(run)
        if str(late_stage_mode.get("late_stage_mode") or "").strip() != "agentic":
            return "not_applicable"
        trace_payload = _load_run_cv_analysis_trace_payload(run)
        if isinstance(trace_payload, dict) and str(trace_payload.get("trace_status") or "").strip():
            return "degraded" if str(trace_payload.get("trace_status") or "").strip() == "degraded" else "present"
        return "missing"
    if filename == "agentic-live-trace.json":
        late_stage_mode = _load_run_late_stage_mode_payload(run)
        if str(late_stage_mode.get("late_stage_mode") or "").strip() != "agentic":
            return "not_applicable"
        trace_payload = _load_run_agentic_live_trace_payload(run)
        if isinstance(trace_payload, dict) and str(trace_payload.get("trace_status") or "").strip():
            return "degraded" if str(trace_payload.get("trace_status") or "").strip() == "degraded" else "present"
        return "missing"
    if filename == "stage-artifacts.json":
        return "missing" if run.status != RunStatus.QUEUED else "not_applicable"
    return "missing"


def _build_run_artifact_bundle_manifest(run: PipelineRun, files: list[RunArtifactFile]) -> dict[str, Any]:
    included_files = [artifact.filename for artifact in files]
    included_file_set = set(included_files)
    artifact_states = {
        filename: _artifact_applicability_state(run, filename, included_file_set)
        for filename in BUNDLE_ARTIFACT_FILENAMES
    }
    missing_files = [filename for filename, state in artifact_states.items() if state == "missing"]
    return {
        "run_id": run.run_id,
        "status": run.status.value,
        "run_mode": run.run_mode,
        "run_mode_label": run_mode_label(run.run_mode),
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "bundle_schema_version": "run_artifact_bundle_v6",
        "late_stage_mode": _load_run_late_stage_mode_payload(run),
        "included_files": included_files,
        "missing_files": missing_files,
        "artifact_states": artifact_states,
    }


def _build_run_export_links(run: PipelineRun) -> list[dict[str, str]]:
    artifact_files = _build_available_run_artifact_files(run)
    links: list[dict[str, str]] = []
    links.append(
        {
            "label": "Download Global Synonyms YAML",
            "href": "/admin/synonyms/global.yaml",
            "helper_text": "Canonical global skill synonym map used as baseline across runs.",
        }
    )
    links.append(
        {
            "label": "Download Global Domain Aliases YAML",
            "href": "/admin/synonyms/global-domain.yaml",
            "helper_text": "Canonical global domain alias map used as baseline across runs.",
        }
    )
    links.append(
        {
            "label": "Download Global Role Family Aliases YAML",
            "href": "/admin/synonyms/global-role-family.yaml",
            "helper_text": "Canonical global role family alias map used as baseline across runs.",
        }
    )
    if artifact_files:
        links.append(
            {
                "label": "Download All Artifacts (.zip)",
                "href": f"/admin/runs/{run.run_id}/artifacts.zip",
                "helper_text": "Includes all currently available run artifacts.",
            }
        )
    for artifact in artifact_files:
        if not artifact.show_in_exports:
            continue
        helper_text = ""
        if artifact.filename == "approved-synonym-proposals.yaml":
            helper_text = "Run-approved delta only; does not replace the global map."
        elif artifact.filename == "synonym-overlay-used.yaml":
            helper_text = "Run overlay delta only; effective map includes base synonyms plus this delta."
        links.append({"label": artifact.label, "href": artifact.href, "helper_text": helper_text})
    return links


def _can_upload_synonym_overlay(run: PipelineRun) -> bool:
    return (
        run.run_mode == "manual_staged"
        and run.status == RunStatus.AWAITING_CONTINUE
        and str(run.next_stage or "").strip() == "rule_filter"
        and str(run.last_completed_stage or "").strip() == "enrich"
    )


def _load_run_effective_config_snapshot(
    run: PipelineRun,
    *,
    fallback_to_runtime_config: bool = True,
) -> dict[str, Any]:
    if run.effective_settings_json:
        payload = decode_json_object_or_none(run.effective_settings_json)
        if payload is not None:
            return apply_synonym_management_defaults(payload)
    if fallback_to_runtime_config:
        try:
            return apply_synonym_management_defaults(load_config(run.config_path))
        except (FileNotFoundError, ValueError):
            return {}
    return {}


def _extract_run_synonym_overlay_info(run: PipelineRun) -> dict[str, Any]:
    if not run.effective_settings_json:
        return {"has_run_overlay": False}
    payload = decode_json_object_or_none(run.effective_settings_json)
    if payload is None:
        return {"has_run_overlay": False}
    runtime = payload.get("skill_synonyms_runtime")
    if not isinstance(runtime, dict):
        return {"has_run_overlay": False}
    section_counts = dict(runtime.get("run_overlay_section_counts") or {})
    normalized_section_counts = {
        "skill_synonyms": int(section_counts.get("skill_synonyms") or 0),
        "domain_alias_map": int(section_counts.get("domain_alias_map") or 0),
        "role_family_alias_map": int(section_counts.get("role_family_alias_map") or 0),
        "domain_neighbors": int(section_counts.get("domain_neighbors") or 0),
        "role_family_neighbors": int(section_counts.get("role_family_neighbors") or 0),
    }
    source = str(runtime.get("run_overlay_source") or "").strip().lower()
    source_labels = {
        "trigger_upload": "Trigger Upload",
        "staged_override": "Staged Override",
        "upload": "Staged Override",
        "proposal_review": "Proposal Review",
        "proposal_review_apply": "Proposal Review Apply",
    }
    snapshot_yaml = ""
    snapshot_label = ""
    run_overlay_yaml = str(runtime.get("run_overlay_yaml") or "")
    if run_overlay_yaml.strip():
        snapshot_yaml = run_overlay_yaml
        snapshot_label = source_labels.get(source, "Run Overlay") if source else "Run Overlay"
    else:
        base_policy_path = str(runtime.get("base_policy_path") or "").strip()
        candidate_paths = []
        if base_policy_path:
            raw_path = Path(base_policy_path)
            candidate_paths.append(raw_path)
            if not raw_path.is_absolute():
                candidate_paths.append(Path.cwd() / raw_path)
        for candidate in candidate_paths:
            try:
                if candidate.is_file():
                    snapshot_yaml = candidate.read_text(encoding="utf-8")
                    snapshot_label = "Default Config"
                    break
            except OSError:
                continue
    return {
        "has_run_overlay": bool(runtime.get("has_run_overlay")),
        "source": source,
        "source_label": source_labels.get(source, "Run Overlay") if source else "",
        "filename": str(runtime.get("run_overlay_filename") or ""),
        "entry_count": int(runtime.get("run_overlay_entry_count") or 0),
        "uploaded_at": str(runtime.get("run_overlay_uploaded_at") or ""),
        "uploaded_at_display": _format_compact_utc_timestamp(runtime.get("run_overlay_uploaded_at")),
        "effective_entry_count": int(runtime.get("entry_count") or 0),
        "section_counts": normalized_section_counts,
        "has_default_overlay": bool(runtime.get("has_overlay")),
        "snapshot_yaml": snapshot_yaml,
        "snapshot_label": snapshot_label,
        "run_overlay_yaml": run_overlay_yaml,
    }

def _global_synonyms_for_proposal_evaluation(run: PipelineRun) -> dict[str, str]:
    effective_config = _load_run_effective_config_snapshot(run, fallback_to_runtime_config=False)
    runtime = dict(effective_config.get("skill_synonyms_runtime") or {})
    pre_run_global_synonyms = dict(runtime.get("pre_run_overlay_skill_synonyms") or {})
    source_map = pre_run_global_synonyms if pre_run_global_synonyms else dict(effective_config.get("skill_synonyms") or {})
    return {
        str(alias).strip().lower(): str(canonical).strip().lower()
        for alias, canonical in source_map.items()
        if str(alias).strip() and str(canonical).strip()
    }

def _can_regenerate_synonym_proposals(run: PipelineRun) -> bool:
    return (
        run.run_mode == "manual_staged"
        and run.status == RunStatus.AWAITING_CONTINUE
        and str(run.last_completed_stage or "").strip() == "enrich"
        and str(run.next_stage or "").strip() == "rule_filter"
        and bool(run.mapping_suggestions_json)
    )

def _validate_overlay_scope(overlay_payload: dict[str, Any], scope: str) -> None:
    normalized_scope = str(scope or "combined").strip().lower() or "combined"
    if normalized_scope not in {"combined", "skill", "domain", "role_family"}:
        raise HTTPException(status_code=422, detail=f"Unknown overlay upload scope: {normalized_scope!r}")
    if normalized_scope == "combined":
        return

    section_keys = {
        "skill_synonyms",
        "domain_alias_map",
        "role_family_alias_map",
        "domain_neighbors",
        "role_family_neighbors",
    }
    present_sections = {
        key
        for key in section_keys
        if isinstance((overlay_payload or {}).get(key), dict) and bool((overlay_payload or {}).get(key))
    }
    allowed_by_scope = {
        "skill": {"skill_synonyms"},
        "domain": {"domain_alias_map", "domain_neighbors"},
        "role_family": {"role_family_alias_map", "role_family_neighbors"},
    }
    allowed_sections = allowed_by_scope[normalized_scope]
    invalid_sections = sorted(section for section in present_sections if section not in allowed_sections)
    if invalid_sections:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Overlay scope '{normalized_scope}' allows sections {sorted(allowed_sections)}; "
                f"found disallowed sections {invalid_sections}"
            ),
        )

def _synonym_management_mode(run: PipelineRun) -> dict[str, bool]:
    config = _load_run_effective_config_snapshot(run, fallback_to_runtime_config=False)
    return resolve_synonym_management_mode(config)

def _synonym_review_section_state(
    *,
    queue: dict[str, Any],
    decision_ledger: list[dict[str, Any]],
    can_regenerate: bool,
) -> str:
    pending_count = int((queue or {}).get("pending_count") or 0)
    total_count = int((queue or {}).get("total_count") or 0)
    approved_count = int((queue or {}).get("approved_count") or 0)
    has_ledger = bool(decision_ledger)
    if pending_count > 0:
        return "decision_active"
    if approved_count > 0 or bool(can_regenerate) or has_ledger:
        return "decision_active"
    if total_count > 0:
        return "summary"
    return "hidden"

def _find_synonym_proposal_index(payload: dict[str, Any], proposal_id: str) -> int | None:
    target = str(proposal_id or "").strip()
    if not target:
        return None
    proposals = list(payload.get("proposals") or [])
    for idx, proposal in enumerate(proposals):
        if not isinstance(proposal, dict):
            continue
        if str(proposal.get("proposal_id") or "").strip() == target:
            return idx
    return None


def _aggregate_mapping_suggestion_payloads(runs: list[PipelineRun]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for run in runs:
        raw_payload = run.mapping_suggestions_json
        if not raw_payload:
            continue
        payload = decode_json_object_or_none(raw_payload)
        if not payload:
            continue
        for suggestion in list(payload.get("suggestions") or []):
            if not isinstance(suggestion, dict):
                continue
            alias = str(suggestion.get("alias") or "").strip().lower()
            canonical = str(suggestion.get("canonical") or "").strip().lower()
            if not alias or not canonical:
                continue
            bucket = grouped.setdefault(
                alias,
                {
                    "alias": alias,
                    "canonical": canonical,
                    "occurrences": 0,
                    "confidence_sum": 0.0,
                    "must_have_skills": set(),
                    "run_ids": set(),
                    "conflicting_canonicals": set(),
                },
            )
            bucket["occurrences"] += 1
            bucket["confidence_sum"] += float(suggestion.get("confidence") or 0.0)
            bucket["run_ids"].add(run.run_id)
            must_have_skill = str(suggestion.get("must_have_skill") or "").strip()
            if must_have_skill:
                bucket["must_have_skills"].add(must_have_skill)
            if canonical != bucket["canonical"]:
                bucket["conflicting_canonicals"].add(canonical)
    suggestions: list[dict[str, Any]] = []
    for bucket in grouped.values():
        occurrences = int(bucket["occurrences"])
        suggestions.append(
            {
                "alias": bucket["alias"],
                "canonical": bucket["canonical"],
                "occurrences": occurrences,
                "avg_confidence": (bucket["confidence_sum"] / occurrences) if occurrences else 0.0,
                "must_have_skills": sorted(bucket["must_have_skills"]),
                "run_ids": sorted(bucket["run_ids"]),
                "conflicting_canonicals": sorted(bucket["conflicting_canonicals"]),
            }
        )
    suggestions.sort(key=lambda item: (-int(item["occurrences"]), str(item["alias"])))
    return {
        "mapping_suggestions_schema_version": MAPPING_SUGGESTIONS_AGGREGATE_SCHEMA_VERSION,
        "suggestions": suggestions,
    }


def _load_run_synonym_proposals_payload(run: PipelineRun) -> dict[str, Any] | None:
    raw_payload = run.synonym_proposals_json
    if not raw_payload:
        return None
    return decode_json_object_or_none(raw_payload)


def _aggregate_synonym_proposal_payloads(runs: list[PipelineRun]) -> dict[str, Any]:
    proposals: list[dict[str, Any]] = []
    for run in runs:
        payload = _load_run_synonym_proposals_payload(run)
        if not payload:
            continue
        for proposal in list(payload.get("proposals") or []):
            if not isinstance(proposal, dict):
                continue
            row = dict(proposal)
            row["run_id"] = str(row.get("run_id") or run.run_id)
            proposals.append(row)
    proposals.sort(
        key=lambda item: (
            str(item.get("proposal_status") or ""),
            -float(item.get("confidence") or 0.0),
            str(item.get("alias") or ""),
        )
    )
    return {
        "synonym_proposals_schema_version": SYNONYM_PROPOSALS_QUEUE_SCHEMA_VERSION,
        "proposals": proposals,
    }


def _find_run_and_synonym_proposal(
    runs: list[PipelineRun],
    proposal_id: str,
) -> tuple[PipelineRun, dict[str, Any], dict[str, Any], int] | None:
    normalized_id = str(proposal_id or "").strip()
    if not normalized_id:
        return None
    for run in runs:
        payload = _load_run_synonym_proposals_payload(run)
        if not payload:
            continue
        proposals = list(payload.get("proposals") or [])
        for idx, proposal in enumerate(proposals):
            if not isinstance(proposal, dict):
                continue
            if str(proposal.get("proposal_id") or "").strip() == normalized_id:
                return run, payload, proposal, idx
    return None


def _approved_synonym_overlay_payload(proposals: list[dict[str, Any]]) -> tuple[dict[str, str], list[str]]:
    overlay: dict[str, str] = {}
    proposal_ids: list[str] = []
    for proposal in proposals:
        if str(proposal.get("proposal_status") or "") != "approved_for_run_overlay":
            continue
        alias = str(proposal.get("alias") or "").strip().lower()
        canonical = str(proposal.get("canonical") or "").strip().lower()
        if not alias or not canonical:
            continue
        overlay[alias] = canonical
        proposal_id = str(proposal.get("proposal_id") or "").strip()
        if proposal_id:
            proposal_ids.append(proposal_id)
    return overlay, sorted(set(proposal_ids))


def _build_synonym_overlay_yaml(overlay: dict[str, str]) -> str:
    if not overlay:
        return ""
    lines = ["skill_synonyms:"]
    for alias, canonical in sorted(overlay.items()):
        lines.append(f"  {alias}: {canonical}")
    return "\n".join(lines) + "\n"


def _build_run_approved_synonym_overlay_yaml(run: PipelineRun) -> str | None:
    payload = _load_run_synonym_proposals_payload(run)
    if not isinstance(payload, dict):
        return None
    proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
    overlay_synonyms, _proposal_ids = _approved_synonym_overlay_payload(proposals)
    if not overlay_synonyms:
        return None
    return _build_synonym_overlay_yaml(overlay_synonyms)


def _triage_synonym_proposal_recommendation(
    proposal: dict[str, Any],
    *,
    now_iso: str,
    runtime: dict[str, Any],
) -> dict[str, Any]:
    provider = str(runtime.get("provider") or "fitcv_builtin").strip().lower()
    model = str(runtime.get("model") or "synonym_triage_v1").strip() or "synonym_triage_v1"
    base_url = str(runtime.get("base_url") or "").strip() or None
    wire_api = str(runtime.get("wire_api") or "builtin").strip() or "builtin"
    if provider not in {"fitcv_builtin", "builtin"}:
        api_key = str(runtime.get("api_key") or "").strip()
        if not api_key:
            raise RuntimeError("missing_provider_api_key")
        provider_result = _call_synonym_triage_provider(
            proposal=proposal,
            runtime=runtime,
            now_iso=now_iso,
        )
        return {
            "recommended_action": str(provider_result.get("recommended_action") or "defer").strip(),
            "recommendation_confidence": round(float(provider_result.get("recommendation_confidence") or 0.5), 3),
            "recommendation_rationale": str(provider_result.get("recommendation_rationale") or "Provider triage response").strip(),
            "recommendation_risk_flags": [
                str(flag).strip()
                for flag in list(provider_result.get("recommendation_risk_flags") or [])
                if str(flag).strip()
            ],
            "recommendation_runtime": {
                "provider": provider,
                "model": model,
                "base_url": base_url,
                "wire_api": wire_api,
                "triage_at": now_iso,
                "triage_version": "synonym_triage_v1",
            },
        }

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
            "provider": provider,
            "model": model,
            "base_url": base_url,
            "wire_api": wire_api,
            "triage_at": now_iso,
            "triage_version": "synonym_triage_v1",
        },
    }


def _call_synonym_triage_provider(
    *,
    proposal: dict[str, Any],
    runtime: dict[str, Any],
    now_iso: str,
) -> dict[str, Any]:
    provider = str(runtime.get("provider") or "openai").strip().lower()
    if provider not in {"openai", "9router"}:
        raise RuntimeError(f"unsupported_provider:{provider}")
    base_url = str(runtime.get("base_url") or "").strip() or "https://api.openai.com/v1"
    api_key = str(runtime.get("api_key") or "").strip()
    if not api_key:
        raise RuntimeError("missing_provider_api_key")
    model = str(runtime.get("model") or "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    wire_api = str(runtime.get("wire_api") or "responses").strip().lower() or "responses"
    timeout = float(runtime.get("timeout_secs") or 20.0)

    proposal_view = {
        "proposal_id": str(proposal.get("proposal_id") or "").strip(),
        "proposal_status": str(proposal.get("proposal_status") or "").strip(),
        "alias": str(proposal.get("alias") or "").strip(),
        "canonical": str(proposal.get("canonical") or "").strip(),
        "confidence": float(proposal.get("confidence") or 0.0),
        "candidate_canonicals": [
            str(item).strip()
            for item in list(proposal.get("candidate_canonicals") or [])
            if str(item).strip()
        ],
    }
    rendered = render_prompt(
        "synonym_triage.recommendation.v1",
        {
            "proposal_json": _json.dumps(proposal_view, ensure_ascii=False),
            "now_iso": now_iso,
        },
    )
    prompt = rendered.text
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if wire_api == "responses":
        url = base_url.rstrip("/") + "/responses"
        payload = {
            "model": model,
            "input": prompt,
        }
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            body = resp.json()
        output_text = _extract_responses_text(body)
    else:
        url = base_url.rstrip("/") + "/chat/completions"
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
        }
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            body = resp.json()
        output_text = str((((body.get("choices") or [{}])[0].get("message") or {}).get("content")) or "").strip()
    if not output_text:
        raise RuntimeError("empty_provider_output")
    parsed = _json.loads(output_text)
    if not isinstance(parsed, dict):
        raise RuntimeError("invalid_provider_json_shape")
    action = str(parsed.get("recommended_action") or "").strip()
    if action not in {"approve", "defer", "reject"}:
        raise RuntimeError("invalid_provider_action")
    confidence = float(parsed.get("recommendation_confidence") or 0.5)
    confidence = min(1.0, max(0.0, confidence))
    return {
        "recommended_action": action,
        "recommendation_confidence": confidence,
        "recommendation_rationale": str(parsed.get("recommendation_rationale") or "").strip() or "Provider recommendation",
        "recommendation_risk_flags": [
            str(flag).strip()
            for flag in list(parsed.get("recommendation_risk_flags") or [])
            if str(flag).strip()
        ],
    }


def _extract_responses_text(payload: dict[str, Any]) -> str:
    output = list(payload.get("output") or [])
    for block in output:
        if not isinstance(block, dict):
            continue
        content = list(block.get("content") or [])
        for item in content:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if text:
                return text
    return str(payload.get("output_text") or "").strip()


def _synonym_triage_fingerprint(
    proposal: dict[str, Any],
    *,
    runtime: dict[str, Any],
    overlay_fingerprint: str | None = None,
) -> str:
    return build_synonym_triage_fingerprint(
        proposal=proposal,
        runtime=runtime,
        overlay_fingerprint=overlay_fingerprint,
    )


def _resolve_synonym_triage_runtime(run: PipelineRun) -> dict[str, Any]:
    provider = str(os.environ.get("FITCV_LANGGRAPH_PROVIDER", "fitcv_builtin") or "fitcv_builtin").strip().lower()
    model = str(os.environ.get("FITCV_LANGGRAPH_MODEL", "synonym_triage_v1") or "synonym_triage_v1").strip()
    base_url = str(os.environ.get("FITCV_LANGGRAPH_OPENAI_BASE_URL", "") or "").strip() or None
    wire_api = str(os.environ.get("FITCV_LANGGRAPH_WIRE_API", "") or "").strip() or "builtin"
    api_key = (
        str(os.environ.get("FITCV_LANGGRAPH_OPENAI_API_KEY", "") or "").strip()
        or str(os.environ.get("OPENAI_API_KEY", "") or "").strip()
    )
    sleep_secs = 0.0
    concurrency = 1
    effective_settings = _load_json_object(run.effective_settings_json)
    if isinstance(effective_settings, dict):
        runtime_inputs = dict(effective_settings.get("runtime_inputs") or {})
        expected = dict(runtime_inputs.get("agentic_runtime_expectation") or {})
        provider = str(expected.get("provider") or provider).strip().lower() or provider
        model = str(expected.get("model") or model).strip() or model
        base_url = str(expected.get("base_url") or base_url or "").strip() or None
        wire_api = str(expected.get("wire_api") or wire_api).strip() or wire_api
        stage_runtime = dict(effective_settings.get("stage_runtime") or {})
        cv_analysis_runtime = dict(stage_runtime.get("cv_analysis") or {})
        try:
            sleep_secs = max(0.0, float(cv_analysis_runtime.get("sleep_secs", 0.0)))
        except (TypeError, ValueError):
            sleep_secs = 0.0
        try:
            concurrency = max(1, int(cv_analysis_runtime.get("concurrency", 1)))
        except (TypeError, ValueError):
            concurrency = 1
    return {
        "provider": provider,
        "model": model,
        "base_url": base_url,
        "wire_api": wire_api,
        "api_key": api_key,
        "sleep_secs": sleep_secs,
        "concurrency": concurrency,
    }


def _global_skill_synonyms_path() -> Path:
    return Path("config") / "taxonomy" / "skill_synonyms.yaml"

_YAML_TOP_LEVEL_KEY_RE = re.compile(r"^[A-Za-z0-9_]+\s*:")

def _render_yaml_top_level_mapping(*, key: str, mappings: dict[str, str]) -> list[str]:
    if not mappings:
        return [f"{key}: {{}}\n"]
    lines = [f"{key}:\n"]
    for alias, canonical in sorted(mappings.items()):
        lines.append(f"  {alias}: {canonical}\n")
    return lines

def _replace_yaml_top_level_mapping_block(
    *,
    raw_yaml: str,
    key: str,
    mappings: dict[str, str],
) -> str:
    lines = raw_yaml.splitlines(keepends=True)
    start_idx: int | None = None
    for idx, line in enumerate(lines):
        if line.startswith(f"{key}:"):
            start_idx = idx
            break
    replacement = _render_yaml_top_level_mapping(key=key, mappings=mappings)
    if start_idx is None:
        if raw_yaml and not raw_yaml.endswith("\n"):
            return raw_yaml + "\n" + "".join(replacement)
        return raw_yaml + "".join(replacement)
    end_idx = start_idx + 1
    while end_idx < len(lines):
        candidate = lines[end_idx]
        if candidate.startswith("#") or not candidate.strip():
            end_idx += 1
            continue
        if candidate[:1].isspace():
            end_idx += 1
            continue
        if _YAML_TOP_LEVEL_KEY_RE.match(candidate):
            break
        end_idx += 1
    return "".join([*lines[:start_idx], *replacement, *lines[end_idx:]])


def _load_global_skill_synonyms_map() -> dict[str, str]:
    path = _global_skill_synonyms_path()
    if not path.exists():
        return {}
    raw_yaml = path.read_text(encoding="utf-8")
    return parse_skill_synonym_overlay_yaml(raw_yaml)


def _persist_global_skill_synonyms_map(mappings: dict[str, str]) -> None:
    path = _global_skill_synonyms_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_build_synonym_overlay_yaml(mappings), encoding="utf-8")

def _global_domain_synonyms_path() -> Path:
    return Path("config") / "taxonomy" / "domain_synonyms.yaml"

def _load_global_domain_alias_map() -> dict[str, str]:
    path = _global_domain_synonyms_path()
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    raw_map = payload.get("domain_alias_map")
    if not isinstance(raw_map, dict):
        return {}
    return {
        str(alias).strip().lower(): str(canonical).strip().lower()
        for alias, canonical in raw_map.items()
        if str(alias).strip() and str(canonical).strip()
    }

def _persist_global_domain_alias_map(mappings: dict[str, str]) -> None:
    path = _global_domain_synonyms_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    raw_yaml = path.read_text(encoding="utf-8") if path.exists() else ""
    updated = _replace_yaml_top_level_mapping_block(raw_yaml=raw_yaml, key="domain_alias_map", mappings=mappings)
    path.write_text(updated, encoding="utf-8")

def _global_role_family_synonyms_path() -> Path:
    return Path("config") / "taxonomy" / "role_family_synonyms.yaml"

def _load_global_role_family_alias_map() -> dict[str, str]:
    path = _global_role_family_synonyms_path()
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    raw_map = payload.get("role_family_alias_map")
    if not isinstance(raw_map, dict):
        return {}
    return {
        str(alias).strip().lower(): str(canonical).strip().lower()
        for alias, canonical in raw_map.items()
        if str(alias).strip() and str(canonical).strip()
    }

def _persist_global_role_family_alias_map(mappings: dict[str, str]) -> None:
    path = _global_role_family_synonyms_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    raw_yaml = path.read_text(encoding="utf-8") if path.exists() else ""
    updated = _replace_yaml_top_level_mapping_block(
        raw_yaml=raw_yaml,
        key="role_family_alias_map",
        mappings=mappings,
    )
    path.write_text(updated, encoding="utf-8")


def _canonical_variants_for_compare(value: str) -> set[str]:
    normalized = " ".join(str(value or "").strip().lower().split())
    if not normalized:
        return set()
    variants: set[str] = {normalized}
    if normalized.endswith(")") and "(" in normalized:
        head, tail = normalized.rsplit("(", 1)
        head = " ".join(head.strip().split())
        paren = " ".join(tail[:-1].strip().split())
        if head:
            variants.add(head)
        if paren:
            variants.add(paren)
        if head and paren:
            acronym = "".join(part[0] for part in head.split() if part and part[0].isalnum())
            if acronym and paren == acronym:
                variants.add(acronym)
    return {item for item in variants if item}

def _canonicals_equivalent_for_promotion(current: str, proposed: str) -> bool:
    current_variants = _canonical_variants_for_compare(current)
    proposed_variants = _canonical_variants_for_compare(proposed)
    if not current_variants or not proposed_variants:
        return False
    return bool(current_variants & proposed_variants)

def _build_promote_global_preview(
    *,
    run: PipelineRun,
    payload: dict[str, Any],
    selected_proposal_ids: list[str],
) -> dict[str, Any]:
    global_maps = {
        "skill": _load_global_skill_synonyms_map(),
        "domain": _load_global_domain_alias_map(),
        "role_family": _load_global_role_family_alias_map(),
    }
    proposal_by_id: dict[str, dict[str, Any]] = {}
    for proposal in list(payload.get("proposals") or []):
        if not isinstance(proposal, dict):
            continue
        proposal_id = str(proposal.get("proposal_id") or "").strip()
        if proposal_id:
            proposal_by_id[proposal_id] = proposal
    selected: list[dict[str, Any]] = []
    for proposal_id in selected_proposal_ids:
        proposal = proposal_by_id.get(proposal_id)
        if proposal is None:
            selected.append(
                {
                    "proposal_id": proposal_id,
                    "field": "",
                    "alias": "",
                    "canonical": "",
                    "status": "missing",
                    "diff_type": "skip",
                    "reason": "proposal_not_found",
                }
            )
            continue
        status = str(proposal.get("proposal_status") or "").strip() or "proposed_unreviewed"
        field = str(proposal.get("field") or "skill").strip().lower() or "skill"
        alias = str(proposal.get("alias") or "").strip().lower()
        canonical = str(proposal.get("canonical") or "").strip().lower()
        selected.append(
            {
                "proposal_id": proposal_id,
                "field": field,
                "alias": alias,
                "canonical": canonical,
                "status": status,
            }
        )
    duplicate_alias_keys: set[tuple[str, str]] = set()
    alias_counts: dict[tuple[str, str], set[str]] = {}
    for row in selected:
        field = str(row.get("field") or "").strip()
        alias = str(row.get("alias") or "").strip()
        canonical = str(row.get("canonical") or "").strip()
        if not field or not alias or not canonical:
            continue
        alias_counts.setdefault((field, alias), set()).add(canonical)
    for key, canonicals in alias_counts.items():
        if len(canonicals) > 1:
            duplicate_alias_keys.add(key)

    counts = {
        "add": 0,
        "update": 0,
        "conflict": 0,
        "skip": 0,
        "new_aliases": 0,
        "unchanged_aliases": 0,
        "overridden_aliases": 0,
    }
    def _fresh_counts() -> dict[str, int]:
        return {
            "add": 0,
            "update": 0,
            "conflict": 0,
            "skip": 0,
            "new_aliases": 0,
            "unchanged_aliases": 0,
            "overridden_aliases": 0,
        }
    counts_by_field: dict[str, dict[str, int]] = {}
    for row in selected:
        status = str(row.get("status") or "").strip()
        field = str(row.get("field") or "").strip() or "skill"
        alias = str(row.get("alias") or "").strip()
        canonical = str(row.get("canonical") or "").strip()
        bucket = counts_by_field.setdefault(field, _fresh_counts())
        if status != "approved_for_run_overlay":
            row["diff_type"] = "skip"
            row["reason"] = "not_approved_for_run_overlay"
            counts["skip"] += 1
            bucket["skip"] += 1
            continue
        if not alias or not canonical:
            row["diff_type"] = "skip"
            row["reason"] = "empty_alias_or_canonical"
            counts["skip"] += 1
            bucket["skip"] += 1
            continue
        if field not in global_maps:
            row["diff_type"] = "skip"
            row["reason"] = "unknown_field"
            counts["skip"] += 1
            bucket["skip"] += 1
            continue
        if (field, alias) in duplicate_alias_keys:
            row["diff_type"] = "conflict"
            row["reason"] = "duplicate_alias_with_multiple_canonicals"
            counts["conflict"] += 1
            bucket["conflict"] += 1
            continue
        current = str(global_maps[field].get(alias) or "").strip().lower()
        if not current:
            row["diff_type"] = "add"
            row["reason"] = "new_alias"
            counts["add"] += 1
            counts["new_aliases"] += 1
            bucket["add"] += 1
            bucket["new_aliases"] += 1
        elif _canonicals_equivalent_for_promotion(current, canonical):
            row["diff_type"] = "skip"
            row["reason"] = "already_present"
            counts["skip"] += 1
            counts["unchanged_aliases"] += 1
            bucket["skip"] += 1
            bucket["unchanged_aliases"] += 1
        else:
            row["diff_type"] = "update"
            row["reason"] = "canonical_change"
            row["current_global_canonical"] = current
            counts["update"] += 1
            counts["overridden_aliases"] += 1
            bucket["update"] += 1
            bucket["overridden_aliases"] += 1

    ready_rows = [row for row in selected if str(row.get("diff_type") or "").strip() in {"add", "update"}]
    already_global_rows = [
        row for row in selected
        if str(row.get("diff_type") or "").strip() == "skip"
        and str(row.get("reason") or "").strip() == "already_present"
    ]
    blocked_rows = [
        row for row in selected
        if row not in ready_rows and row not in already_global_rows
    ]
    def _group_by_field(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {"skill": [], "domain": [], "role_family": [], "unknown": []}
        for item in rows:
            field = str(item.get("field") or "").strip().lower()
            grouped.setdefault(field if field in grouped else "unknown", []).append(item)
        return grouped

    return {
        "run_id": run.run_id,
        "selected_count": len(selected),
        "counts": counts,
        "counts_by_field": counts_by_field,
        "rows": selected,
        "ready_rows": ready_rows,
        "ready_rows_by_field": _group_by_field(ready_rows),
        "already_global_rows": already_global_rows,
        "already_global_rows_by_field": _group_by_field(already_global_rows),
        "blocked_rows": blocked_rows,
        "blocked_rows_by_field": _group_by_field(blocked_rows),
    }


def _auto_apply_synonym_recommendations(
    *,
    run: PipelineRun,
    payload: dict[str, Any],
    acted_by: str,
    note: str,
) -> dict[str, Any]:
    proposals = list(payload.get("proposals") or [])
    action_map = {
        "approve": "approve_for_run_overlay",
        "reject": "reject",
        "defer": "defer",
    }
    reason_counts: dict[str, int] = {}
    applied = 0
    skipped = 0
    failed = 0
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for idx, proposal in enumerate(proposals):
        if not isinstance(proposal, dict):
            skipped += 1
            reason_counts["invalid_row"] = int(reason_counts.get("invalid_row", 0)) + 1
            continue
        status = str(proposal.get("proposal_status") or "").strip() or "proposed_unreviewed"
        if status not in {"proposed_unreviewed", "in_review", "deferred"}:
            skipped += 1
            reason_counts["not_pending"] = int(reason_counts.get("not_pending", 0)) + 1
            continue
        recommendation = str(proposal.get("recommended_action") or "").strip().lower()
        if not recommendation:
            skipped += 1
            reason_counts["missing_recommendation"] = int(reason_counts.get("missing_recommendation", 0)) + 1
            continue
        action = action_map.get(recommendation)
        if action is None:
            skipped += 1
            reason_counts["unsupported_recommendation"] = int(reason_counts.get("unsupported_recommendation", 0)) + 1
            continue
        next_status = transition_synonym_proposal_status(status, action)
        if not next_status:
            failed += 1
            reason_counts["invalid_transition"] = int(reason_counts.get("invalid_transition", 0)) + 1
            continue
        updated = dict(proposal)
        history = [item for item in list(updated.get("review_history") or []) if isinstance(item, dict)]
        history.append(
            {
                "action": action,
                "from_status": status,
                "to_status": next_status,
                "acted_by": acted_by,
                "acted_at": now_iso,
                "note": note,
            }
        )
        updated["proposal_status"] = next_status
        updated["review_history"] = history
        proposals[idx] = updated
        applied += 1
    payload["proposals"] = proposals
    return {
        "applied": applied,
        "skipped": skipped,
        "failed": failed,
        "reason_counts": reason_counts,
    }


def _is_validation_eligible_for_auto_promote(run: PipelineRun) -> bool:
    return run.status == RunStatus.SUCCEEDED


def _commit_synonym_global_promotion(
    *,
    run: PipelineRun,
    payload: dict[str, Any],
    preview: dict[str, Any],
    selected_ids: list[str],
    acted_by: str,
    note: str,
    bq: Any,
    project: str,
    dataset: str,
) -> dict[str, Any]:
    field_specs = {
        "skill": {
            "load": _load_global_skill_synonyms_map,
            "persist": _persist_global_skill_synonyms_map,
            "label": "skill_synonyms",
        },
        "domain": {
            "load": _load_global_domain_alias_map,
            "persist": _persist_global_domain_alias_map,
            "label": "domain_alias_map",
        },
        "role_family": {
            "load": _load_global_role_family_alias_map,
            "persist": _persist_global_role_family_alias_map,
            "label": "role_family_alias_map",
        },
    }
    global_maps: dict[str, dict[str, str]] = {
        field: dict(spec["load"]())
        for field, spec in field_specs.items()
    }
    conflict_fields: set[str] = set()
    for row in list(preview.get("rows") or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("diff_type") or "").strip() == "conflict":
            conflict_fields.add(str(row.get("field") or "").strip().lower() or "skill")

    applied = 0
    skipped = 0
    failed = 0
    new_aliases = 0
    unchanged_aliases = 0
    overridden_aliases = 0
    updated_ids: list[str] = []
    applied_by_field: dict[str, int] = {field: 0 for field in field_specs}
    for row in list(preview.get("rows") or []):
        if not isinstance(row, dict):
            continue
        field = str(row.get("field") or "skill").strip().lower() or "skill"
        diff_type = str(row.get("diff_type") or "").strip()
        if diff_type not in {"add", "update"}:
            if str(row.get("reason") or "").strip() == "already_present":
                unchanged_aliases += 1
            skipped += 1
            continue
        alias = str(row.get("alias") or "").strip()
        canonical = str(row.get("canonical") or "").strip()
        proposal_id = str(row.get("proposal_id") or "").strip()
        if not alias or not canonical or not proposal_id:
            skipped += 1
            continue
        if field not in field_specs:
            skipped += 1
            failed += 1
            continue
        if field in conflict_fields:
            skipped += 1
            failed += 1
            continue
        current = str(global_maps[field].get(alias) or "").strip().lower()
        if not current:
            new_aliases += 1
        elif _canonicals_equivalent_for_promotion(current, canonical):
            unchanged_aliases += 1
            skipped += 1
            continue
        else:
            overridden_aliases += 1
        global_maps[field][alias] = canonical
        applied += 1
        applied_by_field[field] = int(applied_by_field.get(field) or 0) + 1
        updated_ids.append(proposal_id)
    for field, spec in field_specs.items():
        if int(applied_by_field.get(field) or 0) <= 0:
            continue
        spec["persist"](global_maps[field])
    proposals = list(payload.get("proposals") or [])
    if updated_ids:
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        updated_id_set = set(updated_ids)
        for idx, proposal in enumerate(proposals):
            if not isinstance(proposal, dict):
                continue
            proposal_id = str(proposal.get("proposal_id") or "").strip()
            if proposal_id not in updated_id_set:
                continue
            entry = dict(proposal)
            history = [item for item in list(entry.get("global_promotion_history") or []) if isinstance(item, dict)]
            history.append(
                {
                    "action": "promote_to_global",
                    "acted_by": acted_by,
                    "acted_at": now_iso,
                    "note": note,
                    "run_id": run.run_id,
                }
            )
            entry["global_promotion_history"] = history
            proposals[idx] = entry
        payload["proposals"] = proposals
        update_run_synonym_proposals(
            run_id=run.run_id,
            synonym_proposals_json=_json.dumps(payload, ensure_ascii=False),
            bq=bq,
            project=project,
            dataset=dataset,
        )
    append_event(
        RunEvent(
            run_id=run.run_id,
            event_id=str(uuid.uuid4()),
            stage="synonym_proposal_promoted_global",
            level="info",
            message=f"Promoted {applied} synonym proposal mapping(s) to global policy",
            created_at=datetime.datetime.now(datetime.timezone.utc),
            payload_json=_json.dumps(
                {
                    "applied_count": applied,
                    "skipped_count": skipped,
                    "failed_count": failed,
                    "selected_count": len(selected_ids),
                    "new_aliases_count": new_aliases,
                    "unchanged_aliases_count": unchanged_aliases,
                    "overridden_aliases_count": overridden_aliases,
                    "applied_count_by_field": dict(applied_by_field),
                    "conflict_fields": sorted(conflict_fields),
                    "proposal_ids": sorted(set(updated_ids)),
                    "acted_by": acted_by,
                    "note": note,
                },
                ensure_ascii=False,
            ),
        ),
        bq,
        project=project,
        dataset=dataset,
    )
    return {
        "applied": applied,
        "skipped": skipped,
        "failed": failed,
        "new_aliases": new_aliases,
        "unchanged_aliases": unchanged_aliases,
        "overridden_aliases": overridden_aliases,
        "applied_by_field": dict(applied_by_field),
        "conflict_fields": sorted(conflict_fields),
    }

def _run_post_validation_auto_promote_global(
    *,
    run: PipelineRun,
    acted_by: str,
    note: str,
    bq: Any,
    project: str,
    dataset: str,
) -> dict[str, Any]:
    payload = _load_run_synonym_proposals_payload(run)
    promote_counts: dict[str, int] = {
        "applied": 0,
        "skipped": 0,
        "failed": 0,
        "new_aliases": 0,
        "unchanged_aliases": 0,
        "overridden_aliases": 0,
    }
    promote_skip_reason = "disabled"
    if not isinstance(payload, dict):
        promote_skip_reason = "no_payload"
    else:
        mode = _synonym_management_mode(run)
        if bool(mode.get("auto_promote_global_enabled")) and bool(mode.get("promote_global_enabled")):
            if not _is_validation_eligible_for_auto_promote(run):
                promote_skip_reason = "validation_not_eligible"
            else:
                selected_ids = [
                    str(item.get("proposal_id") or "").strip()
                    for item in list(payload.get("proposals") or [])
                    if isinstance(item, dict)
                    and str(item.get("proposal_status") or "").strip() == "approved_for_run_overlay"
                    and str(item.get("field") or "skill").strip().lower() == "skill"
                    and str(item.get("proposal_id") or "").strip()
                ]
                if not selected_ids:
                    promote_skip_reason = "no_approved_proposals"
                else:
                    preview = _build_promote_global_preview(
                        run=run,
                        payload=payload,
                        selected_proposal_ids=selected_ids,
                    )
                    if int((preview.get("counts") or {}).get("conflict") or 0) > 0:
                        promote_counts["failed"] = int((preview.get("counts") or {}).get("conflict") or 0)
                        promote_counts["skipped"] = int((preview.get("counts") or {}).get("skip") or 0)
                        promote_skip_reason = "conflicts_present"
                    else:
                        promote_counts = _commit_synonym_global_promotion(
                            run=run,
                            payload=payload,
                            preview=preview,
                            selected_ids=selected_ids,
                            acted_by=acted_by,
                            note=note,
                            bq=bq,
                            project=project,
                            dataset=dataset,
                        )
                        promote_skip_reason = "applied"

        trace_payload = dict(payload.get("synonym_proposals_trace") or {})
        trace_summary = dict(trace_payload.get("trace_summary") or {})
        trace_summary["auto_promote_global_applied"] = int(promote_counts.get("applied") or 0)
        trace_summary["auto_promote_global_skipped"] = int(promote_counts.get("skipped") or 0)
        trace_summary["auto_promote_global_failed"] = int(promote_counts.get("failed") or 0)
        trace_summary["auto_promote_global_skip_reason"] = promote_skip_reason
        trace_payload["trace_summary"] = trace_summary
        payload["synonym_proposals_trace"] = trace_payload
        update_run_synonym_proposals(
            run.run_id,
            _json.dumps(payload, ensure_ascii=False),
            bq,
            project=project,
            dataset=dataset,
        )
    return {
        "counts": promote_counts,
        "skip_reason": promote_skip_reason,
    }


def _sync_run_overlay_from_approved_synonym_proposals(
    *,
    run: PipelineRun,
    payload: dict[str, Any],
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
    overlay_synonyms, proposal_ids = _approved_synonym_overlay_payload(proposals)
    if not overlay_synonyms:
        return
    effective_config = _load_run_effective_config_snapshot(run)
    overlay_yaml = _build_synonym_overlay_yaml(overlay_synonyms)
    updated_config = apply_runtime_skill_synonym_overlay(
        effective_config,
        overlay_synonyms,
        source="proposal_review",
        filename="approved-synonym-proposals.yaml",
        uploaded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        raw_yaml=overlay_yaml,
    )
    runtime = dict(updated_config.get("skill_synonyms_runtime") or {})
    runtime["run_overlay_proposal_ids"] = proposal_ids
    updated_config["skill_synonyms_runtime"] = runtime
    update_run_effective_settings(
        run.run_id,
        _json.dumps(updated_config, ensure_ascii=False),
        bq,
        project=project,
        dataset=dataset,
    )


def _decision_chain_label(value: Any) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    return DECISION_CHAIN_LABELS.get(normalized, normalized.replace("_", " "))


def _format_pipeline_outcome_detail(row: dict[str, Any]) -> str | None:
    decision_chain = row.get("decision_chain")
    if not isinstance(decision_chain, dict):
        return None

    detail_parts: list[str] = []
    shortlist = decision_chain.get("shortlist")
    if isinstance(shortlist, dict):
        shortlist_status = _decision_chain_label(shortlist.get("status"))
        if shortlist_status and shortlist_status != "not applicable":
            detail_parts.append(f"Shortlist: {shortlist_status}")

    primary_fit = decision_chain.get("primary_fit")
    if isinstance(primary_fit, dict):
        fit_label = str(primary_fit.get("label") or "").strip()
        if fit_label:
            detail_parts.append(f"Primary fit: {fit_label}")

    cv_analysis = decision_chain.get("cv_analysis")
    if isinstance(cv_analysis, dict):
        cv_analysis_status = _decision_chain_label(cv_analysis.get("status"))
        if cv_analysis_status and cv_analysis_status not in {"not run", "ready for CV generation"}:
            detail_parts.append(f"CV analysis: {cv_analysis_status}")

    cv_generation = decision_chain.get("cv_generation")
    if isinstance(cv_generation, dict):
        cv_status = _decision_chain_label(cv_generation.get("status"))
        if cv_status and cv_status not in {"not applicable", "not attempted", "skipped after CV analysis"}:
            detail_parts.append(f"CV: {cv_status}")

    validation = decision_chain.get("validation")
    if isinstance(validation, dict):
        validation_status = _decision_chain_label(validation.get("status"))
        if validation_status and validation_status != "not run":
            detail_parts.append(f"Validation: {validation_status}")

    if not detail_parts:
        return None
    return " | ".join(detail_parts)


def _results_export_rows(run: PipelineRun) -> list[dict[str, Any]]:
    if not run.results_export_json:
        return []
    payload = decode_json_object_or_none(run.results_export_json)
    if payload is None:
        return []
    rows = payload.get("results")
    if not isinstance(rows, list):
        return []
    return [_normalize_results_export_row(dict(row)) for row in rows if isinstance(row, dict)]


def _normalize_results_export_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    decision_chain = normalized.get("decision_chain")
    if not isinstance(decision_chain, dict):
        decision_chain = {}
    cv_generation = decision_chain.get("cv_generation")
    if not isinstance(cv_generation, dict):
        cv_generation = {}
    primary_fit = decision_chain.get("primary_fit")
    if not isinstance(primary_fit, dict):
        primary_fit = {}

    pipeline_status = str(normalized.get("pipeline_status") or "").strip()
    stage_owned_subreason = str(normalized.get("stage_owned_subreason") or "").strip()
    cv_status = str(cv_generation.get("status") or "").strip()
    has_cv = bool(normalized.get("cv"))

    final_status = str(normalized.get("final_status") or "").strip()
    if not final_status:
        if pipeline_status == "ranked_with_cv" or has_cv:
            final_status = "accepted"
        elif pipeline_status in {"ranked_blocked_by_reranker_fit", "ranked_skipped_fit_gate"}:
            final_status = "blocked_by_reranker_fit"
        elif cv_status in {"review_required", "validation_failed", "generation_failed", "persistence_failed"}:
            final_status = cv_status
        elif stage_owned_subreason in {"review_required", "validation_failed", "generation_failed", "persistence_failed"}:
            final_status = stage_owned_subreason
        elif pipeline_status == "ranked_no_cv":
            final_status = "ranked_no_cv"
        elif pipeline_status == "not_shortlisted":
            final_status = "not_shortlisted"
        elif pipeline_status:
            final_status = pipeline_status
        elif stage_owned_subreason:
            final_status = stage_owned_subreason
    if final_status:
        normalized["final_status"] = final_status

    if not str(normalized.get("reason") or "").strip():
        reason = stage_owned_subreason
        if not reason:
            reject_reasons = normalized.get("reject_reasons")
            if isinstance(reject_reasons, list):
                first = next((str(item).strip() for item in reject_reasons if str(item).strip()), "")
                reason = first
        if reason:
            normalized["reason"] = reason

    if not str(normalized.get("fit_label") or "").strip():
        fit_label = str(primary_fit.get("label") or "").strip()
        if fit_label:
            normalized["fit_label"] = fit_label

    return normalized

def _fallback_enriched_rows_from_results_export(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Derive enriched-tab rows from results export when run_structured_jobs is unavailable."""
    fallback_rows: list[dict[str, Any]] = []
    for row in rows:
        job_url = str(row.get("job_url") or "").strip()
        if not job_url:
            continue
        fallback_rows.append(
            {
                "job_url": job_url,
                "title": str(row.get("title") or row.get("job_title") or job_url),
                "location_type": row.get("location_type"),
                "seniority": row.get("seniority"),
                "job_family": row.get("job_family"),
                "domain": row.get("domain"),
                "required_skills": row.get("required_skills") if isinstance(row.get("required_skills"), list) else [],
            }
        )
    return fallback_rows

def _fallback_enriched_rows_from_stage_artifacts(run: PipelineRun) -> list[dict[str, Any]]:
    """Derive enriched-tab rows from enrich stage artifacts during in-flight runs."""
    stage_artifacts = _stage_artifacts_by_id(run)
    enrich_stage = dict(stage_artifacts.get("enrich") or {})
    sample_rows = list(enrich_stage.get("outputs_sample") or [])
    fallback_rows: list[dict[str, Any]] = []
    for row in sample_rows:
        if not isinstance(row, dict):
            continue
        job_url = str(row.get("job_url") or "").strip()
        if not job_url:
            continue
        fallback_rows.append(
            {
                "job_url": job_url,
                "title": str(row.get("title") or row.get("job_title") or job_url),
                "location_type": row.get("location_type"),
                "seniority": row.get("seniority"),
                "job_family": row.get("job_family"),
                "domain": row.get("domain"),
                "required_skills": row.get("required_skills") if isinstance(row.get("required_skills"), list) else [],
            }
        )
    return fallback_rows


def _stage_result_summary_rows(run: PipelineRun) -> list[dict[str, str]]:
    if not run.results_export_json:
        return []
    payload = decode_json_object_or_none(run.results_export_json)
    if payload is None:
        return []
    raw_summary = payload.get("stage_result_summary")
    if not isinstance(raw_summary, dict):
        return []

    stage_position = {stage_id: index for index, stage_id in enumerate(STAGE_SEQUENCE)}
    rows: list[dict[str, str]] = []
    for stage_id, raw_row in raw_summary.items():
        if not isinstance(raw_row, dict):
            continue
        trace_context = raw_row.get("trace_context")
        trace_block = trace_context if isinstance(trace_context, dict) else {}
        normalized_stage_id = str(stage_id or "").strip()
        rows.append(
            {
                "stage_id": normalized_stage_id,
                "status": str(raw_row.get("status") or ""),
                "policy_version": str(raw_row.get("policy_version") or ""),
                "trace_id": str(trace_block.get("trace_id") or ""),
                "span_id": str(trace_block.get("span_id") or ""),
                "parent_span_id": str(trace_block.get("parent_span_id") or ""),
            }
        )
    rows.sort(key=lambda row: (stage_position.get(row["stage_id"], 999), row["stage_id"]))
    return rows


def _event_payload(event: RunEvent) -> dict[str, Any]:
    raw_payload = getattr(event, "payload_json", None)
    if not raw_payload:
        return {}
    return decode_json_object_or_none(raw_payload) or {}


def _pipeline_outcome_surface(row: dict[str, Any]) -> dict[str, str]:
    pipeline_status = str(row.get("pipeline_status") or "")
    deterministic_outcome = str(row.get("deterministic_outcome") or "").strip()
    stage_owned_subreason = str(row.get("stage_owned_subreason") or "").strip()
    source_stage = str(row.get("source_stage") or "").strip()

    if source_stage == "cv_generation":
        if deterministic_outcome == "accepted":
            return {"label": "CV created", "badge_class": "badge-success"}
        if stage_owned_subreason == "review_required":
            return {"label": "CV review required", "badge_class": "badge-warning"}
        if stage_owned_subreason == "validation_failed":
            return {"label": "CV validation failed", "badge_class": "badge-error"}
        if stage_owned_subreason == "generation_failed":
            return {"label": "CV generation failed", "badge_class": "badge-error"}
        if stage_owned_subreason == "persistence_failed":
            return {"label": "CV persistence failed", "badge_class": "badge-error"}

    if source_stage == "cv_analysis":
        if stage_owned_subreason == "ready_for_generation":
            return {"label": "Ready for CV generation", "badge_class": "badge-info"}
        if stage_owned_subreason == "blocked_by_reranker_fit":
            return {"label": "Ranked, blocked by reranker fit", "badge_class": "badge-warning"}
        if stage_owned_subreason == "skipped_fit_gate":
            return {"label": "Skipped after CV analysis", "badge_class": "badge-warning"}
        if stage_owned_subreason == "analysis_failed":
            return {"label": "CV analysis failed", "badge_class": "badge-error"}

    return PIPELINE_OUTCOME_META.get(
        pipeline_status,
        {
            "label": pipeline_status or "Unknown pipeline outcome",
            "badge_class": "badge-info",
        },
    )


def _build_job_primary_label(
    *,
    title: str | None,
    company: str | None,
    location: str | None,
) -> str:
    canonical_title = str(title or "").strip() or "View Job"
    canonical_company = str(company or "").strip()
    canonical_location = str(location or "").strip()
    if canonical_company and canonical_location:
        return f"{canonical_title} ({canonical_company}, {canonical_location})"
    if canonical_company:
        return f"{canonical_title} ({canonical_company})"
    if canonical_location:
        return f"{canonical_title} ({canonical_location})"
    return canonical_title


def _job_metadata_by_url_from_results_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    metadata: dict[str, dict[str, str]] = {}
    for row in rows:
        url = str(row.get("job_url") or "").strip()
        if not url:
            continue
        metadata[url] = {
            "title": str(row.get("job_title") or "").strip(),
            "company": str(row.get("company") or "").strip(),
            "location": str(row.get("location") or "").strip(),
        }
    return metadata


def _bookmark_destination_for_request(request: Request, run_id: str) -> str:
    referer = str(request.headers.get("referer") or "").strip()
    if referer.startswith("http://") or referer.startswith("https://"):
        parsed = urlparse(referer)
        if parsed.path.startswith("/admin/runs/") or parsed.path.startswith("/admin/bookmarks"):
            suffix = f"?{parsed.query}" if parsed.query else ""
            return f"{parsed.path}{suffix}"
    return f"/admin/runs/{run_id}"


def _safe_admin_redirect_target(candidate: str | None, *, fallback: str) -> str:
    path = str(candidate or "").strip()
    if path.startswith("/admin/"):
        return path
    return fallback


def _coerce_positive_int(value: Any, *, default: int, minimum: int = 1, maximum: int = 200) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _paginate_rows(rows: list[dict[str, Any]], *, page: int, page_size: int) -> dict[str, int]:
    total = len(rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    current_page = min(max(1, page), total_pages)
    start = (current_page - 1) * page_size
    end = start + page_size
    return {
        "total": total,
        "total_pages": total_pages,
        "current_page": current_page,
        "start": start,
        "end": min(end, total),
    }


def _build_enriched_tab_context(
    run: PipelineRun,
    *,
    run_id: str,
    project: str,
    dataset: str,
    bq: Any,
    filter_name: str,
    query: str,
    page: int,
    page_size: int,
) -> dict[str, Any]:
    results_rows = _results_export_rows(run)
    enriched_jobs = list_run_structured_jobs(run_id, bq, project=project, dataset=dataset)
    if not enriched_jobs:
        enriched_jobs = _fallback_enriched_rows_from_stage_artifacts(run)
    if not enriched_jobs:
        enriched_jobs = _fallback_enriched_rows_from_results_export(results_rows)
    filter_results = list_filter_results_for_run(run_id, bq, project=project, dataset=dataset)
    if not filter_results and results_rows:
        # sqlite mode does not persist rule_filter_results; derive passed/rejected
        # flags from the results export so UI counters stay consistent.
        for row in results_rows:
            job_url = str(row.get("job_url") or "").strip()
            if not job_url:
                continue
            pipeline_status = str(row.get("pipeline_status") or "").strip()
            if pipeline_status == "deduplicated_before_enrichment":
                continue
            if pipeline_status == "rejected_after_enrichment":
                filter_results.append({"job_url": job_url, "passed": False, "reasons": []})
            else:
                filter_results.append({"job_url": job_url, "passed": True, "reasons": []})
    filter_results_by_job_url: dict[str, dict[str, Any]] = {
        str(row.get("job_url") or ""): row for row in filter_results if row.get("job_url")
    }
    enriched_job_urls = {str(job.get("job_url") or "") for job in enriched_jobs if job.get("job_url")}
    pre_enrichment_rejects = [
        row for row in filter_results
        if str(row.get("job_url") or "") not in enriched_job_urls and row.get("reasons")
    ]
    pipeline_outcomes_by_job_url: dict[str, dict[str, str | None]] = {
        str(row.get("job_url") or ""): {
            "status": str(row.get("pipeline_status") or ""),
            "label": _pipeline_outcome_surface(row)["label"],
            "badge_class": _pipeline_outcome_surface(row)["badge_class"],
            "detail": _format_pipeline_outcome_detail(row),
        }
        for row in results_rows
        if row.get("job_url")
    }
    deduplicated_before_enrichment = [
        {
            "job_url": row.get("job_url"),
            "job_title": row.get("job_title"),
            "reasons": row.get("reject_reasons") or [],
        }
        for row in results_rows
        if row.get("pipeline_status") == "deduplicated_before_enrichment"
    ]

    enriched_passed_count = sum(
        1 for job in enriched_jobs
        if filter_results_by_job_url.get(str(job.get("job_url") or ""), {}).get("passed") is True
    )
    enriched_rejected_count = sum(
        1 for job in enriched_jobs
        if filter_results_by_job_url.get(str(job.get("job_url") or ""), {}).get("passed") is False
    )

    normalized_filter = str(filter_name or "all").strip().lower()
    if normalized_filter not in {"all", "passed", "rejected", "unknown"}:
        normalized_filter = "all"
    normalized_query = str(query or "").strip().lower()
    filter_scoped_rows: list[dict[str, Any]] = []
    for job in enriched_jobs:
        job_url = str(job.get("job_url") or "")
        filter_result = filter_results_by_job_url.get(job_url, {})
        passed = filter_result.get("passed")
        if normalized_filter == "passed" and passed is not True:
            continue
        if normalized_filter == "rejected" and passed is not False:
            continue
        if normalized_filter == "unknown" and passed is not None:
            continue
        filter_scoped_rows.append(job)

    filtered_rows: list[dict[str, Any]] = []
    for job in filter_scoped_rows:
        if normalized_query:
            haystack = " ".join(
                str(job.get(field) or "").lower()
                for field in ("title", "domain", "job_family", "location_type", "seniority")
            )
            if normalized_query not in haystack:
                continue
        filtered_rows.append(job)

    nav_pager = _paginate_rows(filter_scoped_rows, page=page, page_size=page_size)
    pager = _paginate_rows(filtered_rows, page=page, page_size=page_size)
    visible_rows = filtered_rows[pager["start"]:pager["end"]]
    return {
        "run": run,
        "enriched_jobs": visible_rows,
        "filter_results_by_job_url": filter_results_by_job_url,
        "pipeline_outcomes_by_job_url": pipeline_outcomes_by_job_url,
        "pre_enrichment_rejects": pre_enrichment_rejects,
        "deduplicated_before_enrichment": deduplicated_before_enrichment,
        "enriched_passed_count": enriched_passed_count,
        "enriched_rejected_count": enriched_rejected_count,
        "enriched_total_count": len(enriched_jobs),
        "enriched_filtered_total_count": pager["total"],
        "enriched_current_page": nav_pager["current_page"],
        "enriched_total_pages": nav_pager["total_pages"],
        "enriched_page_size": page_size,
        "enriched_page_start": pager["start"] + 1 if pager["total"] else 0,
        "enriched_page_end": pager["end"],
        "enriched_filter": normalized_filter,
        "enriched_query": query,
        "enriched_has_prev": nav_pager["current_page"] > 1,
        "enriched_has_next": nav_pager["current_page"] < nav_pager["total_pages"],
    }


def _timeline_stage_download_for_event(event_stage: str) -> str | None:
    normalized = str(event_stage or "").strip()
    if not normalized:
        return None
    return TIMELINE_STAGE_DOWNLOADS.get(normalized)


def _timeline_event_allows_stage_download(event_stage: str) -> bool:
    return str(event_stage or "").strip() in TIMELINE_STAGE_DOWNLOADABLE_EVENTS


def _timeline_stage_label(event_stage: str) -> str:
    normalized = str(event_stage or "").strip()
    if not normalized:
        return "—"
    return TIMELINE_STAGE_LABELS.get(normalized, normalized.replace("_", " ").title())

def _timeline_human_job_label(job_url: str) -> str:
    normalized_url = str(job_url or "").strip()
    if not normalized_url:
        return "View Job"
    try:
        parsed = urlparse(normalized_url)
    except Exception:
        return normalized_url
    slug = str(parsed.path or "").rstrip("/").split("/")[-1]
    slug = unquote(slug)
    if not slug:
        return normalized_url
    slug = re.sub(r"-\d+$", "", slug)
    title_slug = slug
    company_slug = ""
    if "-at-" in slug:
        title_slug, company_slug = slug.rsplit("-at-", 1)
    title_slug = title_slug.strip("-")
    company_slug = company_slug.strip("-")
    suffix_patterns = {
        "-m-f-d": " (m/f/d)",
        "-m-f-x": " (m/f/x)",
        "-m-w-d": " (m/w/d)",
        "-w-m-d": " (w/m/d)",
        "-f-m-d": " (f/m/d)",
    }
    suffix = ""
    lowered_title = title_slug.lower()
    for pattern, normalized_suffix in suffix_patterns.items():
        if lowered_title.endswith(pattern):
            title_slug = title_slug[: -len(pattern)]
            suffix = normalized_suffix
            break
    title_tokens = [token for token in title_slug.split("-") if token]
    company_tokens = [token for token in company_slug.split("-") if token]
    title_label = " ".join(token.capitalize() for token in title_tokens).strip() or "View Job"
    company_label = " ".join(token.capitalize() for token in company_tokens).strip()
    if suffix:
        title_label = f"{title_label}{suffix}"
    if company_label:
        return f"{title_label} ({company_label})"
    return title_label

def _timeline_job_link_html(job_url: str) -> str | None:
    normalized_url = str(job_url or "").strip()
    if not normalized_url:
        return None
    label = _timeline_human_job_label(normalized_url)
    return (
        f"<a href=\"{html.escape(normalized_url, quote=True)}\" "
        f"target=\"_blank\" rel=\"noopener noreferrer\">{html.escape(label)}</a>"
    )

def _timeline_semantic_outcome(event: RunEvent, payload: dict[str, Any]) -> str:
    stage = str(event.stage or "").strip()
    deterministic_outcome = str(payload.get("deterministic_outcome") or "").strip().lower()
    stage_owned_subreason = str(payload.get("stage_owned_subreason") or "").strip().lower()
    if stage in {"layer4_cv_validation_failed", "cv_validation_failed"}:
        if deterministic_outcome == "rejected" and stage_owned_subreason == "validation_failed":
            return "expected_rejection"
        return "unexpected_failure"
    if stage_owned_subreason == "review_required" or deterministic_outcome == "review_required":
        return "requires_review"
    if stage in {"pipeline_complete", "pipeline_compute_complete"}:
        return "summary"
    return "normal_progress"
def _timeline_stage_summary_message(
    event: RunEvent,
    stage_artifacts_by_id: dict[str, dict[str, Any]],
) -> str:
    payload = _event_payload(event)
    def _format_message(prefix: str, fields: list[tuple[str, Any]]) -> str:
        compact_fields: list[tuple[str, Any]] = []
        for label, value in fields:
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            compact_fields.append((label, value))
        if not compact_fields:
            return f"{prefix}."
        details = ", ".join(f"{label} {value}" for label, value in compact_fields)
        return f"{prefix}: {details}."

    def _resolve_concurrency_value(*values: Any) -> str | None:
        for value in values:
            if value is None:
                continue
            try:
                return str(max(1, int(value)))
            except (TypeError, ValueError):
                continue
        return None
    if event.stage == "enrich_heartbeat":
        phase = str(payload.get("phase") or "").strip()
        fresh_total = payload.get("fresh_jobs_total")
        reused_total = payload.get("reused_jobs_total")
        concurrency = _resolve_concurrency_value(
            payload.get("enrich_concurrency_effective"),
            payload.get("concurrency_effective"),
            payload.get("configured_concurrency"),
        )
        if phase == "batch_start":
            return _format_message(
                "Enrich starting",
                [
                    ("fresh", fresh_total),
                    ("reused", reused_total),
                    ("concurrency", concurrency),
                ],
            )
        if phase == "batch_progress":
            return _format_message(
                "Enrich in progress",
                [
                    ("fresh", fresh_total),
                    ("reused", reused_total),
                    ("concurrency", concurrency),
                ],
            )
        if phase == "batch_done":
            fresh_rows_total = payload.get("fresh_rows_total")
            return _format_message(
                "Enrich complete",
                [
                    ("fresh rows", fresh_rows_total),
                    ("fresh", fresh_total),
                    ("reused", reused_total),
                    ("concurrency", concurrency),
                ],
            )
        return _format_message("Enrich in progress", [("concurrency", concurrency)])
    raw_payload_output = payload.get("output_snapshot")
    payload_output: dict[str, Any] = (
        dict(raw_payload_output)
        if isinstance(raw_payload_output, dict)
        else {}
    )
    if event.stage == "layer4_cv_analysis_invoked":
        ranked_jobs = payload_output.get("ranked_jobs", payload.get("ranked_jobs"))
        cv_analysis_concurrency = _resolve_concurrency_value(
            payload_output.get("cv_analysis_concurrency_effective"),
            payload_output.get("cv_analysis_concurrency_configured"),
            payload.get("cv_analysis_concurrency_effective"),
            payload.get("cv_analysis_concurrency_configured"),
        )
        details = []
        if ranked_jobs is not None:
            details.append(f"{ranked_jobs} ranked job(s)")
        if cv_analysis_concurrency is not None:
            details.append(f"concurrency {cv_analysis_concurrency}")
        if details:
            return f"CV analysis invoked: {', '.join(details)}"
    if event.stage == "layer4_cv_generation_started":
        cv_generation_concurrency = _resolve_concurrency_value(
            payload_output.get("cv_generation_concurrency_effective"),
            payload_output.get("configured_concurrency"),
        )
        match = re.search(r"\[item\s+(\d+)/(\d+)\]", str(event.message or ""), flags=re.IGNORECASE)
        item_index = match.group(1) if match else None
        item_total = match.group(2) if match else None
        job_url = str(payload.get("job_url") or "").strip()
        if not job_url:
            match_url = re.search(r"for\s+(https?://\S+)", str(event.message or ""))
            if match_url:
                job_url = match_url.group(1).rstrip(").,")
        return _format_message(
            "CV generation started",
            [
                ("item", f"{item_index}/{item_total}" if item_index and item_total else None),
                ("job", job_url or None),
                ("concurrency", cv_generation_concurrency),
            ],
        )
    if event.stage == "layer4_cv_generation_result":
        cv_generation_concurrency = _resolve_concurrency_value(
            payload_output.get("cv_generation_concurrency_effective"),
            payload_output.get("configured_concurrency"),
        )
        outcome_match = re.search(r":\s*([a-z_]+)\s*(?:\(concurrency=|\Z)", str(event.message or ""), flags=re.IGNORECASE)
        outcome = outcome_match.group(1).replace("_", " ") if outcome_match else None
        if not outcome:
            raw_outcome = str(payload.get("deterministic_outcome") or payload_output.get("status") or "").strip()
            outcome = raw_outcome.replace("_", " ") if raw_outcome else None
        reuse_status = str(
            payload_output.get("reuse_status")
            or payload_output.get("reuse_status_flat")
            or ""
        ).strip().lower()
        cache_label = None
        compute_label = None
        reused_cv_version_id = str(payload_output.get("reused_cv_version_id") or "").strip()
        if not reuse_status:
            # Backward-compatible default for older/missing payload fields:
            # if a source version exists we reused, otherwise treat as fresh compute.
            reuse_status = "reused_exact_match" if reused_cv_version_id else "fresh_compute"
        if reuse_status == "reused_exact_match":
            cache_label = "hit (exact)"
            compute_label = "reused"
        elif reuse_status == "fresh_compute":
            cache_label = "miss"
            compute_label = "fresh"
        reason_code = str(payload_output.get("review_required_reason_code") or "").strip()
        validation_evidence_fingerprint = str(payload_output.get("validation_evidence_fingerprint") or "").strip()
        job_url = str(payload.get("job_url") or "").strip()
        if not job_url:
            match_url = re.search(r"for\s+(https?://\S+)", str(event.message or ""))
            if match_url:
                job_url = match_url.group(1).rstrip(").,")
        return _format_message(
            "CV generation result",
            [
                ("job", job_url or None),
                ("outcome", outcome),
                ("cache", cache_label),
                ("compute", compute_label),
                ("reason", reason_code or None),
                ("evidence fp", validation_evidence_fingerprint[:12] if validation_evidence_fingerprint else None),
                ("source version", reused_cv_version_id or None),
                ("concurrency", cv_generation_concurrency),
            ],
        )
    if event.stage == "layer4_cv_generation_reused":
        cv_generation_concurrency = _resolve_concurrency_value(
            payload_output.get("cv_generation_concurrency_effective"),
            payload_output.get("configured_concurrency"),
        )
        reused_cv_version_id = str(payload_output.get("reused_cv_version_id") or "").strip()
        job_url = str(payload.get("job_url") or "").strip()
        return _format_message(
            "CV generation reused",
            [
                ("job", job_url or None),
                ("source version", reused_cv_version_id or None),
                ("concurrency", cv_generation_concurrency),
            ],
        )
    if event.stage == "synonym_proposal_triage_completed":
        return _format_message(
            "Synonym triage complete",
            [
                ("scope", "proposals"),
                ("triaged", payload.get("triaged_count")),
                ("reused", payload.get("reused_count")),
                ("fallback", payload.get("fallback_count")),
                ("skipped", payload.get("skipped_count")),
                ("failed", payload.get("failed_count")),
            ],
        )
    if event.stage == "synonym_proposal_auto_apply_completed":
        return _format_message(
            "Synonym auto-apply complete",
            [
                ("scope", "proposals"),
                ("applied", payload.get("applied_count")),
                ("skipped", payload.get("skipped_count")),
                ("failed", payload.get("failed_count")),
            ],
        )
    if event.stage == "cv_review_batch_action":
        return _format_message(
            "CV review batch decision",
            [
                ("action", payload.get("action")),
                ("applied", payload.get("applied")),
                ("skipped", payload.get("skipped")),
                ("failed", payload.get("failed")),
            ],
        )
    if event.stage == "cv_review_required":
        return _format_message(
            "Run paused",
            [
                ("review-required items pending operator action", payload.get("review_required_total")),
                ("auto-accepted", payload.get("auto_accepted")),
            ],
        )
    if event.stage == "cv_review_completed":
        return _format_message(
            "CV review complete",
            [
                ("resolved", payload.get("resolved_count")),
                ("run marked succeeded", payload.get("run_marked_succeeded")),
            ],
        )
    stage_id = _timeline_stage_download_for_event(event.stage)
    if not stage_id:
        return event.message
    artifact = stage_artifacts_by_id.get(stage_id) or {}
    raw_outputs = artifact.get("output_counts")
    outputs: dict[str, Any] = dict(raw_outputs) if isinstance(raw_outputs, dict) else {}
    raw_decision = artifact.get("decision_summary")
    decision: dict[str, Any] = dict(raw_decision) if isinstance(raw_decision, dict) else {}
    if event.stage == "layer1_normalize":
        kept = outputs.get("normalized_jobs")
        raw_jobs = outputs.get("raw_jobs") or decision.get("raw_jobs")
        removed = outputs.get("deduplicated_jobs")
        if kept is not None and removed is not None:
            raw_label = f" of {raw_jobs}" if raw_jobs is not None else ""
            return f"Normalize complete: kept {kept}{raw_label} jobs, removed {removed} duplicate(s)"
    if event.stage == "layer1_jobs":
        enriched = outputs.get("enriched_jobs")
        rejected = outputs.get("pre_enrichment_rejected_jobs")
        fresh = decision.get("fresh_rows")
        reused = decision.get("reused_rows")
        details = []
        if enriched is not None:
            details.append(f"{enriched} enriched")
        if rejected is not None:
            details.append(f"{rejected} rejected before enrich")
        if fresh is not None:
            details.append(f"fresh {fresh}")
        if reused is not None:
            details.append(f"reused {reused}")
        if details:
            return _format_message(
                "Enrich complete",
                [
                    ("enriched", enriched),
                    ("rejected before enrich", rejected),
                    ("fresh", fresh),
                    ("reused", reused),
                ],
            )
        # Fallback when stage artifacts are absent: normalize legacy inline metrics.
        raw_message = str(event.message or "")
        fresh_match = re.search(r"\bfresh\s*=\s*(\d+)", raw_message, flags=re.IGNORECASE)
        reused_match = re.search(r"\breused\s*=\s*(\d+)", raw_message, flags=re.IGNORECASE)
        if fresh_match or reused_match:
            return _format_message(
                "Enrich complete",
                [
                    ("enriched", enriched),
                    ("rejected before enrich", rejected),
                    ("fresh", int(fresh_match.group(1)) if fresh_match else None),
                    ("reused", int(reused_match.group(1)) if reused_match else None),
                ],
            )
    if event.stage == "layer3_filter":
        passed = outputs.get("passed_jobs")
        rejected = outputs.get("candidate_filter_rejected_jobs")
        if passed is not None and rejected is not None:
            return f"Rule filter complete: {passed} passed, {rejected} rejected"
    if event.stage == "layer3_shortlist":
        shortlisted = outputs.get("shortlisted_jobs") or outputs.get("scoring_shortlist_jobs")
        backfilled = outputs.get("backfilled_jobs") or decision.get("backfilled_jobs")
        details = []
        if shortlisted is not None:
            details.append(f"{shortlisted} shortlisted")
        if backfilled is not None:
            details.append(f"{backfilled} backfilled")
        if details:
            return _format_message(
                "Shortlist complete",
                [
                    ("shortlisted", shortlisted),
                    ("backfilled", backfilled),
                ],
            )
    if event.stage == "layer3_ranking":
        ranked = outputs.get("ranked_jobs")
        ranking_concurrency = _resolve_concurrency_value(
            payload_output.get("ranking_concurrency_effective"),
            outputs.get("ranking_concurrency_effective"),
        )
        raw_distribution = decision.get("label_distribution")
        distribution: dict[str, Any] = dict(raw_distribution) if isinstance(raw_distribution, dict) else {}
        raw_reuse_metrics = decision.get("reuse_metrics")
        reuse_metrics: dict[str, Any] = dict(raw_reuse_metrics) if isinstance(raw_reuse_metrics, dict) else {}
        if not reuse_metrics:
            output_summary = dict(payload_output.get("cv_analysis_decision_summary") or {})
            candidate = output_summary.get("reuse_metrics")
            if isinstance(candidate, dict):
                reuse_metrics = dict(candidate)
        if not reuse_metrics:
            output_summary = dict(payload_output.get("ranking_decision_summary") or {})
            candidate = output_summary.get("reuse_metrics")
            if isinstance(candidate, dict):
                reuse_metrics = dict(candidate)
        if not reuse_metrics:
            fallback_reused = payload_output.get("reused_ai_scores")
            fallback_fresh = payload_output.get("fresh_ai_scores")
            if fallback_reused is not None or fallback_fresh is not None:
                reuse_metrics = {
                    "reused_ai_scores": fallback_reused,
                    "fresh_ai_scores": fallback_fresh,
                }
        details = []
        if ranked is not None:
            details.append(f"{ranked} ranked")
        if ranking_concurrency is not None:
            details.append(f"concurrency {ranking_concurrency}")
        for key, label in (("strong_count", "strong"), ("stretch_count", "stretch"), ("skip_count", "skip")):
            count = distribution.get(key)
            if count is not None:
                details.append(f"{label} {count}")
        if details:
            return _format_message(
                "Ranking complete",
                [
                    ("ranked", ranked),
                    ("concurrency", ranking_concurrency),
                    ("strong", distribution.get("strong_count")),
                    ("stretch", distribution.get("stretch_count")),
                    ("skip", distribution.get("skip_count")),
                    ("reused", reuse_metrics.get("reused_ai_scores")),
                    ("fresh", reuse_metrics.get("fresh_ai_scores")),
                ],
            )
    if event.stage == "layer3_ai_score":
        ai_scored = payload_output.get("ai_scored_jobs")
        ranking_concurrency = _resolve_concurrency_value(payload_output.get("ranking_concurrency_effective"))
        details = []
        if ai_scored is not None:
            details.append(f"{ai_scored} jobs")
        if ranking_concurrency is not None:
            details.append(f"concurrency {ranking_concurrency}")
        if details:
            return _format_message(
                "AI scored",
                [
                    ("jobs", ai_scored),
                    ("concurrency", ranking_concurrency),
                ],
            )
    if event.stage == "layer4_cv_analysis":
        ready = outputs.get("ready_for_generation", payload_output.get("ready_for_generation"))
        blocked = outputs.get("blocked_by_reranker_fit", payload_output.get("blocked_by_reranker_fit"))
        skipped = outputs.get("skipped_fit_gate", payload_output.get("skipped_fit_gate"))
        failed = outputs.get("analysis_failed", payload_output.get("analysis_failed"))
        cv_analysis_concurrency = _resolve_concurrency_value(
            payload_output.get("cv_analysis_concurrency_effective"),
            outputs.get("cv_analysis_concurrency_effective"),
        )
        raw_reuse_metrics = decision.get("reuse_metrics")
        reuse_metrics: dict[str, Any] = dict(raw_reuse_metrics) if isinstance(raw_reuse_metrics, dict) else {}
        if not reuse_metrics:
            fallback_reused = payload_output.get("reused_analysis_rows")
            fallback_fresh = payload_output.get("fresh_analysis_rows")
            if fallback_reused is not None or fallback_fresh is not None:
                reuse_metrics = {
                    "reused_analysis_rows": fallback_reused,
                    "fresh_analysis_rows": fallback_fresh,
                }
        if ready is not None and blocked is not None and skipped is not None and failed is not None:
            details: list[tuple[str, Any]] = [
                ("ready", ready),
                ("blocked", blocked),
                ("skipped", skipped),
                ("failed", failed),
                ("reused", reuse_metrics.get("reused_analysis_rows")),
                ("fresh", reuse_metrics.get("fresh_analysis_rows")),
            ]
            if cv_analysis_concurrency is not None:
                details.append(("concurrency", cv_analysis_concurrency))
            return _format_message(
                "CV analysis complete",
                details,
            )
    if event.stage == "layer4_cv_validation_failed":
        job_url = str(payload.get("job_url") or "").strip()
        semantic_outcome = _timeline_semantic_outcome(event, payload)
        if semantic_outcome == "expected_rejection":
            qualifier = "expected policy rejection"
        else:
            qualifier = "unexpected; investigate"
        if job_url:
            return f"CV validation failed ({qualifier}) for {job_url}"
        return f"CV validation failed ({qualifier})"
    if event.stage in {"pipeline_complete", "pipeline_compute_complete"}:
        accepted = outputs.get("accepted")
        review_required = outputs.get("review_required")
        validation_failed = outputs.get("validation_failed")
        generation_failed = outputs.get("generation_failed")
        persistence_failed = outputs.get("persistence_failed")
        reused_cv_generation = None
        fresh_cv_generation = None
        if accepted is None:
            accepted = payload_output.get("quality_summary", {}).get("acceptance_review_failure", {}).get("accepted")
        if review_required is None:
            review_required = payload_output.get("quality_summary", {}).get("acceptance_review_failure", {}).get("review_required")
        if validation_failed is None:
            validation_failed = payload_output.get("quality_summary", {}).get("acceptance_review_failure", {}).get("validation_failed")
        if generation_failed is None:
            generation_failed = payload_output.get("quality_summary", {}).get("acceptance_review_failure", {}).get("generation_failed")
        if persistence_failed is None:
            persistence_failed = payload_output.get("quality_summary", {}).get("acceptance_review_failure", {}).get("persistence_failed")
        late_stage_reuse_metrics = payload_output.get("late_stage_reuse_metrics")
        if isinstance(late_stage_reuse_metrics, dict):
            cv_generation_metrics = late_stage_reuse_metrics.get("cv_generation")
            if isinstance(cv_generation_metrics, dict):
                reused_cv_generation = cv_generation_metrics.get("reused_rows")
                fresh_cv_generation = cv_generation_metrics.get("fresh_rows")
        details = []
        if accepted is not None:
            details.append(f"{accepted} accepted")
        if review_required is not None:
            details.append(f"{review_required} review required")
        if validation_failed is not None:
            details.append(f"{validation_failed} validation failed")
        if generation_failed is not None:
            details.append(f"{generation_failed} generation failed")
        if persistence_failed is not None:
            details.append(f"{persistence_failed} persistence failed")
        if reused_cv_generation is not None:
            details.append(f"{reused_cv_generation} reused")
        if fresh_cv_generation is not None:
            details.append(f"{fresh_cv_generation} fresh")
        if details:
            return f"CV generation complete: {', '.join(details)}"
    return event.message

def _timeline_stage_summary_message_html(
    event: RunEvent,
    *,
    message_text: str,
) -> str | None:
    payload = _event_payload(event)
    raw_payload_output = payload.get("output_snapshot")
    payload_output: dict[str, Any] = (
        dict(raw_payload_output)
        if isinstance(raw_payload_output, dict)
        else {}
    )
    job_url = str(payload.get("job_url") or "").strip()
    if not job_url:
        match_url = re.search(r"job\s+(https?://\S+)", str(message_text or ""))
        if match_url:
            job_url = match_url.group(1).rstrip(").,")
    job_link = _timeline_job_link_html(job_url)
    if event.stage == "layer4_cv_generation_started" and job_link:
        item_match = re.search(r"item\s+(\d+/\d+)", str(message_text or ""), flags=re.IGNORECASE)
        concurrency_match = re.search(r"concurrency\s+(\d+)", str(message_text or ""), flags=re.IGNORECASE)
        item_value = item_match.group(1) if item_match else None
        concurrency_value = concurrency_match.group(1) if concurrency_match else None
        details: list[str] = []
        if item_value:
            details.append(f"item {html.escape(item_value)}")
        details.append(f"job {job_link}")
        if concurrency_value:
            details.append(f"concurrency {html.escape(concurrency_value)}")
        return f"CV generation started: {', '.join(details)}."
    if event.stage == "layer4_cv_generation_result" and job_link:
        outcome_match = re.search(r"outcome\s+([^,]+)", str(message_text or ""), flags=re.IGNORECASE)
        cache_match = re.search(r"cache\s+([^,]+)", str(message_text or ""), flags=re.IGNORECASE)
        compute_match = re.search(r"compute\s+([^,]+)", str(message_text or ""), flags=re.IGNORECASE)
        reason_match = re.search(r"reason\s+([^,]+)", str(message_text or ""), flags=re.IGNORECASE)
        evidence_match = re.search(r"evidence fp\s+([a-f0-9]{6,64})", str(message_text or ""), flags=re.IGNORECASE)
        source_version = str(payload_output.get("reused_cv_version_id") or "").strip()
        concurrency_match = re.search(r"concurrency\s+(\d+)", str(message_text or ""), flags=re.IGNORECASE)
        details: list[str] = [f"job {job_link}"]
        if outcome_match:
            details.append(f"outcome {html.escape(outcome_match.group(1).strip())}")
        if cache_match:
            details.append(f"cache {html.escape(cache_match.group(1).strip())}")
        if compute_match:
            details.append(f"compute {html.escape(compute_match.group(1).strip())}")
        if reason_match:
            details.append(f"reason {html.escape(reason_match.group(1).strip())}")
        if evidence_match:
            details.append(f"evidence fingerprint {html.escape(evidence_match.group(1).strip())}")
        if source_version:
            details.append(f"source version <code>{html.escape(source_version)}</code>")
        if concurrency_match:
            details.append(f"concurrency {html.escape(concurrency_match.group(1))}")
        return f"CV generation result: {', '.join(details)}."
    return None

def _collapse_timeline_noise(events: list[RunEvent]) -> list[tuple[RunEvent, int]]:
    """Collapse repeated informational noise rows."""
    def _timeline_noise_fingerprint(event: RunEvent) -> tuple[str, str, str]:
        stage = str(event.stage or "").strip()
        level = str(event.level or "").strip().lower()
        message = str(event.message or "").strip()
        if stage == "enrich_heartbeat":
            payload = _event_payload(event)
            phase = str(payload.get("phase") or "").strip()
            if not phase:
                return (stage, level, "Enrich in progress")
            enrich_concurrency = payload.get("enrich_concurrency_effective")
            heartbeat_interval = payload.get("heartbeat_interval_secs")
            fresh_jobs_total = payload.get("fresh_jobs_total")
            reused_jobs_total = payload.get("reused_jobs_total")
            fresh_rows_total = payload.get("fresh_rows_total")
            return (
                stage,
                level,
                _stable_sha256_json(
                    {
                        "phase": phase,
                        "fresh_jobs_total": fresh_jobs_total,
                        "reused_jobs_total": reused_jobs_total,
                        "fresh_rows_total": fresh_rows_total,
                        "heartbeat_interval_secs": heartbeat_interval,
                        "enrich_concurrency_effective": enrich_concurrency,
                    }
                ),
            )
        return (stage, level, message)

    collapsed: list[tuple[RunEvent, int]] = []
    last_triage_fingerprint: str | None = None
    last_triage_index: int | None = None
    for event in events:
        stage = str(event.stage or "").strip()
        fingerprint = _timeline_noise_fingerprint(event)
        if collapsed and stage != "synonym_proposal_triage_completed":
            prev_event, prev_count = collapsed[-1]
            if fingerprint == _timeline_noise_fingerprint(prev_event):
                collapsed[-1] = (prev_event, prev_count + 1)
                if stage == "synonym_proposal_triage_completed":
                    last_triage_index = len(collapsed) - 1
                continue
        if stage == "synonym_proposal_triage_completed":
            payload = _event_payload(event)
            fingerprint = _stable_sha256_json(
                {
                    "triaged_count": payload.get("triaged_count"),
                    "reused_count": payload.get("reused_count"),
                    "fresh_count": payload.get("fresh_count"),
                    "fallback_count": payload.get("fallback_count"),
                    "skipped_count": payload.get("skipped_count"),
                    "failed_count": payload.get("failed_count"),
                    "reuse_reason": payload.get("reuse_reason"),
                    "provider": payload.get("provider"),
                    "model": payload.get("model"),
                    "wire_api": payload.get("wire_api"),
                }
            )
            if last_triage_fingerprint == fingerprint:
                if last_triage_index is not None:
                    kept_event, repeat_count = collapsed[last_triage_index]
                    collapsed[last_triage_index] = (kept_event, repeat_count + 1)
                continue
            last_triage_fingerprint = fingerprint
            collapsed.append((event, 1))
            last_triage_index = len(collapsed) - 1
        else:
            last_triage_fingerprint = None
            last_triage_index = None
            collapsed.append((event, 1))
    return collapsed


def _timeline_show_repeat_suffix(event: RunEvent) -> bool:
    """Keep primary timeline message invariant for repeatable progress pulses."""
    stage = str(event.stage or "").strip()
    if stage == "enrich_heartbeat":
        return False
    return True


def _dedupe_timeline_semantic_overlaps(
    collapsed_events: list[tuple[RunEvent, int]],
) -> list[tuple[RunEvent, int]]:
    """Drop duplicate semantic rows when heartbeat completion is mirrored by stage completion."""
    if not collapsed_events:
        return []
    reused_result_keys: set[tuple[str, str]] = set()
    deduped: list[tuple[RunEvent, int]] = []
    for event, repeat_count in collapsed_events:
        stage = str(event.stage or "").strip()
        payload = _event_payload(event)
        output_snapshot = payload.get("output_snapshot")
        outputs = dict(output_snapshot) if isinstance(output_snapshot, dict) else {}
        job_url = str(payload.get("job_url") or "").strip()
        reused_source_id = str(outputs.get("reused_cv_version_id") or "").strip()
        if (
            stage == "layer4_cv_generation_result"
            and str(outputs.get("reuse_status") or "").strip().lower() == "reused_exact_match"
            and job_url
        ):
            reused_result_keys.add((job_url, reused_source_id))
        if stage == "layer4_cv_generation_reused" and job_url:
            if (job_url, reused_source_id) in reused_result_keys:
                continue
        if deduped and stage == "layer1_jobs":
            prev_event, _prev_repeat_count = deduped[-1]
            prev_stage = str(prev_event.stage or "").strip()
            if prev_stage == "enrich_heartbeat":
                prev_payload = _event_payload(prev_event)
                prev_phase = str(prev_payload.get("phase") or "").strip()
                if prev_phase == "batch_done":
                    prev_fresh = prev_payload.get("fresh_jobs_total")
                    prev_reused = prev_payload.get("reused_jobs_total")
                    current_fresh = outputs.get("fresh_rows")
                    current_reused = outputs.get("reused_rows")
                    if prev_fresh == current_fresh and prev_reused == current_reused:
                        deduped.pop()
        deduped.append((event, repeat_count))
    return deduped

def _stage_download_label(stage_id: str | None) -> str:
    if not stage_id:
        return "Download stage JSON"
    return STAGE_DOWNLOAD_LABELS.get(stage_id, f"Download {stage_id.replace('_', ' ').title()} JSON")




class TriggerRequest(BaseModel):
    jobs_path: str = "data/sample_jobs.json"
    config_path: str = "config/env.yaml"
    triggered_by: str = "admin"
    config_overrides: dict[str, Any] = {}
    run_mode: str = "run_all"

    @field_validator("jobs_path")
    @classmethod
    def jobs_path_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("jobs_path must not be empty")
        return v

    @field_validator("run_mode")
    @classmethod
    def run_mode_supported(cls, v: str) -> str:
        normalized = str(v or "").strip()
        if normalized not in {"run_all", "manual_staged"}:
            raise ValueError("run_mode must be 'run_all' or 'manual_staged'")
        return normalized

class SettingUpdate(BaseModel):
    value: Any
    updated_by: str = "admin"


class BulkRunActionRequest(BaseModel):
    run_ids: list[str]

    @field_validator("run_ids")
    @classmethod
    def run_ids_not_empty(cls, v: list[str]) -> list[str]:
        normalized = [str(item or "").strip() for item in v]
        filtered = [item for item in normalized if item]
        if not filtered:
            raise ValueError("run_ids must include at least one run id")
        deduped = list(dict.fromkeys(filtered))
        return deduped

class CvReviewActionRequest(BaseModel):
    review_item_id: str | None = None
    job_url: str
    action: str
    actor: str = "admin"
    note: str | None = None

class SynonymBatchDecision(BaseModel):
    proposal_id: str
    action: str

class SynonymBatchActionRequest(BaseModel):
    decisions: list[SynonymBatchDecision]
    acted_by: str = "admin"
    note: str | None = None


def _can_cancel_run(run: PipelineRun) -> bool:
    return run.status in {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.AWAITING_CONTINUE}


def _can_archive_run(run: PipelineRun) -> bool:
    return run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED} and run.archived_at is None


def _can_unarchive_run(run: PipelineRun) -> bool:
    return run.archived_at is not None


def create_app(bq: Any, project: str, dataset: str, redis_url: str) -> FastAPI:
    global _CP_STORE
    _CP_STORE = ControlPlaneStore(
        bq=bq,
        project=project,
        dataset=dataset,
        insert_run_fn=insert_run,
        update_run_queue_job_id_fn=update_run_queue_job_id,
        update_run_orchestration_binding_fn=update_run_orchestration_binding,
        get_run_fn=bq_store_module.get_run,
        list_runs_fn=bq_store_module.list_runs,
        get_events_fn=bq_store_module.get_events,
        update_run_status_fn=bq_store_module.update_run_status,
        update_run_checkpoint_fn=bq_store_module.update_run_checkpoint,
        request_run_cancel_fn=bq_store_module.request_run_cancel,
        archive_run_fn=bq_store_module.archive_run,
        unarchive_run_fn=bq_store_module.unarchive_run,
        list_cvs_for_run_fn=bq_store_module.list_cvs_for_run,
        get_cv_markdown_fn=bq_store_module.get_cv_markdown,
        list_run_structured_jobs_fn=bq_store_module.list_run_structured_jobs,
        list_filter_results_for_run_fn=bq_store_module.list_filter_results_for_run,
        get_pipeline_runs_schema_status_fn=bq_store_module.get_pipeline_runs_schema_status,
        append_event_fn=bq_store_module.append_event,
        update_run_effective_settings_fn=bq_store_module.update_run_effective_settings,
        update_run_synonym_proposals_fn=bq_store_module.update_run_synonym_proposals,
        update_run_cv_generation_debug_fn=bq_store_module.update_run_cv_generation_debug,
        update_run_stage_transition_artifacts_fn=bq_store_module.update_run_stage_transition_artifacts,
        insert_cv_version_row_fn=bq_store_module.insert_cv_version_row,
    )
    app = FastAPI(title="FitCV Admin Control Plane")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    runtime_settings_schema = settings_schema_with_runtime_defaults(load_config())
    schema_by_key = {entry["key"]: entry for entry in runtime_settings_schema}
    metadata_only_keys = metadata_only_settings_keys()
    editable_keys = editable_settings_keys()
    timing_schema_entries: dict[str, dict[str, Any]] = {
        str(entry.get("key") or ""): entry
        for entry in runtime_settings_schema
        if str(entry.get("group") or "") == "timing"
    }
    canonical_runtime_throughput_keys: list[str] = [
        key
        for key in SETTINGS_SECTIONS["timing"]
        if key.startswith("stage_runtime.")
    ]
    compatibility_runtime_alias_keys: list[str] = [
        key
        for key in SETTINGS_SECTIONS["timing"]
        if str(timing_schema_entries.get(key, {}).get("compatibility_alias_for") or "").strip()
    ]
    canonical_compatibility_aliases: dict[str, list[str]] = {}
    for schema_entry in runtime_settings_schema:
        legacy_key = str(schema_entry.get("key") or "").strip()
        canonical_key = str(schema_entry.get("compatibility_alias_for") or "").strip()
        if canonical_key and legacy_key:
            canonical_compatibility_aliases.setdefault(canonical_key, []).append(legacy_key)
    runtime_throughput_stage_titles = {
        "enrich": "Enrichment",
        "ranking": "Ranking",
        "cv_analysis": "CV Analysis",
        "cv_generation": "CV Generation",
    }
    hidden_deprecated_keys = hidden_deprecated_settings_keys()

    def _filter_canonical_settings_payload(settings: dict[str, Any]) -> dict[str, Any]:
        """Drop throughput compatibility aliases from persisted settings payloads."""
        return {
            key: value
            for key, value in settings.items()
            if key in editable_keys and key not in compatibility_runtime_alias_keys
        }

    def require_run_or_404(run_id: str, *, detail: str = "Run not found") -> PipelineRun:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail=detail)
        return run

    def _reconcile_orphaned_running_run(run: PipelineRun) -> PipelineRun:
        """Repair RUNNING rows if their RQ job disappeared or already terminated."""
        if run.status != RunStatus.RUNNING:
            return run
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        started_at = getattr(run, "started_at", None)
        run_age_seconds = 0.0
        if isinstance(started_at, datetime.datetime):
            started_at_utc = started_at if started_at.tzinfo else started_at.replace(tzinfo=datetime.timezone.utc)
            run_age_seconds = max(0.0, (now_utc - started_at_utc).total_seconds())
        completed_stages = list(getattr(run, "completed_stages", None) or [])
        if run_age_seconds >= 300 and not completed_stages:
            events = get_events(run.run_id, bq, project=project, dataset=dataset)
            if not events:
                update_run_status(
                    run.run_id,
                    RunStatus.FAILED,
                    bq,
                    project=project,
                    dataset=dataset,
                    finished_at=now_utc,
                    error_message="Run remained RUNNING without progress/events for >5 minutes (orphaned startup).",
                )
                append_event(
                    RunEvent(
                        run_id=run.run_id,
                        event_id=str(uuid.uuid4()),
                        stage="run_reconciled",
                        level="warning",
                        message="Run reconciled from orphaned startup (no progress/events >5 minutes)",
                        created_at=now_utc,
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
                return get_run(run.run_id, bq, project=project, dataset=dataset) or run
        queue_job_id = str(getattr(run, "queue_job_id", "") or "").strip()
        if not queue_job_id:
            return run
        try:
            rq_status = str(get_queue_job_status(queue_job_id, redis_url=redis_url) or "").strip().lower()
            if rq_status in {"queued", "started", "deferred"}:
                return run
            if rq_status in {"finished", "failed", "stopped", "canceled", "cancelled"}:
                update_run_status(
                    run.run_id,
                    RunStatus.FAILED if rq_status != "finished" else RunStatus.SUCCEEDED,
                    bq,
                    project=project,
                    dataset=dataset,
                    finished_at=datetime.datetime.now(datetime.timezone.utc),
                    error_message=(
                        None if rq_status == "finished"
                        else f"Queue job {queue_job_id} ended with status={rq_status} before lifecycle finalization"
                    ),
                )
                append_event(
                    RunEvent(
                        run_id=run.run_id,
                        event_id=str(uuid.uuid4()),
                        stage="run_reconciled",
                        level="warning" if rq_status != "finished" else "info",
                        message=f"Run reconciled from orphaned running state (queue status={rq_status})",
                        created_at=datetime.datetime.now(datetime.timezone.utc),
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
                return get_run(run.run_id, bq, project=project, dataset=dataset) or run
            if rq_status == "missing" and queue_job_id.startswith("inline-"):
                # Inline queue state is process-local memory; another process may
                # legitimately report "missing" while the run continues elsewhere.
                return run
            if rq_status == "missing":
                raise LookupError("queue job missing")
            return run
        except LookupError:
            update_run_status(
                run.run_id,
                RunStatus.FAILED,
                bq,
                project=project,
                dataset=dataset,
                finished_at=datetime.datetime.now(datetime.timezone.utc),
                error_message=f"Queue job {queue_job_id} missing while run remained RUNNING",
            )
            append_event(
                RunEvent(
                    run_id=run.run_id,
                    event_id=str(uuid.uuid4()),
                    stage="run_reconciled",
                    level="warning",
                    message="Run reconciled from orphaned running state (queue job missing)",
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            return get_run(run.run_id, bq, project=project, dataset=dataset) or run
        except Exception:
            return run

    def _reconcile_orphaned_queued_run(run: PipelineRun) -> PipelineRun:
        """Repair QUEUED rows if their RQ job already started or terminated."""
        if run.status != RunStatus.QUEUED:
            return run
        queue_job_id = str(getattr(run, "queue_job_id", "") or "").strip()
        if not queue_job_id:
            return run
        created_at = getattr(run, "created_at", None)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if isinstance(created_at, datetime.datetime):
            created_at_utc = created_at if created_at.tzinfo else created_at.replace(tzinfo=datetime.timezone.utc)
            run_age_seconds = max(0.0, (now_utc - created_at_utc).total_seconds())
            if run_age_seconds < 10:
                return run
        try:
            rq_status = str(get_queue_job_status(queue_job_id, redis_url=redis_url) or "").strip().lower()
        except Exception:
            return run
        if rq_status in {"queued", "deferred", ""}:
            return run
        if rq_status == "started":
            update_run_status(
                run.run_id,
                RunStatus.RUNNING,
                bq,
                project=project,
                dataset=dataset,
                started_at=now_utc,
            )
            append_event(
                RunEvent(
                    run_id=run.run_id,
                    event_id=str(uuid.uuid4()),
                    stage="run_reconciled",
                    level="warning",
                    message=f"Run reconciled from QUEUED to RUNNING (queue status={rq_status})",
                    created_at=now_utc,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            return get_run(run.run_id, bq, project=project, dataset=dataset) or run
        if rq_status in {"finished", "failed", "stopped", "canceled", "cancelled"}:
            update_run_status(
                run.run_id,
                RunStatus.FAILED,
                bq,
                project=project,
                dataset=dataset,
                finished_at=now_utc,
                error_message=(
                    f"Queue job {queue_job_id} ended with status={rq_status} while run remained QUEUED "
                    "(likely web/worker storage mismatch or persistence failure)."
                ),
            )
            append_event(
                RunEvent(
                    run_id=run.run_id,
                    event_id=str(uuid.uuid4()),
                    stage="run_reconciled",
                    level="error",
                    message=f"Run reconciled from orphaned queued state (queue status={rq_status})",
                    created_at=now_utc,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            return get_run(run.run_id, bq, project=project, dataset=dataset) or run
        if rq_status == "missing" and queue_job_id.startswith("inline-"):
            return run
        return run

    def _reconcile_orphaned_run(run: PipelineRun) -> PipelineRun:
        run = _reconcile_orphaned_queued_run(run)
        return _reconcile_orphaned_running_run(run)
    all_settings_sections = {
        **SETTINGS_SECTIONS,
        **AGENTIC_SETTINGS_SECTIONS,
    }
    composition_sections = [
        {
            "id": "summary",
            "title": "Summary",
            "helper": "Whether a professional summary section appears in generated CVs.",
            "include_key": "cv_summary_enabled",
            "groups": [
                {"title": "Visibility", "keys": ["cv_summary_enabled"]},
            ],
        },
        {
            "id": "education",
            "title": "Education",
            "helper": "Whether an education section appears in generated CVs.",
            "include_key": "cv_education_enabled",
            "groups": [
                {"title": "Visibility", "keys": ["cv_education_enabled"]},
            ],
        },
        {
            "id": "experience",
            "title": "Experience",
            "helper": "Whether a work experience section appears in generated CVs.",
            "include_key": "cv_experience_enabled",
            "groups": [
                {"title": "Visibility", "keys": ["cv_experience_enabled"]},
            ],
        },
        {
            "id": "skills",
            "title": "Skills",
            "helper": "Whether a skills section appears in generated CVs.",
            "include_key": "cv_skills_enabled",
            "groups": [
                {"title": "Visibility", "keys": ["cv_skills_enabled"]},
            ],
        },
        {
            "id": "certifications",
            "title": "Certifications",
            "helper": "Whether certifications are shown.",
            "include_key": "cv_certifications_enabled",
            "groups": [
                {"title": "Visibility", "keys": ["cv_certifications_enabled"]},
            ],
        },
        {
            "id": "projects",
            "title": "Projects",
            "helper": "Visibility settings for projects.",
            "include_key": "cv_projects_enabled",
            "groups": [
                {"title": "Visibility", "keys": ["cv_projects_enabled"]},
            ],
        },
        {
            "id": "publications",
            "title": "Publications",
            "helper": "Whether a publications section appears in generated CVs.",
            "include_key": "cv_publications_enabled",
            "groups": [
                {"title": "Visibility", "keys": ["cv_publications_enabled"]},
            ],
        },
        {
            "id": "languages",
            "title": "Languages",
            "helper": "Whether a languages section appears in generated CVs.",
            "include_key": "cv_languages_enabled",
            "groups": [
                {"title": "Visibility", "keys": ["cv_languages_enabled"]},
            ],
        },
    ]
    cv_visibility_rows = [
        {
            "title": section["title"],
            "helper": section["helper"],
            "key": section["include_key"],
        }
        for section in composition_sections
    ]
    settings_page_sections = [
        {
            "id": "selection",
            "title": "Selection",
            "helper": "Shape how many jobs enter each expensive stage and which deterministic filters block them early.",
            "cards": [
                {
                    "id": "selection-funnel",
                    "title": "Retrieval Settings",
                    "helper": (
                        "Set the future-run candidate funnel before jobs move into ranking and later CV stages."
                    ),
                    "submit_kind": "section",
                    "submit_slug": "retrieval-core",
                    "keys": SETTINGS_SECTIONS["retrieval-core"],
                },
                {
                    "id": "selection-global-filters",
                    "title": "Global Job Filters",
                    "helper": "Reject jobs before enrichment so low-value candidates never reach later stages.",
                    "submit_kind": "section",
                    "submit_slug": "global-job-filters",
                    "keys": [
                        "global_job_filters.applications_count_max",
                        "global_job_filters.max_age_days",
                    ],
                },
                {
                    "id": "selection-rule-filter",
                    "title": "Rule Filter Settings",
                    "helper": "Choose which post-enrichment checks reject jobs versus only record downstream marks.",
                    "submit_kind": "section",
                    "submit_slug": "rule-filter",
                    "keys": ["rule_filter.selected_filters"],
                },
            ],
        },
        {
            "id": "agentic",
            "title": "Agentic Processing",
            "helper": "Configure bounded agentic behavior for future runs using small, explicit decision areas.",
            "cards": [
                {
                    "id": "agentic-enablement",
                    "title": "Enablement",
                    "helper": "Turn on agentic pathways and core capabilities used by future runs.",
                    "submit_kind": "section",
                    "submit_slug": "agentic-enablement",
                    "save_label": "Save Enablement Settings",
                    "keys": [
                        "cv.agentic_late_stage.enabled",
                        "cv_analysis.semantic_alignment.enabled",
                        "synonym_management.propose_enabled",
                        "synonym_management.apply_to_run_enabled",
                        "synonym_management.promote_global_enabled",
                    ],
                },
                {
                    "id": "agentic-reuse",
                    "title": "Reuse",
                    "helper": "Control exact-match reuse per stage. Keep ON for speed, turn OFF to force fresh compute.",
                    "submit_kind": "section",
                    "submit_slug": "agentic-reuse",
                    "save_label": "Save Reuse Settings",
                    "keys": [
                        "reuse.enrich.enabled",
                        "reuse.ranking.enabled",
                        "reuse.cv_analysis.enabled",
                        "reuse.cv_generation.enabled",
                        "reuse.synonym_triage.enabled",
                    ],
                },
                {
                    "id": "agentic-automation",
                    "title": "Automation",
                    "helper": "Control recommendation, auto-apply, and auto-promote behavior.",
                    "submit_kind": "section",
                    "submit_slug": "agentic-automation",
                    "save_label": "Save Automation Settings",
                    "keys": [
                        "synonym_management.auto_triage_recommendation_enabled",
                        "synonym_management.auto_apply_recommendation_enabled",
                        "synonym_management.auto_promote_global_enabled",
                        "synonym_management.auto_accept_ai_action_enabled",
                    ],
                },
                {
                    "id": "agentic-quality-targets",
                    "title": "Quality Targets",
                    "helper": "Tune lexical and semantic weighting balance for alignment channels.",
                    "submit_kind": "section",
                    "submit_slug": "agentic-advanced",
                    "save_label": "Save Quality Target Settings",
                    "keys": [
                        "cv_analysis.semantic_alignment.required_skill_lexical_weight",
                        "cv_analysis.semantic_alignment.required_skill_semantic_weight",
                        "cv_analysis.semantic_alignment.role_lexical_weight",
                        "cv_analysis.semantic_alignment.role_semantic_weight",
                        "cv_analysis.semantic_alignment.responsibility_lexical_weight",
                        "cv_analysis.semantic_alignment.responsibility_semantic_weight",
                        "cv_analysis.semantic_alignment.domain_lexical_weight",
                        "cv_analysis.semantic_alignment.domain_semantic_weight",
                    ],
                },
                {
                    "id": "agentic-throughput",
                    "title": "Throughput",
                    "helper": "Bound semantic channel candidate pool size for alignment processing.",
                    "submit_kind": "section",
                    "submit_slug": "agentic-advanced",
                    "save_label": "Save Throughput Settings",
                    "keys": [
                        "cv_analysis.semantic_alignment.channel_pool_size",
                    ],
                },
                {
                    "id": "agentic-diagnostics",
                    "title": "Diagnostics",
                    "helper": "Metadata-only semantic runtime contract details.",
                    "submit_kind": "section",
                    "submit_slug": "agentic-advanced",
                    "save_label": "Save Diagnostics Settings",
                    "keys": [
                        "cv_analysis.semantic_alignment.model",
                    ],
                    "is_advanced": True,
                },
                {
                    "id": "agentic-runtime-throughput",
                    "title": "Runtime Throughput",
                    "helper": "Canonical runtime pacing and concurrency controls grouped by stage.",
                    "submit_kind": "section",
                    "submit_slug": "timing",
                    "save_label": "Save Timing Settings",
                    "keys": canonical_runtime_throughput_keys,
                    "group_by_stage": True,
                },
                {
                    "id": "agentic-runtime-compatibility",
                    "title": "Legacy Compatibility",
                    "helper": "Collapsed read-only mapping of legacy aliases to canonical runtime throughput keys.",
                    "submit_kind": "section",
                    "submit_slug": "timing",
                    "keys": compatibility_runtime_alias_keys,
                    "is_collapsible": True,
                    "collapsed_by_default": True,
                    "read_only": True,
                },
            ],
        },
        {
            "id": "ranking",
            "title": "Ranking",
            "helper": "Tune how shortlisted jobs are scored, labeled, and gap-penalized.",
            "cards": [
                {
                    "id": "ranking-weights",
                    "title": "Ranking Weights",
                    "helper": "All six weights must sum to 1.0 (±0.01).",
                    "submit_kind": "group",
                    "submit_slug": "ranking-weights",
                    "keys": RANKING_GROUPS["ranking-weights"],
                },
                {
                    "id": "ranking-preference-fit",
                    "title": "Preference Fit Mix",
                    "helper": "Split preference alignment across domain, role family, and location type.",
                    "submit_kind": "group",
                    "submit_slug": "preference-fit-weights",
                    "form_id": "form-preference-fit-weights",
                    "keys": RANKING_GROUPS["preference-fit-weights"],
                },
                {
                    "id": "ranking-fit-thresholds",
                    "title": "Fit Label Thresholds",
                    "helper": "Set the score boundaries for Strong and Stretch fit labels.",
                    "submit_kind": "group",
                    "submit_slug": "fit-label-thresholds",
                    "form_id": "form-fit-label-thresholds",
                    "keys": RANKING_GROUPS["fit-label-thresholds"],
                },
                {
                    "id": "ranking-gap-thresholds",
                    "title": "Gap Thresholds",
                    "helper": "Control when missing-skill ratios start degrading Strong and Stretch classifications.",
                    "submit_kind": "group",
                    "submit_slug": "gap-thresholds",
                    "form_id": "form-gap-thresholds",
                    "keys": RANKING_GROUPS["gap-thresholds"],
                },
            ],
        },
        {
            "id": "cv-output",
            "title": "CV Output",
            "helper": "Choose the generation model, control section visibility, and bound output length.",
            "cards": [
                {
                    "id": "cv-template-model",
                    "title": "Template & Model",
                    "helper": "Fixed preset metadata plus the active generation model for future runs.",
                    "submit_kind": "group",
                    "submit_slug": "cv-preset",
                    "form_id": "form-cv-preset",
                    "save_label": "Save Preset Settings",
                    "keys": CV_GROUPS["cv-preset"],
                },
                {
                    "id": "cv-visibility",
                    "title": "Section Visibility",
                    "helper": "Decide which sections appear in generated CVs. Formatting-only legacy knobs remain hidden.",
                    "submit_kind": "group",
                    "submit_slug": "cv-composition",
                    "form_id": "form-cv-composition",
                    "save_label": "Save Composition Settings",
                    "keys": CV_GROUPS["cv-composition"],
                    "layout": "composition_matrix",
                },
                {
                    "id": "cv-validation",
                    "title": "Validation",
                    "helper": "Keep the warning-only page budget visible and easy to tune.",
                    "submit_kind": "group",
                    "submit_slug": "cv-validation",
                    "form_id": "form-cv-validation",
                    "save_label": "Save Validation Settings",
                    "keys": CV_GROUPS["cv-validation"],
                },
            ],
        },
        {
            "id": "run-safety",
            "title": "Run Safety",
            "helper": "Control server-owned protections that keep stuck runs from drifting indefinitely.",
            "cards": [
                {
                    "id": "run-safety-timeout",
                    "title": "Run Lifecycle Settings",
                    "helper": "Safety guard for queued, running, and Stage by Stage manual-wait runs. Higher values reduce false timeouts but allow longer zombie-run windows.",
                    "submit_kind": "section",
                    "submit_slug": "run-lifecycle",
                    "keys": SETTINGS_SECTIONS["run-lifecycle"],
                },
            ],
        },
    ]

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/admin/diagnostics/orchestration-schema")
    def admin_orchestration_schema_diagnostics() -> dict[str, Any]:
        schema_status = get_pipeline_runs_schema_status(
            bq,
            project=project,
            dataset=dataset,
        )
        return {
            "table": f"{project}.{dataset}.pipeline_runs",
            "required_columns": ["orchestration_backend", "orchestration_run_id"],
            **schema_status,
        }

    def _build_settings_context(active: dict[str, Any], **extra: Any) -> dict[str, Any]:
        effective = {
            entry["key"]: active.get(entry["key"], entry["default"])
            for entry in SETTINGS_SCHEMA
        }
        active_group_name = extra.get("active_group_name")
        active_section_name = extra.get("active_section_name")
        group_draft = extra.get("group_draft") or {}
        section_draft = extra.get("section_draft") or {}
        group_error = extra.get("group_error") or {}
        section_errors = extra.get("section_errors") or {}
        ia_metadata_by_key = settings_ia_metadata_by_key()

        def _display_value_for_settings(value: Any, entry_type: str) -> str:
            if entry_type == "bool":
                return "Yes" if bool(value) else "No"
            if isinstance(value, list):
                return ", ".join(str(item) for item in value) if value else "—"
            return str(value)

        def _normalize_for_dirty(value: Any, entry_type: str) -> str:
            if entry_type == "bool":
                return "true" if bool(value) else "false"
            if entry_type == "int":
                try:
                    return str(int(value))
                except (TypeError, ValueError):
                    return str(value)
            if entry_type == "float":
                try:
                    return str(float(value))
                except (TypeError, ValueError):
                    return str(value)
            if entry_type == "list[str]":
                if isinstance(value, list):
                    return "|".join(str(item).strip() for item in value)
                return str(value)
            return str(value)

        def _draft_value_for_card(
            *,
            submit_kind: str,
            submit_slug: str,
            key: str,
        ) -> Any | None:
            if submit_kind == "group" and active_group_name == submit_slug and key in group_draft.get(submit_slug, {}):
                return group_draft[submit_slug][key]
            if submit_kind == "section" and active_section_name == submit_slug and key in section_draft:
                return section_draft[key]
            return None

        def _card_error_for(submit_kind: str, submit_slug: str, keys: list[str]) -> str | None:
            if submit_kind == "group":
                return group_error.get(submit_slug)
            if active_section_name != submit_slug:
                return None
            for key in keys:
                if key in section_errors:
                    return section_errors[key]
            return None

        def _decision_domain_for_entry(entry: dict[str, Any]) -> str:
            group = str(entry.get("group") or "")
            if group in {"cv_composition", "cv_validation", "cv_preset"}:
                return "output_artifacts"
            if group == "agentic":
                return "synonym_review"
            if group in {"retrieval", "rule_filter", "ranking", "global_job_filters"}:
                return "extraction_rules"
            return "domain_taxonomy"

        def _build_card(card_spec: dict[str, Any]) -> dict[str, Any]:
            submit_kind = str(card_spec["submit_kind"])
            submit_slug = str(card_spec["submit_slug"])
            card_read_only = bool(card_spec.get("read_only", False))
            action = (
                f"/admin/settings/group/{submit_slug}"
                if submit_kind == "group"
                else f"/admin/settings/section/{submit_slug}"
            )
            visible_keys = [key for key in card_spec["keys"] if key not in hidden_deprecated_keys]
            entries: list[dict[str, Any]] = []
            agentic_enabled = bool(effective.get("cv.agentic_late_stage.enabled"))
            for key in visible_keys:
                entry = schema_by_key[key]
                effective_value = effective[key]
                draft_value = _draft_value_for_card(
                    submit_kind=submit_kind,
                    submit_slug=submit_slug,
                    key=key,
                )
                typed_draft_value = None
                if draft_value is not None:
                    try:
                        typed_draft_value = coerce_value(key, draft_value)
                    except (KeyError, TypeError, ValueError):
                        typed_draft_value = None
                form_value = draft_value if draft_value is not None else effective_value
                comparison_value = typed_draft_value if typed_draft_value is not None else form_value
                is_dirty = _normalize_for_dirty(comparison_value, str(entry["type"])) != _normalize_for_dirty(
                    effective_value,
                    str(entry["type"]),
                )
                has_error = bool(
                    (
                        submit_kind == "group"
                        and str(group_error.get(submit_slug) or "").strip()
                    )
                    or (
                        submit_kind == "section"
                        and active_section_name == submit_slug
                        and key in section_errors
                    )
                )
                is_recommended_delta = (
                    key not in active
                    and key in editable_keys
                    and key not in metadata_only_keys
                    and not bool(settings_ia_contract_for_key(key).get("is_dangerous"))
                )
                is_missing_required = (
                    str(entry.get("type") or "") == "str"
                    and key in active
                    and str(comparison_value or "").strip() == ""
                )
                decision_state = derive_settings_decision_state(
                    is_advanced=bool(settings_ia_contract_for_key(key).get("advanced")),
                    is_unused=bool(key in metadata_only_keys and key not in active),
                    is_changed_from_default=bool(key in active),
                    has_recommended_delta=is_recommended_delta,
                    has_conflict=has_error,
                    has_missing_required=is_missing_required,
                    has_quality_risk=bool(settings_ia_contract_for_key(key).get("is_dangerous") and key in active),
                )
                semantic_alignment_enabled = bool(effective.get("cv_analysis.semantic_alignment.enabled"))
                apply_to_run_enabled = bool(effective.get("synonym_management.apply_to_run_enabled"))
                promote_global_enabled = bool(effective.get("synonym_management.promote_global_enabled"))
                owner_label = "Settings"
                active_label = "Yes"
                compatibility_alias_for = str(entry.get("compatibility_alias_for") or "").strip()
                compatibility_aliases = list(canonical_compatibility_aliases.get(key) or [])
                if key == "cv_generation_model":
                    owner_label = "Settings (non-agentic path)"
                    active_label = "No (agentic mode ON)" if agentic_enabled else "Yes (agentic mode OFF)"
                elif key == "cv.agentic_late_stage.enabled":
                    owner_label = "Settings"
                    active_label = "Yes"
                elif key == "cv_analysis.semantic_alignment.model":
                    owner_label = "Runtime Contract"
                    active_label = "Yes"
                elif key.startswith("cv_analysis.semantic_alignment.") and key.endswith("_semantic_weight"):
                    owner_label = "Settings"
                    active_label = "Yes" if semantic_alignment_enabled else "No (semantic alignment OFF)"
                elif key == "synonym_management.auto_apply_recommendation_enabled":
                    owner_label = "Settings"
                    active_label = "Yes" if apply_to_run_enabled else "No (requires Apply-to-Run Enabled)"
                elif key == "synonym_management.auto_promote_global_enabled":
                    owner_label = "Settings"
                    active_label = "Yes" if promote_global_enabled else "No (requires Promote-Global Enabled)"
                elif compatibility_alias_for:
                    owner_label = "Compatibility surface (legacy alias)"
                    active_label = "Yes (maps to canonical runtime key)"
                entries.append(
                    {
                        "entry": entry,
                        "effective_value": effective_value,
                        "form_value": form_value,
                        "effective_display": _display_value_for_settings(effective_value, str(entry["type"])),
                        "current_display": _display_value_for_settings(comparison_value, str(entry["type"])),
                        "current_source_label": "Persisted override" if key in active else "Baseline default",
                        "is_dirty": is_dirty,
                        "is_modified": is_dirty,
                        "effective_value": effective_value,
                        "source_label": "Persisted override" if key in active else "Baseline default",
                        "override_state": "overridden" if key in active else "inherited",
                        "has_error": has_error,
                        "is_metadata_only": card_read_only or key in metadata_only_keys,
                        "metadata_note": (
                            "Read-only compatibility mapping. Edit canonical Runtime Throughput settings instead."
                            if card_read_only else None
                        ),
                        "ia_contract": settings_ia_contract_for_key(key),
                        "decision_state": decision_state,
                        "decision_status": str(decision_state.get("decision_status") or DECISION_STATUS_CONFIGURED),
                        "decision_reason_codes": list(decision_state.get("reason_codes") or []),
                        "decision_domain": _decision_domain_for_entry(entry),
                        "decision_stage": str(settings_ia_contract_for_key(key).get("stage") or ""),
                        "decision_control_surface": str(settings_ia_contract_for_key(key).get("control_surface") or ""),
                        "decision_area": str(settings_ia_contract_for_key(key).get("decision_area") or ""),
                        "decision_complexity": str(settings_ia_contract_for_key(key).get("complexity_view") or ""),
                        "owner_label": owner_label,
                        "active_label": active_label,
                        "compatibility_alias_for": compatibility_alias_for,
                        "compatibility_aliases": compatibility_aliases,
                    }
                )
            dirty_count = sum(1 for item in entries if item["is_dirty"])
            card_domains = sorted(
                {
                    str(item["ia_contract"].get("domain") or "")
                    for item in entries
                    if str(item["ia_contract"].get("domain") or "")
                }
            )
            card_stages = sorted(
                {
                    stage
                    for item in entries
                    for stage in list(item["ia_contract"].get("workflow_stages") or [])
                    if isinstance(stage, str) and stage
                }
            )
            risk_rank = {"low": 1, "medium": 2, "high": 3}
            card_risk = "low"
            for item in entries:
                item_risk = str(item["ia_contract"].get("risk") or "low")
                if risk_rank.get(item_risk, 1) > risk_rank.get(card_risk, 1):
                    card_risk = item_risk
            stage_groups: list[dict[str, Any]] = []
            if bool(card_spec.get("group_by_stage", False)):
                for stage_id, stage_title in runtime_throughput_stage_titles.items():
                    stage_entries = [item for item in entries if item.get("decision_stage") == stage_id]
                    if stage_entries:
                        stage_groups.append(
                            {
                                "id": stage_id,
                                "title": stage_title,
                                "entries": stage_entries,
                            }
                        )
            return {
                "id": card_spec["id"],
                "title": card_spec["title"],
                "helper": card_spec["helper"],
                "submit_kind": submit_kind,
                "submit_slug": submit_slug,
                "action": action,
                "form_id": card_spec.get("form_id", f"form-{card_spec['id']}"),
                "save_label": card_spec.get("save_label", f"Save {card_spec['title']}"),
                "entries": entries,
                "keys": list(visible_keys),
                "dirty_count": dirty_count,
                "error_message": _card_error_for(submit_kind, submit_slug, list(visible_keys)),
                "layout": card_spec.get("layout", "list"),
                "is_advanced": bool(card_spec.get("is_advanced", False)),
                "is_collapsible": bool(card_spec.get("is_collapsible", False)),
                "collapsed_by_default": bool(card_spec.get("collapsed_by_default", False)),
                "read_only": card_read_only,
                "stage_groups": stage_groups,
                "domains": card_domains,
                "workflow_stages": card_stages,
                "risk": card_risk,
                "guarded_save": (not card_read_only) and (bool(card_spec.get("is_advanced", False)) or card_risk == "high"),
            }

        settings_page_task_sections = [
            {
                "id": section["id"],
                "title": section["title"],
                "helper": section["helper"],
                "cards": [_build_card(card) for card in section["cards"]],
            }
            for section in settings_page_sections
        ]
        domain_stats: dict[str, dict[str, int]] = {}
        for section in settings_page_task_sections:
            for card in section["cards"]:
                for item in card["entries"]:
                    domain = str(item["ia_contract"].get("domain") or "advanced")
                    stats = domain_stats.setdefault(
                        domain,
                        {"total": 0, "modified": 0, "errors": 0, "overrides": 0},
                    )
                    stats["total"] += 1
                    if bool(item["is_modified"]):
                        stats["modified"] += 1
                    if bool(item["has_error"]):
                        stats["errors"] += 1
                    if str(item.get("override_state") or "") == "overridden":
                        stats["overrides"] += 1
        domain_titles = {
            "general": "General",
            "layers": "Layers",
            "stages": "Stages",
            "rules": "Rules",
            "integrations": "Integrations",
            "advanced": "Advanced",
        }
        workflow_stage_titles = {
            "normalize": "Normalize",
            "enrich": "Enrich",
            "rule_filter": "Rule Filter",
            "shortlist": "Shortlist",
            "ranking": "Ranking",
            "cv_analysis": "CV Analysis",
            "cv_generation": "CV Generation",
        }
        stage_titles = {
            "normalize": "Normalize",
            "enrich": "Enrich",
            "rule_filter": "Rule Filter",
            "shortlist": "Shortlist",
            "ranking": "Ranking",
            "cv_analysis": "CV Analysis",
            "cv_generation": "CV Generation",
            "cross_stage": "Cross-Stage",
        }
        control_surface_titles = {
            "standard_pipeline": "Standard Pipeline",
            "agentic_runtime": "Agentic Runtime",
            "shared": "Shared",
        }
        settings_domains = [
            {
                "id": domain_id,
                "title": title,
                "keys": settings_keys_for_domain(domain_id),
            }
            for domain_id, title in domain_titles.items()
        ]
        settings_stage_filters = [
            {
                "id": stage_id,
                "title": title,
                "keys": settings_keys_for_workflow_stage(stage_id),
            }
            for stage_id, title in workflow_stage_titles.items()
        ]
        settings_domain_summaries = [
            {
                "id": domain["id"],
                "title": domain["title"],
                **domain_stats.get(str(domain["id"]), {"total": 0, "modified": 0, "errors": 0, "overrides": 0}),
            }
            for domain in settings_domains
        ]
        settings_stage_scopes = [
            {
                "id": stage_id,
                "title": title,
                "keys": settings_keys_for_stage(stage_id),
            }
            for stage_id, title in stage_titles.items()
        ]
        settings_control_surface_filters = [
            {
                "id": surface_id,
                "title": title,
                "keys": settings_keys_for_control_surface(surface_id),
            }
            for surface_id, title in control_surface_titles.items()
        ]
        decision_entries: list[dict[str, Any]] = []
        for section in settings_page_task_sections:
            for card in section["cards"]:
                for item in card["entries"]:
                    decision_entries.append(
                        {
                            "section_id": section["id"],
                            "card_id": card["id"],
                            "card_title": card["title"],
                            "key": str(item["entry"]["key"]),
                            "label": str(item["entry"]["label"]),
                            "decision_status": str(item.get("decision_status") or DECISION_STATUS_CONFIGURED),
                            "decision_reason_codes": list(item.get("decision_reason_codes") or []),
                            "decision_domain": str(item.get("decision_domain") or "domain_taxonomy"),
                            "risk": str(item["ia_contract"].get("risk") or "low"),
                            "is_modified": bool(item.get("is_modified")),
                            "item": item,
                            "card": card,
                            "section": section,
                        }
                    )
        decision_tabs = [
            {"id": DECISION_STATUS_NEEDS_REVIEW, "title": "Needs Review"},
            {"id": DECISION_STATUS_RECOMMENDED, "title": "Recommended"},
            {"id": DECISION_STATUS_CONFIGURED, "title": "Configured"},
            {"id": DECISION_STATUS_ADVANCED, "title": "Advanced"},
            {"id": "all", "title": "All"},
        ]
        decision_domain_filters = [
            {"id": "domain_taxonomy", "title": "Domain & Taxonomy"},
            {"id": "extraction_rules", "title": "Extraction Rules"},
            {"id": "synonym_review", "title": "Synonym Review"},
            {"id": "output_artifacts", "title": "Output & Artifacts"},
        ]
        settings_decision_groups: list[dict[str, Any]] = []
        for tab in decision_tabs:
            tab_id = str(tab["id"])
            tab_items = [
                item
                for item in decision_entries
                if tab_id == "all" or str(item["decision_status"]) == tab_id
            ]
            tab_items.sort(
                key=lambda item: (
                    decision_status_sort_key(str(item["decision_status"])),
                    0 if str(item["risk"]) == "high" else (1 if str(item["risk"]) == "medium" else 2),
                    str(item["label"]),
                )
            )
            domain_counts: dict[str, int] = {}
            for item in tab_items:
                domain_key = str(item["decision_domain"])
                domain_counts[domain_key] = domain_counts.get(domain_key, 0) + 1
            settings_decision_groups.append(
                {
                    "id": tab_id,
                    "title": tab["title"],
                    "count": len(tab_items),
                    "items": tab_items,
                    "domain_counts": domain_counts,
                }
            )
        settings_readiness_summary = {
            "configured_count": sum(1 for item in decision_entries if str(item["decision_status"]) == DECISION_STATUS_CONFIGURED),
            "needs_review_count": sum(1 for item in decision_entries if str(item["decision_status"]) == DECISION_STATUS_NEEDS_REVIEW),
            "recommended_count": sum(1 for item in decision_entries if str(item["decision_status"]) == DECISION_STATUS_RECOMMENDED),
            "advanced_count": sum(1 for item in decision_entries if str(item["decision_status"]) == DECISION_STATUS_ADVANCED),
            "total_count": len(decision_entries),
        }
        settings_readiness_summary["hidden_defaults_unused_count"] = sum(
            1
            for item in decision_entries
            if "unused" in set(str(code) for code in list(item["decision_reason_codes"]))
        )
        danger_zone_keys = danger_zone_settings_keys()
        def _resolve_mode_summary() -> dict[str, str]:
            agentic_enabled = bool(effective.get("cv.agentic_late_stage.enabled"))
            runtime_provider = str(os.environ.get("FITCV_LANGGRAPH_PROVIDER", "") or "").strip()
            runtime_model = str(os.environ.get("FITCV_LANGGRAPH_MODEL", "") or "").strip()
            has_runtime_env = bool(runtime_provider or runtime_model)
            authority_state = "aligned"
            drift_reason = ""
            if not agentic_enabled and has_runtime_env:
                authority_state = "drifted"
                drift_reason = "Agentic runtime env is configured but agentic mode toggle is OFF."
            elif agentic_enabled and not has_runtime_env:
                authority_state = "drifted"
                drift_reason = "Agentic mode toggle is ON but FITCV_LANGGRAPH provider/model env is missing."
            return {
                "agentic_mode": "ON" if agentic_enabled else "OFF",
                "runtime_provider": runtime_provider or "—",
                "runtime_model": runtime_model or "—",
                "authority_state": authority_state,
                "drift_reason": drift_reason,
            }

        mode_summary = _resolve_mode_summary()
        agentic_runtime_note = ""
        if mode_summary["runtime_provider"] != "—" or mode_summary["runtime_model"] != "—":
            agentic_runtime_note = (
                "Agentic live runtime is env-managed"
                f" (provider={mode_summary['runtime_provider']}, model={mode_summary['runtime_model']}). "
                "In agentic mode, live runtime values can differ from CV model settings on this page."
            )
        settings_truth_notes = [
            "This page edits future-run defaults only.",
            "Per-run overrides are captured at trigger time and do not change these saved defaults.",
            "Use settings-used.json on a completed run as the historical source of truth for what that run actually used.",
            "Use run detail observability to inspect what a specific run did stage by stage.",
        ]
        if agentic_runtime_note:
            settings_truth_notes.append(agentic_runtime_note)
        context: dict[str, Any] = {
            "schema": runtime_settings_schema,
            "schema_by_key": schema_by_key,
            "active": active,
            "effective": effective,
            "ranking_weight_keys": RANKING_GROUPS["ranking-weights"],
            "ranking_groups": RANKING_GROUPS,
            "cv_groups": CV_GROUPS,
            "composition_sections": composition_sections,
            "cv_visibility_rows": cv_visibility_rows,
            "metadata_only_keys": metadata_only_keys,
            "settings_page_task_sections": settings_page_task_sections,
            "settings_metadata_note": "Currently fixed by the active runtime contract",
            "settings_truth_notes": settings_truth_notes,
            "settings_mode_summary": mode_summary,
            "settings_domains": settings_domains,
            "settings_stage_filters": settings_stage_filters,
            "settings_stage_scopes": settings_stage_scopes,
            "settings_control_surface_filters": settings_control_surface_filters,
            "settings_ia_metadata_by_key": ia_metadata_by_key,
            "settings_danger_zone_keys": danger_zone_keys,
            "settings_domain_summaries": settings_domain_summaries,
            "settings_decision_groups": settings_decision_groups,
            "settings_decision_domain_filters": decision_domain_filters,
            "settings_readiness_summary": settings_readiness_summary,
        }
        context.update(extra)
        return context

    def _settings_form_value(form: Any, key: str) -> Any:
        entry = schema_by_key.get(key, {})
        if entry.get("type") == "list[str]":
            return form.getlist(key)
        return form.get(key, "")

    def _is_stale_cancelling(run: PipelineRun) -> bool:
        if run.status != RunStatus.CANCELLING or run.finished_at is not None:
            return False
        if run.started_at is None:
            return True
        if run.cancel_requested_at is None:
            return False
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - run.cancel_requested_at) >= datetime.timedelta(minutes=2)

    def _run_max_runtime_minutes() -> int:
        default_minutes = int(schema_by_key["run_lifecycle.max_runtime_minutes"]["default"])
        active_settings = load_active_settings(bq=bq, project=project, dataset=dataset)
        value = active_settings.get("run_lifecycle.max_runtime_minutes", default_minutes)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return default_minutes

    def _timeout_reference_timestamp(run: PipelineRun) -> datetime.datetime | None:
        if run.status in {RunStatus.RUNNING, RunStatus.CANCELLING}:
            return run.started_at or run.created_at
        if run.status in {RunStatus.QUEUED, RunStatus.AWAITING_CONTINUE}:
            return run.created_at
        return None

    def _timeout_transition_for_run(run: PipelineRun, max_runtime_minutes: int) -> tuple[RunStatus, str, str | None]:
        if run.status == RunStatus.QUEUED:
            return (
                RunStatus.CANCELLED,
                f"Run timed out after waiting more than {max_runtime_minutes} minute(s) in the queue.",
                None,
            )
        if run.status == RunStatus.AWAITING_CONTINUE:
            return (
                RunStatus.CANCELLED,
                f"Run timed out after waiting more than {max_runtime_minutes} minute(s) for manual continuation.",
                None,
            )
        return (
            RunStatus.FAILED,
            f"Run exceeded the maximum runtime of {max_runtime_minutes} minute(s).",
            "run_lifecycle_timeout",
        )

    def _enforce_run_timeout_guard(run: PipelineRun, *, max_runtime_minutes: int | None = None) -> PipelineRun:
        if run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}:
            return run
        if max_runtime_minutes is None:
            max_runtime_minutes = _run_max_runtime_minutes()
        reference_at = _timeout_reference_timestamp(run)
        if reference_at is None:
            return run
        now = datetime.datetime.now(datetime.timezone.utc)
        if (now - reference_at) < datetime.timedelta(minutes=max_runtime_minutes):
            return run

        target_status, message, error_stage = _timeout_transition_for_run(run, max_runtime_minutes)
        if run.status == RunStatus.QUEUED and run.queue_job_id:
            cancel_queued_run(run.queue_job_id, redis_url=redis_url)
        update_kwargs: dict[str, Any] = {
            "project": project,
            "dataset": dataset,
            "finished_at": now,
        }
        if target_status == RunStatus.FAILED:
            update_kwargs["error_message"] = message
            update_kwargs["error_stage"] = error_stage
        update_run_status(run.run_id, target_status, bq, **update_kwargs)
        append_event(
            RunEvent(
                run_id=run.run_id,
                event_id=str(uuid.uuid4()),
                stage="run_timed_out",
                level="warning",
                message=message,
                created_at=now,
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        update_fields: dict[str, Any] = {
            "status": target_status,
            "finished_at": now,
        }
        if target_status == RunStatus.FAILED:
            update_fields["error_message"] = message
            update_fields["error_stage"] = error_stage
        return dataclasses.replace(run, **update_fields)

    def _execute_trigger(
        jobs_path: str,
        config_path: str,
        triggered_by: str,
        config_overrides: dict[str, Any],
        *,
        run_mode: str = "run_all",
    ) -> dict:
        # Build effective config: YAML → BQ settings → per-run overrides
        try:
            base_config = load_config(config_path)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        active_settings = load_active_settings(bq=bq, project=project, dataset=dataset)

        def _normalize_trigger_overrides(raw_overrides: dict[str, Any]) -> dict[str, Any]:
            """Accept flat or nested trigger overrides and normalize to dot-path leaves.

            Supports canonical flat keys (`stage_runtime.enrich.concurrency`) and
            nested payloads (`{"stage_runtime": {"enrich": {"concurrency": 2}}}`).
            Raises 422-style ValueError when conflicting duplicate paths are provided.
            """
            normalized: dict[str, Any] = {}

            def _flatten(prefix: str, value: Any) -> None:
                if isinstance(value, dict):
                    for child_key, child_value in value.items():
                        child_name = str(child_key or "").strip()
                        if not child_name:
                            continue
                        next_prefix = f"{prefix}.{child_name}" if prefix else child_name
                        _flatten(next_prefix, child_value)
                    return
                existing = normalized.get(prefix, None)
                if prefix in normalized and existing != value:
                    raise ValueError(
                        "Conflicting config_overrides for key "
                        f"{prefix!r}: got both {existing!r} and {value!r}"
                    )
                normalized[prefix] = value

            for raw_key, raw_value in dict(raw_overrides or {}).items():
                key = str(raw_key or "").strip()
                if not key:
                    continue
                if "." in key:
                    if key in normalized and normalized[key] != raw_value:
                        raise ValueError(
                            "Conflicting config_overrides for key "
                            f"{key!r}: got both {normalized[key]!r} and {raw_value!r}"
                        )
                    normalized[key] = raw_value
                    continue
                if isinstance(raw_value, dict):
                    _flatten(key, raw_value)
                    continue
                if key in normalized and normalized[key] != raw_value:
                    raise ValueError(
                        "Conflicting config_overrides for key "
                        f"{key!r}: got both {normalized[key]!r} and {raw_value!r}"
                    )
                normalized[key] = raw_value
            return normalized

        # Coerce and validate per-run overrides using the same schema
        coerced_overrides: dict[str, Any] = {}
        try:
            normalized_overrides = _normalize_trigger_overrides(config_overrides)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        for k, v in normalized_overrides.items():
            try:
                coerced_overrides[k] = coerce_value(k, v)
            except KeyError:
                raise HTTPException(status_code=422, detail=f"Unknown setting key: {k!r}")
            except (ValueError, TypeError) as exc:
                raise HTTPException(status_code=422, detail=str(exc))
        try:
            validate_settings(coerced_overrides)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        # Merge: YAML < BQ settings < per-run overrides
        effective_config = dict(base_config)
        apply_settings_to_config(effective_config, active_settings)
        apply_settings_to_config(effective_config, coerced_overrides)
        # Recompute derived fields (required_cv_sections, etc.) from effective composition
        effective_config = apply_cv_compatibility_projection(effective_config)
        reuse_precheck_warning: dict[str, Any] | None = None
        try:
            current_hash = _stable_synonym_hash(effective_config)
            current_count = len(dict(effective_config.get("skill_synonyms") or {}))
            recent_runs = list_runs(
                bq,
                project=project,
                dataset=dataset,
                include_archived=True,
                limit=50,
            )
            prior_success = next(
                (
                    run
                    for run in recent_runs
                    if str(getattr(run, "status", "") or "").strip().lower() in {"succeeded", "awaiting_continue"}
                    and str(getattr(run, "effective_settings_json", "") or "").strip()
                ),
                None,
            )
            if prior_success is not None:
                prior_cfg = _load_json_object(getattr(prior_success, "effective_settings_json", ""))
                prior_hash = _stable_synonym_hash(prior_cfg if isinstance(prior_cfg, dict) else {})
                if prior_hash and prior_hash != current_hash:
                    prior_count = len(dict((prior_cfg or {}).get("skill_synonyms") or {})) if isinstance(prior_cfg, dict) else 0
                    reuse_precheck_warning = {
                        "code": "cv_analysis_reuse_reset_likely",
                        "message": "Skill synonym map changed from previous successful run; exact CV-analysis reuse may reset.",
                        "previous_run_id": str(getattr(prior_success, "run_id", "") or ""),
                        "previous_synonym_count": int(prior_count),
                        "current_synonym_count": int(current_count),
                    }
        except Exception as exc:
            logger.warning("reuse precheck warning generation failed (non-blocking): %s", exc)
        actual_jobs_path, jobs_input_json_snapshot = _resolve_jobs_path_snapshot(jobs_path)
        candidate_json_snapshot = _resolve_default_candidate_profile_snapshot(config_path)
        effective_config = _apply_trigger_runtime_envelope(
            effective_config,
            jobs_input_source="path",
            jobs_input_json=jobs_input_json_snapshot,
            jobs_input_manifest_json=None,
            candidate_profile_source="default_config",
            candidate_profile_json=candidate_json_snapshot,
            run_mode=run_mode,
            reuse_precheck_warning=reuse_precheck_warning,
        )

        run_id = str(uuid.uuid4())
        # Insert FIRST — then enqueue. DB is the source of truth.
        run = PipelineRun(
            run_id=run_id,
            status=RunStatus.QUEUED,
            triggered_by=triggered_by,
            trigger_source="ui",
            jobs_path=actual_jobs_path,
            config_path=config_path,
            created_at=datetime.datetime.now(datetime.timezone.utc),
            effective_settings_json=_json.dumps(effective_config),
            jobs_input_source="path",
            jobs_input_json=jobs_input_json_snapshot,
            candidate_profile_source="default_config",
            candidate_profile_json=candidate_json_snapshot,
            run_mode=run_mode,
            checkpoint_status="pending_first_stage" if run_mode == "manual_staged" else None,
            next_stage="normalize" if run_mode == "manual_staged" else None,
            completed_stages=[],
        )
        _persist_run_initial(run, bq=bq, project=project, dataset=dataset)
        _, queue_job_id = enqueue_run_with_job_id(
            jobs_path=actual_jobs_path,
            config_path=config_path,
            triggered_by=triggered_by,
            redis_url=redis_url,
            run_id=run_id,
        )
        submission = _resolve_submission_binding(run_id, queue_job_id)
        _persist_run_orchestration_binding(
            run_id,
            queue_job_id=submission.queue_job_id,
            orchestration_backend=submission.backend,
            orchestration_run_id=submission.backend_run_id,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        _persist_run_queue_job_id(
            run_id,
            submission.queue_job_id,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        return {"run_id": run_id, "warnings": [reuse_precheck_warning] if reuse_precheck_warning else []}

    def _execute_trigger_with_inputs(
        jobs_path: str,
        config_path: str,
        triggered_by: str,
        config_overrides: dict[str, Any],
        *,
        jobs_input_source: str | None = None,
        jobs_input_json: str | None = None,
        jobs_input_manifest_json: str | None = None,
        candidate_profile_source: str | None = None,
        candidate_profile_json: str | None = None,
        run_synonym_overlay: dict[str, Any] | None = None,
        run_synonym_overlay_filename: str | None = None,
        run_synonym_overlay_raw_yaml: str | None = None,
        run_synonym_overlay_source: str = "trigger_upload",
        run_mode: str = "run_all",
    ) -> dict:
        """Like _execute_trigger but records run-scoped input metadata."""
        # Build effective config: YAML → BQ settings → per-run overrides
        try:
            base_config = load_config(config_path)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        active_settings = load_active_settings(bq=bq, project=project, dataset=dataset)
        def _normalize_trigger_overrides(raw_overrides: dict[str, Any]) -> dict[str, Any]:
            normalized: dict[str, Any] = {}

            def _flatten(prefix: str, value: Any) -> None:
                if isinstance(value, dict):
                    for child_key, child_value in value.items():
                        child_name = str(child_key or "").strip()
                        if not child_name:
                            continue
                        next_prefix = f"{prefix}.{child_name}" if prefix else child_name
                        _flatten(next_prefix, child_value)
                    return
                existing = normalized.get(prefix, None)
                if prefix in normalized and existing != value:
                    raise ValueError(
                        "Conflicting config_overrides for key "
                        f"{prefix!r}: got both {existing!r} and {value!r}"
                    )
                normalized[prefix] = value

            for raw_key, raw_value in dict(raw_overrides or {}).items():
                key = str(raw_key or "").strip()
                if not key:
                    continue
                if "." in key:
                    if key in normalized and normalized[key] != raw_value:
                        raise ValueError(
                            "Conflicting config_overrides for key "
                            f"{key!r}: got both {normalized[key]!r} and {raw_value!r}"
                        )
                    normalized[key] = raw_value
                    continue
                if isinstance(raw_value, dict):
                    _flatten(key, raw_value)
                    continue
                if key in normalized and normalized[key] != raw_value:
                    raise ValueError(
                        "Conflicting config_overrides for key "
                        f"{key!r}: got both {normalized[key]!r} and {raw_value!r}"
                    )
                normalized[key] = raw_value
            return normalized

        coerced_overrides: dict[str, Any] = {}
        try:
            normalized_overrides = _normalize_trigger_overrides(config_overrides)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        for k, v in normalized_overrides.items():
            try:
                coerced_overrides[k] = coerce_value(k, v)
            except KeyError:
                raise HTTPException(status_code=422, detail=f"Unknown setting key: {k!r}")
            except (ValueError, TypeError) as exc:
                raise HTTPException(status_code=422, detail=str(exc))
        try:
            validate_settings(coerced_overrides)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        effective_config = dict(base_config)
        apply_settings_to_config(effective_config, active_settings)
        apply_settings_to_config(effective_config, coerced_overrides)
        # Recompute derived fields (required_cv_sections, etc.) from effective composition
        effective_config = apply_cv_compatibility_projection(effective_config)
        reuse_precheck_warning: dict[str, Any] | None = None
        try:
            current_hash = _stable_synonym_hash(effective_config)
            current_count = len(dict(effective_config.get("skill_synonyms") or {}))
            recent_runs = list_runs(
                bq,
                project=project,
                dataset=dataset,
                include_archived=True,
                limit=50,
            )
            prior_success = next(
                (
                    run
                    for run in recent_runs
                    if str(getattr(run, "status", "") or "").strip().lower() in {"succeeded", "awaiting_continue"}
                    and str(getattr(run, "effective_settings_json", "") or "").strip()
                ),
                None,
            )
            if prior_success is not None:
                prior_cfg = _load_json_object(getattr(prior_success, "effective_settings_json", ""))
                prior_hash = _stable_synonym_hash(prior_cfg if isinstance(prior_cfg, dict) else {})
                if prior_hash and prior_hash != current_hash:
                    prior_count = len(dict((prior_cfg or {}).get("skill_synonyms") or {})) if isinstance(prior_cfg, dict) else 0
                    reuse_precheck_warning = {
                        "code": "cv_analysis_reuse_reset_likely",
                        "message": "Skill synonym map changed from previous successful run; exact CV-analysis reuse may reset.",
                        "previous_run_id": str(getattr(prior_success, "run_id", "") or ""),
                        "previous_synonym_count": int(prior_count),
                        "current_synonym_count": int(current_count),
                    }
        except Exception as exc:
            logger.warning("reuse precheck warning generation failed (non-blocking): %s", exc)
        effective_config = _apply_trigger_runtime_envelope(
            effective_config,
            jobs_input_source=jobs_input_source,
            jobs_input_json=jobs_input_json,
            jobs_input_manifest_json=jobs_input_manifest_json,
            candidate_profile_source=candidate_profile_source,
            candidate_profile_json=candidate_profile_json,
            run_mode=run_mode,
            reuse_precheck_warning=reuse_precheck_warning,
        )

        if run_synonym_overlay:
            effective_config = apply_runtime_synonym_overlay(
                effective_config,
                run_synonym_overlay,
                source=run_synonym_overlay_source,
                filename=str(run_synonym_overlay_filename or "").strip(),
                uploaded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                raw_yaml=str(run_synonym_overlay_raw_yaml or ""),
            )

        run_id = str(uuid.uuid4())
        run = PipelineRun(
            run_id=run_id,
            status=RunStatus.QUEUED,
            triggered_by=triggered_by,
            trigger_source="ui",
            jobs_path=jobs_path,
            config_path=config_path,
            created_at=datetime.datetime.now(datetime.timezone.utc),
            effective_settings_json=_json.dumps(effective_config),
            jobs_input_source=jobs_input_source,
            jobs_input_json=jobs_input_json,
            jobs_input_manifest_json=jobs_input_manifest_json,
            candidate_profile_source=candidate_profile_source,
            candidate_profile_json=candidate_profile_json,
            run_mode=run_mode,
            checkpoint_status="pending_first_stage" if run_mode == "manual_staged" else None,
            next_stage="normalize" if run_mode == "manual_staged" else None,
            completed_stages=[],
        )
        _persist_run_initial(run, bq=bq, project=project, dataset=dataset)
        _, queue_job_id = enqueue_run_with_job_id(
            jobs_path=jobs_path,
            config_path=config_path,
            triggered_by=triggered_by,
            redis_url=redis_url,
            run_id=run_id,
        )
        submission = _resolve_submission_binding(run_id, queue_job_id)
        _persist_run_orchestration_binding(
            run_id,
            queue_job_id=submission.queue_job_id,
            orchestration_backend=submission.backend,
            orchestration_run_id=submission.backend_run_id,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        _persist_run_queue_job_id(
            run_id,
            submission.queue_job_id,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        return {"run_id": run_id, "warnings": [reuse_precheck_warning] if reuse_precheck_warning else []}

    @app.post("/runs", status_code=201)
    def trigger_run(req: TriggerRequest) -> dict:
        return _execute_trigger(
            jobs_path=req.jobs_path,
            config_path=req.config_path,
            triggered_by=req.triggered_by,
            config_overrides=req.config_overrides,
            run_mode=req.run_mode,
        )

    @app.post("/admin/upload-trigger", status_code=201)
    async def upload_trigger(
        jobs_files: list[UploadFile] = File(default_factory=list),
        jobs_file: UploadFile | None = File(None),
        jobs_path: str = Form("data/sample_jobs.json"),
        jobs_input_mode: str = Form("path"),      # "path" | "upload" | "paste"
        jobs_text: str = Form(""),
        config_path: str = Form("config/env.yaml"),
        run_mode: str = Form("run_all"),
        candidate_profile_mode: str = Form("default_config"),  # "default_config" | "upload" | "paste"
        candidate_profile_file: UploadFile | None = File(None),
        candidate_profile_text: str = Form(""),
        synonym_overlay_mode: str = Form("default_config"),
        overlay_upload_scope: str = Form("combined"),
        synonym_overlay_file: UploadFile | None = File(None),
    ) -> dict:
        _MAX_FILES = 20
        _MAX_TOTAL_BYTES = 50 * 1024 * 1024  # 50 MB

        upload_dir = Path("data/uploads")
        upload_dir.mkdir(parents=True, exist_ok=True)

        # ── Jobs input resolution ──────────────────────────────────────
        jobs_input_json_snapshot: str | None = None
        jobs_input_manifest_json: str | None = None
        if jobs_input_mode == "path":
            actual_jobs_path, jobs_input_json_snapshot = _resolve_jobs_path_snapshot(jobs_path)
            jobs_input_source = "path"
        elif jobs_input_mode == "upload":
            # Normalize: accept multi-file (jobs_files) or legacy single-file (jobs_file)
            effective_files: list[UploadFile] = []
            valid_jobs_files = [f for f in (jobs_files or []) if f and f.filename]
            if valid_jobs_files:
                effective_files = valid_jobs_files
            elif jobs_file and jobs_file.filename:
                effective_files = [jobs_file]

            if not effective_files:
                raise HTTPException(status_code=422, detail="jobs_file required for upload mode")

            if len(effective_files) > _MAX_FILES:
                raise HTTPException(
                    status_code=422,
                    detail=f"Too many files: {len(effective_files)} exceeds limit of {_MAX_FILES}",
                )

            # Read and validate each file individually before merging
            validated_arrays: list[list] = []
            total_bytes = 0
            for upload in effective_files:
                raw_bytes = await upload.read()
                total_bytes += len(raw_bytes)
                if total_bytes > _MAX_TOTAL_BYTES:
                    raise HTTPException(
                        status_code=422,
                        detail=f"Total upload size exceeds limit of {_MAX_TOTAL_BYTES // (1024 * 1024)} MB",
                    )
                filename = upload.filename or "<unknown>"
                try:
                    decoded = raw_bytes.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise HTTPException(
                        status_code=422,
                        detail=f"Invalid jobs JSON in {filename}: {exc}",
                    )
                try:
                    parsed = _json.loads(decoded)
                except _json.JSONDecodeError as exc:
                    raise HTTPException(
                        status_code=422,
                        detail=f"Invalid jobs JSON in {filename}: {exc}",
                    )
                if not isinstance(parsed, list):
                    raise HTTPException(
                        status_code=422,
                        detail=f"Invalid jobs JSON in {filename}: top-level value must be a JSON array",
                    )
                validated_arrays.append(parsed)

            # Merge in submitted file order, preserving row order within each file
            merged_jobs: list = []
            for arr in validated_arrays:
                merged_jobs.extend(arr)

            if not merged_jobs:
                raise HTTPException(
                    status_code=422,
                    detail="Merged upload is empty: all uploaded files contain empty arrays",
                )

            # Serialize once and write the canonical merged file
            canonical_merged = _json.dumps(merged_jobs, ensure_ascii=False, indent=2)
            merged_filename = f"{uuid.uuid4().hex}_merged_jobs.json"
            save_path = upload_dir / merged_filename
            save_path.write_text(canonical_merged, encoding="utf-8")
            actual_jobs_path = str(save_path)
            jobs_input_source = "upload"
            jobs_input_json_snapshot = canonical_merged
            jobs_input_manifest_json = _json.dumps(
                {
                    "source_filenames": [
                        str(upload.filename or "").strip()
                        for upload in effective_files
                        if str(upload.filename or "").strip()
                    ],
                },
                ensure_ascii=False,
            )
        elif jobs_input_mode == "paste":
            if not jobs_text or not jobs_text.strip():
                raise HTTPException(status_code=422, detail="jobs_text required for paste mode")
            try:
                parsed_jobs = _json.loads(jobs_text)
            except _json.JSONDecodeError as exc:
                raise HTTPException(status_code=422, detail=f"Invalid JSON in jobs_text: {exc}")
            if not isinstance(parsed_jobs, list):
                raise HTTPException(status_code=422, detail="jobs_text must be a JSON array of objects")
            canonical = _json.dumps(parsed_jobs, ensure_ascii=False, indent=2)
            paste_file = upload_dir / f"{uuid.uuid4().hex}_pasted_jobs.json"
            paste_file.write_text(canonical, encoding="utf-8")
            actual_jobs_path = str(paste_file)
            jobs_input_source = "paste"
            jobs_input_json_snapshot = canonical
        else:
            raise HTTPException(status_code=422, detail=f"Unknown jobs_input_mode: {jobs_input_mode!r}")

        # ── Candidate profile resolution ─────────────────────────────────
        candidate_json_snapshot: str | None = None
        if candidate_profile_mode == "default_config":
            candidate_json_snapshot = _resolve_default_candidate_profile_snapshot(config_path)
            candidate_profile_source = "default_config"
        elif candidate_profile_mode == "upload":
            if not candidate_profile_file or not candidate_profile_file.filename:
                raise HTTPException(status_code=422, detail="candidate_profile_file required for upload mode")
            raw_bytes = await candidate_profile_file.read()
            raw_text = raw_bytes.decode("utf-8")
            candidate_json_snapshot = _resolve_candidate_profile_snapshot_from_text(raw_text)
            candidate_profile_source = "upload"
        elif candidate_profile_mode == "paste":
            if not candidate_profile_text or not candidate_profile_text.strip():
                raise HTTPException(status_code=422, detail="candidate_profile_text required for paste mode")
            candidate_json_snapshot = _resolve_candidate_profile_snapshot_from_text(candidate_profile_text)
            candidate_profile_source = "paste"
        else:
            raise HTTPException(status_code=422, detail=f"Unknown candidate_profile_mode: {candidate_profile_mode!r}")

        # ── Run-scoped synonym overlay resolution ───────────────────────
        synonym_overlay_payload: dict[str, Any] | None = None
        synonym_overlay_filename: str | None = None
        synonym_overlay_raw_yaml: str | None = None
        if synonym_overlay_mode == "default_config":
            pass
        elif synonym_overlay_mode == "upload":
            synonym_overlay_filename, synonym_overlay_payload, synonym_overlay_raw_yaml = await _parse_uploaded_synonym_overlay(
                synonym_overlay_file,
                overlay_upload_scope=overlay_upload_scope,
                missing_file_detail="synonym_overlay_file required for upload mode",
                missing_filename_detail="synonym_overlay_file required for upload mode",
            )
        else:
            raise HTTPException(status_code=422, detail=f"Unknown synonym_overlay_mode: {synonym_overlay_mode!r}")

        return _execute_trigger_with_inputs(
            jobs_path=actual_jobs_path,
            config_path=config_path,
            triggered_by="admin",
            config_overrides={},
            jobs_input_source=jobs_input_source,
            jobs_input_json=jobs_input_json_snapshot,
            jobs_input_manifest_json=jobs_input_manifest_json,
            candidate_profile_source=candidate_profile_source,
            candidate_profile_json=candidate_json_snapshot,
            run_synonym_overlay=synonym_overlay_payload,
            run_synonym_overlay_filename=synonym_overlay_filename,
            run_synonym_overlay_raw_yaml=synonym_overlay_raw_yaml,
            run_synonym_overlay_source="trigger_upload",
            run_mode=run_mode,
        )

    @app.get("/runs")
    def get_runs_list() -> list:
        runs = list_runs(bq, project=project, dataset=dataset)
        runs = [_reconcile_orphaned_run(run) for run in runs]
        return [_run_to_dict(r) for r in runs]

    @app.get("/runs/{run_id}")
    def get_run_detail(run_id: str) -> dict:
        run = require_run_or_404(run_id)
        run = _reconcile_orphaned_run(run)
        return _run_to_dict(run)

    @app.get("/runs/{run_id}/events")
    def get_run_events_list(run_id: str) -> list:
        _ = require_run_or_404(run_id)
        events = get_events(run_id, bq, project=project, dataset=dataset)
        return [
            {
                "event_id": e.event_id,
                "stage": e.stage,
                "level": e.level,
                "message": e.message,
                "payload_json": e.payload_json,
                "created_at": e.created_at.isoformat() if hasattr(e.created_at, "isoformat") else str(e.created_at),
            }
            for e in events
        ]

    @app.get("/settings")
    def get_settings_view() -> dict:
        return load_active_settings(bq=bq, project=project, dataset=dataset)

    def _coerce_and_validate_single_setting(
        key: str,
        raw_value: Any,
    ) -> Any:
        try:
            coerced = coerce_value(key, raw_value)
        except KeyError:
            raise HTTPException(status_code=422, detail=f"Unknown setting key: {key!r}")
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        try:
            validate_settings({key: coerced})
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        return coerced

    async def _parse_uploaded_synonym_overlay(
        synonym_overlay_file: UploadFile | None,
        *,
        overlay_upload_scope: str,
        missing_file_detail: str,
        missing_filename_detail: str,
    ) -> tuple[str, dict[str, Any], str]:
        if not synonym_overlay_file:
            raise HTTPException(status_code=422, detail=missing_file_detail)
        filename = str(synonym_overlay_file.filename or "").strip()
        if not filename:
            raise HTTPException(status_code=422, detail=missing_filename_detail)
        raw_bytes = await synonym_overlay_file.read()
        if not raw_bytes:
            raise HTTPException(status_code=422, detail="Uploaded synonym overlay file is empty")
        try:
            raw_text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(status_code=422, detail="Synonym overlay must be UTF-8 encoded text") from exc
        try:
            overlay_payload = parse_runtime_synonym_overlay_yaml(raw_text)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        _validate_overlay_scope(overlay_payload, overlay_upload_scope)
        return filename, overlay_payload, raw_text

    @app.post("/settings/{key}", status_code=200)
    def update_setting(key: str, body: SettingUpdate) -> dict:
        if key in metadata_only_keys:
            raise HTTPException(status_code=422, detail=f"Setting '{key}' is metadata-only and cannot be saved through single-key routes")
        if key in hidden_deprecated_keys:
            raise HTTPException(
                status_code=422,
                detail=f"Setting '{key}' is hidden_deprecated and cannot be saved through settings routes",
            )
        coerced = _coerce_and_validate_single_setting(key, body.value)
        save_setting(key, coerced, updated_by=body.updated_by, bq=bq, project=project, dataset=dataset)
        return {"key": key, "value": coerced}

    @app.get("/admin/settings", response_class=HTMLResponse)
    def admin_settings_view(request: Request) -> HTMLResponse:
        active = load_active_settings(bq=bq, project=project, dataset=dataset)
        return templates.TemplateResponse(
            request=request,
            name="settings.html",
            context=_build_settings_context(active),
        )

    @app.post("/admin/settings/{key}", response_class=HTMLResponse)
    async def admin_settings_update_key(request: Request, key: str) -> HTMLResponse:
        from fastapi.responses import RedirectResponse
        form = await request.form()
        value = form.get("value", "")
        if key in metadata_only_keys:
            active = load_active_settings(bq=bq, project=project, dataset=dataset)
            return templates.TemplateResponse(
                request=request,
                name="settings.html",
                context=_build_settings_context(
                    active,
                    error=f"Setting '{key}' is metadata-only and cannot be saved through single-key routes",
                ),
                status_code=422,
            )
        if key in hidden_deprecated_keys:
            active = load_active_settings(bq=bq, project=project, dataset=dataset)
            return templates.TemplateResponse(
                request=request,
                name="settings.html",
                context=_build_settings_context(
                    active,
                    error=f"Setting '{key}' is hidden_deprecated and cannot be saved through settings routes",
                ),
                status_code=422,
            )
        try:
            coerced = _coerce_and_validate_single_setting(key, value)
        except HTTPException as exc:
            active = load_active_settings(bq=bq, project=project, dataset=dataset)
            return templates.TemplateResponse(
                request=request,
                name="settings.html",
                context=_build_settings_context(active, error=str(exc.detail)),
                status_code=422,
            )
        save_setting(key, coerced, updated_by="admin", bq=bq, project=project, dataset=dataset)
        return RedirectResponse("/admin/settings", status_code=303)

    @app.post("/admin/settings/group/{group_name}", response_class=HTMLResponse)
    async def admin_settings_update_group(
        request: Request, group_name: str
    ) -> HTMLResponse:
        from uuid import uuid4
        from fastapi.responses import RedirectResponse

        # Resolve group across both namespaces (ranking + cv)
        target_registry: dict[str, list[str]] | None = None
        for registry in ALL_GROUP_REGISTRIES.values():
            if group_name in registry:
                target_registry = registry
                break
        if target_registry is None:
            raise HTTPException(status_code=404, detail=f"Unknown group: {group_name!r}")

        keys = [key for key in target_registry[group_name] if key not in hidden_deprecated_keys]
        form = await request.form()
        active = load_active_settings(bq=bq, project=project, dataset=dataset)
        hidden_submitted = sorted(
            key for key in hidden_deprecated_keys if key in form
        )

        # Coerce all keys in the group
        coerced: dict = {}
        coerce_errors: list[str] = []
        for key in keys:
            if key in metadata_only_keys and key not in form:
                raw = active.get(key, schema_by_key[key]["default"])
            else:
                raw = _settings_form_value(form, key)
            try:
                coerced[key] = coerce_value(key, raw)
            except (KeyError, ValueError) as exc:
                coerce_errors.append(str(exc))

        def _error_response(msg: str) -> HTMLResponse:
            return templates.TemplateResponse(
                request=request,
                name="settings.html",
                context=_build_settings_context(
                    active,
                    group_error={group_name: msg},
                    group_draft={group_name: {key: _settings_form_value(form, key) for key in keys}},
                    active_group_name=group_name,
                ),
                status_code=422,
            )

        if hidden_submitted:
            return _error_response(
                "Hidden deprecated settings are not writable via settings routes: "
                + ", ".join(hidden_submitted)
            )

        if coerce_errors:
            return _error_response("; ".join(coerce_errors))

        # Validate full group as one coherent payload — no write occurs on failure
        try:
            validate_settings(coerced)
        except ValidationError as exc:
            return _error_response(str(exc))

        # Generate shared audit identity for this grouped save
        update_id = str(uuid4())
        updated_by = f"admin:grp:{update_id}"

        # Write — surface BQ failures to the user
        try:
            save_settings_group(
                _filter_canonical_settings_payload(coerced),
                updated_by=updated_by,
                bq=bq,
                project=project,
                dataset=dataset,
            )
        except RuntimeError as exc:
            return _error_response(f"Save failed: {exc}")

        return RedirectResponse("/admin/settings", status_code=303)

    @app.post("/admin/settings/section/{section_name}", response_class=HTMLResponse)
    async def admin_settings_section_save(
        request: Request, section_name: str
    ) -> HTMLResponse:
        """Section-level save for retrieval, timing, and global-job-filters.

        Each key is validated independently (no cross-key constraints within a section).
        A 422 is returned if any value fails validation, with section_errors populated
        so the template can highlight offending fields.
        """
        from uuid import uuid4
        from fastapi.responses import RedirectResponse as _Redirect

        if section_name not in all_settings_sections:
            raise HTTPException(status_code=404, detail=f"Unknown section: {section_name!r}")

        keys = [key for key in all_settings_sections[section_name] if key not in hidden_deprecated_keys]
        if section_name == "timing":
            # Runtime throughput compatibility aliases are read-only display surfaces.
            # Section-save accepts canonical stage_runtime keys only.
            keys = [key for key in keys if key not in compatibility_runtime_alias_keys]
        form = await request.form()
        active = load_active_settings(bq=bq, project=project, dataset=dataset)
        hidden_submitted = sorted(
            key for key in hidden_deprecated_keys if key in form
        )

        coerced: dict = {}
        section_errors: dict[str, str] = {}

        for key in keys:
            if key in metadata_only_keys and key not in form:
                raw = active.get(key, schema_by_key[key]["default"])
            else:
                raw = _settings_form_value(form, key)
            try:
                coerced[key] = _coerce_and_validate_single_setting(key, raw)
            except HTTPException as exc:
                section_errors[key] = str(exc.detail)

        if section_name == "agentic-automation" and not section_errors:
            enable_apply_prereq = str(form.get("__enable_prereq_apply_to_run", "")).strip().lower() in {"true", "1", "yes", "on"}
            enable_promote_prereq = str(form.get("__enable_prereq_promote_global", "")).strip().lower() in {"true", "1", "yes", "on"}

            if enable_apply_prereq:
                coerced["synonym_management.apply_to_run_enabled"] = True
            if enable_promote_prereq:
                coerced["synonym_management.promote_global_enabled"] = True

            apply_gate_on = bool(
                coerced.get(
                    "synonym_management.apply_to_run_enabled",
                    active.get("synonym_management.apply_to_run_enabled", schema_by_key["synonym_management.apply_to_run_enabled"]["default"]),
                )
            )
            promote_gate_on = bool(
                coerced.get(
                    "synonym_management.promote_global_enabled",
                    active.get("synonym_management.promote_global_enabled", schema_by_key["synonym_management.promote_global_enabled"]["default"]),
                )
            )
            auto_apply_on = bool(coerced.get("synonym_management.auto_apply_recommendation_enabled", False))
            auto_promote_on = bool(coerced.get("synonym_management.auto_promote_global_enabled", False))

            if auto_apply_on and not apply_gate_on:
                section_errors["synonym_management.auto_apply_recommendation_enabled"] = (
                    "Auto Apply Recommendation requires Synonym Apply-to-Run gate enabled. "
                    "Enable prerequisite and continue, or keep Auto Apply off."
                )
            if auto_promote_on and not promote_gate_on:
                section_errors["synonym_management.auto_promote_global_enabled"] = (
                    "Auto Promote to Global requires Synonym Promote-Global gate enabled. "
                    "Enable prerequisite and continue, or keep Auto Promote off."
                )

        # Run cross-key validation across all coerced values in this section
        if not section_errors:
            try:
                validate_settings(coerced)
            except ValidationError as exc:
                section_errors[keys[0]] = str(exc)

        def _section_error_response(errors: dict[str, str]) -> HTMLResponse:
            return templates.TemplateResponse(
                request=request,
                name="settings.html",
                context=_build_settings_context(
                    active,
                    section_errors=errors,
                    section_draft={key: _settings_form_value(form, key) for key in keys},
                    active_section_name=section_name,
                ),
                status_code=422,
            )

        if hidden_submitted:
            first_key = keys[0] if keys else section_name
            return _section_error_response(
                {
                    first_key: "Hidden deprecated settings are not writable via settings routes: "
                    + ", ".join(hidden_submitted)
                }
            )

        if section_errors:
            return _section_error_response(section_errors)

        update_id = str(uuid4())
        updated_by = f"admin:section:{update_id}"
        try:
            save_settings_group(
                _filter_canonical_settings_payload(coerced),
                updated_by=updated_by,
                bq=bq,
                project=project,
                dataset=dataset,
            )
        except RuntimeError as exc:
            return _section_error_response({keys[0]: f"Save failed: {exc}"})

        return _Redirect("/admin/settings", status_code=303)

    @app.get("/admin/runs", response_class=HTMLResponse)
    def admin_runs(request: Request) -> HTMLResponse:
        view = request.query_params.get("view", "active")
        if view == "archived":
            runs = list_runs(bq, project=project, dataset=dataset, archived_only=True)
        elif view == "all":
            runs = list_runs(bq, project=project, dataset=dataset, include_archived=True)
        else:  # default: active
            runs = list_runs(bq, project=project, dataset=dataset, include_archived=False)
        runs = [_reconcile_orphaned_run(run) for run in runs]
        runs = [_attach_jobs_path_display(run) for run in runs]
        max_runtime_minutes = _run_max_runtime_minutes()
        runs = [_enforce_run_timeout_guard(run, max_runtime_minutes=max_runtime_minutes) for run in runs]
        pipeline_runs_schema_status = get_pipeline_runs_schema_status(
            bq,
            project=project,
            dataset=dataset,
        )
        run_orchestration_diagnostics = {
            run.run_id: _build_orchestration_diagnostics(run)
            for run in runs
        }
        dead_letter_replay_health = _aggregate_dead_letter_replay_health(
            runs,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        run_status_projection = {run.run_id: _run_status_projection(run) for run in runs}
        return templates.TemplateResponse(
            request=request, name="runs_list.html",
            context={
                "runs": runs,
                "view": view,
                "pipeline_runs_schema_status": pipeline_runs_schema_status,
                "run_orchestration_diagnostics": run_orchestration_diagnostics,
                "dead_letter_replay_health": dead_letter_replay_health,
                "run_status_projection": run_status_projection,
            }
        )

    @app.get("/admin/outbox-replay-health.json")
    def admin_outbox_replay_health(view: str = "active") -> dict[str, Any]:
        if view == "archived":
            runs = list_runs(bq, project=project, dataset=dataset, archived_only=True)
        elif view == "all":
            runs = list_runs(bq, project=project, dataset=dataset, include_archived=True)
        else:
            runs = list_runs(bq, project=project, dataset=dataset, include_archived=False)
        aggregate = _aggregate_dead_letter_replay_health(
            runs,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        return {
            "view": view,
            "run_count": len(runs),
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "outbox_replay_health": aggregate,
        }

    @app.post("/admin/outbox-replay-health/check")
    def admin_outbox_replay_health_check(
        view: str = "active",
        min_replay_success_ratio: float | None = None,
        emit_event: bool = True,
        event_run_id: str = "system-outbox-replay-health",
    ) -> dict[str, Any]:
        if view == "archived":
            runs = list_runs(bq, project=project, dataset=dataset, archived_only=True)
        elif view == "all":
            runs = list_runs(bq, project=project, dataset=dataset, include_archived=True)
        else:
            runs = list_runs(bq, project=project, dataset=dataset, include_archived=False)
        aggregate = _aggregate_dead_letter_replay_health(
            runs,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        effective_min_ratio = (
            float(min_replay_success_ratio)
            if min_replay_success_ratio is not None
            else _default_outbox_replay_min_success_ratio()
        )
        ratio = float(aggregate.get("replay_success_ratio") or 0.0)
        degraded = str(aggregate.get("status") or "") == "degraded"
        ratio_below_threshold = ratio < effective_min_ratio
        alert_triggered = degraded or ratio_below_threshold
        decision = "alert" if alert_triggered else "ok"
        reason = []
        if degraded:
            reason.append("dead_letter_status_degraded")
        if ratio_below_threshold:
            reason.append("replay_ratio_below_threshold")
        reason_code = ",".join(reason) if reason else "healthy"
        payload = {
            "view": view,
            "run_count": len(runs),
            "min_replay_success_ratio": effective_min_ratio,
            "decision": decision,
            "reason_code": reason_code,
            "outbox_replay_health": aggregate,
        }
        if emit_event:
            append_event(
                RunEvent(
                    run_id=str(event_run_id or "system-outbox-replay-health"),
                    event_id=str(uuid.uuid4()),
                    stage="outbox_replay_health_alert",
                    level="warning" if alert_triggered else "info",
                    message=(
                        f"Outbox replay health check decision={decision} "
                        f"reason={reason_code} ratio={ratio:.4f}"
                    ),
                    payload_json=_json.dumps(payload, ensure_ascii=False),
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        return payload

    @app.post("/admin/runs/{run_id}/stop")
    def admin_stop_run(run_id: str) -> dict:
        """Stop a cancellable run. Returns JSON for fetch() callers."""
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _can_cancel_run(run):
            raise HTTPException(
                status_code=409,
                detail=f"Cannot stop run with status '{run.status.value}'",
            )
        now = datetime.datetime.now(datetime.timezone.utc)
        event_id = str(uuid.uuid4())
        if run.status == RunStatus.AWAITING_CONTINUE:
            update_run_status(
                run_id,
                RunStatus.CANCELLED,
                bq,
                project=project,
                dataset=dataset,
                finished_at=now,
            )
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=event_id,
                    stage="run_cancelled",
                    level="warning",
                    message="Run cancelled while awaiting manual continuation",
                    created_at=now,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            return {"status": "cancelled", "run_id": run_id}
        if run.status == RunStatus.QUEUED and run.queue_job_id:
            cancelled_in_queue = cancel_queued_run(run.queue_job_id, redis_url=redis_url)
            if cancelled_in_queue:
                # Job still in queue — mark directly cancelled
                request_run_cancel(run_id, "admin", RunStatus.CANCELLED.value, bq, project=project, dataset=dataset)
                append_event(
                    RunEvent(
                        run_id=run_id, event_id=event_id, stage="cancel_requested",
                        level="warning", message="Stop requested — cancelled from queue",
                        created_at=now,
                    ),
                    bq, project=project, dataset=dataset,
                )
                append_event(
                    RunEvent(
                        run_id=run_id, event_id=str(uuid.uuid4()), stage="run_cancelled",
                        level="warning", message="Run cancelled before pipeline execution",
                        created_at=now,
                    ),
                    bq, project=project, dataset=dataset,
                )
                return {"status": "cancelled", "run_id": run_id}
        if run.status == RunStatus.QUEUED and run.started_at is None:
            request_run_cancel(run_id, "admin", RunStatus.CANCELLED.value, bq, project=project, dataset=dataset)
            append_event(
                RunEvent(
                    run_id=run_id, event_id=event_id, stage="cancel_requested",
                    level="warning", message="Stop requested — cancelled before worker claim",
                    created_at=now,
                ),
                bq, project=project, dataset=dataset,
            )
            append_event(
                RunEvent(
                    run_id=run_id, event_id=str(uuid.uuid4()), stage="run_cancelled",
                    level="warning", message="Run cancelled before pipeline execution",
                    created_at=now,
                ),
                bq, project=project, dataset=dataset,
            )
            return {"status": "cancelled", "run_id": run_id}
        # Running (or queued but already claimed) — set cancelling
        request_run_cancel(run_id, "admin", RunStatus.CANCELLING.value, bq, project=project, dataset=dataset)
        append_event(
            RunEvent(
                run_id=run_id, event_id=event_id, stage="cancel_requested",
                level="warning", message="Stop requested — run will be cancelled at next checkpoint",
                created_at=now,
            ),
            bq, project=project, dataset=dataset,
        )
        return {"status": "cancelling", "run_id": run_id}

    @app.post("/admin/runs/bulk/cancel")
    def admin_bulk_cancel_runs(payload: BulkRunActionRequest) -> dict[str, Any]:
        processed_run_ids: list[str] = []
        skipped_items: list[dict[str, str]] = []
        for run_id in payload.run_ids:
            run = get_run(run_id, bq, project=project, dataset=dataset)
            if run is None:
                skipped_items.append({"run_id": run_id, "reason": "not_found"})
                continue
            if not _can_cancel_run(run):
                skipped_items.append({"run_id": run_id, "reason": "not_cancellable"})
                continue

            if run.status == RunStatus.AWAITING_CONTINUE:
                update_run_status(
                    run_id,
                    RunStatus.CANCELLED,
                    bq,
                    project=project,
                    dataset=dataset,
                    finished_at=datetime.datetime.now(datetime.timezone.utc),
                )
                append_event(
                    RunEvent(
                        run_id=run_id,
                        event_id=str(uuid.uuid4()),
                        stage="run_cancelled",
                        level="warning",
                        message="Run cancelled while awaiting manual continuation",
                        created_at=datetime.datetime.now(datetime.timezone.utc),
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
                processed_run_ids.append(run_id)
                continue

            target_status = RunStatus.CANCELLING.value
            if run.status == RunStatus.QUEUED:
                target_status = RunStatus.CANCELLED.value
                if run.queue_job_id:
                    cancelled_in_queue = cancel_queued_run(run.queue_job_id, redis_url=redis_url)
                    target_status = RunStatus.CANCELLED.value if cancelled_in_queue else RunStatus.CANCELLING.value
                elif run.started_at is not None:
                    target_status = RunStatus.CANCELLING.value

            request_run_cancel(run_id, "admin", target_status, bq, project=project, dataset=dataset)
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="cancel_requested",
                    level="warning",
                    message=(
                        "Bulk cancel requested — run cancelled from queue"
                        if target_status == RunStatus.CANCELLED.value
                        else "Bulk cancel requested — run will be cancelled at next checkpoint"
                    ),
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            processed_run_ids.append(run_id)

        return {
            "action": "cancel",
            "requested": len(payload.run_ids),
            "processed": len(processed_run_ids),
            "skipped": len(skipped_items),
            "processed_run_ids": processed_run_ids,
            "skipped_items": skipped_items,
        }

    @app.post("/admin/runs/bulk/archive")
    def admin_bulk_archive_runs(payload: BulkRunActionRequest) -> dict[str, Any]:
        processed_run_ids: list[str] = []
        skipped_items: list[dict[str, str]] = []
        for run_id in payload.run_ids:
            run = get_run(run_id, bq, project=project, dataset=dataset)
            if run is None:
                skipped_items.append({"run_id": run_id, "reason": "not_found"})
                continue
            if not _can_archive_run(run):
                skipped_items.append({"run_id": run_id, "reason": "not_archivable"})
                continue

            archive_run(run_id, "admin", bq, project=project, dataset=dataset)
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="run_archived",
                    level="info",
                    message="Run archived by admin (bulk action)",
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            processed_run_ids.append(run_id)

        return {
            "action": "archive",
            "requested": len(payload.run_ids),
            "processed": len(processed_run_ids),
            "skipped": len(skipped_items),
            "processed_run_ids": processed_run_ids,
            "skipped_items": skipped_items,
        }

    @app.post("/admin/runs/bulk/unarchive")
    def admin_bulk_unarchive_runs(payload: BulkRunActionRequest) -> dict[str, Any]:
        processed_run_ids: list[str] = []
        skipped_items: list[dict[str, str]] = []
        for run_id in payload.run_ids:
            run = get_run(run_id, bq, project=project, dataset=dataset)
            if run is None:
                skipped_items.append({"run_id": run_id, "reason": "not_found"})
                continue
            if not _can_unarchive_run(run):
                skipped_items.append({"run_id": run_id, "reason": "not_unarchivable"})
                continue

            unarchive_run(run_id, bq, project=project, dataset=dataset)
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="run_unarchived",
                    level="info",
                    message="Run unarchived by admin (bulk action)",
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            processed_run_ids.append(run_id)

        return {
            "action": "unarchive",
            "requested": len(payload.run_ids),
            "processed": len(processed_run_ids),
            "skipped": len(skipped_items),
            "processed_run_ids": processed_run_ids,
            "skipped_items": skipped_items,
        }

    @app.post("/admin/runs/{run_id}/synonym-overlay")
    async def admin_upload_run_synonym_overlay(
        request: Request,
        run_id: str,
        overlay_upload_scope: str = Form("combined"),
        synonym_overlay_file: UploadFile = File(...),
    ) -> RedirectResponse:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _can_upload_synonym_overlay(run):
            raise HTTPException(
                status_code=409,
                detail="Synonym overlay upload is only available for manual runs paused after enrich",
            )
        filename, overlay_payload, raw_text = await _parse_uploaded_synonym_overlay(
            synonym_overlay_file,
            overlay_upload_scope=overlay_upload_scope,
            missing_file_detail="A synonym overlay YAML file is required",
            missing_filename_detail="A synonym overlay YAML file is required",
        )

        effective_config = _load_run_effective_config_snapshot(run)
        uploaded_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        updated_config = apply_runtime_synonym_overlay(
            effective_config,
            overlay_payload,
            source="staged_override",
            filename=filename,
            uploaded_at=uploaded_at,
            raw_yaml=raw_text,
        )
        update_run_effective_settings(
            run_id,
            _json.dumps(updated_config, ensure_ascii=False),
            bq,
            project=project,
            dataset=dataset,
        )
        synonym_mode = dict(updated_config.get("synonym_management") or {})
        if bool(synonym_mode.get("propose_enabled", True)) and run.mapping_suggestions_json:
            mapping_payload = decode_json_object_or_none(run.mapping_suggestions_json) or {}
            suggestions = list(mapping_payload.get("suggestions") or [])
            synonym_payload_json = build_synonym_proposals_payload(
                run_id=run_id,
                summary={"mapping_suggestions": suggestions},
                created_at=datetime.datetime.now(datetime.timezone.utc),
                existing_payload_json=run.synonym_proposals_json,
                global_synonyms=dict(updated_config.get("skill_synonyms") or {}),
            )
            update_run_synonym_proposals(
                run_id,
                synonym_payload_json,
                bq,
                project=project,
                dataset=dataset,
            )
        append_event(
            RunEvent(
                run_id=run_id,
                event_id=str(uuid.uuid4()),
                stage="synonym_overlay_uploaded",
                level="info",
                message=(
                    "Run-scoped synonym overlay uploaded "
                    f"[scope={str(overlay_upload_scope or 'combined').strip().lower() or 'combined'}] "
                    f"({int(((updated_config.get('skill_synonyms_runtime') or {}).get('run_overlay_entry_count') or 0))} skill entries)"
                ),
                created_at=datetime.datetime.now(datetime.timezone.utc),
                payload_json=_json.dumps(
                    {
                        "scope": str(overlay_upload_scope or "combined").strip().lower() or "combined",
                        "filename": filename,
                        "entry_count": int(
                            ((updated_config.get("skill_synonyms_runtime") or {}).get("run_overlay_entry_count") or 0)
                        ),
                        "section_counts": dict(
                            ((updated_config.get("skill_synonyms_runtime") or {}).get("run_overlay_section_counts") or {})
                        ),
                    },
                    ensure_ascii=False,
                ),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        redirect_target = f"/admin/runs/{run_id}/review-queue"
        referer = str(request.headers.get("referer") or "").strip()
        if referer:
            parsed_referer = urlparse(referer)
            referer_path = str(parsed_referer.path or "").strip()
            allowed_paths = {
                f"/admin/runs/{run_id}",
                f"/admin/runs/{run_id}/review-queue",
            }
            if referer_path in allowed_paths:
                query_suffix = f"?{parsed_referer.query}" if parsed_referer.query else ""
                redirect_target = f"{referer_path}{query_suffix}"
        return RedirectResponse(redirect_target, status_code=303)

    @app.post("/admin/runs/{run_id}/continue")
    def admin_continue_run(request: Request, run_id: str) -> dict:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if run.run_mode != "manual_staged":
            raise HTTPException(status_code=409, detail="Only Stage by Stage runs can be continued")
        if run.status in {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.SUCCEEDED}:
            # Idempotent no-op for already-progressed/completed runs.
            return {"status": run.status.value, "run_id": run.run_id, "replay_mode": "noop"}
        if run.status != RunStatus.AWAITING_CONTINUE:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot continue run with status '{run.status.value}'",
            )
        canonical_next_stage = _canonical_continue_next_stage(run)
        if not canonical_next_stage:
            raise HTTPException(
                status_code=409,
                detail="Run has no canonical next stage to continue",
            )
        replay_mode = _resolve_replay_mode(request)
        effective_config = _load_run_effective_config_snapshot(run)
        checkpoint_replay = _checkpoint_replay_context(run)
        prior_policy_signature = str(checkpoint_replay.get("policy_envelope_signature") or "").strip()
        policy_signature = _policy_envelope_signature(effective_config)
        policy_registry_version = _policy_registry_version_from_config(effective_config)
        replay_source_run_id = str(checkpoint_replay.get("replay_source_run_id") or run.run_id)
        if replay_mode == "strict" and prior_policy_signature and prior_policy_signature != policy_signature:
            raise HTTPException(
                status_code=409,
                detail="Strict replay rejected: policy envelope drift detected",
            )
        runtime_inputs = dict(effective_config.get("runtime_inputs") or {})
        runtime_inputs["replay_context"] = {
            "replay_mode": replay_mode,
            "replay_source_run_id": replay_source_run_id,
            "policy_registry_version": policy_registry_version,
            "policy_envelope_signature": policy_signature,
            "requested_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "requested_by": "admin",
        }
        effective_config["runtime_inputs"] = runtime_inputs
        update_run_effective_settings(
            run_id,
            _json.dumps(effective_config, ensure_ascii=False),
            bq,
            project=project,
            dataset=dataset,
        )
        update_run_status(run.run_id, RunStatus.QUEUED, bq, project=project, dataset=dataset)
        update_run_checkpoint(
            run.run_id,
            bq,
            project=project,
            dataset=dataset,
            checkpoint_status="queued_for_continue",
            next_stage=canonical_next_stage,
            last_completed_stage=run.last_completed_stage,
            completed_stages=run.completed_stages,
            checkpoint_payload_json=run.checkpoint_payload_json,
        )
        try:
            _, queue_job_id = continue_run_with_job_id(
                run_id=run.run_id,
                jobs_path=run.jobs_path,
                config_path=run.config_path,
                triggered_by="admin",
                redis_url=redis_url,
            )
        except Exception as exc:
            # Keep manual-staged runs recoverable when queue submission fails.
            update_run_status(run.run_id, RunStatus.AWAITING_CONTINUE, bq, project=project, dataset=dataset)
            update_run_checkpoint(
                run.run_id,
                bq,
                project=project,
                dataset=dataset,
                checkpoint_status=run.checkpoint_status,
                next_stage=canonical_next_stage,
                last_completed_stage=run.last_completed_stage,
                completed_stages=run.completed_stages,
                checkpoint_payload_json=run.checkpoint_payload_json,
            )
            append_event(
                RunEvent(
                    run_id=run.run_id,
                    event_id=str(uuid.uuid4()),
                    stage="manual_continue_enqueue_failed",
                    level="error",
                    message="Manual continue enqueue failed; run restored to awaiting_continue",
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                    payload_json=_json.dumps(
                        {
                            "error": str(exc),
                            "replay_mode": replay_mode,
                            "requested_by": "admin",
                        },
                        ensure_ascii=False,
                    ),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            raise HTTPException(
                status_code=503,
                detail="Continue enqueue failed; run restored to awaiting_continue. Check queue/redis connectivity and retry.",
            ) from exc
        submission = _resolve_submission_binding(run.run_id, queue_job_id)
        _persist_run_orchestration_binding(
            run.run_id,
            queue_job_id=submission.queue_job_id,
            orchestration_backend=submission.backend,
            orchestration_run_id=submission.backend_run_id,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        _persist_run_queue_job_id(run.run_id, submission.queue_job_id, bq=bq, project=project, dataset=dataset)

        append_event(
            RunEvent(
                run_id=run.run_id,
                event_id=str(uuid.uuid4()),
                stage="manual_continue_requested",
                level="info",
                message=f"Manual run queued to continue from {canonical_next_stage} ({replay_mode})",
                created_at=datetime.datetime.now(datetime.timezone.utc),
                payload_json=_json.dumps(
                    {
                        "replay_mode": replay_mode,
                        "replay_source_run_id": replay_source_run_id,
                        "policy_registry_version": policy_registry_version,
                        "policy_envelope_signature": policy_signature,
                    },
                    ensure_ascii=False,
                ),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        return {"status": "queued", "run_id": run.run_id, "replay_mode": replay_mode}

    @app.post("/admin/runs/{run_id}/retry")
    def admin_retry_run(run_id: str) -> dict[str, Any]:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if run.status == RunStatus.CANCELLED:
            raise HTTPException(status_code=409, detail="Cancelled runs cannot be retried")
        if getattr(run, "cancel_requested_at", None) is not None:
            raise HTTPException(status_code=409, detail="Cancel requested: run cannot be retried")
        if run.status not in {RunStatus.FAILED, RunStatus.QUEUED, RunStatus.RUNNING}:
            raise HTTPException(status_code=409, detail=f"Cannot retry run with status '{run.status.value}'")

        store = _resolve_run_store(bq, project=project, dataset=dataset)
        attempt_payloads = store.list_run_attempt_payloads(run_id)
        attempt_ids = {
            str((payload.get("attempt") or {}).get("attempt_id") or "").strip()
            for payload in attempt_payloads
            if isinstance(payload, dict)
        }
        attempt_ids.discard("")
        attempt_count = len(attempt_ids)
        from fitcv_cp.retry_settings import load_retry_settings

        max_attempts = load_retry_settings().max_attempts
        if attempt_count >= max_attempts:
            raise HTTPException(status_code=409, detail="Retry rejected: max_attempts exhausted")

        update_run_status(run.run_id, RunStatus.QUEUED, bq, project=project, dataset=dataset)
        _, queue_job_id = enqueue_run_with_job_id(
            jobs_path=run.jobs_path,
            config_path=run.config_path,
            triggered_by="admin_retry",
            redis_url=redis_url,
            run_id=run.run_id,
        )
        submission = _resolve_submission_binding(run.run_id, queue_job_id)
        _persist_run_orchestration_binding(
            run.run_id,
            queue_job_id=submission.queue_job_id,
            orchestration_backend=submission.backend,
            orchestration_run_id=submission.backend_run_id,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        _persist_run_queue_job_id(run.run_id, submission.queue_job_id, bq=bq, project=project, dataset=dataset)

        append_event(
            RunEvent(
                run_id=run.run_id,
                event_id=str(uuid.uuid4()),
                stage="retry_requested",
                level="info",
                message="Run retry queued by admin",
                created_at=datetime.datetime.now(datetime.timezone.utc),
                payload_json=_json.dumps(
                    {
                        "attempt_count": attempt_count,
                        "max_attempts": max_attempts,
                        "queue_job_id": submission.queue_job_id,
                    },
                    ensure_ascii=False,
                ),
            ),
            bq,
            project=project,
            dataset=dataset,
        )

        return {
            "status": "queued",
            "run_id": run.run_id,
            "queue_job_id": submission.queue_job_id,
            "attempt_count": attempt_count,
            "max_attempts": max_attempts,
        }

    @app.post("/admin/runs/{run_id}/archive")
    def admin_archive_run(run_id: str) -> dict:
        """Archive a terminal run. Returns JSON for fetch() callers."""
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _can_archive_run(run):
            raise HTTPException(
                status_code=409,
                detail=f"Cannot archive run with status '{run.status.value}'",
            )
        archive_run(run_id, "admin", bq, project=project, dataset=dataset)
        append_event(
            RunEvent(
                run_id=run_id, event_id=str(uuid.uuid4()), stage="run_archived",
                level="info", message="Run archived by admin",
                created_at=datetime.datetime.now(datetime.timezone.utc),
            ),
            bq, project=project, dataset=dataset,
        )
        return {"status": "archived", "run_id": run_id}

    @app.post("/admin/runs/{run_id}/repair-cancellation")
    def admin_repair_cancellation(run_id: str) -> dict:
        """Repair a stale cancelling run that never actually started."""
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _is_stale_cancelling(run):
            raise HTTPException(
                status_code=409,
                detail=f"Cannot repair run with status '{run.status.value}'",
            )
        now = datetime.datetime.now(datetime.timezone.utc)
        update_run_status(
            run_id,
            RunStatus.CANCELLED,
            bq,
            project=project,
            dataset=dataset,
            finished_at=now,
        )
        append_event(
            RunEvent(
                run_id=run_id,
                event_id=str(uuid.uuid4()),
                stage="run_cancelled",
                level="warning",
                message="Run repaired from stale cancelling state",
                created_at=now,
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        return {"status": "cancelled", "run_id": run_id}

    @app.post("/admin/runs/{run_id}/unarchive")
    def admin_unarchive_run(run_id: str) -> dict:
        """Unarchive a run, returning it to the active list. Returns JSON for fetch() callers."""
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _can_unarchive_run(run):
            raise HTTPException(status_code=409, detail="Run is not archived")
        unarchive_run(run_id, bq, project=project, dataset=dataset)
        append_event(
            RunEvent(
                run_id=run_id, event_id=str(uuid.uuid4()), stage="run_unarchived",
                level="info", message="Run unarchived by admin",
                created_at=datetime.datetime.now(datetime.timezone.utc),
            ),
            bq, project=project, dataset=dataset,
        )
        return {"status": "unarchived", "run_id": run_id}

    @app.post("/admin/runs/{run_id}/replay-dead-letter-events")
    def admin_replay_dead_letter_events(run_id: str) -> dict:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        dead_letter_file = _event_dead_letter_path()
        try:
            records = _load_event_dead_letter_records(dead_letter_file)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to read dead-letter file: {exc}") from exc
        replay_candidates: list[dict[str, Any]] = []
        kept_records: list[dict[str, Any]] = []
        for record in records:
            row = dict(record.get("row") or {})
            if str(row.get("run_id") or "").strip() == run_id:
                replay_candidates.append(record)
            else:
                kept_records.append(record)

        replayed = 0
        failed = 0
        for record in replay_candidates:
            row = dict(record.get("row") or {})
            created_at_raw = str(row.get("created_at") or "").strip()
            created_at = datetime.datetime.now(datetime.timezone.utc)
            if created_at_raw:
                try:
                    created_at = datetime.datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                except Exception:
                    pass
            status = append_event(
                RunEvent(
                    run_id=str(row.get("run_id") or run_id),
                    event_id=str(row.get("event_id") or str(uuid.uuid4())),
                    stage=str(row.get("stage") or "unknown"),
                    level=str(row.get("level") or "warning"),
                    message=str(row.get("message") or "Replayed dead-letter event"),
                    payload_json=(
                        str(row.get("payload_json"))
                        if row.get("payload_json") is not None
                        else None
                    ),
                    created_at=created_at,
                ),
                bq,
                project=project,
                dataset=dataset,
            )
            if status.get("persistence_status") == "persisted":
                replayed += 1
                continue
            failed += 1
            kept_records.append(record)
        try:
            _persist_event_dead_letter_records(dead_letter_file, kept_records)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to update dead-letter file: {exc}") from exc
        replay_success_ratio = float(replayed / len(replay_candidates)) if replay_candidates else 0.0
        append_event(
            RunEvent(
                run_id=run_id,
                event_id=str(uuid.uuid4()),
                stage="event_dead_letter_replay",
                level="warning" if failed else "info",
                message=(
                    f"Dead-letter replay completed: replayed={replayed} failed={failed} "
                    f"out_of={len(replay_candidates)}"
                ),
                payload_json=_json.dumps(
                    {
                        "replay_candidates": len(replay_candidates),
                        "replayed": replayed,
                        "failed": failed,
                        "replay_success_ratio": replay_success_ratio,
                        "remaining_dead_letter_total": len(kept_records),
                    },
                    ensure_ascii=False,
                ),
                created_at=datetime.datetime.now(datetime.timezone.utc),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        return {
            "status": "ok",
            "run_id": run_id,
            "replay_candidates": len(replay_candidates),
            "replayed": replayed,
            "failed": failed,
            "replay_success_ratio": replay_success_ratio,
            "remaining_dead_letter_total": len(kept_records),
        }

    @app.get("/admin/runs/{run_id}", response_class=HTMLResponse)
    def admin_run_detail(request: Request, run_id: str) -> HTMLResponse:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404)
        run = _reconcile_orphaned_run(run)
        run = _attach_jobs_path_display(run)
        run = _enforce_run_timeout_guard(run, max_runtime_minutes=_run_max_runtime_minutes())
        timeline_limit = _coerce_positive_int(
            request.query_params.get("timeline_limit"),
            default=25,
            minimum=10,
            maximum=200,
        )
        events = get_events(run_id, bq, project=project, dataset=dataset)
        stage_artifacts_by_id = _stage_artifacts_by_id(run)
        stage_quality_metrics = _stage_quality_metrics_from_stage_artifacts(stage_artifacts_by_id)
        stage_quality_metric_rows = _build_stage_quality_metric_rows(stage_quality_metrics)
        late_stage_reuse_metrics = _late_stage_reuse_metrics_from_stage_artifacts(stage_artifacts_by_id)
        late_stage_reuse_metric_rows = _build_late_stage_reuse_metric_rows(late_stage_reuse_metrics)
        run_health_rows = _build_run_health_rows(
            stage_quality_metric_rows,
            late_stage_reuse_metric_rows,
            stage_artifacts_by_id,
        )
        timeline_events: list[dict[str, Any]] = []
        visible_events = _collapse_timeline_noise(events[-timeline_limit:])
        visible_events = _dedupe_timeline_semantic_overlaps(visible_events)
        for ev, repeat_count in visible_events:
            stage_id = _timeline_stage_download_for_event(ev.stage)
            stage_download_url = None
            stage_download_label = None
            if (
                stage_id
                and _timeline_event_allows_stage_download(ev.stage)
                and _build_stage_slice_payload(run, stage_id) is not None
            ):
                stage_download_url = f"/admin/runs/{run_id}/stage-artifacts/{stage_id}.json"
                stage_download_label = _stage_download_label(stage_id)
            message_text = _timeline_stage_summary_message(ev, stage_artifacts_by_id)
            timeline_events.append(
                {
                    "created_at": ev.created_at,
                    "stage": ev.stage,
                    "stage_label": _timeline_stage_label(ev.stage),
                    "level": ev.level,
                    "message": message_text,
                    "message_html": _timeline_stage_summary_message_html(
                        ev,
                        message_text=message_text,
                    ),
                    "repeat_count": int(repeat_count),
                    "show_repeat_suffix": _timeline_show_repeat_suffix(ev),
                    "stage_id": stage_id,
                    "stage_download_url": stage_download_url,
                    "stage_download_label": stage_download_label,
                }
            )
        cv_versions = list_cvs_for_run(run_id, bq, project=project, dataset=dataset)
        results_rows = _results_export_rows(run)
        job_metadata_by_url = _job_metadata_by_url_from_results_rows(results_rows)
        cv_versions_with_bookmarks: list[dict[str, Any]] = []
        for cv in cv_versions:
            if not isinstance(cv, dict):
                continue
            row = dict(cv)
            job_url = str(row.get("job_url") or "").strip()
            metadata = job_metadata_by_url.get(job_url, {})
            job_title = str(row.get("job_title") or metadata.get("title") or "View Job").strip()
            company = str(row.get("company") or metadata.get("company") or "").strip()
            location = str(row.get("location") or metadata.get("location") or "").strip()
            bookmark_key = bookmark_key_for_job(
                job_id=str(row.get("job_id") or "").strip() or None,
                url=job_url or None,
                title=job_title,
            )
            row["bookmark_key"] = bookmark_key
            row["job_title"] = job_title
            row["company"] = company or None
            row["location"] = location or None
            row["job_primary_label"] = _build_job_primary_label(
                title=job_title,
                company=company,
                location=location,
            )
            row["bookmarked"] = is_job_bookmarked(
                job_id=str(row.get("job_id") or "").strip() or None,
                url=job_url or None,
                title=job_title,
            )
            cv_versions_with_bookmarks.append(row)
        output_availability = _build_output_availability(run, cv_versions)
        ranked_cv_outcome_summary = _build_ranked_cv_outcome_summary(results_rows)
        cv_generation_failure_reason_summary = _build_cv_generation_failure_reason_summary(run)
        reranker_blocked_ranked_count = sum(
            1
            for row in results_rows
            if str(row.get("pipeline_status") or "").strip() == "ranked_blocked_by_reranker_fit"
        )
        run_export_links = _build_run_export_links(run)
        agentic_runtime_drift = _run_agentic_runtime_drift_summary(run)
        hitl_review_queue = _build_hitl_review_queue(run)
        synonym_proposal_review_queue = _build_synonym_proposal_review_queue(run)
        synonym_proposal_decision_ledger = _build_synonym_proposal_decision_ledger(run)
        synonym_fingerprints = _synonym_observability_fingerprints(run)
        synonym_management_mode = _synonym_management_mode(run)
        synonym_review_section_state = _synonym_review_section_state(
            queue=synonym_proposal_review_queue,
            decision_ledger=synonym_proposal_decision_ledger,
            can_regenerate=_can_regenerate_synonym_proposals(run),
        )
        markdown_quality_summary = _build_markdown_quality_summary(run)
        event_delivery_health = _run_event_delivery_health(run_id)
        telemetry_export_health = _run_telemetry_export_health(events)
        langfuse_link_health = _run_langfuse_link_health(events)
        dead_letter_replay_summary = _latest_dead_letter_replay_summary(events)
        reuse_anomaly_summary = _latest_reuse_anomaly_summary(events)
        orchestration_diagnostics = _build_orchestration_diagnostics(run)
        from fitcv_cp.run_artifact_contracts import decode_run_attempt_payload_or_none
        run_attempt_events: list[dict[str, Any]] = []
        for event in events:
            payload = decode_run_attempt_payload_or_none(event.payload_json)
            if payload is None:
                continue
            attempt = payload.get("attempt") if isinstance(payload, dict) else None
            if not isinstance(attempt, dict):
                continue
            error = attempt.get("error") if isinstance(attempt.get("error"), dict) else {}
            retry = attempt.get("retry") if isinstance(attempt.get("retry"), dict) else {}
            run_attempt_events.append(
                {
                    "created_at": event.created_at,
                    "attempt_id": attempt.get("attempt_id"),
                    "status": attempt.get("status"),
                    "rq_job_id": attempt.get("rq_job_id"),
                    "worker_id": attempt.get("worker_id"),
                    "lease_expires_at": attempt.get("lease_expires_at"),
                    "finished_at": attempt.get("finished_at"),
                    "error_summary": error.get("summary"),
                    "retry_eligible": retry.get("eligible"),
                }
            )
        run_attempt_events.sort(key=lambda row: row.get("created_at") or datetime.datetime.min, reverse=True)
        replay_context_summary = _run_replay_context_summary(run)
        data_plane_summary = _run_data_plane_summary(run)
        stage_result_summary_rows = _stage_result_summary_rows(run)
        hitl_closure_summary = _build_hitl_closure_summary(run, queue=hitl_review_queue)
        hitl_review_pending_count = int(hitl_review_queue.get("pending_count") or 0)
        show_inline_review_queue = hitl_review_pending_count <= HITL_REVIEW_QUEUE_INLINE_THRESHOLD
        show_review_queue_cta = hitl_review_pending_count > HITL_REVIEW_QUEUE_INLINE_THRESHOLD

        return templates.TemplateResponse(
            request=request, name="run_detail.html", context={
                "run": run,
                "run_status_projection": _run_status_projection(run),
                "run_mode_label": run_mode_label(run.run_mode),
                "events": timeline_events,
                "timeline_has_more": len(events) > timeline_limit,
                "timeline_next_limit": min(timeline_limit + 25, 200),
                "cv_versions": cv_versions_with_bookmarks,
                "output_availability": output_availability,
                "stage_quality_metrics": stage_quality_metrics,
                "stage_quality_metric_rows": stage_quality_metric_rows,
                "late_stage_reuse_metrics": late_stage_reuse_metrics,
                "late_stage_reuse_metric_rows": late_stage_reuse_metric_rows,
                "run_health_rows": run_health_rows,
                "run_export_links": run_export_links,
                "job_metadata_by_url": job_metadata_by_url,
                "reranker_blocked_ranked_count": reranker_blocked_ranked_count,
                "ranked_cv_outcome_summary": ranked_cv_outcome_summary,
                "cv_generation_failure_reason_summary": cv_generation_failure_reason_summary,
                "is_stale_cancelling": _is_stale_cancelling,
                "can_continue_manual_run": (
                    run.run_mode == "manual_staged"
                    and run.status == RunStatus.AWAITING_CONTINUE
                    and bool(run.next_stage)
                ),
                "can_upload_synonym_overlay": _can_upload_synonym_overlay(run),
                "can_regenerate_synonym_proposals": _can_regenerate_synonym_proposals(run),
                "synonym_management_mode": synonym_management_mode,
                "synonym_overlay_info": _extract_run_synonym_overlay_info(run),
                "agentic_runtime_drift": agentic_runtime_drift,
                "hitl_review_queue": hitl_review_queue,
                "hitl_closure_summary": hitl_closure_summary,
                "review_queue_inline_threshold": HITL_REVIEW_QUEUE_INLINE_THRESHOLD,
                "show_inline_review_queue": show_inline_review_queue,
                "show_review_queue_cta": show_review_queue_cta,
                "review_queue_page_url": f"/admin/runs/{run_id}/review-queue",
                "synonym_proposal_review_queue": synonym_proposal_review_queue,
                "synonym_proposal_decision_ledger": synonym_proposal_decision_ledger,
                "synonym_fingerprints": synonym_fingerprints,
                "synonym_review_section_state": synonym_review_section_state,
                "markdown_quality_summary": markdown_quality_summary,
                "event_delivery_health": event_delivery_health,
                "telemetry_export_health": telemetry_export_health,
                "langfuse_link_health": langfuse_link_health,
                "dead_letter_replay_summary": dead_letter_replay_summary,
                "reuse_anomaly_summary": reuse_anomaly_summary,
                "orchestration_diagnostics": orchestration_diagnostics,
                "run_attempt_events": run_attempt_events,
                "replay_context_summary": replay_context_summary,
                "data_plane_summary": data_plane_summary,
                "stage_result_summary_rows": stage_result_summary_rows,
            }
        )

    @app.post("/admin/runs/{run_id}/bookmarks/save")
    async def admin_run_bookmark_save(request: Request, run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        form = await request.form()
        job_id = str(form.get("job_id") or "").strip() or None
        title = str(form.get("title") or "").strip()
        url = str(form.get("url") or "").strip()
        if not title or not url:
            raise HTTPException(status_code=422, detail="Bookmark requires title and url")
        version_id = str(form.get("version_id") or "").strip() or None
        upsert_bookmarked_job(
            job_id=job_id,
            title=title,
            company=str(form.get("company") or "").strip() or None,
            location=str(form.get("location") or "").strip() or None,
            url=url,
            fit_classification=str(form.get("fit_classification") or "").strip() or None,
            source_run_id=run_id,
            source=str(form.get("source") or "").strip() or "pipeline_results",
            snapshot={"version_id": version_id},
        )
        redirect_to = _safe_admin_redirect_target(
            str(form.get("redirect_to") or ""),
            fallback=_bookmark_destination_for_request(request, run_id),
        )
        return RedirectResponse(url=redirect_to, status_code=303)

    @app.post("/admin/runs/{run_id}/bookmarks/delete")
    async def admin_run_bookmark_delete(request: Request, run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        form = await request.form()
        bookmark_key = str(form.get("bookmark_key") or "").strip()
        if not bookmark_key:
            job_id = str(form.get("job_id") or "").strip() or None
            title = str(form.get("title") or "").strip() or None
            url = str(form.get("url") or "").strip() or None
            bookmark_key = bookmark_key_for_job(job_id=job_id, url=url, title=title)
        delete_bookmarked_job(bookmark_key)
        redirect_to = _safe_admin_redirect_target(
            str(form.get("redirect_to") or ""),
            fallback=_bookmark_destination_for_request(request, run_id),
        )
        return RedirectResponse(url=redirect_to, status_code=303)

    @app.post("/admin/reconciler/run-attempts")
    def admin_reconcile_run_attempts() -> dict[str, Any]:
        from fitcv_cp.reconciler import reconcile_abandoned_attempts

        store = _resolve_run_store(bq, project=project, dataset=dataset)
        summary = reconcile_abandoned_attempts(store)
        return {
            "scanned_runs": summary.scanned_runs,
            "abandoned_attempts": summary.abandoned_attempts,
            "requeued_attempts": summary.requeued_attempts,
            "terminal_failed_runs": summary.terminal_failed_runs,
        }

    @app.get("/admin/bookmarks", response_class=HTMLResponse)
    def admin_bookmarks(request: Request) -> HTMLResponse:
        view = str(request.query_params.get("view") or "all").strip().lower()
        allowed_views = {"all", "submitted", "archived"}
        if view not in allowed_views:
            view = "all"

        bookmarks = list_bookmarked_jobs()
        bookmarks_view: list[dict[str, Any]] = []
        for item in bookmarks:
            row = dict(item)
            fit_classification = str(row.get("fit_classification") or "").strip()
            snapshot = dict(row.get("snapshot") or {})
            version_id = str(snapshot.get("version_id") or "").strip() or None
            status = str(row.get("status") or "active").strip().lower()
            if status not in {"active", "submitted", "archived"}:
                status = "active"
            row["saved_at_display"] = _format_compact_utc_timestamp(row.get("saved_at")) or "—"
            row["fit_classification_display"] = fit_classification.lower() if fit_classification else None
            row["fit_classification_badge_class"] = _fit_classification_badge_class(fit_classification)
            row["version_id"] = version_id
            row["status"] = status
            row["job_primary_label"] = _build_job_primary_label(
                title=str(row.get("title") or "").strip() or "View Job",
                company=str(row.get("company") or "").strip() or None,
                location=str(row.get("location") or "").strip() or None,
            )
            bookmarks_view.append(row)

        active_bookmarks = [row for row in bookmarks_view if row.get("status") == "active"]
        submitted_bookmarks = [row for row in bookmarks_view if row.get("status") == "submitted"]
        archived_bookmarks = [row for row in bookmarks_view if row.get("status") == "archived"]

        if view == "submitted":
            visible_bookmarks = submitted_bookmarks
        elif view == "archived":
            visible_bookmarks = archived_bookmarks
        else:
            visible_bookmarks = bookmarks_view
        return templates.TemplateResponse(
            request=request,
            name="bookmarks.html",
            context={
                "view": view,
                "bookmarks": visible_bookmarks,
                "active_bookmarks": active_bookmarks,
                "submitted_bookmarks": submitted_bookmarks,
                "archived_bookmarks": archived_bookmarks,
            },
        )

    @app.post("/admin/bookmarks/delete")
    async def admin_bookmarks_delete(request: Request) -> Response:
        form = await request.form()
        bookmark_key = str(form.get("bookmark_key") or "").strip()
        if not bookmark_key:
            raise HTTPException(status_code=422, detail="bookmark_key is required")
        delete_bookmarked_job(bookmark_key)
        redirect_to = _safe_admin_redirect_target(
            str(form.get("redirect_to") or ""),
            fallback="/admin/bookmarks",
        )
        return RedirectResponse(url=redirect_to, status_code=303)

    @app.post("/admin/bookmarks/status")
    async def admin_bookmarks_status(request: Request) -> Response:
        form = await request.form()
        bookmark_key = str(form.get("bookmark_key") or "").strip()
        status = str(form.get("status") or "").strip()
        if not bookmark_key:
            raise HTTPException(status_code=422, detail="bookmark_key is required")
        if not status:
            raise HTTPException(status_code=422, detail="status is required")
        updated = set_bookmarked_job_status(bookmark_key, status)
        if not updated:
            raise HTTPException(status_code=404, detail="Bookmark not found")
        redirect_to = _safe_admin_redirect_target(
            str(form.get("redirect_to") or ""),
            fallback="/admin/bookmarks",
        )
        return RedirectResponse(url=redirect_to, status_code=303)

    @app.get("/admin/runs/{run_id}/review-queue", response_class=HTMLResponse)
    def admin_run_review_queue(request: Request, run_id: str) -> HTMLResponse:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404)
        run = _reconcile_orphaned_run(run)
        run = _enforce_run_timeout_guard(run, max_runtime_minutes=_run_max_runtime_minutes())
        hitl_review_queue = _build_hitl_review_queue(run)
        hitl_closure_summary = _build_hitl_closure_summary(run, queue=hitl_review_queue)
        return templates.TemplateResponse(
            request=request,
            name="review_queue.html",
            context={
                "run": run,
                "run_status_projection": _run_status_projection(run),
                "run_mode_label": run_mode_label(run.run_mode),
                "hitl_review_queue": hitl_review_queue,
                "hitl_closure_summary": hitl_closure_summary,
                "review_queue_inline_threshold": HITL_REVIEW_QUEUE_INLINE_THRESHOLD,
            },
        )

    @app.post("/admin/runs/{run_id}/cv-review-action")
    async def admin_run_cv_review_action(
        request: Request,
        run_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        form = await request.form()
        payload = CvReviewActionRequest(
            review_item_id=str(form.get("review_item_id") or "").strip() or None,
            job_url=str(form.get("job_url") or ""),
            action=str(form.get("action") or ""),
            actor=str(form.get("actor") or "admin"),
            note=str(form.get("note") or "").strip() or None,
        )
        allow_no_accepted_closure = str(form.get("confirm_no_accepted_cv_closure") or "").strip().lower() in {"1", "true", "yes", "on"}
        allowed_actions = {"approve", "approve_as_is", "regenerate_once", "reject"}
        if payload.action not in allowed_actions:
            raise HTTPException(status_code=422, detail="Invalid review action")

        debug_payload = _load_run_cv_generation_debug_payload(run)
        if not isinstance(debug_payload, dict):
            raise HTTPException(status_code=409, detail="No cv_generation_debug payload available")
        records = list(debug_payload.get("debug_records") or debug_payload.get("cv_generation_debug_records") or [])
        target_record = None
        selector_review_item_id = str(payload.review_item_id or "").strip()
        selector_job_url = str(payload.job_url or "").strip()
        for record in records:
            if not isinstance(record, dict):
                continue
            if str(record.get("status") or "").strip() != "review_required":
                continue
            record_review_item_id = str(record.get("review_item_id") or "").strip()
            record_job_url = str(record.get("job_url") or "").strip()
            if selector_review_item_id and record_review_item_id == selector_review_item_id:
                target_record = record
                break
            if selector_job_url and record_job_url == selector_job_url:
                target_record = record
                break
        if target_record is None:
            raise HTTPException(status_code=404, detail="Review-required record not found for selector")
        target_job_url = str(target_record.get("job_url") or "").strip()
        target_review_item_id = str(target_record.get("review_item_id") or "").strip() or None
        accepted_increment = 0
        finalized_version_id: str | None = None
        regeneration_job_id: str | None = None
        if payload.action == "approve_as_is":
            finalized_ok, finalized_reason, finalized_version_id = _finalize_review_draft_as_cv_artifact(
                run=run,
                job_url=target_job_url,
                record=target_record,
                bq=bq,
                project=project,
                dataset=dataset,
            )
            if not finalized_ok:
                raise HTTPException(status_code=409, detail=f"Cannot approve as final CV: {finalized_reason}")
            accepted_increment = 1
        elif payload.action == "regenerate_once":
            regeneration_job_id = enqueue_cv_regenerate_once_with_job_id(
                run_id=run_id,
                job_url=target_job_url,
                actor=payload.actor or "admin",
                note=payload.note,
                redis_url=redis_url,
            )

        now = datetime.datetime.now(datetime.timezone.utc)
        action_entry = {
            "review_item_id": target_review_item_id,
            "job_url": target_job_url,
            "job_title": str(target_record.get("job_title") or "").strip() or None,
            "action": payload.action,
            "resolution_status": _normalize_hitl_resolution_status(payload.action, None),
            "artifact_finalized": bool(accepted_increment),
            "artifact_version_id": finalized_version_id,
            "actor": payload.actor or "admin",
            "note": payload.note,
            "created_at": now.isoformat(),
        }
        if payload.action == "regenerate_once":
            action_entry["regeneration_requested_at"] = now.isoformat()
            action_entry["regeneration_job_id"] = regeneration_job_id
        review_actions = [item for item in list(debug_payload.get("hitl_review_actions") or []) if isinstance(item, dict)]
        review_actions.append(action_entry)
        debug_payload["hitl_review_actions"] = review_actions
        update_run_cv_generation_debug(
            run_id,
            _json.dumps(debug_payload, ensure_ascii=False),
            bq,
            project=project,
            dataset=dataset,
        )
        append_event(
            RunEvent(
                run_id=run_id,
                event_id=str(uuid.uuid4()),
                stage="cv_review_action",
                level="info",
                message=f"CV review action '{payload.action}' recorded",
                created_at=now,
                payload_json=_json.dumps(
                    {
                        "review_item_id": target_review_item_id,
                        "job_url": target_job_url,
                        "job_title": target_record.get("job_title"),
                        "action": payload.action,
                        "regeneration_job_id": regeneration_job_id,
                        "artifact_finalized": bool(accepted_increment),
                        "artifact_version_id": finalized_version_id,
                        "actor": payload.actor,
                        "note": payload.note,
                    },
                    ensure_ascii=False,
                ),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        if payload.action == "regenerate_once":
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="cv_regenerate_once_requested",
                    level="info",
                    message="Regenerate-once requested from review queue",
                    created_at=now,
                    payload_json=_json.dumps(
                        {
                            "review_item_id": target_review_item_id,
                            "job_url": target_job_url,
                            "job_title": target_record.get("job_title"),
                            "actor": payload.actor,
                            "note": payload.note,
                            "queue_job_id": regeneration_job_id,
                        },
                        ensure_ascii=False,
                    ),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        updated_run = dataclasses.replace(
            run,
            cv_generation_debug_json=_json.dumps(debug_payload, ensure_ascii=False),
            cvs_generated=int(run.cvs_generated or 0) + accepted_increment,
        )
        if accepted_increment:
            update_run_status(
                run_id,
                run.status,
                bq,
                project=project,
                dataset=dataset,
                summary={"cvs_generated": int(updated_run.cvs_generated or 0)},
            )
        queue_state = _build_hitl_review_queue(updated_run)
        if (
            _is_hitl_review_pending_state(run)
            and int(queue_state.get("total_review_required") or 0) > 0
            and int(queue_state.get("pending_count") or 0) == 0
        ):
            closure_summary = _build_hitl_closure_summary(updated_run, queue=queue_state)
            now = datetime.datetime.now(datetime.timezone.utc)
            if bool(closure_summary.get("requires_no_accepted_ack")) and not allow_no_accepted_closure:
                append_event(
                    RunEvent(
                        run_id=run_id,
                        event_id=str(uuid.uuid4()),
                        stage="cv_review_closure_blocked",
                        level="warning",
                        message="Review closure blocked: zero accepted CV artifacts. Confirm explicit closure to continue.",
                        created_at=now,
                        payload_json=_json.dumps(closure_summary, ensure_ascii=False),
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
                redirect_target = f"/admin/runs/{run_id}/review-queue"
                referer = str(request.headers.get("referer") or "").strip()
                if referer:
                    parsed_referer = urlparse(referer)
                    referer_path = str(parsed_referer.path or "").strip()
                    allowed_paths = {
                        f"/admin/runs/{run_id}",
                        f"/admin/runs/{run_id}/review-queue",
                    }
                    if referer_path in allowed_paths:
                        query_suffix = f"?{parsed_referer.query}" if parsed_referer.query else ""
                        redirect_target = f"{referer_path}{query_suffix}"
                return RedirectResponse(redirect_target, status_code=303)
            update_run_status(
                run_id,
                RunStatus.SUCCEEDED,
                bq,
                project=project,
                dataset=dataset,
                finished_at=now,
            )
            _persist_stage_artifacts_terminal_snapshot(
                run_id=run_id,
                bq=bq,
                project=project,
                dataset=dataset,
                terminal_status=RunStatus.SUCCEEDED,
                snapshot_at=now,
                snapshot_complete=True,
                degradation_reason="",
            )
            last_completed_stage, completed_stages, checkpoint_payload_json = _checkpoint_truth_for_review_closure(run)
            update_run_checkpoint(
                run_id,
                bq,
                project=project,
                dataset=dataset,
                checkpoint_status="completed",
                next_stage=None,
                last_completed_stage=last_completed_stage,
                completed_stages=completed_stages,
                checkpoint_payload_json=checkpoint_payload_json,
            )
            post_validation_auto_promote = _run_post_validation_auto_promote_global(
                run=dataclasses.replace(
                    updated_run,
                    status=RunStatus.SUCCEEDED,
                    checkpoint_status="completed",
                ),
                acted_by=payload.actor or "admin",
                note="auto:cv-review-closure",
                bq=bq,
                project=project,
                dataset=dataset,
            )
            closure_payload = _build_hitl_review_audit_payload(
                dataclasses.replace(
                    updated_run,
                    status=RunStatus.SUCCEEDED,
                    checkpoint_status="completed",
                )
            )
            _persist_post_hitl_closure_artifact_reconciliation(
                run_id=run_id,
                run=dataclasses.replace(
                    updated_run,
                    status=RunStatus.SUCCEEDED,
                    checkpoint_status="completed",
                ),
                closure_payload=closure_payload,
                bq=bq,
                project=project,
                dataset=dataset,
            )
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="cv_review_completed",
                    level="info",
                    message="All review-required CV items were resolved; run marked succeeded.",
                    created_at=now,
                    payload_json=_json.dumps(
                        {
                            "closure_mode": closure_payload.get("summary", {}).get("closure_mode"),
                            "review_required_total": closure_payload.get("summary", {}).get("review_required_total"),
                            "resolution_totals": closure_payload.get("summary", {}).get("resolution_totals"),
                            "synonym_auto_promote": post_validation_auto_promote,
                        },
                        ensure_ascii=False,
                    ),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        redirect_target = f"/admin/runs/{run_id}/review-queue"
        referer = str(request.headers.get("referer") or "").strip()
        if referer:
            parsed_referer = urlparse(referer)
            referer_path = str(parsed_referer.path or "").strip()
            allowed_paths = {
                f"/admin/runs/{run_id}",
                f"/admin/runs/{run_id}/review-queue",
            }
            if referer_path in allowed_paths:
                query_suffix = f"?{parsed_referer.query}" if parsed_referer.query else ""
                redirect_target = f"{referer_path}{query_suffix}"
        return RedirectResponse(redirect_target, status_code=303)

    @app.post("/admin/runs/{run_id}/cv-review-batch-action")
    async def admin_run_cv_review_batch_action(
        request: Request,
        run_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        form = await request.form()
        action = str(form.get("action") or "").strip()
        actor = str(form.get("actor") or "admin").strip() or "admin"
        note = str(form.get("note") or "").strip() or None
        allow_no_accepted_closure = str(form.get("confirm_no_accepted_cv_closure") or "").strip().lower() in {"1", "true", "yes", "on"}
        selected_review_item_ids = [str(value or "").strip() for value in form.getlist("review_item_id")]
        selected_review_item_ids = [value for value in selected_review_item_ids if value]
        selected_urls = [str(value or "").strip() for value in form.getlist("job_url")]
        selected_urls = [url for url in selected_urls if url]
        selected_selectors: list[tuple[str, str]] = [("review_item_id", value) for value in selected_review_item_ids]
        selected_selectors.extend(("job_url", value) for value in selected_urls)
        selected_selectors = list(dict.fromkeys(selected_selectors))
        allowed_actions = {"approve", "approve_as_is", "regenerate_once", "reject"}
        if action not in allowed_actions:
            raise HTTPException(status_code=422, detail="Invalid batch review action")
        if not selected_selectors:
            raise HTTPException(status_code=422, detail="Select at least one review-required row")

        debug_payload = _load_run_cv_generation_debug_payload(run)
        if not isinstance(debug_payload, dict):
            raise HTTPException(status_code=409, detail="No cv_generation_debug payload available")
        records = [item for item in list(debug_payload.get("debug_records") or debug_payload.get("cv_generation_debug_records") or []) if isinstance(item, dict)]
        review_required_by_review_item_id = {
            str(record.get("review_item_id") or "").strip(): record
            for record in records
            if str(record.get("status") or "").strip() == "review_required" and str(record.get("review_item_id") or "").strip()
        }
        review_required_by_job_url = {
            str(record.get("job_url") or "").strip(): record
            for record in records
            if str(record.get("status") or "").strip() == "review_required" and str(record.get("job_url") or "").strip()
        }

        review_actions = [item for item in list(debug_payload.get("hitl_review_actions") or []) if isinstance(item, dict)]
        latest_action_by_review_item_id: dict[str, dict[str, Any]] = {}
        latest_action_by_job: dict[str, dict[str, Any]] = {}
        for item in review_actions:
            review_item_id = str(item.get("review_item_id") or "").strip()
            if review_item_id:
                latest_action_by_review_item_id[review_item_id] = item
            job_url = str(item.get("job_url") or "").strip()
            if job_url:
                latest_action_by_job[job_url] = item

        applied = 0
        skipped = 0
        failed = 0
        finalized = 0
        regeneration_requested = 0
        failed_missing_draft = 0
        failed_persist = 0
        failed_truncated_draft = 0
        now = datetime.datetime.now(datetime.timezone.utc)
        for selector_type, selector_value in selected_selectors:
            target_record = None
            if selector_type == "review_item_id":
                target_record = review_required_by_review_item_id.get(selector_value)
            if target_record is None:
                target_record = review_required_by_job_url.get(selector_value)
            if target_record is None:
                failed += 1
                continue
            target_job_url = str(target_record.get("job_url") or "").strip()
            target_review_item_id = str(target_record.get("review_item_id") or "").strip() or None
            latest = (
                latest_action_by_review_item_id.get(target_review_item_id or "")
                or latest_action_by_job.get(target_job_url)
                or {}
            )
            latest_resolution = _normalize_hitl_resolution_status(
                str(latest.get("action") or "").strip() or None,
                str(latest.get("resolution_status") or "").strip() or None,
            )
            if not _is_hitl_resolution_pending(latest_resolution):
                skipped += 1
                continue
            finalized_version_id: str | None = None
            regeneration_job_id: str | None = None
            if action == "approve_as_is":
                finalized_ok, finalized_reason, finalized_version_id = _finalize_review_draft_as_cv_artifact(
                    run=run,
                    job_url=target_job_url,
                    record=target_record,
                    bq=bq,
                    project=project,
                    dataset=dataset,
                )
                if not finalized_ok:
                    failed += 1
                    if finalized_reason == "missing_draft_for_approve":
                        failed_missing_draft += 1
                    elif finalized_reason == "persist_failed":
                        failed_persist += 1
                    elif finalized_reason == "truncated_draft_blocked":
                        failed_truncated_draft += 1
                    continue
                finalized += 1
            elif action == "regenerate_once":
                try:
                    regeneration_job_id = enqueue_cv_regenerate_once_with_job_id(
                        run_id=run_id,
                        job_url=target_job_url,
                        actor=actor,
                        note=note,
                        redis_url=redis_url,
                    )
                except Exception:
                    failed += 1
                    continue
                regeneration_requested += 1
            action_entry = {
                "review_item_id": target_review_item_id,
                "job_url": target_job_url,
                "action": action,
                "resolution_status": _normalize_hitl_resolution_status(action, None),
                "artifact_finalized": bool(finalized_version_id),
                "artifact_version_id": finalized_version_id,
                "actor": actor,
                "note": note,
                "created_at": now.isoformat(),
            }
            if action == "regenerate_once":
                action_entry["regeneration_requested_at"] = now.isoformat()
                action_entry["regeneration_job_id"] = regeneration_job_id
            review_actions.append(action_entry)
            if target_review_item_id:
                latest_action_by_review_item_id[target_review_item_id] = action_entry
            if target_job_url:
                latest_action_by_job[target_job_url] = action_entry
            applied += 1

        debug_payload["hitl_review_actions"] = review_actions
        update_run_cv_generation_debug(
            run_id,
            _json.dumps(debug_payload, ensure_ascii=False),
            bq,
            project=project,
            dataset=dataset,
        )
        if applied > 0 or failed > 0:
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="cv_review_batch_action",
                    level="info",
                    message=(
                        "CV review batch action applied: "
                        f"action={action}, applied={applied}, skipped={skipped}, failed={failed}, finalized={finalized}, missing_draft={failed_missing_draft}, persist_failed={failed_persist}, truncated_draft={failed_truncated_draft}"
                    ),
                    created_at=now,
                    payload_json=_json.dumps(
                        {
                            "action": action,
                            "applied": applied,
                            "skipped": skipped,
                            "failed": failed,
                            "finalized": finalized,
                            "regeneration_requested": regeneration_requested,
                            "failed_missing_draft": failed_missing_draft,
                            "failed_persist": failed_persist,
                            "failed_truncated_draft": failed_truncated_draft,
                            "selected_count": len(selected_selectors),
                        },
                        ensure_ascii=False,
                    ),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        if action == "regenerate_once":
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="cv_regenerate_once_requested",
                    level="info",
                    message=(
                        "Regenerate-once requested from review batch action: "
                        f"requested={regeneration_requested}, failed={failed}, skipped={skipped}"
                    ),
                    created_at=now,
                    payload_json=_json.dumps(
                        {
                            "requested": regeneration_requested,
                            "failed": failed,
                            "skipped": skipped,
                            "selected_count": len(selected_selectors),
                        },
                        ensure_ascii=False,
                    ),
                ),
                bq,
                project=project,
                dataset=dataset,
            )

        updated_run = dataclasses.replace(
            run,
            cv_generation_debug_json=_json.dumps(debug_payload, ensure_ascii=False),
            cvs_generated=int(run.cvs_generated or 0) + finalized,
        )
        if finalized:
            update_run_status(
                run_id,
                run.status,
                bq,
                project=project,
                dataset=dataset,
                summary={"cvs_generated": int(updated_run.cvs_generated or 0)},
            )
        queue_state = _build_hitl_review_queue(updated_run)
        if (
            _is_hitl_review_pending_state(run)
            and int(queue_state.get("total_review_required") or 0) > 0
            and int(queue_state.get("pending_count") or 0) == 0
        ):
            closure_summary = _build_hitl_closure_summary(updated_run, queue=queue_state)
            finished_at = datetime.datetime.now(datetime.timezone.utc)
            if bool(closure_summary.get("requires_no_accepted_ack")) and not allow_no_accepted_closure:
                append_event(
                    RunEvent(
                        run_id=run_id,
                        event_id=str(uuid.uuid4()),
                        stage="cv_review_closure_blocked",
                        level="warning",
                        message="Review closure blocked: zero accepted CV artifacts. Confirm explicit closure to continue.",
                        created_at=finished_at,
                        payload_json=_json.dumps(closure_summary, ensure_ascii=False),
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
                query = urlencode(
                    {
                        "hitl_batch_applied": applied,
                        "hitl_batch_skipped": skipped,
                        "hitl_batch_failed": failed,
                        "hitl_batch_finalized": finalized,
                        "hitl_closure_blocked": 1,
                    }
                )
                redirect_target = f"/admin/runs/{run_id}/review-queue?{query}"
                referer = str(request.headers.get("referer") or "").strip()
                if referer:
                    parsed_referer = urlparse(referer)
                    referer_path = str(parsed_referer.path or "").strip()
                    allowed_paths = {
                        f"/admin/runs/{run_id}",
                        f"/admin/runs/{run_id}/review-queue",
                    }
                    if referer_path in allowed_paths:
                        redirect_target = f"{referer_path}?{query}"
                return RedirectResponse(redirect_target, status_code=303)
            update_run_status(
                run_id,
                RunStatus.SUCCEEDED,
                bq,
                project=project,
                dataset=dataset,
                finished_at=finished_at,
            )
            _persist_stage_artifacts_terminal_snapshot(
                run_id=run_id,
                bq=bq,
                project=project,
                dataset=dataset,
                terminal_status=RunStatus.SUCCEEDED,
                snapshot_at=finished_at,
                snapshot_complete=True,
                degradation_reason="",
            )
            last_completed_stage, completed_stages, checkpoint_payload_json = _checkpoint_truth_for_review_closure(run)
            update_run_checkpoint(
                run_id,
                bq,
                project=project,
                dataset=dataset,
                checkpoint_status="completed",
                next_stage=None,
                last_completed_stage=last_completed_stage,
                completed_stages=completed_stages,
                checkpoint_payload_json=checkpoint_payload_json,
            )
            post_validation_auto_promote = _run_post_validation_auto_promote_global(
                run=dataclasses.replace(
                    updated_run,
                    status=RunStatus.SUCCEEDED,
                    checkpoint_status="completed",
                ),
                acted_by=actor,
                note="auto:cv-review-closure",
                bq=bq,
                project=project,
                dataset=dataset,
            )
            closure_payload = _build_hitl_review_audit_payload(
                dataclasses.replace(
                    updated_run,
                    status=RunStatus.SUCCEEDED,
                    checkpoint_status="completed",
                )
            )
            _persist_post_hitl_closure_artifact_reconciliation(
                run_id=run_id,
                run=dataclasses.replace(
                    updated_run,
                    status=RunStatus.SUCCEEDED,
                    checkpoint_status="completed",
                ),
                closure_payload=closure_payload,
                bq=bq,
                project=project,
                dataset=dataset,
            )
            append_event(
                RunEvent(
                    run_id=run_id,
                    event_id=str(uuid.uuid4()),
                    stage="cv_review_completed",
                    level="info",
                    message="All review-required CV items were resolved; run marked succeeded.",
                    created_at=finished_at,
                    payload_json=_json.dumps(
                        {
                            "closure_mode": closure_payload.get("summary", {}).get("closure_mode"),
                            "review_required_total": closure_payload.get("summary", {}).get("review_required_total"),
                            "resolution_totals": closure_payload.get("summary", {}).get("resolution_totals"),
                            "synonym_auto_promote": post_validation_auto_promote,
                        },
                        ensure_ascii=False,
                    ),
                ),
                bq,
                project=project,
                dataset=dataset,
            )

        query = urlencode(
            {
                "hitl_batch_applied": applied,
                "hitl_batch_skipped": skipped,
                "hitl_batch_failed": failed,
                "hitl_batch_finalized": finalized,
            }
        )
        return RedirectResponse(f"/admin/runs/{run_id}/review-queue?{query}", status_code=303)

    @app.post("/admin/runs/{run_id}/synonym-proposals/{proposal_id}/action")
    async def admin_run_synonym_proposal_action(
        request: Request,
        run_id: str,
        proposal_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        mode = _synonym_management_mode(run)
        if not mode["apply_to_run_enabled"]:
            raise HTTPException(status_code=409, detail="Synonym apply-to-run actions are disabled by rollout settings")
        form = await request.form()
        action = str(form.get("action") or "").strip()
        actor = str(form.get("acted_by") or "admin").strip() or "admin"
        note = str(form.get("note") or "").strip()
        action_map = {
            "approve": "approve_for_run_overlay",
            "reject": "reject",
            "defer": "defer",
        }
        mapped_action = action_map.get(action)
        if not mapped_action:
            raise HTTPException(status_code=422, detail="Invalid synonym proposal action")
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal payload is not available for this run")
        idx = _find_synonym_proposal_index(payload, proposal_id)
        if idx is None:
            raise HTTPException(status_code=404, detail="Synonym proposal not found")
        result = _apply_synonym_proposal_action_in_run(
            run=run,
            payload=payload,
            proposal_index=idx,
            action=mapped_action,
            acted_by=actor,
            note=note,
        )
        return RedirectResponse(f"/admin/runs/{run_id}/synonym-review", status_code=303)

    @app.post("/admin/runs/{run_id}/synonym-proposals/batch-action")
    async def admin_run_synonym_proposals_batch_action(
        request: Request,
        run_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        mode = _synonym_management_mode(run)
        if not mode["apply_to_run_enabled"]:
            raise HTTPException(status_code=409, detail="Synonym apply-to-run actions are disabled by rollout settings")
        form = await request.form()
        acted_by = str(form.get("acted_by") or "admin").strip() or "admin"
        note = str(form.get("note") or "").strip()
        valid_actions = {"approve", "defer", "reject", "reopen_pending"}
        decisions: list[SynonymBatchDecision] = []

        selected_ids = [str(value or "").strip() for value in form.getlist("proposal_id")]
        selected_ids = [proposal_id for proposal_id in selected_ids if proposal_id]
        batch_action = str(form.get("batch_action") or "").strip()
        if selected_ids:
            for proposal_id in selected_ids:
                row_action = str(form.get(f"proposal_action__{proposal_id}") or "").strip()
                resolved_action = row_action if row_action in valid_actions else batch_action
                if resolved_action not in valid_actions:
                    continue
                decisions.append(SynonymBatchDecision(proposal_id=proposal_id, action=resolved_action))
        else:
            # Backward-compat fallback for older UI payload shape.
            for key, raw_value in form.multi_items():
                if not str(key).startswith("proposal_action__"):
                    continue
                proposal_id = str(key).split("proposal_action__", 1)[-1].strip()
                action = str(raw_value or "").strip()
                if not proposal_id or action not in valid_actions:
                    continue
                decisions.append(SynonymBatchDecision(proposal_id=proposal_id, action=action))
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal payload is not available for this run")
        seen: set[tuple[str, str]] = set()
        deduped_decisions: list[SynonymBatchDecision] = []
        for decision in decisions:
            key = (decision.proposal_id, decision.action)
            if key in seen:
                continue
            seen.add(key)
            deduped_decisions.append(decision)
        action_map = {
            "approve": "approve_for_run_overlay",
            "defer": "defer",
            "reject": "reject",
            "reopen_pending": "start_review",
        }
        if not deduped_decisions:
            raise HTTPException(status_code=422, detail="Select at least one synonym proposal row")
        applied = 0
        skipped = 0
        failed = 0
        for decision in deduped_decisions:
            idx = _find_synonym_proposal_index(payload, decision.proposal_id)
            if idx is None:
                skipped += 1
                continue
            try:
                _apply_synonym_proposal_action_in_run(
                    run=run,
                    payload=payload,
                    proposal_index=idx,
                    action=action_map[decision.action],
                    acted_by=acted_by,
                    note=note,
                    persist=False,
                )
            except HTTPException as exc:
                if exc.status_code == 409:
                    skipped += 1
                else:
                    failed += 1
                continue
            applied += 1

        if applied == 0 and failed == 0 and len(deduped_decisions) > 0 and skipped == len(deduped_decisions):
            recent_noop_guard_exists = any(
                event.stage == "synonym_noop_guard_triggered"
                for event in get_events(run.run_id, bq, project=project, dataset=dataset)[-10:]
            )
            if not recent_noop_guard_exists:
                append_event(
                    RunEvent(
                        run_id=run.run_id,
                        event_id=str(uuid.uuid4()),
                        stage="synonym_noop_guard_triggered",
                        level="info",
                        message=(
                            "Synonym batch decision loop suppressed: no actionable proposals remained "
                            f"(requested={len(deduped_decisions)}, skipped={skipped})."
                        ),
                        created_at=datetime.datetime.now(datetime.timezone.utc),
                        payload_json=_json.dumps(
                            {
                                "decisions_requested": len(deduped_decisions),
                                "skipped_count": skipped,
                                "failed_count": failed,
                                "acted_by": acted_by,
                            },
                            ensure_ascii=False,
                        ),
                    ),
                    bq,
                    project=project,
                    dataset=dataset,
                )
            query = urlencode(
                {
                    "synonym_batch_applied": applied,
                    "synonym_batch_skipped": skipped,
                    "synonym_batch_failed": failed,
                }
            )
            return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)

        if applied > 0:
            _persist_synonym_proposal_payload(
                run=run,
                payload=payload,
                acted_by=acted_by,
                note=note,
                event_stage="synonym_proposal_batch_reviewed",
                event_message=f"Applied {applied} synonym proposal review decision(s)",
            )
        append_event(
            RunEvent(
                run_id=run.run_id,
                event_id=str(uuid.uuid4()),
                stage="synonym_proposal_batch_summary",
                level="info",
                message=f"Synonym batch review summary: applied={applied}, skipped={skipped}, failed={failed}",
                created_at=datetime.datetime.now(datetime.timezone.utc),
                payload_json=_json.dumps(
                    {
                        "applied_count": applied,
                        "skipped_count": skipped,
                        "failed_count": failed,
                        "decisions_requested": len(deduped_decisions),
                        "acted_by": acted_by,
                    },
                    ensure_ascii=False,
                ),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        query = urlencode(
            {
                "synonym_batch_applied": applied,
                "synonym_batch_skipped": skipped,
                "synonym_batch_failed": failed,
            }
        )
        return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)

    @app.post("/admin/runs/{run_id}/synonym-proposals/apply-approved-to-run")
    async def admin_run_synonym_proposals_apply_approved_to_run(
        request: Request,
        run_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        mode = _synonym_management_mode(run)
        if not mode["apply_to_run_enabled"]:
            raise HTTPException(status_code=409, detail="Synonym apply-to-run is disabled by rollout settings")
        if run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}:
            raise HTTPException(status_code=409, detail="Cannot apply approved overlay for terminal runs")
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal payload is not available for this run")
        proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
        overlay_synonyms, _proposal_ids = _approved_synonym_overlay_payload(proposals)
        if not overlay_synonyms:
            raise HTTPException(status_code=409, detail="No approved synonym proposals are available for this run")
        form = await request.form()
        acted_by = str(form.get("acted_by") or "admin").strip() or "admin"
        note = str(form.get("note") or "").strip() or "ui:apply-approved-to-run"
        _persist_synonym_proposal_payload(
            run=run,
            payload=payload,
            acted_by=acted_by,
            note=note,
            event_stage="synonym_apply_approved_to_run",
            event_message=f"Applied {len(overlay_synonyms)} approved synonym mapping(s) to this run overlay",
            event_payload={
                "applied_count": len(overlay_synonyms),
                "skipped_count": 0,
                "failed_count": 0,
            },
        )
        query = urlencode(
            {
                "synonym_apply_to_run_applied": len(overlay_synonyms),
                "synonym_apply_to_run_skipped": 0,
                "synonym_apply_to_run_failed": 0,
            }
        )
        return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)

    @app.post("/admin/runs/{run_id}/synonym-proposals/regenerate")
    async def admin_run_synonym_proposals_regenerate(
        run_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        mode = _synonym_management_mode(run)
        if not mode["propose_enabled"]:
            raise HTTPException(status_code=409, detail="Synonym proposal generation is disabled by rollout settings")
        if not _can_regenerate_synonym_proposals(run):
            raise HTTPException(
                status_code=409,
                detail="Synonym proposals can be regenerated only at awaiting-continue enrich->rule_filter checkpoint",
            )
        if not run.mapping_suggestions_json:
            raise HTTPException(status_code=404, detail="Mapping suggestions payload is not available for this run")
        mapping_payload = decode_json_object_or_none(run.mapping_suggestions_json)
        if mapping_payload is None:
            raise HTTPException(status_code=409, detail="Mapping suggestions payload is invalid for this run")
        suggestions = list(mapping_payload.get("suggestions") or [])

        synonym_payload_json = build_synonym_proposals_payload(
            run_id=run.run_id,
            summary={"mapping_suggestions": suggestions},
            created_at=datetime.datetime.now(datetime.timezone.utc),
            existing_payload_json=run.synonym_proposals_json,
            global_synonyms=_global_synonyms_for_proposal_evaluation(run),
        )
        persistence_status = update_run_synonym_proposals(
            run.run_id,
            synonym_payload_json,
            bq,
            project=project,
            dataset=dataset,
        )
        synonym_payload = decode_json_object_or_none(synonym_payload_json) or {}
        trace_summary = dict(
            ((synonym_payload.get("synonym_proposals_trace") or {}).get("trace_summary") or {})
            if isinstance(synonym_payload, dict)
            else {}
        )
        regenerated_total = int(trace_summary.get("generated_for_review_count") or 0)
        regenerated_suppressed = int(trace_summary.get("suppressed_as_already_global_count") or 0)
        failed = 0 if persistence_status.get("persistence_status") in {"persisted", "not_applicable"} else 1
        append_event(
            RunEvent(
                run_id=run.run_id,
                event_id=str(uuid.uuid4()),
                stage="synonym_proposals_regenerated",
                level="info",
                message=(
                    "Synonym proposals regenerated from mapping suggestions: "
                    f"generated={regenerated_total}, suppressed={regenerated_suppressed}, failed={failed}"
                ),
                created_at=datetime.datetime.now(datetime.timezone.utc),
                payload_json=_json.dumps(
                    {
                        "generated_for_review_count": regenerated_total,
                        "suppressed_as_already_global_count": regenerated_suppressed,
                        "failed_count": failed,
                        "persistence_status": persistence_status.get("persistence_status"),
                        "degradation_reason": persistence_status.get("degradation_reason"),
                    },
                    ensure_ascii=False,
                ),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        query = urlencode(
            {
                "synonym_regenerated_total": regenerated_total,
                "synonym_regenerated_suppressed": regenerated_suppressed,
                "synonym_regenerated_failed": failed,
            }
        )
        return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)

    @app.post("/admin/runs/{run_id}/synonym-proposals/promote-preview", response_class=HTMLResponse)
    async def admin_run_synonym_proposals_promote_preview(
        request: Request,
        run_id: str,
    ) -> HTMLResponse:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        mode = _synonym_management_mode(run)
        if not mode["promote_global_enabled"]:
            raise HTTPException(status_code=409, detail="Synonym global promotion is disabled by rollout settings")
        form = await request.form()
        selected_ids = [str(value or "").strip() for value in form.getlist("promote_proposal_id")]
        selected_ids = [proposal_id for proposal_id in selected_ids if proposal_id]
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal payload is not available for this run")
        if not selected_ids:
            proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
            approved_ids: list[str] = []
            approved_rows: list[dict[str, Any]] = []
            for item in proposals:
                if str(item.get("proposal_status") or "").strip() != "approved_for_run_overlay":
                    continue
                proposal_id = str(item.get("proposal_id") or "").strip()
                if not proposal_id:
                    continue
                approved_ids.append(proposal_id)
                approved_rows.append(item)
            if not approved_ids:
                query = urlencode(
                    {
                        "synonym_promote_preview_status": "no_approved",
                    }
                )
                return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)
            global_maps = {
                "skill": _load_global_skill_synonyms_map(),
                "domain": _load_global_domain_alias_map(),
                "role_family": _load_global_role_family_alias_map(),
            }
            for item in approved_rows:
                proposal_id = str(item.get("proposal_id") or "").strip()
                field = str(item.get("field") or "skill").strip().lower() or "skill"
                alias = str(item.get("alias") or "").strip().lower()
                canonical = str(item.get("canonical") or "").strip().lower()
                if not proposal_id or not alias or not canonical:
                    continue
                global_map = global_maps.get(field)
                if global_map is None:
                    selected_ids.append(proposal_id)
                    continue
                # Default preview should focus on promotable deltas.
                if str(global_map.get(alias) or "").strip().lower() == canonical:
                    continue
                selected_ids.append(proposal_id)
            selected_ids = [proposal_id for proposal_id in selected_ids if proposal_id]
        if not selected_ids:
            query = urlencode(
                {
                    "synonym_promote_preview_status": "no_promotable",
                }
            )
            return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)
        preview = _build_promote_global_preview(
            run=run,
            payload=payload,
            selected_proposal_ids=selected_ids,
        )
        return templates.TemplateResponse(
            request=request,
            name="synonym_promote_preview.html",
            context={
                "run": run,
                "preview": preview,
                "selected_ids_csv": ",".join(selected_ids),
            },
        )

    @app.get("/admin/runs/{run_id}/synonym-proposals/promote-review", response_class=HTMLResponse)
    async def admin_run_synonym_proposals_promote_review(
        request: Request,
        run_id: str,
    ) -> HTMLResponse:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        mode = _synonym_management_mode(run)
        if not mode["promote_global_enabled"]:
            raise HTTPException(status_code=409, detail="Synonym global promotion is disabled by rollout settings")
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal payload is not available for this run")
        proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
        approved_ids: list[str] = []
        for item in proposals:
            if str(item.get("proposal_status") or "").strip() != "approved_for_run_overlay":
                continue
            proposal_id = str(item.get("proposal_id") or "").strip()
            if proposal_id:
                approved_ids.append(proposal_id)
        if not approved_ids:
            query = urlencode(
                {
                    "synonym_promote_preview_status": "no_approved",
                }
            )
            return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)
        preview = _build_promote_global_preview(
            run=run,
            payload=payload,
            selected_proposal_ids=approved_ids,
        )
        return templates.TemplateResponse(
            request=request,
            name="synonym_promote_preview.html",
            context={
                "run": run,
                "preview": preview,
                "selected_ids_csv": ",".join(approved_ids),
            },
        )

    @app.post("/admin/runs/{run_id}/synonym-proposals/promote-commit")
    async def admin_run_synonym_proposals_promote_commit(
        request: Request,
        run_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        mode = _synonym_management_mode(run)
        if not mode["promote_global_enabled"]:
            raise HTTPException(status_code=409, detail="Synonym global promotion is disabled by rollout settings")
        form = await request.form()
        selected_ids = [str(value or "").strip() for value in form.getlist("promote_proposal_id")]
        selected_ids = [proposal_id for proposal_id in selected_ids if proposal_id]
        if not selected_ids:
            selected_csv = str(form.get("selected_ids_csv") or "").strip()
            selected_ids = [value.strip() for value in selected_csv.split(",") if value and value.strip()]
        if not selected_ids:
            raise HTTPException(status_code=422, detail="No proposals selected for promotion")
        acted_by = str(form.get("acted_by") or "admin").strip() or "admin"
        note = str(form.get("note") or "").strip()
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal payload is not available for this run")
        preview = _build_promote_global_preview(
            run=run,
            payload=payload,
            selected_proposal_ids=selected_ids,
        )
        promote_result = _commit_synonym_global_promotion(
            run=run,
            payload=payload,
            preview=preview,
            selected_ids=selected_ids,
            acted_by=acted_by,
            note=note,
            bq=bq,
            project=project,
            dataset=dataset,
        )
        query = urlencode(
            {
                "synonym_promote_applied": promote_result["applied"],
                "synonym_promote_skipped": promote_result["skipped"],
                "synonym_promote_failed": promote_result["failed"],
                "synonym_promote_new_aliases": promote_result["new_aliases"],
                "synonym_promote_unchanged_aliases": promote_result["unchanged_aliases"],
                "synonym_promote_overridden_aliases": promote_result["overridden_aliases"],
            }
        )
        return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)

    @app.post("/admin/runs/{run_id}/synonym-proposals/triage-refresh")
    async def admin_run_synonym_proposals_triage_refresh(
        request: Request,
        run_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal payload is not available for this run")
        mode = dict(_synonym_management_mode(run))
        # Triage refresh is advisory by default. Auto-apply / auto-promote requires explicit opt-in
        # in stored effective settings, not just defaulted mode values.
        explicit_settings = decode_json_object_or_none(run.effective_settings_json) or {}
        explicit_synonym_management = dict(explicit_settings.get("synonym_management") or {})
        auto_apply_opt_in = explicit_synonym_management.get("auto_apply_recommendation_enabled") is True
        auto_promote_opt_in = explicit_synonym_management.get("auto_promote_global_enabled") is True
        if not auto_apply_opt_in:
            mode["auto_apply_recommendation_enabled"] = False
        if not auto_promote_opt_in:
            mode["auto_promote_global_enabled"] = False
        form = await request.form()
        acted_by = str(form.get("acted_by") or "admin").strip() or "admin"
        note = str(form.get("note") or "").strip()
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        triage_runtime = _resolve_synonym_triage_runtime(run)
        fingerprints = _synonym_observability_fingerprints(run)
        overlay_fp = str(fingerprints.get("run_overlay_fingerprint") or "").strip() or None
        proposals = list(payload.get("proposals") or [])
        original_status_by_id = {
            str(item.get("proposal_id") or "").strip(): str(item.get("proposal_status") or "").strip()
            for item in proposals
            if isinstance(item, dict) and str(item.get("proposal_id") or "").strip()
        }
        triaged_count = 0
        reused_count = 0
        reused_strict_count = 0
        reused_core_count = 0
        skipped_count = 0
        failed_count = 0
        fallback_count = 0
        fresh_count = 0
        generated_total = 0
        reuse_reason = "reuse_enabled"
        if not bool(mode.get("auto_triage_recommendation_enabled")):
            reuse_reason = "auto_triage_disabled"
        elif not bool(mode.get("triage_recommendation_reuse_enabled")):
            reuse_reason = "reuse_disabled"
        for idx, proposal in enumerate(proposals):
            if not isinstance(proposal, dict):
                skipped_count += 1
                continue
            status = str(proposal.get("proposal_status") or "proposed_unreviewed").strip() or "proposed_unreviewed"
            if status not in {"proposed_unreviewed", "in_review", "deferred"}:
                skipped_count += 1
                continue
            generated_total += 1
            if not bool(mode.get("auto_triage_recommendation_enabled")):
                skipped_count += 1
                continue
            runtime_meta = dict(proposal.get("recommendation_runtime") or {})
            reuse_eval = evaluate_synonym_triage_reuse(
                proposal=proposal,
                runtime=triage_runtime,
                runtime_meta=runtime_meta,
                overlay_fingerprint=overlay_fp,
            )
            reuse_enabled = bool(mode.get("triage_recommendation_reuse_enabled"))
            if reuse_enabled and str(reuse_eval.get("decision") or "") in {"strict_reuse", "core_reuse"}:
                reused_count += 1
                if str(reuse_eval.get("decision") or "") == "strict_reuse":
                    reused_strict_count += 1
                else:
                    reused_core_count += 1
                triaged_count += 1
                continue
            if reuse_enabled:
                reuse_reason = str(reuse_eval.get("reason") or "fingerprint_mismatch")
            try:
                sleep_secs = max(0.0, float(triage_runtime.get("sleep_secs") or 0.0))
                if sleep_secs > 0:
                    time.sleep(sleep_secs)
                recommendation = _triage_synonym_proposal_recommendation(
                    proposal,
                    now_iso=now_iso,
                    runtime=triage_runtime,
                )
            except Exception:
                # Provider/runtime degradation fallback: preserve advisory output
                # using deterministic builtin triage instead of failing the row.
                try:
                    recommendation = _triage_synonym_proposal_recommendation(
                        proposal,
                        now_iso=now_iso,
                        runtime={
                            "provider": "fitcv_builtin",
                            "model": "synonym_triage_v1_fallback",
                            "wire_api": "builtin",
                        },
                    )
                    fallback_count += 1
                except Exception:
                    failed_count += 1
                    continue
            fresh_count += 1
            updated = dict(proposal)
            # Advisory-only: never mutate proposal_status during triage refresh.
            updated.update(recommendation)
            updated["proposal_status"] = status
            recommendation_runtime = dict(updated.get("recommendation_runtime") or {})
            recommendation_runtime["triage_fingerprint"] = str(reuse_eval.get("strict_fingerprint") or "")
            recommendation_runtime["triage_fingerprint_strict"] = str(reuse_eval.get("strict_fingerprint") or "")
            recommendation_runtime["triage_fingerprint_core"] = str(reuse_eval.get("core_fingerprint") or "")
            gate = dict(reuse_eval.get("gate") or {})
            recommendation_runtime["triage_gate_status"] = str(gate.get("status") or "")
            recommendation_runtime["triage_gate_has_conflict"] = bool(gate.get("has_conflict"))
            recommendation_runtime["triage_gate_canonical"] = str(gate.get("canonical") or "")
            recommendation_runtime["triage_gate_candidate_canonicals"] = list(gate.get("candidate_canonicals") or [])
            updated["recommendation_runtime"] = recommendation_runtime
            proposals[idx] = updated
            triaged_count += 1
        for idx, proposal in enumerate(proposals):
            if not isinstance(proposal, dict):
                continue
            proposal_id = str(proposal.get("proposal_id") or "").strip()
            if not proposal_id:
                continue
            original_status = original_status_by_id.get(proposal_id)
            if original_status is not None:
                proposal["proposal_status"] = original_status
                proposals[idx] = proposal
        payload["proposals"] = proposals
        _persist_synonym_proposal_payload(
            run=run,
            payload=payload,
            acted_by=acted_by,
            note=note,
            event_stage="synonym_proposal_triage_completed",
            event_message=(
                "Synonym triage refresh completed: "
                f"triaged={triaged_count}, reused={reused_count}, "
                f"fallback={fallback_count}, skipped={skipped_count}, failed={failed_count}"
            ),
            event_payload={
                "triaged_count": triaged_count,
                "reused_count": reused_count,
                "reused_strict_count": reused_strict_count,
                "reused_core_count": reused_core_count,
                "fresh_count": fresh_count,
                "generated_total": generated_total,
                "fallback_count": fallback_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "reuse_reason": reuse_reason,
                "auto_triage_recommendation_enabled": bool(mode.get("auto_triage_recommendation_enabled")),
                "triage_recommendation_reuse_enabled": bool(mode.get("triage_recommendation_reuse_enabled")),
                "provider": str(triage_runtime.get("provider") or "fitcv_builtin"),
                "model": str(triage_runtime.get("model") or "synonym_triage_v1"),
                "wire_api": str(triage_runtime.get("wire_api") or "builtin"),
                "base_url": str(triage_runtime.get("base_url") or "") or None,
            },
        )
        trace_payload = dict(payload.get("synonym_proposals_trace") or {})
        trace_summary = dict(trace_payload.get("trace_summary") or {})
        trace_summary["triage_recommendation_generated_total"] = int(generated_total)
        trace_summary["triage_recommendation_reused_total"] = int(reused_count)
        trace_summary["triage_recommendation_reused_strict_total"] = int(reused_strict_count)
        trace_summary["triage_recommendation_reused_core_total"] = int(reused_core_count)
        trace_summary["triage_recommendation_fresh_total"] = int(fresh_count)
        trace_summary["triage_recommendation_suppressed_total"] = 0
        trace_summary["triage_recommendation_reuse_reason"] = reuse_reason
        trace_summary["triage_recommendation_fingerprint"] = _stable_sha256_json(
            {
                "provider": str(triage_runtime.get("provider") or "fitcv_builtin"),
                "model": str(triage_runtime.get("model") or "synonym_triage_v1"),
                "wire_api": str(triage_runtime.get("wire_api") or "builtin"),
                "overlay_fingerprint": overlay_fp,
            }
        )
        trace_payload["trace_summary"] = trace_summary
        payload["synonym_proposals_trace"] = trace_payload
        auto_apply_counts = {
            "applied": 0,
            "skipped": 0,
            "failed": 0,
            "reason_counts": {},
        }
        if bool(mode.get("auto_apply_recommendation_enabled")) and bool(mode.get("apply_to_run_enabled")):
            auto_apply_counts = _auto_apply_synonym_recommendations(
                run=run,
                payload=payload,
                acted_by=acted_by,
                note=note or "auto:triage-refresh",
            )
            if int(auto_apply_counts.get("applied") or 0) > 0:
                _sync_run_overlay_from_approved_synonym_proposals(
                    run=run,
                    payload=payload,
                    bq=bq,
                    project=project,
                    dataset=dataset,
                )
                append_event(
                    RunEvent(
                        run_id=run.run_id,
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
                        payload_json=_json.dumps(
                            {
                                "applied_count": int(auto_apply_counts.get("applied") or 0),
                                "skipped_count": int(auto_apply_counts.get("skipped") or 0),
                                "failed_count": int(auto_apply_counts.get("failed") or 0),
                                "reason_counts": dict(auto_apply_counts.get("reason_counts") or {}),
                                "acted_by": acted_by,
                                "note": note or "auto:triage-refresh",
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
            if not _is_validation_eligible_for_auto_promote(run):
                promote_skip_reason = "validation_not_eligible"
            else:
                selected_ids = [
                    str(item.get("proposal_id") or "").strip()
                    for item in list(payload.get("proposals") or [])
                    if isinstance(item, dict)
                    and str(item.get("proposal_status") or "").strip() == "approved_for_run_overlay"
                    and str(item.get("field") or "skill").strip().lower() == "skill"
                    and str(item.get("proposal_id") or "").strip()
                ]
                if not selected_ids:
                    promote_skip_reason = "no_approved_proposals"
                else:
                    preview = _build_promote_global_preview(
                        run=run,
                        payload=payload,
                        selected_proposal_ids=selected_ids,
                    )
                    if int((preview.get("counts") or {}).get("conflict") or 0) > 0:
                        promote_counts["failed"] = int((preview.get("counts") or {}).get("conflict") or 0)
                        promote_counts["skipped"] = int((preview.get("counts") or {}).get("skip") or 0)
                        promote_skip_reason = "conflicts_present"
                    else:
                        promote_counts = _commit_synonym_global_promotion(
                            run=run,
                            payload=payload,
                            preview=preview,
                            selected_ids=selected_ids,
                            acted_by=acted_by,
                            note=note or "auto:triage-refresh",
                            bq=bq,
                            project=project,
                            dataset=dataset,
                        )
                        promote_skip_reason = "applied"
        trace_summary["auto_apply_recommendation_applied"] = int(auto_apply_counts.get("applied") or 0)
        trace_summary["auto_apply_recommendation_skipped"] = int(auto_apply_counts.get("skipped") or 0)
        trace_summary["auto_apply_recommendation_failed"] = int(auto_apply_counts.get("failed") or 0)
        trace_summary["auto_apply_recommendation_reason_counts"] = dict(auto_apply_counts.get("reason_counts") or {})
        trace_summary["auto_promote_global_applied"] = int(promote_counts.get("applied") or 0)
        trace_summary["auto_promote_global_skipped"] = int(promote_counts.get("skipped") or 0)
        trace_summary["auto_promote_global_failed"] = int(promote_counts.get("failed") or 0)
        trace_summary["auto_promote_global_skip_reason"] = promote_skip_reason
        update_run_synonym_proposals(
            run_id=run.run_id,
            synonym_proposals_json=_json.dumps(payload, ensure_ascii=False),
            bq=bq,
            project=project,
            dataset=dataset,
        )
        query = urlencode(
            {
                "synonym_triage_triaged": triaged_count,
                "synonym_triage_reused": reused_count,
                "synonym_triage_fresh": fresh_count,
                "synonym_triage_skipped": skipped_count,
                "synonym_triage_failed": failed_count,
                "synonym_triage_fallback": fallback_count,
                "synonym_auto_apply_applied": int(auto_apply_counts.get("applied") or 0),
                "synonym_auto_apply_failed": int(auto_apply_counts.get("failed") or 0),
                "synonym_auto_promote_applied": int(promote_counts.get("applied") or 0),
                "synonym_auto_promote_failed": int(promote_counts.get("failed") or 0),
            }
        )
        return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)

    @app.post("/admin/runs/{run_id}/synonym-proposals/ai-fast-path-execute")
    async def admin_run_synonym_proposals_ai_fast_path_execute(
        request: Request,
        run_id: str,
    ) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal payload is not available for this run")

        form = await request.form()
        acted_by = str(form.get("acted_by") or "admin").strip() or "admin"
        note = str(form.get("note") or "").strip() or "ai_fast_path"
        mode = _synonym_management_mode(run)

        auto_apply_counts = _auto_apply_synonym_recommendations(
            run=run,
            payload=payload,
            acted_by=acted_by,
            note=note,
        )
        _persist_synonym_proposal_payload(
            run=run,
            payload=payload,
            acted_by=acted_by,
            note=note,
            event_stage="synonym_proposal_ai_fast_path_applied",
            event_message=(
                "AI fast-path decision apply completed: "
                f"applied={int(auto_apply_counts.get('applied') or 0)}, "
                f"skipped={int(auto_apply_counts.get('skipped') or 0)}, "
                f"failed={int(auto_apply_counts.get('failed') or 0)}"
            ),
            event_payload={
                "decision_source": "ai_fast_path",
                "applied_count": int(auto_apply_counts.get("applied") or 0),
                "skipped_count": int(auto_apply_counts.get("skipped") or 0),
                "failed_count": int(auto_apply_counts.get("failed") or 0),
                "reason_counts": dict(auto_apply_counts.get("reason_counts") or {}),
                "acted_by": acted_by,
                "note": note,
            },
        )
        if int(auto_apply_counts.get("applied") or 0) > 0:
            _sync_run_overlay_from_approved_synonym_proposals(
                run=run,
                payload=payload,
                bq=bq,
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
        if bool(mode.get("promote_global_enabled")) and _is_validation_eligible_for_auto_promote(run):
            selected_ids = [
                str(item.get("proposal_id") or "").strip()
                for item in list(payload.get("proposals") or [])
                if isinstance(item, dict)
                and str(item.get("proposal_status") or "").strip() == "approved_for_run_overlay"
                and str(item.get("proposal_id") or "").strip()
            ]
            if selected_ids:
                preview = _build_promote_global_preview(
                    run=run,
                    payload=payload,
                    selected_proposal_ids=selected_ids,
                )
                if int((preview.get("counts") or {}).get("conflict") or 0) > 0:
                    promote_counts["failed"] = int((preview.get("counts") or {}).get("conflict") or 0)
                    promote_counts["skipped"] = int((preview.get("counts") or {}).get("skip") or 0)
                else:
                    promote_counts = _commit_synonym_global_promotion(
                        run=run,
                        payload=payload,
                        preview=preview,
                        selected_ids=selected_ids,
                        acted_by=acted_by,
                        note=note,
                        bq=bq,
                        project=project,
                        dataset=dataset,
                    )

        query = urlencode(
            {
                "synonym_fast_path_applied": int(auto_apply_counts.get("applied") or 0),
                "synonym_fast_path_skipped": int(auto_apply_counts.get("skipped") or 0),
                "synonym_fast_path_failed": int(auto_apply_counts.get("failed") or 0),
                "synonym_fast_path_promote_applied": int(promote_counts.get("applied") or 0),
                "synonym_fast_path_promote_skipped": int(promote_counts.get("skipped") or 0),
                "synonym_fast_path_promote_failed": int(promote_counts.get("failed") or 0),
                "synonym_fast_path_promote_new_aliases": int(promote_counts.get("new_aliases") or 0),
                "synonym_fast_path_promote_unchanged_aliases": int(promote_counts.get("unchanged_aliases") or 0),
                "synonym_fast_path_promote_overridden_aliases": int(promote_counts.get("overridden_aliases") or 0),
            }
        )
        return RedirectResponse(f"/admin/runs/{run_id}/synonym-review?{query}", status_code=303)

    @app.get("/admin/runs/{run_id}/approved-synonym-proposals.yaml")
    def download_run_approved_synonym_overlay_yaml(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        payload = _load_run_synonym_proposals_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposals export is not available for this run")
        proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
        overlay_synonyms, _proposal_ids = _approved_synonym_overlay_payload(proposals)
        if not overlay_synonyms:
            raise HTTPException(status_code=404, detail="No approved synonym proposals are available for this run")
        overlay_yaml = _build_synonym_overlay_yaml(overlay_synonyms)
        return Response(
            content=overlay_yaml,
            media_type="text/yaml",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-approved-synonym-proposals.yaml"'},
        )

    @app.get("/admin/synonyms/global.yaml")
    def download_global_synonyms_yaml() -> Response:
        global_map = _load_global_skill_synonyms_map()
        overlay_yaml = _build_synonym_overlay_yaml(global_map)
        return Response(
            content=overlay_yaml,
            media_type="text/yaml",
            headers={"Content-Disposition": 'attachment; filename="fitcv-global-skill-synonyms.yaml"'},
        )

    @app.get("/admin/synonyms/global-domain.yaml")
    def download_global_domain_synonyms_yaml() -> Response:
        global_map = _load_global_domain_alias_map()
        content = "".join(_render_yaml_top_level_mapping(key="domain_alias_map", mappings=global_map))
        return Response(
            content=content,
            media_type="text/yaml",
            headers={"Content-Disposition": 'attachment; filename="fitcv-global-domain-aliases.yaml"'},
        )

    @app.get("/admin/synonyms/global-role-family.yaml")
    def download_global_role_family_synonyms_yaml() -> Response:
        global_map = _load_global_role_family_alias_map()
        content = "".join(_render_yaml_top_level_mapping(key="role_family_alias_map", mappings=global_map))
        return Response(
            content=content,
            media_type="text/yaml",
            headers={"Content-Disposition": 'attachment; filename="fitcv-global-role-family-aliases.yaml"'},
        )

    @app.get("/admin/runs/{run_id}/tabs/enriched", response_class=HTMLResponse)
    def admin_run_detail_tab_enriched(
        request: Request,
        run_id: str,
        page: int = 1,
        page_size: int = 25,
        filter_name: str = "all",
        q: str = "",
    ) -> HTMLResponse:
        run = require_run_or_404(run_id, detail="")
        page = _coerce_positive_int(page, default=1, minimum=1, maximum=10000)
        page_size = _coerce_positive_int(page_size, default=25, minimum=10, maximum=100)
        context = _build_enriched_tab_context(
            run,
            run_id=run_id,
            project=project,
            dataset=dataset,
            bq=bq,
            filter_name=filter_name,
            query=q,
            page=page,
            page_size=page_size,
        )
        return templates.TemplateResponse(
            request=request,
            name="run_detail_tab_enriched.html",
            context=context,
        )

    @app.get("/admin/runs/{run_id}/tabs/jobs-input", response_class=HTMLResponse)
    def admin_run_detail_tab_jobs_input(request: Request, run_id: str) -> HTMLResponse:
        run = require_run_or_404(run_id, detail="")
        return templates.TemplateResponse(
            request=request,
            name="run_detail_tab_jobs_input.html",
            context={"run": run},
        )

    @app.get("/admin/runs/{run_id}/tabs/profile", response_class=HTMLResponse)
    def admin_run_detail_tab_profile(request: Request, run_id: str) -> HTMLResponse:
        run = require_run_or_404(run_id, detail="")
        candidate_profile_pretty: str | None = None
        if run.candidate_profile_json:
            candidate_profile_pretty = pretty_json_string_or_fallback(run.candidate_profile_json)
        return templates.TemplateResponse(
            request=request,
            name="run_detail_tab_profile.html",
            context={
                "run": run,
                "candidate_profile_pretty": candidate_profile_pretty,
            },
        )

    @app.get("/admin/runs/{run_id}/synonym-review", response_class=HTMLResponse)
    def admin_run_synonym_review_workspace(request: Request, run_id: str) -> HTMLResponse:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return templates.TemplateResponse(
            request=request,
            name="synonym_review.html",
            context={
                "run": run,
                "synonym_proposal_review_queue": _build_synonym_proposal_review_queue(run),
                "synonym_proposal_decision_ledger": _build_synonym_proposal_decision_ledger(run),
                "synonym_management_mode": _synonym_management_mode(run),
                "synonym_overlay_info": _extract_run_synonym_overlay_info(run),
                "can_upload_synonym_overlay": _can_upload_synonym_overlay(run),
            },
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.get("/admin/cvs/{version_id}/download")
    def download_cv(version_id: str):
        content = get_cv_markdown(version_id, bq, project=project, dataset=dataset)
        if content is None:
            raise HTTPException(status_code=404, detail="CV not found")
        return Response(
            content=content,
            media_type="text/markdown",
            headers={"Content-Disposition": f'attachment; filename="cv_{version_id}.md"'}
        )

    @app.get("/admin/runs/{run_id}/export.json")
    def download_run_results_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _run_status_allows_export(run):
            raise HTTPException(status_code=409, detail="Run results export is only available for completed runs")
        if not run.results_export_json:
            raise HTTPException(status_code=404, detail="Run results export is not available for this run")
        pretty_json = _json.dumps(
            {
                "run_id": run.run_id,
                "results": _results_export_rows_with_hitl_audit(run),
            },
            ensure_ascii=False,
            indent=2,
        )
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-results.json"'},
        )

    @app.get("/admin/runs/{run_id}/hitl-review-audit.json")
    def download_run_hitl_review_audit_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _run_status_allows_export(run):
            raise HTTPException(status_code=409, detail="HITL review audit export is only available for completed runs")
        if not run.cv_generation_debug_json:
            raise HTTPException(status_code=404, detail="HITL review audit export is not available for this run")
        return Response(
            content=_json.dumps(_build_hitl_review_audit_payload(run), ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-hitl-review-audit.json"'},
        )

    @app.get("/admin/runs/{run_id}/cv-debug.json")
    def download_run_cv_debug_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _run_status_allows_export(run):
            raise HTTPException(status_code=409, detail="CV debug export is only available for completed runs")
        if not run.cv_generation_debug_json:
            raise HTTPException(status_code=404, detail="CV debug export is not available for this run")
        normalized_payload = _normalized_cv_debug_payload_for_export(run)
        if not isinstance(normalized_payload, dict):
            raise HTTPException(status_code=404, detail="CV debug export is not available for this run")
        pretty_json = _json.dumps(normalized_payload, ensure_ascii=False, indent=2)
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-cv-debug.json"'},
        )

    @app.get("/admin/runs/{run_id}/cv-generation-review-required.json")
    def download_run_cv_generation_review_required_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _run_status_allows_export(run):
            raise HTTPException(status_code=409, detail="Review-required export is only available for completed runs")
        payload = _build_cv_generation_review_required_payload(run)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=404, detail="Review-required export is not available for this run")
        return Response(
            content=_json.dumps(payload, ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-cv-generation-review-required.json"'},
        )

    @app.get("/admin/runs/{run_id}/agentic-live-trace.json")
    def download_run_agentic_live_trace_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if run.status != RunStatus.SUCCEEDED:
            raise HTTPException(
                status_code=409,
                detail="Agentic live trace export is only available for succeeded runs",
            )
        trace_payload = _load_run_agentic_live_trace_payload(run)
        if not isinstance(trace_payload, dict):
            trace_payload = _default_not_applicable_trace_payload(
                run=run,
                trace_name="agentic_live_trace",
            )
        if str(trace_payload.get("trace_status") or "").strip() == "not_applicable":
            raise HTTPException(status_code=404, detail="Agentic live trace export is not available for this run")
        return Response(
            content=_json.dumps(trace_payload, ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-agentic-live-trace.json"'},
        )

    @app.get("/admin/runs/{run_id}/cv-analysis-trace.json")
    def download_run_cv_analysis_trace_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if run.status != RunStatus.SUCCEEDED:
            raise HTTPException(
                status_code=409,
                detail="CV analysis trace export is only available for succeeded runs",
            )
        trace_payload = _load_run_cv_analysis_trace_payload(run)
        if not isinstance(trace_payload, dict):
            trace_payload = _default_not_applicable_trace_payload(
                run=run,
                trace_name="cv_analysis_trace",
            )
        if str(trace_payload.get("trace_status") or "").strip() == "not_applicable":
            raise HTTPException(status_code=404, detail="CV analysis trace export is not available for this run")
        return Response(
            content=_json.dumps(trace_payload, ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-cv-analysis-trace.json"'},
        )

    @app.get("/admin/runs/{run_id}/stage-artifacts.json")
    def download_run_stage_transition_artifacts_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not run.stage_transition_artifacts_json:
            raise HTTPException(status_code=404, detail="Stage transition artifacts export is not available for this run")
        try:
            pretty_json = pretty_json_string(run.stage_transition_artifacts_json)
        except (_json.JSONDecodeError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=f"Stage transition artifacts JSON invalid: {exc}")
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-stage-artifacts.json"'},
        )

    @app.get("/admin/runs/{run_id}/stage-artifacts/{stage_id}.json")
    def download_run_stage_transition_artifact_stage_json(run_id: str, stage_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        payload = _build_stage_slice_payload(run, stage_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Stage artifact is not available for this run")
        return Response(
            content=_json.dumps(payload, ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-{stage_id}.json"'},
        )

    @app.get("/admin/runs/{run_id}/artifacts.zip")
    def download_run_artifact_bundle_zip(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        artifact_files = _build_available_run_artifact_files(run)
        if run.status == RunStatus.SUCCEEDED:
            cv_versions = list_cvs_for_run(run_id, bq, project=project, dataset=dataset)
            for cv in cv_versions:
                version_id = str(cv.get("version_id") or "").strip()
                if not version_id:
                    continue
                cv_markdown = get_cv_markdown(version_id, bq, project=project, dataset=dataset)
                if not cv_markdown:
                    continue
                artifact_files.append(
                    RunArtifactFile(
                        filename=f"cv_{version_id}.md",
                        label=f"CV {version_id} Markdown",
                        href=f"/admin/cvs/{version_id}/download",
                        content=str(cv_markdown),
                        show_in_exports=False,
                    )
                )
        if not artifact_files:
            raise HTTPException(
                status_code=404,
                detail="No run artifacts are currently available for this run",
            )
        artifact_files.extend(
            [
                RunArtifactFile(
                    filename="global-skill-synonyms.yaml",
                    label="Global Skill Synonyms YAML",
                    href="/admin/synonyms/global.yaml",
                    content=_build_synonym_overlay_yaml(_load_global_skill_synonyms_map()),
                    show_in_exports=False,
                ),
                RunArtifactFile(
                    filename="global-domain-aliases.yaml",
                    label="Global Domain Aliases YAML",
                    href="/admin/synonyms/global-domain.yaml",
                    content="".join(
                        _render_yaml_top_level_mapping(
                            key="domain_alias_map",
                            mappings=_load_global_domain_alias_map(),
                        )
                    ),
                    show_in_exports=False,
                ),
                RunArtifactFile(
                    filename="global-role-family-aliases.yaml",
                    label="Global Role Family Aliases YAML",
                    href="/admin/synonyms/global-role-family.yaml",
                    content="".join(
                        _render_yaml_top_level_mapping(
                            key="role_family_alias_map",
                            mappings=_load_global_role_family_alias_map(),
                        )
                    ),
                    show_in_exports=False,
                ),
            ]
        )
        manifest = _build_run_artifact_bundle_manifest(run, artifact_files)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "manifest.json",
                _json.dumps(manifest, ensure_ascii=False, indent=2),
            )
            for artifact in artifact_files:
                archive.writestr(artifact.filename, artifact.content)
        return Response(
            content=buffer.getvalue(),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-artifacts.zip"'},
        )

    @app.get("/admin/runs/{run_id}/settings-used.json")
    def download_run_settings_used_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if run.status != RunStatus.SUCCEEDED:
            raise HTTPException(status_code=409, detail="Settings-used export is only available for succeeded runs")
        if not run.settings_used_json:
            raise HTTPException(status_code=404, detail="Settings-used export is not available for this run")
        try:
            pretty_json = pretty_json_string(run.settings_used_json)
        except (_json.JSONDecodeError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=f"Settings-used JSON invalid: {exc}")
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-settings-used.json"'},
        )

    @app.get("/admin/runs/{run_id}/mapping-suggestions.json")
    def download_run_mapping_suggestions_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not (_run_has_reached_stage(run, "enrich") and _run_has_stage_artifact(run, "enrich")):
            raise HTTPException(
                status_code=404,
                detail="Mapping suggestions export is not available until enrich has completed for this run",
        )
        if not run.mapping_suggestions_json:
            raise HTTPException(status_code=404, detail="Mapping suggestions export is not available for this run")
        try:
            pretty_json = pretty_json_string(run.mapping_suggestions_json)
        except (_json.JSONDecodeError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=f"Mapping suggestions JSON invalid: {exc}")
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-mapping-suggestions.json"'},
        )

    @app.get("/admin/runs/{run_id}/synonym-proposals.json")
    def download_run_synonym_proposals_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _run_has_reached_stage(run, "enrich"):
            raise HTTPException(
                status_code=404,
                detail="Synonym proposals export is not available until enrich has completed for this run",
            )
        if not run.synonym_proposals_json:
            raise HTTPException(status_code=404, detail="Synonym proposals export is not available for this run")
        try:
            pretty_json = pretty_json_string(run.synonym_proposals_json)
        except (_json.JSONDecodeError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=f"Synonym proposals JSON invalid: {exc}")
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-synonym-proposals.json"'},
        )

    @app.get("/admin/runs/{run_id}/synonym-proposals-trace.json")
    def download_run_synonym_proposals_trace_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _run_has_reached_stage(run, "enrich"):
            raise HTTPException(
                status_code=404,
                detail="Synonym proposals trace export is not available until enrich has completed for this run",
            )
        trace_payload = _load_run_synonym_proposals_trace_payload(run)
        if not isinstance(trace_payload, dict):
            raise HTTPException(status_code=404, detail="Synonym proposals trace export is not available for this run")
        if str(trace_payload.get("trace_status") or "").strip() == "not_applicable":
            raise HTTPException(status_code=404, detail="Synonym proposals trace export is not applicable for this run")
        return Response(
            content=_json.dumps(trace_payload, ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-synonym-proposals-trace.json"'},
        )

    @app.get("/admin/runs/{run_id}/synonym-suppression-diff.json")
    def download_run_synonym_suppression_diff_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not _run_has_reached_stage(run, "enrich"):
            raise HTTPException(
                status_code=404,
                detail="Synonym suppression diff export is not available until enrich has completed for this run",
            )
        trace_payload = _load_run_synonym_proposals_trace_payload(run)
        if not isinstance(trace_payload, dict):
            raise HTTPException(status_code=404, detail="Synonym suppression diff export is not available for this run")
        if str(trace_payload.get("trace_status") or "").strip() == "not_applicable":
            raise HTTPException(status_code=404, detail="Synonym suppression diff export is not applicable for this run")
        payload = _build_synonym_suppression_diff_payload(run)
        return Response(
            content=_json.dumps(payload, ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-synonym-suppression-diff.json"'},
        )

    @app.get("/admin/mapping-suggestions.json")
    def download_aggregate_mapping_suggestions_json() -> Response:
        runs = list_runs(
            bq,
            project=project,
            dataset=dataset,
            limit=500,
            include_archived=True,
        )
        payload = _aggregate_mapping_suggestion_payloads(runs)
        return Response(
            content=_json.dumps(payload, ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="fitcv-mapping-suggestions.json"'},
        )

    @app.get("/admin/synonym-proposals.json")
    def download_aggregate_synonym_proposals_json() -> Response:
        runs = list_runs(
            bq,
            project=project,
            dataset=dataset,
            limit=500,
            include_archived=True,
        )
        payload = _aggregate_synonym_proposal_payloads(runs)
        return Response(
            content=_json.dumps(payload, ensure_ascii=False, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="fitcv-synonym-proposals.json"'},
        )

    @app.post("/admin/synonym-proposals/{proposal_id}/start-review")
    def start_synonym_proposal_review(
        proposal_id: str,
        acted_by: str = Form("admin"),
        note: str = Form(""),
    ) -> dict[str, Any]:
        return _apply_synonym_proposal_action(
            proposal_id=proposal_id,
            action="start_review",
            acted_by=acted_by,
            note=note,
        )

    @app.post("/admin/synonym-proposals/{proposal_id}/approve-for-run-overlay")
    def approve_synonym_proposal_for_run_overlay(
        proposal_id: str,
        acted_by: str = Form("admin"),
        note: str = Form(""),
    ) -> dict[str, Any]:
        return _apply_synonym_proposal_action(
            proposal_id=proposal_id,
            action="approve_for_run_overlay",
            acted_by=acted_by,
            note=note,
        )

    @app.post("/admin/synonym-proposals/{proposal_id}/reject")
    def reject_synonym_proposal(
        proposal_id: str,
        acted_by: str = Form("admin"),
        note: str = Form(""),
    ) -> dict[str, Any]:
        return _apply_synonym_proposal_action(
            proposal_id=proposal_id,
            action="reject",
            acted_by=acted_by,
            note=note,
        )

    @app.post("/admin/synonym-proposals/{proposal_id}/defer")
    def defer_synonym_proposal(
        proposal_id: str,
        acted_by: str = Form("admin"),
        note: str = Form(""),
    ) -> dict[str, Any]:
        return _apply_synonym_proposal_action(
            proposal_id=proposal_id,
            action="defer",
            acted_by=acted_by,
            note=note,
        )

    def _apply_synonym_proposal_action_in_run(
        *,
        run: PipelineRun,
        payload: dict[str, Any],
        proposal_index: int,
        action: str,
        acted_by: str,
        note: str,
        persist: bool = True,
    ) -> dict[str, Any]:
        proposals = list(payload.get("proposals") or [])
        if proposal_index < 0 or proposal_index >= len(proposals):
            raise HTTPException(status_code=404, detail="Synonym proposal not found")
        proposal = proposals[proposal_index]
        if not isinstance(proposal, dict):
            raise HTTPException(status_code=404, detail="Synonym proposal not found")
        proposal_id = str(proposal.get("proposal_id") or "").strip()
        next_status = transition_synonym_proposal_status(
            str(proposal.get("proposal_status") or ""),
            action,
        )
        if not next_status:
            raise HTTPException(status_code=409, detail="Synonym proposal action is not valid for the current state")
        updated_proposal = dict(proposal)
        review_history = list(updated_proposal.get("review_history") or [])
        acted_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        review_history.append(
            {
                "action": action,
                "from_status": str(updated_proposal.get("proposal_status") or ""),
                "to_status": next_status,
                "acted_by": str(acted_by or "admin"),
                "acted_at": acted_at,
                "note": str(note or "").strip(),
            }
        )
        updated_proposal["proposal_status"] = next_status
        updated_proposal["review_history"] = review_history
        proposals[proposal_index] = updated_proposal
        payload["proposals"] = proposals
        updated_payload = dict(payload)
        updated_payload["proposals"] = proposals

        if not persist:
            return {
                "proposal_id": proposal_id,
                "run_id": run.run_id,
                "proposal_status": next_status,
                "persistence_status": "deferred",
                "degradation_reason": "",
            }

        persistence_status = _persist_synonym_proposal_payload(
            run=run,
            payload=updated_payload,
            acted_by=acted_by,
            note=note,
            event_stage="synonym_proposal_reviewed",
            event_message=f"Synonym proposal {proposal_id} {next_status}",
            event_payload={
                "proposal_id": proposal_id,
                "action": action,
                "proposal_status": next_status,
            },
        )
        return {
            "proposal_id": proposal_id,
            "run_id": run.run_id,
            "proposal_status": next_status,
            "persistence_status": persistence_status.get("persistence_status", "persisted"),
            "degradation_reason": persistence_status.get("degradation_reason", ""),
        }

    def _persist_synonym_proposal_payload(
        *,
        run: PipelineRun,
        payload: dict[str, Any],
        acted_by: str,
        note: str,
        event_stage: str,
        event_message: str,
        event_payload: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        proposals = [item for item in list(payload.get("proposals") or []) if isinstance(item, dict)]
        overlay_synonyms, proposal_ids = _approved_synonym_overlay_payload(proposals)
        if overlay_synonyms:
            effective_config = _load_run_effective_config_snapshot(run)
            overlay_yaml = _build_synonym_overlay_yaml(overlay_synonyms)
            updated_config = apply_runtime_skill_synonym_overlay(
                effective_config,
                overlay_synonyms,
                source="proposal_review",
                filename="approved-synonym-proposals.yaml",
                uploaded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                raw_yaml=overlay_yaml,
            )
            runtime = dict(updated_config.get("skill_synonyms_runtime") or {})
            runtime["run_overlay_proposal_ids"] = proposal_ids
            updated_config["skill_synonyms_runtime"] = runtime
            update_run_effective_settings(
                run.run_id,
                _json.dumps(updated_config, ensure_ascii=False),
                bq,
                project=project,
                dataset=dataset,
            )
        persistence_status = update_run_synonym_proposals(
            run.run_id,
            _json.dumps(payload, ensure_ascii=False),
            bq,
            project=project,
            dataset=dataset,
        )
        payload_row = dict(event_payload or {})
        payload_row["acted_by"] = str(acted_by or "admin")
        payload_row["note"] = str(note or "").strip()
        append_event(
            RunEvent(
                run_id=run.run_id,
                event_id=str(uuid.uuid4()),
                stage=event_stage,
                level="info",
                message=event_message,
                created_at=datetime.datetime.now(datetime.timezone.utc),
                payload_json=_json.dumps(payload_row, ensure_ascii=False),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        if persistence_status.get("persistence_status") not in {"persisted", "not_applicable"}:
            append_event(
                RunEvent(
                    run_id=run.run_id,
                    event_id=str(uuid.uuid4()),
                    stage="snapshot_persist_failed",
                    level="warning",
                    message=(
                        "synonym_proposals snapshot persistence failed: "
                        f"{persistence_status.get('degradation_reason') or persistence_status.get('persistence_status')}"
                    ),
                    created_at=datetime.datetime.now(datetime.timezone.utc),
                ),
                bq,
                project=project,
                dataset=dataset,
            )
        return persistence_status

    def _apply_synonym_proposal_action(
        *,
        proposal_id: str,
        action: str,
        acted_by: str,
        note: str,
    ) -> dict[str, Any]:
        runs = list_runs(
            bq,
            project=project,
            dataset=dataset,
            limit=500,
            include_archived=True,
        )
        located = _find_run_and_synonym_proposal(runs, proposal_id)
        if located is None:
            raise HTTPException(status_code=404, detail="Synonym proposal not found")
        run, payload, _proposal, idx = located
        return _apply_synonym_proposal_action_in_run(
            run=run,
            payload=payload,
            proposal_index=idx,
            action=action,
            acted_by=acted_by,
            note=note,
        )

    return app


def _run_to_dict(run: PipelineRun) -> dict:
    return {
        "run_id": run.run_id,
        "status": run.status.value,
        "run_mode": run.run_mode,
        "checkpoint_status": run.checkpoint_status,
        "next_stage": run.next_stage,
        "last_completed_stage": run.last_completed_stage,
        "completed_stages": list(run.completed_stages or []),
        "triggered_by": run.triggered_by,
        "jobs_path": run.jobs_path,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "total_jobs": run.total_jobs,
        "passed_filter": run.passed_filter,
        "ranked": run.ranked,
        "cvs_generated": run.cvs_generated,
        "error_message": run.error_message,
        "error_stage": run.error_stage,
        "queue_job_id": run.queue_job_id,
        "orchestration_backend": run.orchestration_backend,
        "orchestration_run_id": run.orchestration_run_id,
        "effective_settings_json": run.effective_settings_json,
        "settings_used_json": run.settings_used_json,
        "cv_generation_debug_json": run.cv_generation_debug_json,
        "stage_transition_artifacts_json": run.stage_transition_artifacts_json,
    }











def _is_hitl_resolution_pending(resolution_status: str | None) -> bool:
    normalized = str(resolution_status or "").strip().lower() or "pending"
    return normalized not in _HITL_TERMINAL_RESOLUTION_STATUSES



