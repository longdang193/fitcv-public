"""
@meta
name: fitcv_cp_app
type: utility
domain: admin_ui
responsibility:
  - Serve run trigger, run detail, and artifact download routes for the admin control plane.
  - Own run-detail inspection shells, checkpoint controls, and stage-owned export gating.
inputs:
  - persisted pipeline runs and events
  - queued run actions and checkpoint state
  - operator trigger payloads and settings snapshots
outputs:
  - admin HTML responses
  - run trigger and continue actions
  - run-owned and stage-owned artifact responses
capabilities:
  - admin_control_plane_core.fastapi-web-server
  - admin_control_plane_core.jinja2-admin-pages
  - admin_control_plane_core.insert-before-enqueue-invariant
  - multi_file_job_input.multiple-file-inputs-in-trigger-form
  - multi_file_job_input.per-file-server-side-validation
  - multi_file_job_input.canonical-merge-preserving-order
  - multi_file_job_input.one-immutable-snapshot-stored-per-run
  - multi_file_job_input.all-or-nothing-rejection-on-validation-failure
  - run_lifecycle_controls.cancel-queued-runs-directly-from-the-queue-via-rq
  - run_lifecycle_controls.direct-cancellation-of-paused-manual-runs-in-awaiting-continue
  - run_lifecycle_controls.stale-cancellation-repair-endpoint
  - run_lifecycle_controls.state-aware-max-runtime-timeout-handling-for-queued-running-cancelling-and-paused-manual-runs
  - run_lifecycle_controls.timeout-copy-now-distinguishes-queue-wait-active-runtime-and-stage-by-stage-manual-wait-time
  - run_lifecycle_controls.archive-and-unarchive-terminal-runs
  - run_lifecycle_controls.batch-cancel-archive-and-unarchive-endpoints-with-explicit-processed-skipped-summaries
  - inspection_debugging.run-detail-inspection-tabs
  - inspection_debugging.run-progress-and-checkpoints
  - inspection_debugging.synonym-overlay-inspection
  - inspection_debugging.run-owned-artifact-exports
  - inspection_debugging.stage-artifact-downloads
  - inspection_debugging.settings-used-export
  - inspection_debugging.results-ledger-inspection
  - inspection_debugging.stage-transition-diagnostics
  - inspection_debugging.prompt-provenance-diagnostics
  - inspection_debugging.ranking-diagnostics
  - inspection_debugging.shortlist-diagnostics
  - inspection_debugging.cv-analysis-diagnostics
  - inspection_debugging.cv-generation-diagnostics
  - inspection_debugging.reuse-diagnostics
  - inspection_debugging.quality-metrics-diagnostics
  - inspection_debugging.enriched-job-debug-export
  - inspection_debugging.rule-filter-diagnostics
  - settings_system.run-safety-settings
  - settings_system.task-first-settings-ui
  - settings_system.advanced-settings-disclosure
  - settings_system.metadata-only-fixed-controls
  - settings_system.compact-cv-visibility-controls
  - settings_system.cv-composition-visibility-settings
  - settings_system.warning-only-cv-max-pages-validation-setting
  - settings_system.grouped-form-validation
  - settings_system.per-run-overrides
  - ui_consistency_theming.consistent-action-hierarchy-primary-secondary-section
  - ui_consistency_theming.human-readable-section-headings
  - trigger_run_management.runs-list-management
  - trigger_run_management.run-detail-actions
  - trigger_run_management.job-input-modes
  - trigger_run_management.candidate-profile-input-modes
  - trigger_run_management.execution-mode-selection
  - trigger_run_management.synonym-overlay-at-trigger
  - trigger_run_management.shared-stage-progress
  - trigger_run_management.manual-checkpoints-and-continue
  - trigger_run_management.synonym-overlay-replacement
  - trigger_run_management.run-health-surface
  - trigger_run_management.run-owned-artifact-exports
  - trigger_run_management.stage-artifact-downloads
  - trigger_run_management.synonym-overlay-inspection
  - trigger_run_management.run-results-export
  - trigger_run_management.shortlist-debug-exports
  - trigger_run_management.decision-chain-outcomes
  - trigger_run_management.reranker-fit-authority
tags:
  - admin-ui
  - run-management
  - lineage-owner
lifecycle:
  status: active
"""
import dataclasses
import datetime
import io
import json as _json
import uuid
import zipfile
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, field_validator

from fitcv.config import (
    apply_cv_compatibility_projection,
    apply_runtime_skill_synonym_overlay,
    load_config,
    parse_skill_synonym_overlay_yaml,
)
from fitcv_cp.bq_store import (
    append_event,
    archive_run,
    get_events, get_run, insert_run, list_filter_results_for_run,
    list_runs, list_cvs_for_run, get_cv_markdown, list_run_structured_jobs,
    request_run_cancel, unarchive_run, update_run_checkpoint,
    update_run_effective_settings,
    update_run_queue_job_id, update_run_status,
)
from fitcv_cp.models import PipelineRun, RunEvent, RunStatus
from fitcv_cp.queue import cancel_queued_run, enqueue_run, enqueue_run_with_job_id
from fitcv_cp.settings_schema import (
    ALL_GROUP_REGISTRIES,
    CV_GROUPS,
    RANKING_GROUPS,
    SETTINGS_SCHEMA,
    SETTINGS_SECTIONS,
    ValidationError,
    apply_settings_to_config,
    coerce_value,
    validate_settings,
)
from fitcv_cp.settings_store import load_active_settings, save_setting, save_settings_group
TEMPLATES_DIR = Path(__file__).parent / "templates"
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
RUN_MODE_LABELS = {
    "run_all": "Run All",
    "manual_staged": "Stage by Stage",
}
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
    "layer1_jobs": "Enrich",
    "layer2_candidate": "Candidate Profile",
    "layer3_filter": "Rule Filter",
    "layer3_shortlist": "Shortlist",
    "layer3_ai_score": "Ranking",
    "layer3_ranking": "Ranking",
    "layer4_cv_analysis": "CV Analysis",
    "layer4_cv_analysis_skip": "CV Analysis",
    "layer4_cv_skip": "CV Analysis",
    "layer4_cv_validation_failed": "CV Generation",
    "pipeline_complete": "CV Generation",
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
    "mapping-suggestions.json",
)


@dataclasses.dataclass(frozen=True)
class RunArtifactFile:
    filename: str
    label: str
    href: str
    content: str
    show_in_exports: bool = True


def _load_stage_transition_artifacts_payload(run: PipelineRun) -> dict[str, Any]:
    if not run.stage_transition_artifacts_json:
        return {}
    try:
        payload = _json.loads(run.stage_transition_artifacts_json)
    except (_json.JSONDecodeError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


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
    for stage_id in ("ranking", "cv_analysis"):
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
            "generation_ready_rate",
            "generation_ready",
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


def _pretty_json_string(raw_json: str) -> str:
    return _json.dumps(_json.loads(raw_json), ensure_ascii=False, indent=2)


def _build_stage_slice_payload(run: PipelineRun, stage_id: str) -> dict[str, Any] | None:
    if stage_id not in BUNDLE_STAGE_IDS:
        return None
    if not run.stage_transition_artifacts_json:
        return None
    artifact_payload = _json.loads(run.stage_transition_artifacts_json)
    artifacts = artifact_payload.get("artifacts") or {}
    stages = artifacts.get("stages") or {}
    stage_artifact = stages.get(stage_id)
    if not isinstance(stage_artifact, dict):
        return None
    return {
        "run_id": run.run_id,
        "stage_id": stage_id,
        "artifact_schema_version": "stage_transition_artifacts_stage_v1",
        "created_at": artifact_payload.get("created_at"),
        "stage_artifact": stage_artifact,
    }


def _build_available_run_artifact_files(run: PipelineRun) -> list[RunArtifactFile]:
    files: list[RunArtifactFile] = []
    stage_artifacts_by_id = _stage_artifacts_by_id(run)

    if run.status == RunStatus.SUCCEEDED and run.results_export_json:
        files.append(
            RunArtifactFile(
                filename="results.json",
                label="Results JSON (Job Ledger)",
                href=f"/admin/runs/{run.run_id}/export.json",
                content=_pretty_json_string(run.results_export_json),
            )
        )
    if run.status == RunStatus.SUCCEEDED and run.cv_generation_debug_json:
        files.append(
            RunArtifactFile(
                filename="cv-debug.json",
                label="CV Debug JSON",
                href=f"/admin/runs/{run.run_id}/cv-debug.json",
                content=_pretty_json_string(run.cv_generation_debug_json),
            )
        )
    if run.status == RunStatus.SUCCEEDED and run.settings_used_json:
        files.append(
            RunArtifactFile(
                filename="settings-used.json",
                label="Settings Used JSON",
                href=f"/admin/runs/{run.run_id}/settings-used.json",
                content=_pretty_json_string(run.settings_used_json),
            )
        )
    if run.mapping_suggestions_json and _run_has_reached_stage(run, "enrich") and "enrich" in stage_artifacts_by_id:
        files.append(
            RunArtifactFile(
                filename="mapping-suggestions.json",
                label="Mapping Suggestions JSON",
                href=f"/admin/runs/{run.run_id}/mapping-suggestions.json",
                content=_pretty_json_string(run.mapping_suggestions_json),
            )
        )
    if run.stage_transition_artifacts_json and run.status != RunStatus.QUEUED:
        files.append(
            RunArtifactFile(
                filename="stage-artifacts.json",
                label="Stage Artifacts JSON (Diagnostics)",
                href=f"/admin/runs/{run.run_id}/stage-artifacts.json",
                content=_pretty_json_string(run.stage_transition_artifacts_json),
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


def _build_run_artifact_bundle_manifest(run: PipelineRun, files: list[RunArtifactFile]) -> dict[str, Any]:
    included_files = [artifact.filename for artifact in files]
    missing_files = [filename for filename in BUNDLE_ARTIFACT_FILENAMES if filename not in included_files]
    return {
        "run_id": run.run_id,
        "status": run.status.value,
        "run_mode": run.run_mode,
        "run_mode_label": RUN_MODE_LABELS.get(run.run_mode, run.run_mode),
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "bundle_schema_version": "run_artifact_bundle_v2",
        "included_files": included_files,
        "missing_files": missing_files,
    }


def _build_run_export_links(run: PipelineRun) -> list[dict[str, str]]:
    artifact_files = _build_available_run_artifact_files(run)
    links: list[dict[str, str]] = []
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
        links.append({"label": artifact.label, "href": artifact.href})
    return links


def _can_upload_synonym_overlay(run: PipelineRun) -> bool:
    return (
        run.run_mode == "manual_staged"
        and run.status == RunStatus.AWAITING_CONTINUE
        and str(run.next_stage or "").strip() == "rule_filter"
        and str(run.last_completed_stage or "").strip() == "enrich"
    )


def _load_run_effective_config_snapshot(run: PipelineRun) -> dict[str, Any]:
    if run.effective_settings_json:
        try:
            payload = _json.loads(run.effective_settings_json)
            if isinstance(payload, dict):
                return payload
        except (_json.JSONDecodeError, TypeError):
            pass
    return load_config(run.config_path)


def _extract_run_synonym_overlay_info(run: PipelineRun) -> dict[str, Any]:
    if not run.effective_settings_json:
        return {"has_run_overlay": False}
    try:
        payload = _json.loads(run.effective_settings_json)
    except (_json.JSONDecodeError, TypeError):
        return {"has_run_overlay": False}
    if not isinstance(payload, dict):
        return {"has_run_overlay": False}
    runtime = payload.get("skill_synonyms_runtime")
    if not isinstance(runtime, dict):
        return {"has_run_overlay": False}
    source = str(runtime.get("run_overlay_source") or "").strip().lower()
    source_labels = {
        "trigger_upload": "Trigger Upload",
        "staged_override": "Staged Override",
        "upload": "Staged Override",
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
        "effective_entry_count": int(runtime.get("entry_count") or 0),
        "has_default_overlay": bool(runtime.get("has_overlay")),
        "snapshot_yaml": snapshot_yaml,
        "snapshot_label": snapshot_label,
    }


def _aggregate_mapping_suggestion_payloads(runs: list[PipelineRun]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for run in runs:
        raw_payload = run.mapping_suggestions_json
        if not raw_payload:
            continue
        try:
            payload = _json.loads(raw_payload)
        except (_json.JSONDecodeError, TypeError):
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
        "mapping_suggestions_schema_version": "mapping_suggestions_aggregate_v1",
        "suggestions": suggestions,
    }


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
        if cv_analysis_status and cv_analysis_status not in {"not run", "ready_for_generation"}:
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
    try:
        payload = _json.loads(run.results_export_json)
    except (_json.JSONDecodeError, TypeError, AttributeError):
        return []
    rows = payload.get("results")
    if not isinstance(rows, list):
        return []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _job_title_by_url_from_results_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    return {
        str(row.get("job_url") or ""): str(row.get("job_title") or "")
        for row in rows
        if row.get("job_url")
    }


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
    enriched_jobs = list_run_structured_jobs(run_id, bq, project=project, dataset=dataset)
    filter_results = list_filter_results_for_run(run_id, bq, project=project, dataset=dataset)
    filter_results_by_job_url: dict[str, dict[str, Any]] = {
        str(row.get("job_url") or ""): row for row in filter_results if row.get("job_url")
    }
    enriched_job_urls = {str(job.get("job_url") or "") for job in enriched_jobs if job.get("job_url")}
    pre_enrichment_rejects = [
        row for row in filter_results
        if str(row.get("job_url") or "") not in enriched_job_urls and row.get("reasons")
    ]
    results_rows = _results_export_rows(run)
    pipeline_outcomes_by_job_url: dict[str, dict[str, str | None]] = {
        str(row.get("job_url") or ""): {
            "status": str(row.get("pipeline_status") or ""),
            "label": PIPELINE_OUTCOME_META.get(
                str(row.get("pipeline_status") or ""),
                {"label": str(row.get("pipeline_status") or "Unknown pipeline outcome")},
            )["label"],
            "badge_class": PIPELINE_OUTCOME_META.get(
                str(row.get("pipeline_status") or ""),
                {"badge_class": "badge-info"},
            )["badge_class"],
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
    filtered_rows: list[dict[str, Any]] = []
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
        if normalized_query:
            haystack = " ".join(
                str(job.get(field) or "").lower()
                for field in ("title", "domain", "job_family", "location_type", "seniority")
            )
            if normalized_query not in haystack:
                continue
        filtered_rows.append(job)

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
        "enriched_current_page": pager["current_page"],
        "enriched_total_pages": pager["total_pages"],
        "enriched_page_size": page_size,
        "enriched_page_start": pager["start"] + 1 if pager["total"] else 0,
        "enriched_page_end": pager["end"],
        "enriched_filter": normalized_filter,
        "enriched_query": query,
        "enriched_has_prev": pager["current_page"] > 1,
        "enriched_has_next": pager["current_page"] < pager["total_pages"],
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


def _timeline_stage_summary_message(
    event: RunEvent,
    stage_artifacts_by_id: dict[str, dict[str, Any]],
) -> str:
    stage_id = _timeline_stage_download_for_event(event.stage)
    if not stage_id:
        return event.message
    artifact = stage_artifacts_by_id.get(stage_id) or {}
    outputs = artifact.get("output_counts") if isinstance(artifact.get("output_counts"), dict) else {}
    decision = artifact.get("decision_summary") if isinstance(artifact.get("decision_summary"), dict) else {}
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
            details.append(f"fresh={fresh}")
        if reused is not None:
            details.append(f"reused={reused}")
        if details:
            return f"Enrich complete: {', '.join(details)}"
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
            return f"Shortlist complete: {', '.join(details)}"
    if event.stage == "layer3_ranking":
        ranked = outputs.get("ranked_jobs")
        distribution = decision.get("label_distribution") if isinstance(decision.get("label_distribution"), dict) else {}
        details = []
        if ranked is not None:
            details.append(f"{ranked} ranked")
        for key, label in (("strong_count", "strong"), ("stretch_count", "stretch"), ("skip_count", "skip")):
            count = distribution.get(key)
            if count is not None:
                details.append(f"{label}={count}")
        if details:
            return f"Ranking complete: {', '.join(details)}"
    if event.stage == "layer4_cv_analysis":
        ready = outputs.get("generation_ready")
        skipped = outputs.get("skipped_fit_gate")
        failed = outputs.get("analysis_failed")
        if ready is not None and skipped is not None and failed is not None:
            return f"CV analysis complete: {ready} ready, {skipped} skipped, {failed} failed"
    if event.stage in {"layer4_cv_validation_failed", "pipeline_complete"}:
        accepted = outputs.get("accepted")
        validation_failed = outputs.get("validation_failed")
        generation_failed = outputs.get("generation_failed")
        persistence_failed = outputs.get("persistence_failed")
        details = []
        if accepted is not None:
            details.append(f"{accepted} accepted")
        if validation_failed is not None:
            details.append(f"{validation_failed} validation failed")
        if generation_failed is not None:
            details.append(f"{generation_failed} failed")
        if persistence_failed is not None:
            details.append(f"{persistence_failed} persistence failed")
        if details:
            return f"CV generation complete: {', '.join(details)}"
    return event.message


def _stage_download_label(stage_id: str | None) -> str | None:
    if not stage_id:
        return None
    return STAGE_DOWNLOAD_LABELS.get(stage_id, f"Download {stage_id.replace('_', ' ').title()} JSON")


class TriggerRequest(BaseModel):
    jobs_path: str = "data/sample_jobs.json"
    config_path: str = ".env.yaml"
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


def _can_cancel_run(run: PipelineRun) -> bool:
    return run.status in {RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.AWAITING_CONTINUE}


def _can_archive_run(run: PipelineRun) -> bool:
    return run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED} and run.archived_at is None


def _can_unarchive_run(run: PipelineRun) -> bool:
    return run.archived_at is not None


def create_app(bq: Any, project: str, dataset: str, redis_url: str) -> FastAPI:
    app = FastAPI(title="FitCV Admin Control Plane")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    schema_by_key = {entry["key"]: entry for entry in SETTINGS_SCHEMA}
    metadata_only_keys = {
        entry["key"]
        for entry in SETTINGS_SCHEMA
        if isinstance(entry.get("options"), list) and len(entry.get("options", [])) <= 1
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
                    "helper": "Higher values broaden recall but increase shortlist, reranking, and downstream latency.",
                    "submit_kind": "section",
                    "submit_slug": "retrieval",
                    "keys": [
                        "pipeline.vector_search_top_n",
                        "pipeline.ai_score_top_n",
                        "pipeline.final_top_n",
                        "pipeline.evidence_top_k",
                    ],
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
                    "helper": "Decide which sections appear in generated CVs without exposing retired formatting-only knobs.",
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
        {
            "id": "advanced",
            "title": "Advanced",
            "helper": "Expert-only tuning for semantic alignment, batching, concurrency, and throttle behavior.",
            "cards": [
                {
                    "id": "advanced-retrieval",
                    "title": "Advanced Retrieval Tuning",
                    "helper": "Hybrid semantic-alignment controls. Useful when evidence quality drifts more than raw throughput.",
                    "submit_kind": "section",
                    "submit_slug": "retrieval",
                    "keys": [
                        "cv_analysis.semantic_alignment.enabled",
                        "cv_analysis.semantic_alignment.model",
                        "cv_analysis.semantic_alignment.responsibility_lexical_weight",
                        "cv_analysis.semantic_alignment.responsibility_semantic_weight",
                        "cv_analysis.semantic_alignment.domain_lexical_weight",
                        "cv_analysis.semantic_alignment.domain_semantic_weight",
                        "cv_analysis.semantic_alignment.channel_pool_size",
                    ],
                    "is_advanced": True,
                },
                {
                    "id": "advanced-runtime",
                    "title": "Advanced Runtime Tuning",
                    "helper": "Timing and throttling controls. Higher concurrency does not bypass global rate limits, so use carefully.",
                    "submit_kind": "section",
                    "submit_slug": "timing",
                    "save_label": "Save Timing Settings",
                    "keys": SETTINGS_SECTIONS["timing"],
                    "is_advanced": True,
                },
            ],
        },
    ]

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

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

        def _display_value_for_settings(value: Any, entry_type: str) -> str:
            if entry_type == "bool":
                return "Yes" if bool(value) else "No"
            if isinstance(value, list):
                return ", ".join(str(item) for item in value) if value else "—"
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

        def _build_card(card_spec: dict[str, Any]) -> dict[str, Any]:
            submit_kind = str(card_spec["submit_kind"])
            submit_slug = str(card_spec["submit_slug"])
            action = (
                f"/admin/settings/group/{submit_slug}"
                if submit_kind == "group"
                else f"/admin/settings/section/{submit_slug}"
            )
            entries: list[dict[str, Any]] = []
            for key in card_spec["keys"]:
                entry = schema_by_key[key]
                effective_value = effective[key]
                draft_value = _draft_value_for_card(
                    submit_kind=submit_kind,
                    submit_slug=submit_slug,
                    key=key,
                )
                current_value = draft_value if draft_value is not None else effective_value
                entries.append(
                    {
                        "entry": entry,
                        "effective_value": effective_value,
                        "current_value": current_value,
                        "effective_display": _display_value_for_settings(effective_value, str(entry["type"])),
                        "current_display": _display_value_for_settings(current_value, str(entry["type"])),
                        "is_dirty": current_value != effective_value,
                        "is_metadata_only": key in metadata_only_keys,
                    }
                )
            dirty_count = sum(1 for item in entries if item["is_dirty"])
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
                "keys": list(card_spec["keys"]),
                "dirty_count": dirty_count,
                "error_message": _card_error_for(submit_kind, submit_slug, list(card_spec["keys"])),
                "layout": card_spec.get("layout", "list"),
                "is_advanced": bool(card_spec.get("is_advanced", False)),
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
        context: dict[str, Any] = {
            "schema": SETTINGS_SCHEMA,
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
        base_config = load_config(config_path)
        active_settings = load_active_settings(bq=bq, project=project, dataset=dataset)

        # Coerce and validate per-run overrides using the same schema
        coerced_overrides: dict[str, Any] = {}
        for k, v in config_overrides.items():
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

        run_id = str(uuid.uuid4())
        # Insert FIRST — then enqueue. DB is the source of truth.
        run = PipelineRun(
            run_id=run_id,
            status=RunStatus.QUEUED,
            triggered_by=triggered_by,
            trigger_source="ui",
            jobs_path=jobs_path,
            config_path=config_path,
            created_at=datetime.datetime.now(datetime.timezone.utc),
            effective_settings_json=_json.dumps(effective_config),
            run_mode=run_mode,
            checkpoint_status="pending_first_stage" if run_mode == "manual_staged" else None,
            next_stage="normalize" if run_mode == "manual_staged" else None,
            completed_stages=[],
        )
        insert_run(run, bq, project=project, dataset=dataset)
        _, queue_job_id = enqueue_run_with_job_id(
            jobs_path=jobs_path,
            config_path=config_path,
            triggered_by=triggered_by,
            redis_url=redis_url,
            run_id=run_id,
        )
        update_run_queue_job_id(run_id, queue_job_id, bq, project=project, dataset=dataset)
        return {"run_id": run_id}

    def _execute_trigger_with_inputs(
        jobs_path: str,
        config_path: str,
        triggered_by: str,
        config_overrides: dict[str, Any],
        *,
        jobs_input_source: str | None = None,
        jobs_input_json: str | None = None,
        candidate_profile_source: str | None = None,
        candidate_profile_json: str | None = None,
        run_synonym_overlay: dict[str, str] | None = None,
        run_synonym_overlay_filename: str | None = None,
        run_synonym_overlay_raw_yaml: str | None = None,
        run_synonym_overlay_source: str = "trigger_upload",
        run_mode: str = "run_all",
    ) -> dict:
        """Like _execute_trigger but records run-scoped input metadata."""
        # Build effective config: YAML → BQ settings → per-run overrides
        base_config = load_config(config_path)
        active_settings = load_active_settings(bq=bq, project=project, dataset=dataset)
        coerced_overrides: dict[str, Any] = {}
        for k, v in config_overrides.items():
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

        # Inject runtime candidate profile override
        if candidate_profile_json:
            effective_config.setdefault("runtime_inputs", {})["candidate_profile_json"] = candidate_profile_json

        if run_synonym_overlay:
            effective_config = apply_runtime_skill_synonym_overlay(
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
            candidate_profile_source=candidate_profile_source,
            candidate_profile_json=candidate_profile_json,
            run_mode=run_mode,
            checkpoint_status="pending_first_stage" if run_mode == "manual_staged" else None,
            next_stage="normalize" if run_mode == "manual_staged" else None,
            completed_stages=[],
        )
        insert_run(run, bq, project=project, dataset=dataset)
        _, queue_job_id = enqueue_run_with_job_id(
            jobs_path=jobs_path,
            config_path=config_path,
            triggered_by=triggered_by,
            redis_url=redis_url,
            run_id=run_id,
        )
        update_run_queue_job_id(run_id, queue_job_id, bq, project=project, dataset=dataset)
        return {"run_id": run_id}

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
        config_path: str = Form(".env.yaml"),
        run_mode: str = Form("run_all"),
        candidate_profile_mode: str = Form("default_config"),  # "default_config" | "upload" | "paste"
        candidate_profile_file: UploadFile | None = File(None),
        candidate_profile_text: str = Form(""),
        synonym_overlay_mode: str = Form("default_config"),
        synonym_overlay_file: UploadFile | None = File(None),
    ) -> dict:
        from fitcv.candidate import load_profile_json_text as _load_json_profile
        from fastapi import HTTPException as _HTTPEx

        _MAX_FILES = 20
        _MAX_TOTAL_BYTES = 50 * 1024 * 1024  # 50 MB

        upload_dir = Path("data/uploads")
        upload_dir.mkdir(parents=True, exist_ok=True)

        from fitcv.candidate import load_profile_yaml as _load_profile_yaml, validate_profile as _validate_profile

        # ── Jobs input resolution ──────────────────────────────────────
        jobs_input_json_snapshot: str | None = None
        if jobs_input_mode == "path":
            if not jobs_path or not jobs_path.strip():
                raise HTTPException(status_code=422, detail="jobs_path required for path mode")
            # Task 1: Resolve and snapshot path-mode jobs input at trigger time
            path_file = Path(jobs_path)
            if not path_file.exists():
                raise HTTPException(status_code=422, detail=f"Jobs file not found: {jobs_path}")
            try:
                raw_text = path_file.read_text(encoding="utf-8")
            except OSError as exc:
                raise HTTPException(status_code=422, detail=f"Cannot read jobs file {jobs_path}: {exc}")
            try:
                parsed_jobs = _json.loads(raw_text)
            except _json.JSONDecodeError as exc:
                raise HTTPException(status_code=422, detail=f"Invalid jobs JSON at {jobs_path}: {exc}")
            if not isinstance(parsed_jobs, list):
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid jobs JSON at {jobs_path}: top-level value must be a JSON array",
                )
            jobs_input_json_snapshot = _json.dumps(parsed_jobs, ensure_ascii=False, indent=2)
            actual_jobs_path = jobs_path
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
            # Task 2: Resolve and snapshot default_config candidate profile at trigger time
            base_cfg_for_profile = load_config(config_path)
            profile_path_str = base_cfg_for_profile.get("paths", {}).get("candidate_profile", "")
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
            candidate_json_snapshot = _json.dumps(resolved_profile, ensure_ascii=False, indent=2)
            candidate_profile_source = "default_config"
        elif candidate_profile_mode == "upload":
            if not candidate_profile_file or not candidate_profile_file.filename:
                raise HTTPException(status_code=422, detail="candidate_profile_file required for upload mode")
            raw_bytes = await candidate_profile_file.read()
            raw_text = raw_bytes.decode("utf-8")
            try:
                _load_json_profile(raw_text)  # validate
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            candidate_json_snapshot = _json.dumps(_json.loads(raw_text), ensure_ascii=False, indent=2)
            candidate_profile_source = "upload"
        elif candidate_profile_mode == "paste":
            if not candidate_profile_text or not candidate_profile_text.strip():
                raise HTTPException(status_code=422, detail="candidate_profile_text required for paste mode")
            try:
                _load_json_profile(candidate_profile_text)  # validate
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            candidate_json_snapshot = _json.dumps(
                _json.loads(candidate_profile_text), ensure_ascii=False, indent=2
            )
            candidate_profile_source = "paste"
        else:
            raise HTTPException(status_code=422, detail=f"Unknown candidate_profile_mode: {candidate_profile_mode!r}")

        # ── Run-scoped synonym overlay resolution ───────────────────────
        synonym_overlay_payload: dict[str, str] | None = None
        synonym_overlay_filename: str | None = None
        synonym_overlay_raw_yaml: str | None = None
        if synonym_overlay_mode == "default_config":
            pass
        elif synonym_overlay_mode == "upload":
            if not synonym_overlay_file or not synonym_overlay_file.filename:
                raise HTTPException(status_code=422, detail="synonym_overlay_file required for upload mode")
            synonym_overlay_filename = str(synonym_overlay_file.filename or "").strip()
            raw_bytes = await synonym_overlay_file.read()
            if not raw_bytes:
                raise HTTPException(status_code=422, detail="Uploaded synonym overlay file is empty")
            try:
                raw_text = raw_bytes.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise HTTPException(status_code=422, detail="Synonym overlay must be UTF-8 encoded text") from exc
            try:
                synonym_overlay_payload = parse_skill_synonym_overlay_yaml(raw_text)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            synonym_overlay_raw_yaml = raw_text
        else:
            raise HTTPException(status_code=422, detail=f"Unknown synonym_overlay_mode: {synonym_overlay_mode!r}")

        return _execute_trigger_with_inputs(
            jobs_path=actual_jobs_path,
            config_path=config_path,
            triggered_by="admin",
            config_overrides={},
            jobs_input_source=jobs_input_source,
            jobs_input_json=jobs_input_json_snapshot,
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
        return [_run_to_dict(r) for r in list_runs(bq, project=project, dataset=dataset)]

    @app.get("/runs/{run_id}")
    def get_run_detail(run_id: str) -> dict:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return _run_to_dict(run)

    @app.get("/runs/{run_id}/events")
    def get_run_events_list(run_id: str) -> list:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        events = get_events(run_id, bq, project=project, dataset=dataset)
        return [
            {
                "event_id": e.event_id,
                "stage": e.stage,
                "level": e.level,
                "message": e.message,
                "created_at": e.created_at.isoformat() if hasattr(e.created_at, "isoformat") else str(e.created_at),
            }
            for e in events
        ]

    @app.get("/settings")
    def get_settings_view() -> dict:
        return load_active_settings(bq=bq, project=project, dataset=dataset)

    @app.post("/settings/{key}", status_code=200)
    def update_setting(key: str, body: SettingUpdate) -> dict:
        try:
            coerced = coerce_value(key, body.value)
        except KeyError:
            raise HTTPException(status_code=422, detail=f"Unknown setting key: {key!r}")
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        try:
            validate_settings({key: coerced})
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
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
        from fastapi import Form
        from fastapi.responses import RedirectResponse
        form = await request.form()
        value = form.get("value", "")
        try:
            coerced = coerce_value(key, value)
            validate_settings({key: coerced})
        except (KeyError, ValidationError, ValueError) as exc:
            active = load_active_settings(bq=bq, project=project, dataset=dataset)
            return templates.TemplateResponse(
                request=request,
                name="settings.html",
                context=_build_settings_context(active, error=str(exc)),
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

        keys = target_registry[group_name]
        form = await request.form()

        # Coerce all keys in the group
        coerced: dict = {}
        coerce_errors: list[str] = []
        for key in keys:
            raw = _settings_form_value(form, key)
            try:
                coerced[key] = coerce_value(key, raw)
            except (KeyError, ValueError) as exc:
                coerce_errors.append(str(exc))

        def _get_active() -> dict:
            return load_active_settings(bq=bq, project=project, dataset=dataset)

        def _error_response(msg: str) -> HTMLResponse:
            active = _get_active()
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
                coerced, updated_by=updated_by, bq=bq, project=project, dataset=dataset
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

        if section_name not in SETTINGS_SECTIONS:
            raise HTTPException(status_code=404, detail=f"Unknown section: {section_name!r}")

        keys = SETTINGS_SECTIONS[section_name]
        form = await request.form()

        coerced: dict = {}
        section_errors: dict[str, str] = {}

        for key in keys:
            raw = _settings_form_value(form, key)
            try:
                coerced[key] = coerce_value(key, raw)
            except (KeyError, ValueError) as exc:
                section_errors[key] = str(exc)

        # Run cross-key validation across all coerced values in this section
        if not section_errors:
            try:
                validate_settings(coerced)
            except ValidationError as exc:
                section_errors[keys[0]] = str(exc)

        def _section_error_response(errors: dict[str, str]) -> HTMLResponse:
            active = load_active_settings(bq=bq, project=project, dataset=dataset)
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

        if section_errors:
            return _section_error_response(section_errors)

        update_id = str(uuid4())
        updated_by = f"admin:section:{update_id}"
        try:
            save_settings_group(
                coerced, updated_by=updated_by, bq=bq, project=project, dataset=dataset
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
        max_runtime_minutes = _run_max_runtime_minutes()
        runs = [_enforce_run_timeout_guard(run, max_runtime_minutes=max_runtime_minutes) for run in runs]
        return templates.TemplateResponse(
            request=request, name="runs_list.html",
            context={"runs": runs, "view": view}
        )

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
        run_id: str,
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
        filename = str(synonym_overlay_file.filename or "").strip()
        if not filename:
            raise HTTPException(status_code=422, detail="A synonym overlay YAML file is required")
        raw_bytes = await synonym_overlay_file.read()
        if not raw_bytes:
            raise HTTPException(status_code=422, detail="Uploaded synonym overlay file is empty")
        try:
            raw_text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(status_code=422, detail="Synonym overlay must be UTF-8 encoded text") from exc
        try:
            overlay_synonyms = parse_skill_synonym_overlay_yaml(raw_text)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        effective_config = _load_run_effective_config_snapshot(run)
        uploaded_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        updated_config = apply_runtime_skill_synonym_overlay(
            effective_config,
            overlay_synonyms,
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
        append_event(
            RunEvent(
                run_id=run_id,
                event_id=str(uuid.uuid4()),
                stage="synonym_overlay_uploaded",
                level="info",
                message=f"Run-scoped synonym overlay uploaded ({len(overlay_synonyms)} entries)",
                created_at=datetime.datetime.now(datetime.timezone.utc),
                payload_json=_json.dumps(
                    {
                        "filename": filename,
                        "entry_count": len(overlay_synonyms),
                    },
                    ensure_ascii=False,
                ),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        return RedirectResponse(f"/admin/runs/{run_id}", status_code=303)

    @app.post("/admin/runs/{run_id}/continue")
    def admin_continue_run(run_id: str) -> dict:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if run.run_mode != "manual_staged":
            raise HTTPException(status_code=409, detail="Only Stage by Stage runs can be continued")
        if run.status != RunStatus.AWAITING_CONTINUE:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot continue run with status '{run.status.value}'",
            )
        if not run.next_stage:
            raise HTTPException(status_code=409, detail="Run has no next stage to continue")
        _, queue_job_id = enqueue_run_with_job_id(
            jobs_path=run.jobs_path,
            config_path=run.config_path,
            triggered_by="admin",
            redis_url=redis_url,
            run_id=run.run_id,
        )
        update_run_status(run.run_id, RunStatus.QUEUED, bq, project=project, dataset=dataset)
        update_run_queue_job_id(run.run_id, queue_job_id, bq, project=project, dataset=dataset)
        update_run_checkpoint(
            run.run_id,
            bq,
            project=project,
            dataset=dataset,
            checkpoint_status="queued_for_continue",
            next_stage=run.next_stage,
            last_completed_stage=run.last_completed_stage,
            completed_stages=run.completed_stages,
            checkpoint_payload_json=run.checkpoint_payload_json,
        )
        append_event(
            RunEvent(
                run_id=run.run_id,
                event_id=str(uuid.uuid4()),
                stage="manual_continue_requested",
                level="info",
                message=f"Manual run queued to continue from {run.next_stage}",
                created_at=datetime.datetime.now(datetime.timezone.utc),
            ),
            bq,
            project=project,
            dataset=dataset,
        )
        return {"status": "queued", "run_id": run.run_id}

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

    @app.get("/admin/runs/{run_id}", response_class=HTMLResponse)
    def admin_run_detail(request: Request, run_id: str) -> HTMLResponse:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404)
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
        visible_events = events[-timeline_limit:]
        for ev in visible_events:
            stage_id = _timeline_stage_download_for_event(ev.stage)
            stage_download_url = None
            stage_download_label = None
            if (
                stage_id
                and _timeline_event_allows_stage_download(ev.stage)
                and stage_id in stage_artifacts_by_id
            ):
                stage_download_url = f"/admin/runs/{run_id}/stage-artifacts/{stage_id}.json"
                stage_download_label = _stage_download_label(stage_id)
            timeline_events.append(
                {
                    "created_at": ev.created_at,
                    "stage": ev.stage,
                    "stage_label": _timeline_stage_label(ev.stage),
                    "level": ev.level,
                    "message": _timeline_stage_summary_message(ev, stage_artifacts_by_id),
                    "stage_id": stage_id,
                    "stage_download_url": stage_download_url,
                    "stage_download_label": stage_download_label,
                }
            )
        cv_versions = list_cvs_for_run(run_id, bq, project=project, dataset=dataset)
        results_rows = _results_export_rows(run)
        job_title_by_url = _job_title_by_url_from_results_rows(results_rows)
        run_export_links = _build_run_export_links(run)

        return templates.TemplateResponse(
            request=request, name="run_detail.html", context={
                "run": run,
                "run_mode_label": RUN_MODE_LABELS.get(run.run_mode, run.run_mode),
                "events": timeline_events,
                "timeline_has_more": len(events) > timeline_limit,
                "timeline_next_limit": min(timeline_limit + 25, 200),
                "cv_versions": cv_versions,
                "stage_quality_metrics": stage_quality_metrics,
                "stage_quality_metric_rows": stage_quality_metric_rows,
                "late_stage_reuse_metrics": late_stage_reuse_metrics,
                "late_stage_reuse_metric_rows": late_stage_reuse_metric_rows,
                "run_health_rows": run_health_rows,
                "run_export_links": run_export_links,
                "job_title_by_url": job_title_by_url,
                "is_stale_cancelling": _is_stale_cancelling,
                "can_continue_manual_run": (
                    run.run_mode == "manual_staged"
                    and run.status == RunStatus.AWAITING_CONTINUE
                    and bool(run.next_stage)
                ),
                "can_upload_synonym_overlay": _can_upload_synonym_overlay(run),
                "synonym_overlay_info": _extract_run_synonym_overlay_info(run),
            }
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
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404)
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
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404)
        return templates.TemplateResponse(
            request=request,
            name="run_detail_tab_jobs_input.html",
            context={"run": run},
        )

    @app.get("/admin/runs/{run_id}/tabs/profile", response_class=HTMLResponse)
    def admin_run_detail_tab_profile(request: Request, run_id: str) -> HTMLResponse:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404)
        candidate_profile_pretty: str | None = None
        if run.candidate_profile_json:
            try:
                candidate_profile_parsed = _json.loads(run.candidate_profile_json)
                candidate_profile_pretty = _json.dumps(candidate_profile_parsed, indent=2, ensure_ascii=False)
            except (_json.JSONDecodeError, TypeError):
                candidate_profile_pretty = run.candidate_profile_json
        return templates.TemplateResponse(
            request=request,
            name="run_detail_tab_profile.html",
            context={
                "run": run,
                "candidate_profile_pretty": candidate_profile_pretty,
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
        if run.status != RunStatus.SUCCEEDED:
            raise HTTPException(status_code=409, detail="Run results export is only available for succeeded runs")
        if not run.results_export_json:
            raise HTTPException(status_code=404, detail="Run results export is not available for this run")
        pretty_json = _json.dumps(_json.loads(run.results_export_json), ensure_ascii=False, indent=2)
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-results.json"'},
        )

    @app.get("/admin/runs/{run_id}/cv-debug.json")
    def download_run_cv_debug_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if run.status != RunStatus.SUCCEEDED:
            raise HTTPException(status_code=409, detail="CV debug export is only available for succeeded runs")
        if not run.cv_generation_debug_json:
            raise HTTPException(status_code=404, detail="CV debug export is not available for this run")
        pretty_json = _json.dumps(_json.loads(run.cv_generation_debug_json), ensure_ascii=False, indent=2)
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-cv-debug.json"'},
        )

    @app.get("/admin/runs/{run_id}/stage-artifacts.json")
    def download_run_stage_transition_artifacts_json(run_id: str) -> Response:
        run = get_run(run_id, bq, project=project, dataset=dataset)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        if not run.stage_transition_artifacts_json:
            raise HTTPException(status_code=404, detail="Stage transition artifacts export is not available for this run")
        pretty_json = _json.dumps(_json.loads(run.stage_transition_artifacts_json), ensure_ascii=False, indent=2)
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
        if not artifact_files:
            raise HTTPException(
                status_code=404,
                detail="No run artifacts are currently available for this run",
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
        pretty_json = _json.dumps(_json.loads(run.settings_used_json), ensure_ascii=False, indent=2)
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
        pretty_json = _json.dumps(_json.loads(run.mapping_suggestions_json), ensure_ascii=False, indent=2)
        return Response(
            content=pretty_json,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="fitcv-run-{run_id}-mapping-suggestions.json"'},
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
    }
