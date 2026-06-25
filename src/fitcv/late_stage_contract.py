"""@meta
name: late_stage_contract
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared late-stage status helpers for analysis/generation/pipeline consumers.
inputs:
  - late-stage status strings and ranked job payloads
outputs:
  - canonical status mappings and deterministic truth payloads
lifecycle:
  - status: active
"""

from __future__ import annotations

from typing import Any

CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS = "blocked_by_reranker_fit"
CV_ANALYSIS_READY_FOR_GENERATION_STATUS = "ready_for_generation"
CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS = "skipped_fit_gate"
CV_ANALYSIS_FAILED_STATUS = "analysis_failed"
CV_GENERATION_REVIEW_REQUIRED_STATUS = "review_required"


def shortlist_status_for_ranked_job(job: dict[str, Any]) -> str:
    shortlist_origin = str(job.get("shortlist_origin") or "").strip().lower()
    if shortlist_origin == "backfill":
        return "backfilled_for_scoring"
    return "returned_by_vector_search"


def validation_status_for_cv_status(status: str) -> str:
    if status == "accepted":
        return "accepted"
    if status == "validation_failed":
        return "failed"
    if status == "persistence_failed":
        return "accepted"
    return "not_run"


def deterministic_truth_fields(status: str | None) -> dict[str, str | None]:
    normalized_status = str(status or "").strip()
    if not normalized_status:
        return {
            "deterministic_outcome": None,
            "stage_owned_subreason": None,
            "source_stage": None,
        }
    if normalized_status == "accepted":
        return {
            "deterministic_outcome": "accepted",
            "stage_owned_subreason": normalized_status,
            "source_stage": "cv_generation",
        }
    if normalized_status == CV_GENERATION_REVIEW_REQUIRED_STATUS:
        return {
            "deterministic_outcome": "not_applicable",
            "stage_owned_subreason": normalized_status,
            "source_stage": "cv_generation",
        }
    if normalized_status in {"validation_failed", "generation_failed", "persistence_failed"}:
        return {
            "deterministic_outcome": "rejected",
            "stage_owned_subreason": normalized_status,
            "source_stage": "cv_generation",
        }
    if normalized_status == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS:
        return {
            "deterministic_outcome": "blocked",
            "stage_owned_subreason": normalized_status,
            "source_stage": "cv_analysis",
        }
    if normalized_status == CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS:
        return {
            "deterministic_outcome": "skipped",
            "stage_owned_subreason": normalized_status,
            "source_stage": "cv_analysis",
        }
    if normalized_status == CV_ANALYSIS_FAILED_STATUS:
        return {
            "deterministic_outcome": "rejected",
            "stage_owned_subreason": normalized_status,
            "source_stage": "cv_analysis",
        }
    if normalized_status == CV_ANALYSIS_READY_FOR_GENERATION_STATUS:
        return {
            "deterministic_outcome": None,
            "stage_owned_subreason": normalized_status,
            "source_stage": "cv_analysis",
        }
    return {
        "deterministic_outcome": None,
        "stage_owned_subreason": None,
        "source_stage": None,
    }


def cv_generation_status_for_analysis_status(status: str) -> str:
    if status in {
        CV_ANALYSIS_READY_FOR_GENERATION_STATUS,
        CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS,
        CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS,
        CV_ANALYSIS_FAILED_STATUS,
    }:
        return "not_attempted"
    return "failed"
