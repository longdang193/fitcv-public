"""@meta
name: pipeline
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.pipeline.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

"""Full pipeline orchestrator — wires all FitCV pipeline stages end-to-end.

Stage order
-----------
1. Ingest + normalise + enrich
2. Candidate profile load
3a. Rule filter (BEFORE embedding — keeps shortlist clean and reduces cost)
3b. Embed eligible jobs + candidate, then vector shortlist + AI scoring
3c. Build ranking features; final ranking
4. Per-job: evidence retrieval → gap analysis → CV generation → validation → versioning

Failure policy
--------------
- Fail fast on setup issues: missing config, bad credentials, unreadable profile.
- Per-job failures in Layer 4 are caught, logged, and skipped (partial success is OK).

Config keys consumed
--------------------
config["paths"]["candidate_profile"]       path to candidate YAML
config["pipeline"]["vector_search_top_n"]  top-N for vector shortlist (e.g. 50)
config["pipeline"]["ai_score_top_n"]       top-N cap for AI scoring  (e.g. 50)
config["pipeline"]["final_top_n"]          final ranked list size     (e.g. 10)
config["pipeline"]["evidence_top_k"]       evidence items per job     (e.g. 5)

embed_scope note
----------------
v1 embeds only rule-passing jobs (cheaper, faster).
"""

import logging
import hashlib
import json
import os
import time
import uuid
import datetime
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from copy import deepcopy
from typing import Any, Callable, cast

from fitcv.ai_score import build_ai_score_input_fingerprint, run_ai_scoring
from fitcv.agentic_cv_analysis import analyze_ranked_job as run_agentic_cv_analysis
from fitcv.agentic_cv_generation import generate_from_analysis as run_agentic_cv_generation
from fitcv.candidate import (
    flatten_skills,
    infer_effective_preferences,
    load_candidate_to_bigquery,
    load_profile_json_text,
    load_profile_yaml,
)
from fitcv.config import (
    CV_SECTION_KEY_TO_NAME,
    get_cv_acceptance_policy,
    get_cv_generation_model,
    get_cv_generation_prompt_version,
    get_gemini_model,
    get_stage_runtime_concurrency,
    load_config,
    resolve_model_routing_part,
)
from fitcv.contracts import (
    STAGE_TRANSITION_ARTIFACTS_PIPELINE_SCHEMA_VERSION,
    normalize_analysis_channel_mapping,
)
from fitcv.pipeline_contracts import ReviewRequiredReasonCode
from fitcv.pipeline_stages.common import (
    pipeline_int,
    extract_job_title,
    extract_job_url,
    normalize_shortlist_row,
    json_safe_value,
    shortlist_outcome_for_row,
    unique_job_urls,
    compute_raw_shortlist_anomaly_urls,
    job_sample,
    candidate_profile_summary,
    shortlist_row_sample,
    ranking_row_sample,
    analysis_record_output_sample,
    analysis_record_changed_sample,
    debug_record_output_sample,
    debug_record_changed_sample,
)
from fitcv.cv_generator import generate_cv, render_cv_markdown
from fitcv.embeddings import embed_and_store_candidate, embed_and_store_jobs
from fitcv.enrich import (
    FRESH_ENRICHMENT_STATUS,
    REUSED_CACHED_ENRICHMENT_STATUS,
    build_enrich_contract_fingerprint,
    build_raw_job_fingerprint,
    enrich_batch,
    get_enrich_prompt_provenance,
    load_run_structured_jobs,
    load_structured_jobs,
    lookup_reusable_structured_jobs,
)
from fitcv.evidence import (
    build_cv_analysis_input_fingerprint,
    retrieve_evidence,
    retrieve_evidence_bundle,
)
from fitcv.gap_analysis import classify_fit, compute_gap
from fitcv.ingest import load_to_bigquery, parse_jobs_file, prepare_raw_rows
from fitcv.normalize import normalize_batch, normalize_batch_with_exclusions
from fitcv.ranking import (
    compute_feature_contributions,
    compute_final_score,
    compute_must_have_match,
    compute_preference_fit_details,
    compute_preference_fit,
    compute_ranking_runtime_diagnostics,
    compute_seniority_fit,
    compute_title_relevance,
    get_active_missing_value_defaults,
    get_preference_fit_weights,
    get_active_ranking_weights,
    rank_jobs,
    store_final_ranking,
)
from fitcv.late_stage_contract import (
    cv_generation_status_for_analysis_status as _shared_cv_generation_status_for_analysis_status,
    deterministic_truth_fields as _shared_deterministic_truth_fields,
    shortlist_status_for_ranked_job as _shared_shortlist_status_for_ranked_job,
    validation_status_for_cv_status as _shared_validation_status_for_cv_status,
)
from fitcv.ranking_contract import fit_label_from_score
from fitcv.rule_filter import (
    apply_pre_enrichment_global_filters,
    apply_rule_filters,
    store_filter_results,
)
from fitcv.runtime_routing import resolve_cv_generation_runtime_provenance
from fitcv.runtime_routing import validate_cv_generation_routing_ready
from fitcv.tracker import (
    create_cv_version_record,
    lookup_reusable_cv_versions,
    store_cv_version,
)
from fitcv.validator import AnalysisGroundingPayload, run_all_validations
from fitcv.telemetry import (
    bound_langfuse_excerpt,
    bound_langfuse_issue_list,
    bound_langfuse_list,
    bound_langfuse_markdown,
    build_langfuse_item_observation_attributes,
    build_trace_context,
    observe_span,
    render_langfuse_labeled_list_section,
    render_langfuse_labeled_text_section,
    render_langfuse_markdown_sections,
    set_span_attributes,
)
from fitcv.vector_search import run_vector_search
from fitcv.vector_search import store_shortlist
from fitcv.pipeline_observability import build_cv_analysis_item_observation_attributes as _build_cv_analysis_item_observation_attributes_observability
from fitcv.pipeline_observability import build_cv_generation_item_observation_attributes as _build_cv_generation_item_observation_attributes_observability
from fitcv.pipeline_observability import build_bounded_event_payload as _build_bounded_event_payload_observability
from fitcv.pipeline_observability import render_cv_analysis_item_output as _render_cv_analysis_item_output_observability
from fitcv.pipeline_observability import render_cv_generation_item_output as _render_cv_generation_item_output_observability
from fitcv.pipeline_stage_artifacts import build_stage_block as _build_stage_block_artifacts
from fitcv.pipeline_stage_artifacts import build_stage_block_not_reached as _build_stage_block_not_reached_artifacts
from fitcv.pipeline_stage_artifacts import build_normalize_stage_block as _build_normalize_stage_block_artifacts
from fitcv.pipeline_stage_artifacts import build_enrich_stage_block as _build_enrich_stage_block_artifacts
from fitcv.pipeline_stage_artifacts import build_rule_filter_stage_block as _build_rule_filter_stage_block_artifacts
from fitcv.pipeline_stage_artifacts import build_shortlist_stage_block as _build_shortlist_stage_block_artifacts
from fitcv.pipeline_stage_artifacts import build_ranking_stage_block as _build_ranking_stage_block_artifacts
from fitcv.pipeline_stage_artifacts import build_cv_analysis_stage_block as _build_cv_analysis_stage_block_artifacts
from fitcv.pipeline_stage_artifacts import build_cv_generation_stage_block as _build_cv_generation_stage_block_artifacts
from fitcv.pipeline_stage_artifacts import sample_rows as _sample_rows_artifacts
from fitcv.pipeline_stage_artifacts import sample_strings as _sample_strings_artifacts
from fitcv.pipeline_stage_artifacts import truncate_stage_text as _truncate_stage_text_artifacts
from fitcv.pipeline_stage_artifacts import truncate_stage_value as _truncate_stage_value_artifacts
from fitcv.reuse import build_reuse_decision
from fitcv.reuse import resolve_reuse_stage_policy
from fitcv.pipeline_store import PipelineStore
from fitcv.pipeline_stage_context import (
    PipelineState,
    infer_last_completed_stage_from_state,
)

logger = logging.getLogger(__name__)
_REPAIRABLE_VALIDATION_FIELDS = ("grounding_violations", "skill_violations")
_EXPORT_ENRICHED_JOB_FIELDS = (
    "location_type",
    "seniority",
    "required_skills",
    "required_skills_canonical",
    "required_skill_entities",
    "preferred_skills",
    "preferred_skills_canonical",
    "preferred_skill_entities",
    "responsibilities",
    "domain",
    "tech_stack",
    "years_experience_min",
    "years_experience_max",
    "keywords",
    "job_family",
    "mapping_suggestions",
    "enrichment_version",
    "enrichment_model",
    "enriched_at",
    "raw_job_fingerprint",
    "enrich_contract_fingerprint",
    "enrich_reuse_status",
)
_DEDUPE_REASON_LABELS = {
    "duplicate_job_url": "duplicate_job_url",
    "near_duplicate_job_posting": "near_duplicate_job_posting",
}
_EMPTY_REPAIR_ATTEMPT = {"performed": False, "missing_sections": []}
_CANDIDATE_NAME_PLACEHOLDER_VALUES = {
    "candidate name",
    "your name",
}
_FIT_LABEL_ORDER = {"skip": 0, "stretch": 1, "strong": 2}
_STAGE_ARTIFACT_SAMPLE_LIMIT = 20
_STAGE_ARTIFACT_TEXT_LIMIT = 240
CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS = "blocked_by_reranker_fit"
CV_ANALYSIS_READY_FOR_GENERATION_STATUS = "ready_for_generation"
CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS = "skipped_fit_gate"
CV_ANALYSIS_FAILED_STATUS = "analysis_failed"
CV_GENERATION_REVIEW_REQUIRED_STATUS = "review_required"
PIPELINE_STATUS_RANKED_BLOCKED_BY_RERANKER = "ranked_blocked_by_reranker_fit"
PIPELINE_STAGE_SEQUENCE = (
    "normalize",
    "enrich",
    "rule_filter",
    "shortlist",
    "ranking",
    "cv_analysis",
    "cv_generation",
)
_PIPELINE_STAGE_SET = set(PIPELINE_STAGE_SEQUENCE)

def _build_stage_dispatch_map() -> dict[str, str]:
    """Build canonical stage-dispatch scaffold keyed by pipeline stage order."""
    return {stage_name: stage_name for stage_name in PIPELINE_STAGE_SEQUENCE}

def _prompt_runtime_metadata(
    config: dict[str, Any],
    *,
    stage_id: str,
    prompt_key: str,
) -> dict[str, str]:
    stage_block = dict((config.get("prompts_runtime") or {}).get(stage_id) or {})
    prompt_block = dict(stage_block.get(prompt_key) or {})
    return {
        "prompt_id": str(prompt_block.get("prompt_id") or ""),
        "prompt_version": str(prompt_block.get("version") or ""),
        "template_path": str(prompt_block.get("template_path") or ""),
        "stage_id": str(prompt_block.get("stage_id") or ""),
    }

def _materialize_scoring_shortlist(
    raw_shortlist: list[dict[str, Any]],
    passed_jobs: list[dict[str, Any]],
    vector_search_top_n: int,
) -> list[dict[str, Any]]:
    """Build the shortlist used for AI scoring from raw vector-search rows.

    VECTOR_SEARCH returns only `job_url` + similarity/rank, but downstream
    scoring needs the full structured JD fields. We therefore merge raw vector
    rows back onto the corresponding passed jobs.

    We also backfill any passed jobs missing from the raw shortlist while
    capacity remains. This protects against transient read-after-write gaps in
    BigQuery job embeddings visibility without losing the fact that retrieval
    itself missed the job URL.
    """
    passed_by_url = {
        extract_job_url(job): job
        for job in passed_jobs
        if extract_job_url(job)
    }
    scoring_shortlist: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for row in raw_shortlist:
        job_url = extract_job_url(row)
        if not job_url or job_url in seen_urls:
            continue
        passed_job = passed_by_url.get(job_url)
        if passed_job is None:
            continue
        seen_urls.add(job_url)
        scoring_shortlist.append(
            {
                **passed_job,
                "job_url": job_url,
                **normalize_shortlist_row(row),
                "vector_rank": len(scoring_shortlist) + 1,
                "shortlist_origin": "vector_search",
            }
        )

    next_rank = len(scoring_shortlist) + 1
    for job in passed_jobs:
        if len(scoring_shortlist) >= vector_search_top_n:
            break
        job_url = extract_job_url(job)
        if not job_url or job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        scoring_shortlist.append(
            {
                **job,
                "job_url": job_url,
                "vector_similarity": 0.0,
                "vector_rank": next_rank,
                "shortlist_origin": "backfill",
            }
        )
        next_rank += 1

    return scoring_shortlist



def _finalize_fresh_enriched_rows(
    rows: list[dict[str, Any]],
    *,
    raw_job_fingerprints: dict[str, str],
    enrich_contract_fingerprint: str,
) -> list[dict[str, Any]]:
    for row in rows:
        job_url = extract_job_url(row)
        if not job_url:
            continue
        row["raw_job_fingerprint"] = raw_job_fingerprints.get(job_url)
        row["enrich_contract_fingerprint"] = enrich_contract_fingerprint
        row["enrich_reuse_status"] = FRESH_ENRICHMENT_STATUS
        row["reuse_decision"] = build_reuse_decision(
            decision=FRESH_ENRICHMENT_STATUS,
            reason_code="no_reusable_enrichment_row",
            fingerprint=raw_job_fingerprints.get(job_url),
            source_artifact_type="enrich",
        )
    return rows


def _finalize_reused_enriched_row(
    row: dict[str, Any],
    *,
    raw_job_fingerprints: dict[str, str],
    enrich_contract_fingerprint: str,
) -> dict[str, Any]:
    job_url = extract_job_url(row)
    if not job_url:
        return row
    row["raw_job_fingerprint"] = raw_job_fingerprints.get(job_url)
    row["enrich_contract_fingerprint"] = enrich_contract_fingerprint
    row["enrich_reuse_status"] = REUSED_CACHED_ENRICHMENT_STATUS
    row["reuse_decision"] = build_reuse_decision(
        decision=REUSED_CACHED_ENRICHMENT_STATUS,
        reason_code="exact_fingerprint_match",
        fingerprint=raw_job_fingerprints.get(job_url),
        source_artifact_type="enrich",
    )
    return row
def _enrich_jobs_with_reuse(
    normalized_jobs: list[dict[str, Any]],
    config: dict[str, Any],
    *,
    pipeline_store: PipelineStore | None = None,
    heartbeat_callback: Callable[[dict[str, Any]], None] | None = None,
    incremental_save_run_id: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    import threading

    if not normalized_jobs:
        return [], []
    if pipeline_store is None:
        pipeline_store = PipelineStore(
            load_raw_jobs_fn=load_to_bigquery,
            load_candidate_profile_fn=load_candidate_to_bigquery,
            lookup_reusable_structured_jobs_fn=lookup_reusable_structured_jobs,
            load_structured_jobs_fn=load_structured_jobs,
            load_run_structured_jobs_fn=load_run_structured_jobs,
            store_filter_results_fn=store_filter_results,
            embed_and_store_jobs_fn=embed_and_store_jobs,
            store_shortlist_fn=store_shortlist,
            store_final_ranking_fn=store_final_ranking,
            store_cv_version_fn=store_cv_version,
        )

    raw_job_fingerprints: dict[str, str] = {}
    for job in normalized_jobs:
        job_url = extract_job_url(job)
        if not job_url:
            continue
        raw_job_fingerprints[job_url] = build_raw_job_fingerprint(job)["fingerprint"]

    enrich_contract_fingerprint = build_enrich_contract_fingerprint(config)["fingerprint"]
    enrich_reuse_enabled = _reuse_stage_enabled(config, "enrich")
    reused_rows_by_url = (
        pipeline_store.lookup_reusable_structured_jobs(
            normalized_jobs,
            config,
            raw_job_fingerprints=raw_job_fingerprints,
            enrich_contract_fingerprint=enrich_contract_fingerprint,
        )
        if enrich_reuse_enabled
        else {}
    )

    fresh_jobs = [
        job for job in normalized_jobs
        if extract_job_url(job) and extract_job_url(job) not in reused_rows_by_url
    ]
    fresh_rows: list[dict[str, Any]] = []
    incrementally_saved_fresh_urls: set[str] = set()

    def _run_enrich_call_with_polling(
        fn: Callable[[], list[dict[str, Any]]],
        *,
        timeout_secs: int | None = None,
        heartbeat_interval_secs: int | None = None,
        on_progress: Callable[[int, float], None] | None = None,
    ) -> list[dict[str, Any]]:
        result_holder: dict[str, Any] = {"rows": None, "error": None}

        def _target() -> None:
            try:
                result_holder["rows"] = fn()
            except Exception as exc:  # noqa: BLE001
                result_holder["error"] = exc

        worker = threading.Thread(target=_target, daemon=True)
        worker.start()
        started_at = time.monotonic()
        heartbeat_count = 0
        poll_interval = float(heartbeat_interval_secs or 1)

        while worker.is_alive():
            worker.join(timeout=poll_interval)
            elapsed = max(0.0, time.monotonic() - started_at)
            if timeout_secs is not None and elapsed >= float(timeout_secs):
                raise TimeoutError(f"Enrich timeout after {timeout_secs}s")
            if worker.is_alive() and on_progress is not None and heartbeat_interval_secs is not None:
                heartbeat_count += 1
                on_progress(heartbeat_count, elapsed)

        error = result_holder.get("error")
        if error is not None:
            raise error
        return list(result_holder.get("rows") or [])

    if fresh_jobs:
        heartbeat_events_enabled = str(os.environ.get("FITCV_ENRICH_HEARTBEAT_EVENTS", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if not heartbeat_events_enabled:
            heartbeat_callback = None

        debug_heartbeat_enabled = str(os.environ.get("FITCV_ENRICH_DEBUG_HEARTBEAT", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        emit_job_events_enabled = str(os.environ.get("FITCV_ENRICH_EMIT_JOB_EVENTS", "") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        per_job_timeout_raw = str(os.environ.get("FITCV_ENRICH_JOB_TIMEOUT_SECS", "180") or "180").strip()
        try:
            per_job_timeout_secs = max(10, int(float(per_job_timeout_raw)))
        except ValueError:
            per_job_timeout_secs = 180
        heartbeat_interval_raw = str(os.environ.get("FITCV_ENRICH_HEARTBEAT_SECS", "15") or "15").strip()
        try:
            heartbeat_interval_secs = max(5, int(float(heartbeat_interval_raw)))
        except ValueError:
            heartbeat_interval_secs = 15

        def _persist_incremental_fresh_rows(rows: list[dict[str, Any]]) -> None:
            if not incremental_save_run_id or not rows:
                return
            finalized_rows = _finalize_fresh_enriched_rows(
                rows,
                raw_job_fingerprints=raw_job_fingerprints,
                enrich_contract_fingerprint=enrich_contract_fingerprint,
            )
            pipeline_store.load_structured_jobs(finalized_rows, config)
            pipeline_store.load_run_structured_jobs(finalized_rows, incremental_save_run_id, config)
            for row in finalized_rows:
                job_url = extract_job_url(row)
                if job_url:
                    incrementally_saved_fresh_urls.add(job_url)

        if debug_heartbeat_enabled:
            total = len(fresh_jobs)
            for idx, job in enumerate(fresh_jobs, start=1):
                job_url = extract_job_url(job) or f"job_{idx}"
                if heartbeat_callback:
                    heartbeat_callback(
                        {
                            "phase": "job_start",
                            "index": idx,
                            "total": total,
                            "job_url": job_url,
                            "timeout_secs": per_job_timeout_secs,
                        }
                    )
                try:
                    batch_rows = _run_enrich_call_with_polling(
                        lambda: enrich_batch([job], config),
                        timeout_secs=per_job_timeout_secs,
                    )
                except TimeoutError as exc:
                    if heartbeat_callback:
                        heartbeat_callback(
                            {
                                "phase": "job_timeout",
                                "index": idx,
                                "total": total,
                                "job_url": job_url,
                                "timeout_secs": per_job_timeout_secs,
                            }
                        )
                    raise TimeoutError(
                        f"Enrich timeout for {job_url} after {per_job_timeout_secs}s (job {idx}/{total})"
                    ) from exc
                if batch_rows:
                    fresh_rows.extend(batch_rows)
                    _persist_incremental_fresh_rows(batch_rows)
                if heartbeat_callback:
                    heartbeat_callback(
                        {
                            "phase": "job_done",
                            "index": idx,
                            "total": total,
                            "job_url": job_url,
                            "rows": len(batch_rows or []),
                        }
                    )
        else:
            job_index_by_url: dict[str, int] = {}
            job_index_lock = threading.Lock()
            next_job_index = 0

            def _job_event_callback(evt: dict[str, Any]) -> None:
                nonlocal next_job_index
                if not emit_job_events_enabled or heartbeat_callback is None:
                    return
                phase = str(evt.get("phase") or "").strip()
                job_url = str(evt.get("job_url") or "").strip()
                if not phase or not job_url:
                    return
                if phase == "job_start":
                    with job_index_lock:
                        next_job_index += 1
                        job_index_by_url[job_url] = next_job_index
                    heartbeat_callback(
                        {
                            "phase": "job_start",
                            "index": job_index_by_url.get(job_url),
                            "total": len(fresh_jobs),
                            "job_url": job_url,
                            "timeout_secs": per_job_timeout_secs,
                        }
                    )
                    return
                if phase == "job_done":
                    heartbeat_callback(
                        {
                            "phase": "job_done",
                            "index": job_index_by_url.get(job_url),
                            "total": len(fresh_jobs),
                            "job_url": job_url,
                            "elapsed_secs": evt.get("elapsed_secs"),
                        }
                    )
                    return

            if heartbeat_callback:
                heartbeat_callback(
                    {
                        "phase": "batch_start",
                        "fresh_jobs_total": len(fresh_jobs),
                        "reused_jobs_total": len(reused_rows_by_url),
                        "heartbeat_interval_secs": heartbeat_interval_secs,
                    }
                )
            def _on_chunk_complete(chunk_rows: list[dict[str, Any]]) -> None:
                try:
                    _persist_incremental_fresh_rows(chunk_rows)
                except Exception:
                    logger.warning("Incremental save failed for chunk", exc_info=True)

            fresh_rows = _run_enrich_call_with_polling(
                lambda: enrich_batch(
                    fresh_jobs,
                    config,
                    job_event_callback=_job_event_callback if emit_job_events_enabled else None,
                    on_chunk_complete=_on_chunk_complete if incremental_save_run_id else None,
                ),
                heartbeat_interval_secs=heartbeat_interval_secs,
                on_progress=(
                    lambda heartbeat_count, elapsed_secs: heartbeat_callback(
                        {
                            "phase": "batch_progress",
                            "fresh_jobs_total": len(fresh_jobs),
                            "reused_jobs_total": len(reused_rows_by_url),
                            "heartbeat_count": heartbeat_count,
                            "elapsed_secs": int(elapsed_secs),
                            "heartbeat_interval_secs": heartbeat_interval_secs,
                        }
                    ) if heartbeat_callback else None
                ),
            )
            if heartbeat_callback:
                heartbeat_callback(
                    {
                        "phase": "batch_done",
                        "fresh_jobs_total": len(fresh_jobs),
                        "reused_jobs_total": len(reused_rows_by_url),
                        "fresh_rows_total": len(fresh_rows),
                    }
                )
        _finalize_fresh_enriched_rows(
            fresh_rows,
            raw_job_fingerprints=raw_job_fingerprints,
            enrich_contract_fingerprint=enrich_contract_fingerprint,
        )
        pending_fresh_rows = [
            row
            for row in fresh_rows
            if (extract_job_url(row) or "") not in incrementally_saved_fresh_urls
        ]
        if pending_fresh_rows and pipeline_store is not None:
            pipeline_store.load_structured_jobs(pending_fresh_rows, config)
            if incremental_save_run_id:
                pipeline_store.load_run_structured_jobs(
                    pending_fresh_rows,
                    incremental_save_run_id,
                    config,
                )

    fresh_rows_by_url = {
        extract_job_url(row): row
        for row in fresh_rows
        if extract_job_url(row)
    }
    enriched_rows: list[dict[str, Any]] = []
    reused_rows: list[dict[str, Any]] = []
    for job in normalized_jobs:
        job_url = extract_job_url(job)
        if not job_url:
            continue
        reused_row = reused_rows_by_url.get(job_url)
        if reused_row is not None:
            finalized_reused_row = _finalize_reused_enriched_row(
                reused_row,
                raw_job_fingerprints=raw_job_fingerprints,
                enrich_contract_fingerprint=enrich_contract_fingerprint,
            )
            reused_rows.append(finalized_reused_row)
            enriched_rows.append(finalized_reused_row)
            continue
        fresh_row = fresh_rows_by_url.get(job_url)
        if fresh_row is not None:
            enriched_rows.append(fresh_row)
    if incremental_save_run_id and reused_rows and pipeline_store is not None:
        pipeline_store.load_run_structured_jobs(
            reused_rows,
            incremental_save_run_id,
            config,
        )
    return enriched_rows, fresh_rows

def _enrich_runtime_projection(config: dict[str, Any]) -> dict[str, Any]:
    projected = dict(config)
    stage_runtime = dict(projected.get("stage_runtime") or {})
    enrich_runtime = dict(stage_runtime.get("enrich") or {})
    if "sleep_secs" in enrich_runtime:
        projected["enrichment_sleep_secs"] = enrich_runtime["sleep_secs"]
    if "batch_size" in enrich_runtime:
        projected["enrichment_batch_size"] = enrich_runtime["batch_size"]
    if "concurrency" in enrich_runtime:
        projected["enrichment_concurrency"] = enrich_runtime["concurrency"]
    return projected

def _reuse_stage_enabled(config: dict[str, Any], stage: str) -> bool:
    return bool(resolve_reuse_stage_policy(config, stage).enabled)


def _merge_ranked_job_with_enriched_context(
    ranked_job: dict[str, Any],
    enriched_by_url: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    job_url = extract_job_url(ranked_job)
    enriched_job = enriched_by_url.get(job_url, {})
    if not enriched_job:
        return dict(ranked_job)
    return {
        **enriched_job,
        **ranked_job,
    }


def _build_export_results(
    *,
    raw_jobs: list[dict[str, Any]],
    enriched: list[dict[str, Any]],
    deduplicated_jobs: list[dict[str, Any]],
    pre_filter_rejected: list[dict[str, Any]],
    candidate_filter_rejected: list[dict[str, Any]],
    passed_jobs: list[dict[str, Any]],
    raw_shortlist: list[dict[str, Any]],
    shortlist_for_scoring: list[dict[str, Any]],
    ranking_inputs: list[dict[str, Any]],
    ranked: list[dict[str, Any]],
    cv_analysis_results: list[dict[str, Any]],
    cv_results: list[dict[str, Any]],
    cv_generation_debug_records: list[dict[str, Any]],
    vector_search_top_n: int,
) -> list[dict[str, Any]]:
    original_by_url = {extract_job_url(job): job for job in raw_jobs if extract_job_url(job)}
    enriched_by_url = {extract_job_url(job): job for job in enriched if extract_job_url(job)}
    passed_by_url = {extract_job_url(job): job for job in passed_jobs if extract_job_url(job)}
    raw_shortlist_by_url = {
        extract_job_url(job): normalize_shortlist_row(job)
        for job in raw_shortlist
        if extract_job_url(job)
    }
    scoring_shortlist_by_url = {
        extract_job_url(job): normalize_shortlist_row(job)
        for job in shortlist_for_scoring
        if extract_job_url(job)
    }
    scoring_by_url = {extract_job_url(job): job for job in ranking_inputs if extract_job_url(job)}
    ranked_by_url = {extract_job_url(job): job for job in ranked if extract_job_url(job)}
    analysis_by_url = {
        str(record.get("job_url") or ""): record
        for record in cv_analysis_results
        if str(record.get("job_url") or "")
    }
    cv_by_url = {str(item["job_url"]): item for item in cv_results if item.get("job_url")}
    passed_job_urls = set(passed_by_url)
    debug_by_url = {
        str(record.get("job_url") or ""): record
        for record in cv_generation_debug_records
        if str(record.get("job_url") or "")
    }
    skipped_fit_gate_urls = {
        str(record.get("job_url") or "")
        for record in cv_generation_debug_records
        if str(record.get("status") or "") == "skipped_fit_gate" and str(record.get("job_url") or "")
    }
    blocked_by_reranker_urls = {
        str(record.get("job_url") or "")
        for record in cv_generation_debug_records
        if str(record.get("status") or "") == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS and str(record.get("job_url") or "")
    }
    deduplicated_by_input_index = {
        int(job.get("input_index", -1)): job
        for job in deduplicated_jobs
        if job.get("input_index") is not None
    }

    reject_reasons_by_url: dict[str, list[str]] = {}
    rule_filter_marks_by_url: dict[str, list[dict[str, Any]]] = {
        extract_job_url(job): list(job.get("marks") or [])
        for job in passed_jobs
        if extract_job_url(job)
    }
    rejected_before_enrichment_urls: set[str] = set()
    rejected_after_enrichment_urls: set[str] = set()
    for rejected in pre_filter_rejected:
        job_url = str(rejected.get("job_url") or "")
        if not job_url:
            continue
        reject_reasons_by_url[job_url] = list(rejected.get("reasons") or [])
        rejected_before_enrichment_urls.add(job_url)
    for rejected in candidate_filter_rejected:
        job_url = str(rejected.get("job_url") or "")
        if not job_url:
            continue
        reject_reasons_by_url[job_url] = list(rejected.get("reasons") or [])
        rule_filter_marks_by_url[job_url] = list(rejected.get("marks") or [])
        rejected_after_enrichment_urls.add(job_url)

    def _status_for(job_url: str) -> str:
        if job_url in cv_by_url:
            return "ranked_with_cv"
        if job_url in blocked_by_reranker_urls:
            return PIPELINE_STATUS_RANKED_BLOCKED_BY_RERANKER
        if job_url in skipped_fit_gate_urls:
            return "ranked_skipped_fit_gate"
        if job_url in ranked_by_url:
            return "ranked_no_cv"
        if job_url in rejected_before_enrichment_urls:
            return "rejected_before_enrichment"
        if job_url in rejected_after_enrichment_urls:
            return "rejected_after_enrichment"
        if job_url in scoring_by_url:
            return "scored_not_ranked"
        if job_url in scoring_shortlist_by_url:
            return "shortlisted_not_scored"
        if job_url in passed_by_url:
            return "not_shortlisted"
        return "unknown_pipeline_state"

    def _sort_key(row: dict[str, Any]) -> tuple[int, float, float, float, int, int]:
        status = str(row["pipeline_status"])
        category = {
            "ranked_with_cv": 0,
            PIPELINE_STATUS_RANKED_BLOCKED_BY_RERANKER: 1,
            "ranked_skipped_fit_gate": 2,
            "ranked_no_cv": 3,
            "not_shortlisted": 4,
            "shortlisted_not_scored": 5,
            "scored_not_ranked": 6,
            "rejected_after_enrichment": 7,
            "rejected_before_enrichment": 8,
            "deduplicated_before_enrichment": 9,
            "unknown_pipeline_state": 10,
        }.get(status, 10)
        scores = dict(row.get("scores") or {})
        final_score = float(scores.get("final_score") or 0.0)
        ai_score = float(scores.get("ai_score") or 0.0)
        vector_score = float(scores.get("vector_score") or 0.0)
        rank = int(row.get("rank") or 0) or 999999
        input_index = int(row.get("_input_index") or 0)
        return (category, -final_score, -ai_score, -vector_score, rank, input_index)

    rows: list[dict[str, Any]] = []
    for input_index, raw_job in enumerate(raw_jobs):
        job_url = extract_job_url(raw_job)
        enriched_job = enriched_by_url.get(job_url)
        deduplicated_job = deduplicated_by_input_index.get(input_index)
        score_source = {
            **scoring_shortlist_by_url.get(job_url, {}),
            **scoring_by_url.get(job_url, {}),
            **ranked_by_url.get(job_url, {}),
        }
        cv_row = cv_by_url.get(job_url)
        analysis_row = analysis_by_url.get(job_url)
        cv_payload = None
        if cv_row is not None:
            cv_payload = {
                "version_id": cv_row.get("cv_version_id"),
                "ranking_fit_label": cv_row.get("ranking_fit_label") or cv_row.get("fit_classification"),
                "fit_classification": cv_row.get("fit_classification"),
                "model_used": cv_row.get("cv_generation_model"),
                "runtime_path": (
                    (cv_row.get("runtime_provenance") or {}).get("runtime_path")
                    if isinstance(cv_row.get("runtime_provenance"), dict)
                    else None
                ),
                "provider": (
                    (cv_row.get("runtime_provenance") or {}).get("provider")
                    if isinstance(cv_row.get("runtime_provenance"), dict)
                    else None
                ),
                "prompt_id": cv_row.get("cv_prompt_id"),
                "prompt_template_path": cv_row.get("cv_prompt_template_path"),
                "schema_version": (
                    cv_row.get("structured_cv", {}) or {}
                ).get("schema_version") if isinstance(cv_row.get("structured_cv"), dict) else None,
                "structured": cv_row.get("structured_cv"),
                "markdown": cv_row.get("cv_markdown"),
                "created_at": cv_row.get("generated_at"),
            }
        pipeline_status = _status_for(job_url)
        reject_reasons = reject_reasons_by_url.get(job_url, [])
        rule_filter_marks = rule_filter_marks_by_url.get(job_url, [])
        if deduplicated_job is not None:
            pipeline_status = "deduplicated_before_enrichment"
            reject_reasons = [
                _DEDUPE_REASON_LABELS.get(str(deduplicated_job.get("dedupe_reason") or ""), "deduplicated_before_enrichment")
            ]
            score_source = {}

        raw_shortlist_row = raw_shortlist_by_url.get(job_url)
        scoring_shortlist_row = scoring_shortlist_by_url.get(job_url)
        if job_url in passed_by_url:
            shortlist_status = _shortlist_status_for_export_row(
                job_url=job_url,
                passed_job_urls=passed_job_urls,
                raw_shortlist_row=raw_shortlist_row,
                scoring_shortlist_row=scoring_shortlist_row,
            )
        else:
            shortlist_status = "not_applicable"

        ranking_fit_label = str(score_source.get("fit_label") or "").strip() or None
        ranking_fit_source = str(score_source.get("fit_label_source") or "").strip() or None
        if ranking_fit_label is not None and ranking_fit_source is None:
            ranking_fit_source = "reranker"
        debug_row = debug_by_url.get(job_url)
        if debug_row is not None:
            cv_status = str(debug_row.get("status") or "not_attempted")
        elif job_url in ranked_by_url:
            cv_status = "not_attempted"
        else:
            cv_status = "not_applicable"
        if isinstance(debug_row, dict) and isinstance(debug_row.get("decision_chain"), dict):
            decision_chain = dict(debug_row["decision_chain"])
        else:
            decision_chain = _build_decision_chain(
                shortlist_status=shortlist_status,
                advanced_to_scoring=job_url in scoring_shortlist_by_url,
                ranking_fit_label=ranking_fit_label,
                ranking_fit_source=ranking_fit_source,
                cv_status=cv_status,
            )
        truth_fields = (
            _deterministic_truth_fields(debug_row.get("status"))
            if isinstance(debug_row, dict)
            else _deterministic_truth_fields(analysis_row.get("status"))
            if isinstance(analysis_row, dict)
            else _deterministic_truth_fields(None)
        )

        rows.append(
            {
                "job_url": job_url,
                "job_title": extract_job_title(enriched_job or raw_job or {}),
                "company": (enriched_job or raw_job or {}).get("company_name")
                or (enriched_job or raw_job or {}).get("companyName"),
                "location_type": (enriched_job or {}).get("location_type"),
                "seniority": (enriched_job or {}).get("seniority"),
                "job_family": (enriched_job or {}).get("job_family"),
                "domain": (enriched_job or {}).get("domain"),
                "pipeline_status": pipeline_status,
                **truth_fields,
                "reject_reasons": reject_reasons,
                "rule_filter_marks": rule_filter_marks,
                "scores": {
                    "final_score": score_source.get("final_score"),
                    "ai_score": score_source.get("ai_score"),
                    "vector_score": score_source.get("vector_similarity"),
                    "fit_label": score_source.get("fit_label"),
                    "ai_score_reuse_status": score_source.get("ai_score_reuse_status"),
                    "ai_score_input_fingerprint": score_source.get("ai_score_input_fingerprint"),
                },
                "cv_analysis": (
                    {
                        "status": analysis_row.get("status"),
                        "analysis_reuse_status": analysis_row.get("analysis_reuse_status"),
                        "analysis_input_fingerprint": analysis_row.get("analysis_input_fingerprint"),
                    }
                    if analysis_row is not None
                    else None
                ),
                "decision_chain": decision_chain,
                "rank": score_source.get("final_rank"),
                "cv": (
                    {
                        key: value
                        for key, value in (cv_payload or {}).items()
                        if key not in {"structured", "markdown"}
                    }
                    if cv_payload is not None
                    else None
                ),
                "_input_index": input_index,
            }
        )

    rows.sort(key=_sort_key)
    for row in rows:
        row.pop("_input_index", None)
    return rows


class PipelineCancelled(Exception):
    """Raised when a cooperative cancellation checkpoint is triggered."""


def _validate_pipeline_stage_name(stage_name: str | None) -> str | None:
    if stage_name is None:
        return None
    normalized = str(stage_name).strip()
    if normalized not in _PIPELINE_STAGE_SET:
        raise ValueError(f"Unknown pipeline stage: {stage_name!r}")
    return normalized


def next_pipeline_stage(stage_name: str | None) -> str | None:
    normalized = _validate_pipeline_stage_name(stage_name)
    if normalized is None:
        return PIPELINE_STAGE_SEQUENCE[0]
    stage_index = PIPELINE_STAGE_SEQUENCE.index(normalized)
    if stage_index + 1 >= len(PIPELINE_STAGE_SEQUENCE):
        return None
    return PIPELINE_STAGE_SEQUENCE[stage_index + 1]


def completed_pipeline_stages_through(stage_name: str | None) -> list[str]:
    normalized = _validate_pipeline_stage_name(stage_name)
    if normalized is None:
        return []
    stage_index = PIPELINE_STAGE_SEQUENCE.index(normalized)
    return list(PIPELINE_STAGE_SEQUENCE[: stage_index + 1])

def _normalize_late_stage_reuse_snapshots(reuse_snapshots: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    payload = dict(reuse_snapshots or {})
    ranking_rows: list[dict[str, Any]] = []
    for item in list(payload.get("ranking_ai_scores") or []):
        if not isinstance(item, dict):
            continue
        ai_score_row = item.get("ai_score_row")
        if isinstance(ai_score_row, dict):
            parser_status = str(
                ai_score_row.get("parser_status")
                or ai_score_row.get("reranker_parser_status")
                or ""
            ).strip().lower()
            score_reasoning = str(
                ai_score_row.get("score_reasoning")
                or ai_score_row.get("reranker_score_reasoning")
                or ""
            ).strip().lower()
            # Do not reuse poisoned reranker cache rows produced by parse failures.
            if (
                parser_status in {"malformed_json", "runtime_exception"}
                or "parse failure" in score_reasoning
                or "default credentials were not found" in score_reasoning
                or "application default credentials" in score_reasoning
            ):
                continue
        ranking_rows.append(dict(item))
    return {
        "ranking_ai_scores": ranking_rows,
        "cv_analysis_records": [
            dict(item)
            for item in list(payload.get("cv_analysis_records") or [])
            if isinstance(item, dict)
        ],
    }


def _index_late_stage_reuse_rows(
    rows: list[dict[str, Any]],
    *,
    fingerprint_key: str,
    payload_key: str,
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        fingerprint = str(row.get(fingerprint_key) or "").strip()
        payload = row.get(payload_key)
        if not fingerprint or not isinstance(payload, dict) or fingerprint in indexed:
            continue
        indexed[fingerprint] = deepcopy(payload)
    return indexed

def _index_cv_analysis_reuse_by_url(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        job_url = str(row.get("job_url") or "").strip()
        payload = row.get("analysis_record")
        if not job_url or not isinstance(payload, dict):
            continue
        indexed.setdefault(job_url, []).append(deepcopy(payload))
    return indexed

def _analysis_input_components(payload: dict[str, Any]) -> dict[str, Any]:
    profile_payload = dict(payload.get("profile") or {})
    job_payload = dict(payload.get("job") or {})

    def _hash(value: Any) -> str:
        seed = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(seed.encode("utf-8")).hexdigest()

    return {
        "contract_fingerprint": str(payload.get("contract_fingerprint") or ""),
        "profile_payload_hash": _hash(profile_payload),
        "job_payload_hash": _hash(job_payload),
    }

def _attach_analysis_input_components(
    *,
    analysis_record: dict[str, Any],
    job: dict[str, Any],
    analysis_input_fingerprint: str,
    analysis_input_components: dict[str, Any],
) -> dict[str, Any]:
    hydrated = dict(analysis_record or {})
    hydrated["analysis_input_fingerprint"] = analysis_input_fingerprint
    hydrated["analysis_input_components"] = dict(analysis_input_components or {})
    hydrated["job_url"] = str(
        hydrated.get("job_url")
        or job.get("job_url")
        or ""
    )
    hydrated["job_title"] = str(
        hydrated.get("job_title")
        or extract_job_title(job)
    )
    hydrated["job_snapshot"] = dict(hydrated.get("job_snapshot") or job)
    reuse_status = str(hydrated.get("analysis_reuse_status") or "")
    if not reuse_status:
        hydrated["analysis_reuse_status"] = "fresh_compute"
    if not isinstance(hydrated.get("reuse_decision"), dict):
        hydrated["reuse_decision"] = build_reuse_decision(
            decision="fresh_compute",
            reason_code="no_reusable_snapshot_match",
            fingerprint=analysis_input_fingerprint,
            source_artifact_type="cv_analysis",
        )
    return hydrated

def _cv_analysis_reuse_mismatch_reason(
    *,
    current_record: dict[str, Any],
    prior_records_for_url: list[dict[str, Any]],
) -> str:
    current_components = dict(current_record.get("analysis_input_components") or {})
    if not prior_records_for_url:
        return "no_overlap_prior_url"
    if not current_components:
        return "missing_current_components"
    prior_components = [
        dict(item.get("analysis_input_components") or {})
        for item in prior_records_for_url
        if isinstance(item, dict)
    ]
    if not any(prior_components):
        return "overlap_without_prior_components"
    contract = str(current_components.get("contract_fingerprint") or "")
    if any(str(item.get("contract_fingerprint") or "") != contract for item in prior_components):
        return "contract_fingerprint_changed"
    profile_hash = str(current_components.get("profile_payload_hash") or "")
    if any(str(item.get("profile_payload_hash") or "") != profile_hash for item in prior_components):
        return "profile_payload_changed"
    job_hash = str(current_components.get("job_payload_hash") or "")
    if any(str(item.get("job_payload_hash") or "") != job_hash for item in prior_components):
        return "job_payload_changed"
    return "overlap_exact_match_not_used"


def _build_late_stage_reuse_metrics(
    *,
    enriched: list[dict[str, Any]],
    ai_scores: list[dict[str, Any]],
    cv_analysis_results: list[dict[str, Any]],
    cv_generation_debug_records: list[dict[str, Any]],
) -> dict[str, Any]:
    reused_enrich_rows = sum(
        1 for row in enriched
        if str(row.get("enrich_reuse_status") or "") == REUSED_CACHED_ENRICHMENT_STATUS
    )
    fresh_enrich_rows = sum(
        1 for row in enriched
        if str(row.get("enrich_reuse_status") or "") == FRESH_ENRICHMENT_STATUS
    )
    reused_ai_scores = sum(
        1 for row in ai_scores
        if str(row.get("ai_score_reuse_status") or "") == "reused_exact_match"
    )
    fresh_ai_scores = sum(
        1 for row in ai_scores
        if str(row.get("ai_score_reuse_status") or "") == "fresh_compute"
    )
    executed_analysis_rows = [
        row for row in cv_analysis_results
        if str(row.get("status") or "") != CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS
    ]
    reused_analysis_rows = sum(
        1 for row in executed_analysis_rows
        if str(row.get("analysis_reuse_status") or "") == "reused_exact_match"
    )
    fresh_analysis_rows = sum(
        1 for row in executed_analysis_rows
        if str(row.get("analysis_reuse_status") or "") == "fresh_compute"
    )
    blocked_before_analysis_rows = sum(
        1 for row in cv_analysis_results
        if str(row.get("status") or "") == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS
    )
    generation_attempted_rows = [
        row for row in cv_generation_debug_records
        if str(row.get("status") or "") in {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}
    ]
    reused_cv_generations = sum(
        1 for row in generation_attempted_rows
        if str(row.get("cv_generation_reuse_status") or "") == "reused_exact_match"
    )
    fresh_cv_generations = sum(
        1 for row in generation_attempted_rows
        if str(row.get("cv_generation_reuse_status") or "") == "fresh_compute"
    )
    return {
        "enrich": {
            "reused_rows": reused_enrich_rows,
            "fresh_rows": fresh_enrich_rows,
            "total_rows": len(enriched),
            "reuse_rate": _safe_rate(reused_enrich_rows, len(enriched)),
        },
        "ranking": {
            "reused_ai_scores": reused_ai_scores,
            "fresh_ai_scores": fresh_ai_scores,
            "total_ai_scores": len(ai_scores),
            "reuse_rate": _safe_rate(reused_ai_scores, len(ai_scores)),
        },
        "cv_analysis": {
            "analysis_rows_executed": len(executed_analysis_rows),
            "reused_analysis_rows": reused_analysis_rows,
            "fresh_analysis_rows": fresh_analysis_rows,
            "blocked_before_analysis_rows": blocked_before_analysis_rows,
            "analysis_reuse_rate": _safe_rate(reused_analysis_rows, len(executed_analysis_rows)),
        },
        "cv_generation": {
            "reused_rows": reused_cv_generations,
            "fresh_rows": fresh_cv_generations,
            "total_rows": len(generation_attempted_rows),
            "reuse_rate": _safe_rate(reused_cv_generations, len(generation_attempted_rows)),
        },
    }

def _reuse_anomaly_payload(
    *,
    reuse_metrics: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    guard_cfg = dict((config.get("reuse") or {}).get("anomaly_guard") or {})
    min_overlap = int(guard_cfg.get("min_overlap", 5) or 5)
    floor = float(guard_cfg.get("reuse_rate_floor", 0.05) or 0.05)
    stages: list[dict[str, Any]] = []
    for stage_id, stage_metrics_raw in dict(reuse_metrics or {}).items():
        stage_metrics = dict(stage_metrics_raw or {})
        total = int(
            stage_metrics.get("total_rows")
            or stage_metrics.get("total_ai_scores")
            or stage_metrics.get("analysis_rows_executed")
            or 0
        )
        reused = int(
            stage_metrics.get("reused_rows")
            or stage_metrics.get("reused_ai_scores")
            or stage_metrics.get("reused_analysis_rows")
            or 0
        )
        fresh = int(
            stage_metrics.get("fresh_rows")
            or stage_metrics.get("fresh_ai_scores")
            or stage_metrics.get("fresh_analysis_rows")
            or 0
        )
        rate = float(stage_metrics.get("reuse_rate") or stage_metrics.get("analysis_reuse_rate") or 0.0)
        if total < min_overlap or rate >= floor:
            continue
        stages.append(
            {
                "stage_id": stage_id,
                "total": total,
                "reused": reused,
                "fresh": fresh,
                "reuse_rate": rate,
                "reason_histogram": {
                    "exact_fingerprint_match": reused,
                    "no_reusable_snapshot_match": fresh,
                },
            }
        )
    if not stages:
        return None
    return {
        "status": "breached",
        "min_overlap": min_overlap,
        "reuse_rate_floor": floor,
        "stages": stages,
    }


def _build_late_stage_reuse_snapshots(
    *,
    ai_scores: list[dict[str, Any]],
    cv_analysis_results: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "late_stage_reuse_v1",
        "ranking_ai_scores": [
            {
                "job_url": str(row.get("job_url") or ""),
                "ai_score_input_fingerprint": str(row.get("ai_score_input_fingerprint") or ""),
                "ai_score_row": deepcopy(row),
            }
            for row in ai_scores
            if str(row.get("job_url") or "") and str(row.get("ai_score_input_fingerprint") or "")
        ],
        "cv_analysis_records": [
            {
                "job_url": str(row.get("job_url") or ""),
                "analysis_input_fingerprint": str(row.get("analysis_input_fingerprint") or ""),
                "analysis_record": deepcopy(row),
            }
            for row in cv_analysis_results
            if str(row.get("job_url") or "") and str(row.get("analysis_input_fingerprint") or "")
        ],
    }


def _empty_pipeline_state(run_id: str) -> dict[str, Any]:
    return PipelineState(run_id=run_id).as_state_dict()


def _restore_pipeline_state(
    *,
    run_id: str,
    checkpoint_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    return PipelineState.from_checkpoint_payload(
        run_id=run_id,
        checkpoint_payload=checkpoint_payload,
    ).as_state_dict()


def _checkpoint_payload_from_state(state: dict[str, Any]) -> dict[str, Any]:
    keys = PipelineState.payload_keys()
    payload: dict[str, Any] = {
        "schema_version": int(getattr(PipelineState, "CHECKPOINT_SCHEMA_VERSION", 1)),
    }
    for key in keys:
        if key == "candidate_query_debug":
            payload[key] = json_safe_value(state.get(key) or {})
            continue
        if key == "completed_stage":
            value = str(state.get(key) or state.get("last_completed_stage") or "").strip()
            payload[key] = value or None
            continue
        payload[key] = json_safe_value(state.get(key) or [])
    return payload


def _infer_last_completed_stage_from_state(state: dict[str, Any]) -> str | None:
    return infer_last_completed_stage_from_state(state)


def _canonical_resume_start_stage(
    *,
    requested_start_stage: str | None,
    checkpoint_payload: dict[str, Any] | None,
    run_id: str,
) -> str | None:
    validated_start_stage = _validate_pipeline_stage_name(requested_start_stage)
    if not checkpoint_payload:
        return validated_start_stage

    resume_state = _restore_pipeline_state(run_id=run_id, checkpoint_payload=checkpoint_payload)
    last_completed_stage = _infer_last_completed_stage_from_state(resume_state)
    canonical_next_stage = next_pipeline_stage(last_completed_stage)
    if canonical_next_stage:
        return canonical_next_stage
    return validated_start_stage


def _collect_mapping_suggestions(enriched: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    for job in enriched:
        job_url = extract_job_url(job)
        job_title = extract_job_title(job)
        for suggestion in list(job.get("mapping_suggestions") or []):
            if not isinstance(suggestion, dict):
                continue
            record: dict[str, Any] = {
                "run_id": run_id,
                "job_url": job_url,
                "job_title": job_title,
                "must_have_skill": str(suggestion.get("must_have_skill") or ""),
                "matches": bool(suggestion.get("matches")),
                "confidence": float(suggestion.get("confidence") or 0.0),
                "alias": str(suggestion.get("alias") or ""),
                "canonical": str(suggestion.get("canonical") or ""),
                "field": str(suggestion.get("field") or "skill"),
            }
            if record["alias"] and record["canonical"]:
                dedupe_key = (
                    record["field"].strip().lower(),
                    record["alias"].strip().lower(),
                    record["canonical"].strip().lower(),
                    record["must_have_skill"].strip().lower(),
                )
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                suggestions.append(record)
        for field_name, source_key in (
            ("domain", "domain_mapping_suggestions"),
            ("role_family", "role_family_mapping_suggestions"),
        ):
            for suggestion in list(job.get(source_key) or []):
                if not isinstance(suggestion, dict):
                    continue
                alias = str(suggestion.get("alias") or "").strip()
                canonical = str(suggestion.get("canonical") or "").strip()
                if not alias or not canonical:
                    continue
                record = {
                    "run_id": run_id,
                    "job_url": job_url,
                    "job_title": job_title,
                    "must_have_skill": "",
                    "matches": bool(suggestion.get("matches", True)),
                    "confidence": float(suggestion.get("confidence") or 0.0),
                    "alias": alias,
                    "canonical": canonical,
                    "field": field_name,
                }
                dedupe_key = (
                    field_name,
                    alias.lower(),
                    canonical.lower(),
                    "",
                )
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                suggestions.append(record)
    return suggestions


def _build_stage_progress_summary(
    *,
    run_id: str,
    last_completed_stage: str,
    state: dict[str, Any],
    profile: dict[str, Any] | None,
    config: dict[str, Any],
    vector_top_n: int | None = None,
    candidate_summary: str | None = None,
    candidate_query_components: dict[str, Any] | None = None,
    candidate_query_debug: dict[str, Any] | None = None,
    final_top_n: int | None = None,
) -> dict[str, Any]:
    raw_jobs = list(state.get("raw_jobs") or [])
    normalized = list(state.get("normalized") or [])
    deduplicated_jobs = list(state.get("deduplicated_jobs") or [])
    pre_filter_rejected_jobs = list(state.get("pre_filter_rejected_jobs") or [])
    enriched = list(state.get("enriched") or [])
    passed_jobs = list(state.get("passed_jobs") or [])
    candidate_filter_rejected_jobs = list(state.get("candidate_filter_rejected_jobs") or [])
    raw_shortlist = list(state.get("raw_shortlist") or [])
    shortlist = list(state.get("shortlist") or [])
    backfilled_job_urls = list(state.get("backfilled_job_urls") or [])
    ai_scores = list(state.get("ai_scores") or [])
    ranking_inputs = list(state.get("ranking_inputs") or [])
    ranked = list(state.get("ranked") or [])
    cv_analysis_results = list(state.get("cv_analysis_results") or [])
    cv_generation_debug_records = list(state.get("cv_generation_debug_records") or [])
    cv_results = list(state.get("cv_results") or [])
    candidate_profile = profile or {"preferences": {}}
    vector_top_n_value = int(
        vector_top_n if vector_top_n is not None else pipeline_int(config, "vector_search_top_n", default=0)
    )
    final_top_n_value = int(
        final_top_n if final_top_n is not None else pipeline_int(config, "final_top_n", default=0)
    )
    candidate_summary_value = str(candidate_summary or "")
    candidate_query_components_value = dict(candidate_query_components or {})
    candidate_query_debug_value = dict(candidate_query_debug or state.get("candidate_query_debug") or {})
    stage_transition_artifacts = _build_stage_transition_artifacts(
        raw_jobs=raw_jobs,
        normalized=normalized,
        deduplicated_jobs=deduplicated_jobs,
        pre_filter_rejected_jobs=pre_filter_rejected_jobs,
        enriched=enriched,
        passed_jobs=passed_jobs,
        candidate_filter_rejected_jobs=candidate_filter_rejected_jobs,
        raw_shortlist=raw_shortlist,
        shortlist=shortlist,
        backfilled_job_urls=backfilled_job_urls,
        vector_top_n=vector_top_n_value,
        candidate_summary=candidate_summary_value,
        candidate_query_components=candidate_query_components_value,
        candidate_query_debug=candidate_query_debug_value,
        ai_scores=ai_scores,
        ranking_inputs=ranking_inputs,
        ranked=ranked,
        cv_analysis_results=cv_analysis_results,
        final_top_n=final_top_n_value,
        cv_generation_debug_records=cv_generation_debug_records,
        profile=candidate_profile,
        config=config,
    )
    return {
        "run_id": run_id,
        "last_completed_stage": last_completed_stage,
        "completed_stages": completed_pipeline_stages_through(last_completed_stage),
        "next_stage": next_pipeline_stage(last_completed_stage),
        "total_jobs": len(raw_jobs),
        "passed_filter": len(passed_jobs),
        "ranked": len(ranked),
        "cvs_generated": len(cv_results),
        "mapping_suggestions": _collect_mapping_suggestions(enriched, run_id),
        "stage_transition_artifacts": stage_transition_artifacts,
    }


def _build_checkpoint_summary(
    *,
    run_id: str,
    paused_after_stage: str,
    state: dict[str, Any],
    profile: dict[str, Any] | None,
    config: dict[str, Any],
    vector_top_n: int | None = None,
    candidate_summary: str | None = None,
    candidate_query_components: dict[str, Any] | None = None,
    candidate_query_debug: dict[str, Any] | None = None,
    final_top_n: int | None = None,
) -> dict[str, Any]:
    summary = _build_stage_progress_summary(
        run_id=run_id,
        last_completed_stage=paused_after_stage,
        state=state,
        profile=profile,
        config=config,
        vector_top_n=vector_top_n,
        candidate_summary=candidate_summary,
        candidate_query_components=candidate_query_components,
        candidate_query_debug=candidate_query_debug,
        final_top_n=final_top_n,
    )
    cv_analysis_results = list(state.get("cv_analysis_results") or [])
    cv_generation_debug_records = list(state.get("cv_generation_debug_records") or [])
    ranked = list(state.get("ranked") or [])
    cv_analysis_reached = len(ranked) > 0 or len(cv_analysis_results) > 0
    cv_generation_reached = any(
        str(record.get("status") or "") in {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}
        for record in cv_generation_debug_records
    )
    late_stage_mode_payload = _build_late_stage_mode_payload(
        agentic_late_stage_enabled=_agentic_late_stage_enabled(config),
        stage_reached=cv_analysis_reached or cv_generation_reached,
    )
    summary["cv_analysis_trace"] = _build_cv_analysis_trace_summary(
        run_id=run_id,
        cv_analysis_results=cv_analysis_results,
        late_stage_mode=late_stage_mode_payload,
    )
    summary["agentic_live_trace"] = _build_agentic_live_trace_summary(
        run_id=run_id,
        cv_generation_debug_records=cv_generation_debug_records,
        late_stage_mode=late_stage_mode_payload,
    )
    summary["paused_after_stage"] = paused_after_stage
    checkpoint_payload = _checkpoint_payload_from_state(state)
    checkpoint_payload["completed_stage"] = paused_after_stage
    summary["checkpoint_payload"] = checkpoint_payload
    return summary


def _handle_stage_boundary(
    *,
    run_id: str,
    last_completed_stage: str,
    stop_after_stage: str | None,
    state: dict[str, Any],
    profile: dict[str, Any] | None,
    config: dict[str, Any],
    vector_top_n: int,
    candidate_summary: str,
    candidate_query_components: dict[str, Any],
    candidate_query_debug: dict[str, Any],
    final_top_n: int,
    stage_progress_callback: Callable[[dict[str, Any]], None] | None,
) -> dict[str, Any] | None:
    if stage_progress_callback is not None:
        stage_progress_callback(
            _build_stage_progress_summary(
                run_id=run_id,
                last_completed_stage=last_completed_stage,
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
            )
        )
    if stop_after_stage == last_completed_stage:
        return _build_checkpoint_summary(
            run_id=run_id,
            paused_after_stage=last_completed_stage,
            state=state,
            profile=profile,
            config=config,
            vector_top_n=vector_top_n,
            candidate_summary=candidate_summary,
            candidate_query_components=candidate_query_components,
            candidate_query_debug=candidate_query_debug,
            final_top_n=final_top_n,
        )
    return None


# ── helpers ───────────────────────────────────────────────────────────────────

def create_run_id() -> str:
    """Return a new UUID4 string to identify this pipeline run."""
    return str(uuid.uuid4())


def _should_retry_missing_sections(validation: dict[str, Any]) -> bool:
    missing_sections = list(validation.get("missing_sections") or [])
    if not missing_sections:
        return False
    return all(not validation.get(field) for field in _REPAIRABLE_VALIDATION_FIELDS)


def _normalize_candidate_name_token(value: str) -> str:
    normalized = str(value or "")
    normalized = normalized.replace("[", " ").replace("]", " ")
    normalized = " ".join(normalized.split()).strip().lower()
    return normalized


def _is_candidate_name_placeholder(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return _normalize_candidate_name_token(value) in _CANDIDATE_NAME_PLACEHOLDER_VALUES


def _resolved_candidate_profile_name(profile: dict[str, Any] | None) -> str:
    if not isinstance(profile, dict):
        return ""
    candidate_name = str(profile.get("name") or "").strip()
    if not candidate_name or _is_candidate_name_placeholder(candidate_name):
        return ""
    return candidate_name


def _is_candidate_name_placeholder_validation(validation: dict[str, Any]) -> bool:
    grounding_violations = list(validation.get("grounding_violations") or [])
    if not grounding_violations:
        return False
    return all("candidate-name placeholder" in str(item).lower() for item in grounding_violations)


def _should_repair_candidate_name_placeholder(
    validation: dict[str, Any],
    structured_cv: dict[str, Any] | None,
    profile: dict[str, Any] | None,
) -> bool:
    if validation.get("valid"):
        return False
    if not isinstance(structured_cv, dict):
        return False
    if not _resolved_candidate_profile_name(profile):
        return False
    if list(validation.get("missing_sections") or []):
        return False
    if list(validation.get("skill_violations") or []):
        return False
    if list(validation.get("deterministic_grounding_violations") or []):
        return False
    if list(validation.get("semantic_grounding_violations") or []):
        return False
    if not _is_candidate_name_placeholder_validation(validation):
        return False
    sections = structured_cv.get("sections")
    if not isinstance(sections, dict):
        return False
    header = sections.get("header")
    if not isinstance(header, dict):
        return False
    return _is_candidate_name_placeholder(header.get("name"))


def _unwrap_generated_cv(generated_cv: Any) -> tuple[dict[str, Any] | None, str]:
    if isinstance(generated_cv, dict):
        markdown = str(generated_cv.get("markdown") or "")
        structured_cv = generated_cv.get("structured_cv")
        if isinstance(structured_cv, dict):
            return structured_cv, markdown
        return None, markdown
    return None, str(generated_cv)


def _build_debug_evidence_used(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    debug_evidence: list[dict[str, Any]] = []
    for item in evidence:
        debug_item: dict[str, Any] = {
            "evidence_type": str(item.get("evidence_type") or ""),
            "source_ref": str(item.get("source_ref") or ""),
            "name": str(item.get("name") or ""),
            "matched_channels": list(item.get("matched_channels") or []),
            "selection_reasons": list(item.get("selection_reasons") or []),
        }
        channel_subscores = dict(item.get("channel_subscores") or {})
        semantic_alignment = dict(item.get("semantic_alignment") or {})
        if channel_subscores:
            debug_item["channel_subscores"] = channel_subscores
        if semantic_alignment:
            debug_item["semantic_alignment"] = semantic_alignment
        debug_evidence.append(
            {key: value for key, value in debug_item.items() if value not in ("", None)}
        )
    return debug_evidence


def _build_cv_generation_analysis_input_summary(job: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "required_skills": list(job.get("required_skills") or []),
        "preferred_skills": list(job.get("preferred_skills") or []),
        "responsibilities": list(job.get("responsibilities") or []),
        "job_family": str(job.get("job_family") or ""),
        "domain": str(job.get("domain") or ""),
        "location_type": str(job.get("location_type") or ""),
    }
    return {
        key: value
        for key, value in summary.items()
        if value not in (None, "", [])
    }


def _cv_generation_enabled_sections(config: dict[str, Any]) -> list[str]:
    composition = (config.get("cv") or {}).get("composition") or {}
    enabled_sections: list[str] = []
    for section_key, section_cfg in composition.items():
        if isinstance(section_cfg, dict) and section_cfg.get("enabled", True):
            enabled_sections.append(CV_SECTION_KEY_TO_NAME.get(section_key, section_key.title()))
    return enabled_sections


def _agentic_late_stage_enabled(config: dict[str, Any]) -> bool:
    cv_block = config.get("cv") or {}
    late_stage_block = cv_block.get("agentic_late_stage") or {}
    if isinstance(late_stage_block, dict):
        return True
    return False


def _cv_analysis_stage_concurrency(config: dict[str, Any]) -> int:
    return get_stage_runtime_concurrency(config, stage="cv_analysis", default=1)

def _enrich_stage_concurrency(config: dict[str, Any]) -> int:
    return get_stage_runtime_concurrency(
        config,
        stage="enrich",
        default=1,
        compatibility_fallback_key="enrichment_concurrency",
    )

def _ranking_stage_concurrency(config: dict[str, Any]) -> int:
    return get_stage_runtime_concurrency(config, stage="ranking", default=1)


def _build_late_stage_mode_payload(
    *,
    agentic_late_stage_enabled: bool,
    stage_reached: bool,
) -> dict[str, Any]:
    del agentic_late_stage_enabled
    return {
        "late_stage_mode": "agentic",
        "agentic_late_stage_enabled": True,
        "mode_source": "cv.agentic_late_stage.unified_runtime",
        "agentic_status": (
            "completed" if stage_reached else "pending"
        ),
    }


def _build_validation_grounding_payload(
    *,
    evidence_payload: list[dict[str, Any]],
    evidence_used: list[dict[str, Any]],
    evidence_selection_summary: dict[str, Any] | None,
    analysis_input_summary: dict[str, Any] | None,
) -> AnalysisGroundingPayload:
    return {
        "evidence_payload": list(evidence_payload),
        "evidence_used": list(evidence_used),
        "evidence_selection_summary": dict(evidence_selection_summary or {}),
        "analysis_input_summary": dict(analysis_input_summary or {}),
    }


def _build_validation_snapshot(validation: dict[str, Any] | None) -> dict[str, Any] | None:
    if validation is None:
        return None
    return {
        "valid": bool(validation.get("valid")),
        "missing_sections": list(validation.get("missing_sections") or []),
        "grounding_violations": list(validation.get("grounding_violations") or []),
        "deterministic_grounding_violations": list(validation.get("deterministic_grounding_violations") or []),
        "semantic_grounding_violations": list(validation.get("semantic_grounding_violations") or []),
        "skill_violations": list(validation.get("skill_violations") or []),
        "warnings": list(validation.get("warnings") or []),
        "support_source_summary": dict(validation.get("support_source_summary") or {}),
    }


def _build_repair_attempt(missing_sections: list[str] | None = None) -> dict[str, Any]:
    return {
        "performed": bool(missing_sections),
        "missing_sections": list(missing_sections or []),
    }


def _build_candidate_name_repair_attempt() -> dict[str, Any]:
    return {
        "performed": True,
        "missing_sections": [],
        "reason": "candidate_name_placeholder",
    }


def _repair_candidate_name_placeholder(
    structured_cv: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """@capability cv_system.header-placeholder-repair"""
    repaired_structured_cv = deepcopy(structured_cv)
    sections = repaired_structured_cv.setdefault("sections", {})
    header = sections.setdefault("header", {})
    header["name"] = _resolved_candidate_profile_name(profile)
    repaired_markdown = render_cv_markdown(repaired_structured_cv, config)
    return repaired_structured_cv, repaired_markdown


def _resolve_layer4_fit(
    job: dict[str, Any],
    gap_fit: str | None,
    config: dict[str, Any],
) -> str:
    """Return the authoritative post-filter fit label for a ranked job."""
    del gap_fit
    ranked_fit_raw = str(job.get("fit_label") or "").strip().lower()
    ranked_fit = ranked_fit_raw if ranked_fit_raw in _FIT_LABEL_ORDER else None
    if ranked_fit is not None:
        return ranked_fit
    raw_ai_score = job.get("ai_score")
    if raw_ai_score is None:
        return "skip"
    return fit_label_from_score(float(raw_ai_score), config)


def _shortlist_status_for_export_row(
    *,
    job_url: str,
    passed_job_urls: set[str],
    raw_shortlist_row: dict[str, Any] | None,
    scoring_shortlist_row: dict[str, Any] | None,
) -> str:
    if job_url not in passed_job_urls:
        return "not_applicable"
    if raw_shortlist_row is not None:
        return "returned_by_vector_search"
    if scoring_shortlist_row is not None and str(scoring_shortlist_row.get("shortlist_origin") or "") == "backfill":
        return "backfilled_for_scoring"
    if scoring_shortlist_row is not None:
        return "advanced_to_scoring"
    return "not_returned_in_raw_hits"


def _shortlist_status_for_ranked_job(job: dict[str, Any]) -> str:
    return _shared_shortlist_status_for_ranked_job(job)


def _validation_status_for_cv_status(status: str) -> str:
    return _shared_validation_status_for_cv_status(status)


def _deterministic_truth_fields(status: str | None) -> dict[str, str | None]:
    return _shared_deterministic_truth_fields(status)

_bounded_event_payload = _build_bounded_event_payload_observability


def _extract_generation_trace_metrics(agentic_live_trace: dict[str, Any] | None) -> dict[str, Any]:
    trace = dict(agentic_live_trace or {})
    attempts = [
        dict(item)
        for item in list(trace.get("attempts") or [])
        if isinstance(item, dict)
    ]
    latencies = [
        int(item.get("latency_ms") or 0)
        for item in attempts
        if int(item.get("latency_ms") or 0) > 0
    ]
    total_latency_ms = int(sum(latencies)) if latencies else None
    retry_count = max(0, len(attempts) - 1)
    final_attempt = attempts[-1] if attempts else {}

    usage_block = None
    for key in ("usage", "token_usage", "response_usage"):
        value = final_attempt.get(key)
        if isinstance(value, dict):
            usage_block = dict(value)
            break
    cost_block = None
    if isinstance(final_attempt.get("cost"), dict):
        cost_block = dict(final_attempt["cost"])
    elif final_attempt.get("cost_usd") is not None:
        cost_block = {"usd": final_attempt.get("cost_usd")}

    return {
        "latency_ms": total_latency_ms,
        "attempt_count": len(attempts),
        "retry_count": retry_count,
        "usage": usage_block,
        "cost": cost_block,
    }


def _build_analysis_evidence_selection_summary(
    evidence_bundle: dict[str, Any],
    evidence: list[dict[str, Any]],
    *,
    fallback_used: bool,
) -> dict[str, Any]:
    payload = {
        "channel_counts": normalize_analysis_channel_mapping(
            evidence_bundle.get("channel_counts") or {}
        ),
        "fallback_used": fallback_used,
        "effective_channel_pool_size": int(evidence_bundle.get("effective_channel_pool_size") or 0),
        "merged_pool_size": int(evidence_bundle.get("merged_pool_size") or 0),
        "deduped_pool_size": int(evidence_bundle.get("deduped_pool_size") or 0),
        "selected_evidence_count": len(evidence),
        "selected_evidence_ids": list(evidence_bundle.get("selected_evidence_ids") or []),
        "unselected_top_candidates": list(evidence_bundle.get("unselected_top_candidates") or []),
        "hybrid_alignment": normalize_analysis_channel_mapping(
            evidence_bundle.get("hybrid_alignment") or {}
        ),
        "semantic_alignment": dict(evidence_bundle.get("semantic_alignment") or {}),
    }
    return {key: value for key, value in payload.items() if value not in ({}, [], None)}


def _render_cv_analysis_item_input(*, profile: dict[str, Any], job: dict[str, Any]) -> str:
    job_title = extract_job_title(job) or "Unknown job"
    required_skills = bound_langfuse_list(
        [str(item).strip() for item in list(job.get("required_skills") or []) if str(item).strip()],
        max_items=8,
        max_item_chars=300,
    )
    candidate_skills = bound_langfuse_list(
        flatten_skills(profile),
        max_items=20,
        max_item_chars=80,
    )
    experience_highlights = bound_langfuse_list(
        list(profile.get("experience_highlights") or []),
        max_items=8,
        max_item_chars=300,
    )
    sections = [
        (
            "## Job",
            [f"Title: {job_title}"],
        ),
    ]
    sections.append(("### Job Excerpt", [bound_langfuse_excerpt(str(job.get("description") or ""), max_chars=1500) or ""]))
    sections.append(("### Requirements Excerpt", [f"- {item}" for item in required_skills]))
    sections.append(("## Candidate", [f"Headline: {bound_langfuse_excerpt(str(profile.get('headline') or ''), max_chars=240) or 'Unknown candidate'}"]))
    sections.append(("### Skills", [f"- {item}" for item in candidate_skills]))
    sections.append(("### Experience Highlights", [f"- {item}" for item in experience_highlights]))
    sections.append(
        (
            "## Instructions",
            [
                bound_langfuse_excerpt(
                    str(job.get("analysis_instructions") or "Evaluate fit for generation readiness."),
                    max_chars=2000,
                )
                or "Evaluate fit for generation readiness."
            ],
        )
    )
    sections.append(("## Rubric", ["- domain", "- seniority", "- stack", "- scope"]))
    return render_langfuse_markdown_sections(sections)


def _render_cv_analysis_item_output(analysis_record: dict[str, Any]) -> str:
    return _render_cv_analysis_item_output_observability(
        analysis_record,
        bound_langfuse_list=bound_langfuse_list,
        bound_langfuse_excerpt=bound_langfuse_excerpt,
        render_langfuse_markdown_sections=render_langfuse_markdown_sections,
    )


def _build_cv_analysis_item_observation_attributes(
    *,
    run_id: str,
    profile: dict[str, Any],
    job: dict[str, Any],
    analysis_record: dict[str, Any],
) -> dict[str, Any]:
    return _build_cv_analysis_item_observation_attributes_observability(
        run_id=run_id,
        profile=profile,
        job=job,
        analysis_record=analysis_record,
        extract_job_url=extract_job_url,
        extract_job_title=extract_job_title,
        flatten_skills=flatten_skills,
        bound_langfuse_list=bound_langfuse_list,
        bound_langfuse_excerpt=bound_langfuse_excerpt,
        build_langfuse_item_observation_attributes=build_langfuse_item_observation_attributes,
        render_cv_analysis_item_input=_render_cv_analysis_item_input,
        render_cv_analysis_item_output=_render_cv_analysis_item_output,
    )


def _emit_cv_analysis_item_observation(
    *,
    run_id: str,
    profile: dict[str, Any],
    job: dict[str, Any],
    analysis_record: dict[str, Any],
) -> None:
    attributes = _build_cv_analysis_item_observation_attributes(
        run_id=run_id,
        profile=profile,
        job=job,
        analysis_record=analysis_record,
    )
    with observe_span("pipeline.cv_analysis_item", attributes=attributes):
        return


def _render_cv_generation_item_input(
    *,
    job: dict[str, Any],
    evidence_used: list[dict[str, Any]],
    fit_classification: str | None,
) -> str:
    required_skills = bound_langfuse_list(
        [str(item).strip() for item in list(job.get("required_skills") or []) if str(item).strip()],
        max_items=8,
        max_item_chars=300,
    )
    evidence_lines = bound_langfuse_list(
        [
            str(item.get("name") or item.get("source_ref") or item.get("evidence_type") or "evidence").strip()
            for item in evidence_used
            if str(item.get("name") or item.get("source_ref") or item.get("evidence_type") or "").strip()
        ],
        max_items=8,
        max_item_chars=240,
    )
    sections = [
        ("## Job", [f"Title: {extract_job_title(job) or 'Unknown job'}"]),
        ("### Job Excerpt", [bound_langfuse_excerpt(str(job.get("description") or ""), max_chars=1500) or ""]),
        ("### Constraints", [f"- {item}" for item in required_skills]),
        ("## Analysis Inputs", [f"Fit Classification: {fit_classification or 'unknown'}"]),
        ("## Selected Evidence", [f"- {item}" for item in evidence_lines] or ["- No evidence selected"]),
        (
            "## Generation Instructions",
            [
                bound_langfuse_excerpt(
                    str(job.get("generation_instructions") or "Generate grounded CV sections only from selected evidence."),
                    max_chars=2000,
                )
                or "Generate grounded CV sections only from selected evidence."
            ],
        ),
    ]
    return render_langfuse_markdown_sections(sections)


def _render_cv_generation_item_output(debug_record: dict[str, Any]) -> str:
    return _render_cv_generation_item_output_observability(
        debug_record,
        cv_generation_review_required_status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
        bound_langfuse_markdown=bound_langfuse_markdown,
        bound_langfuse_excerpt=bound_langfuse_excerpt,
        bound_langfuse_issue_list=bound_langfuse_issue_list,
        render_langfuse_markdown_sections=render_langfuse_markdown_sections,
    )


def _build_cv_generation_item_observation_attributes(
    *,
    run_id: str,
    analysis_record: dict[str, Any],
    debug_record: dict[str, Any],
) -> dict[str, Any]:
    return _build_cv_generation_item_observation_attributes_observability(
        run_id=run_id,
        analysis_record=analysis_record,
        debug_record=debug_record,
        cv_generation_review_required_status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
        extract_job_url=extract_job_url,
        extract_job_title=extract_job_title,
        bound_langfuse_list=bound_langfuse_list,
        bound_langfuse_excerpt=bound_langfuse_excerpt,
        bound_langfuse_issue_list=bound_langfuse_issue_list,
        bound_langfuse_markdown=bound_langfuse_markdown,
        build_langfuse_item_observation_attributes=build_langfuse_item_observation_attributes,
        render_cv_generation_item_input=_render_cv_generation_item_input,
        render_cv_generation_item_output=_render_cv_generation_item_output,
    )


def _emit_cv_generation_item_observation(
    *,
    run_id: str,
    analysis_record: dict[str, Any],
    debug_record: dict[str, Any],
) -> None:
    attributes = _build_cv_generation_item_observation_attributes(
        run_id=run_id,
        analysis_record=analysis_record,
        debug_record=debug_record,
    )
    with observe_span("pipeline.cv_generation_item", attributes=attributes):
        return


def _authoritative_ranking_fit_label(
    job: dict[str, Any],
    fit_classification: str | None,
) -> str | None:
    ranked_fit_raw = str(job.get("fit_label") or "").strip().lower()
    if ranked_fit_raw in _FIT_LABEL_ORDER:
        return ranked_fit_raw
    fallback_fit_raw = str(fit_classification or "").strip().lower()
    return fallback_fit_raw or None


def _cv_generation_status_for_analysis_status(status: str) -> str:
    return _shared_cv_generation_status_for_analysis_status(status)


def _build_decision_chain(
    *,
    shortlist_status: str,
    advanced_to_scoring: bool,
    ranking_fit_label: str | None,
    ranking_fit_source: str | None,
    cv_analysis_status: str = "not_run",
    cv_status: str,
) -> dict[str, Any]:
    return {
        "shortlist": {
            "status": shortlist_status,
            "advanced_to_scoring": advanced_to_scoring,
        },
        "primary_fit": {
            "source": ranking_fit_source,
            "label": ranking_fit_label,
        },
        "cv_analysis": {
            "status": cv_analysis_status,
            "completed": cv_analysis_status not in {"not_run", "failed", CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS},
        },
        "cv_generation": {
            "status": cv_status,
            "attempted": cv_status not in {"not_applicable", "not_attempted", "skipped_fit_gate"},
        },
        "validation": {
            "status": _validation_status_for_cv_status(cv_status),
        },
    }


def _build_cv_generation_debug_record(
    *,
    job: dict[str, Any],
    status: str,
    fit_classification: str | None,
    evidence_used: list[dict[str, Any]],
    evidence_selection_summary: dict[str, Any] | None,
    analysis_input_summary: dict[str, Any] | None,
    gap_summary: dict[str, Any] | None,
    structured_cv_initial: dict[str, Any] | None,
    validation_initial: dict[str, Any] | None,
    repair_attempt: dict[str, Any],
    structured_cv_final: dict[str, Any] | None,
    markdown_final: str | None,
    enabled_sections: list[str] | None,
    cv_generation_model: str | None,
    runtime_provenance: dict[str, Any] | None = None,
    cv_prompt_id: str | None,
    cv_prompt_template_path: str | None,
    error: dict[str, str] | None,
    agentic_live_trace: dict[str, Any] | None = None,
    validation_final: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_validation = dict(validation_final or validation_initial or {})
    if "valid" not in resolved_validation and validation_initial is not None:
        resolved_validation["valid"] = bool(validation_initial.get("valid"))
    resolved_validation.setdefault("missing_sections", list(resolved_validation.get("missing_sections") or []))
    resolved_validation.setdefault("grounding_violations", list(resolved_validation.get("grounding_violations") or []))
    resolved_validation.setdefault(
        "deterministic_grounding_violations",
        list(resolved_validation.get("deterministic_grounding_violations") or []),
    )
    resolved_validation.setdefault(
        "semantic_grounding_violations",
        list(resolved_validation.get("semantic_grounding_violations") or []),
    )
    resolved_validation.setdefault("skill_violations", list(resolved_validation.get("skill_violations") or []))
    resolved_validation.setdefault("warnings", list(resolved_validation.get("warnings") or []))
    ranking_fit_label = _authoritative_ranking_fit_label(job, fit_classification)
    ranking_fit_source = str(job.get("fit_label_source") or "reranker").strip() or None
    cv_analysis_status = status
    cv_generation_status = status
    if status in {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}:
        cv_analysis_status = CV_ANALYSIS_READY_FOR_GENERATION_STATUS
    elif status in {
        CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS,
        CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS,
        CV_ANALYSIS_FAILED_STATUS,
    }:
        cv_generation_status = "not_attempted"
    default_cv_generation_reuse_status: str | None = None
    if status in {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}:
        # Any attempted non-reused path should count as fresh unless a later branch
        # explicitly marks this record as reused_exact_match.
        default_cv_generation_reuse_status = "fresh_compute"
    decision_chain = _build_decision_chain(
        shortlist_status=_shortlist_status_for_ranked_job(job),
        advanced_to_scoring=True,
        ranking_fit_label=ranking_fit_label,
        ranking_fit_source=ranking_fit_source,
        cv_analysis_status=cv_analysis_status,
        cv_status=cv_generation_status,
    )
    payload = {
        "job_url": str(job.get("job_url") or ""),
        "job_title": extract_job_title(job),
        "status": status,
        "ranking_fit_label": ranking_fit_label,
        # Backward-compatible alias for downstream consumers still reading reranker_fit_label.
        "reranker_fit_label": ranking_fit_label,
        "fit_classification": fit_classification,
        "decision_chain": decision_chain,
        "analysis_input_summary": dict(analysis_input_summary or {}),
        "evidence_used": evidence_used,
        "evidence_selection_summary": dict(evidence_selection_summary or {}),
        "gap_summary": gap_summary,
        "structured_cv_initial": structured_cv_initial,
        "validation_initial": validation_initial,
        "validation_final": validation_final,
        "repair_attempt": repair_attempt,
        "structured_cv_final": structured_cv_final,
        "markdown_final": markdown_final,
        "enabled_sections": list(enabled_sections or []),
        "cv_generation_model": cv_generation_model,
        "cv_prompt_id": cv_prompt_id,
        "cv_prompt_template_path": cv_prompt_template_path,
        "cv_generation_reuse_status": default_cv_generation_reuse_status,
        "cv_generation_input_fingerprint": None,
        # Reranker blocks and fit-gate skips are expected outcomes, not generation runtime errors.
        "outcome_reason": error if status in {"skipped_fit_gate", CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS} else None,
        "error": error if status not in {"skipped_fit_gate", CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS} else None,
        "review_required_reason_code": _review_required_reason_code_value(
            _normalize_review_required_reason_code(
                status=status,
                error=error,
                validation_initial=resolved_validation,
            )
        ),
        "validation_evidence_fingerprint": _build_validation_evidence_fingerprint(
            status=status,
            validation_initial=resolved_validation,
            error=error,
        ),
        "attempt_count": 1,
        "failed_rule_ids": _extract_failed_rule_ids(resolved_validation),
        "first_failing_section_key": _first_failing_section_key(resolved_validation),
        "operator_note": _build_operator_note(status=status, error=error, validation_initial=resolved_validation),
    }
    if isinstance(runtime_provenance, dict):
        payload["runtime_provenance"] = dict(runtime_provenance)
    if isinstance(agentic_live_trace, dict):
        payload["agentic_live_trace"] = dict(agentic_live_trace)
    return payload


def _resolved_cv_generation_model(
    default_model: str | None,
    runtime_provenance: dict[str, Any] | None,
) -> str | None:
    if isinstance(runtime_provenance, dict):
        runtime_model = str(runtime_provenance.get("model") or "").strip()
        if runtime_model:
            return runtime_model
    return default_model


def _default_cv_generation_runtime_provenance(
    cv_generation_model: str | None,
) -> dict[str, Any]:
    return {
        "runtime_path": "fitcv_cv_generation_builtin",
        "provider": "fitcv_builtin",
        "model": str(cv_generation_model or "").strip() or None,
    }


def _non_agentic_cv_generation_runtime_provenance(
    config: dict[str, Any],
    cv_generation_model: str | None,
) -> dict[str, Any]:
    return resolve_cv_generation_runtime_provenance(
        config,
        default_model=str(cv_generation_model or "").strip() or None,
    )


def _normalize_review_required_reason_code(
    *,
    status: str,
    error: dict[str, str] | None,
    validation_initial: dict[str, Any] | None = None,
) -> ReviewRequiredReasonCode | None:
    if status == "persistence_failed":
        return ReviewRequiredReasonCode.PERSISTENCE_FAILED
    if status == "validation_failed":
        return ReviewRequiredReasonCode.POST_VALIDATION_FAILED
    if status != CV_GENERATION_REVIEW_REQUIRED_STATUS:
        return None
    stage = str((error or {}).get("stage") or "").strip().lower()
    message = str((error or {}).get("message") or "").strip().lower()
    if stage in {"provider", "provider_error", "generation"}:
        return ReviewRequiredReasonCode.PROVIDER_ERROR
    if "timeout" in stage or "timeout" in message:
        return ReviewRequiredReasonCode.TIMEOUT
    if stage == "markdown_quality_review":
        return ReviewRequiredReasonCode.MARKDOWN_STRUCTURE_VIOLATION
    if stage == "policy_acceptance":
        if "ratio" in message:
            return ReviewRequiredReasonCode.POLICY_REQUIRED_RATIO_FAIL
        if "missing" in message:
            return ReviewRequiredReasonCode.POLICY_MISSING_REQUIRED_FAIL
        return ReviewRequiredReasonCode.POLICY_ACCEPTANCE_FAIL
    if stage == "validation":
        return ReviewRequiredReasonCode.POST_VALIDATION_FAILED
    if stage == "review_gate":
        if "unsupported requirements require review" in message:
            return ReviewRequiredReasonCode.UNSUPPORTED_REQUIREMENT_GAP
        if "low confidence sections" in message:
            return ReviewRequiredReasonCode.LOW_CONFIDENCE_SECTIONS
        if "markdown quality" in message:
            return ReviewRequiredReasonCode.QUALITY_GATE_FAILED
        failed_rule_ids = _extract_failed_rule_ids(validation_initial)
        if failed_rule_ids:
            return ReviewRequiredReasonCode.VALIDATION_GUARDRAIL_FAILED
        if "validation failed" in message or "guardrail" in message:
            return ReviewRequiredReasonCode.VALIDATION_GUARDRAIL_FAILED
        return ReviewRequiredReasonCode.REVIEW_GATE_MANUAL_REQUIRED
    if "template" in stage or "template" in message:
        return ReviewRequiredReasonCode.TEMPLATE_CONTRACT_VIOLATION
    if "empty" in message:
        return ReviewRequiredReasonCode.EMPTY_OUTPUT
    return ReviewRequiredReasonCode.MANUAL_REVIEW_OTHER


def _review_required_reason_code_value(code: ReviewRequiredReasonCode | None) -> str | None:
    if code is None:
        return None
    return code.value


def _build_validation_evidence_fingerprint(
    *,
    status: str,
    validation_initial: dict[str, Any] | None,
    error: dict[str, str] | None,
) -> str:
    validation_snapshot = dict(validation_initial or {})
    evidence_payload = {
        "schema_version": "validation_evidence_fingerprint_v1",
        "status": str(status or ""),
        "missing_sections": list(validation_snapshot.get("missing_sections") or []),
        "failed_rule_ids": _extract_failed_rule_ids(validation_snapshot),
        "first_failing_section_key": _first_failing_section_key(validation_snapshot),
        "markdown_quality_blocking_issues": list(validation_snapshot.get("markdown_quality_blocking_issues") or []),
        "markdown_quality_review_flags": list(validation_snapshot.get("markdown_quality_review_flags") or []),
        "error_stage": str((error or {}).get("stage") or ""),
        "error_message": str((error or {}).get("message") or ""),
    }
    seed = json.dumps(evidence_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()

def _extract_failed_rule_ids(validation: dict[str, Any] | None) -> list[str]:
    if not isinstance(validation, dict):
        return []
    rule_ids: list[str] = []
    keys = (
        "grounding_violations",
        "deterministic_grounding_violations",
        "semantic_grounding_violations",
        "skill_violations",
        "markdown_quality_blocking_issues",
    )
    for key in keys:
        for item in list(validation.get(key) or []):
            if isinstance(item, dict):
                rule_id = str(item.get("rule_id") or item.get("code") or "").strip()
                if rule_id:
                    rule_ids.append(rule_id)
            elif isinstance(item, str) and item.strip():
                rule_ids.append(item.strip())
    return sorted(set(rule_ids))

def _first_failing_section_key(validation: dict[str, Any] | None) -> str | None:
    if not isinstance(validation, dict):
        return None
    missing_sections = [str(item).strip() for item in list(validation.get("missing_sections") or []) if str(item).strip()]
    return missing_sections[0] if missing_sections else None

def _build_operator_note(
    *,
    status: str,
    error: dict[str, str] | None,
    validation_initial: dict[str, Any] | None,
) -> str | None:
    if status in {"validation_failed", CV_GENERATION_REVIEW_REQUIRED_STATUS}:
        failed_rule_ids = _extract_failed_rule_ids(validation_initial)
        if failed_rule_ids:
            return f"Validation failed with {len(failed_rule_ids)} rule(s)."
        failing_section = _first_failing_section_key(validation_initial)
        if failing_section:
            return f"Validation failed in section '{failing_section}'."
    message = str((error or {}).get("message") or "").strip()
    return message or None

def _is_recoverable_cv_failure(*, status: str, error: dict[str, str] | None) -> bool:
    if status == "generation_failed":
        message = str((error or {}).get("message") or "").strip().lower()
        return any(token in message for token in ("timeout", "tempor", "rate limit", "unavailable", "provider"))
    reason_code = _normalize_review_required_reason_code(status=status, error=error)
    return reason_code in {ReviewRequiredReasonCode.PROVIDER_ERROR, ReviewRequiredReasonCode.TIMEOUT}

def _hitl_review_reason_for_agentic_case(
    analysis_record: dict[str, Any] | None,
    generation_result: dict[str, Any] | None,
    validation_snapshot: dict[str, Any] | None = None,
) -> str | None:
    if not isinstance(analysis_record, dict) or not isinstance(generation_result, dict):
        return None
    if str(generation_result.get("status") or "").strip().lower() != "accepted":
        return None
    section_hints = analysis_record.get("section_confidence_hints")
    if isinstance(section_hints, dict):
        low_sections = sorted(
            str(section).strip()
            for section, hint in section_hints.items()
            if str(hint or "").strip().lower() in {"low", "very_low", "none", "unsupported"}
        )
        if low_sections:
            return f"Low confidence sections: {', '.join(low_sections)}"
    do_not_claim = [str(item).strip() for item in list(analysis_record.get("do_not_claim") or []) if str(item).strip()]
    if do_not_claim:
        unsupported_requirements: list[str] = []
        for item in list(analysis_record.get("requirement_coverage") or []):
            if not isinstance(item, dict):
                continue
            support_strength = str(item.get("support_strength") or "").strip().lower()
            if support_strength in {"unsupported", "weak", "insufficient"}:
                requirement = str(item.get("requirement") or "").strip()
                if requirement:
                    unsupported_requirements.append(requirement)
        if unsupported_requirements:
            return (
                "Unsupported requirements require review: "
                + ", ".join(sorted(set(unsupported_requirements))[:6])
                + ". Review the generated CV output against these requirements and decide approve as-is, regenerate once, or reject."
            )
    markdown_review_flags = list((validation_snapshot or {}).get("markdown_quality_review_flags") or [])
    if markdown_review_flags:
        return "Markdown quality requires review: " + str(markdown_review_flags[0])
    markdown_blocking_issues = list((validation_snapshot or {}).get("markdown_quality_blocking_issues") or [])
    if markdown_blocking_issues:
        return "Markdown quality issue detected: " + str(markdown_blocking_issues[0])
    return None

def _markdown_quality_review_reason(validation: dict[str, Any] | None) -> str | None:
    if not isinstance(validation, dict):
        return None
    review_flags = [str(item).strip() for item in list(validation.get("markdown_quality_review_flags") or []) if str(item).strip()]
    if review_flags:
        return "Markdown quality requires review: " + review_flags[0]
    return None


def _evaluate_cv_acceptance_policy(
    *,
    fit_classification: str | None,
    gap_summary: dict[str, Any] | None,
    policy: dict[str, Any],
) -> tuple[bool, str | None, str]:
    fit = str(fit_classification or "").strip().lower()
    if fit not in {"strong", "stretch"}:
        return True, None, "policy_not_applicable_fit"

    required_match = dict(policy.get("required_match") or {})
    min_ratio_by_fit = dict(required_match.get("min_ratio_by_fit") or {})
    max_missing_by_fit = dict(required_match.get("max_missing_by_fit") or {})
    force_review_fits = {
        str(item).strip().lower()
        for item in list(policy.get("force_review_when_any_required_missing_for_fits") or [])
        if str(item).strip()
    }
    gap = dict(gap_summary or {})
    matched_required = len(list(gap.get("matched") or []))
    missing_required = len(list(gap.get("missing") or []))
    matchable_required = int(gap.get("matchable_required_count") or (matched_required + missing_required))
    required_ratio = float(matched_required / matchable_required) if matchable_required > 0 else 0.0
    min_ratio = float(min_ratio_by_fit.get(fit, 0.0))
    max_missing = int(max_missing_by_fit.get(fit, 10_000))

    if fit in force_review_fits and missing_required > 0:
        return False, "policy_missing_required_fail", "policy_force_review_missing_required"
    if required_ratio < min_ratio:
        return False, "policy_required_ratio_fail", "policy_required_ratio_below_threshold"
    if missing_required > max_missing:
        return False, "policy_missing_required_fail", "policy_missing_required_above_threshold"
    return True, None, "policy_pass"


def _summarize_cv_generation_model(
    cv_generation_debug_records: list[dict[str, Any]],
    default_model: str | None,
) -> str | None:
    attempted_statuses = {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}
    models = [
        str(record.get("cv_generation_model") or "").strip()
        for record in cv_generation_debug_records
        if str(record.get("status") or "") in attempted_statuses
        and str(record.get("cv_generation_model") or "").strip()
    ]
    unique_models = sorted(set(models))
    if len(unique_models) == 1:
        return unique_models[0]
    if len(unique_models) > 1:
        return "mixed"
    return default_model


def _summarize_cv_generation_provider(
    cv_generation_debug_records: list[dict[str, Any]],
) -> str | None:
    attempted_statuses = {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}
    providers = [
        str((record.get("runtime_provenance") or {}).get("provider") or "").strip()
        for record in cv_generation_debug_records
        if str(record.get("status") or "") in attempted_statuses
        and isinstance(record.get("runtime_provenance"), dict)
        and str((record.get("runtime_provenance") or {}).get("provider") or "").strip()
    ]
    unique_providers = sorted(set(providers))
    if len(unique_providers) == 1:
        return unique_providers[0]
    if len(unique_providers) > 1:
        return "mixed"
    return None


def _build_agentic_live_trace_summary(
    *,
    run_id: str | None,
    cv_generation_debug_records: list[dict[str, Any]],
    late_stage_mode: dict[str, Any],
) -> dict[str, Any]:
    agentic_mode = str(late_stage_mode.get("late_stage_mode") or "").strip()
    if agentic_mode != "agentic":
        return {
            "run_id": run_id,
            "trace_schema_version": "agentic_step_trace_run_v1",
            "trace_family": "agentic_step_trace",
            "step_id": "cv_generation",
            "late_stage_mode": dict(late_stage_mode),
            "trace_status": "not_applicable",
            "trace_summary": {
                "records_total": 0,
                "present_records": 0,
                "attempted_generation_jobs_total": 0,
            },
            "records": [],
            "degradation": {},
            "artifact_refs": {},
        }

    attempted_statuses = {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}
    attempted_records = [
        record for record in cv_generation_debug_records
        if str(record.get("status") or "") in attempted_statuses
    ]
    trace_records: list[dict[str, Any]] = []
    for record in attempted_records:
        raw_trace = record.get("agentic_live_trace")
        if not isinstance(raw_trace, dict):
            continue
        trace_record = dict(raw_trace)
        job_url = str(record.get("job_url") or "").strip()
        trace_record.setdefault("record_id", job_url or str(record.get("job_title") or "").strip())
        trace_record.setdefault("scope_type", "job")
        trace_record.setdefault("scope_key", job_url)
        trace_record["status"] = str(record.get("status") or "")
        trace_record["decision_chain"] = dict(record.get("decision_chain") or {})
        trace_record["artifact_refs"] = {
            "cv_debug_artifact": "cv-debug.json",
            "stage_artifact": "cv_generation.json",
        }
        trace_records.append(trace_record)
    attempted_total = len(attempted_records)
    present_total = len(trace_records)
    trace_status = "completed"
    degradation: dict[str, Any] = {}
    if attempted_total == 0:
        trace_status = "partial"
        degradation = {"reason": "agentic_enabled_without_attempted_generation_records"}
    elif present_total < attempted_total:
        trace_status = "partial"
        degradation = {"reason": "missing_job_trace_records"}
    elif any(str(trace.get("trace_status") or "") == "degraded" for trace in trace_records):
        trace_status = "degraded"
        degradation = {"reason": "provider_or_capture_degraded"}
    return {
        "run_id": run_id,
        "trace_schema_version": "agentic_step_trace_run_v1",
        "trace_family": "agentic_step_trace",
        "step_id": "cv_generation",
        "late_stage_mode": dict(late_stage_mode),
        "trace_status": trace_status,
        "trace_summary": {
            "records_total": attempted_total,
            "present_records": present_total,
            "attempted_generation_jobs_total": attempted_total,
        },
        "records": trace_records,
        "degradation": degradation,
        "artifact_refs": {
            "cv_debug_artifact": "cv-debug.json",
            "stage_artifact": "cv_generation.json",
        },
    }


def _build_cv_analysis_trace_summary(
    *,
    run_id: str | None,
    cv_analysis_results: list[dict[str, Any]],
    late_stage_mode: dict[str, Any],
) -> dict[str, Any]:
    agentic_mode = str(late_stage_mode.get("late_stage_mode") or "").strip()
    if agentic_mode != "agentic":
        return {
            "run_id": run_id,
            "trace_schema_version": "agentic_step_trace_run_v1",
            "trace_family": "agentic_step_trace",
            "step_id": "cv_analysis",
            "late_stage_mode": dict(late_stage_mode),
            "trace_status": "not_applicable",
            "trace_summary": {
                "records_total": 0,
                "present_records": 0,
                "attempted_analysis_jobs_total": 0,
            },
            "records": [],
            "degradation": {},
            "artifact_refs": {},
        }

    trace_records: list[dict[str, Any]] = []
    attempted_total = 0
    for record in cv_analysis_results:
        status = str(record.get("status") or "").strip()
        if status != CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS:
            attempted_total += 1
        raw_trace = record.get("cv_analysis_trace")
        trace_record: dict[str, Any]
        if isinstance(raw_trace, dict):
            trace_record = dict(raw_trace)
        else:
            # Backward-compatibility fallback for reused historical analysis
            # records that predate cv_analysis_trace embedding.
            trace_record = {
                "trace_schema_version": "agentic_step_trace_record_v1",
                "trace_family": "agentic_step_trace",
                "step_id": "cv_analysis",
                "trace_status": "degraded" if status == CV_ANALYSIS_FAILED_STATUS else "completed",
                "runtime_provenance": {
                    "runtime_path": "fitcv_agentic_cv_analysis_builtin",
                    "provider": "fitcv_builtin",
                    "mode_source": "cv.agentic_late_stage.unified_runtime",
                },
                "attempts": [
                    {
                        "attempt_index": 1,
                        "attempt_type": "analysis",
                        "attempt_status": status or "unknown",
                        "provider_status": "failed" if status == CV_ANALYSIS_FAILED_STATUS else "completed",
                    }
                ],
                "input_summary": {
                    "analysis_input_fingerprint": record.get("analysis_input_fingerprint"),
                },
                "output_summary": {
                    "selected_evidence_count": len(list(record.get("evidence_used") or [])),
                    "fallback_used": bool(
                        dict(record.get("evidence_selection_summary") or {}).get("fallback_used", False)
                    ),
                },
                "validation_summary": {"status": "not_run"},
                "repair_summary": {"repair_attempted": False, "repair_attempts": 0},
                "error_summary": dict(record.get("error") or {}) or None,
            }
        job_url = str(record.get("job_url") or "").strip()
        trace_record.setdefault("record_id", job_url or str(record.get("job_title") or "").strip())
        trace_record.setdefault("scope_type", "job")
        trace_record.setdefault("scope_key", job_url)
        trace_record["status"] = status
        trace_record["decision_chain"] = dict(record.get("decision_chain") or {})
        trace_record["artifact_refs"] = {
            "cv_debug_artifact": "cv-debug.json",
            "stage_artifact": "cv_analysis.json",
        }
        trace_records.append(trace_record)

    records_total = len(cv_analysis_results)
    present_total = len(trace_records)
    trace_status = "completed"
    degradation: dict[str, Any] = {}
    if records_total == 0:
        trace_status = "partial"
        degradation = {"reason": "agentic_enabled_without_cv_analysis_records"}
    elif present_total < records_total:
        trace_status = "partial"
        degradation = {"reason": "missing_job_trace_records"}
    elif any(str(trace.get("trace_status") or "").strip() == "degraded" for trace in trace_records):
        trace_status = "degraded"
        degradation = {"reason": "analysis_or_capture_degraded"}

    return {
        "run_id": run_id,
        "trace_schema_version": "agentic_step_trace_run_v1",
        "trace_family": "agentic_step_trace",
        "step_id": "cv_analysis",
        "late_stage_mode": dict(late_stage_mode),
        "trace_status": trace_status,
        "trace_summary": {
            "records_total": records_total,
            "present_records": present_total,
            "attempted_analysis_jobs_total": attempted_total,
        },
        "records": trace_records,
        "degradation": degradation,
        "artifact_refs": {
            "cv_debug_artifact": "cv-debug.json",
            "stage_artifact": "cv_analysis.json",
        },
    }


def _build_cv_analysis_record(
    *,
    job: dict[str, Any],
    status: str,
    analysis_input_fingerprint: str | None,
    analysis_reuse_status: str,
    evidence_payload: list[dict[str, Any]],
    evidence_used: list[dict[str, Any]],
    evidence_selection_summary: dict[str, Any] | None,
    gap_summary: dict[str, Any] | None,
    fit_classification: str | None,
    error: dict[str, str] | None,
    pre_writing_decision: dict[str, Any] | None = None,
    readiness_diagnostics: dict[str, Any] | None = None,
    reuse_decision: dict[str, Any] | None = None,
    analysis_input_components: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ranking_fit_label = _authoritative_ranking_fit_label(job, fit_classification)
    ranking_fit_source = str(job.get("fit_label_source") or "reranker").strip() or None
    cv_status = _cv_generation_status_for_analysis_status(status)
    resolved_reuse_decision = reuse_decision or build_reuse_decision(
        decision=analysis_reuse_status,
        reason_code=analysis_reuse_status,
        fingerprint=analysis_input_fingerprint,
        source_artifact_type="cv_analysis",
    )
    decision_chain = _build_decision_chain(
        shortlist_status=_shortlist_status_for_ranked_job(job),
        advanced_to_scoring=True,
        ranking_fit_label=ranking_fit_label,
        ranking_fit_source=ranking_fit_source,
        cv_analysis_status=status,
        cv_status=cv_status,
    )
    return {
        "job_url": str(job.get("job_url") or ""),
        "job_title": extract_job_title(job),
        "status": status,
        "analysis_input_fingerprint": analysis_input_fingerprint,
        "analysis_input_components": dict(analysis_input_components or {}),
        "analysis_reuse_status": analysis_reuse_status,
        "reuse_decision": dict(resolved_reuse_decision),
        "ranking_fit_label": ranking_fit_label,
        "fit_classification": fit_classification,
        "decision_chain": decision_chain,
        "job_snapshot": dict(job),
        "evidence_payload": list(evidence_payload),
        "evidence_used": evidence_used,
        "evidence_selection_summary": dict(evidence_selection_summary or {}),
        "gap_summary": gap_summary,
        "pre_writing_decision": dict(pre_writing_decision or {}),
        "readiness_diagnostics": dict(readiness_diagnostics or {}),
        # Reranker blocks and fit-gate skips are expected analysis outcomes, not runtime errors.
        "outcome_reason": error if status in {CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS, CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS} else None,
        "error": error if status not in {CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS, CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS} else None,
    }


def _stage_block_not_reached(stage: str) -> dict[str, Any]:
    def _stage_result_builder(stage_id: str) -> dict[str, Any]:
        return _build_stage_result(
            stage_id=stage_id,
            status="not_reached",
            input_counts={},
            output_counts={},
            decision_summary={},
        )

    return _build_stage_block_not_reached_artifacts(
        stage=stage,
        stage_result_builder=_stage_result_builder,
    )


def _truncate_stage_text(value: str, *, limit: int = _STAGE_ARTIFACT_TEXT_LIMIT) -> str:
    return _truncate_stage_text_artifacts(value, limit=limit)


def _truncate_stage_value(value: Any) -> Any:
    return _truncate_stage_value_artifacts(value, text_limit=_STAGE_ARTIFACT_TEXT_LIMIT)


def _sample_rows(
    rows: list[Any],
    row_builder: Callable[[Any], dict[str, Any] | None],
    *,
    limit: int = _STAGE_ARTIFACT_SAMPLE_LIMIT,
) -> list[dict[str, Any]]:
    return _sample_rows_artifacts(
        rows,
        row_builder,
        limit=limit,
        text_limit=_STAGE_ARTIFACT_TEXT_LIMIT,
    )


def _sample_strings(values: list[str], *, limit: int = _STAGE_ARTIFACT_SAMPLE_LIMIT) -> list[str]:
    return _sample_strings_artifacts(
        values,
        limit=limit,
        text_limit=_STAGE_ARTIFACT_TEXT_LIMIT,
    )


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _build_shortlist_quality_metrics(
    *,
    backfilled_jobs_total: int,
    scoring_shortlisted_jobs_total: int,
) -> dict[str, Any]:
    return {
        "backfill_rate": _safe_rate(backfilled_jobs_total, scoring_shortlisted_jobs_total),
        "backfilled_jobs_total": backfilled_jobs_total,
        "scoring_shortlisted_jobs_total": scoring_shortlisted_jobs_total,
    }


def _build_ranking_quality_metrics(ranking_inputs: list[dict[str, Any]], config: dict[str, Any] | None = None) -> dict[str, Any]:
    strong_count = 0
    stretch_count = 0
    skip_count = 0
    for row in ranking_inputs:
        fit_label = str(row.get("fit_label") or "").strip().lower()
        if fit_label == "strong":
            strong_count += 1
        elif fit_label == "stretch":
            stretch_count += 1
        elif fit_label == "skip":
            skip_count += 1
    total_scored = strong_count + stretch_count + skip_count
    return {
        "label_distribution": {
            "strong_count": strong_count,
            "stretch_count": stretch_count,
            "skip_count": skip_count,
            "strong_rate": _safe_rate(strong_count, total_scored),
            "stretch_rate": _safe_rate(stretch_count, total_scored),
            "skip_rate": _safe_rate(skip_count, total_scored),
            "total_scored": total_scored,
        }
    }


def _build_cv_analysis_quality_metrics(cv_analysis_results: list[dict[str, Any]]) -> dict[str, Any]:
    blocked_by_reranker_fit = 0
    ready_for_generation = 0
    skipped_fit_gate = 0
    analysis_failed = 0
    for record in cv_analysis_results:
        status = str(record.get("status") or "").strip().lower()
        if status == CV_ANALYSIS_READY_FOR_GENERATION_STATUS:
            ready_for_generation += 1
        elif status == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS:
            blocked_by_reranker_fit += 1
        elif status == CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS:
            skipped_fit_gate += 1
        elif status == CV_ANALYSIS_FAILED_STATUS:
            analysis_failed += 1
    total_processed = ready_for_generation + blocked_by_reranker_fit + skipped_fit_gate + analysis_failed
    return {
        "blocked_by_reranker_fit_rate": _safe_rate(blocked_by_reranker_fit, total_processed),
        "skip_rate": _safe_rate(skipped_fit_gate, total_processed),
        "ready_for_generation_rate": _safe_rate(ready_for_generation, total_processed),
        "analysis_failed_rate": _safe_rate(analysis_failed, total_processed),
        "blocked_by_reranker_fit": blocked_by_reranker_fit,
        "skipped_fit_gate": skipped_fit_gate,
        "ready_for_generation": ready_for_generation,
        "analysis_failed": analysis_failed,
        "total_processed": total_processed,
    }


def _build_cv_generation_quality_metrics(
    cv_generation_debug_records: list[dict[str, Any]],
) -> dict[str, Any]:
    accepted = 0
    review_required = 0
    validation_failed = 0
    generation_failed = 0
    persistence_failed = 0
    for record in cv_generation_debug_records:
        status = str(record.get("status") or "").strip().lower()
        if status == "accepted":
            accepted += 1
        elif status == CV_GENERATION_REVIEW_REQUIRED_STATUS:
            review_required += 1
        elif status == "validation_failed":
            validation_failed += 1
        elif status == "generation_failed":
            generation_failed += 1
        elif status == "persistence_failed":
            persistence_failed += 1
    total_attempted = accepted + review_required + validation_failed + generation_failed + persistence_failed
    return {
        "validation_fail_rate": _safe_rate(validation_failed, total_attempted),
        "accepted_rate": _safe_rate(accepted, total_attempted),
        "review_required_rate": _safe_rate(review_required, total_attempted),
        "generation_failed_rate": _safe_rate(generation_failed, total_attempted),
        "persistence_failed_rate": _safe_rate(persistence_failed, total_attempted),
        "accepted": accepted,
        "review_required": review_required,
        "validation_failed": validation_failed,
        "generation_failed": generation_failed,
        "persistence_failed": persistence_failed,
        "total_attempted": total_attempted,
    }


def _collect_stage_quality_metrics(stage_transition_artifacts: dict[str, Any]) -> dict[str, Any]:
    stage_metrics: dict[str, Any] = {}
    for stage_id, block in dict(stage_transition_artifacts.get("stages") or {}).items():
        metrics = dict(block.get("decision_summary") or {}).get("quality_metrics")
        if isinstance(metrics, dict) and metrics:
            stage_metrics[stage_id] = metrics
    return stage_metrics


def _job_sample(job: dict[str, Any]) -> dict[str, Any] | None:
    return job_sample(job, export_fields=_EXPORT_ENRICHED_JOB_FIELDS)

def _candidate_profile_summary(profile: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
    return candidate_profile_summary(profile, config)


def _shortlist_row_sample(row: dict[str, Any]) -> dict[str, Any] | None:
    return shortlist_row_sample(row)


def _rule_filter_decision_sample(
    row: dict[str, Any],
    *,
    filter_outcome: str,
) -> dict[str, Any] | None:
    base = job_sample(row, export_fields=_EXPORT_ENRICHED_JOB_FIELDS)
    if not base:
        return None
    sample = {
        **base,
        "filter_outcome": filter_outcome,
        "reasons": list(row.get("reasons") or []),
        "marks": list(row.get("marks") or []),
    }
    return {
        key: value
        for key, value in sample.items()
        if value not in (None, "", [])
    }


def _ranking_row_sample(row: dict[str, Any]) -> dict[str, Any] | None:
    return ranking_row_sample(row)


def _analysis_record_output_sample(record: dict[str, Any]) -> dict[str, Any] | None:
    return analysis_record_output_sample(
        record,
        deterministic_truth_fields=_deterministic_truth_fields,
    )


def _analysis_record_changed_sample(record: dict[str, Any]) -> dict[str, Any] | None:
    return analysis_record_changed_sample(
        record,
        deterministic_truth_fields=_deterministic_truth_fields,
    )


def _debug_record_output_sample(record: dict[str, Any]) -> dict[str, Any] | None:
    return debug_record_output_sample(
        record,
        deterministic_truth_fields=_deterministic_truth_fields,
    )


def _debug_record_changed_sample(record: dict[str, Any]) -> dict[str, Any] | None:
    return debug_record_changed_sample(
        record,
        deterministic_truth_fields=_deterministic_truth_fields,
    )


def _stage_block(
    *,
    stage_id: str,
    status: str,
    input_counts: dict[str, Any],
    output_counts: dict[str, Any],
    decision_summary: dict[str, Any],
    inputs_sample: list[dict[str, Any]],
    outputs_sample: list[dict[str, Any]],
    dropped_or_changed_sample: list[dict[str, Any]],
    settings_refs: list[str] | None = None,
    late_stage_mode: dict[str, Any] | None = None,
) -> dict[str, Any]:
    def _stage_result_builder(
        local_stage_id: str,
        local_status: str,
        local_input_counts: dict[str, Any],
        local_output_counts: dict[str, Any],
        local_decision_summary: dict[str, Any],
    ) -> dict[str, Any]:
        return _build_stage_result(
            stage_id=local_stage_id,
            status=local_status,
            input_counts=local_input_counts,
            output_counts=local_output_counts,
            decision_summary=local_decision_summary,
        )

    return cast(
        dict[str, Any],
        _build_stage_block_artifacts(
            stage_id=stage_id,
            status=status,
            input_counts=input_counts,
            output_counts=output_counts,
            decision_summary=decision_summary,
            inputs_sample=inputs_sample,
            outputs_sample=outputs_sample,
            dropped_or_changed_sample=dropped_or_changed_sample,
            settings_refs=settings_refs,
            late_stage_mode=late_stage_mode,
            truncate_value=_truncate_stage_value,
            stage_result_builder=_stage_result_builder,
        ),
    )


def _otel_id(seed: str, *, length: int) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:length]


def _resolve_stage_decision(
    *,
    status: str,
    decision_summary: dict[str, Any],
) -> str:
    if status == "not_reached":
        return "not_applicable"
    if status != "completed":
        return "fail"
    review_required = int(decision_summary.get("review_required") or 0)
    if review_required > 0:
        return "manual_review"
    return "pass"


def _build_stage_result(
    *,
    stage_id: str,
    status: str,
    input_counts: dict[str, Any],
    output_counts: dict[str, Any],
    decision_summary: dict[str, Any],
) -> dict[str, Any]:
    summary = dict(decision_summary or {})
    decision = _resolve_stage_decision(status=status, decision_summary=summary)
    trace_seed = f"{stage_id}:{status}:{summary.get('debug_records_captured', '')}"
    # Stage result trace context is used for deterministic artifact linkage.
    # Avoid creating extra OTel spans here to reduce low-signal null span exports.
    trace_context = build_trace_context(trace_seed, emit_otel_span=False)
    return {
        "stage_id": stage_id,
        "status": status,
        "stage_version": "1.0.0",
        "output": dict(output_counts or {}),
        "evidence": {
            "input_counts": dict(input_counts or {}),
            "decision_summary": summary,
        },
        "validation": {
            "checks": [],
            "summary": {
                "status": status,
            },
        },
        "decision": decision,
        "policy_version": f"policy.{stage_id}.v1",
        "trace_context": trace_context,
    }


def _build_stage_transition_artifacts(
    *,
    raw_jobs: list[dict[str, Any]],
    normalized: list[dict[str, Any]],
    deduplicated_jobs: list[dict[str, Any]],
    pre_filter_rejected_jobs: list[dict[str, Any]],
    enriched: list[dict[str, Any]],
    passed_jobs: list[dict[str, Any]],
    candidate_filter_rejected_jobs: list[dict[str, Any]],
    raw_shortlist: list[dict[str, Any]],
    shortlist: list[dict[str, Any]],
    backfilled_job_urls: list[str],
    vector_top_n: int,
    candidate_summary: str,
    candidate_query_components: dict[str, Any],
    ai_scores: list[dict[str, Any]],
    ranking_inputs: list[dict[str, Any]],
    ranked: list[dict[str, Any]],
    cv_analysis_results: list[dict[str, Any]] | None = None,
    final_top_n: int,
    cv_generation_debug_records: list[dict[str, Any]],
    profile: dict[str, Any],
    config: dict[str, Any],
    candidate_query_debug: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """@capability cv_system.stage-artifact-diagnostics"""
    candidate_query_debug = dict(candidate_query_debug or {})
    cv_analysis_results = list(cv_analysis_results or [])
    shortlist_reached = len(passed_jobs) > 0
    ranking_reached = shortlist_reached and (len(shortlist) > 0 or len(ai_scores) > 0 or len(ranking_inputs) > 0)
    cv_analysis_reached = len(ranked) > 0 or len(cv_analysis_results) > 0
    generation_execution_records = [
        record for record in cv_generation_debug_records
        if str(record.get("status") or "") in {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}
    ]
    cv_generation_reached = len(generation_execution_records) > 0
    raw_shortlist_urls = set(unique_job_urls(raw_shortlist))
    raw_shortlist_anomaly_urls = compute_raw_shortlist_anomaly_urls(raw_shortlist, passed_jobs)
    shortlist_candidate_query_components = {
        "headline": str(candidate_query_components.get("headline") or ""),
        "target_role": str(candidate_query_components.get("target_role") or ""),
        "recent_roles": list(candidate_query_components.get("recent_roles") or []),
        "role_family_hints": list(candidate_query_components.get("role_family_hints") or []),
        "flattened_skill_sample": list(candidate_query_components.get("flattened_skills") or []),
        "domain_hints": list(candidate_query_components.get("domain_hints") or []),
    }
    shortlist_candidate_query_components = {
        key: value
        for key, value in shortlist_candidate_query_components.items()
        if value not in (None, "", [])
    }
    shortlist_candidate_query_debug = {
        "candidate_query_reuse_status": str(candidate_query_debug.get("candidate_query_reuse_status") or ""),
        "candidate_query_signature": str(candidate_query_debug.get("candidate_query_signature") or ""),
        "candidate_query_contract_fingerprint": str(
            candidate_query_debug.get("candidate_query_contract_fingerprint") or ""
        ),
        "components_hash": str(candidate_query_debug.get("components_hash") or ""),
        "canonical_text_hash": str(candidate_query_debug.get("canonical_text_hash") or ""),
        "bm25_terms_hash": str(candidate_query_debug.get("bm25_terms_hash") or ""),
        "protected_terms_hash": str(candidate_query_debug.get("protected_terms_hash") or ""),
        "protected_terms_count": int(candidate_query_debug.get("protected_terms_count") or 0),
        "shortlist_lexical_scoring_mode": str(candidate_query_debug.get("shortlist_lexical_scoring_mode") or ""),
    }

    shortlist_candidate_query_debug = {
        key: value
        for key, value in shortlist_candidate_query_debug.items()
        if value not in ("", None)
    }
    ranked_urls = {extract_job_url(job) for job in ranked if extract_job_url(job)}
    dedupe_reason_counts: dict[str, int] = {}
    for job in deduplicated_jobs:
        reason = _DEDUPE_REASON_LABELS.get(str(job.get("dedupe_reason") or ""), "deduplicated")
        dedupe_reason_counts[reason] = dedupe_reason_counts.get(reason, 0) + 1
    grouped_reject_reasons: dict[str, int] = {}
    grouped_mark_codes: dict[str, int] = {}
    for rejected in candidate_filter_rejected_jobs:
        for reason in list(rejected.get("reasons") or []):
            grouped_reject_reasons[str(reason)] = grouped_reject_reasons.get(str(reason), 0) + 1
        for mark in list(rejected.get("marks") or []):
            code = str(mark.get("code") or "")
            if code:
                grouped_mark_codes[code] = grouped_mark_codes.get(code, 0) + 1
    for passed in passed_jobs:
        for mark in list(passed.get("marks") or []):
            code = str(mark.get("code") or "")
            if code:
                grouped_mark_codes[code] = grouped_mark_codes.get(code, 0) + 1
    selected_rule_filters = list(
        (
            config.get("rule_filter", {}) if isinstance(config.get("rule_filter"), dict) else {}
        ).get(
            "selected_filters",
            [
                "seniority_mismatch",
                "location_type_excluded",
                "contract_type_excluded",
                "experience_level_excluded",
            ],
        )
    )
    ranking_fit_distribution: dict[str, int] = {}
    for row in ranking_inputs:
        fit_label = str(row.get("fit_label") or "")
        if fit_label:
            ranking_fit_distribution[fit_label] = ranking_fit_distribution.get(fit_label, 0) + 1
    ranking_weights = get_active_ranking_weights(config)
    ranking_defaults = get_active_missing_value_defaults(config)
    preference_fit_weights = get_preference_fit_weights(config)
    zero_weight_features = [
        feature_name
        for feature_name, weight in ranking_weights.items()
        if float(weight) == 0.0
    ]
    contributing_features = [
        feature_name
        for feature_name, weight in ranking_weights.items()
        if float(weight) > 0.0
    ]
    cv_status_counts = {
        "ranked_jobs_total": len(ranked),
        "debug_records_captured": len(cv_generation_debug_records),
        "accepted_count": 0,
        "review_required_count": 0,
        "blocked_by_reranker_fit_count": 0,
        "skipped_fit_gate_count": 0,
        "analysis_failed_count": 0,
        "validation_failed_count": 0,
        "generation_failed_count": 0,
        "persistence_failed_count": 0,
    }
    for record in cv_generation_debug_records:
        status = str(record.get("status") or "")
        if status == "accepted":
            cv_status_counts["accepted_count"] += 1
        elif status == CV_GENERATION_REVIEW_REQUIRED_STATUS:
            cv_status_counts["review_required_count"] += 1
        elif status == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS:
            cv_status_counts["blocked_by_reranker_fit_count"] += 1
        elif status == "skipped_fit_gate":
            cv_status_counts["skipped_fit_gate_count"] += 1
        elif status == "analysis_failed":
            cv_status_counts["analysis_failed_count"] += 1
        elif status == "validation_failed":
            cv_status_counts["validation_failed_count"] += 1
        elif status == "generation_failed":
            cv_status_counts["generation_failed_count"] += 1
        elif status == "persistence_failed":
            cv_status_counts["persistence_failed_count"] += 1
    enrich_prompt_provenance = get_enrich_prompt_provenance(config)
    ranking_prompt_provenance = _prompt_runtime_metadata(
        config,
        stage_id="ranking",
        prompt_key="ai_score",
    )
    cv_generation_prompt_provenance = _prompt_runtime_metadata(
        config,
        stage_id="cv_generation",
        prompt_key="structured_write",
    )
    enrich_reuse_counts = {
        "reused_rows": sum(
            1 for job in enriched
            if str(job.get("enrich_reuse_status") or "") == REUSED_CACHED_ENRICHMENT_STATUS
        ),
        "fresh_rows": sum(
            1 for job in enriched
            if str(job.get("enrich_reuse_status") or "") == FRESH_ENRICHMENT_STATUS
        ),
        "total_enriched_rows": len(enriched),
    }
    enrich_reuse_metrics = {
        "reused_rows": int(enrich_reuse_counts["reused_rows"]),
        "fresh_rows": int(enrich_reuse_counts["fresh_rows"]),
        "total_rows": int(enrich_reuse_counts["total_enriched_rows"]),
    }
    enrich_reuse_metrics["reuse_rate"] = _safe_rate(
        int(enrich_reuse_metrics["reused_rows"]),
        int(enrich_reuse_metrics["total_rows"]),
    )
    shortlist_embedding_reuse_counts = {
        "embedding_reused_jobs": sum(
            1 for job in passed_jobs
            if str(job.get("embedding_reuse_status") or "") == "reused_cached_embedding"
        ),
        "embedding_fresh_jobs": sum(
            1 for job in passed_jobs
            if str(job.get("embedding_reuse_status") or "") == "fresh_embedding"
        ),
        "embedding_total_jobs": len(passed_jobs),
    }
    shortlist_quality_metrics = _build_shortlist_quality_metrics(
        backfilled_jobs_total=len(backfilled_job_urls),
        scoring_shortlisted_jobs_total=len(shortlist),
    )
    ranking_quality_metrics = _build_ranking_quality_metrics(ranking_inputs, config=config)
    ranking_reuse_metrics = {
        "reused_ai_scores": sum(
            1 for row in ai_scores
            if str(row.get("ai_score_reuse_status") or "") == "reused_exact_match"
        ),
        "fresh_ai_scores": sum(
            1 for row in ai_scores
            if str(row.get("ai_score_reuse_status") or "") == "fresh_compute"
        ),
        "total_ai_scores": len(ai_scores),
    }
    cv_analysis_quality_metrics = _build_cv_analysis_quality_metrics(cv_analysis_results)
    cv_analysis_executed_rows = [
        record for record in cv_analysis_results
        if str(record.get("status") or "") != CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS
    ]
    cv_analysis_reuse_metrics: dict[str, Any] = {
        "analysis_rows_executed": len(cv_analysis_executed_rows),
        "reused_analysis_rows": sum(
            1 for record in cv_analysis_executed_rows
            if str(record.get("analysis_reuse_status") or "") == "reused_exact_match"
        ),
        "fresh_analysis_rows": sum(
            1 for record in cv_analysis_executed_rows
            if str(record.get("analysis_reuse_status") or "") == "fresh_compute"
        ),
        "blocked_before_analysis_rows": sum(
            1 for record in cv_analysis_results
            if str(record.get("status") or "") == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS
        ),
    }
    cv_analysis_reuse_metrics["analysis_reuse_rate"] = _safe_rate(
        int(cv_analysis_reuse_metrics["reused_analysis_rows"]),
        int(cv_analysis_reuse_metrics["analysis_rows_executed"]),
    )
    cv_generation_quality_metrics = _build_cv_generation_quality_metrics(cv_generation_debug_records)
    cv_generation_reuse_metrics = {
        "reused_rows": sum(
            1 for record in cv_generation_debug_records
            if str(record.get("cv_generation_reuse_status") or "") == "reused_exact_match"
        ),
        "fresh_rows": sum(
            1 for record in cv_generation_debug_records
            if str(record.get("cv_generation_reuse_status") or "") == "fresh_compute"
        ),
        "total_rows": sum(
            1 for record in cv_generation_debug_records
            if str(record.get("status") or "") in {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}
        ),
    }
    cv_generation_reuse_metrics["reuse_rate"] = _safe_rate(
        int(cv_generation_reuse_metrics["reused_rows"]),
        int(cv_generation_reuse_metrics["total_rows"]),
    )
    agentic_late_stage_enabled = _agentic_late_stage_enabled(config)

    return {
        "schema_version": STAGE_TRANSITION_ARTIFACTS_PIPELINE_SCHEMA_VERSION,
        "stages": {
            "normalize": _build_normalize_stage_block_artifacts(
                raw_jobs=raw_jobs,
                normalized=normalized,
                deduplicated_jobs=deduplicated_jobs,
                dedupe_reason_counts=dedupe_reason_counts,
                stage_block_builder=_stage_block,
                sample_rows_builder=_sample_rows,
                job_sample_builder=_job_sample,
                dedupe_reason_label_resolver=lambda reason: _DEDUPE_REASON_LABELS.get(
                    reason,
                    "deduplicated",
                ),
            ),
            "enrich": _build_enrich_stage_block_artifacts(
                normalized=normalized,
                pre_filter_rejected_jobs=pre_filter_rejected_jobs,
                enriched=enriched,
                profile=profile,
                config=config,
                enrich_prompt_provenance=enrich_prompt_provenance,
                enrich_reuse_counts=enrich_reuse_counts,
                enrich_reuse_metrics=enrich_reuse_metrics,
                stage_block_builder=_stage_block,
                sample_rows_builder=_sample_rows,
                job_sample_builder=_job_sample,
                extract_job_url=extract_job_url,
                candidate_profile_summary_builder=_candidate_profile_summary,
            ),
            "rule_filter": _build_rule_filter_stage_block_artifacts(
                enriched=enriched,
                passed_jobs=passed_jobs,
                candidate_filter_rejected_jobs=candidate_filter_rejected_jobs,
                grouped_reject_reasons=grouped_reject_reasons,
                grouped_mark_codes=grouped_mark_codes,
                selected_rule_filters=selected_rule_filters,
                stage_block_builder=_stage_block,
                sample_rows_builder=_sample_rows,
                job_sample_builder=_job_sample,
                rule_filter_decision_sample_builder=_rule_filter_decision_sample,
            ),
            "shortlist": _build_shortlist_stage_block_artifacts(
                shortlist_reached=shortlist_reached,
                passed_jobs=passed_jobs,
                raw_shortlist_urls=raw_shortlist_urls,
                raw_shortlist_anomaly_urls=raw_shortlist_anomaly_urls,
                raw_shortlist=raw_shortlist,
                shortlist=shortlist,
                backfilled_job_urls=backfilled_job_urls,
                shortlist_embedding_reuse_counts=shortlist_embedding_reuse_counts,
                shortlist_candidate_query_components=shortlist_candidate_query_components,
                shortlist_candidate_query_debug=shortlist_candidate_query_debug,
                shortlist_quality_metrics=shortlist_quality_metrics,
                candidate_summary=candidate_summary,
                vector_top_n=vector_top_n,
                stage_block_builder=_stage_block,
                stage_block_not_reached_builder=_stage_block_not_reached,
                sample_rows_builder=_sample_rows,
                sample_strings_builder=_sample_strings,
                job_sample_builder=_job_sample,
                shortlist_row_sample_builder=_shortlist_row_sample,
                extract_job_url=extract_job_url,
                extract_job_title=extract_job_title,
            ),
            "ranking": _build_ranking_stage_block_artifacts(
                ranking_reached=ranking_reached,
                ai_scores=ai_scores,
                ranking_inputs=ranking_inputs,
                ranked=ranked,
                ranked_urls=ranked_urls,
                final_top_n=final_top_n,
                ranking_fit_distribution=ranking_fit_distribution,
                ranking_quality_metrics=ranking_quality_metrics,
                ranking_reuse_metrics=ranking_reuse_metrics,
                ranking_prompt_provenance=ranking_prompt_provenance,
                ranking_weights=ranking_weights,
                ranking_defaults=ranking_defaults,
                preference_fit_weights=preference_fit_weights,
                zero_weight_features=zero_weight_features,
                contributing_features=contributing_features,
                profile=profile,
                config=config,
                stage_block_builder=_stage_block,
                stage_block_not_reached_builder=_stage_block_not_reached,
                sample_rows_builder=_sample_rows,
                ranking_row_sample_builder=_ranking_row_sample,
                extract_job_url=extract_job_url,
                gemini_model_resolver=get_gemini_model,
                effective_preferences_resolver=infer_effective_preferences,
            ),
            "cv_analysis": _build_cv_analysis_stage_block_artifacts(
                cv_analysis_reached=cv_analysis_reached,
                ranked=ranked,
                cv_analysis_results=cv_analysis_results,
                cv_analysis_quality_metrics=cv_analysis_quality_metrics,
                cv_analysis_reuse_metrics=cv_analysis_reuse_metrics,
                config=config,
                agentic_late_stage_enabled=agentic_late_stage_enabled,
                blocked_by_reranker_status=CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS,
                ready_for_generation_status=CV_ANALYSIS_READY_FOR_GENERATION_STATUS,
                skipped_fit_gate_status=CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS,
                failed_status=CV_ANALYSIS_FAILED_STATUS,
                stage_block_builder=_stage_block,
                stage_block_not_reached_builder=_stage_block_not_reached,
                sample_rows_builder=_sample_rows,
                ranking_row_sample_builder=_ranking_row_sample,
                analysis_record_output_sample_builder=_analysis_record_output_sample,
                analysis_record_changed_sample_builder=_analysis_record_changed_sample,
                late_stage_mode_payload_builder=_build_late_stage_mode_payload,
            ),
            "cv_generation": _build_cv_generation_stage_block_artifacts(
                cv_generation_reached=cv_generation_reached,
                cv_analysis_results=cv_analysis_results,
                cv_generation_debug_records=cv_generation_debug_records,
                cv_status_counts=cv_status_counts,
                cv_generation_quality_metrics=cv_generation_quality_metrics,
                cv_generation_reuse_metrics=cv_generation_reuse_metrics,
                cv_generation_prompt_provenance=cv_generation_prompt_provenance,
                config=config,
                agentic_late_stage_enabled=agentic_late_stage_enabled,
                stage_block_builder=_stage_block,
                stage_block_not_reached_builder=_stage_block_not_reached,
                sample_rows_builder=_sample_rows,
                analysis_record_output_sample_builder=_analysis_record_output_sample,
                debug_record_output_sample_builder=_debug_record_output_sample,
                debug_record_changed_sample_builder=_debug_record_changed_sample,
                late_stage_mode_payload_builder=_build_late_stage_mode_payload,
                cv_generation_model_summarizer=_summarize_cv_generation_model,
                cv_generation_provider_summarizer=_summarize_cv_generation_provider,
                cv_generation_model_resolver=get_cv_generation_model,
            ),
        },
    }
def _build_cv_generation_input_fingerprint(
    *,
    analysis_record: dict[str, Any],
    config: dict[str, Any],
    cv_prompt_id: str | None,
    cv_prompt_template_path: str | None,
    cv_prompt_version: str | None,
    cv_generation_model: str | None,
    enabled_sections: list[str] | None,
    cv_acceptance_policy: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = {
        "schema_version": "cv_generation_input_fingerprint_v1",
        "analysis_input_fingerprint": str(analysis_record.get("analysis_input_fingerprint") or ""),
        "job_url": str(analysis_record.get("job_url") or ""),
        "fit_classification": str(analysis_record.get("fit_classification") or ""),
        "cv_prompt_id": str(cv_prompt_id or ""),
        "cv_prompt_template_path": str(cv_prompt_template_path or ""),
        "cv_prompt_version": str(cv_prompt_version or ""),
        "cv_generation_model": str(cv_generation_model or ""),
        "enabled_sections": sorted(str(item or "") for item in (enabled_sections or [])),
        "cv_acceptance_policy": dict(cv_acceptance_policy or {}),
        "agentic_late_stage_enabled": bool(_agentic_late_stage_enabled(config)),
    }
    seed = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {
        "fingerprint": hashlib.sha256(seed.encode("utf-8")).hexdigest(),
        "payload": payload,
    }

def build_ranking_features(
    shortlist: list[dict[str, Any]],
    ai_scores: list[dict[str, Any]],
    profile: dict[str, Any],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Merge shortlist + AI-score records into a single six-feature ranking row."""
    shortlist_index: dict[str, dict[str, Any]] = {
        row["job_url"]: row for row in shortlist
    }
    weights = get_active_ranking_weights(config)
    null_defaults = get_active_missing_value_defaults(config)
    candidate_skills = flatten_skills(profile)
    preference_resolution = infer_effective_preferences(profile, config)
    effective_preferences = dict(preference_resolution["effective_preferences"] or {})
    inferred_preferences = dict(preference_resolution["inferred_preferences"] or {})
    preference_sources = dict(preference_resolution["preference_sources"] or {})

    features: list[dict[str, Any]] = []
    for ai_row in ai_scores:
        job_url = str(ai_row.get("job_url") or "")
        sl_row = shortlist_index.get(job_url)
        if sl_row is None:
            continue  # not in shortlist — skip

        vector_rank = sl_row.get("vector_rank", sl_row.get("rank"))
        raw_vector_similarity = sl_row.get("vector_similarity", sl_row.get("similarity_score"))
        vector_similarity = (
            float(raw_vector_similarity)
            if raw_vector_similarity is not None
            else null_defaults["vector_similarity"]
        )
        raw_ai_score = ai_row.get("ai_score")
        ai_score = (
            float(raw_ai_score)
            if raw_ai_score is not None
            else null_defaults["ai_score"]
        )
        ranking_source: dict[str, Any] = {
            **sl_row,
            **ai_row,
        }
        required_skills = list(
            ranking_source.get("required_skills_canonical")
            or ranking_source.get("required_skills")
            or []
        )
        must_have_match = compute_must_have_match(required_skills, candidate_skills, config)
        title_relevance = compute_title_relevance(
            extract_job_title(ranking_source),
            str(effective_preferences.get("target_role") or "") or None,
            job_family=str(ranking_source.get("job_family") or "") or None,
            config=config,
        )
        seniority_fit = compute_seniority_fit(
            str(ranking_source.get("seniority") or "") or None,
            str(effective_preferences.get("seniority_target") or "") or None,
            config,
        )
        preference_fit_details = compute_preference_fit_details(ranking_source, effective_preferences, config)
        preference_fit = float(preference_fit_details["score"])

        feature_values = {
            "ai_score": ai_score,
            "must_have_match": must_have_match,
            "vector_similarity": vector_similarity,
            "title_relevance": title_relevance,
            "seniority_fit": seniority_fit,
            "preference_fit": preference_fit,
        }

        feature: dict[str, Any] = {
            **ranking_source,
            "vector_rank": int(vector_rank or 0),
            **feature_values,
            "feature_contributions": compute_feature_contributions(feature_values, weights, null_defaults),
            "preference_fit_components": preference_fit_details["components"],
            "effective_preferences": effective_preferences,
            "inferred_preferences": inferred_preferences,
            "preference_sources": preference_sources,
            "fit_label_source": "reranker" if ai_row.get("fit_label") is not None else "reranker_score_thresholds",
        }
        feature["final_score"] = compute_final_score(feature, weights, null_defaults)
        features.append(feature)

    return features


# ── orchestrator ──────────────────────────────────────────────────────────────

def run_pipeline(
    jobs_path: str,
    config_path: str = ".env.yaml",
    reporter: object = None,  # Optional[PipelineReporter] — avoids circular import
    config: dict | None = None,  # If provided, skips load_config(config_path)
    run_id: str | None = None,
    cancellation_check: Callable[[], bool] | None = None,
    start_stage: str | None = None,
    stop_after_stage: str | None = None,
    checkpoint_payload: dict[str, Any] | None = None,
    reuse_snapshots: dict[str, Any] | None = None,
    stage_progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run the full FitCV candidate pipeline end-to-end.

    @capability bounded_parallel_enrichment.pre-enrichment-global-filters-run-first
    @capability cv_system.fit-gate-resolution
    @capability cv_system.exact-match-late-stage-reuse

    Parameters
    ----------
    reporter:
        Optional PipelineReporter instance injected by the control-plane worker.
        When provided, stage events are emitted to pipeline_run_events in BigQuery.
        When None, no events are emitted (normal CLI / test usage).
    config:
        Optional pre-built config dict. When provided, `config_path` is ignored.
        Used by the worker to inject the effective settings snapshot stored at
        trigger time. When None, config is loaded from `config_path` as usual.
    run_id:
        Optional externally provided run ID. When present, it is treated as the
        canonical identifier for summaries, events, and persisted records.

    Returns
    -------
    dict with keys:
        run_id          : UUID4 of this run
        total_jobs      : number of raw jobs ingested
        passed_filter   : number of jobs that passed rule filtering
        ranked          : number of jobs in the final shortlist
        cvs_generated   : number of successfully generated + validated CVs
    """
    if config is None:
        config = load_config(config_path)
    pipeline_store = PipelineStore(
        load_raw_jobs_fn=load_to_bigquery,
        load_candidate_profile_fn=load_candidate_to_bigquery,
        lookup_reusable_structured_jobs_fn=lookup_reusable_structured_jobs,
        load_structured_jobs_fn=load_structured_jobs,
        load_run_structured_jobs_fn=load_run_structured_jobs,
        store_filter_results_fn=store_filter_results,
        embed_and_store_jobs_fn=embed_and_store_jobs,
        store_shortlist_fn=store_shortlist,
        store_final_ranking_fn=store_final_ranking,
        store_cv_version_fn=store_cv_version,
    )
    run_id = run_id or create_run_id()
    with observe_span("fitcv.run_pipeline", attributes={"run_id": run_id}):
        start_stage = _canonical_resume_start_stage(
            requested_start_stage=start_stage,
            checkpoint_payload=checkpoint_payload,
            run_id=run_id,
        ) or PIPELINE_STAGE_SEQUENCE[0]
        stop_after_stage = _validate_pipeline_stage_name(stop_after_stage)
        if stop_after_stage is not None:
            if PIPELINE_STAGE_SEQUENCE.index(stop_after_stage) < PIPELINE_STAGE_SEQUENCE.index(start_stage):
                raise ValueError(
                    f"stop_after_stage {stop_after_stage!r} cannot precede start_stage {start_stage!r}"
                )
        logger.info("Pipeline run started [run_id=%s]", run_id)
        if reporter is not None:
            reporter.emit("pipeline_start", "info", f"Run started [run_id={run_id}]")  # type: ignore[union-attr]
        state = _restore_pipeline_state(run_id=run_id, checkpoint_payload=checkpoint_payload)
        normalized_reuse_snapshots = _normalize_late_stage_reuse_snapshots(reuse_snapshots)
        ranking_ai_score_reuse_index = _index_late_stage_reuse_rows(
            normalized_reuse_snapshots["ranking_ai_scores"],
            fingerprint_key="ai_score_input_fingerprint",
            payload_key="ai_score_row",
        )
        cv_analysis_reuse_index = _index_late_stage_reuse_rows(
            normalized_reuse_snapshots["cv_analysis_records"],
            fingerprint_key="analysis_input_fingerprint",
            payload_key="analysis_record",
        )
        cv_analysis_reuse_by_url = _index_cv_analysis_reuse_by_url(
            normalized_reuse_snapshots["cv_analysis_records"],
        )
        raw_jobs = list(state["raw_jobs"])
        normalized = list(state["normalized"])
        deduplicated_jobs = list(state["deduplicated_jobs"])
        pre_filter_rejected_jobs = list(state["pre_filter_rejected_jobs"])
        enriched = list(state["enriched"])
        passed_jobs = list(state["passed_jobs"])
        candidate_filter_rejected_jobs = list(state["candidate_filter_rejected_jobs"])
        raw_shortlist = list(state["raw_shortlist"])
        shortlist = list(state["shortlist"])
        backfilled_job_urls = list(state["backfilled_job_urls"])
        ai_scores = list(state["ai_scores"])
        ranking_inputs = list(state["ranking_inputs"])
        ranked = list(state["ranked"])
        cv_analysis_results = list(state["cv_analysis_results"])
        results: list[dict[str, Any]] = list(state["cv_results"])
        cv_generation_debug_records: list[dict[str, Any]] = list(state["cv_generation_debug_records"])
        profile: dict[str, Any] | None = None
        candidate_skill_names: list[str] = []
        candidate_summary = ""
        candidate_query_components: dict[str, Any] = {}
        candidate_query_debug: dict[str, Any] = {}
        vector_top_n = pipeline_int(config, "vector_search_top_n", default=0)
        final_top_n = pipeline_int(config, "final_top_n", default=0)

        if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("normalize"):
            with observe_span("pipeline.normalize", attributes={"run_id": run_id}):
                raw_jobs = parse_jobs_file(jobs_path)
                normalized = normalize_batch(raw_jobs)
                _normalized_with_exclusions, deduplicated_jobs = normalize_batch_with_exclusions(raw_jobs)
                if reporter is not None:
                    reporter.emit(  # type: ignore[union-attr]
                        "layer1_normalize",
                        "info",
                        f"Normalization dedupe: kept {len(normalized)} of {len(raw_jobs)} jobs, removed {len(deduplicated_jobs)} duplicate(s)",
                    )

                raw_rows = prepare_raw_rows(raw_jobs)
                pipeline_store.load_raw_jobs(raw_rows, config)
                state["raw_jobs"] = raw_jobs
                state["normalized"] = normalized
                state["deduplicated_jobs"] = deduplicated_jobs
                set_span_attributes(
                    {
                        "input_jobs": len(raw_jobs),
                        "normalized_jobs": len(normalized),
                        "deduplicated_jobs": len(deduplicated_jobs),
                    }
                )
            stage_boundary_result = _handle_stage_boundary(
                run_id=run_id,
                last_completed_stage="normalize",
                stop_after_stage=stop_after_stage,
                state=state,
                profile=None,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
                stage_progress_callback=stage_progress_callback,
            )
            if stage_boundary_result is not None:
                return stage_boundary_result

        normalized = list(state["normalized"])

        if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("enrich"):
            with observe_span("pipeline.enrich", attributes={"run_id": run_id}):
                enrich_runtime_config = _enrich_runtime_projection(config)
                enrich_concurrency = _enrich_stage_concurrency(config)
                raw_global = config.get("global_job_filters", {})
                global_settings = (
                    {f"global_job_filters.{k}": v for k, v in raw_global.items()}
                    if raw_global else None
                )
                pre_filter = apply_pre_enrichment_global_filters(normalized, global_settings)
                pre_filter_passed_urls: set[str] = set(pre_filter["passed"])
                surviving_normalized = [
                    j for j in normalized
                    if str(j.get("job_url", "")) in pre_filter_passed_urls
                ]
                pre_filter_rejected_jobs = list(pre_filter["rejected"])
                if reporter is not None:
                    n_pre_rejected = len(normalized) - len(surviving_normalized)
                    reporter.emit(  # type: ignore[union-attr]
                        "layer1b_pre_filter", "info",
                        f"Pre-enrichment filter: {len(surviving_normalized)} pass, {n_pre_rejected} rejected",
                    )

                if cancellation_check and cancellation_check():
                    raise PipelineCancelled("Cancelled before enrichment")
                enriched, fresh_enriched_rows = _enrich_jobs_with_reuse(
                    surviving_normalized,
                    enrich_runtime_config,
                    pipeline_store=pipeline_store,
                    incremental_save_run_id=run_id,
                    heartbeat_callback=(
                        (lambda payload: reporter.emit(  # type: ignore[union-attr]
                            "enrich_heartbeat",
                            "info",
                            "Enrich in progress",
                            {
                                **dict(payload or {}),
                                "enrich_concurrency_effective": int(enrich_concurrency),
                            },
                        ))
                        if reporter is not None else None
                    ),
                )
                reused_count = sum(
                    1 for row in enriched
                    if str(row.get("enrich_reuse_status") or "") == REUSED_CACHED_ENRICHMENT_STATUS
                )
                fresh_count = sum(
                    1 for row in enriched
                    if str(row.get("enrich_reuse_status") or "") == FRESH_ENRICHMENT_STATUS
                )
                if reporter is not None:
                    reporter.emit(  # type: ignore[union-attr]
                        "layer1_jobs", "info",
                        (
                            f"Ingested {len(raw_jobs)} jobs, enriched {len(enriched)} "
                            f"(after pre-filter; fresh={fresh_count}, reused={reused_count})"
                        ),
                    )
                state["pre_filter_rejected_jobs"] = pre_filter_rejected_jobs
                state["enriched"] = enriched
                set_span_attributes(
                    {
                        "normalized_jobs": len(normalized),
                        "pre_filter_rejected": len(pre_filter_rejected_jobs),
                        "enriched_jobs": len(enriched),
                        "fresh_enriched": fresh_count,
                        "reused_enriched": reused_count,
                    }
                )
            stage_boundary_result = _handle_stage_boundary(
                run_id=run_id,
                last_completed_stage="enrich",
                stop_after_stage=stop_after_stage,
                state=state,
                profile=None,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
                stage_progress_callback=stage_progress_callback,
            )
            if stage_boundary_result is not None:
                return stage_boundary_result

        pre_filter_rejected_jobs = list(state["pre_filter_rejected_jobs"])
        enriched = list(state["enriched"])

        if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("rule_filter"):
            with observe_span("pipeline.rule_filter", attributes={"run_id": run_id}):
                runtime_profile_json: str | None = (
                    config.get("runtime_inputs", {}).get("candidate_profile_json")
                )
                if runtime_profile_json:
                    profile = load_profile_json_text(runtime_profile_json)
                else:
                    profile_path: str = str(config["paths"]["candidate_profile"])
                    profile = load_profile_yaml(profile_path)
                pipeline_store.load_candidate_profile(profile, config)
                candidate_skill_names = flatten_skills(profile)
                if reporter is not None:
                    reporter.emit("layer2_candidate", "info", "Candidate profile loaded")  # type: ignore[union-attr]

                filter_result = apply_rule_filters(enriched, profile["preferences"], config)
                combined_filter_result = {
                    "passed": filter_result["passed"],
                    "passed_records": filter_result.get("passed_records", []),
                    "rejected": pre_filter_rejected_jobs + filter_result["rejected"],
                }
                passed_job_urls = [str(url) for url in filter_result["passed"]]
                passed_records_by_url = {
                    str(item.get("job_url") or ""): item
                    for item in filter_result.get("passed_records", [])
                    if str(item.get("job_url") or "")
                }
                enriched_by_url = {
                    str(job.get("job_url") or ""): job
                    for job in enriched
                }
                passed_jobs = [
                    {
                        **enriched_by_url[url],
                        "marks": list((passed_records_by_url.get(url) or {}).get("marks") or []),
                    }
                    for url in passed_job_urls
                    if url in enriched_by_url
                ]
                candidate_filter_rejected_jobs = list(filter_result["rejected"])
                pipeline_store.store_filter_results(combined_filter_result, run_id, config)
                if reporter is not None:
                    reporter.emit("layer3_filter", "info", f"{len(passed_jobs)} passed rule filter")  # type: ignore[union-attr]
                state["passed_jobs"] = passed_jobs
                state["candidate_filter_rejected_jobs"] = candidate_filter_rejected_jobs
                set_span_attributes(
                    {
                        "candidate_skills": len(candidate_skill_names),
                        "passed_jobs": len(passed_jobs),
                        "candidate_filter_rejected": len(candidate_filter_rejected_jobs),
                    }
                )
            stage_boundary_result = _handle_stage_boundary(
                run_id=run_id,
                last_completed_stage="rule_filter",
                stop_after_stage=stop_after_stage,
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
                stage_progress_callback=stage_progress_callback,
            )
            if stage_boundary_result is not None:
                return stage_boundary_result
        else:
            runtime_profile_json = config.get("runtime_inputs", {}).get("candidate_profile_json")
            if runtime_profile_json:
                profile = load_profile_json_text(runtime_profile_json)
            else:
                profile_path = str(config["paths"]["candidate_profile"])
                profile = load_profile_yaml(profile_path)
            candidate_skill_names = flatten_skills(profile)

        passed_jobs = list(state["passed_jobs"])
        candidate_filter_rejected_jobs = list(state["candidate_filter_rejected_jobs"])
        passed_job_urls = [extract_job_url(job) for job in passed_jobs if extract_job_url(job)]

        if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("shortlist"):
            with observe_span("pipeline.shortlist", attributes={"run_id": run_id, "vector_top_n": vector_top_n}):
                # Active shortlist runtime only prepares reusable job embeddings here.
                # The candidate-side vector actually used for retrieval is generated
                # inside run_vector_search() from the deterministic candidate query text.
                pipeline_store.embed_and_store_jobs(passed_jobs, config)
                raw_shortlist_result = run_vector_search(
                    profile,
                    [str(job.get("job_url") or "") for job in passed_jobs],
                    config,
                    top_n=vector_top_n,
                    include_debug=True,
                )
                candidate_query_record: dict[str, Any] = {}
                if isinstance(raw_shortlist_result, dict):
                    raw_shortlist = list(raw_shortlist_result.get("rows") or [])
                    candidate_query_record = dict(raw_shortlist_result.get("candidate_query") or {})
                else:
                    raw_shortlist = list(raw_shortlist_result)
                from fitcv.vector_search import (
                    build_candidate_query_components,
                    build_candidate_query_embedding_contract_fingerprint,
                    build_candidate_query_signature_record,
                    build_candidate_query_text,
                    build_weighted_bm25_query_terms,
                )

                candidate_query_components = dict(
                    candidate_query_record.get("components") or build_candidate_query_components(profile, config)
                )
                candidate_summary = str(
                    candidate_query_record.get("text") or build_candidate_query_text(profile, config)
                )
                signature_record = build_candidate_query_signature_record(candidate_query_components)
                contract_record = build_candidate_query_embedding_contract_fingerprint(config)
                bm25_term_record = build_weighted_bm25_query_terms(candidate_query_components, config)
                components_hash = hashlib.sha256(
                    json.dumps(candidate_query_components, sort_keys=True, ensure_ascii=False).encode("utf-8")
                ).hexdigest()
                canonical_text_hash = hashlib.sha256(candidate_summary.encode("utf-8")).hexdigest()
                candidate_query_debug = {
                    "candidate_query_reuse_status": str(
                        candidate_query_record.get("candidate_query_reuse_status") or ""
                    ),
                    "candidate_query_signature": str(
                        candidate_query_record.get("candidate_query_signature") or signature_record["signature"]
                    ),
                    "candidate_query_contract_fingerprint": str(
                        candidate_query_record.get("candidate_query_contract_fingerprint")
                        or contract_record["fingerprint"]
                    ),
                    "components_hash": components_hash,
                    "canonical_text_hash": canonical_text_hash,
                    "bm25_terms_hash": str(bm25_term_record.get("bm25_terms_hash") or ""),
                    "protected_terms_hash": str(bm25_term_record.get("protected_terms_hash") or ""),
                    "protected_terms_count": int(bm25_term_record.get("protected_terms_count") or 0),
                    "shortlist_lexical_scoring_mode": str((bm25_term_record.get("payload") or {}).get("scoring_mode") or ""),
                }
                shortlist_fail_fast = bool((config.get("pipeline", {}) or {}).get("shortlist_fail_fast_empty_raw_hits", False))
                if shortlist_fail_fast and passed_jobs and not raw_shortlist:
                    raise RuntimeError(
                        "Vector shortlist returned zero raw hits for non-empty passed jobs; fail-fast guard active"
                    )
                shortlist = _materialize_scoring_shortlist(raw_shortlist, passed_jobs, vector_top_n)
                pipeline_store.store_shortlist(shortlist, config)
                raw_shortlist_urls = set(unique_job_urls(raw_shortlist))
                raw_shortlist_anomaly_urls = compute_raw_shortlist_anomaly_urls(raw_shortlist, passed_jobs)
                backfilled_job_urls = [
                    str(job.get("job_url") or "")
                    for job in shortlist
                    if str(job.get("job_url") or "") not in raw_shortlist_urls
                ]
                if reporter is not None:
                    shortlist_message = f"Vector shortlist: {len(raw_shortlist_urls)} raw hits"
                    if backfilled_job_urls:
                        shortlist_message += f", {len(shortlist)} scoring jobs ({len(backfilled_job_urls)} backfilled)"
                    if raw_shortlist_anomaly_urls:
                        shortlist_message += f", {len(raw_shortlist_anomaly_urls)} raw-hit anomalies"
                    reporter.emit("layer3_shortlist", "info", shortlist_message)  # type: ignore[union-attr]
                state["raw_shortlist"] = raw_shortlist
                state["shortlist"] = shortlist
                state["backfilled_job_urls"] = backfilled_job_urls
                state["candidate_query_debug"] = candidate_query_debug
                set_span_attributes(
                    {
                        "passed_jobs": len(passed_jobs),
                        "raw_shortlist_hits": len(raw_shortlist_urls),
                        "shortlist_jobs": len(shortlist),
                        "backfilled_jobs": len(backfilled_job_urls),
                    }
                )
            stage_boundary_result = _handle_stage_boundary(
                run_id=run_id,
                last_completed_stage="shortlist",
                stop_after_stage=stop_after_stage,
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
                stage_progress_callback=stage_progress_callback,
            )
            if stage_boundary_result is not None:
                return stage_boundary_result

        raw_shortlist = list(state["raw_shortlist"])
        shortlist = list(state["shortlist"])
        backfilled_job_urls = list(state["backfilled_job_urls"])
        candidate_query_debug = dict(state.get("candidate_query_debug") or candidate_query_debug)
        raw_shortlist_urls = set(unique_job_urls(raw_shortlist))
        raw_shortlist_anomaly_urls = compute_raw_shortlist_anomaly_urls(raw_shortlist, passed_jobs)

        if not candidate_query_components or not candidate_summary:
            from fitcv.vector_search import build_candidate_query_components, build_candidate_query_text

            candidate_query_components = build_candidate_query_components(profile, config)
            candidate_summary = build_candidate_query_text(profile, config)

        if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("ranking"):
            with observe_span("pipeline.ai_score", attributes={"run_id": run_id}):
                ai_top_n = pipeline_int(config, "ai_score_top_n", default=0)
                if cancellation_check and cancellation_check():
                    raise PipelineCancelled("Cancelled before AI scoring")
                ai_score_candidates = shortlist[:ai_top_n]
                ranking_reuse_enabled = _reuse_stage_enabled(config, "ranking")
                fresh_scoring_jobs: list[dict[str, Any]] = []
                fresh_ai_score_fingerprints: dict[str, str] = {}
                reused_ai_scores_by_url: dict[str, dict[str, Any]] = {}
                for shortlisted_job in ai_score_candidates:
                    top_evidence = list(shortlisted_job.get("top_evidence") or [])[:2]
                    fingerprint_record = build_ai_score_input_fingerprint(
                        shortlisted_job,
                        candidate_summary,
                        top_evidence,
                        config,
                    )
                    job_url = extract_job_url(shortlisted_job)
                    reused_ai_row = (
                        ranking_ai_score_reuse_index.get(fingerprint_record["fingerprint"])
                        if ranking_reuse_enabled
                        else None
                    )
                    if reused_ai_row is not None and job_url:
                        reused_ai_scores_by_url[job_url] = {
                            **deepcopy(reused_ai_row),
                            "job_url": job_url,
                            "ai_score_input_fingerprint": fingerprint_record["fingerprint"],
                            "ai_score_reuse_status": "reused_exact_match",
                            "reuse_decision": build_reuse_decision(
                                decision="reused_exact_match",
                                reason_code="exact_fingerprint_match",
                                fingerprint=fingerprint_record["fingerprint"],
                                source_artifact_type="ranking_ai_score",
                            ),
                        }
                        continue
                    fresh_scoring_jobs.append(shortlisted_job)
                    if job_url:
                        fresh_ai_score_fingerprints[job_url] = fingerprint_record["fingerprint"]

                fresh_ai_scores = run_ai_scoring(
                    fresh_scoring_jobs,
                    candidate_summary,
                    config,
                    top_n=len(fresh_scoring_jobs),
                ) if fresh_scoring_jobs else []
                fresh_ai_scores_by_url: dict[str, dict[str, Any]] = {}
                for ai_row in fresh_ai_scores:
                    job_url = str(ai_row.get("job_url") or "")
                    fresh_ai_scores_by_url[job_url] = {
                        **ai_row,
                        "ai_score_input_fingerprint": fresh_ai_score_fingerprints.get(job_url),
                        "ai_score_reuse_status": "fresh_compute" if ranking_reuse_enabled else "reuse_disabled",
                        "reuse_decision": build_reuse_decision(
                            decision="fresh_compute" if ranking_reuse_enabled else "reuse_disabled",
                            reason_code="no_reusable_snapshot_match" if ranking_reuse_enabled else "stage_reuse_disabled",
                            fingerprint=fresh_ai_score_fingerprints.get(job_url),
                            source_artifact_type="ranking_ai_score",
                        ),
                    }

                ai_scores = []
                for shortlisted_job in ai_score_candidates:
                    job_url = extract_job_url(shortlisted_job)
                    score_row: dict[str, Any] | None = reused_ai_scores_by_url.get(job_url) or fresh_ai_scores_by_url.get(job_url)
                    if score_row is not None:
                        ai_scores.append(score_row)
                reused_ai_count = sum(
                    1 for row in ai_scores
                    if str(row.get("ai_score_reuse_status") or "") == "reused_exact_match"
                )
                fresh_ai_count = sum(
                    1 for row in ai_scores
                    if str(row.get("ai_score_reuse_status") or "") == "fresh_compute"
                )
                if reporter is not None:
                    reporter.emit(  # type: ignore[union-attr]
                        "layer3_ai_score",
                        "info",
                        f"AI scored: {len(ai_scores)} jobs",
                        _bounded_event_payload(
                            event_name="ranking_ai_scored",
                            event_family="summary",
                            source_stage="ranking",
                            event_status="completed",
                            output_snapshot={
                                "ai_scored_jobs": len(ai_scores),
                                "ranking_concurrency_effective": _ranking_stage_concurrency(config),
                            },
                            artifact_refs={"stage_id": "ranking"},
                        ),
                    )
                set_span_attributes(
                    {
                        "ai_score_candidates": len(ai_score_candidates),
                        "ai_scores": len(ai_scores),
                        "fresh_ai_scores": fresh_ai_count,
                        "reused_ai_scores": reused_ai_count,
                    }
                )

            with observe_span("pipeline.ranking", attributes={"run_id": run_id, "final_top_n": final_top_n}):
                ranking_inputs = build_ranking_features(shortlist, ai_scores, profile, config)
                ranked = rank_jobs(ranking_inputs, top_n=final_top_n)
                pipeline_store.store_final_ranking(ranked, config)
                if reporter is not None:
                    reporter.emit(  # type: ignore[union-attr]
                        "layer3_ranking",
                        "info",
                        f"Final ranking: top {len(ranked)} jobs",
                        _bounded_event_payload(
                            event_name="ranking_completed",
                            event_family="summary",
                            source_stage="ranking",
                            event_status="completed",
                            output_snapshot={
                                "ranked_jobs": len(ranked),
                                "ranking_concurrency_effective": _ranking_stage_concurrency(config),
                                "reused_ai_scores": reused_ai_count,
                                "fresh_ai_scores": fresh_ai_count,
                            },
                            artifact_refs={"stage_id": "ranking"},
                        ),
                    )
                state["ai_scores"] = ai_scores
                state["ranking_inputs"] = ranking_inputs
                state["ranked"] = ranked
                set_span_attributes(
                    {
                        "ranking_inputs": len(ranking_inputs),
                        "ranked_jobs": len(ranked),
                    }
                )
            stage_boundary_result = _handle_stage_boundary(
                run_id=run_id,
                last_completed_stage="ranking",
                stop_after_stage=stop_after_stage,
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
                stage_progress_callback=stage_progress_callback,
            )
            if stage_boundary_result is not None:
                return stage_boundary_result

        ai_scores = list(state["ai_scores"])
        ranking_inputs = list(state["ranking_inputs"])
        ranked = list(state["ranked"])

        enriched_by_url = {
            str(job.get("job_url") or ""): job
            for job in enriched
            if job.get("job_url")
        }
        ranked_jobs_for_cv = [
            _merge_ranked_job_with_enriched_context(job, enriched_by_url)
            for job in ranked
        ]
        cv_generation_prompt_runtime = _prompt_runtime_metadata(
            config,
            stage_id="cv_generation",
            prompt_key="structured_write",
        )
        cv_generation_model_value = get_cv_generation_model(config)
        cv_prompt_id_value = cv_generation_prompt_runtime["prompt_id"]
        cv_prompt_template_path_value = cv_generation_prompt_runtime["template_path"]
        cv_prompt_version_value = get_cv_generation_prompt_version(config)
        cv_acceptance_policy = get_cv_acceptance_policy(config)
        enabled_cv_sections = _cv_generation_enabled_sections(config)
        agentic_late_stage_enabled = _agentic_late_stage_enabled(config)
        cv_analysis_concurrency = _cv_analysis_stage_concurrency(config)
        if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("cv_analysis"):
            with observe_span("pipeline.cv_analysis", attributes={"run_id": run_id, "ranked_jobs": len(ranked_jobs_for_cv)}):
                cv_analysis_started_monotonic = time.monotonic()
                if reporter is not None:
                    reporter.emit(
                        "layer4_cv_analysis_invoked",
                        "info",
                        f"CV analysis invoked for {len(ranked_jobs_for_cv)} ranked job(s)",
                        _bounded_event_payload(
                            event_name="cv_analysis_invoked",
                            event_family="invocation",
                            source_stage="cv_analysis",
                            event_status="started",
                            fallback_used=False,
                            provenance={
                                "late_stage_mode": "agentic",
                                "cv_analysis_concurrency_effective": cv_analysis_concurrency,
                            },
                            input_snapshot={
                                "ranked_jobs": len(ranked_jobs_for_cv),
                                "cv_analysis_concurrency_configured": cv_analysis_concurrency,
                            },
                            output_snapshot={
                                "cv_analysis_concurrency_effective": cv_analysis_concurrency,
                            },
                            artifact_refs={"stage_id": "cv_analysis"},
                        ),
                    )  # type: ignore[union-attr]
                if cancellation_check and cancellation_check():
                    raise PipelineCancelled("Cancelled before CV analysis")
                cv_analysis_results = []
                cv_analysis_reuse_enabled = _reuse_stage_enabled(config, "cv_analysis")
                pending_agentic_analysis: list[
                    tuple[int, dict[str, Any], Future[dict[str, Any]], str, dict[str, Any]]
                ] = []
                agentic_executor: ThreadPoolExecutor | None = None
                for job in ranked_jobs_for_cv:
                    ranking_fit_label = _resolve_layer4_fit(job, gap_fit=None, config=config)
                    if ranking_fit_label == "skip":
                        logger.info(
                            "[run_id=%s] Blocking job %s before CV analysis (reranker fit=skip)",
                            run_id,
                            job.get("job_url"),
                        )
                        analysis_record = _build_cv_analysis_record(
                            job=job,
                            status=CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS,
                            analysis_input_fingerprint=None,
                            analysis_reuse_status="not_run_reranker_skip",
                            evidence_payload=[],
                            evidence_used=[],
                            evidence_selection_summary=None,
                            gap_summary=None,
                            fit_classification=ranking_fit_label,
                            error={
                                "stage": "reranker_fit",
                                "message": f"Blocked {job.get('job_url')} before CV analysis (reranker fit=skip)",
                            },
                        )
                        cv_analysis_results.append(analysis_record)
                        _emit_cv_analysis_item_observation(
                            run_id=run_id,
                            profile=profile,
                            job=job,
                            analysis_record=analysis_record,
                        )
                        cv_generation_debug_records.append(
                            _build_cv_generation_debug_record(
                                job=job,
                                status=CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS,
                                fit_classification=ranking_fit_label,
                                evidence_used=[],
                                evidence_selection_summary=None,
                                analysis_input_summary=_build_cv_generation_analysis_input_summary(job),
                                gap_summary=None,
                                structured_cv_initial=None,
                                validation_initial=None,
                                repair_attempt=dict(_EMPTY_REPAIR_ATTEMPT),
                                structured_cv_final=None,
                                markdown_final=None,
                                enabled_sections=enabled_cv_sections,
                                cv_generation_model=cv_generation_model_value,
                                runtime_provenance=None,
                                cv_prompt_id=cv_prompt_id_value,
                                cv_prompt_template_path=cv_prompt_template_path_value,
                                error=analysis_record.get("outcome_reason"),
                            )
                        )
                        continue
                    analysis_fingerprint_record = build_cv_analysis_input_fingerprint(profile, job, config)
                    analysis_input_components = _analysis_input_components(
                        dict(analysis_fingerprint_record.get("payload") or {}),
                    )
                    reused_analysis_record = (
                        cv_analysis_reuse_index.get(analysis_fingerprint_record["fingerprint"])
                        if cv_analysis_reuse_enabled
                        else None
                    )
                    if reused_analysis_record is not None:
                        analysis_record = {
                            **deepcopy(reused_analysis_record),
                            "job_url": str(job.get("job_url") or ""),
                            "job_title": extract_job_title(job),
                            "job_snapshot": dict(job),
                            "analysis_input_fingerprint": analysis_fingerprint_record["fingerprint"],
                            "analysis_input_components": dict(analysis_input_components),
                            "analysis_reuse_status": "reused_exact_match",
                            "reuse_decision": build_reuse_decision(
                                decision="reused_exact_match",
                                reason_code="exact_fingerprint_match",
                                fingerprint=analysis_fingerprint_record["fingerprint"],
                                source_artifact_type="cv_analysis",
                            ),
                        }
                        cv_analysis_results.append(analysis_record)
                        _emit_cv_analysis_item_observation(
                            run_id=run_id,
                            profile=profile,
                            job=job,
                            analysis_record=analysis_record,
                        )
                        reused_status = str(analysis_record.get("status") or "")
                        if reused_status in {"skipped_fit_gate", "analysis_failed"}:
                            debug_error = (
                                analysis_record.get("outcome_reason")
                                if reused_status == "skipped_fit_gate"
                                else analysis_record.get("error")
                            )
                            cv_generation_debug_records.append(
                                _build_cv_generation_debug_record(
                                    job=job,
                                    status=reused_status,
                                    fit_classification=analysis_record.get("fit_classification"),
                                    evidence_used=list(analysis_record.get("evidence_used") or []),
                                    evidence_selection_summary=analysis_record.get("evidence_selection_summary"),
                                    analysis_input_summary=_build_cv_generation_analysis_input_summary(job),
                                    gap_summary=analysis_record.get("gap_summary"),
                                    structured_cv_initial=None,
                                    validation_initial=None,
                                    repair_attempt=dict(_EMPTY_REPAIR_ATTEMPT),
                                    structured_cv_final=None,
                                    markdown_final=None,
                                    enabled_sections=enabled_cv_sections,
                                    cv_generation_model=cv_generation_model_value,
                                    runtime_provenance=None,
                                    cv_prompt_id=cv_prompt_id_value,
                                    cv_prompt_template_path=cv_prompt_template_path_value,
                                    error=debug_error if isinstance(debug_error, dict) else None,
                                )
                            )
                            if reporter is not None and reused_status == "analysis_failed":
                                reporter.emit(
                                    "layer4_cv_error",
                                    "error",
                                    f"CV analysis failed for {job.get('job_url')}: {debug_error}",
                                    _bounded_event_payload(
                                        event_name="cv_analysis_decision",
                                        event_family="decision",
                                        source_stage="cv_analysis",
                                        event_status="completed",
                                        job_url=str(job.get("job_url") or ""),
                                        deterministic_outcome="rejected",
                                        stage_owned_subreason=CV_ANALYSIS_FAILED_STATUS,
                                        input_snapshot={
                                            "ranking_fit_label": analysis_record.get("ranking_fit_label"),
                                        },
                                        output_snapshot={
                                            "error_stage": str(
                                                (debug_error or {}).get("stage") if isinstance(debug_error, dict) else ""
                                            ),
                                        },
                                        artifact_refs={"stage_id": "cv_analysis"},
                                    ),
                                )  # type: ignore[union-attr]
                        continue
                    evidence: list[dict[str, Any]] = []
                    evidence_selection_summary: dict[str, Any] = {}
                    gap: dict[str, Any] | None = None
                    fit = "skip"
                    try:
                        if agentic_late_stage_enabled:
                            if cv_analysis_concurrency > 1:
                                if agentic_executor is None:
                                    agentic_executor = ThreadPoolExecutor(max_workers=cv_analysis_concurrency)
                                future: Future[dict[str, Any]] = agentic_executor.submit(
                                    lambda pending_job: dict(run_agentic_cv_analysis(pending_job, profile, config)),
                                    job,
                                )
                                placeholder_index = len(cv_analysis_results)
                                cv_analysis_results.append({})
                                pending_agentic_analysis.append(
                                    (
                                        placeholder_index,
                                        dict(job),
                                        future,
                                        analysis_fingerprint_record["fingerprint"],
                                        dict(analysis_input_components),
                                    )
                                )
                                continue
                            analysis_record = _attach_analysis_input_components(
                                analysis_record=dict(run_agentic_cv_analysis(job, profile, config)),
                                job=job,
                                analysis_input_fingerprint=analysis_fingerprint_record["fingerprint"],
                                analysis_input_components=analysis_input_components,
                            )
                            cv_analysis_results.append(analysis_record)
                            _emit_cv_analysis_item_observation(
                                run_id=run_id,
                                profile=profile,
                                job=job,
                                analysis_record=analysis_record,
                            )
                            evidence = list(analysis_record.get("evidence_payload") or [])
                            evidence_selection_summary = dict(analysis_record.get("evidence_selection_summary") or {})
                            gap = analysis_record.get("gap_summary")
                            fit = str(analysis_record.get("fit_classification") or fit)
                            if str(analysis_record.get("status") or "") != "ready_for_generation":
                                cv_generation_debug_records.append(
                                    _build_cv_generation_debug_record(
                                        job=job,
                                        status=str(analysis_record.get("status") or "analysis_failed"),
                                        fit_classification=fit,
                                        evidence_used=analysis_record["evidence_used"],
                                        evidence_selection_summary=analysis_record.get("evidence_selection_summary"),
                                        analysis_input_summary=_build_cv_generation_analysis_input_summary(job),
                                        gap_summary=gap,
                                        structured_cv_initial=None,
                                        validation_initial=None,
                                        repair_attempt=dict(_EMPTY_REPAIR_ATTEMPT),
                                        structured_cv_final=None,
                                        markdown_final=None,
                                        enabled_sections=enabled_cv_sections,
                                        cv_generation_model=cv_generation_model_value,
                                        runtime_provenance=None,
                                        cv_prompt_id=cv_prompt_id_value,
                                        cv_prompt_template_path=cv_prompt_template_path_value,
                                        error=analysis_record.get("outcome_reason") or analysis_record["error"],
                                    )
                                )
                            continue
                        evidence_top_k = pipeline_int(config, "evidence_top_k", default=0)
                        evidence_bundle = retrieve_evidence_bundle(
                            profile,
                            job,
                            top_k=evidence_top_k,
                            config=config,
                        )
                        evidence = list(evidence_bundle.get("selected_evidence") or [])
                        evidence_selection_summary = _build_analysis_evidence_selection_summary(
                            evidence_bundle,
                            evidence,
                            fallback_used=False,
                        )
                        if not evidence:
                            evidence = retrieve_evidence(
                                profile,
                                job,
                                top_k=evidence_top_k,
                            )
                            if evidence:
                                evidence_selection_summary = _build_analysis_evidence_selection_summary(
                                    {
                                        **evidence_bundle,
                                        "selected_evidence_ids": [str(item.get("evidence_id") or "") for item in evidence],
                                        "merged_pool_size": max(
                                            len(evidence),
                                            int(evidence_selection_summary.get("merged_pool_size") or 0),
                                        ),
                                        "deduped_pool_size": max(
                                            len(evidence),
                                            int(evidence_selection_summary.get("deduped_pool_size") or 0),
                                        ),
                                        "unselected_top_candidates": list(
                                            evidence_selection_summary.get("unselected_top_candidates") or []
                                        ),
                                        "hybrid_alignment": evidence_selection_summary.get("hybrid_alignment") or {},
                                        "semantic_alignment": evidence_selection_summary.get("semantic_alignment") or {},
                                    },
                                    evidence,
                                    fallback_used=True,
                                )

                        gap = compute_gap(
                            required_skills=job.get("required_skills") or [],
                            candidate_skills=candidate_skill_names,
                            years_experience_min=job.get("years_experience_min"),
                            years_experience_max=job.get("years_experience_max"),
                            years_candidate=profile.get("years_experience"),
                            config=config,
                        )

                        fit = _resolve_layer4_fit(job, gap_fit=None, config=config)
                        if fit == "skip":
                            logger.info("[run_id=%s] Skipping job %s (fit=skip)", run_id, job.get("job_url"))
                            analysis_record = _build_cv_analysis_record(
                                job=job,
                                status="skipped_fit_gate",
                                analysis_input_fingerprint=analysis_fingerprint_record["fingerprint"],
                                analysis_input_components=analysis_input_components,
                                analysis_reuse_status="fresh_compute",
                                evidence_payload=evidence,
                                evidence_used=_build_debug_evidence_used(evidence),
                                evidence_selection_summary=evidence_selection_summary,
                                gap_summary=gap,
                                fit_classification=fit,
                                error={
                                    "stage": "fit_gate",
                                    "message": f"Skipped {job.get('job_url')} (fit=skip)",
                                },
                            )
                            cv_analysis_results.append(analysis_record)
                            _emit_cv_analysis_item_observation(
                                run_id=run_id,
                                profile=profile,
                                job=job,
                                analysis_record=analysis_record,
                            )
                            cv_generation_debug_records.append(
                                _build_cv_generation_debug_record(
                                    job=job,
                                    status="skipped_fit_gate",
                                    fit_classification=fit,
                                    evidence_used=analysis_record["evidence_used"],
                                    evidence_selection_summary=analysis_record.get("evidence_selection_summary"),
                                    analysis_input_summary=_build_cv_generation_analysis_input_summary(job),
                                    gap_summary=gap,
                                    structured_cv_initial=None,
                                    validation_initial=None,
                                    repair_attempt=dict(_EMPTY_REPAIR_ATTEMPT),
                                    structured_cv_final=None,
                                    markdown_final=None,
                                    enabled_sections=enabled_cv_sections,
                                    cv_generation_model=cv_generation_model_value,
                                    runtime_provenance=None,
                                    cv_prompt_id=cv_prompt_id_value,
                                    cv_prompt_template_path=cv_prompt_template_path_value,
                                    error=analysis_record.get("outcome_reason") or analysis_record["error"],
                                )
                            )
                            continue

                        analysis_record = _build_cv_analysis_record(
                            job=job,
                            status="ready_for_generation",
                            analysis_input_fingerprint=analysis_fingerprint_record["fingerprint"],
                            analysis_input_components=analysis_input_components,
                            analysis_reuse_status="fresh_compute",
                            evidence_payload=evidence,
                            evidence_used=_build_debug_evidence_used(evidence),
                            evidence_selection_summary=evidence_selection_summary,
                            gap_summary=gap,
                            fit_classification=fit,
                            error=None,
                        )
                        cv_analysis_results.append(analysis_record)
                        _emit_cv_analysis_item_observation(
                            run_id=run_id,
                            profile=profile,
                            job=job,
                            analysis_record=analysis_record,
                        )
                    except Exception as exc:
                        logger.error("[run_id=%s] CV analysis failed for %s: %s", run_id, job.get("job_url"), exc)
                        analysis_record = _build_cv_analysis_record(
                            job=job,
                            status="analysis_failed",
                            analysis_input_fingerprint=analysis_fingerprint_record["fingerprint"],
                            analysis_input_components=analysis_input_components,
                            analysis_reuse_status="fresh_compute",
                            evidence_payload=evidence,
                            evidence_used=_build_debug_evidence_used(evidence),
                            evidence_selection_summary=evidence_selection_summary,
                            gap_summary=gap,
                            fit_classification=fit if fit else None,
                            error={
                                "stage": "analysis",
                                "message": str(exc),
                            },
                        )
                        cv_analysis_results.append(analysis_record)
                        _emit_cv_analysis_item_observation(
                            run_id=run_id,
                            profile=profile,
                            job=job,
                            analysis_record=analysis_record,
                        )
                        cv_generation_debug_records.append(
                            _build_cv_generation_debug_record(
                                job=job,
                                status="analysis_failed",
                                fit_classification=analysis_record.get("fit_classification"),
                                evidence_used=analysis_record["evidence_used"],
                                evidence_selection_summary=analysis_record.get("evidence_selection_summary"),
                                analysis_input_summary=_build_cv_generation_analysis_input_summary(job),
                                gap_summary=gap,
                                structured_cv_initial=None,
                                validation_initial=None,
                                repair_attempt=dict(_EMPTY_REPAIR_ATTEMPT),
                                structured_cv_final=None,
                                markdown_final=None,
                                enabled_sections=enabled_cv_sections,
                                cv_generation_model=cv_generation_model_value,
                                runtime_provenance=None,
                                cv_prompt_id=cv_prompt_id_value,
                                cv_prompt_template_path=cv_prompt_template_path_value,
                                error=analysis_record["error"],
                            )
                        )
                        if reporter is not None:
                            reporter.emit(
                                "layer4_cv_error",
                                "error",
                                f"CV analysis failed for {job.get('job_url')}: {exc}",
                                _bounded_event_payload(
                                    event_name="cv_analysis_decision",
                                    event_family="decision",
                                    source_stage="cv_analysis",
                                    event_status="completed",
                                    job_url=str(job.get("job_url") or ""),
                                    deterministic_outcome="rejected",
                                    stage_owned_subreason=CV_ANALYSIS_FAILED_STATUS,
                                    input_snapshot={
                                        "ranking_fit_label": analysis_record.get("ranking_fit_label"),
                                    },
                                    output_snapshot={
                                        "error_stage": str(analysis_record["error"].get("stage") or ""),
                                    },
                                    artifact_refs={"stage_id": "cv_analysis"},
                                ),
                            )  # type: ignore[union-attr]
                        continue
                if agentic_executor is not None:
                    agentic_executor.shutdown(wait=True)
                for (
                    placeholder_index,
                    pending_job,
                    future,
                    analysis_input_fingerprint,
                    analysis_input_components,
                ) in pending_agentic_analysis:
                    analysis_record = _attach_analysis_input_components(
                        analysis_record=dict(future.result()),
                        job=pending_job,
                        analysis_input_fingerprint=analysis_input_fingerprint,
                        analysis_input_components=analysis_input_components,
                    )
                    cv_analysis_results[placeholder_index] = analysis_record
                    _emit_cv_analysis_item_observation(
                        run_id=run_id,
                        profile=profile,
                        job=pending_job,
                        analysis_record=analysis_record,
                    )
                    evidence = list(analysis_record.get("evidence_payload") or [])
                    evidence_selection_summary = dict(analysis_record.get("evidence_selection_summary") or {})
                    gap = analysis_record.get("gap_summary")
                    fit = str(analysis_record.get("fit_classification") or "skip")
                    if str(analysis_record.get("status") or "") != "ready_for_generation":
                        cv_generation_debug_records.append(
                            _build_cv_generation_debug_record(
                                job=pending_job,
                                status=str(analysis_record.get("status") or "analysis_failed"),
                                fit_classification=fit,
                                evidence_used=analysis_record["evidence_used"],
                                evidence_selection_summary=evidence_selection_summary,
                                analysis_input_summary=_build_cv_generation_analysis_input_summary(pending_job),
                                gap_summary=gap,
                                structured_cv_initial=None,
                                validation_initial=None,
                                repair_attempt=dict(_EMPTY_REPAIR_ATTEMPT),
                                structured_cv_final=None,
                                markdown_final=None,
                                enabled_sections=enabled_cv_sections,
                                cv_generation_model=cv_generation_model_value,
                                runtime_provenance=None,
                                cv_prompt_id=cv_prompt_id_value,
                                cv_prompt_template_path=cv_prompt_template_path_value,
                                error=analysis_record.get("outcome_reason") or analysis_record["error"],
                            )
                        )
            if reporter is not None:
                fresh_analysis_reuse_reason_counts: dict[str, int] = {}
                fresh_overlap_urls = 0
                fresh_no_overlap_urls = 0
                for record in cv_analysis_results:
                    if str(record.get("analysis_reuse_status") or "") != "fresh_compute":
                        continue
                    if str(record.get("status") or "") == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS:
                        continue
                    job_url = str(record.get("job_url") or "").strip()
                    prior_records_for_url = list(cv_analysis_reuse_by_url.get(job_url) or [])
                    if prior_records_for_url:
                        fresh_overlap_urls += 1
                    else:
                        fresh_no_overlap_urls += 1
                    reason = _cv_analysis_reuse_mismatch_reason(
                        current_record=record,
                        prior_records_for_url=prior_records_for_url,
                    )
                    fresh_analysis_reuse_reason_counts[reason] = int(
                        fresh_analysis_reuse_reason_counts.get(reason) or 0
                    ) + 1
                blocked_by_reranker_diagnostics = [
                    {
                        "job_url": str(record.get("job_url") or ""),
                        "ranking_fit_label": str(record.get("ranking_fit_label") or ""),
                        "fit_classification": str(record.get("fit_classification") or ""),
                        "ai_score": (record.get("job_snapshot") or {}).get("ai_score"),
                        "fit_label_source": str(((record.get("job_snapshot") or {}).get("fit_label_source")) or ""),
                    }
                    for record in cv_analysis_results
                    if str(record.get("status") or "") == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS
                ][:10]
                reporter.emit(
                    "layer4_cv_analysis",
                    "info",
                    (
                        "CV analysis complete: "
                        f"{sum(1 for record in cv_analysis_results if str(record.get('status') or '') == 'ready_for_generation')} ready, "
                        f"{sum(1 for record in cv_analysis_results if str(record.get('status') or '') == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS)} blocked by reranker, "
                        f"{sum(1 for record in cv_analysis_results if str(record.get('status') or '') == 'skipped_fit_gate')} skipped, "
                        f"{sum(1 for record in cv_analysis_results if str(record.get('status') or '') == 'analysis_failed')} failed"
                    ),
                    _bounded_event_payload(
                        event_name="cv_analysis_decision",
                        event_family="decision",
                        source_stage="cv_analysis",
                        event_status="completed",
                        deterministic_outcome=None,
                        stage_owned_subreason="stage_summary",
                        input_snapshot={
                            "ranked_jobs": len(ranked),
                            "cv_analysis_concurrency_configured": cv_analysis_concurrency,
                        },
                        output_snapshot={
                            "reused_analysis_rows": sum(
                                1 for record in cv_analysis_results
                                if str(record.get("analysis_reuse_status") or "") == "reused_exact_match"
                            ),
                            "fresh_analysis_rows": sum(
                                1 for record in cv_analysis_results
                                if str(record.get("analysis_reuse_status") or "") == "fresh_compute"
                            ),
                            "ready_for_generation": sum(
                                1 for record in cv_analysis_results
                                if str(record.get("status") or "") == CV_ANALYSIS_READY_FOR_GENERATION_STATUS
                            ),
                            "blocked_by_reranker_fit": sum(
                                1 for record in cv_analysis_results
                                if str(record.get("status") or "") == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS
                            ),
                            "skipped_fit_gate": sum(
                                1 for record in cv_analysis_results
                                if str(record.get("status") or "") == CV_ANALYSIS_SKIPPED_FIT_GATE_STATUS
                            ),
                            "analysis_failed": sum(
                                1 for record in cv_analysis_results
                                if str(record.get("status") or "") == CV_ANALYSIS_FAILED_STATUS
                            ),
                            "fresh_analysis_reuse_mismatch_reasons": dict(fresh_analysis_reuse_reason_counts),
                            "fresh_analysis_overlap_urls": fresh_overlap_urls,
                            "fresh_analysis_no_overlap_urls": fresh_no_overlap_urls,
                            "cv_analysis_concurrency_effective": cv_analysis_concurrency,
                        },
                        artifact_refs={"stage_id": "cv_analysis"},
                        latency_ms=int((time.monotonic() - cv_analysis_started_monotonic) * 1000),
                    ),
                )  # type: ignore[union-attr]
                if blocked_by_reranker_diagnostics:
                    reporter.emit(
                        "layer4_cv_analysis_blocked_details",
                        "info",
                        f"Blocked by reranker diagnostics: {len(blocked_by_reranker_diagnostics)} job(s)",
                        _bounded_event_payload(
                            event_name="cv_analysis_blocked_diagnostics",
                            event_family="debug",
                            source_stage="cv_analysis",
                            event_status="completed",
                            deterministic_outcome="rejected",
                            stage_owned_subreason=CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS,
                            input_snapshot={
                                "fit_label_thresholds": dict(config.get("fit_label_thresholds") or {}),
                            },
                            output_snapshot={
                                "blocked_jobs": blocked_by_reranker_diagnostics,
                            },
                            artifact_refs={"stage_id": "cv_analysis"},
                        ),
                    )  # type: ignore[union-attr]
            set_span_attributes(
                {
                    "cv_analysis_records": len(cv_analysis_results),
                    "cv_analysis_ready": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == CV_ANALYSIS_READY_FOR_GENERATION_STATUS
                    ),
                    "cv_analysis_failed": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == CV_ANALYSIS_FAILED_STATUS
                    ),
                    "cv_analysis_concurrency_effective": cv_analysis_concurrency,
                }
            )
            state["cv_analysis_results"] = cv_analysis_results
            state["cv_generation_debug_records"] = cv_generation_debug_records
            stage_boundary_result = _handle_stage_boundary(
                run_id=run_id,
                last_completed_stage="cv_analysis",
                stop_after_stage=stop_after_stage,
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
                stage_progress_callback=stage_progress_callback,
            )
            if stage_boundary_result is not None:
                return stage_boundary_result

        cv_analysis_results = list(state["cv_analysis_results"])
        if cancellation_check and cancellation_check():
            raise PipelineCancelled("Cancelled before CV generation")
        generation_ready_records = [
            record for record in cv_analysis_results
            if str(record.get("status") or "") == "ready_for_generation"
        ]
        if generation_ready_records and not agentic_late_stage_enabled:
            try:
                validate_cv_generation_routing_ready(config)
            except RuntimeError as exc:
                preflight_error = str(exc)
                for analysis_record in generation_ready_records:
                    job = dict(analysis_record.get("job_snapshot") or {})
                    fit = str(analysis_record.get("fit_classification") or "skip")
                    evidence = list(analysis_record.get("evidence_payload") or [])
                    evidence_used = _build_debug_evidence_used(evidence)
                    evidence_selection_summary = dict(analysis_record.get("evidence_selection_summary") or {})
                    analysis_input_summary = _build_cv_generation_analysis_input_summary(job)
                    runtime_provenance = _non_agentic_cv_generation_runtime_provenance(
                        config,
                        cv_generation_model_value,
                    )
                    debug_record = _build_cv_generation_debug_record(
                        job=job,
                        status="generation_failed",
                        fit_classification=fit,
                        evidence_used=evidence_used,
                        evidence_selection_summary=evidence_selection_summary,
                        analysis_input_summary=analysis_input_summary,
                        gap_summary=analysis_record.get("gap_summary"),
                        structured_cv_initial=None,
                        validation_initial=None,
                        repair_attempt=dict(_EMPTY_REPAIR_ATTEMPT),
                        structured_cv_final=None,
                        markdown_final=None,
                        enabled_sections=enabled_cv_sections,
                        cv_generation_model=cv_generation_model_value,
                        runtime_provenance=runtime_provenance,
                        cv_prompt_id=cv_prompt_id_value,
                        cv_prompt_template_path=cv_prompt_template_path_value,
                        error={
                            "stage": "configuration",
                            "message": preflight_error,
                        },
                        agentic_live_trace=None,
                    )
                    cv_generation_debug_records.append(debug_record)
                    _emit_cv_generation_item_observation(
                        run_id=run_id,
                        analysis_record=analysis_record,
                        debug_record=debug_record,
                    )
                if reporter is not None:
                    reporter.emit(
                        "layer4_cv_error",
                        "error",
                        f"CV generation preflight failed: {preflight_error}",
                        _bounded_event_payload(
                            event_name="cv_generation_preflight_failed",
                            event_family="decision",
                            source_stage="cv_generation",
                            event_status="completed",
                            deterministic_outcome="rejected",
                            stage_owned_subreason="generation_failed",
                            output_snapshot={"error_stage": "configuration"},
                            artifact_refs={"stage_id": "cv_generation"},
                        ),
                    )  # type: ignore[union-attr]
                generation_ready_records = []
        indexed_generation_ready_records = list(enumerate(generation_ready_records))
        configured_cv_generation_concurrency = get_stage_runtime_concurrency(
            config,
            stage="cv_generation",
            default=1,
        )
        generation_total = len(indexed_generation_ready_records)
        cv_generation_reuse_enabled = _reuse_stage_enabled(config, "cv_generation")
        cv_generation_fingerprint_by_index: dict[int, str] = {}
        cv_generation_reuse_index: dict[str, dict[str, Any]] = {}
        if cv_generation_reuse_enabled and indexed_generation_ready_records:
            for generation_index, analysis_record in indexed_generation_ready_records:
                fingerprint_record = _build_cv_generation_input_fingerprint(
                    analysis_record=analysis_record,
                    config=config,
                    cv_prompt_id=cv_prompt_id_value,
                    cv_prompt_template_path=cv_prompt_template_path_value,
                    cv_prompt_version=cv_prompt_version_value,
                    cv_generation_model=cv_generation_model_value,
                    enabled_sections=enabled_cv_sections,
                    cv_acceptance_policy=cv_acceptance_policy,
                )
                cv_generation_fingerprint_by_index[generation_index] = str(fingerprint_record["fingerprint"])
            cv_generation_reuse_index = lookup_reusable_cv_versions(
                list(cv_generation_fingerprint_by_index.values()),
                config,
                limit=max(500, len(cv_generation_fingerprint_by_index) * 3),
            )

        def _prepare_cv_generation_work_item(
            *,
            generation_index: int,
            analysis_record: dict[str, Any],
        ) -> dict[str, Any]:
            job = dict(analysis_record.get("job_snapshot") or {})
            evidence = list(analysis_record.get("evidence_payload") or [])
            evidence_used = _build_debug_evidence_used(evidence)
            evidence_selection_summary = dict(analysis_record.get("evidence_selection_summary") or {})
            analysis_input_summary = _build_cv_generation_analysis_input_summary(job)
            return {
                "job": job,
                "evidence": evidence,
                "evidence_used": evidence_used,
                "evidence_selection_summary": evidence_selection_summary,
                "analysis_input_summary": analysis_input_summary,
                "analysis_grounding": _build_validation_grounding_payload(
                    evidence_payload=evidence,
                    evidence_used=evidence_used,
                    evidence_selection_summary=evidence_selection_summary,
                    analysis_input_summary=analysis_input_summary,
                ),
                "gap": analysis_record.get("gap_summary"),
                "fit": str(analysis_record.get("fit_classification") or "skip"),
                "generation_worker_slot": generation_index % configured_cv_generation_concurrency,
            }

        def _initialize_cv_generation_runtime_state(work_item: dict[str, Any]) -> dict[str, Any]:
            job = dict(work_item["job"])
            # Agentic path resolves runtime provenance from generation result.
            # Avoid non-agentic routing readiness checks here, which can require
            # provider credentials not needed for mocked/agentic execution paths.
            job_runtime_provenance: dict[str, Any] | None = (
                None
                if agentic_late_stage_enabled
                else _non_agentic_cv_generation_runtime_provenance(config, cv_generation_model_value)
            )
            return {
                "job": job,
                "evidence": list(work_item["evidence"]),
                "evidence_used": list(work_item["evidence_used"]),
                "evidence_selection_summary": dict(work_item["evidence_selection_summary"]),
                "analysis_input_summary": dict(work_item["analysis_input_summary"]),
                "analysis_grounding": cast(AnalysisGroundingPayload, work_item["analysis_grounding"]),
                "gap": work_item["gap"],
                "fit": str(work_item["fit"] or "skip"),
                "structured_cv_initial": None,
                "validation_initial": None,
                "repair_attempt": dict(_EMPTY_REPAIR_ATTEMPT),
                "structured_cv_final": None,
                "markdown_final": None,
                "job_runtime_provenance": job_runtime_provenance,
                "job_cv_generation_model_value": _resolved_cv_generation_model(
                    cv_generation_model_value,
                    job_runtime_provenance,
                ),
                "job_agentic_live_trace": None,
                "generation_attempt_count": 1,
                "generation_started_at_iso": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "generation_finished_at_iso": None,
                "generation_worker_slot": int(work_item["generation_worker_slot"]),
            }

        def _emit_cv_generation_started_event(
            *,
            generation_index: int,
            generation_total: int,
            state: dict[str, Any],
        ) -> None:
            if reporter is None:
                return
            job = cast(dict[str, Any], state["job"])
            fit = str(state["fit"] or "skip")
            generation_started_at_iso = str(state["generation_started_at_iso"])
            generation_worker_slot = int(state["generation_worker_slot"])
            reporter.emit(
                "layer4_cv_generation_started",
                "info",
                f"CV generation started for {job.get('job_url')} [item {generation_index + 1}/{generation_total}]",
                _bounded_event_payload(
                    event_name="cv_generation_started",
                    event_family="invocation",
                    source_stage="cv_generation",
                    event_status="started",
                    job_url=str(job.get("job_url") or ""),
                    fallback_used=False,
                    input_snapshot={
                        "ranking_fit_label": _authoritative_ranking_fit_label(job, fit),
                        "fit_classification": fit,
                        "generation_index": int(generation_index),
                        "generation_total": int(generation_total),
                    },
                    output_snapshot={
                        "configured_concurrency": int(configured_cv_generation_concurrency),
                        "cv_generation_concurrency_effective": int(configured_cv_generation_concurrency),
                        "worker_slot": int(generation_worker_slot),
                        "started_at": generation_started_at_iso,
                    },
                    artifact_refs={"stage_id": "cv_generation"},
                ),
            )  # type: ignore[union-attr]

        def _emit_cv_generation_result_event(
            *,
            state: dict[str, Any],
            status: str,
            attempt_count: int = 1,
            retry_count: int = 0,
            latency_ms: int | None = None,
            usage: dict[str, Any] | None = None,
            cost: dict[str, Any] | None = None,
            reuse_status: str | None = None,
            reused_cv_version_id: str | None = None,
            cv_generation_input_fingerprint: str | None = None,
            review_required_reason_code: str | None = None,
            validation_evidence_fingerprint: str | None = None,
        ) -> None:
            if reporter is None:
                return
            effective_reuse_status = str(reuse_status or "").strip() or "fresh_compute"
            job = cast(dict[str, Any], state["job"])
            fit = str(state["fit"] or "skip")
            generation_started_at_iso = str(state["generation_started_at_iso"])
            generation_worker_slot = int(state["generation_worker_slot"])
            generation_finished_at_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            reporter.emit(
                "layer4_cv_generation_result",
                "info",
                f"CV generation result for {job.get('job_url')}: {status}",
                _bounded_event_payload(
                    event_name="cv_generation_result",
                    event_family="decision",
                    source_stage="cv_generation",
                    event_status="completed",
                    job_url=str(job.get("job_url") or ""),
                    deterministic_outcome=str(status or ""),
                    fallback_used=False,
                    input_snapshot={
                        "ranking_fit_label": _authoritative_ranking_fit_label(job, fit),
                        "fit_classification": fit,
                    },
                    output_snapshot={
                        "status": str(status or ""),
                        "reuse_status": effective_reuse_status,
                        "reused_cv_version_id": str(reused_cv_version_id or ""),
                        "cv_generation_input_fingerprint": str(cv_generation_input_fingerprint or ""),
                        "review_required_reason_code": str(review_required_reason_code or ""),
                        "validation_evidence_fingerprint": str(validation_evidence_fingerprint or ""),
                        "attempt_count": int(attempt_count),
                        "retry_count": int(retry_count),
                        "configured_concurrency": int(configured_cv_generation_concurrency),
                        "cv_generation_concurrency_effective": int(configured_cv_generation_concurrency),
                        "worker_slot": int(generation_worker_slot),
                        "started_at": generation_started_at_iso,
                        "finished_at": generation_finished_at_iso,
                        "status_flat": str(status or ""),
                        "reuse_status_flat": effective_reuse_status,
                        "reused_cv_version_id_flat": str(reused_cv_version_id or ""),
                        "cv_generation_input_fingerprint_flat": str(cv_generation_input_fingerprint or ""),
                        "review_required_reason_code_flat": str(review_required_reason_code or ""),
                        "validation_evidence_fingerprint_flat": str(validation_evidence_fingerprint or ""),
                    },
                    artifact_refs={"stage_id": "cv_generation"},
                    latency_ms=latency_ms,
                    usage=usage,
                    cost=cost,
                ),
            )  # type: ignore[union-attr]

        def _emit_cv_generation_invoked_event(
            *,
            state: dict[str, Any],
            cv_generation_model_value: str | None,
        ) -> None:
            if reporter is None:
                return
            job = cast(dict[str, Any], state["job"])
            fit = str(state["fit"] or "skip")
            reporter.emit(
                "layer4_cv_generation_invoked",
                "info",
                f"CV generation invoked for {job.get('job_url')}",
                _bounded_event_payload(
                    event_name="cv_generation_invoked",
                    event_family="invocation",
                    source_stage="cv_generation",
                    event_status="started",
                    job_url=str(job.get("job_url") or ""),
                    fallback_used=False,
                    provenance={
                        "cv_generation_model": cv_generation_model_value,
                    },
                    input_snapshot={
                        "ranking_fit_label": _authoritative_ranking_fit_label(job, fit),
                        "fit_classification": fit,
                    },
                    artifact_refs={"stage_id": "cv_generation"},
                ),
            )  # type: ignore[union-attr]

        def _handle_cv_generation_validation_failed(
            *,
            state: dict[str, Any],
            analysis_record: dict[str, Any],
            validation: dict[str, Any],
            latency_ms: int,
            cv_prompt_id_value: str,
            cv_prompt_template_path_value: str,
            enabled_cv_sections: list[str],
            run_id: str,
            cv_generation_input_fingerprint: str | None,
        ) -> bool:
            validation_payload = dict(validation or {})
            if validation_payload.get("valid", False):
                return False
            job = cast(dict[str, Any], state["job"])
            fit = str(state["fit"] or "skip")
            evidence_used = cast(list[dict[str, Any]], state["evidence_used"])
            evidence_selection_summary = cast(dict[str, Any], state["evidence_selection_summary"])
            analysis_input_summary = cast(dict[str, Any], state["analysis_input_summary"])
            gap = state["gap"]
            structured_cv_initial = cast(dict[str, Any] | None, state["structured_cv_initial"])
            validation_initial = cast(dict[str, Any] | None, state["validation_initial"])
            repair_attempt = cast(dict[str, Any], state["repair_attempt"])
            job_cv_generation_model_value = cast(str | None, state["job_cv_generation_model_value"])
            job_runtime_provenance = cast(dict[str, Any] | None, state["job_runtime_provenance"])
            job_agentic_live_trace = cast(dict[str, Any] | None, state["job_agentic_live_trace"])

            _emit_cv_generation_result_event(
                state=state,
                status="validation_failed",
                attempt_count=1,
                retry_count=0,
                latency_ms=latency_ms,
                review_required_reason_code=_review_required_reason_code_value(
                    _normalize_review_required_reason_code(
                        status="validation_failed",
                        error={"stage": "validation", "message": f"CV validation failed for {job.get('job_url')}"},
                        validation_initial=validation_initial,
                    )
                ),
                validation_evidence_fingerprint=_build_validation_evidence_fingerprint(
                    status="validation_failed",
                    validation_initial=validation_initial,
                    error={"stage": "validation", "message": f"CV validation failed for {job.get('job_url')}"},
                ),
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
            )
            failure_details: dict[str, Any] = {
                "missing_sections": validation_payload.get("missing_sections") or [],
                "grounding_violations": validation_payload.get("grounding_violations") or [],
                "deterministic_grounding_violations": validation_payload.get("deterministic_grounding_violations") or [],
                "semantic_grounding_violations": validation_payload.get("semantic_grounding_violations") or [],
                "skill_violations": validation_payload.get("skill_violations") or [],
                "markdown_quality_blocking_issues": validation_payload.get("markdown_quality_blocking_issues") or [],
                "markdown_quality_review_flags": validation_payload.get("markdown_quality_review_flags") or [],
                "warnings": validation_payload.get("warnings") or [],
                "support_source_summary": validation_payload.get("support_source_summary") or {},
            }
            logger.warning(
                "[run_id=%s] CV for %s failed validation: %s",
                run_id,
                job.get("job_url"),
                failure_details,
            )
            validation_failed_debug_record = _build_cv_generation_debug_record(
                job=job,
                status="validation_failed",
                fit_classification=fit,
                evidence_used=evidence_used,
                evidence_selection_summary=evidence_selection_summary,
                analysis_input_summary=analysis_input_summary,
                gap_summary=gap,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                validation_final=validation_payload if validation_payload else None,
                repair_attempt=repair_attempt,
                structured_cv_final=None,
                markdown_final=None,
                enabled_sections=enabled_cv_sections,
                cv_generation_model=job_cv_generation_model_value,
                runtime_provenance=job_runtime_provenance,
                cv_prompt_id=cv_prompt_id_value,
                cv_prompt_template_path=cv_prompt_template_path_value,
                error={
                    "stage": "validation",
                    "message": f"CV validation failed for {job.get('job_url')}",
                },
                agentic_live_trace=job_agentic_live_trace,
            )
            cv_generation_debug_records.append(validation_failed_debug_record)
            _emit_cv_generation_item_observation(
                run_id=run_id,
                analysis_record=analysis_record,
                debug_record=validation_failed_debug_record,
            )
            if reporter is not None:
                reporter.emit(
                    "layer4_cv_validation_failed",
                    "warning",
                    f"CV validation failed for {job.get('job_url')}",
                    _bounded_event_payload(
                        event_name="cv_generation_decision",
                        event_family="decision",
                        source_stage="cv_generation",
                        event_status="completed",
                        job_url=str(job.get("job_url") or ""),
                        deterministic_outcome="rejected",
                        stage_owned_subreason="validation_failed",
                        provenance={
                            "cv_generation_model": job_cv_generation_model_value,
                        },
                        input_snapshot={
                            "ranking_fit_label": _authoritative_ranking_fit_label(job, fit),
                            "fit_classification": fit,
                            "selected_evidence_count": len(evidence_used),
                        },
                        output_snapshot={
                            "validation_status": "failed",
                            "missing_sections": list(validation_payload.get("missing_sections") or []),
                            "grounding_violations": list(validation_payload.get("grounding_violations") or []),
                            "deterministic_grounding_violations": list(
                                validation_payload.get("deterministic_grounding_violations") or []
                            ),
                            "semantic_grounding_violations": list(
                                validation_payload.get("semantic_grounding_violations") or []
                            ),
                            "skill_violations": list(validation_payload.get("skill_violations") or []),
                            "markdown_quality_blocking_issues": list(
                                validation_payload.get("markdown_quality_blocking_issues") or []
                            ),
                            "markdown_quality_review_flags": list(
                                validation_payload.get("markdown_quality_review_flags") or []
                            ),
                            "warnings": list(validation_payload.get("warnings") or []),
                        },
                        artifact_refs={"stage_id": "cv_generation"},
                    ),
                )  # type: ignore[union-attr]
            return True

        def _handle_cv_generation_markdown_review_required(
            *,
            state: dict[str, Any],
            analysis_record: dict[str, Any],
            markdown_review_reason: str | None,
            structured_cv: dict[str, Any] | None,
            cv_markdown: str,
            latency_ms: int,
            cv_prompt_id_value: str,
            cv_prompt_template_path_value: str,
            enabled_cv_sections: list[str],
            run_id: str,
            cv_generation_input_fingerprint: str | None,
        ) -> bool:
            if not markdown_review_reason:
                return False
            job = cast(dict[str, Any], state["job"])
            fit = str(state["fit"] or "skip")
            evidence_used = cast(list[dict[str, Any]], state["evidence_used"])
            evidence_selection_summary = cast(dict[str, Any], state["evidence_selection_summary"])
            analysis_input_summary = cast(dict[str, Any], state["analysis_input_summary"])
            gap = state["gap"]
            structured_cv_initial = cast(dict[str, Any] | None, state["structured_cv_initial"])
            validation_initial = cast(dict[str, Any] | None, state["validation_initial"])
            repair_attempt = cast(dict[str, Any], state["repair_attempt"])
            job_cv_generation_model_value = cast(str | None, state["job_cv_generation_model_value"])
            job_runtime_provenance = cast(dict[str, Any] | None, state["job_runtime_provenance"])
            job_agentic_live_trace = cast(dict[str, Any] | None, state["job_agentic_live_trace"])

            _emit_cv_generation_result_event(
                state=state,
                status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                attempt_count=1,
                retry_count=0,
                latency_ms=latency_ms,
                review_required_reason_code=_review_required_reason_code_value(
                    _normalize_review_required_reason_code(
                        status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                        error={
                            "stage": "markdown_quality_review",
                            "message": str(markdown_review_reason or ""),
                        },
                        validation_initial=validation_initial,
                    )
                ),
                validation_evidence_fingerprint=_build_validation_evidence_fingerprint(
                    status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                    validation_initial=validation_initial,
                    error={
                        "stage": "markdown_quality_review",
                        "message": str(markdown_review_reason or ""),
                    },
                ),
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
            )
            review_required_debug_record = _build_cv_generation_debug_record(
                job=job,
                status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                fit_classification=fit,
                evidence_used=evidence_used,
                evidence_selection_summary=evidence_selection_summary,
                analysis_input_summary=analysis_input_summary,
                gap_summary=gap,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                repair_attempt=repair_attempt,
                structured_cv_final=structured_cv,
                markdown_final=cv_markdown,
                enabled_sections=enabled_cv_sections,
                cv_generation_model=job_cv_generation_model_value,
                runtime_provenance=job_runtime_provenance,
                cv_prompt_id=cv_prompt_id_value,
                cv_prompt_template_path=cv_prompt_template_path_value,
                error={
                    "stage": "markdown_quality_review",
                    "message": markdown_review_reason,
                },
                agentic_live_trace=job_agentic_live_trace,
            )
            cv_generation_debug_records.append(review_required_debug_record)
            _emit_cv_generation_item_observation(
                run_id=run_id,
                analysis_record=analysis_record,
                debug_record=review_required_debug_record,
            )
            return True

        def _handle_cv_generation_policy_review_required(
            *,
            state: dict[str, Any],
            analysis_record: dict[str, Any],
            policy_pass: bool,
            policy_reason_code: str,
            policy_note: str,
            structured_cv: dict[str, Any] | None,
            cv_markdown: str,
            latency_ms: int,
            cv_prompt_id_value: str,
            cv_prompt_template_path_value: str,
            enabled_cv_sections: list[str],
            run_id: str,
            cv_generation_input_fingerprint: str | None,
        ) -> bool:
            if policy_pass:
                return False
            job = cast(dict[str, Any], state["job"])
            fit = str(state["fit"] or "skip")
            evidence_used = cast(list[dict[str, Any]], state["evidence_used"])
            evidence_selection_summary = cast(dict[str, Any], state["evidence_selection_summary"])
            analysis_input_summary = cast(dict[str, Any], state["analysis_input_summary"])
            gap = state["gap"]
            structured_cv_initial = cast(dict[str, Any] | None, state["structured_cv_initial"])
            validation_initial = cast(dict[str, Any] | None, state["validation_initial"])
            repair_attempt = cast(dict[str, Any], state["repair_attempt"])
            job_cv_generation_model_value = cast(str | None, state["job_cv_generation_model_value"])
            job_runtime_provenance = cast(dict[str, Any] | None, state["job_runtime_provenance"])
            job_agentic_live_trace = cast(dict[str, Any] | None, state["job_agentic_live_trace"])

            _emit_cv_generation_result_event(
                state=state,
                status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                attempt_count=1,
                retry_count=0,
                latency_ms=latency_ms,
                review_required_reason_code=_review_required_reason_code_value(
                    _normalize_review_required_reason_code(
                        status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                        error={
                            "stage": "policy_acceptance",
                            "message": (
                                f"Policy acceptance blocked ({policy_reason_code}): {policy_note}. "
                                f"Manual review required."
                            ),
                        },
                        validation_initial=validation_initial,
                    )
                ),
                validation_evidence_fingerprint=_build_validation_evidence_fingerprint(
                    status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                    validation_initial=validation_initial,
                    error={
                        "stage": "policy_acceptance",
                        "message": (
                            f"Policy acceptance blocked ({policy_reason_code}): {policy_note}. "
                            f"Manual review required."
                        ),
                    },
                ),
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
            )
            policy_message = (
                f"Policy acceptance blocked ({policy_reason_code}): {policy_note}. "
                f"Manual review required."
            )
            review_required_debug_record = _build_cv_generation_debug_record(
                job=job,
                status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                fit_classification=fit,
                evidence_used=evidence_used,
                evidence_selection_summary=evidence_selection_summary,
                analysis_input_summary=analysis_input_summary,
                gap_summary=gap,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                repair_attempt=repair_attempt,
                structured_cv_final=structured_cv,
                markdown_final=cv_markdown,
                enabled_sections=enabled_cv_sections,
                cv_generation_model=job_cv_generation_model_value,
                runtime_provenance=job_runtime_provenance,
                cv_prompt_id=cv_prompt_id_value,
                cv_prompt_template_path=cv_prompt_template_path_value,
                error={
                    "stage": "policy_acceptance",
                    "message": policy_message,
                },
                agentic_live_trace=job_agentic_live_trace,
            )
            cv_generation_debug_records.append(review_required_debug_record)
            _emit_cv_generation_item_observation(
                run_id=run_id,
                analysis_record=analysis_record,
                debug_record=review_required_debug_record,
            )
            return True

        def _handle_cv_generation_accepted_debug_and_events(
            *,
            state: dict[str, Any],
            analysis_record: dict[str, Any],
            structured_cv_final: dict[str, Any] | None,
            markdown_final: str | None,
            enabled_cv_sections: list[str],
            cv_prompt_id_value: str,
            cv_prompt_template_path_value: str,
            job_cv_generation_model_value: str | None,
            job_runtime_provenance: dict[str, Any] | None,
            job_agentic_live_trace: dict[str, Any] | None,
            generation_attempt_count: int,
            latency_ms: int,
            run_id: str,
            cv_generation_input_fingerprint: str | None,
        ) -> None:
            job = cast(dict[str, Any], state["job"])
            fit = str(state["fit"] or "skip")
            evidence_used = cast(list[dict[str, Any]], state["evidence_used"])
            evidence_selection_summary = cast(dict[str, Any], state["evidence_selection_summary"])
            analysis_input_summary = cast(dict[str, Any], state["analysis_input_summary"])
            gap = state["gap"]
            structured_cv_initial = cast(dict[str, Any] | None, state["structured_cv_initial"])
            validation_initial = cast(dict[str, Any] | None, state["validation_initial"])
            repair_attempt = cast(dict[str, Any], state["repair_attempt"])

            accepted_debug_record = _build_cv_generation_debug_record(
                job=job,
                status="accepted",
                fit_classification=fit,
                evidence_used=evidence_used,
                evidence_selection_summary=evidence_selection_summary,
                analysis_input_summary=analysis_input_summary,
                gap_summary=gap,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                repair_attempt=repair_attempt,
                structured_cv_final=structured_cv_final,
                markdown_final=markdown_final,
                enabled_sections=enabled_cv_sections,
                cv_generation_model=job_cv_generation_model_value,
                runtime_provenance=job_runtime_provenance,
                cv_prompt_id=cv_prompt_id_value,
                cv_prompt_template_path=cv_prompt_template_path_value,
                error=None,
                agentic_live_trace=job_agentic_live_trace,
            )
            accepted_debug_record["attempt_count"] = generation_attempt_count
            accepted_debug_record["cv_generation_input_fingerprint"] = cv_generation_input_fingerprint
            cv_generation_debug_records.append(accepted_debug_record)
            _emit_cv_generation_item_observation(
                run_id=run_id,
                analysis_record=analysis_record,
                debug_record=accepted_debug_record,
            )
            _emit_cv_generation_result_event(
                state=state,
                status="accepted",
                attempt_count=generation_attempt_count,
                retry_count=max(generation_attempt_count - 1, 0),
                latency_ms=latency_ms,
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
                review_required_reason_code=str(accepted_debug_record.get("review_required_reason_code") or ""),
                validation_evidence_fingerprint=str(accepted_debug_record.get("validation_evidence_fingerprint") or ""),
            )

        def _handle_cv_generation_failure(
            *,
            state: dict[str, Any],
            analysis_record: dict[str, Any],
            structured_cv_final: dict[str, Any] | None,
            markdown_final: str | None,
            cv_prompt_id_value: str,
            cv_prompt_template_path_value: str,
            enabled_cv_sections: list[str],
            latency_ms: int,
            run_id: str,
            exc: Exception,
            cv_generation_input_fingerprint: str | None,
        ) -> None:
            job = cast(dict[str, Any], state["job"])
            fit = str(state["fit"] or "skip")
            evidence_used = cast(list[dict[str, Any]], state["evidence_used"])
            evidence_selection_summary = cast(dict[str, Any], state["evidence_selection_summary"])
            analysis_input_summary = cast(dict[str, Any], state["analysis_input_summary"])
            gap = state["gap"]
            structured_cv_initial = cast(dict[str, Any] | None, state["structured_cv_initial"])
            validation_initial = cast(dict[str, Any] | None, state["validation_initial"])
            repair_attempt = cast(dict[str, Any], state["repair_attempt"])
            job_cv_generation_model_value = cast(str | None, state["job_cv_generation_model_value"])
            job_runtime_provenance = cast(dict[str, Any] | None, state["job_runtime_provenance"])
            job_agentic_live_trace = cast(dict[str, Any] | None, state["job_agentic_live_trace"])

            logger.error("[run_id=%s] Failed for %s: %s", run_id, job.get("job_url"), exc)
            failure_status = "persistence_failed" if structured_cv_final is not None or markdown_final is not None else "generation_failed"
            failure_stage = "persistence" if failure_status == "persistence_failed" else "generation"
            failure_debug_record = _build_cv_generation_debug_record(
                job=job,
                status=failure_status,
                fit_classification=fit,
                evidence_used=evidence_used,
                evidence_selection_summary=evidence_selection_summary,
                analysis_input_summary=analysis_input_summary,
                gap_summary=gap,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                repair_attempt=repair_attempt,
                structured_cv_final=structured_cv_final if failure_status == "persistence_failed" else None,
                markdown_final=markdown_final if failure_status == "persistence_failed" else None,
                enabled_sections=enabled_cv_sections,
                cv_generation_model=job_cv_generation_model_value,
                runtime_provenance=job_runtime_provenance,
                cv_prompt_id=cv_prompt_id_value,
                cv_prompt_template_path=cv_prompt_template_path_value,
                error={
                    "stage": failure_stage,
                    "message": str(exc),
                },
                agentic_live_trace=job_agentic_live_trace,
            )
            failure_debug_record["cv_generation_input_fingerprint"] = cv_generation_input_fingerprint
            _emit_cv_generation_result_event(
                state=state,
                status=failure_status,
                attempt_count=1,
                retry_count=0,
                latency_ms=latency_ms,
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
                review_required_reason_code=str(failure_debug_record.get("review_required_reason_code") or ""),
                validation_evidence_fingerprint=str(failure_debug_record.get("validation_evidence_fingerprint") or ""),
            )
            cv_generation_debug_records.append(failure_debug_record)
            _emit_cv_generation_item_observation(
                run_id=run_id,
                analysis_record=analysis_record,
                debug_record=failure_debug_record,
            )
            if reporter is not None:
                reporter.emit(
                    "layer4_cv_error",
                    "error",
                    f"CV generation failed for {job.get('job_url')}: {exc}",
                    _bounded_event_payload(
                        event_name="cv_generation_decision",
                        event_family="decision",
                        source_stage="cv_generation",
                        event_status="completed",
                        job_url=str(job.get("job_url") or ""),
                        deterministic_outcome="rejected",
                        stage_owned_subreason=failure_status,
                        provenance={
                            "cv_generation_model": job_cv_generation_model_value,
                        },
                        input_snapshot={
                            "ranking_fit_label": _authoritative_ranking_fit_label(job, fit),
                            "fit_classification": fit,
                            "selected_evidence_count": len(evidence_used),
                        },
                        output_snapshot={
                            "error_stage": failure_stage,
                        },
                        artifact_refs={"stage_id": "cv_generation"},
                    ),
                )  # type: ignore[union-attr]

        generation_work_items_by_index: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
        if configured_cv_generation_concurrency > 1 and len(indexed_generation_ready_records) > 1:
            def _prepare_indexed_work_item(
                generation_index: int,
                analysis_record: dict[str, Any],
            ) -> tuple[int, dict[str, Any], dict[str, Any]]:
                prepared = _prepare_cv_generation_work_item(
                    generation_index=generation_index,
                    analysis_record=analysis_record,
                )
                return generation_index, analysis_record, prepared

            with ThreadPoolExecutor(max_workers=configured_cv_generation_concurrency) as executor:
                future_to_index: dict[Future[tuple[int, dict[str, Any], dict[str, Any]]], int] = {}
                for generation_index, analysis_record in indexed_generation_ready_records:
                    future = executor.submit(_prepare_indexed_work_item, generation_index, analysis_record)
                    future_to_index[future] = generation_index
                for future in future_to_index:
                    completed_index, completed_record, completed_prepared = future.result()
                    generation_work_items_by_index[completed_index] = (completed_record, completed_prepared)
        else:
            for generation_index, analysis_record in indexed_generation_ready_records:
                generation_work_items_by_index[generation_index] = (
                    analysis_record,
                    _prepare_cv_generation_work_item(
                        generation_index=generation_index,
                        analysis_record=analysis_record,
                    ),
                )

        def _begin_cv_generation_item(
            generation_index: int,
            analysis_record: dict[str, Any],
            work_item: dict[str, Any],
        ) -> dict[str, Any]:
            with observe_span(
                "pipeline.cv_generation",
                attributes={
                    "run_id": run_id,
                    "generation_ready_jobs": generation_total,
                    "job_url": str((work_item["job"] or {}).get("job_url") or ""),
                },
            ):
                generation_state = _initialize_cv_generation_runtime_state(work_item)
                _emit_cv_generation_started_event(
                    generation_index=generation_index,
                    generation_total=generation_total,
                    state=generation_state,
                )
            runtime = {
                "analysis_record": analysis_record,
                "generation_state": generation_state,
                "cv_generation_started_monotonic": time.monotonic(),
                "job": cast(dict[str, Any], generation_state["job"]),
                "evidence": cast(list[dict[str, Any]], generation_state["evidence"]),
                "evidence_used": cast(list[dict[str, Any]], generation_state["evidence_used"]),
                "evidence_selection_summary": cast(dict[str, Any], generation_state["evidence_selection_summary"]),
                "analysis_input_summary": cast(dict[str, Any], generation_state["analysis_input_summary"]),
                "analysis_grounding": cast(AnalysisGroundingPayload, generation_state["analysis_grounding"]),
                "gap": generation_state["gap"],
                "fit": str(generation_state["fit"] or "skip"),
                "structured_cv_initial": cast(dict[str, Any] | None, generation_state["structured_cv_initial"]),
                "validation_initial": cast(dict[str, Any] | None, generation_state["validation_initial"]),
                "repair_attempt": cast(dict[str, Any], generation_state["repair_attempt"]),
                "structured_cv_final": cast(dict[str, Any] | None, generation_state["structured_cv_final"]),
                "markdown_final": cast(str | None, generation_state["markdown_final"]),
                "job_runtime_provenance": cast(dict[str, Any] | None, generation_state["job_runtime_provenance"]),
                "job_cv_generation_model_value": cast(str | None, generation_state["job_cv_generation_model_value"]),
                "job_agentic_live_trace": cast(dict[str, Any] | None, generation_state["job_agentic_live_trace"]),
                "generation_attempt_count": int(generation_state["generation_attempt_count"]),
                "generation_started_at_iso": str(generation_state["generation_started_at_iso"]),
                "generation_finished_at_iso": cast(str | None, generation_state["generation_finished_at_iso"]),
                "generation_worker_slot": int(generation_state["generation_worker_slot"]),
            }
            return runtime

        def _execute_cv_generation_item(
            *,
            generation_state: dict[str, Any],
            analysis_record: dict[str, Any],
            validation: dict[str, Any],
            structured_cv: dict[str, Any] | None,
            cv: str,
            fit: str,
            gap: Any,
            evidence: list[dict[str, Any]],
            job: dict[str, Any],
            analysis_grounding: AnalysisGroundingPayload,
            cv_generation_started_monotonic: float,
            structured_cv_final: dict[str, Any] | None,
            markdown_final: str | None,
            job_cv_generation_model_value: str | None,
            job_runtime_provenance: dict[str, Any] | None,
            job_agentic_live_trace: dict[str, Any] | None,
            generation_attempt_count: int,
            validation_initial: dict[str, Any] | None,
            repair_attempt: dict[str, Any],
            evidence_used: list[dict[str, Any]],
            evidence_selection_summary: dict[str, Any],
            analysis_input_summary: dict[str, Any],
            cv_generation_input_fingerprint: str | None,
        ) -> tuple[bool, dict[str, Any] | None, str | None]:
            _emit_cv_generation_invoked_event(
                state=generation_state,
                cv_generation_model_value=job_cv_generation_model_value,
            )
            latency_ms = int((time.monotonic() - cv_generation_started_monotonic) * 1000)
            if _handle_cv_generation_validation_failed(
                state=generation_state,
                analysis_record=analysis_record,
                validation=validation,
                latency_ms=latency_ms,
                cv_prompt_id_value=cv_prompt_id_value,
                cv_prompt_template_path_value=cv_prompt_template_path_value,
                enabled_cv_sections=enabled_cv_sections,
                run_id=run_id,
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
            ):
                return True, structured_cv_final, markdown_final

            markdown_review_reason = _markdown_quality_review_reason(validation)
            if _handle_cv_generation_markdown_review_required(
                state=generation_state,
                analysis_record=analysis_record,
                markdown_review_reason=markdown_review_reason,
                structured_cv=structured_cv,
                cv_markdown=cv,
                latency_ms=latency_ms,
                cv_prompt_id_value=cv_prompt_id_value,
                cv_prompt_template_path_value=cv_prompt_template_path_value,
                enabled_cv_sections=enabled_cv_sections,
                run_id=run_id,
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
            ):
                return True, structured_cv_final, markdown_final

            policy_pass, policy_reason_code, policy_note = _evaluate_cv_acceptance_policy(
                fit_classification=fit,
                gap_summary=gap,
                policy=cv_acceptance_policy,
            )
            if _handle_cv_generation_policy_review_required(
                state=generation_state,
                analysis_record=analysis_record,
                policy_pass=policy_pass,
                policy_reason_code=policy_reason_code,
                policy_note=policy_note,
                structured_cv=structured_cv,
                cv_markdown=cv,
                latency_ms=latency_ms,
                cv_prompt_id_value=cv_prompt_id_value,
                cv_prompt_template_path_value=cv_prompt_template_path_value,
                enabled_cv_sections=enabled_cv_sections,
                run_id=run_id,
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
            ):
                return True, structured_cv_final, markdown_final

            structured_cv_final = structured_cv
            markdown_final = cv
            version = create_cv_version_record(
                job_url=str(job.get("job_url") or ""),
                run_id=run_id,
                enrichment_version=str(config.get("enrichment_version") or "v1"),
                vector_rank=int(job.get("vector_rank") or 0),
                ai_score=float(job.get("ai_score") or 0.0),
                final_score=float(job.get("final_score") or 0.0),
                evidence_ids=[str(e.get("evidence_id") or "") for e in evidence],
                prompt_version=cv_prompt_version_value,
                cv_markdown=cv,
                gap_summary=gap or {},
                fit_classification=fit,
                cv_structured=structured_cv,
                cv_generation_model=job_cv_generation_model_value,
                cv_prompt_version=cv_prompt_version_value,
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
                cv_generation_reuse_status="fresh_compute",
            )
            pipeline_store.store_cv_version(version, config)
            results.append({
                "job_url": str(job.get("job_url") or ""),
                "fit": fit,
                "ranking_fit_label": _authoritative_ranking_fit_label(job, fit),
                "cv_version_id": version["version_id"],
                "gap": gap,
                "structured_cv": structured_cv,
                "cv_generation_model": job_cv_generation_model_value,
                "runtime_provenance": job_runtime_provenance,
                "cv_prompt_id": cv_prompt_id_value,
                "cv_prompt_template_path": cv_prompt_template_path_value,
                "cv_markdown": cv,
                "generated_at": version.get("generated_at"),
                "fit_classification": fit,
                "cv_generation_reuse_status": "fresh_compute",
                "cv_generation_input_fingerprint": cv_generation_input_fingerprint,
                "reuse_decision": build_reuse_decision(
                    decision="fresh_compute",
                    reason_code="no_reusable_snapshot_match",
                    fingerprint=cv_generation_input_fingerprint,
                    source_artifact_type="cv_generation",
                ),
            })
            _handle_cv_generation_accepted_debug_and_events(
                state=generation_state,
                analysis_record=analysis_record,
                structured_cv_final=structured_cv_final,
                markdown_final=markdown_final,
                enabled_cv_sections=enabled_cv_sections,
                cv_prompt_id_value=cv_prompt_id_value,
                cv_prompt_template_path_value=cv_prompt_template_path_value,
                job_cv_generation_model_value=job_cv_generation_model_value,
                job_runtime_provenance=job_runtime_provenance,
                job_agentic_live_trace=job_agentic_live_trace,
                generation_attempt_count=generation_attempt_count,
                latency_ms=latency_ms,
                run_id=run_id,
                cv_generation_input_fingerprint=cv_generation_input_fingerprint,
            )
            logger.info("[run_id=%s] CV generated for %s (fit=%s)", run_id, job.get("job_url"), fit)
            return False, structured_cv_final, markdown_final

        def _run_non_agentic_cv_generation(
            *,
            job: dict[str, Any],
            evidence: list[dict[str, Any]],
            gap: Any,
            fit: str,
            evidence_selection_summary: dict[str, Any],
            analysis_grounding: AnalysisGroundingPayload,
            structured_cv_initial: dict[str, Any] | None,
            validation_initial: dict[str, Any] | None,
            repair_attempt: dict[str, Any],
        ) -> tuple[dict[str, Any] | None, str, dict[str, Any], dict[str, Any] | None, dict[str, Any] | None, dict[str, Any]]:
            generated_cv = generate_cv(
                job,
                evidence,
                gap,
                profile,
                config,
                fit_classification=fit,
                evidence_selection_summary=evidence_selection_summary,
            )
            structured_cv, cv = _unwrap_generated_cv(generated_cv)
            structured_cv_initial = structured_cv
            validation = run_all_validations(
                cv,
                profile,
                config,
                structured_cv=structured_cv,
                analysis_grounding=analysis_grounding,
            )
            validation_initial = _build_validation_snapshot(validation)
            if not validation["valid"] and _should_repair_candidate_name_placeholder(validation, structured_cv, profile):
                assert structured_cv is not None
                repair_attempt = _build_candidate_name_repair_attempt()
                logger.info(
                    "[run_id=%s] Repairing candidate-name placeholder for %s",
                    run_id,
                    job.get("job_url"),
                )
                structured_cv, cv = _repair_candidate_name_placeholder(
                    structured_cv,
                    profile,
                    config,
                )
                validation = run_all_validations(
                    cv,
                    profile,
                    config,
                    structured_cv=structured_cv,
                    analysis_grounding=analysis_grounding,
                )
            if not validation["valid"] and _should_retry_missing_sections(validation):
                missing_sections: list[str] = list(validation.get("missing_sections") or [])
                repair_attempt = _build_repair_attempt(missing_sections)
                logger.info(
                    "[run_id=%s] Retrying CV for %s with missing sections: %s",
                    run_id,
                    job.get("job_url"),
                    missing_sections,
                )
                generated_cv = generate_cv(
                    job,
                    evidence,
                    gap,
                    profile,
                    config,
                    fit_classification=fit,
                    evidence_selection_summary=evidence_selection_summary,
                    repair_missing_sections=missing_sections,
                )
                structured_cv, cv = _unwrap_generated_cv(generated_cv)
                validation = run_all_validations(
                    cv,
                    profile,
                    config,
                    structured_cv=structured_cv,
                    analysis_grounding=analysis_grounding,
                )
            return structured_cv, cv, validation, structured_cv_initial, validation_initial, repair_attempt

        def _run_agentic_cv_generation(
            *,
            analysis_record: dict[str, Any],
            job: dict[str, Any],
            fit: str,
            evidence_used: list[dict[str, Any]],
            evidence_selection_summary: dict[str, Any],
            analysis_input_summary: dict[str, Any],
            gap: Any,
            structured_cv_initial: dict[str, Any] | None,
            validation_initial: dict[str, Any] | None,
            repair_attempt: dict[str, Any],
            structured_cv_final: dict[str, Any] | None,
            markdown_final: str | None,
            job_runtime_provenance: dict[str, Any] | None,
            job_cv_generation_model_value: str | None,
            job_agentic_live_trace: dict[str, Any] | None,
            generation_attempt_count: int,
            generation_worker_slot: int,
            generation_started_at_iso: str,
            cv_generation_input_fingerprint: str | None,
        ) -> dict[str, Any]:
            agentic_generation_result = run_agentic_cv_generation(
                analysis_record=analysis_record,
                profile=profile,
                config=config,
            )
            job_cv_generation_model_value = _resolved_cv_generation_model(
                cv_generation_model_value,
                agentic_generation_result.get("runtime_provenance"),
            )
            if isinstance(agentic_generation_result.get("runtime_provenance"), dict):
                job_runtime_provenance = dict(agentic_generation_result["runtime_provenance"])
            if isinstance(agentic_generation_result.get("agentic_live_trace"), dict):
                job_agentic_live_trace = dict(agentic_generation_result["agentic_live_trace"])
            generation_metrics = _extract_generation_trace_metrics(job_agentic_live_trace)
            fit = str(agentic_generation_result["fit_classification"] or fit)
            analysis_input_summary = dict(agentic_generation_result["analysis_input_summary"])
            evidence_used = list(agentic_generation_result["evidence_used"])
            evidence_selection_summary = dict(agentic_generation_result["evidence_selection_summary"])
            gap = agentic_generation_result["gap_summary"]
            structured_cv_initial = agentic_generation_result["structured_cv_initial"]
            raw_validation_initial = agentic_generation_result["validation_initial"]
            validation_initial = dict(raw_validation_initial) if raw_validation_initial is not None else None
            raw_validation_final = agentic_generation_result.get("validation")
            validation_final = dict(raw_validation_final) if isinstance(raw_validation_final, dict) else None
            if validation_initial is None and validation_final is not None:
                validation_initial = dict(validation_final)
            repair_attempt = dict(agentic_generation_result["repair_attempt"])
            structured_cv_final = agentic_generation_result["structured_cv_final"]
            markdown_final = agentic_generation_result["markdown_final"]
            deferred_reporter_payload: dict[str, Any] | None = None
            if reporter is not None:
                effective_reuse_status = str(agentic_generation_result.get("reuse_status") or "").strip() or "fresh_compute"
                effective_reused_cv_version_id = str(agentic_generation_result.get("reused_cv_version_id") or "").strip()
                deferred_reporter_payload = {
                    "channel": "layer4_cv_generation_result",
                    "level": "info",
                    "message": f"CV generation result for {job.get('job_url')}: {agentic_generation_result.get('status')}",
                    "payload": _bounded_event_payload(
                        event_name="cv_generation_result",
                        event_family="decision",
                        source_stage="cv_generation",
                        event_status="completed",
                        job_url=str(job.get("job_url") or ""),
                        deterministic_outcome=str(agentic_generation_result.get("status") or ""),
                        fallback_used=False,
                        input_snapshot={
                            "ranking_fit_label": _authoritative_ranking_fit_label(job, fit),
                            "fit_classification": fit,
                        },
                        output_snapshot={
                            "status": str(agentic_generation_result.get("status") or ""),
                            "reuse_status": effective_reuse_status,
                            "reused_cv_version_id": effective_reused_cv_version_id,
                            "reuse_status_flat": effective_reuse_status,
                            "reused_cv_version_id_flat": effective_reused_cv_version_id,
                            "cv_generation_input_fingerprint": str(cv_generation_input_fingerprint or ""),
                            "review_required_reason_code": str(
                                _normalize_review_required_reason_code(
                                    status=str(agentic_generation_result.get("status") or ""),
                                    error=cast(dict[str, str] | None, agentic_generation_result.get("error")),
                                    validation_initial=(validation_final or validation_initial),
                                )
                                or ""
                            ),
                            "validation_evidence_fingerprint": _build_validation_evidence_fingerprint(
                                status=str(agentic_generation_result.get("status") or ""),
                                validation_initial=(validation_final or validation_initial),
                                error=cast(dict[str, str] | None, agentic_generation_result.get("error")),
                            ),
                            "attempt_count": int(generation_metrics["attempt_count"]),
                            "retry_count": int(generation_metrics["retry_count"]),
                            "configured_concurrency": int(configured_cv_generation_concurrency),
                            "worker_slot": int(generation_worker_slot),
                            "started_at": generation_started_at_iso,
                            "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            "missing_sections": (
                                list(((validation_final or validation_initial) or {}).get("missing_sections") or [])
                                if str(agentic_generation_result.get("status") or "") == "validation_failed"
                                else []
                            ),
                            "grounding_violations": (
                                list(((validation_final or validation_initial) or {}).get("grounding_violations") or [])
                                if str(agentic_generation_result.get("status") or "") == "validation_failed"
                                else []
                            ),
                            "deterministic_grounding_violations": (
                                list(((validation_final or validation_initial) or {}).get("deterministic_grounding_violations") or [])
                                if str(agentic_generation_result.get("status") or "") == "validation_failed"
                                else []
                            ),
                            "semantic_grounding_violations": (
                                list(((validation_final or validation_initial) or {}).get("semantic_grounding_violations") or [])
                                if str(agentic_generation_result.get("status") or "") == "validation_failed"
                                else []
                            ),
                            "skill_violations": (
                                list(((validation_final or validation_initial) or {}).get("skill_violations") or [])
                                if str(agentic_generation_result.get("status") or "") == "validation_failed"
                                else []
                            ),
                            "warnings": (
                                list(((validation_final or validation_initial) or {}).get("warnings") or [])
                                if str(agentic_generation_result.get("status") or "") == "validation_failed"
                                else []
                            ),
                        },
                        artifact_refs={"stage_id": "cv_generation"},
                        latency_ms=cast(int | None, generation_metrics["latency_ms"]),
                        usage=cast(dict[str, Any] | None, generation_metrics["usage"]),
                        cost=cast(dict[str, Any] | None, generation_metrics["cost"]),
                    ),
                }
            if agentic_generation_result["status"] != "accepted":
                retry_attempt_count = 1
                generation_error = agentic_generation_result["error"]
                if _is_recoverable_cv_failure(
                    status=str(agentic_generation_result["status"] or ""),
                    error=cast(dict[str, str] | None, generation_error),
                ):
                    retry_attempt_count = 2
                    generation_attempt_count = 2
                    retry_result = run_agentic_cv_generation(
                        analysis_record=analysis_record,
                        profile=profile,
                        config=config,
                    )
                    if isinstance(retry_result.get("runtime_provenance"), dict):
                        job_runtime_provenance = dict(retry_result["runtime_provenance"])
                    if isinstance(retry_result.get("agentic_live_trace"), dict):
                        job_agentic_live_trace = dict(retry_result["agentic_live_trace"])
                    if retry_result.get("status") == "accepted":
                        fit = str(retry_result["fit_classification"] or fit)
                        structured_cv = retry_result["structured_cv_final"]
                        cv = str(retry_result["markdown_final"] or "")
                        validation: dict[str, Any] = {"valid": True, "missing_sections": []}
                        structured_cv_initial = retry_result["structured_cv_initial"]
                        validation_initial = dict(retry_result["validation_initial"] or {})
                        repair_attempt = dict(retry_result["repair_attempt"] or _EMPTY_REPAIR_ATTEMPT)
                        structured_cv_final = structured_cv
                        markdown_final = cv
                        agentic_generation_result = retry_result
                    else:
                        generation_error = retry_result.get("error")
                        agentic_generation_result = retry_result
                if agentic_generation_result["status"] != "accepted":
                    try:
                        debug_record = _build_cv_generation_debug_record(
                            job=job,
                            status=agentic_generation_result["status"],
                            fit_classification=fit,
                            evidence_used=evidence_used,
                            evidence_selection_summary=evidence_selection_summary,
                            analysis_input_summary=analysis_input_summary,
                            gap_summary=gap,
                            structured_cv_initial=structured_cv_initial,
                            validation_initial=validation_initial,
                            validation_final=validation_final,
                            repair_attempt=repair_attempt,
                            structured_cv_final=structured_cv_final,
                            markdown_final=markdown_final,
                            enabled_sections=enabled_cv_sections,
                            cv_generation_model=job_cv_generation_model_value,
                            runtime_provenance=job_runtime_provenance,
                            cv_prompt_id=cv_prompt_id_value,
                            cv_prompt_template_path=cv_prompt_template_path_value,
                            error=(
                                {
                                    "stage": str(generation_error["stage"]),
                                    "message": str(generation_error["message"]),
                                }
                                if generation_error is not None
                                else None
                            ),
                            agentic_live_trace=job_agentic_live_trace,
                        )
                    except Exception:
                        debug_record = {
                            "job_url": str(job.get("job_url") or ""),
                            "job_title": extract_job_title(job),
                            "status": str(agentic_generation_result.get("status") or "generation_failed"),
                            "fit_classification": fit,
                            "validation_initial": validation_initial,
                            "error": generation_error,
                        }
                    debug_record["attempt_count"] = retry_attempt_count
                    debug_record["cv_generation_input_fingerprint"] = cv_generation_input_fingerprint
                    return {
                        "should_continue": True,
                        "deferred_debug_record": debug_record,
                        "deferred_observation": True,
                        "deferred_reporter_payload": deferred_reporter_payload,
                    }
            review_reason = _hitl_review_reason_for_agentic_case(
                analysis_record,
                agentic_generation_result,
                validation_initial,
            )
            if review_reason:
                review_error = {
                    "stage": "review_gate",
                    "message": review_reason,
                }
                if deferred_reporter_payload is not None:
                    payload = dict(deferred_reporter_payload.get("payload") or {})
                    output_snapshot = dict(payload.get("output_snapshot") or {})
                    output_snapshot["status"] = CV_GENERATION_REVIEW_REQUIRED_STATUS
                    output_snapshot["review_required_reason_code"] = _review_required_reason_code_value(
                        _normalize_review_required_reason_code(
                            status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                            error=review_error,
                            validation_initial=validation_initial,
                        )
                    )
                    payload["deterministic_outcome"] = CV_GENERATION_REVIEW_REQUIRED_STATUS
                    payload["output_snapshot"] = output_snapshot
                    deferred_reporter_payload["message"] = (
                        f"CV generation result for {job.get('job_url')}: {CV_GENERATION_REVIEW_REQUIRED_STATUS}"
                    )
                    deferred_reporter_payload["payload"] = payload
                review_debug_record = _build_cv_generation_debug_record(
                    job=job,
                    status=CV_GENERATION_REVIEW_REQUIRED_STATUS,
                    fit_classification=fit,
                    evidence_used=evidence_used,
                    evidence_selection_summary=evidence_selection_summary,
                    analysis_input_summary=analysis_input_summary,
                    gap_summary=gap,
                    structured_cv_initial=structured_cv_initial,
                    validation_initial=validation_initial,
                    repair_attempt=repair_attempt,
                    structured_cv_final=structured_cv_final,
                    markdown_final=markdown_final,
                    enabled_sections=enabled_cv_sections,
                    cv_generation_model=job_cv_generation_model_value,
                    runtime_provenance=job_runtime_provenance,
                    cv_prompt_id=cv_prompt_id_value,
                    cv_prompt_template_path=cv_prompt_template_path_value,
                    error=review_error,
                    agentic_live_trace=job_agentic_live_trace,
                )
                review_debug_record["cv_generation_input_fingerprint"] = cv_generation_input_fingerprint
                return {
                    "should_continue": True,
                    "deferred_debug_record": review_debug_record,
                    "deferred_observation": False,
                    "deferred_reporter_payload": deferred_reporter_payload,
                }
            structured_cv = structured_cv_final
            cv = str(markdown_final or "")
            validation = {"valid": True, "missing_sections": []}
            return {
                "should_continue": False,
                "deferred_reporter_payload": deferred_reporter_payload,
                "fit": fit,
                "analysis_input_summary": analysis_input_summary,
                "evidence_used": evidence_used,
                "evidence_selection_summary": evidence_selection_summary,
                "gap": gap,
                "structured_cv_initial": structured_cv_initial,
                "validation_initial": validation_initial,
                "repair_attempt": repair_attempt,
                "structured_cv_final": structured_cv_final,
                "markdown_final": markdown_final,
                "job_runtime_provenance": job_runtime_provenance,
                "job_cv_generation_model_value": job_cv_generation_model_value,
                "job_agentic_live_trace": job_agentic_live_trace,
                "generation_attempt_count": generation_attempt_count,
                "structured_cv": structured_cv,
                "cv": cv,
                "validation": validation,
            }

        def _compute_cv_generation_outcome(
            *,
            agentic_enabled: bool,
            analysis_record: dict[str, Any],
            job: dict[str, Any],
            evidence: list[dict[str, Any]],
            gap: Any,
            fit: str,
            evidence_selection_summary: dict[str, Any],
            analysis_input_summary: dict[str, Any],
            analysis_grounding: AnalysisGroundingPayload,
            structured_cv_initial: dict[str, Any] | None,
            validation_initial: dict[str, Any] | None,
            repair_attempt: dict[str, Any],
            structured_cv_final: dict[str, Any] | None,
            markdown_final: str | None,
            job_runtime_provenance: dict[str, Any] | None,
            job_cv_generation_model_value: str | None,
            job_agentic_live_trace: dict[str, Any] | None,
            generation_attempt_count: int,
            generation_worker_slot: int,
            generation_started_at_iso: str,
            cv_generation_input_fingerprint: str | None,
        ) -> dict[str, Any]:
            if agentic_enabled:
                return _run_agentic_cv_generation(
                    analysis_record=analysis_record,
                    job=job,
                    fit=fit,
                    evidence_used=evidence,
                    evidence_selection_summary=evidence_selection_summary,
                    analysis_input_summary=analysis_input_summary,
                    gap=gap,
                    structured_cv_initial=structured_cv_initial,
                    validation_initial=validation_initial,
                    repair_attempt=repair_attempt,
                    structured_cv_final=structured_cv_final,
                    markdown_final=markdown_final,
                    job_runtime_provenance=job_runtime_provenance,
                    job_cv_generation_model_value=job_cv_generation_model_value,
                    job_agentic_live_trace=job_agentic_live_trace,
                    generation_attempt_count=generation_attempt_count,
                    generation_worker_slot=generation_worker_slot,
                    generation_started_at_iso=generation_started_at_iso,
                    cv_generation_input_fingerprint=cv_generation_input_fingerprint,
                )

            (
                structured_cv,
                cv,
                validation,
                structured_cv_initial,
                validation_initial,
                repair_attempt,
            ) = _run_non_agentic_cv_generation(
                job=job,
                evidence=evidence,
                gap=gap,
                fit=fit,
                evidence_selection_summary=evidence_selection_summary,
                analysis_grounding=analysis_grounding,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                repair_attempt=repair_attempt,
            )
            return {
                "should_continue": False,
                "fit": fit,
                "analysis_input_summary": analysis_input_summary,
                "evidence_used": evidence,
                "evidence_selection_summary": evidence_selection_summary,
                "gap": gap,
                "structured_cv_initial": structured_cv_initial,
                "validation_initial": validation_initial,
                "repair_attempt": repair_attempt,
                "structured_cv_final": structured_cv_final,
                "markdown_final": markdown_final,
                "job_runtime_provenance": job_runtime_provenance,
                "job_cv_generation_model_value": job_cv_generation_model_value,
                "job_agentic_live_trace": job_agentic_live_trace,
                "generation_attempt_count": generation_attempt_count,
                "structured_cv": structured_cv,
                "cv": cv,
                "validation": validation,
            }

        generation_runtime_by_index: dict[int, dict[str, Any]] = {}
        for generation_index in sorted(generation_work_items_by_index):
            analysis_record, work_item = generation_work_items_by_index[generation_index]
            generation_runtime_by_index[generation_index] = _begin_cv_generation_item(generation_index, analysis_record, work_item)

        def _compute_for_index(generation_index: int) -> dict[str, Any]:
            runtime = generation_runtime_by_index[generation_index]
            return _compute_cv_generation_outcome(
                agentic_enabled=agentic_late_stage_enabled,
                analysis_record=cast(dict[str, Any], runtime["analysis_record"]),
                job=cast(dict[str, Any], runtime["job"]),
                evidence=cast(list[dict[str, Any]], runtime["evidence"]),
                gap=runtime["gap"],
                fit=str(runtime["fit"] or "skip"),
                evidence_selection_summary=cast(dict[str, Any], runtime["evidence_selection_summary"]),
                analysis_input_summary=cast(dict[str, Any], runtime["analysis_input_summary"]),
                analysis_grounding=cast(AnalysisGroundingPayload, runtime["analysis_grounding"]),
                structured_cv_initial=cast(dict[str, Any] | None, runtime["structured_cv_initial"]),
                validation_initial=cast(dict[str, Any] | None, runtime["validation_initial"]),
                repair_attempt=cast(dict[str, Any], runtime["repair_attempt"]),
                structured_cv_final=cast(dict[str, Any] | None, runtime["structured_cv_final"]),
                markdown_final=cast(str | None, runtime["markdown_final"]),
                job_runtime_provenance=cast(dict[str, Any] | None, runtime["job_runtime_provenance"]),
                job_cv_generation_model_value=cast(str | None, runtime["job_cv_generation_model_value"]),
                job_agentic_live_trace=cast(dict[str, Any] | None, runtime["job_agentic_live_trace"]),
                generation_attempt_count=int(runtime["generation_attempt_count"]),
                generation_worker_slot=int(runtime["generation_worker_slot"]),
                generation_started_at_iso=str(runtime["generation_started_at_iso"]),
                cv_generation_input_fingerprint=str(cv_generation_fingerprint_by_index.get(generation_index) or "") or None,
            )

        compute_outcomes_by_index: dict[int, dict[str, Any]] = {}
        if configured_cv_generation_concurrency > 1 and len(generation_runtime_by_index) > 1:
            with ThreadPoolExecutor(max_workers=configured_cv_generation_concurrency) as executor:
                future_to_generation_index: dict[Future[dict[str, Any]], int] = {
                    executor.submit(_compute_for_index, generation_index): generation_index
                    for generation_index in sorted(generation_runtime_by_index)
                }
                for future in as_completed(future_to_generation_index):
                    generation_index = future_to_generation_index[future]
                    try:
                        compute_outcomes_by_index[generation_index] = future.result()
                    except Exception as exc:
                        compute_outcomes_by_index[generation_index] = {"_compute_exception": exc}
        else:
            for generation_index in sorted(generation_runtime_by_index):
                try:
                    compute_outcomes_by_index[generation_index] = _compute_for_index(generation_index)
                except Exception as exc:
                    compute_outcomes_by_index[generation_index] = {"_compute_exception": exc}

        for generation_index in sorted(generation_work_items_by_index):
            runtime = generation_runtime_by_index[generation_index]
            analysis_record = cast(dict[str, Any], runtime["analysis_record"])
            generation_state = cast(dict[str, Any], runtime["generation_state"])
            cv_generation_started_monotonic = cast(float, runtime["cv_generation_started_monotonic"])
            job = cast(dict[str, Any], runtime["job"])
            evidence = cast(list[dict[str, Any]], runtime["evidence"])
            evidence_used = cast(list[dict[str, Any]], runtime["evidence_used"])
            evidence_selection_summary = cast(dict[str, Any], runtime["evidence_selection_summary"])
            analysis_input_summary = cast(dict[str, Any], runtime["analysis_input_summary"])
            analysis_grounding = cast(AnalysisGroundingPayload, runtime["analysis_grounding"])
            gap = runtime["gap"]
            fit = str(runtime["fit"] or "skip")
            structured_cv_initial = cast(dict[str, Any] | None, runtime["structured_cv_initial"])
            validation_initial = cast(dict[str, Any] | None, runtime["validation_initial"])
            repair_attempt = cast(dict[str, Any], runtime["repair_attempt"])
            structured_cv_final = cast(dict[str, Any] | None, runtime["structured_cv_final"])
            markdown_final = cast(str | None, runtime["markdown_final"])
            job_runtime_provenance = cast(dict[str, Any] | None, runtime["job_runtime_provenance"])
            job_cv_generation_model_value = cast(str | None, runtime["job_cv_generation_model_value"])
            job_agentic_live_trace = cast(dict[str, Any] | None, runtime["job_agentic_live_trace"])
            generation_attempt_count = int(runtime["generation_attempt_count"])
            reuse_fingerprint = str(cv_generation_fingerprint_by_index.get(generation_index) or "").strip()
            reused_cv_row = (
                cv_generation_reuse_index.get(reuse_fingerprint)
                if cv_generation_reuse_enabled and reuse_fingerprint
                else None
            )

            if isinstance(reused_cv_row, dict):
                reused_markdown = str(reused_cv_row.get("cv_markdown") or "")
                reused_structured = reused_cv_row.get("cv_structured")
                if isinstance(reused_structured, dict) and reused_markdown:
                    structured_cv_final = dict(reused_structured)
                    markdown_final = reused_markdown
                    version = create_cv_version_record(
                        job_url=str(job.get("job_url") or ""),
                        run_id=run_id,
                        enrichment_version=str(config.get("enrichment_version") or "v1"),
                        vector_rank=int(job.get("vector_rank") or 0),
                        ai_score=float(job.get("ai_score") or 0.0),
                        final_score=float(job.get("final_score") or 0.0),
                        evidence_ids=[str(e.get("evidence_id") or "") for e in evidence],
                        prompt_version=cv_prompt_version_value,
                        cv_markdown=reused_markdown,
                        gap_summary=cast(dict[str, Any], gap or {}),
                        fit_classification=fit,
                        cv_structured=structured_cv_final,
                        cv_generation_model=job_cv_generation_model_value,
                        cv_prompt_version=cv_prompt_version_value,
                        cv_generation_input_fingerprint=reuse_fingerprint,
                        cv_generation_reuse_status="reused_exact_match",
                    )
                    pipeline_store.store_cv_version(version, config)
                    results.append({
                        "job_url": str(job.get("job_url") or ""),
                        "fit": fit,
                        "ranking_fit_label": _authoritative_ranking_fit_label(job, fit),
                        "cv_version_id": version["version_id"],
                        "gap": gap,
                        "structured_cv": structured_cv_final,
                        "cv_generation_model": job_cv_generation_model_value,
                        "runtime_provenance": job_runtime_provenance,
                        "cv_prompt_id": cv_prompt_id_value,
                        "cv_prompt_template_path": cv_prompt_template_path_value,
                        "cv_markdown": reused_markdown,
                        "generated_at": version.get("generated_at"),
                        "fit_classification": fit,
                        "cv_generation_reuse_status": "reused_exact_match",
                        "cv_generation_input_fingerprint": reuse_fingerprint,
                        "reuse_decision": build_reuse_decision(
                            decision="reused_exact_match",
                            reason_code="exact_fingerprint_match",
                            fingerprint=reuse_fingerprint,
                            source_artifact_type="cv_generation",
                        ),
                    })
                    reused_debug_record = _build_cv_generation_debug_record(
                        job=job,
                        status="accepted",
                        fit_classification=fit,
                        evidence_used=evidence_used,
                        evidence_selection_summary=evidence_selection_summary,
                        analysis_input_summary=analysis_input_summary,
                        gap_summary=cast(dict[str, Any] | None, gap),
                        structured_cv_initial=structured_cv_final,
                        validation_initial={"valid": True, "missing_sections": []},
                        repair_attempt=dict(_EMPTY_REPAIR_ATTEMPT),
                        structured_cv_final=structured_cv_final,
                        markdown_final=reused_markdown,
                        enabled_sections=enabled_cv_sections,
                        cv_generation_model=job_cv_generation_model_value,
                        runtime_provenance=job_runtime_provenance,
                        cv_prompt_id=cv_prompt_id_value,
                        cv_prompt_template_path=cv_prompt_template_path_value,
                        error=None,
                        agentic_live_trace=None,
                    )
                    reused_debug_record["cv_generation_reuse_status"] = "reused_exact_match"
                    reused_debug_record["cv_generation_input_fingerprint"] = reuse_fingerprint
                    reused_debug_record["reused_cv_version_id"] = reused_cv_row.get("version_id")
                    cv_generation_debug_records.append(reused_debug_record)
                    _emit_cv_generation_item_observation(
                        run_id=run_id,
                        analysis_record=analysis_record,
                        debug_record=reused_debug_record,
                    )
                    _emit_cv_generation_result_event(
                        state=generation_state,
                        status="accepted",
                        attempt_count=1,
                        retry_count=0,
                        latency_ms=int((time.monotonic() - cv_generation_started_monotonic) * 1000),
                        reuse_status="reused_exact_match",
                        reused_cv_version_id=str(reused_cv_row.get("version_id") or ""),
                        cv_generation_input_fingerprint=reuse_fingerprint or None,
                        review_required_reason_code=str(reused_debug_record.get("review_required_reason_code") or ""),
                        validation_evidence_fingerprint=str(reused_debug_record.get("validation_evidence_fingerprint") or ""),
                    )
                    if reporter is not None:
                        reporter.emit(
                            "layer4_cv_generation_reused",
                            "info",
                            f"CV generation reused for {job.get('job_url')}",
                            _bounded_event_payload(
                                event_name="cv_generation_reused",
                                event_family="decision",
                                source_stage="cv_generation",
                                event_status="completed",
                                job_url=str(job.get("job_url") or ""),
                                deterministic_outcome="accepted",
                                stage_owned_subreason="reused_exact_match",
                                output_snapshot={
                                    "reuse_status": "reused_exact_match",
                                    "reused_cv_version_id": str(reused_cv_row.get("version_id") or ""),
                                    "cv_generation_input_fingerprint": reuse_fingerprint,
                                    "configured_concurrency": int(configured_cv_generation_concurrency),
                                    "cv_generation_concurrency_effective": int(configured_cv_generation_concurrency),
                                    "worker_slot": int(generation_state.get("generation_worker_slot") or 0),
                                    "attempt_count": 1,
                                    "retry_count": 0,
                                },
                                artifact_refs={"stage_id": "cv_generation"},
                            ),
                        )  # type: ignore[union-attr]
                    continue

            try:
                compute_outcome = compute_outcomes_by_index[generation_index]
                compute_exception = cast(Exception | None, compute_outcome.get("_compute_exception"))
                if compute_exception is not None:
                    raise compute_exception

                if agentic_late_stage_enabled:
                    agentic_outcome = compute_outcome
                    if cast(bool, agentic_outcome["should_continue"]):
                        deferred_reporter_payload = cast(dict[str, Any] | None, agentic_outcome.get("deferred_reporter_payload"))
                        if deferred_reporter_payload is not None and reporter is not None:
                            reporter.emit(
                                str(deferred_reporter_payload.get("channel") or "layer4_cv_generation_result"),
                                str(deferred_reporter_payload.get("level") or "info"),
                                str(deferred_reporter_payload.get("message") or ""),
                                cast(dict[str, Any], deferred_reporter_payload.get("payload") or {}),
                            )  # type: ignore[union-attr]
                        deferred_debug_record = cast(dict[str, Any] | None, agentic_outcome.get("deferred_debug_record"))
                        if deferred_debug_record is not None:
                            cv_generation_debug_records.append(deferred_debug_record)
                            if cast(bool, agentic_outcome.get("deferred_observation")):
                                _emit_cv_generation_item_observation(
                                    run_id=run_id,
                                    analysis_record=analysis_record,
                                    debug_record=deferred_debug_record,
                                )
                        continue
                    fit = str(agentic_outcome["fit"] or fit)
                    # For terminal non-deferred outcomes, emit exactly one normalized result
                    # event via downstream handlers to avoid stale duplicate rows in timeline.
                    analysis_input_summary = cast(dict[str, Any], agentic_outcome["analysis_input_summary"])
                    evidence_used = cast(list[dict[str, Any]], agentic_outcome["evidence_used"])
                    evidence_selection_summary = cast(dict[str, Any], agentic_outcome["evidence_selection_summary"])
                    gap = agentic_outcome["gap"]
                    structured_cv_initial = cast(dict[str, Any] | None, agentic_outcome["structured_cv_initial"])
                    validation_initial = cast(dict[str, Any] | None, agentic_outcome["validation_initial"])
                    repair_attempt = cast(dict[str, Any], agentic_outcome["repair_attempt"])
                    structured_cv_final = cast(dict[str, Any] | None, agentic_outcome["structured_cv_final"])
                    markdown_final = cast(str | None, agentic_outcome["markdown_final"])
                    job_runtime_provenance = cast(dict[str, Any] | None, agentic_outcome["job_runtime_provenance"])
                    job_cv_generation_model_value = cast(str | None, agentic_outcome["job_cv_generation_model_value"])
                    job_agentic_live_trace = cast(dict[str, Any] | None, agentic_outcome["job_agentic_live_trace"])
                    generation_attempt_count = int(agentic_outcome["generation_attempt_count"])
                    structured_cv = cast(dict[str, Any] | None, agentic_outcome["structured_cv"])
                    cv = str(agentic_outcome["cv"] or "")
                    validation = cast(dict[str, Any], agentic_outcome["validation"])
                else:
                    non_agentic_outcome = compute_outcome
                    fit = str(non_agentic_outcome["fit"] or fit)
                    analysis_input_summary = cast(dict[str, Any], non_agentic_outcome["analysis_input_summary"])
                    evidence_used = cast(list[dict[str, Any]], non_agentic_outcome["evidence_used"])
                    evidence_selection_summary = cast(dict[str, Any], non_agentic_outcome["evidence_selection_summary"])
                    gap = non_agentic_outcome["gap"]
                    structured_cv_initial = cast(dict[str, Any] | None, non_agentic_outcome["structured_cv_initial"])
                    validation_initial = cast(dict[str, Any] | None, non_agentic_outcome["validation_initial"])
                    repair_attempt = cast(dict[str, Any], non_agentic_outcome["repair_attempt"])
                    structured_cv_final = cast(dict[str, Any] | None, non_agentic_outcome["structured_cv_final"])
                    markdown_final = cast(str | None, non_agentic_outcome["markdown_final"])
                    job_runtime_provenance = cast(dict[str, Any] | None, non_agentic_outcome["job_runtime_provenance"])
                    job_cv_generation_model_value = cast(str | None, non_agentic_outcome["job_cv_generation_model_value"])
                    job_agentic_live_trace = cast(dict[str, Any] | None, non_agentic_outcome["job_agentic_live_trace"])
                    generation_attempt_count = int(non_agentic_outcome["generation_attempt_count"])
                    structured_cv = cast(dict[str, Any] | None, non_agentic_outcome["structured_cv"])
                    cv = str(non_agentic_outcome["cv"] or "")
                    validation = cast(dict[str, Any], non_agentic_outcome["validation"])

                generation_state["fit"] = fit
                generation_state["evidence_used"] = evidence_used
                generation_state["evidence_selection_summary"] = evidence_selection_summary
                generation_state["analysis_input_summary"] = analysis_input_summary
                generation_state["gap"] = gap
                generation_state["structured_cv_initial"] = structured_cv_initial
                generation_state["validation_initial"] = validation_initial
                generation_state["repair_attempt"] = repair_attempt
                generation_state["structured_cv_final"] = structured_cv_final
                generation_state["markdown_final"] = markdown_final
                generation_state["job_runtime_provenance"] = job_runtime_provenance
                generation_state["job_cv_generation_model_value"] = job_cv_generation_model_value
                generation_state["job_agentic_live_trace"] = job_agentic_live_trace
                generation_state["generation_attempt_count"] = generation_attempt_count

                should_continue, structured_cv_final, markdown_final = _execute_cv_generation_item(
                    generation_state=generation_state,
                    analysis_record=analysis_record,
                    validation=validation,
                    structured_cv=structured_cv,
                    cv=cv,
                    fit=fit,
                    gap=gap,
                    evidence=evidence,
                    job=job,
                    analysis_grounding=analysis_grounding,
                    cv_generation_started_monotonic=cv_generation_started_monotonic,
                    structured_cv_final=structured_cv_final,
                    markdown_final=markdown_final,
                    job_cv_generation_model_value=job_cv_generation_model_value,
                    job_runtime_provenance=job_runtime_provenance,
                    job_agentic_live_trace=job_agentic_live_trace,
                    generation_attempt_count=generation_attempt_count,
                    validation_initial=validation_initial,
                    repair_attempt=repair_attempt,
                    evidence_used=evidence_used,
                    evidence_selection_summary=evidence_selection_summary,
                    analysis_input_summary=analysis_input_summary,
                    cv_generation_input_fingerprint=reuse_fingerprint or None,
                )
                if should_continue:
                    continue

            except Exception as exc:  # per-job failure — log and skip, don't crash the run
                _handle_cv_generation_failure(
                    state=generation_state,
                    analysis_record=analysis_record,
                    structured_cv_final=structured_cv_final,
                    markdown_final=markdown_final,
                    cv_prompt_id_value=cv_prompt_id_value,
                    cv_prompt_template_path_value=cv_prompt_template_path_value,
                    enabled_cv_sections=enabled_cv_sections,
                    latency_ms=int((time.monotonic() - cv_generation_started_monotonic) * 1000),
                    run_id=run_id,
                    exc=exc,
                    cv_generation_input_fingerprint=reuse_fingerprint or None,
                )
                continue

        set_span_attributes(
            {
                "generation_ready_jobs": len(generation_ready_records),
                "generated_cvs": len(results),
                "cv_generation_review_required": sum(
                    1 for record in cv_generation_debug_records
                    if str(record.get("status") or "") == CV_GENERATION_REVIEW_REQUIRED_STATUS
                ),
                "cv_generation_failed": sum(
                    1 for record in cv_generation_debug_records
                    if str(record.get("status") or "") in {"validation_failed", "generation_failed", "persistence_failed"}
                ),
            }
        )
        state["cv_analysis_results"] = cv_analysis_results
        state["cv_results"] = results
        state["cv_generation_debug_records"] = cv_generation_debug_records
        stage_transition_artifacts = _build_stage_transition_artifacts(
            raw_jobs=raw_jobs,
            normalized=normalized,
            deduplicated_jobs=deduplicated_jobs,
            pre_filter_rejected_jobs=pre_filter_rejected_jobs,
            enriched=enriched,
            passed_jobs=passed_jobs,
            candidate_filter_rejected_jobs=candidate_filter_rejected_jobs,
            raw_shortlist=raw_shortlist,
            shortlist=shortlist,
            backfilled_job_urls=backfilled_job_urls,
            vector_top_n=vector_top_n,
            candidate_summary=candidate_summary,
            candidate_query_components=candidate_query_components,
            candidate_query_debug=candidate_query_debug,
            ai_scores=ai_scores,
            ranking_inputs=ranking_inputs,
            ranked=ranked,
            cv_analysis_results=cv_analysis_results,
            final_top_n=final_top_n,
            cv_generation_debug_records=cv_generation_debug_records,
            profile=profile,
            config=config,
        )
        late_stage_reuse_snapshots = _build_late_stage_reuse_snapshots(
            ai_scores=ai_scores,
            cv_analysis_results=cv_analysis_results,
        )
        cv_analysis_reached = len(ranked) > 0 or len(cv_analysis_results) > 0
        cv_generation_reached = any(
            str(record.get("status") or "") in {"accepted", CV_GENERATION_REVIEW_REQUIRED_STATUS, "validation_failed", "generation_failed", "persistence_failed"}
            for record in cv_generation_debug_records
        )
        late_stage_mode_payload = _build_late_stage_mode_payload(
            agentic_late_stage_enabled=agentic_late_stage_enabled,
            stage_reached=cv_analysis_reached or cv_generation_reached,
        )
        summary: dict[str, Any] = {
            "run_id": run_id,
            "total_jobs": len(raw_jobs),
            "passed_filter": len(passed_jobs),
            "ranked": len(ranked),
            "cvs_generated": len(results),
            "late_stage_mode": late_stage_mode_payload,
            "cv_analysis_trace": _build_cv_analysis_trace_summary(
                run_id=run_id,
                cv_analysis_results=cv_analysis_results,
                late_stage_mode=late_stage_mode_payload,
            ),
            "agentic_live_trace": _build_agentic_live_trace_summary(
                run_id=run_id,
                cv_generation_debug_records=cv_generation_debug_records,
                late_stage_mode=late_stage_mode_payload,
            ),
            "late_stage_reuse_snapshots": late_stage_reuse_snapshots,
            "cv_generation_debug_records": cv_generation_debug_records,
            "mapping_suggestions": _collect_mapping_suggestions(enriched, run_id),
            "stage_transition_artifacts": stage_transition_artifacts,
            "export_results": _build_export_results(
                raw_jobs=raw_jobs,
                enriched=enriched,
                deduplicated_jobs=deduplicated_jobs,
                pre_filter_rejected=pre_filter_rejected_jobs,
                candidate_filter_rejected=candidate_filter_rejected_jobs,
                passed_jobs=passed_jobs,
                raw_shortlist=raw_shortlist,
                shortlist_for_scoring=shortlist,
                ranking_inputs=ranking_inputs,
                ranked=ranked,
                cv_analysis_results=cv_analysis_results,
                cv_results=results,
                cv_generation_debug_records=cv_generation_debug_records,
                vector_search_top_n=vector_top_n,
            ),
        }
        logger.info("Pipeline run complete [run_id=%s] summary=%s", run_id, summary)
        if reporter is not None:
            analysis_quality = _build_cv_analysis_quality_metrics(cv_analysis_results)
            generation_quality = _build_cv_generation_quality_metrics(cv_generation_debug_records)
            late_stage_reuse_metrics = _build_late_stage_reuse_metrics(
                enriched=enriched,
                ai_scores=ai_scores,
                cv_analysis_results=cv_analysis_results,
                cv_generation_debug_records=cv_generation_debug_records,
            )
            reuse_anomaly = _reuse_anomaly_payload(
                reuse_metrics=late_stage_reuse_metrics,
                config=config,
            )
            total_retry_count = sum(
                max(0, int(record.get("attempt_count") or 1) - 1)
                for record in cv_generation_debug_records
                if isinstance(record, dict)
            )
            event_summary = {
                "run_id": run_id,
                "total_jobs": summary["total_jobs"],
                "passed_filter": summary["passed_filter"],
                "ranked": summary["ranked"],
                "cvs_generated": summary["cvs_generated"],
                "quality_summary": {
                    "acceptance_review_failure": {
                        "accepted": generation_quality.get("accepted"),
                        "review_required": generation_quality.get("review_required"),
                        "validation_failed": generation_quality.get("validation_failed"),
                        "generation_failed": generation_quality.get("generation_failed"),
                        "persistence_failed": generation_quality.get("persistence_failed"),
                        "accepted_rate": generation_quality.get("accepted_rate"),
                        "review_required_rate": generation_quality.get("review_required_rate"),
                        "failure_rate": _safe_rate(
                            int(generation_quality.get("validation_failed") or 0)
                            + int(generation_quality.get("generation_failed") or 0)
                            + int(generation_quality.get("persistence_failed") or 0),
                            int(generation_quality.get("total_attempted") or 0),
                        ),
                    },
                    "analysis_to_generation_conversion": {
                        "ready_for_generation": analysis_quality.get("ready_for_generation"),
                        "generation_attempted": generation_quality.get("total_attempted"),
                        "conversion_rate": _safe_rate(
                            int(generation_quality.get("total_attempted") or 0),
                            int(analysis_quality.get("ready_for_generation") or 0),
                        ),
                    },
                    "retry_counts": {
                        "total_retry_count": total_retry_count,
                        "attempted_jobs": generation_quality.get("total_attempted"),
                    },
                },
                "late_stage_reuse_metrics": late_stage_reuse_metrics,
            }
            reporter.emit(
                "pipeline_compute_complete",
                "info",
                (
                    "Pipeline compute complete: "
                    f"ranked={summary['ranked']}, "
                    f"attempted={generation_quality.get('total_attempted')}, "
                    f"accepted={generation_quality.get('accepted')}, "
                    f"review_required={generation_quality.get('review_required')}, "
                    f"failed={int(generation_quality.get('validation_failed') or 0) + int(generation_quality.get('generation_failed') or 0) + int(generation_quality.get('persistence_failed') or 0)}, "
                    f"retries={total_retry_count}"
                ),
                _bounded_event_payload(
                    event_name="pipeline_compute_complete",
                    event_family="summary",
                    source_stage="cv_generation",
                    event_status="completed",
                    input_snapshot={
                        "total_jobs": summary["total_jobs"],
                        "passed_filter": summary["passed_filter"],
                        "ranked": summary["ranked"],
                    },
                    output_snapshot={
                        "cvs_generated": summary["cvs_generated"],
                        "quality_summary": event_summary["quality_summary"],
                    },
                ),
            )  # type: ignore[union-attr]
            if reuse_anomaly is not None:
                reporter.emit(
                    "reuse_anomaly",
                    "warning",
                    "Reuse anomaly detected: overlap present but reuse under floor",
                    _bounded_event_payload(
                        event_name="reuse_anomaly",
                        event_family="diagnostic",
                        source_stage="cv_generation",
                        event_status="warning",
                        output_snapshot=reuse_anomaly,
                    ),
                )  # type: ignore[union-attr]
    return summary















