"""
@meta
name: fitcv_pipeline
type: utility
domain: pipeline
responsibility:
  - Orchestrate FitCV stages from ingestion through ranking, CV analysis, and CV generation.
  - Build run-owned stage artifacts, compact exports, and reranker skip short-circuit records.
inputs:
  - jobs JSON
  - candidate profile
  - effective runtime config
outputs:
  - ranked jobs, generated CVs, results ledger, diagnostics, and stage artifacts
capabilities:
  - pipeline_performance.pre-enrichment-global-job-filters-applications-count-max-max-age-days
  - pipeline_performance.operator-facing-enriched-job-exports-now-keep-canonical-semantic-fields-and-fingerprint-reuse-provenance-while-omitting-retired-raw-duplicate-classification-baggage
  - pipeline_performance.large-runs-avoid-some-row-scaled-layer-4-event-noise-by-relying-more-on-aggregate-stage-summaries-plus-stage-owned-artifacts
  - pipeline_performance.cv-analysis-now-uses-bounded-semantic-lift-for-required-skill-and-role-channels-instead-of-reserving-semantic-work-only-for-domain-and-responsibility-alignment
  - pipeline_performance.ranked-jobs-with-authoritative-reranker-fit-label-skip-now-stop-before-evidence-retrieval-gap-computation-and-semantic-alignment-inside-cv-analysis
tags:
  - pipeline
  - orchestration
lifecycle:
  status: active
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
v1 embeds only rule-passing jobs (cheaper, faster).  A future
config["pipeline"]["embed_scope"] key (filtered_only | all_enriched_jobs)
can make this configurable without code changes.
"""

import logging
import uuid
from copy import deepcopy
from typing import Any, Callable

from fitcv.ai_score import build_ai_score_input_fingerprint, run_ai_scoring
from fitcv.candidate import (
    flatten_skills,
    infer_effective_preferences,
    load_candidate_to_bigquery,
    load_profile_json_text,
    load_profile_yaml,
)
from fitcv.config import (
    CV_SECTION_KEY_TO_NAME,
    get_cv_generation_model,
    get_cv_generation_prompt_version,
    get_gemini_model,
    load_config,
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
    compute_seniority_fit,
    compute_title_relevance,
    get_active_missing_value_defaults,
    get_preference_fit_weights,
    get_active_ranking_weights,
    rank_jobs,
    store_final_ranking,
)
from fitcv.rule_filter import (
    apply_pre_enrichment_global_filters,
    apply_rule_filters,
    store_filter_results,
)
from fitcv.tracker import create_cv_version_record, store_cv_version
from fitcv.validator import AnalysisGroundingPayload, run_all_validations
from fitcv.vector_search import run_vector_search

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


def _extract_job_url(job: dict[str, Any]) -> str:
    return str(job.get("job_url") or job.get("jobUrl") or "")


def _extract_job_title(job: dict[str, Any]) -> str:
    return str(job.get("title") or job.get("job_title") or "")


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


def _normalize_shortlist_row(shortlist_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "vector_similarity": shortlist_row.get("vector_similarity", shortlist_row.get("similarity_score")),
        "vector_rank": shortlist_row.get("vector_rank", shortlist_row.get("rank")),
        "shortlist_origin": str(shortlist_row.get("shortlist_origin") or "vector_search"),
    }


def _shortlist_outcome_for_row(
    *,
    raw_hit_present: bool,
    shortlist_origin: str,
    retrieval_anomaly_present: bool = False,
) -> str:
    normalized_origin = shortlist_origin.strip().lower()
    if retrieval_anomaly_present:
        return "raw_hit_excluded_from_scoring"
    if normalized_origin == "backfill":
        return "backfilled_for_scoring"
    if raw_hit_present:
        return "returned_by_vector_search"
    return "not_returned_in_raw_hits"


def _unique_job_urls(rows: list[dict[str, Any]]) -> list[str]:
    urls: list[str] = []
    seen_urls: set[str] = set()
    for row in rows:
        job_url = _extract_job_url(row)
        if not job_url or job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        urls.append(job_url)
    return urls


def _raw_shortlist_anomaly_urls(
    raw_shortlist: list[dict[str, Any]],
    passed_jobs: list[dict[str, Any]],
) -> list[str]:
    passed_job_urls = {_extract_job_url(job) for job in passed_jobs if _extract_job_url(job)}
    return [
        job_url for job_url in _unique_job_urls(raw_shortlist)
        if job_url not in passed_job_urls
    ]


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
        _extract_job_url(job): job
        for job in passed_jobs
        if _extract_job_url(job)
    }
    scoring_shortlist: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for row in raw_shortlist:
        job_url = _extract_job_url(row)
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
                **_normalize_shortlist_row(row),
                "vector_rank": len(scoring_shortlist) + 1,
                "shortlist_origin": "vector_search",
            }
        )

    next_rank = len(scoring_shortlist) + 1
    for job in passed_jobs:
        if len(scoring_shortlist) >= vector_search_top_n:
            break
        job_url = _extract_job_url(job)
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


def _enrich_jobs_with_reuse(
    normalized_jobs: list[dict[str, Any]],
    config: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not normalized_jobs:
        return [], []

    raw_job_fingerprints: dict[str, str] = {}
    for job in normalized_jobs:
        job_url = _extract_job_url(job)
        if not job_url:
            continue
        raw_job_fingerprints[job_url] = build_raw_job_fingerprint(job)["fingerprint"]

    enrich_contract_fingerprint = build_enrich_contract_fingerprint(config)["fingerprint"]
    reused_rows_by_url = lookup_reusable_structured_jobs(
        normalized_jobs,
        config,
        raw_job_fingerprints=raw_job_fingerprints,
        enrich_contract_fingerprint=enrich_contract_fingerprint,
    )

    fresh_jobs = [
        job for job in normalized_jobs
        if _extract_job_url(job) and _extract_job_url(job) not in reused_rows_by_url
    ]
    fresh_rows: list[dict[str, Any]] = []
    if fresh_jobs:
        fresh_rows = enrich_batch(fresh_jobs, config)
        for row in fresh_rows:
            job_url = _extract_job_url(row)
            if not job_url:
                continue
            row["raw_job_fingerprint"] = raw_job_fingerprints.get(job_url)
            row["enrich_contract_fingerprint"] = enrich_contract_fingerprint
            row["enrich_reuse_status"] = FRESH_ENRICHMENT_STATUS

    fresh_rows_by_url = {
        _extract_job_url(row): row
        for row in fresh_rows
        if _extract_job_url(row)
    }
    enriched_rows: list[dict[str, Any]] = []
    for job in normalized_jobs:
        job_url = _extract_job_url(job)
        if not job_url:
            continue
        reused_row = reused_rows_by_url.get(job_url)
        if reused_row is not None:
            reused_row["raw_job_fingerprint"] = raw_job_fingerprints.get(job_url)
            reused_row["enrich_contract_fingerprint"] = enrich_contract_fingerprint
            reused_row["enrich_reuse_status"] = REUSED_CACHED_ENRICHMENT_STATUS
            enriched_rows.append(reused_row)
            continue
        fresh_row = fresh_rows_by_url.get(job_url)
        if fresh_row is not None:
            enriched_rows.append(fresh_row)
    return enriched_rows, fresh_rows


def _merge_ranked_job_with_enriched_context(
    ranked_job: dict[str, Any],
    enriched_by_url: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    job_url = _extract_job_url(ranked_job)
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
    original_by_url = {_extract_job_url(job): job for job in raw_jobs if _extract_job_url(job)}
    enriched_by_url = {_extract_job_url(job): job for job in enriched if _extract_job_url(job)}
    passed_by_url = {_extract_job_url(job): job for job in passed_jobs if _extract_job_url(job)}
    raw_shortlist_by_url = {
        _extract_job_url(job): _normalize_shortlist_row(job)
        for job in raw_shortlist
        if _extract_job_url(job)
    }
    scoring_shortlist_by_url = {
        _extract_job_url(job): _normalize_shortlist_row(job)
        for job in shortlist_for_scoring
        if _extract_job_url(job)
    }
    scoring_by_url = {_extract_job_url(job): job for job in ranking_inputs if _extract_job_url(job)}
    ranked_by_url = {_extract_job_url(job): job for job in ranked if _extract_job_url(job)}
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
        _extract_job_url(job): list(job.get("marks") or [])
        for job in passed_jobs
        if _extract_job_url(job)
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
        job_url = _extract_job_url(raw_job)
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

        rows.append(
            {
                "job_url": job_url,
                "job_title": _extract_job_title(enriched_job or raw_job or {}),
                "company": (enriched_job or raw_job or {}).get("company_name")
                or (enriched_job or raw_job or {}).get("companyName"),
                "location_type": (enriched_job or {}).get("location_type"),
                "seniority": (enriched_job or {}).get("seniority"),
                "job_family": (enriched_job or {}).get("job_family"),
                "domain": (enriched_job or {}).get("domain"),
                "pipeline_status": pipeline_status,
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


def _json_safe_pipeline_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe_pipeline_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_pipeline_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_pipeline_value(item) for item in value]
    if isinstance(value, set):
        return [_json_safe_pipeline_value(item) for item in sorted(value)]
    return value


def _normalize_late_stage_reuse_snapshots(reuse_snapshots: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    payload = dict(reuse_snapshots or {})
    return {
        "ranking_ai_scores": [
            dict(item)
            for item in list(payload.get("ranking_ai_scores") or [])
            if isinstance(item, dict)
        ],
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


def _build_late_stage_reuse_metrics(
    *,
    ai_scores: list[dict[str, Any]],
    cv_analysis_results: list[dict[str, Any]],
) -> dict[str, Any]:
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
    return {
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
    return {
        "run_id": run_id,
        "raw_jobs": [],
        "normalized": [],
        "deduplicated_jobs": [],
        "pre_filter_rejected_jobs": [],
        "enriched": [],
        "passed_jobs": [],
        "candidate_filter_rejected_jobs": [],
        "raw_shortlist": [],
        "shortlist": [],
        "backfilled_job_urls": [],
        "candidate_query_debug": {},
        "ai_scores": [],
        "ranking_inputs": [],
        "ranked": [],
        "cv_analysis_results": [],
        "cv_results": [],
        "cv_generation_debug_records": [],
    }


def _restore_pipeline_state(
    *,
    run_id: str,
    checkpoint_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    state = _empty_pipeline_state(run_id)
    payload = checkpoint_payload or {}
    for key in state:
        if key == "run_id":
            continue
        value = payload.get(key)
        if isinstance(state[key], list):
            state[key] = list(value or [])
        elif value is not None:
            state[key] = value
    return state


def _checkpoint_payload_from_state(state: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "raw_jobs",
        "normalized",
        "deduplicated_jobs",
        "pre_filter_rejected_jobs",
        "enriched",
        "passed_jobs",
        "candidate_filter_rejected_jobs",
        "raw_shortlist",
        "shortlist",
        "backfilled_job_urls",
        "candidate_query_debug",
        "ai_scores",
        "ranking_inputs",
        "ranked",
        "cv_analysis_results",
        "cv_generation_debug_records",
    )
    return {
        key: _json_safe_pipeline_value(state.get(key) or [])
        for key in keys
    }


def _collect_mapping_suggestions(enriched: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for job in enriched:
        job_url = _extract_job_url(job)
        job_title = _extract_job_title(job)
        for suggestion in list(job.get("mapping_suggestions") or []):
            if not isinstance(suggestion, dict):
                continue
            record = {
                "run_id": run_id,
                "job_url": job_url,
                "job_title": job_title,
                "must_have_skill": str(suggestion.get("must_have_skill") or ""),
                "matches": bool(suggestion.get("matches")),
                "confidence": float(suggestion.get("confidence") or 0.0),
                "alias": str(suggestion.get("alias") or ""),
                "canonical": str(suggestion.get("canonical") or ""),
            }
            if record["alias"] and record["canonical"]:
                dedupe_key = (
                    record["alias"].strip().lower(),
                    record["canonical"].strip().lower(),
                    record["must_have_skill"].strip().lower(),
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
        vector_top_n if vector_top_n is not None else config.get("pipeline", {}).get("vector_search_top_n", 0)
    )
    final_top_n_value = int(
        final_top_n if final_top_n is not None else config.get("pipeline", {}).get("final_top_n", 0)
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
    summary["paused_after_stage"] = paused_after_stage
    summary["checkpoint_payload"] = _checkpoint_payload_from_state(state)
    return summary


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


def _fit_label_from_ai_score(score: float, config: dict[str, Any]) -> str:
    thresholds = dict(config.get("fit_label_thresholds") or {})
    strong_threshold = float(thresholds.get("strong", 0.70))
    stretch_threshold = float(thresholds.get("stretch", 0.40))
    if score >= strong_threshold:
        return "strong"
    if score >= stretch_threshold:
        return "stretch"
    return "skip"


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
    return _fit_label_from_ai_score(float(raw_ai_score), config)


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
    shortlist_origin = str(job.get("shortlist_origin") or "").strip().lower()
    if shortlist_origin == "backfill":
        return "backfilled_for_scoring"
    return "returned_by_vector_search"


def _validation_status_for_cv_status(status: str) -> str:
    if status == "accepted":
        return "accepted"
    if status == "validation_failed":
        return "failed"
    if status == "persistence_failed":
        return "accepted"
    return "not_run"


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
    cv_prompt_id: str | None,
    cv_prompt_template_path: str | None,
    error: dict[str, str] | None,
) -> dict[str, Any]:
    ranking_fit_label = str(fit_classification or "").strip() or None
    ranking_fit_source = str(job.get("fit_label_source") or "reranker").strip() or None
    cv_analysis_status = status
    cv_generation_status = status
    if status in {"accepted", "validation_failed", "generation_failed", "persistence_failed"}:
        cv_analysis_status = "ready_for_generation"
    elif status == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS:
        cv_generation_status = "not_attempted"
    decision_chain = _build_decision_chain(
        shortlist_status=_shortlist_status_for_ranked_job(job),
        advanced_to_scoring=True,
        ranking_fit_label=ranking_fit_label,
        ranking_fit_source=ranking_fit_source,
        cv_analysis_status=cv_analysis_status,
        cv_status=cv_generation_status,
    )
    return {
        "job_url": str(job.get("job_url") or ""),
        "job_title": _extract_job_title(job),
        "status": status,
        "ranking_fit_label": ranking_fit_label,
        "fit_classification": fit_classification,
        "decision_chain": decision_chain,
        "analysis_input_summary": dict(analysis_input_summary or {}),
        "evidence_used": evidence_used,
        "evidence_selection_summary": dict(evidence_selection_summary or {}),
        "gap_summary": gap_summary,
        "structured_cv_initial": structured_cv_initial,
        "validation_initial": validation_initial,
        "repair_attempt": repair_attempt,
        "structured_cv_final": structured_cv_final,
        "markdown_final": markdown_final,
        "enabled_sections": list(enabled_sections or []),
        "cv_generation_model": cv_generation_model,
        "cv_prompt_id": cv_prompt_id,
        "cv_prompt_template_path": cv_prompt_template_path,
        # Reranker blocks and fit-gate skips are expected outcomes, not generation runtime errors.
        "outcome_reason": error if status in {"skipped_fit_gate", CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS} else None,
        "error": error if status not in {"skipped_fit_gate", CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS} else None,
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
) -> dict[str, Any]:
    ranking_fit_label = str(fit_classification or "").strip() or None
    ranking_fit_source = str(job.get("fit_label_source") or "reranker").strip() or None
    if status == "ready_for_generation":
        cv_status = "not_attempted"
    elif status == "skipped_fit_gate":
        cv_status = "skipped_fit_gate"
    elif status == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS:
        cv_status = "not_attempted"
    else:
        cv_status = "failed"
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
        "job_title": _extract_job_title(job),
        "status": status,
        "analysis_input_fingerprint": analysis_input_fingerprint,
        "analysis_reuse_status": analysis_reuse_status,
        "ranking_fit_label": ranking_fit_label,
        "fit_classification": fit_classification,
        "decision_chain": decision_chain,
        "job_snapshot": dict(job),
        "evidence_payload": list(evidence_payload),
        "evidence_used": evidence_used,
        "evidence_selection_summary": dict(evidence_selection_summary or {}),
        "gap_summary": gap_summary,
        # Reranker blocks and fit-gate skips are expected analysis outcomes, not runtime errors.
        "outcome_reason": error if status in {"skipped_fit_gate", CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS} else None,
        "error": error if status not in {"skipped_fit_gate", CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS} else None,
    }


def _stage_block_not_reached(stage: str) -> dict[str, Any]:
    return {
        "stage_id": stage,
        "stage": stage,
        "status": "not_reached",
        "input_counts": {},
        "output_counts": {},
        "decision_summary": {},
        "outputs_sample": [],
        "dropped_or_changed_sample": [],
        "inputs_sample": [],
    }


def _truncate_stage_text(value: str, *, limit: int = _STAGE_ARTIFACT_TEXT_LIMIT) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "...[truncated]"


def _truncate_stage_value(value: Any) -> Any:
    if isinstance(value, str):
        return _truncate_stage_text(value)
    if isinstance(value, list):
        return [_truncate_stage_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _truncate_stage_value(inner)
            for key, inner in value.items()
        }
    return value


def _sample_rows(
    rows: list[Any],
    row_builder: Callable[[Any], dict[str, Any] | None],
    *,
    limit: int = _STAGE_ARTIFACT_SAMPLE_LIMIT,
) -> list[dict[str, Any]]:
    sampled: list[dict[str, Any]] = []
    for row in rows:
        built = row_builder(row)
        if not built:
            continue
        sampled.append(_truncate_stage_value(built))
        if len(sampled) >= limit:
            break
    return sampled


def _sample_strings(values: list[str], *, limit: int = _STAGE_ARTIFACT_SAMPLE_LIMIT) -> list[str]:
    return [_truncate_stage_text(value) for value in values[:limit] if value]


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


def _build_ranking_quality_metrics(ranking_inputs: list[dict[str, Any]]) -> dict[str, Any]:
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
    generation_ready = 0
    skipped_fit_gate = 0
    analysis_failed = 0
    for record in cv_analysis_results:
        status = str(record.get("status") or "").strip().lower()
        if status in {"ready_for_generation", "generation_ready"}:
            generation_ready += 1
        elif status == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS:
            blocked_by_reranker_fit += 1
        elif status == "skipped_fit_gate":
            skipped_fit_gate += 1
        elif status == "analysis_failed":
            analysis_failed += 1
    total_processed = generation_ready + blocked_by_reranker_fit + skipped_fit_gate + analysis_failed
    return {
        "blocked_by_reranker_fit_rate": _safe_rate(blocked_by_reranker_fit, total_processed),
        "skip_rate": _safe_rate(skipped_fit_gate, total_processed),
        "generation_ready_rate": _safe_rate(generation_ready, total_processed),
        "analysis_failed_rate": _safe_rate(analysis_failed, total_processed),
        "blocked_by_reranker_fit": blocked_by_reranker_fit,
        "skipped_fit_gate": skipped_fit_gate,
        "generation_ready": generation_ready,
        "analysis_failed": analysis_failed,
        "total_processed": total_processed,
    }


def _build_cv_generation_quality_metrics(
    cv_generation_debug_records: list[dict[str, Any]],
) -> dict[str, Any]:
    accepted = 0
    validation_failed = 0
    generation_failed = 0
    persistence_failed = 0
    for record in cv_generation_debug_records:
        status = str(record.get("status") or "").strip().lower()
        if status == "accepted":
            accepted += 1
        elif status == "validation_failed":
            validation_failed += 1
        elif status == "generation_failed":
            generation_failed += 1
        elif status == "persistence_failed":
            persistence_failed += 1
    total_attempted = accepted + validation_failed + generation_failed + persistence_failed
    return {
        "validation_fail_rate": _safe_rate(validation_failed, total_attempted),
        "accepted_rate": _safe_rate(accepted, total_attempted),
        "generation_failed_rate": _safe_rate(generation_failed, total_attempted),
        "persistence_failed_rate": _safe_rate(persistence_failed, total_attempted),
        "accepted": accepted,
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
    job_url = _extract_job_url(job)
    if not job_url:
        return None
    sample = {
        "job_url": job_url,
        "job_title": _extract_job_title(job),
        "company": str(job.get("company_name") or job.get("companyName") or ""),
    }
    optional_fields: dict[str, Any] = {}
    for field in _EXPORT_ENRICHED_JOB_FIELDS:
        value = job.get(field)
        optional_fields[field] = value
    for key, value in optional_fields.items():
        if value not in (None, "", []):
            sample[key] = value
    marks = list(job.get("marks") or [])
    if marks:
        sample["marks"] = marks
    return sample


def _candidate_profile_summary(profile: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
    preferences = dict(profile.get("preferences") or {})
    preference_resolution = infer_effective_preferences(profile, config)
    flattened_skills = flatten_skills(profile)
    summary = {
        "target_role": str(preferences.get("target_role") or ""),
        "effective_target_role": str(preference_resolution["effective_preferences"].get("target_role") or ""),
        "effective_role_families": list(preference_resolution["effective_preferences"].get("role_families") or []),
        "effective_domains": list(preference_resolution["effective_preferences"].get("domains") or []),
        "preference_sources": dict(preference_resolution["preference_sources"] or {}),
        "years_experience": profile.get("years_experience"),
        "skills_sample": flattened_skills[:5],
    }
    return {key: value for key, value in summary.items() if value not in (None, "", [])}


def _shortlist_row_sample(row: dict[str, Any]) -> dict[str, Any] | None:
    job_url = _extract_job_url(row)
    if not job_url:
        return None
    shortlist_origin = str(row.get("shortlist_origin") or "vector_search")
    raw_hit_present = bool(row.get("raw_hit_present", shortlist_origin != "backfill"))
    retrieval_anomaly_present = bool(row.get("retrieval_anomaly_present", False))
    sample = {
        "job_url": job_url,
        "job_title": _extract_job_title(row),
        "vector_similarity": row.get("vector_similarity", row.get("similarity_score")),
        "vector_rank": row.get("vector_rank", row.get("rank")),
        "shortlist_origin": shortlist_origin,
        "shortlist_outcome": _shortlist_outcome_for_row(
            raw_hit_present=raw_hit_present,
            shortlist_origin=shortlist_origin,
            retrieval_anomaly_present=retrieval_anomaly_present,
        ),
        "raw_hit_present": raw_hit_present,
        "retrieval_anomaly_present": retrieval_anomaly_present,
        "embedding_reuse_status": row.get("embedding_reuse_status"),
        "embedding_input_signature": row.get("embedding_input_signature"),
        "embedding_contract_fingerprint": row.get("embedding_contract_fingerprint"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "")}


def _rule_filter_decision_sample(
    row: dict[str, Any],
    *,
    filter_outcome: str,
) -> dict[str, Any] | None:
    base = _job_sample(row)
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
    job_url = _extract_job_url(row)
    if not job_url:
        return None
    sample = {
        "job_url": job_url,
        "job_title": _extract_job_title(row),
        "ai_score": row.get("ai_score"),
        "ai_score_reuse_status": row.get("ai_score_reuse_status"),
        "ai_score_input_fingerprint": row.get("ai_score_input_fingerprint"),
        "must_have_match": row.get("must_have_match"),
        "vector_similarity": row.get("vector_similarity"),
        "title_relevance": row.get("title_relevance"),
        "seniority_fit": row.get("seniority_fit"),
        "preference_fit": row.get("preference_fit"),
        "feature_contributions": row.get("feature_contributions"),
        "preference_fit_components": row.get("preference_fit_components"),
        "effective_target_role": ((row.get("effective_preferences") or {}).get("target_role") if isinstance(row.get("effective_preferences"), dict) else None),
        "effective_role_families": ((row.get("effective_preferences") or {}).get("role_families") if isinstance(row.get("effective_preferences"), dict) else None),
        "effective_domains": ((row.get("effective_preferences") or {}).get("domains") if isinstance(row.get("effective_preferences"), dict) else None),
        "preference_sources": row.get("preference_sources"),
        "final_score": row.get("final_score"),
        "ranking_fit_label": row.get("fit_label"),
        "shortlist_origin": row.get("shortlist_origin"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "")}


def _analysis_record_output_sample(record: dict[str, Any]) -> dict[str, Any] | None:
    status = str(record.get("status") or "")
    if status != "ready_for_generation":
        return None
    job_url = str(record.get("job_url") or "")
    if not job_url:
        return None
    sample = {
        "job_url": job_url,
        "job_title": str(record.get("job_title") or ""),
        "status": status,
        "analysis_reuse_status": record.get("analysis_reuse_status"),
        "analysis_input_fingerprint": record.get("analysis_input_fingerprint"),
        "ranking_fit_label": record.get("ranking_fit_label"),
        "fit_classification": record.get("fit_classification"),
        "evidence_used": record.get("evidence_used"),
        "evidence_selection_summary": record.get("evidence_selection_summary"),
        "gap_summary": record.get("gap_summary"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "", [])}


def _analysis_record_changed_sample(record: dict[str, Any]) -> dict[str, Any] | None:
    status = str(record.get("status") or "")
    if status == "ready_for_generation":
        return None
    job_url = str(record.get("job_url") or "")
    if not job_url:
        return None
    sample = {
        "job_url": job_url,
        "job_title": str(record.get("job_title") or ""),
        "change_type": status,
        "analysis_reuse_status": record.get("analysis_reuse_status"),
        "analysis_input_fingerprint": record.get("analysis_input_fingerprint"),
        "ranking_fit_label": record.get("ranking_fit_label"),
        "fit_classification": record.get("fit_classification"),
        "evidence_used": record.get("evidence_used"),
        "evidence_selection_summary": record.get("evidence_selection_summary"),
        "gap_summary": record.get("gap_summary"),
        "outcome_reason": record.get("outcome_reason") or record.get("error"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "", [])}


def _debug_record_output_sample(record: dict[str, Any]) -> dict[str, Any] | None:
    status = str(record.get("status") or "")
    if status not in {"accepted", "persistence_failed"}:
        return None
    job_url = str(record.get("job_url") or "")
    if not job_url:
        return None
    sample = {
        "job_url": job_url,
        "job_title": str(record.get("job_title") or ""),
        "status": status,
        "ranking_fit_label": record.get("ranking_fit_label"),
        "fit_classification": record.get("fit_classification"),
        "analysis_input_summary": record.get("analysis_input_summary"),
        "evidence_used": record.get("evidence_used"),
        "evidence_selection_summary": record.get("evidence_selection_summary"),
        "gap_summary": record.get("gap_summary"),
        "validation_initial": record.get("validation_initial"),
        "repair_attempt": record.get("repair_attempt"),
        "enabled_sections": record.get("enabled_sections"),
        "cv_generation_model": record.get("cv_generation_model"),
        "cv_prompt_id": record.get("cv_prompt_id"),
        "cv_prompt_template_path": record.get("cv_prompt_template_path"),
        "structured_cv_final": record.get("structured_cv_final"),
        "markdown_final": record.get("markdown_final"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "", [])}


def _debug_record_changed_sample(record: dict[str, Any]) -> dict[str, Any] | None:
    status = str(record.get("status") or "")
    if status in {"accepted", "persistence_failed"}:
        return None
    job_url = str(record.get("job_url") or "")
    if not job_url:
        return None
    sample = {
        "job_url": job_url,
        "job_title": str(record.get("job_title") or ""),
        "change_type": status,
        "ranking_fit_label": record.get("ranking_fit_label"),
        "fit_classification": record.get("fit_classification"),
        "analysis_input_summary": record.get("analysis_input_summary"),
        "evidence_used": record.get("evidence_used"),
        "evidence_selection_summary": record.get("evidence_selection_summary"),
        "gap_summary": record.get("gap_summary"),
        "validation_initial": record.get("validation_initial"),
        "repair_attempt": record.get("repair_attempt"),
        "enabled_sections": record.get("enabled_sections"),
        "cv_generation_model": record.get("cv_generation_model"),
        "cv_prompt_id": record.get("cv_prompt_id"),
        "cv_prompt_template_path": record.get("cv_prompt_template_path"),
        "structured_cv_final": record.get("structured_cv_final"),
        "error": record.get("error"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "", [])}


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
) -> dict[str, Any]:
    block = {
        "stage_id": stage_id,
        "stage": stage_id,
        "status": status,
        "input_counts": input_counts,
        "output_counts": output_counts,
        "decision_summary": decision_summary,
        "outputs_sample": outputs_sample,
        "dropped_or_changed_sample": dropped_or_changed_sample,
        "inputs_sample": inputs_sample,
    }
    if settings_refs:
        block["settings_refs"] = settings_refs
    return _truncate_stage_value(block)


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
        if str(record.get("status") or "") in {"accepted", "validation_failed", "generation_failed", "persistence_failed"}
    ]
    cv_generation_reached = len(generation_execution_records) > 0
    raw_shortlist_urls = set(_unique_job_urls(raw_shortlist))
    raw_shortlist_anomaly_urls = _raw_shortlist_anomaly_urls(raw_shortlist, passed_jobs)
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
    }
    shortlist_candidate_query_debug = {
        key: value
        for key, value in shortlist_candidate_query_debug.items()
        if value not in ("", None)
    }
    ranked_urls = {_extract_job_url(job) for job in ranked if _extract_job_url(job)}
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
    ranking_quality_metrics = _build_ranking_quality_metrics(ranking_inputs)
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
    cv_analysis_reuse_metrics = {
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

    return {
        "schema_version": "stage_transition_artifacts_v6",
        "stages": {
            "normalize": _stage_block(
                stage_id="normalize",
                status="completed",
                input_counts={"raw_jobs": len(raw_jobs)},
                output_counts={
                    "normalized_jobs": len(normalized),
                    "deduplicated_jobs": len(deduplicated_jobs),
                },
                decision_summary={"dedupe_reason_counts": dedupe_reason_counts},
                inputs_sample=_sample_rows(raw_jobs, _job_sample),
                outputs_sample=_sample_rows(normalized, _job_sample),
                dropped_or_changed_sample=_sample_rows(
                    deduplicated_jobs,
                    lambda job: {
                        **(_job_sample(job) or {}),
                        "change_type": "deduplicated_before_enrichment",
                        "dedupe_reason": _DEDUPE_REASON_LABELS.get(str(job.get("dedupe_reason") or ""), "deduplicated"),
                    } if _job_sample(job) else None,
                ),
            ),
            "enrich": _stage_block(
                stage_id="enrich",
                status="completed",
                input_counts={
                    "normalized_jobs": len(normalized),
                    "jobs_entering_enrichment": len(normalized) - len(pre_filter_rejected_jobs),
                },
                output_counts={
                    "enriched_jobs": len(enriched),
                    "pre_enrichment_rejected_jobs": len(pre_filter_rejected_jobs),
                },
                decision_summary={
                    "candidate_profile_summary": _candidate_profile_summary(profile, config),
                    "enrich_prompt_id": enrich_prompt_provenance["prompt_id"],
                    "enrich_prompt_version": enrich_prompt_provenance["prompt_version"],
                    "enrich_prompt_template_path": enrich_prompt_provenance["template_path"],
                    "enrich_prompt_model": enrich_prompt_provenance["model"],
                    **enrich_reuse_counts,
                },
                inputs_sample=_sample_rows(
                    [job for job in normalized if _extract_job_url(job) not in {_extract_job_url(item) for item in pre_filter_rejected_jobs}],
                    _job_sample,
                ),
                outputs_sample=_sample_rows(enriched, _job_sample),
                dropped_or_changed_sample=_sample_rows(
                    pre_filter_rejected_jobs,
                    lambda job: {
                        **(_job_sample(job) or {}),
                        "change_type": "rejected_before_enrichment",
                        "reasons": list(job.get("reasons") or []),
                    } if _job_sample(job) else None,
                ),
            ),
            "rule_filter": _stage_block(
                stage_id="rule_filter",
                status="completed",
                input_counts={"enriched_jobs": len(enriched)},
                output_counts={
                    "passed_jobs": len(passed_jobs),
                    "rejected_jobs": len(candidate_filter_rejected_jobs),
                },
                decision_summary={
                    "reject_reason_counts": grouped_reject_reasons,
                    "mark_code_counts": grouped_mark_codes,
                    "selected_filters": selected_rule_filters,
                },
                inputs_sample=_sample_rows(enriched, _job_sample),
                outputs_sample=_sample_rows(
                    passed_jobs,
                    lambda job: _rule_filter_decision_sample(job, filter_outcome="pass"),
                ),
                dropped_or_changed_sample=_sample_rows(
                    candidate_filter_rejected_jobs,
                    lambda job: (
                        {
                            **(_rule_filter_decision_sample(job, filter_outcome="reject") or {}),
                            "change_type": "rejected_after_enrichment",
                        }
                        if _rule_filter_decision_sample(job, filter_outcome="reject")
                        else None
                    ),
                ),
            ),
            "shortlist": _stage_block(
                stage_id="shortlist",
                status="completed" if shortlist_reached else "not_reached",
                input_counts={"passed_jobs": len(passed_jobs)},
                output_counts={
                    "raw_vector_rows": len(raw_shortlist),
                    "raw_vector_unique_jobs": len(raw_shortlist_urls),
                    "raw_vector_hits": len(raw_shortlist_urls),
                    "scoring_shortlist_jobs": len(shortlist),
                    "backfilled_jobs": len(backfilled_job_urls),
                    "retrieval_anomalies": len(raw_shortlist_anomaly_urls),
                    **shortlist_embedding_reuse_counts,
                },
                decision_summary={
                    "candidate_query_text": candidate_summary,
                    "candidate_query_components": shortlist_candidate_query_components,
                    **shortlist_candidate_query_debug,
                    "quality_metrics": shortlist_quality_metrics,
                    "vector_search_top_n": vector_top_n,
                    "jobs_not_returned_in_raw_hits": len(
                        [job for job in passed_jobs if _extract_job_url(job) not in raw_shortlist_urls]
                    ),
                    **shortlist_embedding_reuse_counts,
                    "raw_shortlist_anomaly_urls": _sample_strings(raw_shortlist_anomaly_urls),
                    "backfilled_job_urls": _sample_strings(backfilled_job_urls),
                },
                inputs_sample=_sample_rows(passed_jobs, _job_sample),
                outputs_sample=_sample_rows(shortlist, _shortlist_row_sample),
                dropped_or_changed_sample=_sample_rows(
                    [
                        *[
                            {
                                **job,
                                "change_type": "not_returned_in_raw_hits",
                                "shortlist_outcome": "not_returned_in_raw_hits",
                                "raw_hit_present": False,
                                "retrieval_anomaly_present": False,
                            }
                            for job in passed_jobs
                            if (
                                _extract_job_url(job) not in raw_shortlist_urls
                                and _extract_job_url(job) not in backfilled_job_urls
                            )
                        ],
                        *[
                            {
                                "job_url": job_url,
                                **next(
                                    (job for job in shortlist if _extract_job_url(job) == job_url),
                                    {"title": next(
                                        (_extract_job_title(job) for job in passed_jobs if _extract_job_url(job) == job_url),
                                        "",
                                    )},
                                ),
                                "change_type": "backfilled_for_scoring",
                                "shortlist_outcome": "backfilled_for_scoring",
                                "raw_hit_present": False,
                                "retrieval_anomaly_present": False,
                            }
                            for job_url in backfilled_job_urls
                        ],
                        *[
                            {
                                "job_url": job_url,
                                "title": "",
                                "change_type": "raw_hit_excluded_from_scoring",
                                "shortlist_outcome": "raw_hit_excluded_from_scoring",
                                "raw_hit_present": True,
                                "retrieval_anomaly_present": True,
                            }
                            for job_url in raw_shortlist_anomaly_urls
                        ],
                    ],
                    lambda item: {
                        **(_job_sample(item) or {"job_url": str(item.get("job_url") or ""), "job_title": str(item.get("title") or "")}),
                        "change_type": str(item.get("change_type") or ""),
                        "shortlist_outcome": str(item.get("shortlist_outcome") or ""),
                        "raw_hit_present": bool(item.get("raw_hit_present", False)),
                        "retrieval_anomaly_present": bool(item.get("retrieval_anomaly_present", False)),
                        **(
                            {
                                "embedding_reuse_status": item.get("embedding_reuse_status"),
                                "embedding_input_signature": item.get("embedding_input_signature"),
                                "embedding_contract_fingerprint": item.get("embedding_contract_fingerprint"),
                            }
                            if item.get("embedding_reuse_status") is not None
                            or item.get("embedding_input_signature") is not None
                            or item.get("embedding_contract_fingerprint") is not None
                            else {}
                        ),
                        **(
                            {
                                "vector_similarity": item.get("vector_similarity"),
                                "vector_rank": item.get("vector_rank"),
                                "shortlist_origin": item.get("shortlist_origin"),
                            }
                            if item.get("vector_rank") is not None or item.get("vector_similarity") is not None
                            else {}
                        ),
                    }
                    if str(item.get("job_url") or "")
                    else None,
                ),
                settings_refs=["pipeline.vector_search_top_n"],
            ) if shortlist_reached else _stage_block_not_reached("shortlist"),
            "ranking": _stage_block(
                stage_id="ranking",
                status="completed" if ranking_reached else "not_reached",
                input_counts={
                    "ai_scores": len(ai_scores),
                    "ranking_inputs": len(ranking_inputs),
                },
                output_counts={
                    "ranked_jobs": len(ranked),
                    "final_top_n": final_top_n,
                },
                decision_summary={
                    "ranking_fit_label_counts": ranking_fit_distribution,
                    "quality_metrics": ranking_quality_metrics,
                    "reuse_metrics": ranking_reuse_metrics,
                    "ranking_prompt_id": ranking_prompt_provenance["prompt_id"],
                    "ranking_prompt_version": ranking_prompt_provenance["prompt_version"],
                    "ranking_prompt_template_path": ranking_prompt_provenance["template_path"],
                    "ai_score_model": get_gemini_model(config),
                    "configured_ranking_weights": ranking_weights,
                    "configured_missing_value_defaults": ranking_defaults,
                    "configured_preference_fit_weights": preference_fit_weights,
                    "configured_fit_label_thresholds": dict(config.get("fit_label_thresholds") or {}),
                    "zero_weight_features": zero_weight_features,
                    "contributing_features": contributing_features,
                    "candidate_preference_resolution": infer_effective_preferences(profile, config),
                },
                inputs_sample=_sample_rows(ranking_inputs, _ranking_row_sample),
                outputs_sample=_sample_rows(ranked, _ranking_row_sample),
                dropped_or_changed_sample=_sample_rows(
                    [row for row in ranking_inputs if _extract_job_url(row) not in ranked_urls],
                    lambda row: {
                        **(_ranking_row_sample(row) or {}),
                        "change_type": "scored_not_ranked",
                    } if _ranking_row_sample(row) else None,
                ),
                settings_refs=[
                    "ranking_weights",
                    "preference_fit_weights",
                    "missing_value_defaults",
                    "fit_label_thresholds",
                    "pipeline.final_top_n",
                    "prompts.ranking.ai_score.prompt_id",
                ],
            ) if ranking_reached else _stage_block_not_reached("ranking"),
            "cv_analysis": _stage_block(
                stage_id="cv_analysis",
                status="completed" if cv_analysis_reached else "not_reached",
                input_counts={"ranked_jobs": len(ranked)},
                output_counts={
                    "blocked_by_reranker_fit": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == CV_ANALYSIS_BLOCKED_BY_RERANKER_STATUS
                    ),
                    "generation_ready": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == "ready_for_generation"
                    ),
                    "skipped_fit_gate": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == "skipped_fit_gate"
                    ),
                    "analysis_failed": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == "analysis_failed"
                    ),
                },
                decision_summary={
                    "analysis_records_captured": len(cv_analysis_results),
                    "quality_metrics": cv_analysis_quality_metrics,
                    "reuse_metrics": cv_analysis_reuse_metrics,
                    "evidence_top_k": int(config.get("pipeline", {}).get("evidence_top_k", 0) or 0),
                    "effective_channel_pool_size": int(
                        config.get("cv_analysis", {}).get("semantic_alignment", {}).get("channel_pool_size", 0) or 0
                    ),
                    "selected_evidence_total": sum(
                        int((record.get("evidence_selection_summary") or {}).get("selected_evidence_count") or 0)
                        for record in cv_analysis_results
                    ),
                    "merged_candidate_pool_total": sum(
                        int((record.get("evidence_selection_summary") or {}).get("merged_pool_size") or 0)
                        for record in cv_analysis_results
                    ),
                },
                inputs_sample=_sample_rows(ranked, _ranking_row_sample),
                outputs_sample=_sample_rows(cv_analysis_results, _analysis_record_output_sample),
                dropped_or_changed_sample=_sample_rows(cv_analysis_results, _analysis_record_changed_sample),
                settings_refs=[
                    "pipeline.evidence_top_k",
                    "cv_analysis.semantic_alignment.enabled",
                    "cv_analysis.semantic_alignment.model",
                    "cv_analysis.semantic_alignment.required_skill_lexical_weight",
                    "cv_analysis.semantic_alignment.required_skill_semantic_weight",
                    "cv_analysis.semantic_alignment.role_lexical_weight",
                    "cv_analysis.semantic_alignment.role_semantic_weight",
                    "cv_analysis.semantic_alignment.responsibility_lexical_weight",
                    "cv_analysis.semantic_alignment.responsibility_semantic_weight",
                    "cv_analysis.semantic_alignment.domain_lexical_weight",
                    "cv_analysis.semantic_alignment.domain_semantic_weight",
                    "cv_analysis.semantic_alignment.channel_pool_size",
                ],
            ) if cv_analysis_reached else _stage_block_not_reached("cv_analysis"),
            "cv_generation": _stage_block(
                stage_id="cv_generation",
                status="completed" if cv_generation_reached else "not_reached",
                input_counts={
                    "analysis_ready_jobs": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == "ready_for_generation"
                    ),
                },
                output_counts={
                    "accepted": cv_status_counts["accepted_count"],
                    "validation_failed": cv_status_counts["validation_failed_count"],
                    "generation_failed": cv_status_counts["generation_failed_count"],
                    "persistence_failed": cv_status_counts["persistence_failed_count"],
                },
                decision_summary={
                    "debug_records_captured": cv_status_counts["debug_records_captured"],
                    "analysis_ready_jobs_total": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == "ready_for_generation"
                    ),
                    "quality_metrics": cv_generation_quality_metrics,
                    "cv_generation_model": get_cv_generation_model(config),
                    "cv_prompt_id": cv_generation_prompt_provenance["prompt_id"],
                    "cv_prompt_template_path": cv_generation_prompt_provenance["template_path"],
                },
                inputs_sample=_sample_rows(
                    [record for record in cv_analysis_results if str(record.get("status") or "") == "ready_for_generation"],
                    _analysis_record_output_sample,
                ),
                outputs_sample=_sample_rows(cv_generation_debug_records, _debug_record_output_sample),
                dropped_or_changed_sample=_sample_rows(
                    [
                        record for record in cv_generation_debug_records
                        if str(record.get("status") or "") in {"validation_failed", "generation_failed", "persistence_failed"}
                    ],
                    _debug_record_changed_sample,
                ),
                settings_refs=["cv.generation.model", "prompts.cv_generation.structured_write.prompt_id"],
            ) if cv_generation_reached else _stage_block_not_reached("cv_generation"),
        },
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
            _extract_job_title(ranking_source),
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
    run_id = run_id or create_run_id()
    start_stage = _validate_pipeline_stage_name(start_stage) or PIPELINE_STAGE_SEQUENCE[0]
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
    vector_top_n = int(config.get("pipeline", {}).get("vector_search_top_n", 0))
    final_top_n = int(config.get("pipeline", {}).get("final_top_n", 0))

    if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("normalize"):
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
        load_to_bigquery(raw_rows, config)
        state["raw_jobs"] = raw_jobs
        state["normalized"] = normalized
        state["deduplicated_jobs"] = deduplicated_jobs
        if stage_progress_callback is not None:
            stage_progress_callback(
                _build_stage_progress_summary(
                    run_id=run_id,
                    last_completed_stage="normalize",
                    state=state,
                    profile=None,
                    config=config,
                    vector_top_n=vector_top_n,
                    candidate_summary=candidate_summary,
                    candidate_query_components=candidate_query_components,
                    candidate_query_debug=candidate_query_debug,
                    final_top_n=final_top_n,
                )
            )
        if stop_after_stage == "normalize":
            return _build_checkpoint_summary(
                run_id=run_id,
                paused_after_stage="normalize",
                state=state,
                profile=None,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
            )

    normalized = list(state["normalized"])

    if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("enrich"):
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
        enriched, fresh_enriched_rows = _enrich_jobs_with_reuse(surviving_normalized, config)
        if fresh_enriched_rows:
            load_structured_jobs(fresh_enriched_rows, config)
        load_run_structured_jobs(enriched, run_id, config)
        if reporter is not None:
            reused_count = sum(
                1 for row in enriched
                if str(row.get("enrich_reuse_status") or "") == REUSED_CACHED_ENRICHMENT_STATUS
            )
            fresh_count = sum(
                1 for row in enriched
                if str(row.get("enrich_reuse_status") or "") == FRESH_ENRICHMENT_STATUS
            )
            reporter.emit(  # type: ignore[union-attr]
                "layer1_jobs", "info",
                (
                    f"Ingested {len(raw_jobs)} jobs, enriched {len(enriched)} "
                    f"(after pre-filter; fresh={fresh_count}, reused={reused_count})"
                ),
            )
        state["pre_filter_rejected_jobs"] = pre_filter_rejected_jobs
        state["enriched"] = enriched
        if stage_progress_callback is not None:
            stage_progress_callback(
                _build_stage_progress_summary(
                    run_id=run_id,
                    last_completed_stage="enrich",
                    state=state,
                    profile=None,
                    config=config,
                    vector_top_n=vector_top_n,
                    candidate_summary=candidate_summary,
                    candidate_query_components=candidate_query_components,
                    candidate_query_debug=candidate_query_debug,
                    final_top_n=final_top_n,
                )
            )
        if stop_after_stage == "enrich":
            return _build_checkpoint_summary(
                run_id=run_id,
                paused_after_stage="enrich",
                state=state,
                profile=None,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
            )

    pre_filter_rejected_jobs = list(state["pre_filter_rejected_jobs"])
    enriched = list(state["enriched"])

    if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("rule_filter"):
        runtime_profile_json: str | None = (
            config.get("runtime_inputs", {}).get("candidate_profile_json")
        )
        if runtime_profile_json:
            profile = load_profile_json_text(runtime_profile_json)
        else:
            profile_path: str = str(config["paths"]["candidate_profile"])
            profile = load_profile_yaml(profile_path)
        load_candidate_to_bigquery(profile, config)
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
        store_filter_results(combined_filter_result, run_id, config)
        if reporter is not None:
            reporter.emit("layer3_filter", "info", f"{len(passed_jobs)} passed rule filter")  # type: ignore[union-attr]
        state["passed_jobs"] = passed_jobs
        state["candidate_filter_rejected_jobs"] = candidate_filter_rejected_jobs
        if stage_progress_callback is not None:
            stage_progress_callback(
                _build_stage_progress_summary(
                    run_id=run_id,
                    last_completed_stage="rule_filter",
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
        if stop_after_stage == "rule_filter":
            return _build_checkpoint_summary(
                run_id=run_id,
                paused_after_stage="rule_filter",
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
            )
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
    passed_job_urls = [_extract_job_url(job) for job in passed_jobs if _extract_job_url(job)]

    if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("shortlist"):
        # Active shortlist runtime only prepares reusable job embeddings here.
        # The candidate-side vector actually used for retrieval is generated
        # inside run_vector_search() from the deterministic candidate query text.
        embed_and_store_jobs(passed_jobs, config)
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
        )

        candidate_query_components = dict(
            candidate_query_record.get("components") or build_candidate_query_components(profile, config)
        )
        candidate_summary = str(
            candidate_query_record.get("text") or build_candidate_query_text(profile, config)
        )
        signature_record = build_candidate_query_signature_record(candidate_query_components)
        contract_record = build_candidate_query_embedding_contract_fingerprint(config)
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
        }
        shortlist = _materialize_scoring_shortlist(raw_shortlist, passed_jobs, vector_top_n)
        raw_shortlist_urls = set(_unique_job_urls(raw_shortlist))
        raw_shortlist_anomaly_urls = _raw_shortlist_anomaly_urls(raw_shortlist, passed_jobs)
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
        if stage_progress_callback is not None:
            stage_progress_callback(
                _build_stage_progress_summary(
                    run_id=run_id,
                    last_completed_stage="shortlist",
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
        if stop_after_stage == "shortlist":
            return _build_checkpoint_summary(
                run_id=run_id,
                paused_after_stage="shortlist",
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
            )

    raw_shortlist = list(state["raw_shortlist"])
    shortlist = list(state["shortlist"])
    backfilled_job_urls = list(state["backfilled_job_urls"])
    candidate_query_debug = dict(state.get("candidate_query_debug") or candidate_query_debug)
    raw_shortlist_urls = set(_unique_job_urls(raw_shortlist))
    raw_shortlist_anomaly_urls = _raw_shortlist_anomaly_urls(raw_shortlist, passed_jobs)

    if not candidate_query_components or not candidate_summary:
        from fitcv.vector_search import build_candidate_query_components, build_candidate_query_text

        candidate_query_components = build_candidate_query_components(profile, config)
        candidate_summary = build_candidate_query_text(profile, config)

    if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("ranking"):
        ai_top_n = int(config["pipeline"]["ai_score_top_n"])
        if cancellation_check and cancellation_check():
            raise PipelineCancelled("Cancelled before AI scoring")
        ai_score_candidates = shortlist[:ai_top_n]
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
            job_url = _extract_job_url(shortlisted_job)
            reused_ai_row = ranking_ai_score_reuse_index.get(fingerprint_record["fingerprint"])
            if reused_ai_row is not None and job_url:
                reused_ai_scores_by_url[job_url] = {
                    **deepcopy(reused_ai_row),
                    "job_url": job_url,
                    "ai_score_input_fingerprint": fingerprint_record["fingerprint"],
                    "ai_score_reuse_status": "reused_exact_match",
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
                "ai_score_reuse_status": "fresh_compute",
            }

        ai_scores = []
        for shortlisted_job in ai_score_candidates:
            job_url = _extract_job_url(shortlisted_job)
            ai_row = reused_ai_scores_by_url.get(job_url) or fresh_ai_scores_by_url.get(job_url)
            if ai_row is not None:
                ai_scores.append(ai_row)
        if reporter is not None:
            reporter.emit("layer3_ai_score", "info", f"AI scored: {len(ai_scores)} jobs")  # type: ignore[union-attr]

        ranking_inputs = build_ranking_features(shortlist, ai_scores, profile, config)
        ranked = rank_jobs(ranking_inputs, top_n=final_top_n)
        store_final_ranking(ranked, config)
        if reporter is not None:
            reporter.emit("layer3_ranking", "info", f"Final ranking: top {len(ranked)} jobs")  # type: ignore[union-attr]
        state["ai_scores"] = ai_scores
        state["ranking_inputs"] = ranking_inputs
        state["ranked"] = ranked
        if stage_progress_callback is not None:
            stage_progress_callback(
                _build_stage_progress_summary(
                    run_id=run_id,
                    last_completed_stage="ranking",
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
        if stop_after_stage == "ranking":
            return _build_checkpoint_summary(
                run_id=run_id,
                paused_after_stage="ranking",
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
            )

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
    enabled_cv_sections = _cv_generation_enabled_sections(config)
    if PIPELINE_STAGE_SEQUENCE.index(start_stage) <= PIPELINE_STAGE_SEQUENCE.index("cv_analysis"):
        if cancellation_check and cancellation_check():
            raise PipelineCancelled("Cancelled before CV analysis")
        cv_analysis_results = []
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
                        cv_prompt_id=cv_prompt_id_value,
                        cv_prompt_template_path=cv_prompt_template_path_value,
                        error=analysis_record.get("outcome_reason"),
                    )
                )
                continue
            analysis_fingerprint_record = build_cv_analysis_input_fingerprint(profile, job, config)
            reused_analysis_record = cv_analysis_reuse_index.get(analysis_fingerprint_record["fingerprint"])
            if reused_analysis_record is not None:
                analysis_record = {
                    **deepcopy(reused_analysis_record),
                    "job_url": str(job.get("job_url") or ""),
                    "job_title": _extract_job_title(job),
                    "job_snapshot": dict(job),
                    "analysis_input_fingerprint": analysis_fingerprint_record["fingerprint"],
                    "analysis_reuse_status": "reused_exact_match",
                }
                cv_analysis_results.append(analysis_record)
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
                            cv_prompt_id=cv_prompt_id_value,
                            cv_prompt_template_path=cv_prompt_template_path_value,
                            error=debug_error if isinstance(debug_error, dict) else None,
                        )
                    )
                    if reporter is not None and reused_status == "analysis_failed":
                        reporter.emit("layer4_cv_error", "error", f"CV analysis failed for {job.get('job_url')}: {debug_error}")  # type: ignore[union-attr]
                continue
            evidence: list[dict[str, Any]] = []
            evidence_selection_summary: dict[str, Any] = {}
            gap: dict[str, Any] | None = None
            fit = "skip"
            try:
                evidence_top_k = int(config["pipeline"]["evidence_top_k"])
                evidence_bundle = retrieve_evidence_bundle(
                    profile,
                    job,
                    top_k=evidence_top_k,
                    config=config,
                )
                evidence = list(evidence_bundle.get("selected_evidence") or [])
                evidence_selection_summary = {
                    "channel_counts": dict(evidence_bundle.get("channel_counts") or {}),
                    "effective_channel_pool_size": int(evidence_bundle.get("effective_channel_pool_size") or 0),
                    "merged_pool_size": int(evidence_bundle.get("merged_pool_size") or 0),
                    "deduped_pool_size": int(evidence_bundle.get("deduped_pool_size") or 0),
                    "selected_evidence_count": len(evidence),
                    "selected_evidence_ids": list(evidence_bundle.get("selected_evidence_ids") or []),
                    "unselected_top_candidates": list(evidence_bundle.get("unselected_top_candidates") or []),
                    "hybrid_alignment": dict(evidence_bundle.get("hybrid_alignment") or {}),
                    "semantic_alignment": dict(evidence_bundle.get("semantic_alignment") or {}),
                }
                evidence_selection_summary = {
                    key: value for key, value in evidence_selection_summary.items() if value not in ({}, [], None)
                }
                if not evidence:
                    evidence = retrieve_evidence(
                        profile,
                        job,
                        top_k=evidence_top_k,
                    )
                    if evidence:
                        evidence_selection_summary = {
                            "channel_counts": evidence_selection_summary.get("channel_counts") or {},
                            "effective_channel_pool_size": int(
                                evidence_selection_summary.get("effective_channel_pool_size") or 0
                            ),
                            "merged_pool_size": max(len(evidence), int(evidence_selection_summary.get("merged_pool_size") or 0)),
                            "deduped_pool_size": max(len(evidence), int(evidence_selection_summary.get("deduped_pool_size") or 0)),
                            "selected_evidence_count": len(evidence),
                            "selected_evidence_ids": [str(item.get("evidence_id") or "") for item in evidence],
                            "unselected_top_candidates": list(
                                evidence_selection_summary.get("unselected_top_candidates") or []
                            ),
                            "hybrid_alignment": dict(evidence_selection_summary.get("hybrid_alignment") or {}),
                            "semantic_alignment": dict(evidence_selection_summary.get("semantic_alignment") or {}),
                        }
                        evidence_selection_summary = {
                            key: value for key, value in evidence_selection_summary.items() if value not in ({}, [], None)
                        }

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
                            cv_prompt_id=cv_prompt_id_value,
                            cv_prompt_template_path=cv_prompt_template_path_value,
                            error=analysis_record.get("outcome_reason") or analysis_record["error"],
                        )
                    )
                    continue

                cv_analysis_results.append(
                    _build_cv_analysis_record(
                        job=job,
                        status="ready_for_generation",
                        analysis_input_fingerprint=analysis_fingerprint_record["fingerprint"],
                        analysis_reuse_status="fresh_compute",
                        evidence_payload=evidence,
                        evidence_used=_build_debug_evidence_used(evidence),
                        evidence_selection_summary=evidence_selection_summary,
                        gap_summary=gap,
                        fit_classification=fit,
                        error=None,
                    )
                )
            except Exception as exc:
                logger.error("[run_id=%s] CV analysis failed for %s: %s", run_id, job.get("job_url"), exc)
                analysis_record = _build_cv_analysis_record(
                    job=job,
                    status="analysis_failed",
                    analysis_input_fingerprint=analysis_fingerprint_record["fingerprint"],
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
                        cv_prompt_id=cv_prompt_id_value,
                        cv_prompt_template_path=cv_prompt_template_path_value,
                            error=analysis_record["error"],
                    )
                )
                if reporter is not None:
                    reporter.emit("layer4_cv_error", "error", f"CV analysis failed for {job.get('job_url')}: {exc}")  # type: ignore[union-attr]
                continue
        if reporter is not None:
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
            )  # type: ignore[union-attr]
        state["cv_analysis_results"] = cv_analysis_results
        state["cv_generation_debug_records"] = cv_generation_debug_records
        if stage_progress_callback is not None:
            stage_progress_callback(
                _build_stage_progress_summary(
                    run_id=run_id,
                    last_completed_stage="cv_analysis",
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
        if stop_after_stage == "cv_analysis":
            return _build_checkpoint_summary(
                run_id=run_id,
                paused_after_stage="cv_analysis",
                state=state,
                profile=profile,
                config=config,
                vector_top_n=vector_top_n,
                candidate_summary=candidate_summary,
                candidate_query_components=candidate_query_components,
                candidate_query_debug=candidate_query_debug,
                final_top_n=final_top_n,
            )

    cv_analysis_results = list(state["cv_analysis_results"])
    if cancellation_check and cancellation_check():
        raise PipelineCancelled("Cancelled before CV generation")
    generation_ready_records = [
        record for record in cv_analysis_results
        if str(record.get("status") or "") == "ready_for_generation"
    ]
    for analysis_record in generation_ready_records:
        job = dict(analysis_record.get("job_snapshot") or {})
        evidence = list(analysis_record.get("evidence_payload") or [])
        evidence_used = _build_debug_evidence_used(evidence)
        evidence_selection_summary = dict(analysis_record.get("evidence_selection_summary") or {})
        analysis_input_summary = _build_cv_generation_analysis_input_summary(job)
        analysis_grounding = _build_validation_grounding_payload(
            evidence_payload=evidence,
            evidence_used=evidence_used,
            evidence_selection_summary=evidence_selection_summary,
            analysis_input_summary=analysis_input_summary,
        )
        gap = analysis_record.get("gap_summary")
        fit = str(analysis_record.get("fit_classification") or "skip")
        structured_cv_initial: dict[str, Any] | None = None
        validation_initial: dict[str, Any] | None = None
        repair_attempt = dict(_EMPTY_REPAIR_ATTEMPT)
        structured_cv_final: dict[str, Any] | None = None
        markdown_final: str | None = None
        try:
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
                missing_sections = list(validation.get("missing_sections") or [])
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
            if not validation["valid"]:
                failure_details = {
                    "missing_sections": validation.get("missing_sections") or [],
                    "grounding_violations": validation.get("grounding_violations") or [],
                    "deterministic_grounding_violations": validation.get("deterministic_grounding_violations") or [],
                    "semantic_grounding_violations": validation.get("semantic_grounding_violations") or [],
                    "skill_violations": validation.get("skill_violations") or [],
                    "warnings": validation.get("warnings") or [],
                    "support_source_summary": validation.get("support_source_summary") or {},
                }
                logger.warning(
                    "[run_id=%s] CV for %s failed validation: %s",
                    run_id,
                    job.get("job_url"),
                    failure_details,
                )
                # Store rejected version for later review (v2 feature placeholder)
                # store_rejected_cv(job, validation, config)
                cv_generation_debug_records.append(
                    _build_cv_generation_debug_record(
                        job=job,
                        status="validation_failed",
                        fit_classification=fit,
                        evidence_used=evidence_used,
                        evidence_selection_summary=evidence_selection_summary,
                        analysis_input_summary=analysis_input_summary,
                        gap_summary=gap,
                        structured_cv_initial=structured_cv_initial,
                        validation_initial=validation_initial,
                        repair_attempt=repair_attempt,
                        structured_cv_final=None,
                        markdown_final=None,
                        enabled_sections=enabled_cv_sections,
                        cv_generation_model=cv_generation_model_value,
                        cv_prompt_id=cv_prompt_id_value,
                        cv_prompt_template_path=cv_prompt_template_path_value,
                        error={
                            "stage": "validation",
                            "message": f"CV validation failed for {job.get('job_url')}",
                        },
                    )
                )
                continue

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
                gap_summary=gap,
                fit_classification=fit,
                cv_structured=structured_cv,
                cv_generation_model=cv_generation_model_value,
                cv_prompt_version=cv_prompt_version_value,
            )
            store_cv_version(version, config)
            results.append({
                "job_url": str(job.get("job_url") or ""),
                "fit": fit,
                "ranking_fit_label": fit,
                "cv_version_id": version["version_id"],
                "gap": gap,
                "structured_cv": structured_cv,
                "cv_generation_model": cv_generation_model_value,
                "cv_prompt_id": cv_prompt_id_value,
                "cv_prompt_template_path": cv_prompt_template_path_value,
                "cv_markdown": cv,
                "generated_at": version.get("generated_at"),
                "fit_classification": fit,
            })
            cv_generation_debug_records.append(
                _build_cv_generation_debug_record(
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
                    cv_generation_model=cv_generation_model_value,
                    cv_prompt_id=cv_prompt_id_value,
                    cv_prompt_template_path=cv_prompt_template_path_value,
                    error=None,
                )
            )
            logger.info("[run_id=%s] CV generated for %s (fit=%s)", run_id, job.get("job_url"), fit)

        except Exception as exc:  # per-job failure — log and skip, don't crash the run
            logger.error("[run_id=%s] Failed for %s: %s", run_id, job.get("job_url"), exc)
            failure_status = "persistence_failed" if structured_cv_final is not None or markdown_final is not None else "generation_failed"
            failure_stage = "persistence" if failure_status == "persistence_failed" else "generation"
            cv_generation_debug_records.append(
                _build_cv_generation_debug_record(
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
                    cv_generation_model=cv_generation_model_value,
                    cv_prompt_id=cv_prompt_id_value,
                    cv_prompt_template_path=cv_prompt_template_path_value,
                    error={
                        "stage": failure_stage,
                        "message": str(exc),
                    },
                )
            )
            if reporter is not None:
                reporter.emit(
                    "layer4_cv_error",
                    "error",
                    f"CV generation failed for {job.get('job_url')}: {exc}",
                )  # type: ignore[union-attr]
            continue

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
    summary: dict[str, Any] = {
        "run_id": run_id,
        "total_jobs": len(raw_jobs),
        "passed_filter": len(passed_jobs),
        "ranked": len(ranked),
        "cvs_generated": len(results),
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
        event_summary = {
            "run_id": run_id,
            "total_jobs": summary["total_jobs"],
            "passed_filter": summary["passed_filter"],
            "ranked": summary["ranked"],
            "cvs_generated": summary["cvs_generated"],
        }
        reporter.emit("pipeline_complete", "info", str(event_summary))  # type: ignore[union-attr]
    return summary
