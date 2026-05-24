"""@meta
name: pipeline_stage_runner
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Execute isolated pipeline stage runner helpers used by orchestrator.
inputs:
  - Pipeline state, config, and stage dependency callbacks
outputs:
  - Mutated pipeline state and stage side effects
lifecycle:
  - status: active
"""

from __future__ import annotations

import time
from copy import deepcopy
from typing import Any, Callable

def _reuse_stage_enabled(config: dict[str, Any], stage: str) -> bool:
    reuse_block = dict(config.get("reuse") or {})
    stage_block = dict(reuse_block.get(str(stage or "").strip()) or {})
    return bool(stage_block.get("enabled", True))

def execute_normalize_stage(
    *,
    run_id: str,
    jobs_path: str,
    state: dict[str, Any],
    pipeline_store: Any,
    config: dict[str, Any],
    reporter: Any | None,
    observe_span: Callable[..., Any],
    set_span_attributes: Callable[[dict[str, Any]], None],
    parse_jobs_file: Callable[[str], list[dict[str, Any]]],
    normalize_batch: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    normalize_batch_with_exclusions: Callable[[list[dict[str, Any]]], tuple[Any, list[dict[str, Any]]]],
    prepare_raw_rows: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> None:
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


def execute_enrich_stage(
    *,
    run_id: str,
    state: dict[str, Any],
    config: dict[str, Any],
    reporter: Any | None,
    pipeline_store: Any,
    observe_span: Callable[..., Any],
    set_span_attributes: Callable[[dict[str, Any]], None],
    cancellation_check: Callable[[], bool] | None,
    cancellation_error_cls: type[Exception],
    enrich_jobs_with_reuse: Callable[..., tuple[list[dict[str, Any]], list[dict[str, Any]]]],
    apply_pre_enrichment_global_filters: Callable[[list[dict[str, Any]], dict[str, Any] | None], dict[str, Any]],
    fresh_enrichment_status: str,
    reused_cached_enrichment_status: str,
) -> None:
    normalized = list(state["normalized"])
    raw_jobs = list(state["raw_jobs"])
    with observe_span("pipeline.enrich", attributes={"run_id": run_id}):
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
            raise cancellation_error_cls("Cancelled before enrichment")
        enriched, fresh_enriched_rows = enrich_jobs_with_reuse(
            surviving_normalized,
            config,
            pipeline_store=pipeline_store,
        )
        if fresh_enriched_rows:
            pipeline_store.load_structured_jobs(fresh_enriched_rows, config)
        pipeline_store.load_run_structured_jobs(enriched, run_id, config)
        reused_count = sum(
            1 for row in enriched
            if str(row.get("enrich_reuse_status") or "") == reused_cached_enrichment_status
        )
        fresh_count = sum(
            1 for row in enriched
            if str(row.get("enrich_reuse_status") or "") == fresh_enrichment_status
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


def execute_rule_filter_stage(
    *,
    run_id: str,
    state: dict[str, Any],
    config: dict[str, Any],
    reporter: Any | None,
    pipeline_store: Any,
    observe_span: Callable[..., Any],
    set_span_attributes: Callable[[dict[str, Any]], None],
    load_profile_json_text: Callable[[str], dict[str, Any]],
    load_profile_yaml: Callable[[str], dict[str, Any]],
    flatten_skills: Callable[[dict[str, Any]], list[str]],
    apply_rule_filters: Callable[[list[dict[str, Any]], dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    pre_filter_rejected_jobs = list(state["pre_filter_rejected_jobs"])
    enriched = list(state["enriched"])
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
    return profile, candidate_skill_names


def execute_shortlist_stage(
    *,
    run_id: str,
    state: dict[str, Any],
    profile: dict[str, Any],
    passed_jobs: list[dict[str, Any]],
    config: dict[str, Any],
    vector_top_n: int,
    reporter: Any | None,
    pipeline_store: Any,
    observe_span: Callable[..., Any],
    set_span_attributes: Callable[[dict[str, Any]], None],
    run_vector_search: Callable[..., Any],
    materialize_scoring_shortlist: Callable[[list[dict[str, Any]], list[dict[str, Any]], int], list[dict[str, Any]]],
    unique_job_urls: Callable[[list[dict[str, Any]]], list[str]],
    raw_shortlist_anomaly_urls: Callable[[list[dict[str, Any]], list[dict[str, Any]]], list[str]],
) -> tuple[list[dict[str, Any]], str, dict[str, Any], dict[str, Any]]:
    with observe_span("pipeline.shortlist", attributes={"run_id": run_id, "vector_top_n": vector_top_n}):
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
        shortlist_fail_fast = bool((config.get("pipeline", {}) or {}).get("shortlist_fail_fast_empty_raw_hits", False))
        if shortlist_fail_fast and passed_jobs and not raw_shortlist:
            raise RuntimeError(
                "Vector shortlist returned zero raw hits for non-empty passed jobs; fail-fast guard active"
            )
        shortlist = materialize_scoring_shortlist(raw_shortlist, passed_jobs, vector_top_n)
        pipeline_store.store_shortlist(shortlist, config)
        raw_shortlist_urls = set(unique_job_urls(raw_shortlist))
        raw_shortlist_anomaly_rows = raw_shortlist_anomaly_urls(raw_shortlist, passed_jobs)
        backfilled_job_urls = [
            str(job.get("job_url") or "")
            for job in shortlist
            if str(job.get("job_url") or "") not in raw_shortlist_urls
        ]
        if reporter is not None:
            shortlist_message = f"Vector shortlist: {len(raw_shortlist_urls)} raw hits"
            if backfilled_job_urls:
                shortlist_message += f", {len(shortlist)} scoring jobs ({len(backfilled_job_urls)} backfilled)"
            if raw_shortlist_anomaly_rows:
                shortlist_message += f", {len(raw_shortlist_anomaly_rows)} raw-hit anomalies"
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
    return raw_shortlist, candidate_summary, candidate_query_components, candidate_query_debug


def execute_ranking_stage(
    *,
    run_id: str,
    state: dict[str, Any],
    shortlist: list[dict[str, Any]],
    candidate_summary: str,
    profile: dict[str, Any],
    config: dict[str, Any],
    final_top_n: int,
    reporter: Any | None,
    pipeline_store: Any,
    observe_span: Callable[..., Any],
    set_span_attributes: Callable[[dict[str, Any]], None],
    cancellation_check: Callable[[], bool] | None,
    cancellation_error_cls: type[Exception],
    ranking_ai_score_reuse_index: dict[str, dict[str, Any]],
    build_ai_score_input_fingerprint: Callable[..., dict[str, Any]],
    extract_job_url: Callable[[dict[str, Any]], str],
    run_ai_scoring: Callable[..., list[dict[str, Any]]],
    build_ranking_features: Callable[..., list[dict[str, Any]]],
    rank_jobs: Callable[..., list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with observe_span("pipeline.ai_score", attributes={"run_id": run_id}):
        ai_top_n = int(config["pipeline"]["ai_score_top_n"])
        ranking_reuse_enabled = _reuse_stage_enabled(config, "ranking")
        if cancellation_check and cancellation_check():
            raise cancellation_error_cls("Cancelled before AI scoring")
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
            reporter.emit("layer3_ai_score", "info", f"AI scored: {len(ai_scores)} jobs")  # type: ignore[union-attr]
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
            reporter.emit("layer3_ranking", "info", f"Final ranking: top {len(ranked)} jobs")  # type: ignore[union-attr]
        state["ai_scores"] = ai_scores
        state["ranking_inputs"] = ranking_inputs
        state["ranked"] = ranked
        set_span_attributes(
            {
                "ranking_inputs": len(ranking_inputs),
                "ranked_jobs": len(ranked),
            }
        )
    return ai_scores, ranking_inputs


def finalize_cv_analysis_stage(
    *,
    run_id: str,
    ranked: list[dict[str, Any]],
    cv_analysis_results: list[dict[str, Any]],
    cv_analysis_started_monotonic: float,
    cv_generation_debug_records: list[dict[str, Any]],
    state: dict[str, Any],
    config: dict[str, Any],
    reporter: Any | None,
    set_span_attributes: Callable[[dict[str, Any]], None],
    bounded_event_payload_builder: Callable[..., dict[str, Any]],
    ready_status: str,
    blocked_status: str,
    skipped_status: str,
    failed_status: str,
) -> None:
    if reporter is not None:
        blocked_by_reranker_diagnostics = [
            {
                "job_url": str(record.get("job_url") or ""),
                "ranking_fit_label": str(record.get("ranking_fit_label") or ""),
                "fit_classification": str(record.get("fit_classification") or ""),
                "ai_score": (record.get("job_snapshot") or {}).get("ai_score"),
                "fit_label_source": str(((record.get("job_snapshot") or {}).get("fit_label_source")) or ""),
            }
            for record in cv_analysis_results
            if str(record.get("status") or "") == blocked_status
        ][:10]
        reporter.emit(
            "layer4_cv_analysis",
            "info",
            (
                "CV analysis complete: "
                f"{sum(1 for record in cv_analysis_results if str(record.get('status') or '') == 'ready_for_generation')} ready, "
                f"{sum(1 for record in cv_analysis_results if str(record.get('status') or '') == blocked_status)} blocked by reranker, "
                f"{sum(1 for record in cv_analysis_results if str(record.get('status') or '') == 'skipped_fit_gate')} skipped, "
                f"{sum(1 for record in cv_analysis_results if str(record.get('status') or '') == 'analysis_failed')} failed"
            ),
            bounded_event_payload_builder(
                event_name="cv_analysis_decision",
                event_family="decision",
                source_stage="cv_analysis",
                event_status="completed",
                deterministic_outcome=None,
                stage_owned_subreason="stage_summary",
                input_snapshot={
                    "ranked_jobs": len(ranked),
                },
                output_snapshot={
                    "ready_for_generation": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == ready_status
                    ),
                    "blocked_by_reranker_fit": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == blocked_status
                    ),
                    "skipped_fit_gate": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == skipped_status
                    ),
                    "analysis_failed": sum(
                        1 for record in cv_analysis_results
                        if str(record.get("status") or "") == failed_status
                    ),
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
                bounded_event_payload_builder(
                    event_name="cv_analysis_blocked_diagnostics",
                    event_family="debug",
                    source_stage="cv_analysis",
                    event_status="completed",
                    deterministic_outcome="rejected",
                    stage_owned_subreason=blocked_status,
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
                if str(record.get("status") or "") == ready_status
            ),
            "cv_analysis_failed": sum(
                1 for record in cv_analysis_results
                if str(record.get("status") or "") == failed_status
            ),
        }
    )
    state["cv_analysis_results"] = cv_analysis_results
    state["cv_generation_debug_records"] = cv_generation_debug_records


def execute_cv_analysis_jobs(
    *,
    ranked_jobs_for_cv: list[dict[str, Any]],
    process_job: Callable[[dict[str, Any]], None],
) -> None:
    for job in ranked_jobs_for_cv:
        process_job(job)


def execute_cv_generation_records(
    *,
    generation_ready_records: list[dict[str, Any]],
    process_record: Callable[[dict[str, Any]], None],
) -> None:
    for analysis_record in generation_ready_records:
        process_record(analysis_record)


def select_cv_generation_ready_records(
    *,
    cv_analysis_results: list[dict[str, Any]],
    ready_status: str = "ready_for_generation",
) -> list[dict[str, Any]]:
    return [
        record for record in cv_analysis_results
        if str(record.get("status") or "") == ready_status
    ]


def emit_cv_generation_result_event(
    *,
    reporter: Any | None,
    bounded_event_payload_builder: Callable[..., dict[str, Any]],
    job: dict[str, Any],
    fit: str,
    authoritative_ranking_fit_label: Callable[[dict[str, Any], str], str],
    status: str,
    attempt_count: int = 1,
    retry_count: int = 0,
    latency_ms: int | None = None,
    usage: dict[str, Any] | None = None,
    cost: dict[str, Any] | None = None,
    reuse_status: str | None = None,
    reused_cv_version_id: str | None = None,
) -> None:
    if reporter is None:
        return
    effective_reuse_status = str(reuse_status or "").strip() or "fresh_compute"
    effective_reused_cv_version_id = str(reused_cv_version_id or "").strip()
    reporter.emit(
        "layer4_cv_generation_result",
        "info",
        f"CV generation result for {job.get('job_url')}: {status}",
        bounded_event_payload_builder(
            event_name="cv_generation_result",
            event_family="decision",
            source_stage="cv_generation",
            event_status="completed",
            job_url=str(job.get("job_url") or ""),
            deterministic_outcome=str(status or ""),
            fallback_used=False,
            input_snapshot={
                "ranking_fit_label": authoritative_ranking_fit_label(job, fit),
                "fit_classification": fit,
            },
            output_snapshot={
                "status": str(status or ""),
                "attempt_count": int(attempt_count),
                "retry_count": int(retry_count),
                "reuse_status": effective_reuse_status,
                "reused_cv_version_id": effective_reused_cv_version_id,
                "reuse_status_flat": effective_reuse_status,
                "reused_cv_version_id_flat": effective_reused_cv_version_id,
            },
            artifact_refs={"stage_id": "cv_generation"},
            latency_ms=latency_ms,
            usage=usage,
            cost=cost,
        ),
    )  # type: ignore[union-attr]


def handle_cv_generation_review_required_branches(
    *,
    validation: dict[str, Any],
    cv_acceptance_policy: dict[str, Any],
    fit: str,
    gap: dict[str, Any] | None,
    job: dict[str, Any],
    analysis_record: dict[str, Any],
    structured_cv: dict[str, Any],
    cv: str,
    structured_cv_initial: dict[str, Any] | None,
    validation_initial: dict[str, Any] | None,
    repair_attempt: dict[str, Any],
    evidence_used: list[dict[str, Any]],
    evidence_selection_summary: dict[str, Any],
    analysis_input_summary: dict[str, Any],
    enabled_cv_sections: list[str],
    job_cv_generation_model_value: str | None,
    job_runtime_provenance: dict[str, Any] | None,
    cv_prompt_id_value: str,
    cv_prompt_template_path_value: str,
    job_agentic_live_trace: dict[str, Any] | None,
    cv_generation_review_required_status: str,
    reporter: Any | None,
    bounded_event_payload_builder: Callable[..., dict[str, Any]],
    authoritative_ranking_fit_label: Callable[[dict[str, Any], str], str],
    build_cv_generation_debug_record: Callable[..., dict[str, Any]],
    emit_cv_generation_item_observation: Callable[[dict[str, Any]], None],
    evaluate_cv_acceptance_policy: Callable[..., tuple[bool, str, str]],
    markdown_quality_review_reason: Callable[[dict[str, Any]], str | None],
    latency_ms: int,
    emit_result_event: Callable[..., None],
) -> dict[str, Any] | None:
    markdown_review_reason = markdown_quality_review_reason(validation)
    if markdown_review_reason:
        emit_result_event(
            reporter=reporter,
            bounded_event_payload_builder=bounded_event_payload_builder,
            job=job,
            fit=fit,
            authoritative_ranking_fit_label=authoritative_ranking_fit_label,
            status=cv_generation_review_required_status,
            attempt_count=1,
            retry_count=0,
            latency_ms=latency_ms,
        )
        review_required_debug_record = build_cv_generation_debug_record(
            job=job,
            status=cv_generation_review_required_status,
            fit_classification=fit,
            evidence_used=evidence_used,
            evidence_selection_summary=evidence_selection_summary,
            analysis_input_summary=analysis_input_summary,
            gap_summary=gap,
            structured_cv_initial=structured_cv_initial,
            validation_initial=validation_initial,
            repair_attempt=repair_attempt,
            structured_cv_final=structured_cv,
            markdown_final=cv,
            enabled_sections=enabled_cv_sections,
            cv_generation_model=job_cv_generation_model_value,
            runtime_provenance=job_runtime_provenance,
            cv_prompt_id=cv_prompt_id_value,
            cv_prompt_template_path=cv_prompt_template_path_value,
            error={"stage": "markdown_quality_review", "message": markdown_review_reason},
            agentic_live_trace=job_agentic_live_trace,
        )
        emit_cv_generation_item_observation(review_required_debug_record)
        return review_required_debug_record

    policy_pass, policy_reason_code, policy_note = evaluate_cv_acceptance_policy(
        fit_classification=fit,
        gap_summary=gap,
        policy=cv_acceptance_policy,
    )
    if policy_pass:
        return None
    emit_result_event(
        reporter=reporter,
        bounded_event_payload_builder=bounded_event_payload_builder,
        job=job,
        fit=fit,
        authoritative_ranking_fit_label=authoritative_ranking_fit_label,
        status=cv_generation_review_required_status,
        attempt_count=1,
        retry_count=0,
        latency_ms=latency_ms,
    )
    policy_message = (
        f"Policy acceptance blocked ({policy_reason_code}): {policy_note}. "
        f"Manual review required."
    )
    review_required_debug_record = build_cv_generation_debug_record(
        job=job,
        status=cv_generation_review_required_status,
        fit_classification=fit,
        evidence_used=evidence_used,
        evidence_selection_summary=evidence_selection_summary,
        analysis_input_summary=analysis_input_summary,
        gap_summary=gap,
        structured_cv_initial=structured_cv_initial,
        validation_initial=validation_initial,
        repair_attempt=repair_attempt,
        structured_cv_final=structured_cv,
        markdown_final=cv,
        enabled_sections=enabled_cv_sections,
        cv_generation_model=job_cv_generation_model_value,
        runtime_provenance=job_runtime_provenance,
        cv_prompt_id=cv_prompt_id_value,
        cv_prompt_template_path=cv_prompt_template_path_value,
        error={"stage": "policy_acceptance", "message": policy_message},
        agentic_live_trace=job_agentic_live_trace,
    )
    emit_cv_generation_item_observation(review_required_debug_record)
    return review_required_debug_record


def handle_cv_generation_accepted_persistence(
    *,
    job: dict[str, Any],
    run_id: str,
    config: dict[str, Any],
    fit: str,
    gap: dict[str, Any] | None,
    structured_cv: dict[str, Any],
    cv: str,
    cv_prompt_version_value: str,
    job_cv_generation_model_value: str | None,
    job_runtime_provenance: dict[str, Any] | None,
    cv_prompt_id_value: str,
    cv_prompt_template_path_value: str,
    evidence: list[dict[str, Any]],
    evidence_used: list[dict[str, Any]],
    evidence_selection_summary: dict[str, Any],
    analysis_input_summary: dict[str, Any],
    structured_cv_initial: dict[str, Any] | None,
    validation_initial: dict[str, Any] | None,
    repair_attempt: dict[str, Any],
    structured_cv_final: dict[str, Any] | None,
    markdown_final: str | None,
    enabled_cv_sections: list[str],
    job_agentic_live_trace: dict[str, Any] | None,
    generation_attempt_count: int,
    cv_generation_started_monotonic: float,
    reporter: Any | None,
    bounded_event_payload_builder: Callable[..., dict[str, Any]],
    authoritative_ranking_fit_label: Callable[[dict[str, Any], str], str],
    pipeline_store: Any,
    create_cv_version_record: Callable[..., dict[str, Any]],
    build_cv_generation_debug_record: Callable[..., dict[str, Any]],
    emit_cv_generation_item_observation: Callable[[dict[str, Any]], None],
    emit_result_event: Callable[..., None],
    results: list[dict[str, Any]],
    cv_generation_debug_records: list[dict[str, Any]],
) -> None:
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
    )
    pipeline_store.store_cv_version(version, config)
    results.append({
        "job_url": str(job.get("job_url") or ""),
        "fit": fit,
        "ranking_fit_label": fit,
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
    })
    accepted_debug_record = build_cv_generation_debug_record(
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
    cv_generation_debug_records.append(accepted_debug_record)
    emit_cv_generation_item_observation(accepted_debug_record)
    emit_result_event(
        reporter=reporter,
        bounded_event_payload_builder=bounded_event_payload_builder,
        job=job,
        fit=fit,
        authoritative_ranking_fit_label=authoritative_ranking_fit_label,
        status="accepted",
        attempt_count=generation_attempt_count,
        retry_count=max(generation_attempt_count - 1, 0),
        latency_ms=int((time.monotonic() - cv_generation_started_monotonic) * 1000),
    )


def handle_cv_generation_failure_segment(
    *,
    exc: Exception,
    job: dict[str, Any],
    run_id: str,
    fit: str,
    structured_cv_final: dict[str, Any] | None,
    markdown_final: str | None,
    structured_cv_initial: dict[str, Any] | None,
    validation_initial: dict[str, Any] | None,
    repair_attempt: dict[str, Any],
    evidence_used: list[dict[str, Any]],
    evidence_selection_summary: dict[str, Any],
    analysis_input_summary: dict[str, Any],
    gap: dict[str, Any] | None,
    enabled_cv_sections: list[str],
    job_cv_generation_model_value: str | None,
    job_runtime_provenance: dict[str, Any] | None,
    cv_prompt_id_value: str,
    cv_prompt_template_path_value: str,
    job_agentic_live_trace: dict[str, Any] | None,
    cv_generation_started_monotonic: float,
    reporter: Any | None,
    bounded_event_payload_builder: Callable[..., dict[str, Any]],
    authoritative_ranking_fit_label: Callable[[dict[str, Any], str], str],
    build_cv_generation_debug_record: Callable[..., dict[str, Any]],
    emit_cv_generation_item_observation: Callable[[dict[str, Any]], None],
    emit_result_event: Callable[..., None],
    cv_generation_debug_records: list[dict[str, Any]],
    logger_error: Callable[[str, str, Any], None],
) -> None:
    logger_error("[run_id=%s] Failed for %s: %s", run_id, job.get("job_url"), exc)
    failure_status = "persistence_failed" if structured_cv_final is not None or markdown_final is not None else "generation_failed"
    failure_stage = "persistence" if failure_status == "persistence_failed" else "generation"
    emit_result_event(
        reporter=reporter,
        bounded_event_payload_builder=bounded_event_payload_builder,
        job=job,
        fit=fit,
        authoritative_ranking_fit_label=authoritative_ranking_fit_label,
        status=failure_status,
        attempt_count=1,
        retry_count=0,
        latency_ms=int((time.monotonic() - cv_generation_started_monotonic) * 1000),
    )
    failure_debug_record = build_cv_generation_debug_record(
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
        error={"stage": failure_stage, "message": str(exc)},
        agentic_live_trace=job_agentic_live_trace,
    )
    cv_generation_debug_records.append(failure_debug_record)
    emit_cv_generation_item_observation(failure_debug_record)
    if reporter is not None:
        reporter.emit(
            "layer4_cv_error",
            "error",
            f"CV generation failed for {job.get('job_url')}: {exc}",
            bounded_event_payload_builder(
                event_name="cv_generation_decision",
                event_family="decision",
                source_stage="cv_generation",
                event_status="completed",
                job_url=str(job.get("job_url") or ""),
                deterministic_outcome="rejected",
                stage_owned_subreason=failure_status,
                provenance={"cv_generation_model": job_cv_generation_model_value},
                input_snapshot={
                    "ranking_fit_label": authoritative_ranking_fit_label(job, fit),
                    "fit_classification": fit,
                    "selected_evidence_count": len(evidence_used),
                },
                output_snapshot={"error_stage": failure_stage},
                artifact_refs={"stage_id": "cv_generation"},
            ),
        )  # type: ignore[union-attr]


def handle_cv_analysis_reranker_skip(
    *,
    run_id: str,
    job: dict[str, Any],
    ranking_fit_label: str,
    blocked_status: str,
    build_cv_analysis_record: Callable[..., dict[str, Any]],
    emit_cv_analysis_item_observation: Callable[..., None],
    build_cv_generation_debug_record: Callable[..., dict[str, Any]],
    build_cv_generation_analysis_input_summary: Callable[[dict[str, Any]], dict[str, Any]],
    empty_repair_attempt: dict[str, Any],
    enabled_cv_sections: list[str],
    cv_generation_model_value: str,
    cv_prompt_id_value: str,
    cv_prompt_template_path_value: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    analysis_record = build_cv_analysis_record(
        job=job,
        status=blocked_status,
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
    emit_cv_analysis_item_observation(
        run_id=run_id,
        job=job,
        analysis_record=analysis_record,
    )
    debug_record = build_cv_generation_debug_record(
        job=job,
        status=blocked_status,
        fit_classification=ranking_fit_label,
        evidence_used=[],
        evidence_selection_summary=None,
        analysis_input_summary=build_cv_generation_analysis_input_summary(job),
        gap_summary=None,
        structured_cv_initial=None,
        validation_initial=None,
        repair_attempt=dict(empty_repair_attempt),
        structured_cv_final=None,
        markdown_final=None,
        enabled_sections=enabled_cv_sections,
        cv_generation_model=cv_generation_model_value,
        runtime_provenance=None,
        cv_prompt_id=cv_prompt_id_value,
        cv_prompt_template_path=cv_prompt_template_path_value,
        error=analysis_record.get("outcome_reason"),
    )
    return analysis_record, debug_record


def handle_cv_analysis_reused_record(
    *,
    job: dict[str, Any],
    reused_analysis_record: dict[str, Any],
    analysis_input_fingerprint: str,
    build_extract_job_title: Callable[[dict[str, Any]], str],
    build_cv_generation_debug_record: Callable[..., dict[str, Any]],
    build_cv_generation_analysis_input_summary: Callable[[dict[str, Any]], dict[str, Any]],
    empty_repair_attempt: dict[str, Any],
    enabled_cv_sections: list[str],
    cv_generation_model_value: str,
    cv_prompt_id_value: str,
    cv_prompt_template_path_value: str,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    analysis_record = {
        **deepcopy(reused_analysis_record),
        "job_url": str(job.get("job_url") or ""),
        "job_title": build_extract_job_title(job),
        "job_snapshot": dict(job),
        "analysis_input_fingerprint": analysis_input_fingerprint,
        "analysis_reuse_status": "reused_exact_match",
    }
    reused_status = str(analysis_record.get("status") or "")
    if reused_status not in {"skipped_fit_gate", "analysis_failed"}:
        return analysis_record, None, None

    debug_error = (
        analysis_record.get("outcome_reason")
        if reused_status == "skipped_fit_gate"
        else analysis_record.get("error")
    )
    debug_record = build_cv_generation_debug_record(
        job=job,
        status=reused_status,
        fit_classification=analysis_record.get("fit_classification"),
        evidence_used=list(analysis_record.get("evidence_used") or []),
        evidence_selection_summary=analysis_record.get("evidence_selection_summary"),
        analysis_input_summary=build_cv_generation_analysis_input_summary(job),
        gap_summary=analysis_record.get("gap_summary"),
        structured_cv_initial=None,
        validation_initial=None,
        repair_attempt=dict(empty_repair_attempt),
        structured_cv_final=None,
        markdown_final=None,
        enabled_sections=enabled_cv_sections,
        cv_generation_model=cv_generation_model_value,
        runtime_provenance=None,
        cv_prompt_id=cv_prompt_id_value,
        cv_prompt_template_path=cv_prompt_template_path_value,
        error=debug_error if isinstance(debug_error, dict) else None,
    )
    error_payload: dict[str, Any] | None = None
    if reused_status == "analysis_failed":
        error_payload = {
            "debug_error": debug_error,
            "ranking_fit_label": analysis_record.get("ranking_fit_label"),
            "error_stage": str((debug_error or {}).get("stage") if isinstance(debug_error, dict) else ""),
        }
    return analysis_record, debug_record, error_payload


def handle_cv_analysis_compute_branch(
    *,
    run_id: str,
    job: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
    analysis_input_fingerprint: str,
    candidate_skill_names: list[str],
    agentic_late_stage_enabled: bool,
    enabled_cv_sections: list[str],
    cv_generation_model_value: str,
    cv_prompt_id_value: str,
    cv_prompt_template_path_value: str,
    empty_repair_attempt: dict[str, Any],
    run_agentic_cv_analysis: Callable[..., dict[str, Any]],
    emit_cv_analysis_item_observation: Callable[..., None],
    build_cv_generation_debug_record: Callable[..., dict[str, Any]],
    build_cv_generation_analysis_input_summary: Callable[[dict[str, Any]], dict[str, Any]],
    retrieve_evidence_bundle: Callable[..., dict[str, Any]],
    build_analysis_evidence_selection_summary: Callable[..., dict[str, Any]],
    retrieve_evidence: Callable[..., list[dict[str, Any]]],
    compute_gap: Callable[..., dict[str, Any]],
    resolve_layer4_fit: Callable[..., str],
    build_cv_analysis_record: Callable[..., dict[str, Any]],
    build_debug_evidence_used: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    logger_error: Callable[[str, str, str, Any], None],
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    evidence: list[dict[str, Any]] = []
    evidence_selection_summary: dict[str, Any] = {}
    gap: dict[str, Any] | None = None
    fit = "skip"
    try:
        if agentic_late_stage_enabled:
            analysis_record = dict(run_agentic_cv_analysis(job, profile, config))
            emit_cv_analysis_item_observation(job=job, analysis_record=analysis_record)
            evidence = list(analysis_record.get("evidence_payload") or [])
            evidence_selection_summary = dict(analysis_record.get("evidence_selection_summary") or {})
            gap = analysis_record.get("gap_summary")
            fit = str(analysis_record.get("fit_classification") or fit)
            if str(analysis_record.get("status") or "") != "ready_for_generation":
                return analysis_record, build_cv_generation_debug_record(
                    job=job,
                    status=str(analysis_record.get("status") or "analysis_failed"),
                    fit_classification=fit,
                    evidence_used=analysis_record["evidence_used"],
                    evidence_selection_summary=analysis_record.get("evidence_selection_summary"),
                    analysis_input_summary=build_cv_generation_analysis_input_summary(job),
                    gap_summary=gap,
                    structured_cv_initial=None,
                    validation_initial=None,
                    repair_attempt=dict(empty_repair_attempt),
                    structured_cv_final=None,
                    markdown_final=None,
                    enabled_sections=enabled_cv_sections,
                    cv_generation_model=cv_generation_model_value,
                    runtime_provenance=None,
                    cv_prompt_id=cv_prompt_id_value,
                    cv_prompt_template_path=cv_prompt_template_path_value,
                    error=analysis_record.get("outcome_reason") or analysis_record["error"],
                ), None
            return analysis_record, None, None

        evidence_top_k = int(config["pipeline"]["evidence_top_k"])
        evidence_bundle = retrieve_evidence_bundle(profile, job, top_k=evidence_top_k, config=config)
        evidence = list(evidence_bundle.get("selected_evidence") or [])
        evidence_selection_summary = build_analysis_evidence_selection_summary(
            evidence_bundle, evidence, fallback_used=False
        )
        if not evidence:
            evidence = retrieve_evidence(profile, job, top_k=evidence_top_k)
            if evidence:
                evidence_selection_summary = build_analysis_evidence_selection_summary(
                    {
                        **evidence_bundle,
                        "selected_evidence_ids": [str(item.get("evidence_id") or "") for item in evidence],
                        "merged_pool_size": max(len(evidence), int(evidence_selection_summary.get("merged_pool_size") or 0)),
                        "deduped_pool_size": max(len(evidence), int(evidence_selection_summary.get("deduped_pool_size") or 0)),
                        "unselected_top_candidates": list(evidence_selection_summary.get("unselected_top_candidates") or []),
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
        fit = resolve_layer4_fit(job, gap_fit=None, config=config)
        if fit == "skip":
            analysis_record = build_cv_analysis_record(
                job=job,
                status="skipped_fit_gate",
                analysis_input_fingerprint=analysis_input_fingerprint,
                analysis_reuse_status="fresh_compute",
                evidence_payload=evidence,
                evidence_used=build_debug_evidence_used(evidence),
                evidence_selection_summary=evidence_selection_summary,
                gap_summary=gap,
                fit_classification=fit,
                error={"stage": "fit_gate", "message": f"Skipped {job.get('job_url')} (fit=skip)"},
            )
            emit_cv_analysis_item_observation(job=job, analysis_record=analysis_record)
            debug = build_cv_generation_debug_record(
                job=job,
                status="skipped_fit_gate",
                fit_classification=fit,
                evidence_used=analysis_record["evidence_used"],
                evidence_selection_summary=analysis_record.get("evidence_selection_summary"),
                analysis_input_summary=build_cv_generation_analysis_input_summary(job),
                gap_summary=gap,
                structured_cv_initial=None,
                validation_initial=None,
                repair_attempt=dict(empty_repair_attempt),
                structured_cv_final=None,
                markdown_final=None,
                enabled_sections=enabled_cv_sections,
                cv_generation_model=cv_generation_model_value,
                runtime_provenance=None,
                cv_prompt_id=cv_prompt_id_value,
                cv_prompt_template_path=cv_prompt_template_path_value,
                error=analysis_record.get("outcome_reason") or analysis_record["error"],
            )
            return analysis_record, debug, None

        analysis_record = build_cv_analysis_record(
            job=job,
            status="ready_for_generation",
            analysis_input_fingerprint=analysis_input_fingerprint,
            analysis_reuse_status="fresh_compute",
            evidence_payload=evidence,
            evidence_used=build_debug_evidence_used(evidence),
            evidence_selection_summary=evidence_selection_summary,
            gap_summary=gap,
            fit_classification=fit,
            error=None,
        )
        emit_cv_analysis_item_observation(job=job, analysis_record=analysis_record)
        return analysis_record, None, None
    except Exception as exc:
        logger_error("[run_id=%s] CV analysis failed for %s: %s", run_id, job.get("job_url"), exc)
        analysis_record = build_cv_analysis_record(
            job=job,
            status="analysis_failed",
            analysis_input_fingerprint=analysis_input_fingerprint,
            analysis_reuse_status="fresh_compute",
            evidence_payload=evidence,
            evidence_used=build_debug_evidence_used(evidence),
            evidence_selection_summary=evidence_selection_summary,
            gap_summary=gap,
            fit_classification=fit if fit else None,
            error={"stage": "analysis", "message": str(exc)},
        )
        emit_cv_analysis_item_observation(job=job, analysis_record=analysis_record)
        debug = build_cv_generation_debug_record(
            job=job,
            status="analysis_failed",
            fit_classification=analysis_record.get("fit_classification"),
            evidence_used=analysis_record["evidence_used"],
            evidence_selection_summary=analysis_record.get("evidence_selection_summary"),
            analysis_input_summary=build_cv_generation_analysis_input_summary(job),
            gap_summary=gap,
            structured_cv_initial=None,
            validation_initial=None,
            repair_attempt=dict(empty_repair_attempt),
            structured_cv_final=None,
            markdown_final=None,
            enabled_sections=enabled_cv_sections,
            cv_generation_model=cv_generation_model_value,
            runtime_provenance=None,
            cv_prompt_id=cv_prompt_id_value,
            cv_prompt_template_path=cv_prompt_template_path_value,
            error=analysis_record["error"],
        )
        return analysis_record, debug, {
            "exception": str(exc),
            "ranking_fit_label": analysis_record.get("ranking_fit_label"),
            "error_stage": str(analysis_record["error"].get("stage") or ""),
        }

