"""@meta
name: pipeline_stage_artifacts
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Build stage-transition artifact blocks and stage-level summaries.
inputs:
  - Pipeline stage data and helper callbacks
outputs:
  - Stage artifact block dictionaries with schema-stable keys
lifecycle:
  - status: active
"""

from typing import Any, Callable, Protocol, cast


class LateStageModePayloadBuilder(Protocol):
    def __call__(
        self,
        *,
        agentic_late_stage_enabled: bool,
        stage_reached: bool,
    ) -> dict[str, Any]: ...


def truncate_stage_text(value: str, *, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "...[truncated]"


def truncate_stage_value(value: Any, *, text_limit: int) -> Any:
    if isinstance(value, str):
        return truncate_stage_text(value, limit=text_limit)
    if isinstance(value, list):
        return [truncate_stage_value(item, text_limit=text_limit) for item in value]
    if isinstance(value, dict):
        return {
            str(key): truncate_stage_value(inner, text_limit=text_limit)
            for key, inner in value.items()
        }
    return value


def sample_rows(
    rows: list[Any],
    row_builder: Callable[[Any], dict[str, Any] | None],
    *,
    limit: int,
    text_limit: int,
) -> list[dict[str, Any]]:
    sampled: list[dict[str, Any]] = []
    for row in rows:
        built = row_builder(row)
        if not built:
            continue
        sampled.append(truncate_stage_value(built, text_limit=text_limit))
        if len(sampled) >= limit:
            break
    return sampled


def sample_strings(values: list[str], *, limit: int, text_limit: int) -> list[str]:
    return [
        truncate_stage_text(value, limit=text_limit)
        for value in values[:limit]
        if value
    ]


def build_stage_block_not_reached(
    *,
    stage: str,
    stage_result_builder: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    stage_result = stage_result_builder(stage)
    return {
        "stage_id": stage,
        "stage": stage,
        "status": "not_reached",
        "stage_result": stage_result,
        "input_counts": {},
        "output_counts": {},
        "decision_summary": {},
        "outputs_sample": [],
        "dropped_or_changed_sample": [],
        "inputs_sample": [],
    }


def build_stage_block(
    *,
    stage_id: str,
    status: str,
    input_counts: dict[str, Any],
    output_counts: dict[str, Any],
    decision_summary: dict[str, Any],
    inputs_sample: list[dict[str, Any]],
    outputs_sample: list[dict[str, Any]],
    dropped_or_changed_sample: list[dict[str, Any]],
    truncate_value: Callable[[Any], Any],
    stage_result_builder: Callable[[str, str, dict[str, Any], dict[str, Any], dict[str, Any]], dict[str, Any]],
    settings_refs: list[str] | None = None,
    late_stage_mode: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stage_result = stage_result_builder(
        stage_id,
        status,
        input_counts,
        output_counts,
        decision_summary,
    )
    block: dict[str, Any] = {
        "stage_id": stage_id,
        "stage": stage_id,
        "status": status,
        "stage_result": stage_result,
        "input_counts": input_counts,
        "output_counts": output_counts,
        "decision_summary": decision_summary,
        "outputs_sample": outputs_sample,
        "dropped_or_changed_sample": dropped_or_changed_sample,
        "inputs_sample": inputs_sample,
    }
    if settings_refs:
        block["settings_refs"] = settings_refs
    if late_stage_mode:
        block["late_stage_mode"] = late_stage_mode
    return cast(dict[str, Any], truncate_value(block))


def build_normalize_stage_block(
    *,
    raw_jobs: list[dict[str, Any]],
    normalized: list[dict[str, Any]],
    deduplicated_jobs: list[dict[str, Any]],
    dedupe_reason_counts: dict[str, Any],
    stage_block_builder: Callable[..., dict[str, Any]],
    sample_rows_builder: Callable[[list[Any], Callable[[Any], dict[str, Any] | None]], list[dict[str, Any]]],
    job_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    dedupe_reason_label_resolver: Callable[[str], str],
) -> dict[str, Any]:
    return stage_block_builder(
        stage_id="normalize",
        status="completed",
        input_counts={"raw_jobs": len(raw_jobs)},
        output_counts={
            "normalized_jobs": len(normalized),
            "deduplicated_jobs": len(deduplicated_jobs),
        },
        decision_summary={"dedupe_reason_counts": dedupe_reason_counts},
        inputs_sample=sample_rows_builder(raw_jobs, job_sample_builder),
        outputs_sample=sample_rows_builder(normalized, job_sample_builder),
        dropped_or_changed_sample=sample_rows_builder(
            deduplicated_jobs,
            lambda job: {
                **(job_sample_builder(job) or {}),
                "change_type": "deduplicated_before_enrichment",
                "dedupe_reason": dedupe_reason_label_resolver(
                    str(job.get("dedupe_reason") or ""),
                ),
            }
            if job_sample_builder(job)
            else None,
        ),
    )


def build_enrich_stage_block(
    *,
    normalized: list[dict[str, Any]],
    pre_filter_rejected_jobs: list[dict[str, Any]],
    enriched: list[dict[str, Any]],
    profile: dict[str, Any],
    config: dict[str, Any],
    enrich_prompt_provenance: dict[str, Any],
    enrich_reuse_counts: dict[str, Any],
    enrich_reuse_metrics: dict[str, Any],
    stage_block_builder: Callable[..., dict[str, Any]],
    sample_rows_builder: Callable[[list[Any], Callable[[Any], dict[str, Any] | None]], list[dict[str, Any]]],
    job_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    extract_job_url: Callable[[dict[str, Any]], str],
    candidate_profile_summary_builder: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    rejected_urls = {
        extract_job_url(item) for item in pre_filter_rejected_jobs
    }
    surviving_normalized = [
        job for job in normalized if extract_job_url(job) not in rejected_urls
    ]
    return stage_block_builder(
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
            "candidate_profile_summary": candidate_profile_summary_builder(profile, config),
            "enrich_prompt_id": enrich_prompt_provenance["prompt_id"],
            "enrich_prompt_version": enrich_prompt_provenance["prompt_version"],
            "enrich_prompt_template_path": enrich_prompt_provenance["template_path"],
            "enrich_prompt_model": enrich_prompt_provenance["model"],
            **enrich_reuse_counts,
            "reuse_metrics": enrich_reuse_metrics,
        },
        inputs_sample=sample_rows_builder(
            surviving_normalized,
            job_sample_builder,
        ),
        outputs_sample=sample_rows_builder(enriched, job_sample_builder),
        dropped_or_changed_sample=sample_rows_builder(
            pre_filter_rejected_jobs,
            lambda job: {
                **(job_sample_builder(job) or {}),
                "change_type": "rejected_before_enrichment",
                "reasons": list(job.get("reasons") or []),
            }
            if job_sample_builder(job)
            else None,
        ),
    )


def build_rule_filter_stage_block(
    *,
    enriched: list[dict[str, Any]],
    passed_jobs: list[dict[str, Any]],
    candidate_filter_rejected_jobs: list[dict[str, Any]],
    grouped_reject_reasons: dict[str, Any],
    grouped_mark_codes: dict[str, Any],
    selected_rule_filters: list[str],
    stage_block_builder: Callable[..., dict[str, Any]],
    sample_rows_builder: Callable[[list[Any], Callable[[Any], dict[str, Any] | None]], list[dict[str, Any]]],
    job_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    rule_filter_decision_sample_builder: Callable[..., dict[str, Any] | None],
) -> dict[str, Any]:
    return stage_block_builder(
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
        inputs_sample=sample_rows_builder(enriched, job_sample_builder),
        outputs_sample=sample_rows_builder(
            passed_jobs,
            lambda job: rule_filter_decision_sample_builder(
                job,
                filter_outcome="pass",
            ),
        ),
        dropped_or_changed_sample=sample_rows_builder(
            candidate_filter_rejected_jobs,
            lambda job: (
                {
                    **(
                        rule_filter_decision_sample_builder(
                            job,
                            filter_outcome="reject",
                        )
                        or {}
                    ),
                    "change_type": "rejected_after_enrichment",
                }
                if rule_filter_decision_sample_builder(
                    job,
                    filter_outcome="reject",
                )
                else None
            ),
        ),
    )


def build_shortlist_stage_block(
    *,
    shortlist_reached: bool,
    passed_jobs: list[dict[str, Any]],
    raw_shortlist_urls: set[str],
    raw_shortlist_anomaly_urls: list[str],
    raw_shortlist: list[dict[str, Any]],
    shortlist: list[dict[str, Any]],
    backfilled_job_urls: list[str],
    shortlist_embedding_reuse_counts: dict[str, Any],
    shortlist_candidate_query_components: dict[str, Any],
    shortlist_candidate_query_debug: dict[str, Any],
    shortlist_quality_metrics: dict[str, Any],
    candidate_summary: str,
    vector_top_n: int,
    stage_block_builder: Callable[..., dict[str, Any]],
    stage_block_not_reached_builder: Callable[[str], dict[str, Any]],
    sample_rows_builder: Callable[[list[Any], Callable[[Any], dict[str, Any] | None]], list[dict[str, Any]]],
    sample_strings_builder: Callable[[list[str]], list[str]],
    job_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    shortlist_row_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    extract_job_url: Callable[[dict[str, Any]], str],
    extract_job_title: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    if not shortlist_reached:
        return stage_block_not_reached_builder("shortlist")
    dropped_rows: list[dict[str, Any]] = [
        *[
            {
                **job,
                "change_type": "not_returned_in_raw_hits",
                "shortlist_outcome": "not_returned_in_raw_hits",
                "raw_hit_present": False,
                "retrieval_anomaly_present": False,
            }
            for job in passed_jobs
            if extract_job_url(job) not in raw_shortlist_urls
            and extract_job_url(job) not in backfilled_job_urls
        ],
        *[
            {
                "job_url": job_url,
                **next(
                    (job for job in shortlist if extract_job_url(job) == job_url),
                    {"title": next((extract_job_title(job) for job in passed_jobs if extract_job_url(job) == job_url), "")},
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
    ]
    return stage_block_builder(
        stage_id="shortlist",
        status="completed",
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
            "jobs_not_returned_in_raw_hits": len([job for job in passed_jobs if extract_job_url(job) not in raw_shortlist_urls]),
            **shortlist_embedding_reuse_counts,
            "raw_shortlist_anomaly_urls": sample_strings_builder(raw_shortlist_anomaly_urls),
            "backfilled_job_urls": sample_strings_builder(backfilled_job_urls),
        },
        inputs_sample=sample_rows_builder(passed_jobs, job_sample_builder),
        outputs_sample=sample_rows_builder(shortlist, shortlist_row_sample_builder),
        dropped_or_changed_sample=sample_rows_builder(
            dropped_rows,
            lambda item: {
                **(job_sample_builder(item) or {"job_url": str(item.get("job_url") or ""), "job_title": str(item.get("title") or "")}),
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
    )


def build_ranking_stage_block(
    *,
    ranking_reached: bool,
    ai_scores: list[dict[str, Any]],
    ranking_inputs: list[dict[str, Any]],
    ranked: list[dict[str, Any]],
    ranked_urls: set[str],
    final_top_n: int,
    ranking_fit_distribution: dict[str, Any],
    ranking_quality_metrics: dict[str, Any],
    ranking_reuse_metrics: dict[str, Any],
    ranking_prompt_provenance: dict[str, Any],
    ranking_weights: dict[str, Any],
    ranking_defaults: dict[str, Any],
    preference_fit_weights: dict[str, Any],
    zero_weight_features: list[str],
    contributing_features: list[str],
    profile: dict[str, Any],
    config: dict[str, Any],
    stage_block_builder: Callable[..., dict[str, Any]],
    stage_block_not_reached_builder: Callable[[str], dict[str, Any]],
    sample_rows_builder: Callable[[list[Any], Callable[[Any], dict[str, Any] | None]], list[dict[str, Any]]],
    ranking_row_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    extract_job_url: Callable[[dict[str, Any]], str],
    gemini_model_resolver: Callable[[dict[str, Any]], str],
    effective_preferences_resolver: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    if not ranking_reached:
        return stage_block_not_reached_builder("ranking")
    return stage_block_builder(
        stage_id="ranking",
        status="completed",
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
            "ai_score_model": gemini_model_resolver(config),
            "configured_ranking_weights": ranking_weights,
            "configured_missing_value_defaults": ranking_defaults,
            "configured_preference_fit_weights": preference_fit_weights,
            "configured_fit_label_thresholds": dict(config.get("fit_label_thresholds") or {}),
            "zero_weight_features": zero_weight_features,
            "contributing_features": contributing_features,
            "candidate_preference_resolution": effective_preferences_resolver(profile, config),
        },
        inputs_sample=sample_rows_builder(ranking_inputs, ranking_row_sample_builder),
        outputs_sample=sample_rows_builder(ranked, ranking_row_sample_builder),
        dropped_or_changed_sample=sample_rows_builder(
            [row for row in ranking_inputs if extract_job_url(row) not in ranked_urls],
            lambda row: {
                **(ranking_row_sample_builder(row) or {}),
                "change_type": "scored_not_ranked",
            }
            if ranking_row_sample_builder(row)
            else None,
        ),
        settings_refs=[
            "ranking_weights",
            "preference_fit_weights",
            "missing_value_defaults",
            "fit_label_thresholds",
            "pipeline.final_top_n",
            "prompts.ranking.ai_score.prompt_id",
        ],
    )


def build_cv_analysis_stage_block(
    *,
    cv_analysis_reached: bool,
    ranked: list[dict[str, Any]],
    cv_analysis_results: list[dict[str, Any]],
    cv_analysis_quality_metrics: dict[str, Any],
    cv_analysis_reuse_metrics: dict[str, Any],
    config: dict[str, Any],
    agentic_late_stage_enabled: bool,
    blocked_by_reranker_status: str,
    ready_for_generation_status: str,
    skipped_fit_gate_status: str,
    failed_status: str,
    stage_block_builder: Callable[..., dict[str, Any]],
    stage_block_not_reached_builder: Callable[[str], dict[str, Any]],
    sample_rows_builder: Callable[[list[Any], Callable[[Any], dict[str, Any] | None]], list[dict[str, Any]]],
    ranking_row_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    analysis_record_output_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    analysis_record_changed_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    late_stage_mode_payload_builder: LateStageModePayloadBuilder,
) -> dict[str, Any]:
    if not cv_analysis_reached:
        return stage_block_not_reached_builder("cv_analysis")
    return stage_block_builder(
        stage_id="cv_analysis",
        status="completed",
        input_counts={"ranked_jobs": len(ranked)},
        output_counts={
            "blocked_by_reranker_fit": sum(
                1
                for record in cv_analysis_results
                if str(record.get("status") or "") == blocked_by_reranker_status
            ),
            "ready_for_generation": sum(
                1
                for record in cv_analysis_results
                if str(record.get("status") or "") == ready_for_generation_status
            ),
            "skipped_fit_gate": sum(
                1
                for record in cv_analysis_results
                if str(record.get("status") or "") == skipped_fit_gate_status
            ),
            "analysis_failed": sum(
                1
                for record in cv_analysis_results
                if str(record.get("status") or "") == failed_status
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
        inputs_sample=sample_rows_builder(ranked, ranking_row_sample_builder),
        outputs_sample=sample_rows_builder(cv_analysis_results, analysis_record_output_sample_builder),
        dropped_or_changed_sample=sample_rows_builder(
            cv_analysis_results,
            analysis_record_changed_sample_builder,
        ),
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
        late_stage_mode=late_stage_mode_payload_builder(
            agentic_late_stage_enabled=agentic_late_stage_enabled,
            stage_reached=cv_analysis_reached,
        ),
    )


def build_cv_generation_stage_block(
    *,
    cv_generation_reached: bool,
    cv_analysis_results: list[dict[str, Any]],
    cv_generation_debug_records: list[dict[str, Any]],
    cv_status_counts: dict[str, Any],
    cv_generation_quality_metrics: dict[str, Any],
    cv_generation_reuse_metrics: dict[str, Any],
    cv_generation_prompt_provenance: dict[str, Any],
    config: dict[str, Any],
    agentic_late_stage_enabled: bool,
    stage_block_builder: Callable[..., dict[str, Any]],
    stage_block_not_reached_builder: Callable[[str], dict[str, Any]],
    sample_rows_builder: Callable[[list[Any], Callable[[Any], dict[str, Any] | None]], list[dict[str, Any]]],
    analysis_record_output_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    debug_record_output_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    debug_record_changed_sample_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
    late_stage_mode_payload_builder: Callable[..., dict[str, Any]],
    cv_generation_model_summarizer: Callable[[list[dict[str, Any]], str], str],
    cv_generation_provider_summarizer: Callable[[list[dict[str, Any]]], str],
    cv_generation_model_resolver: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    if not cv_generation_reached:
        return stage_block_not_reached_builder("cv_generation")
    ready_records = [
        record
        for record in cv_analysis_results
        if str(record.get("status") or "") == "ready_for_generation"
    ]
    return stage_block_builder(
        stage_id="cv_generation",
        status="completed",
        input_counts={
            "analysis_ready_jobs": len(ready_records),
        },
        output_counts={
            "accepted": cv_status_counts["accepted_count"],
            "review_required": cv_status_counts["review_required_count"],
            "validation_failed": cv_status_counts["validation_failed_count"],
            "generation_failed": cv_status_counts["generation_failed_count"],
            "persistence_failed": cv_status_counts["persistence_failed_count"],
        },
        decision_summary={
            "debug_records_captured": cv_status_counts["debug_records_captured"],
            "analysis_ready_jobs_total": len(ready_records),
            "quality_metrics": cv_generation_quality_metrics,
            "reuse_metrics": cv_generation_reuse_metrics,
            "cv_generation_model": cv_generation_model_summarizer(
                cv_generation_debug_records,
                cv_generation_model_resolver(config),
            ),
            "cv_generation_provider": cv_generation_provider_summarizer(
                cv_generation_debug_records,
            ),
            "cv_prompt_id": cv_generation_prompt_provenance["prompt_id"],
            "cv_prompt_template_path": cv_generation_prompt_provenance["template_path"],
        },
        inputs_sample=sample_rows_builder(
            ready_records,
            analysis_record_output_sample_builder,
        ),
        outputs_sample=sample_rows_builder(
            cv_generation_debug_records,
            debug_record_output_sample_builder,
        ),
        dropped_or_changed_sample=sample_rows_builder(
            [
                record
                for record in cv_generation_debug_records
                if str(record.get("status") or "")
                in {"validation_failed", "generation_failed", "persistence_failed"}
            ],
            debug_record_changed_sample_builder,
        ),
        settings_refs=[
            "cv.generation.model",
            "prompts.cv_generation.structured_write.prompt_id",
        ],
        late_stage_mode=late_stage_mode_payload_builder(
            agentic_late_stage_enabled=agentic_late_stage_enabled,
            stage_reached=cv_generation_reached,
        ),
    )
