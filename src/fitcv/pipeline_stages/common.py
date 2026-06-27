"""@meta
name: pipeline_stages.common
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared stage helper utilities extracted from src.fitcv.pipeline under Task 6 (A5).
inputs:
  - Pipeline stage rows/records and config mappings.
outputs:
  - Normalized helper outputs for telemetry and artifact shaping.
lifecycle:
  - status: active
"""

from __future__ import annotations

from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlparse

from fitcv.candidate import flatten_skills, infer_effective_preferences


def pipeline_int(config: Mapping[str, Any], key: str, *, default: int = 0) -> int:
    pipeline_block = config.get("pipeline")
    if not isinstance(pipeline_block, dict):
        return int(default)
    value = pipeline_block.get(key, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def extract_job_url(job: Mapping[str, Any]) -> str:
    return str(job.get("job_url") or job.get("jobUrl") or "")

def normalize_job_url_key(job_url: str | None) -> str:
    normalized_url = str(job_url or "").strip()
    if not normalized_url:
        return ""
    try:
        parsed = urlparse(normalized_url)
    except Exception:
        return normalized_url.rstrip("/")
    if not parsed.scheme and not parsed.netloc:
        return normalized_url.rstrip("/")
    normalized_scheme = str(parsed.scheme or "").lower()
    normalized_netloc = str(parsed.netloc or "").lower()
    normalized_path = str(parsed.path or "").rstrip("/")
    normalized_query = ""
    if normalized_netloc.endswith("indeed.com") and normalized_path == "/viewjob":
        stable_pairs = [
            (str(key or "").lower(), str(value or "").strip())
            for key, value in parse_qsl(parsed.query, keep_blank_values=False)
            if str(key or "").lower() in {"jk", "vjk"} and str(value or "").strip()
        ]
        if stable_pairs:
            stable_pairs.sort()
            normalized_query = urlencode(stable_pairs)
    return parsed._replace(
        scheme=normalized_scheme,
        netloc=normalized_netloc,
        path=normalized_path,
        params="",
        query=normalized_query,
        fragment="",
    ).geturl()

def job_identity_keys(job: Mapping[str, Any]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()

    raw_job_fingerprint = str(job.get("raw_job_fingerprint") or "").strip()
    if raw_job_fingerprint:
        fingerprint_key = f"fp:{raw_job_fingerprint}"
        keys.append(fingerprint_key)
        seen.add(fingerprint_key)

    for field_name in ("source_job_url", "job_url", "jobUrl"):
        normalized_url = normalize_job_url_key(str(job.get(field_name) or ""))
        if not normalized_url:
            continue
        url_key = f"url:{normalized_url}"
        if url_key in seen:
            continue
        keys.append(url_key)
        seen.add(url_key)

    return keys


def extract_job_title(job: Mapping[str, Any]) -> str:
    return str(job.get("title") or job.get("job_title") or "")


def normalize_shortlist_row(shortlist_row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "vector_similarity": shortlist_row.get("vector_similarity", shortlist_row.get("similarity_score")),
        "vector_rank": shortlist_row.get("vector_rank", shortlist_row.get("rank")),
        "shortlist_origin": str(shortlist_row.get("shortlist_origin") or "vector_search"),
    }


def json_safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe_value(item) for item in value]
    if isinstance(value, set):
        return [json_safe_value(item) for item in sorted(value)]
    return value

def shortlist_outcome_for_row(
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

def unique_job_urls(rows: list[Mapping[str, Any]]) -> list[str]:
    urls: list[str] = []
    seen_urls: set[str] = set()
    for row in rows:
        job_url = extract_job_url(row)
        if not job_url or job_url in seen_urls:
            continue
        seen_urls.add(job_url)
        urls.append(job_url)
    return urls

def compute_raw_shortlist_anomaly_urls(
    raw_shortlist: list[Mapping[str, Any]],
    passed_jobs: list[Mapping[str, Any]],
) -> list[str]:
    passed_job_urls = {extract_job_url(job) for job in passed_jobs if extract_job_url(job)}
    return [
        job_url
        for job_url in unique_job_urls(raw_shortlist)
        if job_url not in passed_job_urls
    ]

def job_sample(
    job: Mapping[str, Any],
    *,
    export_fields: tuple[str, ...],
) -> dict[str, Any] | None:
    job_url = extract_job_url(job)
    if not job_url:
        return None
    sample: dict[str, Any] = {
        "job_url": job_url,
        "job_title": extract_job_title(job),
        "company": str(job.get("company_name") or job.get("companyName") or ""),
    }
    optional_fields: dict[str, Any] = {}
    for field in export_fields:
        optional_fields[field] = job.get(field)
    for key, value in optional_fields.items():
        if value not in (None, "", []):
            sample[key] = value
    marks = list(job.get("marks") or [])
    if marks:
        sample["marks"] = marks
    return sample

def candidate_profile_summary(
    profile: Mapping[str, Any],
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    preferences = dict(profile.get("preferences") or {})
    preference_resolution = infer_effective_preferences(dict(profile), dict(config) if config is not None else None)
    flattened_skills = flatten_skills(dict(profile))
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

def shortlist_row_sample(row: Mapping[str, Any]) -> dict[str, Any] | None:
    job_url = extract_job_url(row)
    if not job_url:
        return None
    shortlist_origin = str(row.get("shortlist_origin") or "vector_search")
    raw_hit_present = bool(row.get("raw_hit_present", shortlist_origin != "backfill"))
    retrieval_anomaly_present = bool(row.get("retrieval_anomaly_present", False))
    sample = {
        "job_url": job_url,
        "job_title": extract_job_title(row),
        "vector_similarity": row.get("vector_similarity", row.get("similarity_score")),
        "vector_rank": row.get("vector_rank", row.get("rank")),
        "shortlist_origin": shortlist_origin,
        "shortlist_outcome": shortlist_outcome_for_row(
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

def ranking_row_sample(row: Mapping[str, Any]) -> dict[str, Any] | None:
    job_url = extract_job_url(row)
    if not job_url:
        return None
    effective_preferences = row.get("effective_preferences")
    effective_preferences_dict = effective_preferences if isinstance(effective_preferences, dict) else {}
    sample = {
        "job_url": job_url,
        "job_title": extract_job_title(row),
        "ai_score": row.get("ai_score"),
        "ai_score_reuse_status": row.get("ai_score_reuse_status"),
        "ai_score_input_fingerprint": row.get("ai_score_input_fingerprint"),
        "reranker_parser_status": row.get("parser_status"),
        "diagnostic_score_reasoning": row.get("score_reasoning"),
        "diagnostic_key_risks": row.get("key_risks"),
        "diagnostic_matched_strengths": row.get("matched_strengths"),
        "reranker_score_reasoning": row.get("score_reasoning"),
        "reranker_key_risks": row.get("key_risks"),
        "reranker_matched_strengths": row.get("matched_strengths"),
        "must_have_match": row.get("must_have_match"),
        "vector_similarity": row.get("vector_similarity"),
        "title_relevance": row.get("title_relevance"),
        "seniority_fit": row.get("seniority_fit"),
        "preference_fit": row.get("preference_fit"),
        "feature_contributions": row.get("feature_contributions"),
        "preference_fit_components": row.get("preference_fit_components"),
        "effective_target_role": effective_preferences_dict.get("target_role"),
        "effective_role_families": effective_preferences_dict.get("role_families"),
        "effective_domains": effective_preferences_dict.get("domains"),
        "preference_sources": row.get("preference_sources"),
        "final_score": row.get("final_score"),
        "ranking_fit_label": row.get("fit_label"),
        "shortlist_origin": row.get("shortlist_origin"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "")}

def analysis_record_output_sample(
    record: Mapping[str, Any],
    *,
    deterministic_truth_fields: Any,
) -> dict[str, Any] | None:
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
        **deterministic_truth_fields(status),
        "analysis_reuse_status": record.get("analysis_reuse_status"),
        "analysis_input_fingerprint": record.get("analysis_input_fingerprint"),
        "ranking_fit_label": record.get("ranking_fit_label"),
        "fit_classification": record.get("fit_classification"),
        "evidence_used": record.get("evidence_used"),
        "evidence_selection_summary": record.get("evidence_selection_summary"),
        "gap_summary": record.get("gap_summary"),
        "pre_writing_decision": record.get("pre_writing_decision"),
        "readiness_diagnostics": record.get("readiness_diagnostics"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "", [])}

def analysis_record_changed_sample(
    record: Mapping[str, Any],
    *,
    deterministic_truth_fields: Any,
) -> dict[str, Any] | None:
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
        **deterministic_truth_fields(status),
        "analysis_reuse_status": record.get("analysis_reuse_status"),
        "analysis_input_fingerprint": record.get("analysis_input_fingerprint"),
        "ranking_fit_label": record.get("ranking_fit_label"),
        "fit_classification": record.get("fit_classification"),
        "evidence_used": record.get("evidence_used"),
        "evidence_selection_summary": record.get("evidence_selection_summary"),
        "gap_summary": record.get("gap_summary"),
        "pre_writing_decision": record.get("pre_writing_decision"),
        "readiness_diagnostics": record.get("readiness_diagnostics"),
        "outcome_reason": record.get("outcome_reason") or record.get("error"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "", [])}

def debug_record_output_sample(
    record: Mapping[str, Any],
    *,
    deterministic_truth_fields: Any,
) -> dict[str, Any] | None:
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
        **deterministic_truth_fields(status),
        "ranking_fit_label": record.get("ranking_fit_label"),
        "fit_classification": record.get("fit_classification"),
        "analysis_input_summary": record.get("analysis_input_summary"),
        "evidence_used": record.get("evidence_used"),
        "evidence_selection_summary": record.get("evidence_selection_summary"),
        "gap_summary": record.get("gap_summary"),
        "validation_initial": record.get("validation_initial"),
        "repair_attempt": record.get("repair_attempt"),
        "structured_cv_final": record.get("structured_cv_final"),
        "enabled_sections": record.get("enabled_sections"),
        "cv_generation_model": record.get("cv_generation_model"),
        "cv_prompt_id": record.get("cv_prompt_id"),
        "cv_prompt_template_path": record.get("cv_prompt_template_path"),
        "cv_generation_reuse_status": record.get("cv_generation_reuse_status"),
        "cv_generation_input_fingerprint": record.get("cv_generation_input_fingerprint"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "", [])}

def debug_record_changed_sample(
    record: Mapping[str, Any],
    *,
    deterministic_truth_fields: Any,
) -> dict[str, Any] | None:
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
        **deterministic_truth_fields(status),
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
        "cv_generation_reuse_status": record.get("cv_generation_reuse_status"),
        "cv_generation_input_fingerprint": record.get("cv_generation_input_fingerprint"),
        "error": record.get("error"),
    }
    return {key: value for key, value in sample.items() if value not in (None, "", [])}



