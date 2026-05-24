"""@meta
name: pipeline_observability
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Build bounded pipeline event payloads and render stage observation outputs.
inputs:
  - Pipeline stage records and formatting helper callbacks
outputs:
  - JSON-safe event payloads and markdown observation text
lifecycle:
  - status: active
"""

from typing import Any, Callable


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


def _extract_bounded_error_summary(
    error_payload: Any,
    *,
    bound_langfuse_excerpt: Callable[..., str],
    max_chars: int = 1000,
) -> str:
    if isinstance(error_payload, dict):
        return bound_langfuse_excerpt(
            str(error_payload.get("message") or error_payload.get("stage") or ""),
            max_chars=max_chars,
        )
    return bound_langfuse_excerpt(str(error_payload or ""), max_chars=max_chars)


def build_bounded_event_payload(
    *,
    event_name: str,
    event_family: str,
    source_stage: str,
    event_status: str,
    job_url: str | None = None,
    deterministic_outcome: str | None = None,
    stage_owned_subreason: str | None = None,
    confidence: float | None = None,
    fallback_used: bool = False,
    provenance: dict[str, Any] | None = None,
    input_snapshot: dict[str, Any] | None = None,
    output_snapshot: dict[str, Any] | None = None,
    artifact_refs: dict[str, Any] | None = None,
    latency_ms: int | None = None,
    usage: dict[str, Any] | None = None,
    cost: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_name": event_name,
        "event_family": event_family,
        "source_stage": source_stage,
        "event_status": event_status,
        "deterministic_outcome": deterministic_outcome,
        "fallback_used": fallback_used,
    }
    if job_url:
        payload["job_url"] = job_url
    if stage_owned_subreason is not None:
        payload["stage_owned_subreason"] = stage_owned_subreason
    if confidence is not None:
        payload["confidence"] = float(confidence)
    if provenance:
        payload["provenance"] = _json_safe_pipeline_value(provenance)
    if input_snapshot:
        payload["input_snapshot"] = _json_safe_pipeline_value(input_snapshot)
    if output_snapshot:
        payload["output_snapshot"] = _json_safe_pipeline_value(output_snapshot)
    if artifact_refs:
        payload["artifact_refs"] = _json_safe_pipeline_value(artifact_refs)
    if latency_ms is not None:
        payload["latency_ms"] = int(latency_ms)
    if usage:
        payload["usage"] = _json_safe_pipeline_value(usage)
    if cost:
        payload["cost"] = _json_safe_pipeline_value(cost)
    return payload


def render_cv_analysis_item_output(
    analysis_record: dict[str, Any],
    *,
    bound_langfuse_list: Callable[..., list[str]],
    bound_langfuse_excerpt: Callable[..., str],
    render_langfuse_markdown_sections: Callable[[list[tuple[str, list[str]]]], str],
) -> str:
    fit_decision = str(analysis_record.get("fit_classification") or "unknown")
    matched = bound_langfuse_list(
        list((analysis_record.get("gap_summary") or {}).get("matched") or []),
        max_items=8,
        max_item_chars=240,
    )
    risks = bound_langfuse_list(
        list((analysis_record.get("gap_summary") or {}).get("missing") or []),
        max_items=8,
        max_item_chars=240,
    )
    status = str(analysis_record.get("status") or "unknown")
    error_payload = analysis_record.get("outcome_reason") or analysis_record.get("error")
    error_summary = _extract_bounded_error_summary(error_payload, bound_langfuse_excerpt=bound_langfuse_excerpt)
    reasoning_summary = bound_langfuse_excerpt(status, max_chars=1000) or "unknown"
    sections = [
        ("## Fit Decision", [fit_decision]),
        ("## Fit Score", [str(analysis_record.get("fit_score") or "n/a")]),
        ("## Reasoning Summary", [reasoning_summary]),
        ("## Evidence", [f"- {item}" for item in matched] or ["- No bounded evidence available"]),
        ("## Risks", [f"- {item}" for item in risks]),
        (
            "## Generation Readiness",
            ["true" if status == "ready_for_generation" else "false"],
        ),
    ]
    if error_summary:
        sections.append(("## Failure Summary", [error_summary]))
    return render_langfuse_markdown_sections(sections)


def render_cv_generation_item_output(
    debug_record: dict[str, Any],
    *,
    cv_generation_review_required_status: str,
    bound_langfuse_markdown: Callable[..., str],
    bound_langfuse_excerpt: Callable[..., str],
    bound_langfuse_issue_list: Callable[[list[Any]], list[str]],
    render_langfuse_markdown_sections: Callable[[list[tuple[str, list[str]]]], str],
) -> str:
    status = str(debug_record.get("status") or "unknown")
    markdown_final = bound_langfuse_markdown(str(debug_record.get("markdown_final") or "").strip(), max_chars=12000)
    validation_initial = dict(debug_record.get("validation_initial") or {})
    review_issue_inputs = (
        list(validation_initial.get("missing_sections") or [])
        + list(validation_initial.get("grounding_violations") or [])
        + list(validation_initial.get("skill_violations") or [])
        + list(validation_initial.get("markdown_quality_blocking_issues") or [])
        + list(validation_initial.get("markdown_quality_review_flags") or [])
    )
    validation_valid = bool(validation_initial.get("valid")) if validation_initial else status == "accepted"
    error_payload = debug_record.get("error")
    failure_summary = _extract_bounded_error_summary(error_payload, bound_langfuse_excerpt=bound_langfuse_excerpt)
    if status == cv_generation_review_required_status and not review_issue_inputs and failure_summary:
        review_issue_inputs.append(failure_summary)
    review_issues = bound_langfuse_issue_list(review_issue_inputs)
    sections = [
        ("## Status", [status]),
        ("## Generated CV Markdown", [markdown_final or "No markdown generated"]),
        ("## Validation Summary", ["valid" if validation_valid else status]),
        ("## Review Issues", [f"- {item}" for item in review_issues]),
        ("## Persistence Outcome", ["stored" if status == "accepted" else status]),
    ]
    if failure_summary:
        sections.append(("## Failure Summary", [failure_summary]))
    return render_langfuse_markdown_sections(sections)


def build_cv_generation_item_observation_attributes(
    *,
    run_id: str,
    analysis_record: dict[str, Any],
    debug_record: dict[str, Any],
    cv_generation_review_required_status: str,
    extract_job_url: Callable[[dict[str, Any]], str],
    extract_job_title: Callable[[dict[str, Any]], str],
    bound_langfuse_list: Callable[..., list[str]],
    bound_langfuse_excerpt: Callable[..., str],
    bound_langfuse_issue_list: Callable[[list[Any]], list[str]],
    bound_langfuse_markdown: Callable[..., str],
    build_langfuse_item_observation_attributes: Callable[..., dict[str, Any]],
    render_cv_generation_item_input: Callable[..., str],
    render_cv_generation_item_output: Callable[..., str],
) -> dict[str, Any]:
    job = dict(analysis_record.get("job_snapshot") or {})
    status = str(debug_record.get("status") or "")
    required_skills = bound_langfuse_list(
        [str(item).strip() for item in list(job.get("required_skills") or []) if str(item).strip()],
        max_items=8,
        max_item_chars=300,
    )
    evidence_lines = bound_langfuse_list(
        [
            str(item.get("name") or item.get("source_ref") or item.get("evidence_type") or "evidence").strip()
            for item in list(debug_record.get("evidence_used") or [])
            if str(item.get("name") or item.get("source_ref") or item.get("evidence_type") or "").strip()
        ],
        max_items=8,
        max_item_chars=240,
    )
    validation_initial = dict(debug_record.get("validation_initial") or {})
    review_issue_inputs = (
        list(validation_initial.get("missing_sections") or [])
        + list(validation_initial.get("grounding_violations") or [])
        + list(validation_initial.get("skill_violations") or [])
        + list(validation_initial.get("markdown_quality_blocking_issues") or [])
        + list(validation_initial.get("markdown_quality_review_flags") or [])
    )
    validation_valid = bool(validation_initial.get("valid")) if validation_initial else status == "accepted"
    error_payload = debug_record.get("error")
    failure_summary = _extract_bounded_error_summary(error_payload, bound_langfuse_excerpt=bound_langfuse_excerpt)
    if status == cv_generation_review_required_status and not review_issue_inputs and failure_summary:
        review_issue_inputs.append(failure_summary)
    review_issues = bound_langfuse_issue_list(review_issue_inputs)
    input_structured = {
        "job_id": extract_job_url(job) or extract_job_title(job),
        "job_excerpt": bound_langfuse_excerpt(str(job.get("description") or ""), max_chars=1500),
        "constraints": required_skills,
        "selected_evidence": evidence_lines,
        "analysis_inputs": {
            "analysis_input_fingerprint": analysis_record.get("analysis_input_fingerprint"),
            "fit_classification": analysis_record.get("fit_classification"),
        },
        "generation_instructions": bound_langfuse_excerpt(
            str(job.get("generation_instructions") or "Generate grounded CV sections only from selected evidence."),
            max_chars=2000,
        ),
    }
    output_structured = {
        "status": status,
        "cv_markdown": bound_langfuse_markdown(str(debug_record.get("markdown_final") or "").strip(), max_chars=12000),
        "cv_structured": debug_record.get("structured_cv_final"),
        "validation_summary": {
            "valid": validation_valid,
            "review_issues": review_issues,
        },
        "persistence_outcome": "stored" if status == "accepted" else status,
        "failure_summary": failure_summary,
    }
    metadata = {
        "run_id": run_id,
        "job_url": extract_job_url(job),
        "job_title": extract_job_title(job),
        "status": status,
        "selected": status == "accepted",
        "attempt_count": debug_record.get("attempt_count"),
        "analysis_input_fingerprint": analysis_record.get("analysis_input_fingerprint"),
        "parent_observation_name": "cv_analysis_item",
        "disposition": status,
        "output_structured": output_structured,
    }
    return build_langfuse_item_observation_attributes(
        observation_name="cv_generation_item",
        observation_type="generation",
        rendered_input=render_cv_generation_item_input(
            job=job,
            evidence_used=list(debug_record.get("evidence_used") or []),
            fit_classification=debug_record.get("fit_classification"),
        ),
        rendered_output=render_cv_generation_item_output(debug_record),
        input_structured=input_structured,
        output_structured=output_structured,
        metadata=metadata,
        prompt_name="cv_generation_item",
        extra_attributes={
            "fitcv.run_id": run_id,
            "fitcv.job_url": extract_job_url(job),
            "fitcv.stage_id": "cv_generation",
        },
    )


def build_cv_analysis_item_observation_attributes(
    *,
    run_id: str,
    profile: dict[str, Any],
    job: dict[str, Any],
    analysis_record: dict[str, Any],
    extract_job_url: Callable[[dict[str, Any]], str],
    extract_job_title: Callable[[dict[str, Any]], str],
    flatten_skills: Callable[[dict[str, Any]], list[str]],
    bound_langfuse_list: Callable[..., list[str]],
    bound_langfuse_excerpt: Callable[..., str],
    build_langfuse_item_observation_attributes: Callable[..., dict[str, Any]],
    render_cv_analysis_item_input: Callable[..., str],
    render_cv_analysis_item_output: Callable[..., str],
) -> dict[str, Any]:
    status = str(analysis_record.get("status") or "")
    fit_decision = str(analysis_record.get("fit_classification") or "unknown")
    generation_readiness = status == "ready_for_generation"
    error_payload = analysis_record.get("outcome_reason") or analysis_record.get("error")
    error_summary = _extract_bounded_error_summary(error_payload, bound_langfuse_excerpt=bound_langfuse_excerpt)
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
    requirements_excerpt = bound_langfuse_list(
        [str(item).strip() for item in list(job.get("required_skills") or []) if str(item).strip()],
        max_items=8,
        max_item_chars=300,
    )
    matched = bound_langfuse_list(
        list((analysis_record.get("gap_summary") or {}).get("matched") or []),
        max_items=8,
        max_item_chars=240,
    )
    risks = bound_langfuse_list(
        list((analysis_record.get("gap_summary") or {}).get("missing") or []),
        max_items=8,
        max_item_chars=240,
    )
    reasoning_summary = bound_langfuse_excerpt(status, max_chars=1000)
    input_structured = {
        "job_id": extract_job_url(job) or extract_job_title(job),
        "job_title": extract_job_title(job),
        "job_excerpt": bound_langfuse_excerpt(str(job.get("description") or ""), max_chars=1500),
        "requirements_excerpt": requirements_excerpt,
        "candidate_excerpt": {
            "headline": bound_langfuse_excerpt(str(profile.get("headline") or ""), max_chars=240),
            "skills": candidate_skills,
            "experience_highlights": experience_highlights,
        },
        "instructions": bound_langfuse_excerpt(
            str(job.get("analysis_instructions") or "Evaluate fit for generation readiness."),
            max_chars=2000,
        ),
        "rubric": {
            "fit_dimensions": ["domain", "seniority", "stack", "scope"],
        },
        "context_refs": {
            "analysis_input_fingerprint": analysis_record.get("analysis_input_fingerprint"),
            "retrieval_context_summary": bound_langfuse_excerpt(
                ", ".join(matched) if matched else "No bounded retrieval context available",
                max_chars=1000,
            ),
        },
    }
    output_structured = {
        "fit_decision": fit_decision,
        "fit_score": analysis_record.get("fit_score"),
        "reasoning_summary": reasoning_summary,
        "evidence": matched,
        "risks": risks,
        "generation_readiness": generation_readiness,
        "disposition": status,
        "failure_summary": error_summary,
    }
    metadata = {
        "run_id": run_id,
        "job_url": extract_job_url(job),
        "job_title": extract_job_title(job),
        "analysis_input_fingerprint": analysis_record.get("analysis_input_fingerprint"),
        "analysis_reuse_status": analysis_record.get("analysis_reuse_status"),
        "ranking_fit_label": analysis_record.get("ranking_fit_label"),
        "reuse_status": analysis_record.get("analysis_reuse_status"),
        "deterministic_gate_result": status,
        "selected": True,
        "status": status,
    }
    return build_langfuse_item_observation_attributes(
        observation_name="cv_analysis_item",
        observation_type="generation",
        rendered_input=render_cv_analysis_item_input(profile=profile, job=job),
        rendered_output=render_cv_analysis_item_output(analysis_record),
        input_structured=input_structured,
        output_structured=output_structured,
        metadata=metadata,
        prompt_name="cv_analysis_item",
        extra_attributes={
            "fitcv.run_id": run_id,
            "fitcv.job_url": extract_job_url(job),
            "fitcv.stage_id": "cv_analysis",
        },
    )
