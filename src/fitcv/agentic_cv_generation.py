"""@meta
name: agentic_cv_generation
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.agentic_cv_generation.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from copy import deepcopy
from contextlib import contextmanager
import datetime
import json
from pathlib import Path
import importlib
import os
import sys
import time
from typing import Any, Callable, Final, Iterator, Literal, TypedDict, cast

from fitcv.agentic_cv_analysis import (
    BLOCKED_BY_RERANKER_STATUS,
    READY_FOR_GENERATION_STATUS,
    SKIPPED_FIT_GATE_STATUS,
    FitClassification,
    build_analysis_input_summary,
    build_decision_chain,
    build_evidence_used,
    extract_job_title,
    extract_job_url,
)
from fitcv.candidate_name_policy import is_candidate_name_placeholder, resolved_candidate_profile_name
from fitcv.config import get_cv_generation_model
from fitcv.runtime_routing import (
    build_langgraph_env_overrides,
    resolve_cv_generation_runtime_provenance,
)
from fitcv.cv_generator import (
    _normalize_structured_cv,
    _resolve_template_path,
    build_live_structured_cv_response_schema as _canonical_live_structured_cv_response_schema,
    build_structured_generation_prompt,
    generate_cv,
    render_cv_markdown,
)
from fitcv.validator import AnalysisGroundingPayload, run_all_validations

ACCEPTED_STATUS: Final[Literal["accepted"]] = "accepted"
VALIDATION_FAILED_STATUS: Final[Literal["validation_failed"]] = "validation_failed"
GENERATION_FAILED_STATUS: Final[Literal["generation_failed"]] = "generation_failed"
DEFAULT_MAX_SUMMARY_LINES = 3
DEFAULT_FITCV_LANGGRAPH_REPO_NAME = "fitcv-langgraph"

_REPAIRABLE_VALIDATION_FIELDS = ("grounding_violations", "skill_violations")
GenerationStatus = Literal[
    "accepted",
    "validation_failed",
    "generation_failed",
    "blocked_by_reranker_fit",
    "skipped_fit_gate",
    "analysis_failed",
]


class RepairAttempt(TypedDict, total=False):
    performed: bool
    missing_sections: list[str]
    reason: str


class ValidationSnapshot(TypedDict):
    valid: bool
    missing_sections: list[str]
    grounding_violations: list[str]
    deterministic_grounding_violations: list[str]
    semantic_grounding_violations: list[str]
    skill_violations: list[str]
    warnings: list[str]
    support_source_summary: dict[str, Any]
    markdown_quality_blocking_issues: list[str]
    markdown_quality_review_flags: list[str]


class ErrorPayload(TypedDict):
    stage: str
    message: str


class CvGenerationResult(TypedDict, total=False):
    job_url: str
    job_title: str
    status: GenerationStatus
    ranking_fit_label: str | None
    fit_classification: FitClassification | None
    decision_chain: dict[str, Any]
    analysis_input_summary: dict[str, Any]
    evidence_used: list[dict[str, Any]]
    evidence_selection_summary: dict[str, Any]
    gap_summary: dict[str, Any] | None
    structured_cv_initial: dict[str, Any] | None
    validation_initial: ValidationSnapshot | None
    repair_attempt: RepairAttempt
    structured_cv_final: dict[str, Any] | None
    markdown_final: str | None
    validation: dict[str, Any] | None
    outcome_reason: ErrorPayload | None
    error: ErrorPayload | None
    runtime_provenance: dict[str, Any]
    agentic_live_trace: dict[str, Any]


_LIVE_TRACE_SCHEMA_VERSION = "agentic_step_trace_record_v1"
_LIVE_TRACE_SCHEMA_NAME = "fitcv_structured_cv_document"
_LIVE_TRACE_PROMPT_CONTRACT = "fitcv_structured_generation_prompt"
_LIVE_TRACE_FAMILY = "agentic_step_trace"
_LIVE_TRACE_STEP_ID = "cv_generation"
_LIVE_TRACE_DEBUG_ENV_KEYS = (
    "FITCV_LANGGRAPH_DEBUG_LIVE",
    "FITCV_LANGGRAPH_DEBUG_LIVE_DUMP_PATH",
)



def _empty_repair_attempt() -> RepairAttempt:
    return {
        "performed": False,
        "missing_sections": [],
    }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _discover_fitcv_langgraph_repo_root() -> Path | None:
    env_value = os.environ.get("FITCV_LANGGRAPH_REPO_ROOT", "").strip()
    if env_value:
        candidate = Path(env_value)
        if candidate.is_dir():
            return candidate
    for ancestor in _repo_root().parents:
        candidate = ancestor / DEFAULT_FITCV_LANGGRAPH_REPO_NAME
        if candidate.is_dir():
            return candidate
    return None


def _build_fitcv_langgraph_env_values(repo_root: Path | None) -> dict[str, str]:
    del repo_root
    env_values = dict(os.environ)
    for key, value in build_langgraph_env_overrides().items():
        env_values.setdefault(key, value)
    return env_values

@contextmanager
def _temporary_environ(values: dict[str, str]) -> Iterator[None]:
    previous: dict[str, str | None] = {
        key: os.environ.get(key)
        for key in values
    }
    os.environ.update(values)
    try:
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def _build_requirement_priorities(job: dict[str, Any]) -> list[dict[str, Any]]:
    priorities: list[dict[str, Any]] = []
    for index, requirement in enumerate(list(job.get("required_skills") or [])):
        priorities.append(
            {
                "requirement": str(requirement),
                "priority": "primary" if index < 2 else "secondary",
                "target_sections": ["experience", "skills"],
            }
        )
    return priorities


def _build_generation_ready_analysis(
    analysis_record: dict[str, Any],
    profile: dict[str, Any],
    job: dict[str, Any],
) -> dict[str, Any]:
    allowed_claim_ids = [
        str(item.get("evidence_id") or item.get("claim_id") or "")
        for item in list(analysis_record.get("evidence_payload") or [])
        if str(item.get("evidence_id") or item.get("claim_id") or "")
    ]
    required_skills = [str(skill) for skill in list(job.get("required_skills") or []) if str(skill)]
    requirement_priorities = _build_requirement_priorities(job)
    hold_reason = str(
        (analysis_record.get("outcome_reason") or analysis_record.get("error") or {}).get("message") or ""
    ).strip()
    ready_for_generation = str(analysis_record.get("status") or "") == READY_FOR_GENERATION_STATUS
    unsupported_requirements_count = 0 if allowed_claim_ids else len(required_skills)
    selected_claim_ids = list(allowed_claim_ids)
    return {
        "analysis_id": str(analysis_record.get("analysis_input_fingerprint") or extract_job_url(job) or "analysis"),
        "job_input": {
            "title": str(job.get("title") or job.get("job_title") or analysis_record.get("job_title") or ""),
            "company": str(job.get("company") or job.get("companyName") or ""),
        },
        "profile_input": {
            "candidate_name": resolved_candidate_profile_name(profile) or str(profile.get("name") or ""),
        },
        "required_sections": ["summary", "experience", "skills"],
        "generation_constraints": {
            "max_summary_lines": DEFAULT_MAX_SUMMARY_LINES,
        },
        "analysis_context": {
            "allowed_claim_ids": selected_claim_ids,
        },
        "requirement_priorities": requirement_priorities,
        "allowed_claim_evidence": [
            {
                "claim_id": claim_id,
                "evidence": claim_id,
                "supports_requirements": required_skills,
            }
            for claim_id in selected_claim_ids
        ],
        "pre_writing_decision": {
            "ready_for_generation": ready_for_generation,
            "hold_reasons": [] if ready_for_generation else [hold_reason or "Generation blocked by upstream hold."],
            "uncertainty_notes": [],
        },
        "readiness_diagnostics": {
            "supported_requirements_count": len(required_skills) if selected_claim_ids else 0,
            "unsupported_requirements_count": unsupported_requirements_count,
            "weak_evidence_claim_ids": [],
            "selected_evidence_claim_ids": selected_claim_ids,
            "readiness_score": len(selected_claim_ids),
            "score_components": {
                "support_points": len(selected_claim_ids),
                "unsupported_requirement_penalty": unsupported_requirements_count,
                "weak_evidence_penalty": 0,
                "manual_review_penalty": 0,
            },
            "generation_ready_reason": (
                "Ready for generation from FitCV late-stage adapter."
                if ready_for_generation
                else "Blocked before generation by FitCV late-stage adapter."
            ),
        },
    }

def _augmented_gap_summary_from_analysis(analysis_record: dict[str, Any]) -> dict[str, Any]:
    gap_summary = dict(analysis_record.get("gap_summary") or {})
    do_not_claim = [str(item) for item in list(analysis_record.get("do_not_claim") or []) if str(item)]
    requirement_coverage = [
        dict(item)
        for item in list(analysis_record.get("requirement_coverage") or [])
        if isinstance(item, dict)
    ]
    section_confidence_hints = dict(analysis_record.get("section_confidence_hints") or {})
    if do_not_claim:
        gap_summary["do_not_claim"] = do_not_claim
    if requirement_coverage:
        gap_summary["requirement_coverage"] = requirement_coverage
    if section_confidence_hints:
        gap_summary["section_confidence_hints"] = section_confidence_hints
    return gap_summary



def _build_runtime_provenance(env_values: dict[str, str]) -> dict[str, Any]:
    provider = str(env_values.get("FITCV_LANGGRAPH_PROVIDER", "openai") or "openai").strip().lower()
    model = str(env_values.get("FITCV_LANGGRAPH_MODEL", "") or "").strip() or None
    base_url = str(env_values.get("FITCV_LANGGRAPH_OPENAI_BASE_URL", "") or "").strip() or None
    return {
        "runtime_path": "fitcv_langgraph_live",
        "provider": provider,
        "model": model,
        "base_url": base_url,
    }


def _live_runtime_provenance_or_none() -> dict[str, Any] | None:
    repo_root = _discover_fitcv_langgraph_repo_root()
    env_values = _build_fitcv_langgraph_env_values(repo_root)
    try:
        runtime_loader = globals().get("load_live_provider_config_from_env")
        if runtime_loader is None:
            if repo_root is not None:
                src_root = repo_root / "src"
                if src_root.is_dir():
                    src_root_text = str(src_root)
                    if src_root_text not in sys.path:
                        sys.path.insert(0, src_root_text)
            live_module = importlib.import_module("fitcv_langgraph.providers.live")
            runtime_loader = getattr(live_module, "load_live_provider_config_from_env")
        runtime_loader(env_values)
    except Exception:
        return None
    return _build_runtime_provenance(env_values)


def _build_live_structured_cv_response_schema() -> dict[str, Any]:
    return _canonical_live_structured_cv_response_schema(config={})


def _generate_cv_with_live_provider(
    *,
    job: dict[str, Any],
    evidence: list[dict[str, Any]],
    gap: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
    fit_classification: str,
    evidence_selection_summary: dict[str, Any] | None,
    repair_missing_sections: list[str] | None,
    env_values: dict[str, str],
    trace_attempt: dict[str, Any] | None = None,
    attempt_index: int = 1,
) -> dict[str, Any]:
    repo_root = _discover_fitcv_langgraph_repo_root()
    if repo_root is not None:
        src_root = repo_root / "src"
        if src_root.is_dir():
            src_root_text = str(src_root)
            if src_root_text not in sys.path:
                sys.path.insert(0, src_root_text)
    live_module = importlib.import_module("fitcv_langgraph.providers.live")
    runtime_loader = getattr(live_module, "load_live_provider_config_from_env")
    client_cls = getattr(live_module, "OpenAIResponsesClient")
    provider_config = runtime_loader(env_values)
    client = client_cls(provider_config)

    template_path = _resolve_template_path(config)
    template = Path(template_path).read_text(encoding="utf-8")
    prompt = build_structured_generation_prompt(
        jd=job,
        evidence=evidence,
        gap=gap,
        template=template,
        profile=profile,
        config=config,
        evidence_selection_summary=evidence_selection_summary,
        repair_missing_sections=repair_missing_sections,
    )
    started_at = datetime.datetime.now(datetime.timezone.utc)
    started_monotonic = time.monotonic()
    if trace_attempt is not None:
        trace_attempt.update(
            {
                "attempt_index": attempt_index,
                "attempt_type": "repair_retry" if repair_missing_sections else "initial_generation",
                "started_at": started_at.isoformat(),
                "input_character_count": len(prompt),
                "input_item_count": len(evidence),
                "retry_reason": "missing_sections" if repair_missing_sections else None,
                "debug_flags_active": {
                    key: bool(str(env_values.get(key) or "").strip())
                    for key in _LIVE_TRACE_DEBUG_ENV_KEYS
                },
                "prompt_contract": _LIVE_TRACE_PROMPT_CONTRACT,
                "template_path": str(template_path),
                "response_schema_name": _LIVE_TRACE_SCHEMA_NAME,
            }
        )
    response_payload: dict[str, Any]
    response_id: str | None = None
    provider_status = "accepted"
    error_message = ""
    try:
        response_payload = client.generate_json(
            instructions=(
                "Generate one FitCV structured CV document. "
                "Follow the prompt exactly, obey the rendering reference template, "
                "and return only JSON matching the schema."
            ),
            input_text=prompt,
            schema_name=_LIVE_TRACE_SCHEMA_NAME,
            schema=_canonical_live_structured_cv_response_schema(config=config),
        )
        if isinstance(response_payload, dict):
            response_id = str(response_payload.get("response_id") or response_payload.get("id") or "").strip() or None
    except Exception as exc:
        provider_status = "error"
        error_message = str(exc)
        finished_at = datetime.datetime.now(datetime.timezone.utc)
        if trace_attempt is not None:
            trace_attempt.update(
                {
                    "finished_at": finished_at.isoformat(),
                    "latency_ms": int((time.monotonic() - started_monotonic) * 1000),
                    "provider_status": provider_status,
                    "accepted_output_present": False,
                    "error_stage": "agentic_live_provider",
                    "error_message": error_message,
                }
            )
        raise
    finished_at = datetime.datetime.now(datetime.timezone.utc)
    if trace_attempt is not None:
        trace_attempt.update(
            {
                "finished_at": finished_at.isoformat(),
                "latency_ms": int((time.monotonic() - started_monotonic) * 1000),
                "provider_status": provider_status,
                "accepted_output_present": True,
                "response_id": response_id,
            }
        )
    structured_cv = _normalize_structured_cv(
        response_payload,
        jd=job,
        profile=profile,
        config=config,
        fit_classification=fit_classification,
    )
    markdown = render_cv_markdown(structured_cv, config)
    return {
        "structured_cv": structured_cv,
        "markdown": markdown,
    }


def _build_live_trace_runtime_provenance(
    runtime_provenance: dict[str, Any],
    *,
    template_path: str | None,
) -> dict[str, Any]:
    payload = dict(runtime_provenance)
    payload["prompt_contract"] = _LIVE_TRACE_PROMPT_CONTRACT
    payload["template_path"] = str(template_path or "")
    payload["response_schema_name"] = _LIVE_TRACE_SCHEMA_NAME
    return payload


def _empty_agentic_live_trace(
    runtime_provenance: dict[str, Any],
    *,
    template_path: str | None,
) -> dict[str, Any]:
    return {
        "trace_schema_version": _LIVE_TRACE_SCHEMA_VERSION,
        "trace_family": _LIVE_TRACE_FAMILY,
        "step_id": _LIVE_TRACE_STEP_ID,
        "trace_status": "completed",
        "runtime_provenance": _build_live_trace_runtime_provenance(
            runtime_provenance,
            template_path=template_path,
        ),
        "attempts": [],
        "input_summary": {
            "attempt_count": 0,
            "input_item_count": 0,
        },
        "output_summary": {
            "accepted_output_present": False,
            "final_status": "",
        },
        "validation_summary": {
            "initial_valid": False,
            "final_valid": False,
            "initial_missing_fields": [],
            "final_missing_fields": [],
            "violation_count": 0,
            "warning_count": 0,
        },
        "repair_summary": {
            "repair_attempted": False,
            "repair_attempt_count": 0,
            "repair_targets": [],
        },
        "error_summary": None,
    }


def _error_code_from_message(message: str) -> str | None:
    normalized = str(message or "")
    for token in normalized.replace(":", " ").split():
        if token.isdigit():
            return token
    return None


def _update_live_trace_validation_cycle(
    trace_payload: dict[str, Any],
    *,
    validation_initial: ValidationSnapshot | None,
    validation_final: dict[str, Any] | None,
) -> None:
    if not isinstance(trace_payload.get("validation_summary"), dict):
        return
    validation_summary = dict(trace_payload["validation_summary"])
    if validation_initial is None:
        validation_summary["initial_valid"] = False
        validation_summary["initial_missing_fields"] = []
    else:
        validation_summary["initial_valid"] = bool(validation_initial["valid"])
        validation_summary["initial_missing_fields"] = list(validation_initial["missing_sections"])
    if isinstance(validation_final, dict):
        validation_summary["final_valid"] = bool(validation_final.get("valid"))
        validation_summary["final_missing_fields"] = list(validation_final.get("missing_sections") or [])
        validation_summary["violation_count"] = (
            len(list(validation_final.get("grounding_violations") or []))
            + len(list(validation_final.get("skill_violations") or []))
        )
        validation_summary["warning_count"] = len(list(validation_final.get("warnings") or []))
    trace_payload["validation_summary"] = validation_summary


def _coerce_fit_classification(value: Any) -> FitClassification | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"strong", "stretch", "skip"}:
        return cast(FitClassification, normalized)
    return None


def _coerce_passthrough_status(value: Any) -> GenerationStatus:
    normalized = str(value or "").strip()
    if normalized in {
        BLOCKED_BY_RERANKER_STATUS,
        SKIPPED_FIT_GATE_STATUS,
        "analysis_failed",
    }:
        return cast(GenerationStatus, normalized)
    return GENERATION_FAILED_STATUS


def _coerce_error_payload(value: Any) -> ErrorPayload | None:
    if not isinstance(value, dict):
        return None
    stage = str(value.get("stage") or "").strip()
    message = str(value.get("message") or "").strip()
    if not stage or not message:
        return None
    return {
        "stage": stage,
        "message": message,
    }




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
    if not resolved_candidate_profile_name(profile):
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
    return is_candidate_name_placeholder(header.get("name"))


def _should_retry_missing_sections(validation: dict[str, Any]) -> bool:
    missing_sections = list(validation.get("missing_sections") or [])
    if not missing_sections:
        return False
    return all(not validation.get(field) for field in _REPAIRABLE_VALIDATION_FIELDS)

def _shallow_section_repair_targets(structured_cv: dict[str, Any] | None) -> list[str]:
    if not isinstance(structured_cv, dict):
        return []
    sections = structured_cv.get("sections")
    if not isinstance(sections, dict):
        return []
    targets: list[str] = []
    experience_rows = list(sections.get("experience") or [])
    if experience_rows and any(
        isinstance(item, dict)
        and not [str(b).strip() for b in list(item.get("bullets") or []) if str(b).strip()]
        for item in experience_rows
    ):
        targets.append("experience")
    project_rows = list(sections.get("projects") or [])
    if project_rows and any(
        isinstance(item, dict)
        and str(item.get("context") or "").strip()
        and not [str(b).strip() for b in list(item.get("bullets") or []) if str(b).strip()]
        for item in project_rows
    ):
        targets.append("projects")
    return targets


def _build_validation_grounding_payload(
    analysis_record: dict[str, Any],
    job: dict[str, Any],
    evidence_payload: list[dict[str, Any]],
    evidence_used: list[dict[str, Any]],
) -> AnalysisGroundingPayload:
    return {
        "evidence_payload": list(evidence_payload),
        "evidence_used": list(evidence_used),
        "evidence_selection_summary": dict(analysis_record.get("evidence_selection_summary") or {}),
        "analysis_input_summary": build_analysis_input_summary(job),
    }


def _build_validation_snapshot(validation: dict[str, Any] | None) -> ValidationSnapshot | None:
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
        "markdown_quality_blocking_issues": list(validation.get("markdown_quality_blocking_issues") or []),
        "markdown_quality_review_flags": list(validation.get("markdown_quality_review_flags") or []),
    }

def _run_generation_validations(
    markdown: str,
    *,
    profile: dict[str, Any],
    config: dict[str, Any],
    structured_cv: dict[str, Any] | None,
    analysis_grounding: AnalysisGroundingPayload,
) -> dict[str, Any]:
    return run_all_validations(
        markdown,
        profile=profile,
        config=config,
        structured_cv=structured_cv,
        analysis_grounding=analysis_grounding,
    )

def _determine_repair_targets(validation: dict[str, Any], structured_cv: dict[str, Any] | None) -> list[str]:
    repair_targets: list[str] = []
    if not validation["valid"] and _should_retry_missing_sections(validation):
        repair_targets = list(validation.get("missing_sections") or [])
    if repair_targets:
        return repair_targets
    return _shallow_section_repair_targets(structured_cv)

def _normalize_missing_section_keys(missing_sections: list[str] | None) -> list[str]:
    keys: list[str] = []
    for raw in list(missing_sections or []):
        value = str(raw).strip().lower()
        if not value:
            continue
        if value in {"skills", "experience", "projects", "education", "languages", "certifications"}:
            keys.append(value)
    return list(dict.fromkeys(keys))

def _backfill_required_sections_from_profile(
    *,
    structured_cv: dict[str, Any] | None,
    profile: dict[str, Any],
    missing_sections: list[str] | None,
) -> tuple[dict[str, Any] | None, list[str]]:
    if not isinstance(structured_cv, dict):
        return structured_cv, []
    repair_keys = _normalize_missing_section_keys(missing_sections)
    if not repair_keys:
        return structured_cv, []

    repaired = deepcopy(structured_cv)
    sections = repaired.setdefault("sections", {})
    if not isinstance(sections, dict):
        return structured_cv, []

    repaired_keys: list[str] = []
    if "skills" in repair_keys:
        profile_skills: list[str] = []
        for item in list(profile.get("skills") or []):
            if isinstance(item, dict):
                value = str(item.get("name") or "").strip()
            else:
                value = str(item).strip()
            if value:
                profile_skills.append(value)
        unique_skills = list(dict.fromkeys(profile_skills))[:12]
        if unique_skills:
            sections["skills"] = {"groups": [{"label": "Core Skills", "items": unique_skills}]}
            repaired_keys.append("skills")

    if "experience" in repair_keys:
        existing_experience = list(sections.get("experience") or [])
        if not existing_experience:
            fallback_experience: list[dict[str, Any]] = []
            for exp in list(profile.get("experiences") or [])[:3]:
                if not isinstance(exp, dict):
                    continue
                bullet_texts = [
                    (
                        str(item.get("text") or "").strip()
                        if isinstance(item, dict)
                        else str(item).strip()
                    )
                    for item in list(exp.get("bullets") or [])
                    if (
                        str(item.get("text") or "").strip()
                        if isinstance(item, dict)
                        else str(item).strip()
                    )
                ]
                fallback_experience.append(
                    {
                        "role": str(exp.get("role") or "").strip(),
                        "company": str(exp.get("company") or "").strip(),
                        "start": exp.get("start"),
                        "end": exp.get("end"),
                        "location": str(exp.get("location") or "").strip() or None,
                        "bullets": bullet_texts[:2] or ["Delivered cross-functional work aligned with business goals."],
                    }
                )
            if fallback_experience:
                sections["experience"] = fallback_experience
                repaired_keys.append("experience")

    if "projects" in repair_keys:
        existing_projects = list(sections.get("projects") or [])
        if not existing_projects:
            fallback_projects: list[dict[str, Any]] = []
            for project in list(profile.get("projects") or [])[:3]:
                if not isinstance(project, dict):
                    continue
                bullets = [
                    str(item).strip()
                    for item in list(project.get("highlights") or project.get("bullets") or [])
                    if str(item).strip()
                ]
                fallback_projects.append(
                    {
                        "name": str(project.get("name") or "").strip(),
                        "context": str(project.get("context") or project.get("period") or "").strip() or None,
                        "bullets": bullets[:2] or ["Built project outcome with measurable business impact."],
                    }
                )
            if fallback_projects:
                sections["projects"] = fallback_projects
                repaired_keys.append("projects")

    if "education" in repair_keys:
        existing_education = list(sections.get("education") or [])
        if not existing_education:
            fallback_education = []
            for edu in list(profile.get("education") or [])[:2]:
                if not isinstance(edu, dict):
                    continue
                fallback_education.append(
                    {
                        "degree": str(edu.get("degree") or "").strip(),
                        "institution": str(edu.get("institution") or "").strip(),
                        "field": str(edu.get("field") or "").strip() or None,
                        "start": edu.get("start"),
                        "end": edu.get("end"),
                    }
                )
            if fallback_education:
                sections["education"] = fallback_education
                repaired_keys.append("education")

    if "languages" in repair_keys:
        existing_languages = list(sections.get("languages") or [])
        if not existing_languages:
            fallback_languages = []
            for lang in list(profile.get("languages") or [])[:5]:
                if isinstance(lang, dict):
                    name = str(lang.get("name") or "").strip()
                    level = str(lang.get("level") or "").strip() or None
                else:
                    name = str(lang).strip()
                    level = None
                if not name:
                    continue
                fallback_languages.append({"name": name, "level": level})
            if fallback_languages:
                sections["languages"] = fallback_languages
                repaired_keys.append("languages")

    return repaired, repaired_keys

def _run_repair_cycle(
    *,
    structured_cv: dict[str, Any] | None,
    markdown: str,
    validation: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
    analysis_grounding: AnalysisGroundingPayload,
    retry_executor: Callable[[list[str]], tuple[dict[str, Any] | None, str, dict[str, Any]]],
) -> tuple[dict[str, Any] | None, str, dict[str, Any], RepairAttempt]:
    repair_attempt = _empty_repair_attempt()
    if not validation["valid"] and _should_repair_candidate_name_placeholder(validation, structured_cv, profile):
        repair_attempt = _build_candidate_name_repair_attempt()
        structured_cv, markdown = _repair_candidate_name_placeholder(structured_cv or {}, profile, config)
        validation = _run_generation_validations(
            markdown,
            profile=profile,
            config=config,
            structured_cv=structured_cv,
            analysis_grounding=analysis_grounding,
        )

    repair_targets = _determine_repair_targets(validation, structured_cv)
    if repair_targets:
        repair_attempt = _build_repair_attempt(repair_targets)
        structured_cv, markdown, validation = retry_executor(repair_targets)

    if not validation.get("valid"):
        structured_cv, repaired_keys = _backfill_required_sections_from_profile(
            structured_cv=structured_cv,
            profile=profile,
            missing_sections=list(validation.get("missing_sections") or []),
        )
        if repaired_keys:
            markdown = render_cv_markdown(structured_cv or {}, config)
            validation = _run_generation_validations(
                markdown,
                profile=profile,
                config=config,
                structured_cv=structured_cv,
                analysis_grounding=analysis_grounding,
            )
            if validation.get("valid"):
                repair_attempt = {
                    "performed": True,
                    "missing_sections": repaired_keys,
                    "reason": "deterministic_section_backfill",
                }

    return structured_cv, markdown, validation, repair_attempt

def _execute_generation_attempt(
    generator: Callable[[list[str] | None], Any],
    *,
    profile: dict[str, Any],
    config: dict[str, Any],
    analysis_grounding: AnalysisGroundingPayload,
    repair_missing_sections: list[str] | None = None,
) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
    generated_cv = generator(repair_missing_sections)
    structured_cv, markdown = _unwrap_generated_cv(generated_cv)
    validation = _run_generation_validations(
        markdown,
        profile=profile,
        config=config,
        structured_cv=structured_cv,
        analysis_grounding=analysis_grounding,
    )
    return structured_cv, markdown, validation

def _cv_generation_sleep_secs(config: dict[str, Any]) -> float:
    stage_runtime = dict(config.get("stage_runtime") or {})
    generation_runtime = dict(stage_runtime.get("cv_generation") or {})
    return float(generation_runtime.get("sleep_secs", 0.0))

def _build_live_provider_generator(
    *,
    job: dict[str, Any],
    evidence_payload: list[dict[str, Any]],
    gap_summary: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
    fit: str,
    evidence_selection_summary: dict[str, Any],
    env_values: dict[str, str],
) -> Callable[[list[str] | None, dict[str, Any], int], Any]:
    def _call(
        repair_missing_sections: list[str] | None,
        trace_attempt: dict[str, Any],
        attempt_index: int,
    ) -> Any:
        return _generate_cv_with_live_provider(
            job=job,
            evidence=evidence_payload,
            gap=gap_summary,
            profile=profile,
            config=config,
            fit_classification=fit,
            evidence_selection_summary=evidence_selection_summary,
            repair_missing_sections=repair_missing_sections,
            env_values=env_values,
            trace_attempt=trace_attempt,
            attempt_index=attempt_index,
        )

    return _call

def _build_fallback_provider_generator(
    *,
    job: dict[str, Any],
    evidence_payload: list[dict[str, Any]],
    gap_summary: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
    fit: str,
    evidence_selection_summary: dict[str, Any],
) -> Callable[[list[str] | None], Any]:
    def _call(repair_missing_sections: list[str] | None) -> Any:
        return generate_cv(
            job,
            evidence_payload,
            gap_summary,
            profile,
            config,
            fit_classification=fit,
            evidence_selection_summary=evidence_selection_summary,
            repair_missing_sections=repair_missing_sections,
        )

    return _call

def _build_fallback_retry_executor(
    *,
    fallback_provider_generator: Callable[[list[str] | None], Any],
    profile: dict[str, Any],
    config: dict[str, Any],
    analysis_grounding: AnalysisGroundingPayload,
) -> Callable[[list[str]], tuple[dict[str, Any] | None, str, dict[str, Any]]]:
    def _retry(repair_targets: list[str]) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
        sleep_secs = _cv_generation_sleep_secs(config)
        if sleep_secs > 0.0:
            time.sleep(sleep_secs)
        return _execute_generation_attempt(
            fallback_provider_generator,
            profile=profile,
            config=config,
            analysis_grounding=analysis_grounding,
            repair_missing_sections=repair_targets,
        )

    return _retry


def _build_repair_attempt(missing_sections: list[str] | None = None) -> RepairAttempt:
    return {
        "performed": bool(missing_sections),
        "missing_sections": list(missing_sections or []),
    }


def _build_candidate_name_repair_attempt() -> RepairAttempt:
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
    repaired_structured_cv = deepcopy(structured_cv)
    sections = repaired_structured_cv.setdefault("sections", {})
    header = sections.setdefault("header", {})
    header["name"] = resolved_candidate_profile_name(profile)
    repaired_markdown = render_cv_markdown(repaired_structured_cv, config)
    return repaired_structured_cv, repaired_markdown


def _unwrap_generated_cv(generated_cv: Any) -> tuple[dict[str, Any] | None, str]:
    if isinstance(generated_cv, dict):
        markdown = str(generated_cv.get("markdown") or "")
        structured_cv = generated_cv.get("structured_cv")
        if isinstance(structured_cv, dict):
            return structured_cv, markdown
        return None, markdown
    return None, str(generated_cv)


def _build_result(
    *,
    analysis_record: dict[str, Any],
    job: dict[str, Any],
    status: GenerationStatus,
    fit_classification: FitClassification | None,
    structured_cv_initial: dict[str, Any] | None,
    validation_initial: ValidationSnapshot | None,
    repair_attempt: RepairAttempt,
    structured_cv_final: dict[str, Any] | None,
    markdown_final: str | None,
    validation: dict[str, Any] | None,
    error: ErrorPayload | None,
    runtime_provenance: dict[str, Any] | None = None,
    agentic_live_trace: dict[str, Any] | None = None,
) -> CvGenerationResult:
    evidence_payload = list(analysis_record.get("evidence_payload") or [])
    evidence_used = list(analysis_record.get("evidence_used") or [])
    if not evidence_used and evidence_payload:
        evidence_used = build_evidence_used(evidence_payload)

    cv_analysis_status = str(analysis_record.get("status") or "")
    cv_status: str = status
    if status in {ACCEPTED_STATUS, VALIDATION_FAILED_STATUS, GENERATION_FAILED_STATUS}:
        cv_analysis_status = READY_FOR_GENERATION_STATUS
    if status == BLOCKED_BY_RERANKER_STATUS:
        cv_status = "not_attempted"

    result: CvGenerationResult = {
        "job_url": extract_job_url(job),
        "job_title": extract_job_title(job),
        "status": status,
        "ranking_fit_label": str(fit_classification or "").strip() or None,
        "fit_classification": fit_classification,
        "decision_chain": build_decision_chain(
            job=job,
            fit_classification=fit_classification,
            cv_analysis_status=cv_analysis_status,
            cv_status=cv_status,
        ),
        "analysis_input_summary": build_analysis_input_summary(job),
        "evidence_used": evidence_used,
        "evidence_selection_summary": dict(analysis_record.get("evidence_selection_summary") or {}),
        "gap_summary": analysis_record.get("gap_summary"),
        "structured_cv_initial": structured_cv_initial,
        "validation_initial": validation_initial,
        "repair_attempt": repair_attempt,
        "structured_cv_final": structured_cv_final,
        "markdown_final": markdown_final,
        "validation": validation,
        "outcome_reason": error if status in {SKIPPED_FIT_GATE_STATUS, BLOCKED_BY_RERANKER_STATUS} else None,
        "error": error if status not in {SKIPPED_FIT_GATE_STATUS, BLOCKED_BY_RERANKER_STATUS} else None,
    }
    if runtime_provenance:
        result["runtime_provenance"] = dict(runtime_provenance)
    if agentic_live_trace:
        result["agentic_live_trace"] = dict(agentic_live_trace)
    return result

def _build_generation_result_payload(
    *,
    analysis_record: dict[str, Any],
    job: dict[str, Any],
    fit_classification: FitClassification | None,
    structured_cv_initial: dict[str, Any] | None,
    validation_initial: ValidationSnapshot | None,
    repair_attempt: RepairAttempt,
    status: GenerationStatus,
    structured_cv_final: dict[str, Any] | None,
    markdown_final: str | None,
    validation: dict[str, Any] | None,
    error: ErrorPayload | None,
    runtime_provenance: dict[str, Any] | None = None,
    agentic_live_trace: dict[str, Any] | None = None,
) -> CvGenerationResult:
    return _build_result(
        analysis_record=analysis_record,
        job=job,
        status=status,
        fit_classification=fit_classification,
        structured_cv_initial=structured_cv_initial,
        validation_initial=validation_initial,
        repair_attempt=repair_attempt,
        structured_cv_final=structured_cv_final,
        markdown_final=markdown_final,
        validation=validation,
        error=error,
        runtime_provenance=runtime_provenance,
        agentic_live_trace=agentic_live_trace,
    )


def generate_from_analysis(
    analysis_record: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
) -> CvGenerationResult:
    job = dict(analysis_record.get("job_snapshot") or {})
    if not job:
        job = {
            "job_url": str(analysis_record.get("job_url") or ""),
            "job_title": str(analysis_record.get("job_title") or ""),
            "title": str(analysis_record.get("job_title") or ""),
        }

    status = str(analysis_record.get("status") or "")
    fit_classification = _coerce_fit_classification(analysis_record.get("fit_classification"))
    if status != READY_FOR_GENERATION_STATUS:
        passthrough_error = analysis_record.get("outcome_reason") or analysis_record.get("error")
        return _build_generation_result_payload(
            analysis_record=analysis_record,
            job=job,
            status=_coerce_passthrough_status(status),
            fit_classification=fit_classification,
            structured_cv_initial=None,
            validation_initial=None,
            repair_attempt=_empty_repair_attempt(),
            structured_cv_final=None,
            markdown_final=None,
            validation=None,
            error=_coerce_error_payload(passthrough_error),
            runtime_provenance=None,
        )

    live_runtime_provenance = _live_runtime_provenance_or_none()
    if live_runtime_provenance is not None:
        evidence_payload = list(analysis_record.get("evidence_payload") or [])
        evidence_used = list(analysis_record.get("evidence_used") or [])
        if not evidence_used and evidence_payload:
            evidence_used = build_evidence_used(evidence_payload)
        analysis_grounding = _build_validation_grounding_payload(
            analysis_record,
            job,
            evidence_payload,
            evidence_used,
        )
        gap_summary = _augmented_gap_summary_from_analysis(analysis_record)
        fit = str(fit_classification or "skip")
        structured_cv_initial: dict[str, Any] | None = None
        validation_initial: ValidationSnapshot | None = None
        repair_attempt = _empty_repair_attempt()
        env_values = _build_fitcv_langgraph_env_values(_discover_fitcv_langgraph_repo_root())
        trace_payload = _empty_agentic_live_trace(
            live_runtime_provenance,
            template_path=str(_resolve_template_path(config)),
        )
        first_attempt_trace: dict[str, Any] = {}
        try:
            live_provider_generator = _build_live_provider_generator(
                job=job,
                evidence_payload=evidence_payload,
                gap_summary=gap_summary,
                profile=profile,
                config=config,
                fit=fit,
                evidence_selection_summary=dict(analysis_record.get("evidence_selection_summary") or {}),
                env_values=env_values,
            )
            first_attempt_trace.setdefault("attempt_index", 1)
            first_attempt_trace.setdefault("attempt_type", "initial_generation")
            first_attempt_trace.setdefault("retry_reason", None)
            trace_payload["attempts"].append(first_attempt_trace)
            structured_cv, markdown, validation = _execute_generation_attempt(
                lambda repair_missing_sections: live_provider_generator(
                    repair_missing_sections,
                    first_attempt_trace,
                    1,
                ),
                profile=profile,
                config=config,
                analysis_grounding=analysis_grounding,
                repair_missing_sections=None,
            )
            first_attempt_trace.setdefault("provider_status", "accepted")
            first_attempt_trace.setdefault("accepted_output_present", True)
            structured_cv_initial = structured_cv
            validation_initial = _build_validation_snapshot(validation)

            def _live_retry_executor(
                repair_targets: list[str],
            ) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
                sleep_secs = _cv_generation_sleep_secs(config)
                if sleep_secs > 0.0:
                    time.sleep(sleep_secs)
                second_attempt_trace: dict[str, Any] = {}
                second_attempt_trace.setdefault("attempt_index", 2)
                second_attempt_trace.setdefault("provider_status", "accepted")
                second_attempt_trace.setdefault("attempt_type", "repair_retry")
                second_attempt_trace.setdefault("accepted_output_present", True)
                second_attempt_trace.setdefault("retry_reason", "missing_or_shallow_sections")
                trace_payload["attempts"].append(second_attempt_trace)
                return _execute_generation_attempt(
                    lambda repair_missing_sections: live_provider_generator(
                        repair_missing_sections,
                        second_attempt_trace,
                        2,
                    ),
                    profile=profile,
                    config=config,
                    analysis_grounding=analysis_grounding,
                    repair_missing_sections=repair_targets,
                )
            structured_cv, markdown, validation, repair_attempt = _run_repair_cycle(
                structured_cv=structured_cv,
                markdown=markdown,
                validation=validation,
                profile=profile,
                config=config,
                analysis_grounding=analysis_grounding,
                retry_executor=_live_retry_executor,
            )
            _update_live_trace_validation_cycle(
                trace_payload,
                validation_initial=validation_initial,
                validation_final=validation,
            )
            trace_payload["input_summary"] = {
                "attempt_count": len(list(trace_payload.get("attempts") or [])),
                "input_item_count": len(evidence_payload),
            }
            trace_payload["repair_summary"] = {
                "repair_attempted": bool(repair_attempt.get("performed")),
                "repair_attempt_count": len(list(trace_payload.get("attempts") or [])) - 1,
                "repair_targets": list(repair_attempt.get("missing_sections") or []),
                "repair_reason": str(repair_attempt.get("reason") or ""),
            }

            if not validation["valid"]:
                try:
                    fallback_provider_generator = _build_fallback_provider_generator(
                        job=job,
                        evidence_payload=evidence_payload,
                        gap_summary=gap_summary,
                        profile=profile,
                        config=config,
                        fit=fit,
                        evidence_selection_summary=dict(analysis_record.get("evidence_selection_summary") or {}),
                    )
                    fallback_structured_cv, fallback_markdown, fallback_validation = _execute_generation_attempt(
                        fallback_provider_generator,
                        profile=profile,
                        config=config,
                        analysis_grounding=analysis_grounding,
                        repair_missing_sections=None,
                    )
                    if not fallback_validation.get("valid"):
                        fallback_structured_cv, fallback_markdown, fallback_validation, fallback_repair_attempt = _run_repair_cycle(
                            structured_cv=fallback_structured_cv,
                            markdown=fallback_markdown,
                            validation=fallback_validation,
                            profile=profile,
                            config=config,
                            analysis_grounding=analysis_grounding,
                            retry_executor=_build_fallback_retry_executor(
                                fallback_provider_generator=fallback_provider_generator,
                                profile=profile,
                                config=config,
                                analysis_grounding=analysis_grounding,
                            ),
                        )
                        if fallback_repair_attempt.get("performed"):
                            repair_attempt = dict(fallback_repair_attempt)
                    if fallback_validation.get("valid"):
                        trace_payload["output_summary"] = {
                            "accepted_output_present": True,
                            "final_status": ACCEPTED_STATUS,
                            "fallback_provider_used_after_live_validation_failure": True,
                        }
                        trace_payload["error_summary"] = None
                        runtime_with_fallback = dict(live_runtime_provenance)
                        runtime_with_fallback["fallback_provider_used_after_live_validation_failure"] = True
                        return _build_generation_result_payload(
                            analysis_record=analysis_record,
                            job=job,
                            status=ACCEPTED_STATUS,
                            fit_classification=fit_classification,
                            structured_cv_initial=structured_cv_initial,
                            validation_initial=validation_initial,
                            repair_attempt=repair_attempt,
                            structured_cv_final=fallback_structured_cv,
                            markdown_final=fallback_markdown,
                            validation=fallback_validation,
                            error=None,
                            runtime_provenance=runtime_with_fallback,
                            agentic_live_trace=trace_payload,
                        )
                except Exception:
                    pass
                trace_payload["output_summary"] = {
                    "accepted_output_present": False,
                    "final_status": VALIDATION_FAILED_STATUS,
                }
                trace_payload["error_summary"] = {
                    "error_stage": "validation",
                    "error_message": f"Live provider CV validation failed for {extract_job_url(job)}",
                }
                return _build_generation_result_payload(
                    analysis_record=analysis_record,
                    job=job,
                    status=VALIDATION_FAILED_STATUS,
                    fit_classification=fit_classification,
                    structured_cv_initial=structured_cv_initial,
                    validation_initial=validation_initial,
                    repair_attempt=repair_attempt,
                    structured_cv_final=None,
                    markdown_final=None,
                    validation=validation,
                    error={
                        "stage": "validation",
                        "message": f"Live provider CV validation failed for {extract_job_url(job)}",
                    },
                    runtime_provenance=live_runtime_provenance,
                    agentic_live_trace=trace_payload,
                )

            trace_payload["output_summary"] = {
                "accepted_output_present": True,
                "final_status": ACCEPTED_STATUS,
            }
            trace_payload["error_summary"] = None
            return _build_generation_result_payload(
                analysis_record=analysis_record,
                job=job,
                status=ACCEPTED_STATUS,
                fit_classification=fit_classification,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                repair_attempt=repair_attempt,
                structured_cv_final=structured_cv,
                markdown_final=markdown,
                validation=validation,
                error=None,
                runtime_provenance=live_runtime_provenance,
                agentic_live_trace=trace_payload,
            )
        except Exception as exc:
            if not trace_payload["attempts"]:
                trace_payload["attempts"].append(first_attempt_trace)
            trace_payload["trace_status"] = "degraded"
            trace_payload["input_summary"] = {
                "attempt_count": len(list(trace_payload.get("attempts") or [])),
                "input_item_count": len(evidence_payload),
            }
            trace_payload["repair_summary"] = {
                "repair_attempted": bool(repair_attempt.get("performed")),
                "repair_attempt_count": len(list(trace_payload.get("attempts") or [])) - 1,
                "repair_targets": list(repair_attempt.get("missing_sections") or []),
                "repair_reason": str(repair_attempt.get("reason") or ""),
            }
            _update_live_trace_validation_cycle(
                trace_payload,
                validation_initial=validation_initial,
                validation_final=None,
            )
            latest_attempt = (
                trace_payload["attempts"][-1]
                if isinstance(trace_payload.get("attempts"), list) and trace_payload["attempts"]
                else None
            )
            if isinstance(latest_attempt, dict):
                latest_attempt.setdefault("provider_status", "error")
                latest_attempt.setdefault("accepted_output_present", False)
                latest_attempt.setdefault("error_stage", "agentic_live_provider")
                latest_attempt.setdefault("error_message", str(exc))
                latest_attempt.setdefault("error_code", _error_code_from_message(str(exc)))
            trace_payload["output_summary"] = {
                "accepted_output_present": False,
                "final_status": GENERATION_FAILED_STATUS,
            }
            trace_payload["error_summary"] = {
                "error_stage": "agentic_live_provider",
                "error_code": _error_code_from_message(str(exc)),
                "error_message": str(exc),
            }
            return _build_generation_result_payload(
                analysis_record=analysis_record,
                job=job,
                status=GENERATION_FAILED_STATUS,
                fit_classification=fit_classification,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                repair_attempt=repair_attempt,
                structured_cv_final=None,
                markdown_final=None,
                validation=None,
                error={
                    "stage": "agentic_live_provider",
                    "message": str(exc),
                },
                runtime_provenance=live_runtime_provenance,
                agentic_live_trace=trace_payload,
            )

    evidence_payload = list(analysis_record.get("evidence_payload") or [])
    evidence_used = list(analysis_record.get("evidence_used") or [])
    if not evidence_used and evidence_payload:
        evidence_used = build_evidence_used(evidence_payload)
    analysis_grounding = _build_validation_grounding_payload(
        analysis_record,
        job,
        evidence_payload,
        evidence_used,
    )
    gap_summary = _augmented_gap_summary_from_analysis(analysis_record)
    fit = str(fit_classification or "skip")
    fallback_runtime_provenance = resolve_cv_generation_runtime_provenance(
        config,
        default_model=get_cv_generation_model(config),
    )
    structured_cv_initial = None
    validation_initial = None
    repair_attempt = _empty_repair_attempt()

    try:
        fallback_provider_generator = _build_fallback_provider_generator(
            job=job,
            evidence_payload=evidence_payload,
            gap_summary=gap_summary,
            profile=profile,
            config=config,
            fit=fit,
            evidence_selection_summary=dict(analysis_record.get("evidence_selection_summary") or {}),
        )

        structured_cv, markdown, validation = _execute_generation_attempt(
            fallback_provider_generator,
            profile=profile,
            config=config,
            analysis_grounding=analysis_grounding,
            repair_missing_sections=None,
        )
        structured_cv_initial = structured_cv
        validation_initial = _build_validation_snapshot(validation)
        structured_cv, markdown, validation, repair_attempt = _run_repair_cycle(
            structured_cv=structured_cv,
            markdown=markdown,
            validation=validation,
            profile=profile,
            config=config,
            analysis_grounding=analysis_grounding,
            retry_executor=_build_fallback_retry_executor(
                fallback_provider_generator=fallback_provider_generator,
                profile=profile,
                config=config,
                analysis_grounding=analysis_grounding,
            ),
        )

        if not validation["valid"]:
            return _build_generation_result_payload(
                analysis_record=analysis_record,
                job=job,
                status=VALIDATION_FAILED_STATUS,
                fit_classification=fit_classification,
                structured_cv_initial=structured_cv_initial,
                validation_initial=validation_initial,
                repair_attempt=repair_attempt,
                structured_cv_final=None,
                markdown_final=None,
                validation=validation,
                error={
                    "stage": "validation",
                    "message": f"CV validation failed for {extract_job_url(job)}",
                },
                runtime_provenance=fallback_runtime_provenance,
            )

        return _build_generation_result_payload(
            analysis_record=analysis_record,
            job=job,
            status=ACCEPTED_STATUS,
            fit_classification=fit_classification,
            structured_cv_initial=structured_cv_initial,
            validation_initial=validation_initial,
            repair_attempt=repair_attempt,
            structured_cv_final=structured_cv,
            markdown_final=markdown,
            validation=validation,
            error=None,
            runtime_provenance=fallback_runtime_provenance,
        )
    except Exception as exc:
        return _build_generation_result_payload(
            analysis_record=analysis_record,
            job=job,
            status=GENERATION_FAILED_STATUS,
            fit_classification=fit_classification,
            structured_cv_initial=structured_cv_initial,
            validation_initial=validation_initial,
            repair_attempt=repair_attempt,
            structured_cv_final=None,
            markdown_final=None,
            validation=None,
            error={
                "stage": "generation",
                "message": str(exc),
            },
            runtime_provenance=fallback_runtime_provenance,
        )






