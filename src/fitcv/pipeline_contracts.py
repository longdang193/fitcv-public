"""@meta
name: pipeline_contracts
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Single-source-of-truth contracts for FitCV pipeline runtime.
inputs:
  - Used by src.fitcv.pipeline and control-plane helpers for stable taxonomies.
outputs:
  - Enums and helpers for pipeline invariants.
lifecycle:
  - status: active
"""

from __future__ import annotations

from enum import Enum


class ReviewRequiredReasonCode(str, Enum):
    """Canonical reason-code taxonomy for CV generation review-required outcomes."""

    PROVIDER_ERROR = "provider_error"
    PROVIDER_RESPONSE_UNUSABLE = "provider_response_unusable"
    TIMEOUT = "timeout"
    EMPTY_OUTPUT = "empty_output"
    TEMPLATE_CONTRACT_VIOLATION = "template_contract_violation"
    MARKDOWN_STRUCTURE_VIOLATION = "markdown_structure_violation"
    POST_VALIDATION_FAILED = "post_validation_failed"
    PERSISTENCE_FAILED = "persistence_failed"

    POLICY_REQUIRED_RATIO_FAIL = "policy_required_ratio_fail"
    POLICY_MISSING_REQUIRED_FAIL = "policy_missing_required_fail"
    POLICY_ACCEPTANCE_FAIL = "policy_acceptance_fail"

    UNSUPPORTED_REQUIREMENT_GAP = "unsupported_requirement_gap"
    LOW_CONFIDENCE_SECTIONS = "low_confidence_sections"
    QUALITY_GATE_FAILED = "quality_gate_failed"
    VALIDATION_GUARDRAIL_FAILED = "validation_guardrail_failed"
    EVIDENCE_COVERAGE_INSUFFICIENT = "evidence_coverage_insufficient"
    REVIEW_GATE_MANUAL_REQUIRED = "review_gate_manual_required"

    MANUAL_REVIEW_OTHER = "manual_review_other"


def is_review_required_reason_code(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        ReviewRequiredReasonCode(value)
    except ValueError:
        return False
    return True

