"""@meta
name: candidate_name_policy
type: module
domain: runtime
ownership: feature
capabilities:
  - inspection_debugging.cv-generation-diagnostics
responsibility:
  - Provide SSOT helpers for candidate-name placeholder normalization and resolution.
inputs:
  - candidate profile dictionaries and candidate-name string values
outputs:
  - normalized placeholder checks and resolved candidate display name
lifecycle:
  - status: active
"""
from typing import Any

_CANDIDATE_NAME_PLACEHOLDER_VALUES = {
    "candidate name",
    "your name",
}


def normalize_candidate_name_token(value: str) -> str:
    normalized = str(value or "")
    normalized = normalized.replace("[", " ").replace("]", " ")
    normalized = " ".join(normalized.split()).strip().lower()
    return normalized


def is_candidate_name_placeholder(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return normalize_candidate_name_token(value) in _CANDIDATE_NAME_PLACEHOLDER_VALUES


def resolved_candidate_profile_name(profile: dict[str, Any] | None) -> str:
    if not isinstance(profile, dict):
        return ""
    candidate_name = str(profile.get("name") or "").strip()
    if not candidate_name or is_candidate_name_placeholder(candidate_name):
        return ""
    return candidate_name
