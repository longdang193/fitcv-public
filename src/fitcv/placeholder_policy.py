"""@meta
name: placeholder_policy
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared placeholder token normalization and checks for validator and generator symmetry.
inputs:
  - Raw structured-field values from CV pipeline surfaces
outputs:
  - Canonical placeholder normalization and membership checks
lifecycle:
  - status: active
"""

from typing import Any

_PLACEHOLDER_TOKENS = {
    "",
    "none",
    "null",
    "n/a",
    "na",
    "not specified",
    "not provided",
    "unknown",
}


def normalize_placeholder_token(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.replace("–", "-").replace("—", "-")
    return " ".join(normalized.split())


def is_placeholder_token(value: Any) -> bool:
    return normalize_placeholder_token(value) in _PLACEHOLDER_TOKENS
