"""@meta
name: runtime_contracts
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Shared runtime parsing and orchestration status normalization contracts.
inputs:
  - Environment variables and backend-native status values
outputs:
  - Canonical control-plane runtime contracts
lifecycle:
  - status: active
"""

from __future__ import annotations

TRUTHY_VALUES = {"1", "true", "yes", "on"}

ORCHESTRATION_STATUS_ALIASES: dict[str, str] = {
    "queued": "queued",
    "pending": "queued",
    "scheduled": "queued",
    "started": "started",
    "running": "started",
    "deferred": "deferred",
    "paused": "deferred",
    "finished": "finished",
    "completed": "finished",
    "complete": "finished",
    "failed": "failed",
    "crashed": "failed",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "cancelling": "cancelling",
    "missing": "missing",
    "missing_run": "missing",
    "unknown": "unknown",
}


def is_truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in TRUTHY_VALUES


def parse_bounded_float_env(
    value: str | None,
    *,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    try:
        parsed = float(str(value or "").strip())
    except ValueError:
        return default
    return max(minimum, min(parsed, maximum))


def normalize_orchestration_status(status: str | None) -> str:
    key = str(status or "").strip().lower()
    if not key:
        return "unknown"
    return ORCHESTRATION_STATUS_ALIASES.get(key, "unknown")
