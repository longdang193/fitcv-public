"""@meta
name: retry_settings
type: module
domain: run_orchestration
ownership: infrastructure
responsibility:
  - Provide SSOT-first retry settings shared by web/worker/reconciler.
inputs:
  - fitcv.config.load_control_plane_config() output
outputs:
  - Normalized retry settings with bounded defaults
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from fitcv.config import load_control_plane_config


@dataclass(frozen=True)
class RetrySettings:
    enabled: bool
    max_attempts: int
    backoff_seconds: tuple[int, ...]
    lease_seconds: int
    reconciler_interval_seconds: int
    error_details_max_chars: int


def _parse_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _parse_int_list(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        items: Iterable[Any] = value
    else:
        text = str(value or "").strip()
        if not text:
            return []
        items = [part.strip() for part in text.split(",")]
    parsed: list[int] = []
    for item in items:
        try:
            parsed.append(max(0, int(item)))
        except (TypeError, ValueError):
            continue
    return parsed


def load_retry_settings(control_plane_cfg: dict[str, Any] | None = None) -> RetrySettings:
    """Load retry policy from control-plane config.

    SSOT rule: runtime config is canonical; env toggles not used for retry policy.
    """

    cfg = control_plane_cfg or load_control_plane_config()
    fitcv_cp = dict(cfg.get("fitcv_cp") or {})
    retry = dict(fitcv_cp.get("retry") or {})

    enabled = _parse_bool(retry.get("enabled"), default=False)
    max_attempts = _parse_int(retry.get("max_attempts"), default=1, minimum=1, maximum=20)

    backoff = _parse_int_list(retry.get("backoff_seconds"))
    if not backoff:
        backoff = [1, 2, 4, 8]
    if len(backoff) > 20:
        backoff = backoff[:20]

    lease_seconds = _parse_int(retry.get("lease_seconds"), default=900, minimum=30, maximum=24 * 3600)
    reconciler_interval_seconds = _parse_int(
        retry.get("reconciler_interval_seconds"),
        default=0,
        minimum=0,
        maximum=3600,
    )
    error_details_max_chars = _parse_int(
        retry.get("error_details_max_chars"),
        default=2048,
        minimum=256,
        maximum=65536,
    )

    return RetrySettings(
        enabled=enabled,
        max_attempts=max_attempts,
        backoff_seconds=tuple(int(x) for x in backoff),
        lease_seconds=lease_seconds,
        reconciler_interval_seconds=reconciler_interval_seconds,
        error_details_max_chars=error_details_max_chars,
    )
