"""@meta
name: reuse
type: module
domain: pipeline
ownership: infrastructure
responsibility:
  - Centralize cross-stage reuse policy normalization and decision-envelope helpers.
inputs:
  - Runtime config reuse block
outputs:
  - Normalized stage policy objects and deterministic reuse decision envelopes
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_EXACT = "exact"
_EXACT_OR_CORE = "exact_or_core"
_SUCCEEDED_ONLY = "succeeded_only"
_SUCCEEDED_OR_CHECKPOINTED = "succeeded_or_checkpointed"

_STAGE_DEFAULTS: dict[str, dict[str, str]] = {
    "enrich": {"source_scope": _SUCCEEDED_OR_CHECKPOINTED, "match_mode": _EXACT},
    "ranking": {"source_scope": _SUCCEEDED_OR_CHECKPOINTED, "match_mode": _EXACT},
    "cv_analysis": {"source_scope": _SUCCEEDED_OR_CHECKPOINTED, "match_mode": _EXACT},
    "cv_generation": {"source_scope": _SUCCEEDED_OR_CHECKPOINTED, "match_mode": _EXACT},
    "synonym_triage": {"source_scope": _SUCCEEDED_OR_CHECKPOINTED, "match_mode": _EXACT_OR_CORE},
}


@dataclass(frozen=True)
class ReuseStagePolicy:
    stage: str
    enabled: bool
    source_scope: str
    match_mode: str


def _normalize_source_scope(value: Any, *, fallback: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {_SUCCEEDED_ONLY, _SUCCEEDED_OR_CHECKPOINTED}:
        return normalized
    return fallback


def _normalize_match_mode(value: Any, *, fallback: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {_EXACT, _EXACT_OR_CORE}:
        return normalized
    return fallback


def resolve_reuse_stage_policy(config: dict[str, Any], stage: str) -> ReuseStagePolicy:
    stage_key = str(stage or "").strip()
    stage_defaults = dict(_STAGE_DEFAULTS.get(stage_key) or {})
    fallback_source_scope = str(stage_defaults.get("source_scope") or _SUCCEEDED_OR_CHECKPOINTED)
    fallback_match_mode = str(stage_defaults.get("match_mode") or _EXACT)

    reuse_block = dict(config.get("reuse") or {})
    stage_block = dict(reuse_block.get(stage_key) or {})

    return ReuseStagePolicy(
        stage=stage_key,
        enabled=bool(stage_block.get("enabled", True)),
        source_scope=_normalize_source_scope(stage_block.get("source_scope"), fallback=fallback_source_scope),
        match_mode=_normalize_match_mode(stage_block.get("match_mode"), fallback=fallback_match_mode),
    )


def build_reuse_decision(
    *,
    decision: str,
    reason_code: str,
    fingerprint: str | None,
    source_run_id: str | None = None,
    source_artifact_type: str | None = None,
) -> dict[str, Any]:
    return {
        "decision": str(decision or "").strip(),
        "reason_code": str(reason_code or "").strip(),
        "fingerprint": str(fingerprint or "").strip() or None,
        "source_run_id": str(source_run_id or "").strip() or None,
        "source_artifact_type": str(source_artifact_type or "").strip() or None,
    }
