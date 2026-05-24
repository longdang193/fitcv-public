"""@meta
name: reuse_law_engine
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.exact-match-late-stage-reuse
responsibility:
  - Provide reusable policy-gate helpers for cross-stage reuse decisions.
inputs:
  - Stage semantic/runtime fingerprints and policy gate inputs
outputs:
  - Deterministic reuse identities, decisions, and provenance payloads
lifecycle:
  - status: active
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


def _sha256_json(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ReuseIdentity:
    stage: str
    scope_tier: str
    proposal_semantic_id: str
    runtime_invariance_id: str
    final_reuse_key: str


@dataclass(frozen=True)
class ReuseDecision:
    enabled: bool
    seed_available: bool
    runtime_match: bool
    semantic_match: bool
    soft_blocked: bool
    scope_tier: str
    seed_run_id: str | None
    seed_created_at: str | None


def build_identity(
    stage: str,
    semantic_payload: dict[str, Any] | None,
    runtime_payload: dict[str, Any] | None,
    scope_tier: str = "dataset",
) -> ReuseIdentity:
    semantic = dict(semantic_payload or {})
    runtime = dict(runtime_payload or {})
    scope = str(scope_tier or "dataset").strip().lower() or "dataset"
    if scope not in {"strict", "dataset", "global"}:
        scope = "dataset"
    proposal_semantic_id = _sha256_json(
        {
            "field": str(semantic.get("field") or ""),
            "alias": str(semantic.get("alias") or ""),
            "canonical": str(semantic.get("canonical") or ""),
            "sorted_candidates": [str(x) for x in list(semantic.get("sorted_candidates") or [])],
            "family": str(semantic.get("family") or ""),
        }
    )
    runtime_invariance_id = _sha256_json(
        {
            "provider": str(runtime.get("provider") or ""),
            "model": str(runtime.get("model") or ""),
            "prompt_version": str(runtime.get("prompt_version") or ""),
            "triage_version": str(runtime.get("triage_version") or ""),
            "semantic_settings_hash": str(runtime.get("semantic_settings_hash") or ""),
            "guardrail_flags": [str(x) for x in list(runtime.get("guardrail_flags") or [])],
        }
    )
    final_reuse_key = _sha256_json(
        {
            "proposal_semantic_id": proposal_semantic_id,
            "runtime_invariance_id": runtime_invariance_id,
            "scope_tier_salt": scope,
            "stage": str(stage or ""),
        }
    )
    return ReuseIdentity(
        stage=str(stage or ""),
        scope_tier=scope,
        proposal_semantic_id=proposal_semantic_id,
        runtime_invariance_id=runtime_invariance_id,
        final_reuse_key=final_reuse_key,
    )


def evaluate_gate(
    identity: ReuseIdentity,
    gate_inputs: dict[str, Any] | None,
    policy: dict[str, Any] | None,
) -> ReuseDecision:
    inputs = dict(gate_inputs or {})
    cfg = dict(policy or {})
    return ReuseDecision(
        enabled=bool(cfg.get("enabled", True)),
        seed_available=bool(inputs.get("seed_available", False)),
        runtime_match=bool(inputs.get("runtime_match", False)),
        semantic_match=bool(inputs.get("semantic_match", False)),
        soft_blocked=bool(inputs.get("soft_blocked", False)),
        scope_tier=str(identity.scope_tier or "dataset"),
        seed_run_id=str(inputs.get("seed_run_id") or "") or None,
        seed_created_at=str(inputs.get("seed_created_at") or "") or None,
    )


def emit_provenance(decision: ReuseDecision) -> dict[str, Any]:
    if not decision.enabled:
        return _fresh("reuse_disabled", decision)
    if not decision.seed_available:
        return _fresh("seed_missing", decision)
    if not decision.runtime_match:
        return _fresh("runtime_mismatch", decision)
    if not decision.semantic_match:
        return _fresh("semantic_mismatch", decision)
    if decision.soft_blocked:
        return _fresh("soft_blocked", decision)
    return {
        "reuse_status": "reused_exact_match",
        "reuse_reason": "reuse_enabled",
        "scope_tier": decision.scope_tier,
        "decision_source": "policy_gate",
        "gate_runtime_match": decision.runtime_match,
        "gate_semantic_match": decision.semantic_match,
        "gate_soft_blocked": decision.soft_blocked,
        "seed_run_id": decision.seed_run_id,
        "seed_created_at": decision.seed_created_at,
        "invalidation_reason": None,
    }


def _fresh(reason: str, decision: ReuseDecision) -> dict[str, Any]:
    return {
        "reuse_status": "fresh_compute",
        "reuse_reason": reason,
        "scope_tier": decision.scope_tier,
        "decision_source": "policy_gate",
        "gate_runtime_match": decision.runtime_match,
        "gate_semantic_match": decision.semantic_match,
        "gate_soft_blocked": decision.soft_blocked,
        "seed_run_id": decision.seed_run_id,
        "seed_created_at": decision.seed_created_at,
        "invalidation_reason": reason,
    }
