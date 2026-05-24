"""@meta
name: run_artifact_contracts
type: module
domain: run_orchestration
ownership: infrastructure
responsibility:
  - Provide shared SSOT helper contracts for run artifact payload construction.
inputs:
  - run records, replay context, and runtime artifact values
outputs:
  - normalized run-mode labels and JSON-safe artifact payload fragments
  - shared JSON decode helpers for run artifacts
lifecycle:
  - status: active
"""

from __future__ import annotations

import datetime
import hashlib
import json as _json
from typing import Any

RUN_MODE_LABELS = {
    "run_all": "Run All",
    "manual_staged": "Stage by Stage",
}


def string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def normalized_run_mode(value: Any) -> str:
    run_mode = string_or_none(value)
    if run_mode in RUN_MODE_LABELS:
        return run_mode
    return "run_all"


def run_mode_label(value: Any) -> str:
    return RUN_MODE_LABELS[normalized_run_mode(value)]


def iso_or_none(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime.datetime) else None


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, set):
        return [json_safe(item) for item in sorted(value)]
    return value


def decode_json_object_or_none(raw_payload: str | None) -> dict[str, Any] | None:
    if not raw_payload:
        return None
    try:
        payload = _json.loads(raw_payload)
    except (_json.JSONDecodeError, TypeError):
        return None
    return payload if isinstance(payload, dict) else None


def decode_json_object_or_raise(raw_payload: str | None) -> dict[str, Any]:
    payload = _json.loads(raw_payload or "")
    if not isinstance(payload, dict):
        raise ValueError("decoded_json_not_object")
    return payload

def encode_json_object(payload: dict[str, Any]) -> str:
    return _json.dumps(payload, ensure_ascii=False)

def stable_json_dumps(payload: Any) -> str:
    return _json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def stable_sha256_fingerprint(payload: Any) -> str:
    raw = stable_json_dumps(payload)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def require_payload_keys(
    payload: dict[str, Any],
    *,
    required_keys: set[str],
    context: str,
) -> dict[str, Any]:
    missing = sorted(key for key in required_keys if key not in payload)
    if missing:
        raise ValueError(f"missing_required_payload_keys:{context}:{','.join(missing)}")
    return payload


def schema_version_or_none(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    value = payload.get("schema_version")
    return value if isinstance(value, str) and value.strip() else None


def schema_version_matches(payload: dict[str, Any] | None, expected: str) -> bool:
    return schema_version_or_none(payload) == expected


def pretty_json_string(raw_json: str) -> str:
    return _json.dumps(_json.loads(raw_json), ensure_ascii=False, indent=2)


def pretty_json_string_or_fallback(raw_json: str | None) -> str:
    if not raw_json:
        return ""
    try:
        return pretty_json_string(str(raw_json))
    except (_json.JSONDecodeError, TypeError, ValueError):
        return str(raw_json)


def replay_context_payload(*, replay_context: dict[str, Any], run_id: str) -> dict[str, str]:
    return {
        "replay_mode": str(replay_context.get("replay_mode") or "strict"),
        "replay_source_run_id": str(replay_context.get("replay_source_run_id") or run_id),
        "policy_registry_version": str(replay_context.get("policy_registry_version") or "policy_registry.v1"),
        "policy_envelope_signature": str(replay_context.get("policy_envelope_signature") or ""),
    }
