"""@meta
name: review_identity
type: module
domain: operator_control_plane
ownership: infrastructure
responsibility:
  - Provide deterministic identity helpers for review-required CV records.
inputs:
  - run_id and review record payload fields
outputs:
  - stable review_item_id values for queue/action normalization
lifecycle:
  - status: active
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

TERMINAL_REVIEW_RESOLUTION_STATUSES = {
    "approved_as_is",
    "rejected",
    "regenerated_and_accepted",
    "regenerated_and_rejected",
}


def normalize_review_item_id(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw or None


def build_review_item_id(
    *,
    run_id: str,
    record: dict[str, Any],
    fallback_index: int | None = None,
) -> str:
    """Build a deterministic review item id from stable record fields.

    The identity intentionally avoids volatile fields (timestamps, regenerated
    drafts) so legacy rows can be deterministically derived at read time.
    """
    payload = {
        "run_id": str(run_id or "").strip(),
        "job_url": str(record.get("job_url") or "").strip(),
        "job_title": str(record.get("job_title") or "").strip(),
        "rank": int(record.get("rank") or 0),
        "attempt_count": int(record.get("attempt_count") or 0),
        "fallback_index": int(fallback_index or 0),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"ri_{digest[:20]}"


def ensure_review_item_id(
    *,
    run_id: str,
    record: dict[str, Any],
    fallback_index: int | None = None,
) -> str:
    existing = normalize_review_item_id(record.get("review_item_id"))
    if existing is not None:
        record["review_item_id"] = existing
        return existing
    generated = build_review_item_id(run_id=run_id, record=record, fallback_index=fallback_index)
    record["review_item_id"] = generated
    return generated


def is_review_resolution_pending(resolution_status: Any) -> bool:
    normalized = str(resolution_status or "").strip().lower() or "pending"
    return normalized not in TERMINAL_REVIEW_RESOLUTION_STATUSES
