"""@meta
name: shortlist_runtime
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared shortlist sqlite runtime and deterministic payload helpers.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Shared runtime helper symbols and deterministic hashing behavior
lifecycle:
  - status: active
"""

import sqlite3
import time
from collections.abc import Callable
import json
import hashlib
from typing import Any
from typing import TypeVar

T = TypeVar("T")


def sqlite_path() -> str:
    from fitcv.persistence import get_local_sqlite_path

    return get_local_sqlite_path()


def configure_sqlite_connection(conn: sqlite3.Connection) -> None:
    # Reduce transient sqlite failures on Docker Desktop Windows bind mounts.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")


def run_sqlite_io_retry(
    operation: Callable[[], T],
    *,
    max_attempts: int = 3,
) -> T:
    """Retry sqlite disk I/O errors with bounded linear backoff."""
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            last_error = exc
            if "disk I/O error" not in str(exc):
                raise
            if attempt >= max_attempts - 1:
                raise
            time.sleep(0.5 * (attempt + 1))
    if last_error is not None:
        raise last_error
    raise RuntimeError("sqlite retry operation failed without captured exception")


def normalize_text_scalar(value: Any) -> str:
    """Collapse repeated whitespace while preserving original casing."""
    return " ".join(str(value or "").split()).strip()


def canonicalize_for_hash(value: Any) -> Any:
    """Canonicalize nested payload values for deterministic hashing."""
    if isinstance(value, dict):
        return {
            key: canonicalize_for_hash(value[key])
            for key in sorted(value)
        }
    if isinstance(value, list):
        return [canonicalize_for_hash(item) for item in value]
    if isinstance(value, str):
        return value.casefold()
    return value


def hash_payload(payload: dict[str, Any]) -> tuple[str, str]:
    """Return canonical payload JSON and SHA-256 hash digest."""
    canonical_payload = canonicalize_for_hash(payload)
    payload_json = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return payload_json, digest


def build_contract_fingerprint(payload: dict[str, Any]) -> str:
    """Return deterministic SHA-256 digest for a JSON-like contract payload."""
    canonical_payload = canonicalize_for_hash(payload)
    payload_json = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def build_bigquery_client(config: dict[str, Any]) -> Any:
    """Build BigQuery client using optional service-account key path."""
    from fitcv.persistence import build_bigquery_client as _build_bigquery_client

    return _build_bigquery_client(config)
