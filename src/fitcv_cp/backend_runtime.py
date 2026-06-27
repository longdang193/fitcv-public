"""@meta
name: backend_runtime
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.backend_runtime.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from fitcv.config import load_control_plane_config

_ACTIVE_BACKEND_RUNTIME: BackendRuntime | None = None


@dataclass(frozen=True)
class BackendRuntime:
    backend_type: str
    project: str
    dataset: str
    sqlite_path: str


def set_backend_runtime(runtime: BackendRuntime | None) -> None:
    """Set process-wide backend runtime for live data-plane helpers."""
    global _ACTIVE_BACKEND_RUNTIME
    _ACTIVE_BACKEND_RUNTIME = runtime


def get_backend_runtime() -> BackendRuntime | None:
    """Return active backend runtime when startup already resolved it."""
    return _ACTIVE_BACKEND_RUNTIME


def resolve_backend_runtime_or_active() -> BackendRuntime:
    active = get_backend_runtime()
    if active is not None:
        return active
    return resolve_backend_runtime()


def resolve_backend_runtime() -> BackendRuntime:
    """Resolve backend mode and runtime connection settings.

    Uses control-plane config plus explicit env overrides for deterministic local runs.
    """
    cfg = load_control_plane_config()
    data_backend = dict(cfg.get("data_backend") or {})
    backend_type = str(
        os.environ.get("FITCV_CP_DATA_BACKEND")
        or data_backend.get("type")
        or "bigquery"
    ).strip().lower() or "bigquery"
    if backend_type not in {"bigquery", "sqlite"}:
        raise ValueError(f"Unsupported backend type: {backend_type}")

    bq_cfg = dict(data_backend.get("bigquery") or {})
    sqlite_cfg = dict(data_backend.get("sqlite") or {})
    project = str(os.environ.get("GCP_PROJECT") or bq_cfg.get("project") or "").strip()
    dataset = str(os.environ.get("BIGQUERY_DATASET") or bq_cfg.get("dataset") or "fitcv").strip() or "fitcv"
    sqlite_path = str(
        os.environ.get("FITCV_CP_SQLITE_PATH")
        or sqlite_cfg.get("path")
        or "data/fitcv_cp.sqlite3"
    ).strip() or "data/fitcv_cp.sqlite3"
    return BackendRuntime(
        backend_type=backend_type,
        project=project,
        dataset=dataset,
        sqlite_path=sqlite_path,
    )
