"""@meta
name: main
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.main.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import logging
import os
import warnings
from pathlib import Path
from typing import Any

from fitcv_cp.app import create_app
from fitcv_cp.backend_runtime import resolve_backend_runtime
from fitcv_cp.bq_store import get_pipeline_runs_schema_status
from fitcv.config import resolve_model_routing_part

logger = logging.getLogger(__name__)

def _load_dotenv_defaults() -> None:
    """Load local `.env` defaults without overriding existing process env."""
    dotenv_path = Path.cwd() / ".env"
    if not dotenv_path.exists() or not dotenv_path.is_file():
        return
    try:
        for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_key = key.strip()
            if not env_key or os.environ.get(env_key) is not None:
                continue
            os.environ[env_key] = value.strip().strip("'\"")
    except OSError as exc:
        logger.warning("Failed to read .env defaults from %s: %s", dotenv_path, exc)


def _validate_google_credentials_path() -> None:
    """Resolve credentials path when possible; otherwise fall back to ADC."""
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        return

    path = Path(credentials_path)
    if not path.exists():
        warnings.warn(
            "GOOGLE_APPLICATION_CREDENTIALS does not exist: "
            f"{credentials_path}. Falling back to ADC.",
            RuntimeWarning,
            stacklevel=2,
        )
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        return
    if path.is_dir():
        candidates = sorted(candidate for candidate in path.glob("*.json") if candidate.is_file())
        if len(candidates) == 1:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(candidates[0])
            return
        warnings.warn(
            "GOOGLE_APPLICATION_CREDENTIALS points to a directory without a single "
            f"key JSON file: {credentials_path}. Falling back to ADC.",
            RuntimeWarning,
            stacklevel=2,
        )
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)


def _build_bigquery_client() -> Any:
    _validate_google_credentials_path()
    from google.cloud import bigquery

    return bigquery.Client()

def _warn_or_fail_langgraph_override_drift() -> None:
    """Detect env override drift against control-plane SSOT routing."""
    routed = resolve_model_routing_part("cv_generation_structured_write")
    env_provider = str(os.environ.get("FITCV_LANGGRAPH_PROVIDER") or "").strip().lower()
    env_model = str(os.environ.get("FITCV_LANGGRAPH_MODEL") or "").strip()
    env_base_url = str(os.environ.get("FITCV_LANGGRAPH_OPENAI_BASE_URL") or "").strip()
    env_wire_api = str(os.environ.get("FITCV_LANGGRAPH_WIRE_API") or "").strip()
    if not any((env_provider, env_model, env_base_url, env_wire_api)):
        return

    routed_provider = str(routed.get("provider") or "").strip().lower()
    routed_model = str(routed.get("model") or "").strip()
    routed_base_url = str(routed.get("base_url") or "").strip()
    routed_wire_api = str(routed.get("wire_api") or "").strip()
    drift_fields: list[str] = []
    if env_provider and env_provider != routed_provider:
        drift_fields.append("provider")
    if env_model and env_model != routed_model:
        drift_fields.append("model")
    if env_base_url and env_base_url != routed_base_url:
        drift_fields.append("base_url")
    if env_wire_api and env_wire_api != routed_wire_api:
        drift_fields.append("wire_api")
    if not drift_fields:
        return

    message = (
        "LangGraph env override conflicts with control-plane routing SSOT "
        f"(fields={','.join(drift_fields)}). "
        "Clear FITCV_LANGGRAPH_* env vars or align them with config/runtime/control_plane.yaml."
    )
    strict = str(os.environ.get("FITCV_LANGGRAPH_OVERRIDE_STRICT") or "").strip().lower() in {"1", "true", "yes", "on"}
    if strict:
        raise RuntimeError(message)
    logger.warning(message)

def _ensure_safe_local_execution_mode() -> None:
    """Default to queue execution on Windows when execution mode is unset."""
    if os.name != "nt":
        return
    raw = str(os.environ.get("FITCV_CP_INLINE_EXECUTION", "") or "").strip().lower()
    if raw:
        return
    os.environ["FITCV_CP_INLINE_EXECUTION"] = "0"
    logger.warning(
        "FITCV_CP_INLINE_EXECUTION was unset on Windows; defaulted to queue mode (inline disabled)."
    )


def build_app() -> Any:
    _load_dotenv_defaults()
    _ensure_safe_local_execution_mode()
    _warn_or_fail_langgraph_override_drift()
    runtime = resolve_backend_runtime()
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")

    if runtime.backend_type == "sqlite":
        logger.info("control-plane backend mode: sqlite")
        return create_app(
            bq=None,
            project=runtime.project or "local",
            dataset=runtime.dataset,
            redis_url=redis_url,
        )

    if not runtime.project:
        raise ValueError("GCP_PROJECT must be set for bigquery backend mode")

    bq = _build_bigquery_client()
    schema_status = get_pipeline_runs_schema_status(
        bq,
        project=runtime.project,
        dataset=runtime.dataset,
    )
    if schema_status.get("status") == "complete":
        logger.info(
            "orchestration schema mode: complete (%s.%s.pipeline_runs)",
            runtime.project,
            runtime.dataset,
        )
    elif schema_status.get("status") == "fallback":
        missing = ", ".join(schema_status.get("missing_columns") or [])
        logger.warning(
            "orchestration schema mode: fallback (missing columns: %s). "
            "Run migration to add orchestration_backend and orchestration_run_id.",
            missing or "unknown",
        )
    else:
        logger.warning(
            "orchestration schema mode: unknown (%s).",
            schema_status.get("warning") or "schema check failed",
        )

    return create_app(
        bq=bq,
        project=runtime.project,
        dataset=runtime.dataset,
        redis_url=redis_url,
    )


app = build_app()
