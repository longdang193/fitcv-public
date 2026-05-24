"""@meta
name: reporter
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.reporter.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import datetime
import json
import logging
import os
import uuid
from typing import Any, Optional

import httpx
from fitcv.telemetry import (
    build_trace_context,
    current_trace_context,
    langfuse_link_status,
    telemetry_export_status,
)
from fitcv_cp.bq_store import append_event
from fitcv_cp.models import RunEvent
from fitcv_cp.runtime_contracts import is_truthy_env

logger = logging.getLogger(__name__)

_MAX_STRING_LENGTH = 500
_MAX_LIST_ITEMS = 20
_MAX_OBJECT_KEYS = 30
_SENSITIVE_KEY_PARTS = {
    "password",
    "secret",
    "authorization",
    "api_key",
    "private_key",
    "access_key",
    "cookie",
}



def _truncate_string(value: str) -> str:
    if len(value) <= _MAX_STRING_LENGTH:
        return value
    return f"{value[:_MAX_STRING_LENGTH]}...[truncated]"


def _redact_and_bound(value: Any, *, depth: int = 0) -> Any:
    if depth > 4:
        return "[truncated_depth]"
    if isinstance(value, dict):
        reduced: dict[str, Any] = {}
        for idx, (key, val) in enumerate(value.items()):
            if idx >= _MAX_OBJECT_KEYS:
                reduced["__truncated_keys__"] = True
                break
            key_text = str(key)
            key_lower = key_text.lower()
            if any(part in key_lower for part in _SENSITIVE_KEY_PARTS):
                reduced[key_text] = "[REDACTED]"
                continue
            reduced[key_text] = _redact_and_bound(val, depth=depth + 1)
        return reduced
    if isinstance(value, list):
        items = [_redact_and_bound(item, depth=depth + 1) for item in value[:_MAX_LIST_ITEMS]]
        if len(value) > _MAX_LIST_ITEMS:
            items.append("[truncated_items]")
        return items
    if isinstance(value, str):
        return _truncate_string(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return _truncate_string(str(value))


def _build_langfuse_rich_io_contract(
    *,
    stage: str,
    level: str,
    message: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    if not is_truthy_env(os.environ.get("FITCV_LANGFUSE_RICH_IO_ENABLED")):
        return {
            "status": "disabled",
            "degradation_reason": "langfuse_rich_io_disabled",
            "input": None,
            "output": None,
        }
    stage_lower = stage.lower()
    stage_family = "generic"
    if "normalize" in stage_lower:
        stage_family = "normalize"
    elif "cv_analysis" in stage_lower:
        stage_family = "cv_analysis"
    elif "cv_generation" in stage_lower:
        stage_family = "cv_generation"

    bounded_payload = _redact_and_bound(payload)
    rich_input: dict[str, Any] = {
        "stage": stage,
        "stage_family": stage_family,
        "message": _truncate_string(message),
        "payload": bounded_payload,
    }
    rich_output: dict[str, Any] = {
        "level": level,
        "event_status": "emitted",
        "stage_family": stage_family,
    }
    latency_ms = payload.get("latency_ms")
    if isinstance(latency_ms, int) and latency_ms >= 0:
        rich_output["latency_ms"] = latency_ms
    usage_payload = payload.get("usage")
    if isinstance(usage_payload, dict):
        rich_output["usage"] = _redact_and_bound(usage_payload)
    cost_payload = payload.get("cost")
    if isinstance(cost_payload, dict):
        rich_output["cost"] = _redact_and_bound(cost_payload)
    elif isinstance(cost_payload, (int, float)):
        rich_output["cost"] = {"total": float(cost_payload), "currency": "usd"}
    elif isinstance(payload.get("cost_usd"), (int, float)):
        rich_output["cost"] = {"total": float(payload["cost_usd"]), "currency": "usd"}

    if stage_family in {"normalize", "cv_analysis", "cv_generation"}:
        input_snapshot = payload.get("input_snapshot")
        output_snapshot = payload.get("output_snapshot")
        if isinstance(input_snapshot, (dict, list)):
            rich_input["input_snapshot"] = _redact_and_bound(input_snapshot)
        if isinstance(output_snapshot, (dict, list)):
            rich_output["output_snapshot"] = _redact_and_bound(output_snapshot)

    return {
        "status": "ready",
        "degradation_reason": None,
        "input": rich_input,
        "output": rich_output,
    }


def _langfuse_ingestion_enabled() -> bool:
    return is_truthy_env(os.environ.get("FITCV_LANGFUSE_RICH_IO_ENABLED"))


def _build_langfuse_ingestion_headers() -> dict[str, str] | None:
    public_key = str(os.environ.get("FITCV_LANGFUSE_PROJECT_PUBLIC_KEY") or "").strip()
    secret_key = str(os.environ.get("FITCV_LANGFUSE_PROJECT_SECRET_KEY") or "").strip()
    if not public_key or not secret_key:
        return None
    import base64

    token = base64.b64encode(f"{public_key}:{secret_key}".encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }


def _emit_langfuse_native_io(
    *,
    run_id: str,
    stage: str,
    trace_id: str,
    rich_contract: dict[str, Any],
) -> tuple[str, str | None]:
    if not _langfuse_ingestion_enabled():
        return "disabled", "langfuse_rich_io_disabled"
    stage_family = str((rich_contract.get("input") or {}).get("stage_family") or "")
    if stage_family in {"normalize", "cv_analysis", "cv_generation"}:
        return "superseded_by_span_contract", "otel_langfuse_span_contract_active"
    headers = _build_langfuse_ingestion_headers()
    if headers is None:
        return "degraded", "langfuse_credentials_missing"
    if stage_family not in {"generic"}:
        return "not_applicable", None
    base_url = str(os.environ.get("FITCV_LANGFUSE_BASE_URL") or "").strip().rstrip("/")
    if not base_url:
        return "degraded", "langfuse_base_url_missing"
    ingestion_url = f"{base_url}/api/public/ingestion"
    now = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    rich_input = dict(rich_contract.get("input") or {})
    rich_output = dict(rich_contract.get("output") or {})
    batch: list[dict[str, Any]] = [
        {
            "id": str(uuid.uuid4()),
            "timestamp": now,
            "type": "trace-create",
            "body": {
                # Attach rich payloads to the same trace context surfaced in run events.
                "id": trace_id,
                "name": f"{stage}:rich_io",
                "sessionId": run_id,
                "userId": str(os.environ.get("FITCV_LANGFUSE_USER_ID") or "fitcv-control-plane"),
                "input": rich_input,
                "output": rich_output,
                "metadata": {
                    "source_trace_id": trace_id,
                    "stage": stage,
                    "stage_family": stage_family,
                    "rich_io_source": "fitcv-control-plane",
                },
            },
        }
    ]
    latency_ms = rich_output.get("latency_ms")
    if isinstance(latency_ms, int) and latency_ms > 0:
        end_at = datetime.datetime.now(datetime.timezone.utc)
        start_at = end_at - datetime.timedelta(milliseconds=int(latency_ms))
        batch.append(
            {
                "id": str(uuid.uuid4()),
                "timestamp": now,
                "type": "observation-create",
                "body": {
                    "id": f"{trace_id}:latency",
                    "traceId": trace_id,
                    "name": f"{stage}:rich_io_latency",
                    "type": "SPAN",
                    "startTime": start_at.isoformat().replace("+00:00", "Z"),
                    "endTime": end_at.isoformat().replace("+00:00", "Z"),
                    "level": "DEFAULT",
                    "metadata": {
                        "source_trace_id": trace_id,
                        "stage": stage,
                        "stage_family": stage_family,
                        "latency_source": "payload.latency_ms",
                    },
                },
            }
        )
    body = {
        "batch": batch,
        "metadata": {"source": "fitcv-control-plane"},
    }
    try:
        resp = httpx.post(ingestion_url, headers=headers, json=body, timeout=5.0)
        if 200 <= resp.status_code < 300:
            return f"sent:{trace_id}", None
        return "degraded", f"langfuse_ingestion_http_{resp.status_code}"
    except Exception:
        return "degraded", "langfuse_ingestion_failed"


class PipelineReporter:
    def __init__(self, run_id: str, bq: Any, *, project: str, dataset: str) -> None:
        self._run_id = run_id
        self._bq = bq
        self._project = project
        self._dataset = dataset

    def emit(
        self,
        stage: str,
        level: str,
        message: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        source_payload = dict(payload or {})
        payload_value = dict(source_payload)
        active_trace_context = current_trace_context()
        if active_trace_context is not None:
            payload_value["trace_context"] = active_trace_context
        else:
            payload_value["trace_context"] = build_trace_context(
                f"run:{self._run_id}:stage:{stage}:message:{message}",
                emit_otel_span=False,
            )
        payload_value["telemetry_export"] = telemetry_export_status()
        payload_value["langfuse_rich_io"] = _build_langfuse_rich_io_contract(
            stage=stage,
            level=level,
            message=message,
            payload=source_payload,
        )
        trace_id = str(payload_value["trace_context"].get("trace_id") or "")
        native_status, native_reason = _emit_langfuse_native_io(
            run_id=self._run_id,
            stage=stage,
            trace_id=trace_id,
            rich_contract=dict(payload_value["langfuse_rich_io"] or {}),
        )
        payload_value["langfuse_link"] = langfuse_link_status(
            trace_id,
            verified=native_status.startswith("sent:"),
        )
        payload_value["langfuse_rich_io_native"] = {
            "status": native_status,
            "degradation_reason": native_reason,
        }
        event = RunEvent(
            run_id=self._run_id,
            event_id=str(uuid.uuid4()),
            stage=stage,
            level=level,
            message=message,
            created_at=datetime.datetime.now(datetime.timezone.utc),
            payload_json=json.dumps(payload_value),
        )
        try:
            status = append_event(event, self._bq, project=self._project, dataset=self._dataset)
            if status.get("persistence_status") != "persisted":
                logger.warning(
                    "Reporter event degraded [run_id=%s stage=%s status=%s reason=%s]",
                    self._run_id,
                    stage,
                    status.get("persistence_status"),
                    status.get("degradation_reason"),
                )
        except Exception as exc:
            logger.warning("Reporter failed to write event: %s", exc)



