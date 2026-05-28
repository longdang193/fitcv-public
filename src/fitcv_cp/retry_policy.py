"""@meta
name: retry_policy
type: module
domain: run_orchestration
ownership: infrastructure
responsibility:
  - Classify runtime failures into retry policy buckets.
  - Provide stable error summaries for SSOT persistence and UI display.
inputs:
  - Exceptions raised during orchestration / worker execution.
outputs:
  - Retry classification (`transient|permanent|canceled|unknown`) + stable summary.
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RetryClassification:
    classification: str
    summary: str
    details: dict[str, Any] | None = None


_TRANSIENT_HTTP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_PERMANENT_HTTP_STATUS_CODES = {400, 401, 403, 404, 409, 422}


def classify_exception_for_retry(exc: BaseException) -> RetryClassification:
    if isinstance(exc, KeyboardInterrupt):
        return RetryClassification("canceled", "keyboard_interrupt")

    try:
        import httpx
    except Exception:
        httpx = None  # type: ignore[assignment]

    if httpx is not None:
        timeout_types = (
            getattr(httpx, "TimeoutException", Exception),
        )
        if isinstance(exc, timeout_types):
            return RetryClassification("transient", "timeout")

        status_error_type = getattr(httpx, "HTTPStatusError", None)
        if status_error_type is not None and isinstance(exc, status_error_type):
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if isinstance(status_code, int):
                if status_code in _TRANSIENT_HTTP_STATUS_CODES:
                    return RetryClassification("transient", f"http_{status_code}")
                if status_code in _PERMANENT_HTTP_STATUS_CODES:
                    return RetryClassification("permanent", f"http_{status_code}")
                return RetryClassification("unknown", f"http_{status_code}")
            return RetryClassification("unknown", "http_status_error")

        transport_error_type = getattr(httpx, "TransportError", None)
        if transport_error_type is not None and isinstance(exc, transport_error_type):
            return RetryClassification("transient", "transport_error")

    if isinstance(exc, TimeoutError):
        return RetryClassification("transient", "timeout")

    return RetryClassification("unknown", exc.__class__.__name__)

