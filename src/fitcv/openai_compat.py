"""@meta
name: openai_compat
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared decoding helpers for OpenAI-compatible HTTP provider responses.
inputs:
  - HTTP response objects from OpenAI-compatible providers
outputs:
  - Decoded response payloads and extracted assistant text
lifecycle:
  - status: active
"""

import json
from typing import Any


def extract_openai_sse_json_payload(raw_text: str) -> dict[str, Any] | None:
    """Return the last JSON object carried in an SSE payload, ignoring [DONE]."""
    if not str(raw_text or "").strip():
        return None

    latest_payload: dict[str, Any] | None = None
    current_lines: list[str] = []

    def _flush_event() -> None:
        nonlocal current_lines, latest_payload
        if not current_lines:
            return
        candidate = "\n".join(current_lines).strip()
        current_lines = []
        if not candidate or candidate == "[DONE]":
            return
        try:
            parsed = json.loads(candidate)
        except Exception:
            return
        if isinstance(parsed, dict):
            latest_payload = dict(parsed)

    for raw_line in str(raw_text).splitlines():
        stripped = raw_line.rstrip("\r").strip()
        if not stripped:
            _flush_event()
            continue
        if stripped.startswith(":"):
            continue
        if stripped.startswith("data:"):
            current_lines.append(stripped[5:].lstrip())

    _flush_event()
    return latest_payload


def decode_openai_compat_response_body(resp: Any) -> dict[str, Any]:
    """Decode plain JSON or SSE-framed JSON from OpenAI-compatible providers."""
    try:
        parsed = resp.json()
    except json.JSONDecodeError:
        parsed = None
    except ValueError:
        parsed = None
    else:
        if isinstance(parsed, dict):
            return dict(parsed)
        return {}

    content_type = str((getattr(resp, "headers", {}) or {}).get("content-type") or "").lower()
    raw_text = str(getattr(resp, "text", "") or "")
    if "event-stream" in content_type or "data:" in raw_text:
        payload = extract_openai_sse_json_payload(raw_text)
        if payload is not None:
            return payload
        stripped = raw_text.lstrip()
        if stripped:
            try:
                decoder = json.JSONDecoder()
                parsed, _end = decoder.raw_decode(stripped)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                return dict(parsed)

    return dict(json.loads(raw_text or "{}") or {})


def extract_openai_responses_text(body: dict[str, Any]) -> str:
    """Extract assistant text from OpenAI-compatible /responses payloads."""
    direct = str(body.get("output_text") or "").strip()
    if direct:
        return direct
    output = body.get("output")
    if not isinstance(output, list):
        return ""
    chunks: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        for content_item in item.get("content") or []:
            if not isinstance(content_item, dict):
                continue
            text = str(content_item.get("text") or "").strip()
            if text:
                chunks.append(text)
    return "\n".join(chunks).strip()


def extract_openai_chat_completions_text(body: dict[str, Any]) -> str:
    """Extract assistant text from OpenAI-compatible /chat/completions payloads."""
    return str((((body.get("choices") or [{}])[0]).get("message") or {}).get("content") or "").strip()
