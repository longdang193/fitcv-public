"""@meta
name: run_artifact_mirror
type: module
domain: run_orchestration
ownership: infrastructure
responsibility:
  - Provide stable public utilities for deterministic terminal run artifact mirrors.
inputs:
  - run records and run events from control-plane store
outputs:
  - artifacts/live_run_<run_id> filesystem mirrors
lifecycle:
  - status: active
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import logging
from pathlib import Path
from typing import Any

from fitcv_cp.bq_store import get_events, get_run
from fitcv_cp.models import RunEvent, RunStatus

logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if dataclasses.is_dataclass(value):
        return {str(k): _json_safe(v) for k, v in dataclasses.asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value)]
    return value


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def build_terminal_run_artifact_payloads(
    *,
    run_record: Any,
    events: list[RunEvent],
) -> dict[str, Any]:
    payloads: dict[str, Any] = {
        "run.json": _json_safe(run_record),
        "events.json": [_json_safe(event) for event in events],
    }
    results_export_raw = getattr(run_record, "results_export_json", None)
    if isinstance(results_export_raw, str) and results_export_raw.strip():
        try:
            parsed = json.loads(results_export_raw)
            if isinstance(parsed, dict):
                payloads["export.json"] = {
                    "run_id": str(getattr(run_record, "run_id", "")),
                    "results": list(parsed.get("results") or []),
                }
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                "[run_id=%s] Skipping export.json mirror write due to invalid results_export_json",
                getattr(run_record, "run_id", ""),
            )
    for filename, attr_name in (
        ("cv-debug.json", "cv_generation_debug_json"),
        ("stage-artifacts.json", "stage_transition_artifacts_json"),
        ("settings-used.json", "settings_used_json"),
        ("mapping-suggestions.json", "mapping_suggestions_json"),
        ("synonym-proposals.json", "synonym_proposals_json"),
    ):
        raw_payload = getattr(run_record, attr_name, None)
        if not isinstance(raw_payload, str) or not raw_payload.strip():
            continue
        try:
            payloads[filename] = json.loads(raw_payload)
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                "[run_id=%s] Skipping %s mirror write due to invalid JSON payload",
                getattr(run_record, "run_id", ""),
                filename,
            )
    cv_debug_payload = payloads.get("cv-debug.json")
    if isinstance(cv_debug_payload, dict):
        agentic_trace = cv_debug_payload.get("agentic_live_trace")
        if isinstance(agentic_trace, dict):
            payloads["agentic-live-trace.json"] = agentic_trace
        cv_analysis_trace = cv_debug_payload.get("cv_analysis_trace")
        if isinstance(cv_analysis_trace, dict):
            payloads["cv-analysis-trace.json"] = cv_analysis_trace
    return payloads


def persist_terminal_run_artifact_mirror(
    *,
    run_id: str,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    run_record = get_run(run_id, bq, project=project, dataset=dataset)
    if run_record is None:
        logger.warning("[run_id=%s] Skipping artifact mirror write: run not found", run_id)
        return
    if run_record.status not in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}:
        return
    events = list(get_events(run_id, bq, project=project, dataset=dataset))
    payloads = build_terminal_run_artifact_payloads(run_record=run_record, events=events)
    mirror_dir = Path("artifacts") / f"live_run_{run_id}"
    mirror_dir.mkdir(parents=True, exist_ok=True)
    for filename, payload in payloads.items():
        _write_json_atomic(mirror_dir / filename, payload)
