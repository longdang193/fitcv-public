"""@meta
name: reconciler
type: module
domain: run_orchestration
ownership: infrastructure
responsibility:
  - Reconcile abandoned run attempts (crash / lost worker) into deterministic next action.
  - Enforce bounded retry policy using SSOT-first attempt events.
inputs:
  - Run store (sqlite or BigQuery-backed) + current time.
outputs:
  - SSOT updates: attempt terminal events, run terminalization, and/or re-enqueue.
lifecycle:
  - status: active
"""

from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import dataclass
from typing import Any

from fitcv_cp.models import RunEvent, RunStatus
from fitcv_cp.queue import enqueue_run_with_job_id
from fitcv_cp.retry_settings import load_retry_settings
from fitcv_cp.run_artifact_contracts import decode_run_attempt_payload_or_none, run_attempt_payload_v1
from fitcv_cp.store import RunStore


@dataclass(frozen=True)
class ReconcileSummary:
    scanned_runs: int
    abandoned_attempts: int
    requeued_attempts: int
    terminal_failed_runs: int


def _parse_iso_or_none(value: Any) -> datetime.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        return None


def reconcile_abandoned_attempts(
    store: RunStore,
    *,
    now: datetime.datetime | None = None,
) -> ReconcileSummary:
    """Reconcile lease-expired running attempts.

    SSOT implementation:
    - worker writes `run_attempt.v1` payloads as RunEvent(stage="run_attempt")
    - reconciler parses latest attempt state per attempt_id from event stream
    - if running lease expired: reconciler writes terminal attempt event + either
      re-enqueues (retry policy enabled) or terminalizes run as failed
    - if cancel was requested: reconciler terminalizes as cancelled and blocks retry
    """

    now = now or datetime.datetime.now(datetime.timezone.utc)
    scanned_runs = 0
    abandoned_attempts = 0
    requeued_attempts = 0
    terminal_failed_runs = 0
    settings = load_retry_settings()

    for run in store.list_runs(limit=200, include_archived=False, archived_only=False):
        scanned_runs += 1
        if getattr(run, "status", None) not in {RunStatus.QUEUED, RunStatus.RUNNING}:
            continue

        cancel_requested = getattr(run, "cancel_requested_at", None) is not None

        events = store.get_events(run.run_id)
        attempt_events: list[tuple[datetime.datetime, dict[str, Any]]] = []
        for event in events:
            payload = decode_run_attempt_payload_or_none(event.payload_json)
            if payload is None:
                continue
            attempt_events.append((event.created_at, payload))

        latest_by_attempt_id: dict[str, tuple[datetime.datetime, dict[str, Any]]] = {}
        for created_at, payload in attempt_events:
            attempt = payload.get("attempt") if isinstance(payload, dict) else None
            if not isinstance(attempt, dict):
                continue
            attempt_id = attempt.get("attempt_id")
            if not isinstance(attempt_id, str) or not attempt_id.strip():
                continue
            prior = latest_by_attempt_id.get(attempt_id)
            if prior is None or created_at > prior[0]:
                latest_by_attempt_id[attempt_id] = (created_at, payload)

        if not latest_by_attempt_id:
            continue

        attempt_ids = sorted(latest_by_attempt_id.keys())
        attempt_count = len(attempt_ids)

        for attempt_id in attempt_ids:
            created_at, payload = latest_by_attempt_id[attempt_id]
            _ = created_at
            attempt = payload.get("attempt") if isinstance(payload, dict) else None
            if not isinstance(attempt, dict):
                continue
            status = str(attempt.get("status") or "").strip().lower()
            if status != RunStatus.RUNNING.value:
                continue
            lease_expires_at = _parse_iso_or_none(attempt.get("lease_expires_at"))
            if lease_expires_at is None or lease_expires_at >= now:
                continue

            abandoned_attempts += 1

            if cancel_requested:
                store.append_event(
                    RunEvent(
                        run_id=run.run_id,
                        event_id=str(uuid.uuid4()),
                        stage="run_attempt",
                        level="info",
                        message="Run attempt cancelled (cancel requested; lease expired)",
                        created_at=now,
                        payload_json=json.dumps(
                            run_attempt_payload_v1(
                                attempt_id=attempt_id,
                                status=RunStatus.CANCELLED.value,
                                finished_at=now,
                                error_classification="canceled",
                                error_summary="cancel_requested_lease_expired",
                                retry_eligible=False,
                            ),
                            ensure_ascii=False,
                        ),
                    )
                )
                store.update_run_status(
                    run.run_id,
                    RunStatus.CANCELLED,
                    finished_at=now,
                    error_message="cancel_requested_lease_expired",
                )
                continue

            store.append_event(
                RunEvent(
                    run_id=run.run_id,
                    event_id=str(uuid.uuid4()),
                    stage="run_attempt",
                    level="error",
                    message="Run attempt abandoned (lease expired)",
                    created_at=now,
                    payload_json=json.dumps(
                        run_attempt_payload_v1(
                            attempt_id=attempt_id,
                            status="abandoned",
                            finished_at=now,
                            error_classification="transient",
                            error_summary="abandoned_lease_expired",
                            retry_eligible=True,
                        ),
                        ensure_ascii=False,
                    ),
                )
            )

            if not settings.enabled:
                store.update_run_status(
                    run.run_id,
                    RunStatus.FAILED,
                    finished_at=now,
                    error_message="abandoned_attempt_lease_expired",
                )
                terminal_failed_runs += 1
                continue

            if attempt_count >= settings.max_attempts:
                store.update_run_status(
                    run.run_id,
                    RunStatus.FAILED,
                    finished_at=now,
                    error_message="max_attempts_exhausted_abandoned",
                )
                terminal_failed_runs += 1
                continue

            enqueue_run_with_job_id(
                jobs_path=str(getattr(run, "jobs_path", "")),
                config_path=str(getattr(run, "config_path", "")),
                triggered_by="reconciler",
                run_id=run.run_id,
            )
            store.update_run_status(
                run.run_id,
                RunStatus.QUEUED,
                error_message="requeued_after_abandoned_attempt",
            )

            requeued_attempts += 1

    return ReconcileSummary(
        scanned_runs=scanned_runs,
        abandoned_attempts=abandoned_attempts,
        requeued_attempts=requeued_attempts,
        terminal_failed_runs=terminal_failed_runs,
    )

