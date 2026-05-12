"""@meta
name: orchestrator
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.orchestrator.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Literal

import httpx

from fitcv_cp import queue


@dataclass(frozen=True)
class RunSubmission:
    run_id: str
    queue_job_id: str
    backend_run_id: str | None = None
    backend: str = "queue"


OrchestrationMode = Literal["default_queue", "prefect"]


@dataclass(frozen=True)
class OrchestrationAdapter:
    name: OrchestrationMode

    def enqueue_run_with_job_id(
        self,
        *,
        jobs_path: str,
        config_path: str,
        triggered_by: str,
        redis_url: str,
        run_id: str | None = None,
    ) -> tuple[str, str]:
        submission = self.submit(
            jobs_path=jobs_path,
            config_path=config_path,
            triggered_by=triggered_by,
            redis_url=redis_url,
            run_id=run_id,
        )
        return submission.run_id, submission.queue_job_id

    def enqueue_run(
        self,
        *,
        jobs_path: str,
        config_path: str,
        triggered_by: str,
        redis_url: str,
        run_id: str | None = None,
    ) -> str:
        submission = self.submit(
            jobs_path=jobs_path,
            config_path=config_path,
            triggered_by=triggered_by,
            redis_url=redis_url,
            run_id=run_id,
        )
        return submission.run_id

    def cancel_queued_run(self, *, queue_job_id: str, redis_url: str) -> bool:
        return self.cancel(queue_job_id=queue_job_id, redis_url=redis_url)

    def submit(
        self,
        *,
        jobs_path: str,
        config_path: str,
        triggered_by: str,
        redis_url: str,
        run_id: str | None = None,
    ) -> RunSubmission:
        run_id_value, queue_job_id = queue.enqueue_run_with_job_id(
            jobs_path=jobs_path,
            config_path=config_path,
            triggered_by=triggered_by,
            redis_url=redis_url,
            run_id=run_id,
        )
        return RunSubmission(
            run_id=run_id_value,
            queue_job_id=queue_job_id,
            backend_run_id=queue_job_id,
            backend="queue",
        )

    def continue_run(
        self,
        *,
        run_id: str,
        jobs_path: str,
        config_path: str,
        triggered_by: str,
        redis_url: str,
    ) -> RunSubmission:
        # Continue uses the same bounded execution submit path with a fixed run_id.
        return self.submit(
            jobs_path=jobs_path,
            config_path=config_path,
            triggered_by=triggered_by,
            redis_url=redis_url,
            run_id=run_id,
        )

    def cancel(self, *, queue_job_id: str, redis_url: str) -> bool:
        return queue.cancel_queued_run(queue_job_id=queue_job_id, redis_url=redis_url)

    def status(self, *, queue_job_id: str, redis_url: str) -> str:
        return queue.get_queue_job_status(queue_job_id=queue_job_id, redis_url=redis_url)


@dataclass(frozen=True)
class PrefectOrchestrationAdapter(OrchestrationAdapter):
    """Prefect-mode adapter with queue fallback when Prefect is unavailable."""

    def _prefect_config(self) -> dict[str, str] | None:
        api_url = str(os.environ.get("PREFECT_API_URL", "") or "").strip().rstrip("/")
        deployment_id = str(os.environ.get("PREFECT_DEPLOYMENT_ID", "") or "").strip()
        if not api_url or not deployment_id:
            return None
        return {
            "api_url": api_url,
            "deployment_id": deployment_id,
            "api_key": str(os.environ.get("PREFECT_API_KEY", "") or "").strip(),
            "api_version": str(os.environ.get("PREFECT_API_VERSION", "0.8.4") or "0.8.4").strip(),
        }

    def _prefect_headers(self, cfg: dict[str, str]) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "x-prefect-api-version": cfg["api_version"],
        }
        if cfg["api_key"]:
            headers["Authorization"] = f"Bearer {cfg['api_key']}"
        return headers

    def _prefect_submit(
        self,
        *,
        cfg: dict[str, str],
        run_id: str,
        jobs_path: str,
        config_path: str,
        triggered_by: str,
    ) -> str:
        payload = {
            "name": f"fitcv-{run_id}",
            "idempotency_key": run_id,
            "parameters": {
                "run_id": run_id,
                "jobs_path": jobs_path,
                "config_path": config_path,
                "triggered_by": triggered_by,
            },
            "labels": {
                "fitcv.run_id": run_id,
                "fitcv.triggered_by": triggered_by,
            },
        }
        url = f"{cfg['api_url']}/deployments/{cfg['deployment_id']}/create_flow_run"
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(url, headers=self._prefect_headers(cfg), json=payload)
            resp.raise_for_status()
            row = resp.json() if resp.content else {}
        flow_run_id = str((row or {}).get("id") or "").strip()
        if not flow_run_id:
            raise RuntimeError("Prefect create_flow_run response missing flow-run id")
        return flow_run_id

    def _prefect_set_cancelling(self, *, cfg: dict[str, str], flow_run_id: str) -> bool:
        url = f"{cfg['api_url']}/flow_runs/{flow_run_id}/set_state"
        payload = {"state": {"type": "CANCELLING", "name": "Cancelling"}}
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(url, headers=self._prefect_headers(cfg), json=payload)
            if resp.status_code >= 400:
                return False
        return True

    def _prefect_status(self, *, cfg: dict[str, str], flow_run_id: str) -> str:
        url = f"{cfg['api_url']}/flow_runs/{flow_run_id}"
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url, headers=self._prefect_headers(cfg))
            if resp.status_code == 404:
                return "missing"
            resp.raise_for_status()
            row = resp.json() if resp.content else {}
        state = dict((row or {}).get("state") or {})
        state_type = str(state.get("type") or "").strip().upper()
        mapping = {
            "SCHEDULED": "queued",
            "PENDING": "queued",
            "RUNNING": "started",
            "COMPLETED": "finished",
            "FAILED": "failed",
            "CRASHED": "failed",
            "CANCELLING": "cancelling",
            "CANCELLED": "cancelled",
            "PAUSED": "deferred",
        }
        return mapping.get(state_type, "unknown")

    def submit(
        self,
        *,
        jobs_path: str,
        config_path: str,
        triggered_by: str,
        redis_url: str,
        run_id: str | None = None,
    ) -> RunSubmission:
        cfg = self._prefect_config()
        if not cfg:
            submission = super().submit(
                jobs_path=jobs_path,
                config_path=config_path,
                triggered_by=triggered_by,
                redis_url=redis_url,
                run_id=run_id,
            )
            return RunSubmission(
                run_id=submission.run_id,
                queue_job_id=submission.queue_job_id,
                backend_run_id=submission.backend_run_id,
                backend="prefect",
            )

        run_id_value = str(run_id or "").strip()
        if not run_id_value:
            run_id_value = str(uuid.uuid4())
        try:
            flow_run_id = self._prefect_submit(
                cfg=cfg,
                run_id=run_id_value,
                jobs_path=jobs_path,
                config_path=config_path,
                triggered_by=triggered_by,
            )
            return RunSubmission(
                run_id=run_id_value,
                queue_job_id=flow_run_id,
                backend_run_id=flow_run_id,
                backend="prefect",
            )
        except Exception:
            submission = super().submit(
                jobs_path=jobs_path,
                config_path=config_path,
                triggered_by=triggered_by,
                redis_url=redis_url,
                run_id=run_id_value,
            )
            return RunSubmission(
                run_id=submission.run_id,
                queue_job_id=submission.queue_job_id,
                backend_run_id=submission.backend_run_id,
                backend="prefect",
            )

    def cancel(self, *, queue_job_id: str, redis_url: str) -> bool:
        cfg = self._prefect_config()
        if not cfg:
            return super().cancel(queue_job_id=queue_job_id, redis_url=redis_url)
        try:
            if self._prefect_set_cancelling(cfg=cfg, flow_run_id=queue_job_id):
                return True
        except Exception:
            pass
        return super().cancel(queue_job_id=queue_job_id, redis_url=redis_url)

    def status(self, *, queue_job_id: str, redis_url: str) -> str:
        cfg = self._prefect_config()
        if not cfg:
            return super().status(queue_job_id=queue_job_id, redis_url=redis_url)
        try:
            return self._prefect_status(cfg=cfg, flow_run_id=queue_job_id)
        except Exception:
            return super().status(queue_job_id=queue_job_id, redis_url=redis_url)


def get_orchestration_adapter() -> OrchestrationAdapter:
    """Resolve orchestration adapter from runtime mode, defaulting to queue."""
    mode = str(os.environ.get("FITCV_ORCHESTRATION_MODE", "default_queue") or "default_queue").strip().lower()
    if mode == "prefect":
        return PrefectOrchestrationAdapter(name="prefect")
    return OrchestrationAdapter(name="default_queue")
