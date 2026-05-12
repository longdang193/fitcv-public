"""@meta
name: queue
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.queue.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import importlib
import multiprocessing
import os
import sys
import threading
import time
import types
import uuid
from datetime import datetime, timezone
from typing import Optional

# Set spawn context BEFORE rq is imported — rq's scheduler module uses
# get_context('fork') at import time, which fails on Windows.
_orig_get_context = multiprocessing.get_context
_spawn_ctx = _orig_get_context("spawn")


def _patched_get_context(method: str):
    if method == "fork":
        return _spawn_ctx
    return _orig_get_context(method)


multiprocessing.get_context = _patched_get_context

# Pre-load a stub for rq.scheduler BEFORE any rq sub-module is imported.
# rq/worker/__init__.py → rq/worker/base.py executes
#   "from ..scheduler import RQScheduler" at module scope.
# By injecting a stub into sys.modules first, Python finds it there and
# returns it instead of executing the real module body (which would hit the
# unpatched get_context).
_rq_scheduler_stub = types.ModuleType("rq.scheduler")
_rq_scheduler_stub.ForkProcess = _spawn_ctx.Process
_rq_scheduler_stub.RQScheduler = object  # placeholder; replaced below
sys.modules["rq.scheduler"] = _rq_scheduler_stub

import redis
from rq import Queue

# Remove the stub so reload() can re-import the real module from disk.
sys.modules.pop("rq.scheduler", None)
import rq.scheduler as _real_scheduler_mod
_real_scheduler = importlib.reload(_real_scheduler_mod)
_rq_scheduler_stub.ForkProcess = _real_scheduler.ForkProcess
_rq_scheduler_stub.RQScheduler = _real_scheduler.RQScheduler
from rq.job import Job

_queue: Optional[Queue] = None
_INLINE_JOB_STATUS: dict[str, str] = {}


def _inline_execution_enabled() -> bool:
    raw = str(os.environ.get("FITCV_CP_INLINE_EXECUTION", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _inline_start_delay_seconds() -> float:
    raw = str(os.environ.get("FITCV_CP_INLINE_START_DELAY_SECONDS", "0.05") or "0.05").strip()
    try:
        delay = float(raw)
    except ValueError:
        return 0.05
    return max(0.0, min(delay, 1.0))


def _run_inline_job(job_id: str, run_id: str, jobs_path: str, config_path: str) -> None:
    from fitcv_cp import worker_job  # noqa: F401
    from fitcv_cp.bq_store import append_event, update_run_status
    from fitcv_cp.models import RunEvent, RunStatus

    _INLINE_JOB_STATUS[job_id] = "started"
    try:
        worker_job.execute_pipeline_run(run_id=run_id, jobs_path=jobs_path, config_path=config_path)
        _INLINE_JOB_STATUS[job_id] = "finished"
    except Exception as exc:
        _INLINE_JOB_STATUS[job_id] = "failed"
        update_run_status(
            run_id,
            RunStatus.FAILED,
            bq=None,
            project="local",
            dataset="local",
            finished_at=datetime.now(timezone.utc),
            error_message=f"Inline execution failed: {exc}",
            error_stage="inline_execution",
        )
        append_event(
            RunEvent(
                run_id=run_id,
                event_id=str(uuid.uuid4()),
                stage="inline_execution",
                level="error",
                message=f"Inline execution failed: {exc}",
                created_at=datetime.now(timezone.utc),
            ),
            bq=None,
            project="local",
            dataset="local",
        )


def _run_inline_job_after_delay(job_id: str, run_id: str, jobs_path: str, config_path: str) -> None:
    delay_seconds = _inline_start_delay_seconds()
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    _run_inline_job(job_id, run_id, jobs_path, config_path)


def get_queue(redis_url: str = "redis://redis:6379/0") -> Queue:
    global _queue
    if _queue is None:
        conn = redis.from_url(redis_url)
        _queue = Queue("fitcv", connection=conn)
    return _queue


def enqueue_run_with_job_id(
    jobs_path: str,
    config_path: str,
    triggered_by: str,
    redis_url: str = "redis://redis:6379/0",
    run_id: Optional[str] = None,
) -> tuple[str, str]:
    """Enqueue a pipeline run. Returns (run_id, rq_job_id)."""
    if run_id is None:
        run_id = str(uuid.uuid4())
    if _inline_execution_enabled():
        job_id = f"inline-{uuid.uuid4()}"
        _INLINE_JOB_STATUS[job_id] = "queued"
        thread = threading.Thread(
            target=_run_inline_job_after_delay,
            args=(job_id, run_id, jobs_path, config_path),
            daemon=True,
        )
        thread.start()
        return run_id, job_id
    from fitcv_cp import worker_job  # noqa: F401
    q = get_queue(redis_url)
    job = q.enqueue(
        worker_job.execute_pipeline_run,
        run_id=run_id,
        jobs_path=jobs_path,
        config_path=config_path,
        job_timeout=3600,
    )
    return run_id, job.id


def enqueue_run(
    jobs_path: str,
    config_path: str,
    triggered_by: str,
    redis_url: str = "redis://redis:6379/0",
    run_id: Optional[str] = None,
) -> str:
    """Enqueue a pipeline run. Returns the run_id (backward-compatible wrapper)."""
    run_id, _job_id = enqueue_run_with_job_id(
        jobs_path=jobs_path,
        config_path=config_path,
        triggered_by=triggered_by,
        redis_url=redis_url,
        run_id=run_id,
    )
    return run_id


def cancel_queued_run(queue_job_id: str, redis_url: str = "redis://redis:6379/0") -> bool:
    """Attempt to cancel a queued RQ job before the worker claims it.

    Returns True if the job was successfully cancelled/removed.
    Returns False if the job was already claimed, missing, or not cancelable.
    """
    if queue_job_id.startswith("inline-"):
        state = _INLINE_JOB_STATUS.get(queue_job_id)
        if state == "queued":
            _INLINE_JOB_STATUS[queue_job_id] = "cancelled"
            return True
        return False
    from rq.exceptions import NoSuchJobError

    conn = redis.from_url(redis_url)
    try:
        job = Job.fetch(queue_job_id, connection=conn)
        job.cancel()
        return True
    except (NoSuchJobError, Exception):
        return False

def get_queue_job_status(queue_job_id: str, redis_url: str = "redis://redis:6379/0") -> str:
    """Return canonical queue job status for orchestration adapter usage."""
    if queue_job_id.startswith("inline-"):
        return _INLINE_JOB_STATUS.get(queue_job_id, "missing")
    from rq.exceptions import NoSuchJobError

    conn = redis.from_url(redis_url)
    try:
        job = Job.fetch(queue_job_id, connection=conn)
        return str(job.get_status(refresh=True) or "unknown")
    except NoSuchJobError:
        return "missing"
    except Exception:
        return "unknown"
