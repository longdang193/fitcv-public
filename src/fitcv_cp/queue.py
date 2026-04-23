"""
@meta
name: fitcv_cp_queue
type: utility
domain: run_orchestration
responsibility:
  - Own queue enqueue and cancel helpers for control-plane run execution.
  - Bridge manual continue requests into queued worker execution safely on Windows.
inputs:
  - queued run IDs and config paths
  - redis connection settings
outputs:
  - enqueued RQ jobs
  - queue cancellation outcomes
capabilities:
  - admin_control_plane_core.rq-background-worker-integration
  - run_lifecycle_controls.cancel-queued-runs-directly-from-the-queue-via-rq
  - trigger_run_management.runs-list-management
  - trigger_run_management.run-detail-actions
  - trigger_run_management.manual-checkpoints-and-continue
tags:
  - queue
  - orchestration
  - lineage-owner
lifecycle:
  status: active
"""
import importlib
import multiprocessing
import sys
import types
import uuid
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
    from fitcv_cp import worker_job  # noqa: F401

    if run_id is None:
        run_id = str(uuid.uuid4())
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
    from rq.exceptions import NoSuchJobError

    conn = redis.from_url(redis_url)
    try:
        job = Job.fetch(queue_job_id, connection=conn)
        job.cancel()
        return True
    except (NoSuchJobError, Exception):
        return False
