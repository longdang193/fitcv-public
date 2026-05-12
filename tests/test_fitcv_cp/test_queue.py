"""
@meta
type: test
scope: unit
domain: run_orchestration
covers:
  - control-plane queue behavior
excludes:
  - live RQ workers
tags:
  - fast
  - ci-safe
"""

from unittest.mock import MagicMock, patch
from fitcv_cp.queue import enqueue_run
import fitcv_cp.queue as queue_module


def test_enqueue_run_returns_uuid():
    """@proves admin_control_plane_core.rq-background-worker-integration
    @proves trigger_run_management.manual-checkpoints-and-continue
    @proves trigger_run_management.runs-list-management
    """
    mock_q = MagicMock()
    with patch("fitcv_cp.queue.get_queue", return_value=mock_q):
        with patch.dict("os.environ", {"FITCV_CP_INLINE_EXECUTION": "0"}):
            run_id = enqueue_run(
                jobs_path="data/sample_jobs.json",
                config_path=".env.yaml",
                triggered_by="admin",
                redis_url="redis://localhost:6379/0",
            )
    assert isinstance(run_id, str) and len(run_id) == 36
    mock_q.enqueue.assert_called_once()


# ── enqueue_run_with_job_id ──────────────────────────────────────────────────

def test_enqueue_run_with_job_id_returns_tuple():
    """@proves admin_control_plane_core.rq-background-worker-integration"""
    from fitcv_cp.queue import enqueue_run_with_job_id
    mock_q = MagicMock()
    mock_job = MagicMock()
    mock_job.id = "rq-job-abc"
    mock_q.enqueue.return_value = mock_job
    with patch("fitcv_cp.queue.get_queue", return_value=mock_q):
        with patch.dict("os.environ", {"FITCV_CP_INLINE_EXECUTION": "0"}):
            run_id, job_id = enqueue_run_with_job_id(
                jobs_path="data/jobs.json",
                config_path=".env.yaml",
                triggered_by="admin",
                redis_url="redis://localhost:6379/0",
            )
    assert isinstance(run_id, str) and len(run_id) == 36
    assert job_id == "rq-job-abc"


def test_enqueue_run_still_returns_str():
    """Existing enqueue_run() keeps returning a plain str (backward compat)."""
    from fitcv_cp.queue import enqueue_run
    mock_q = MagicMock()
    mock_job = MagicMock()
    mock_job.id = "rq-job-xyz"
    mock_q.enqueue.return_value = mock_job
    with patch("fitcv_cp.queue.get_queue", return_value=mock_q):
        with patch.dict("os.environ", {"FITCV_CP_INLINE_EXECUTION": "0"}):
            result = enqueue_run(
                jobs_path="data/jobs.json",
                config_path=".env.yaml",
                triggered_by="admin",
                redis_url="redis://localhost:6379/0",
            )
    assert isinstance(result, str) and len(result) == 36


# ── cancel_queued_run ────────────────────────────────────────────────────────

def test_cancel_queued_run_returns_true_when_cancelable():
    """@proves admin_control_plane_core.rq-background-worker-integration
    @proves run_lifecycle_controls.cancel-queued-runs-directly-from-the-queue-via-rq
    @proves trigger_run_management.manual-checkpoints-and-continue
    @proves trigger_run_management.run-detail-actions
    """
    from fitcv_cp.queue import cancel_queued_run
    mock_job = MagicMock()
    with patch("fitcv_cp.queue.Job.fetch", return_value=mock_job):
        result = cancel_queued_run("rq-job-abc", redis_url="redis://localhost:6379/0")
    assert result is True
    mock_job.cancel.assert_called_once()


def test_cancel_queued_run_returns_false_when_not_found():
    """@proves run_lifecycle_controls.cancel-queued-runs-directly-from-the-queue-via-rq
    @proves trigger_run_management.manual-checkpoints-and-continue
    """
    from fitcv_cp.queue import cancel_queued_run
    from rq.exceptions import NoSuchJobError
    with patch("fitcv_cp.queue.Job.fetch", side_effect=NoSuchJobError("rq-job-missing")):
        result = cancel_queued_run("rq-job-missing", redis_url="redis://localhost:6379/0")
    assert result is False


def test_inline_start_delay_seconds_bounds_values() -> None:
    with patch.dict("os.environ", {"FITCV_CP_INLINE_START_DELAY_SECONDS": "0.2"}, clear=False):
        assert queue_module._inline_start_delay_seconds() == 0.2
    with patch.dict("os.environ", {"FITCV_CP_INLINE_START_DELAY_SECONDS": "-4"}, clear=False):
        assert queue_module._inline_start_delay_seconds() == 0.0
    with patch.dict("os.environ", {"FITCV_CP_INLINE_START_DELAY_SECONDS": "bogus"}, clear=False):
        assert queue_module._inline_start_delay_seconds() == 0.05


def test_run_inline_job_after_delay_waits_before_execution() -> None:
    with patch("fitcv_cp.queue.time.sleep") as sleep_mock:
        with patch("fitcv_cp.queue._run_inline_job") as run_mock:
            with patch.dict("os.environ", {"FITCV_CP_INLINE_START_DELAY_SECONDS": "0.25"}, clear=False):
                queue_module._run_inline_job_after_delay(
                    "inline-job-1",
                    "run-1",
                    "data/jobs.json",
                    "config/env.yaml",
                )
    sleep_mock.assert_called_once_with(0.25)
    run_mock.assert_called_once_with(
        "inline-job-1",
        "run-1",
        "data/jobs.json",
        "config/env.yaml",
    )

