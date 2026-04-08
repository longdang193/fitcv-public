from unittest.mock import MagicMock, patch
from fitcv_cp.queue import enqueue_run


def test_enqueue_run_returns_uuid():
    mock_q = MagicMock()
    with patch("fitcv_cp.queue.get_queue", return_value=mock_q):
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
    from fitcv_cp.queue import enqueue_run_with_job_id
    mock_q = MagicMock()
    mock_job = MagicMock()
    mock_job.id = "rq-job-abc"
    mock_q.enqueue.return_value = mock_job
    with patch("fitcv_cp.queue.get_queue", return_value=mock_q):
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
        result = enqueue_run(
            jobs_path="data/jobs.json",
            config_path=".env.yaml",
            triggered_by="admin",
            redis_url="redis://localhost:6379/0",
        )
    assert isinstance(result, str) and len(result) == 36


# ── cancel_queued_run ────────────────────────────────────────────────────────

def test_cancel_queued_run_returns_true_when_cancelable():
    from fitcv_cp.queue import cancel_queued_run
    mock_job = MagicMock()
    with patch("fitcv_cp.queue.Job.fetch", return_value=mock_job):
        result = cancel_queued_run("rq-job-abc", redis_url="redis://localhost:6379/0")
    assert result is True
    mock_job.cancel.assert_called_once()


def test_cancel_queued_run_returns_false_when_not_found():
    from fitcv_cp.queue import cancel_queued_run
    from rq.exceptions import NoSuchJobError
    with patch("fitcv_cp.queue.Job.fetch", side_effect=NoSuchJobError("rq-job-missing")):
        result = cancel_queued_run("rq-job-missing", redis_url="redis://localhost:6379/0")
    assert result is False
