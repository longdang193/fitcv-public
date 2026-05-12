from __future__ import annotations

from unittest.mock import patch

from fitcv_cp.orchestrator import (
    OrchestrationAdapter,
    PrefectOrchestrationAdapter,
    get_orchestration_adapter,
)


def test_get_orchestration_adapter_defaults_to_queue() -> None:
    with patch.dict("os.environ", {}, clear=True):
        adapter = get_orchestration_adapter()
    assert isinstance(adapter, OrchestrationAdapter)
    assert adapter.name == "default_queue"


def test_get_orchestration_adapter_prefect_mode() -> None:
    with patch.dict("os.environ", {"FITCV_ORCHESTRATION_MODE": "prefect"}, clear=True):
        adapter = get_orchestration_adapter()
    assert isinstance(adapter, PrefectOrchestrationAdapter)
    assert adapter.name == "prefect"


def test_default_adapter_submit_cancel_status_and_continue() -> None:
    adapter = OrchestrationAdapter(name="default_queue")
    with patch("fitcv_cp.orchestrator.queue.enqueue_run_with_job_id", return_value=("run-1", "job-1")) as enqueue_mock, \
         patch("fitcv_cp.orchestrator.queue.cancel_queued_run", return_value=True) as cancel_mock, \
         patch("fitcv_cp.orchestrator.queue.get_queue_job_status", return_value="queued") as status_mock:
        submission = adapter.submit(
            jobs_path="jobs.json",
            config_path=".env.yaml",
            triggered_by="admin",
            redis_url="redis://localhost:6379/0",
        )
        continue_submission = adapter.continue_run(
            run_id="run-1",
            jobs_path="jobs.json",
            config_path=".env.yaml",
            triggered_by="admin",
            redis_url="redis://localhost:6379/0",
        )
        cancelled = adapter.cancel(queue_job_id="job-1", redis_url="redis://localhost:6379/0")
        status = adapter.status(queue_job_id="job-1", redis_url="redis://localhost:6379/0")

    assert submission.run_id == "run-1"
    assert submission.queue_job_id == "job-1"
    assert submission.backend == "queue"
    assert continue_submission.run_id == "run-1"
    assert cancelled is True
    assert status == "queued"
    assert enqueue_mock.call_count == 2
    cancel_mock.assert_called_once_with(queue_job_id="job-1", redis_url="redis://localhost:6379/0")
    status_mock.assert_called_once_with(queue_job_id="job-1", redis_url="redis://localhost:6379/0")


def test_prefect_adapter_preserves_submit_contract_with_prefect_backend_label() -> None:
    adapter = PrefectOrchestrationAdapter(name="prefect")
    with patch("fitcv_cp.orchestrator.queue.enqueue_run_with_job_id", return_value=("run-9", "job-9")):
        submission = adapter.submit(
            jobs_path="jobs.json",
            config_path=".env.yaml",
            triggered_by="admin",
            redis_url="redis://localhost:6379/0",
        )

    assert submission.run_id == "run-9"
    assert submission.queue_job_id == "job-9"
    assert submission.backend == "prefect"

def test_prefect_adapter_uses_prefect_api_when_configured() -> None:
    adapter = PrefectOrchestrationAdapter(name="prefect")
    with patch.dict(
        "os.environ",
        {"PREFECT_API_URL": "http://prefect.local/api", "PREFECT_DEPLOYMENT_ID": "dep-1"},
        clear=True,
    ), patch.object(
        PrefectOrchestrationAdapter, "_prefect_submit", return_value="flow-123"
    ) as submit_mock, patch(
        "fitcv_cp.orchestrator.queue.enqueue_run_with_job_id"
    ) as queue_submit_mock:
        submission = adapter.submit(
            run_id="run-55",
            jobs_path="jobs.json",
            config_path=".env.yaml",
            triggered_by="admin",
            redis_url="redis://localhost:6379/0",
        )

    assert submission.run_id == "run-55"
    assert submission.queue_job_id == "flow-123"
    assert submission.backend == "prefect"
    submit_mock.assert_called_once()
    queue_submit_mock.assert_not_called()

def test_prefect_adapter_falls_back_to_queue_when_prefect_submit_fails() -> None:
    adapter = PrefectOrchestrationAdapter(name="prefect")
    with patch.dict(
        "os.environ",
        {"PREFECT_API_URL": "http://prefect.local/api", "PREFECT_DEPLOYMENT_ID": "dep-1"},
        clear=True,
    ), patch.object(
        PrefectOrchestrationAdapter, "_prefect_submit", side_effect=RuntimeError("boom")
    ), patch(
        "fitcv_cp.orchestrator.queue.enqueue_run_with_job_id", return_value=("run-77", "job-77")
    ) as queue_submit_mock:
        submission = adapter.submit(
            run_id="run-77",
            jobs_path="jobs.json",
            config_path=".env.yaml",
            triggered_by="admin",
            redis_url="redis://localhost:6379/0",
        )

    assert submission.run_id == "run-77"
    assert submission.queue_job_id == "job-77"
    assert submission.backend == "prefect"
    queue_submit_mock.assert_called_once()

def test_prefect_adapter_status_and_cancel_use_prefect_when_available() -> None:
    adapter = PrefectOrchestrationAdapter(name="prefect")
    with patch.dict(
        "os.environ",
        {"PREFECT_API_URL": "http://prefect.local/api", "PREFECT_DEPLOYMENT_ID": "dep-1"},
        clear=True,
    ), patch.object(
        PrefectOrchestrationAdapter, "_prefect_status", return_value="started"
    ) as status_mock, patch.object(
        PrefectOrchestrationAdapter, "_prefect_set_cancelling", return_value=True
    ) as cancel_mock, patch(
        "fitcv_cp.orchestrator.queue.cancel_queued_run"
    ) as queue_cancel_mock, patch(
        "fitcv_cp.orchestrator.queue.get_queue_job_status"
    ) as queue_status_mock:
        status = adapter.status(queue_job_id="flow-1", redis_url="redis://localhost:6379/0")
        cancelled = adapter.cancel(queue_job_id="flow-1", redis_url="redis://localhost:6379/0")

    assert status == "started"
    assert cancelled is True
    status_mock.assert_called_once()
    cancel_mock.assert_called_once()
    queue_cancel_mock.assert_not_called()
    queue_status_mock.assert_not_called()
