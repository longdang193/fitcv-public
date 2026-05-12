import datetime

from fitcv_cp.models import PipelineRun, RunStatus
from fitcv_cp.models import RunEvent
from fitcv_cp.store import ControlPlaneStore


def _run() -> PipelineRun:
    return PipelineRun(
        run_id="rid-1",
        status=RunStatus.QUEUED,
        triggered_by="tester",
        trigger_source="web",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=datetime.datetime.now(datetime.timezone.utc),
    )


def test_control_plane_store_uses_injected_insert_fn() -> None:
    captured: dict[str, object] = {}

    def _insert(run, bq, *, project, dataset):
        captured["run"] = run
        captured["bq"] = bq
        captured["project"] = project
        captured["dataset"] = dataset

    store = ControlPlaneStore(
        bq=None,
        project="p",
        dataset="d",
        insert_run_fn=_insert,
    )
    store.insert_run(_run())
    assert captured["project"] == "p"
    assert captured["dataset"] == "d"
    assert captured["bq"] is None
    assert isinstance(captured["run"], PipelineRun)


def test_control_plane_store_uses_injected_binding_fn() -> None:
    captured: dict[str, object] = {}

    def _binding(run_id, *, queue_job_id, orchestration_backend, orchestration_run_id, bq, project, dataset):
        captured["run_id"] = run_id
        captured["queue_job_id"] = queue_job_id
        captured["backend"] = orchestration_backend
        captured["backend_run_id"] = orchestration_run_id
        captured["project"] = project
        captured["dataset"] = dataset

    store = ControlPlaneStore(
        bq=None,
        project="p",
        dataset="d",
        update_run_orchestration_binding_fn=_binding,
    )
    store.update_run_orchestration_binding(
        "rid-1",
        queue_job_id="q1",
        orchestration_backend="inline",
        orchestration_run_id="inline-1",
    )
    assert captured["run_id"] == "rid-1"
    assert captured["queue_job_id"] == "q1"
    assert captured["backend"] == "inline"


def test_control_plane_store_uses_injected_get_run_fn() -> None:
    run = _run()

    def _get(run_id, bq, *, project, dataset):
        assert run_id == run.run_id
        assert bq is None
        assert project == "p"
        assert dataset == "d"
        return run

    store = ControlPlaneStore(
        bq=None,
        project="p",
        dataset="d",
        get_run_fn=_get,
    )
    result = store.get_run(run.run_id)
    assert result is run


def test_control_plane_store_uses_injected_update_status_fn() -> None:
    captured: dict[str, object] = {}

    def _update(run_id, status, bq, *, project, dataset, **kwargs):
        captured["run_id"] = run_id
        captured["status"] = status
        captured["project"] = project
        captured["dataset"] = dataset
        captured["kwargs"] = kwargs

    store = ControlPlaneStore(
        bq=None,
        project="p",
        dataset="d",
        update_run_status_fn=_update,
    )
    store.update_run_status("rid-1", RunStatus.RUNNING, started_at=datetime.datetime.now(datetime.timezone.utc))
    assert captured["run_id"] == "rid-1"
    assert captured["status"] == RunStatus.RUNNING


def test_control_plane_store_uses_injected_archive_fn() -> None:
    captured: dict[str, object] = {}

    def _archive(run_id, archived_by, bq, *, project, dataset):
        captured["run_id"] = run_id
        captured["archived_by"] = archived_by
        captured["project"] = project
        captured["dataset"] = dataset

    store = ControlPlaneStore(
        bq=None,
        project="p",
        dataset="d",
        archive_run_fn=_archive,
    )
    store.archive_run("rid-1", "admin")
    assert captured["run_id"] == "rid-1"
    assert captured["archived_by"] == "admin"


def test_control_plane_store_uses_injected_cv_read_fns() -> None:
    def _list_cvs(run_id, bq, *, project, dataset):
        assert run_id == "rid-1"
        return [{"version_id": "v1"}]

    def _get_md(version_id, bq, *, project, dataset):
        assert version_id == "v1"
        return "# CV"

    store = ControlPlaneStore(
        bq=None,
        project="p",
        dataset="d",
        list_cvs_for_run_fn=_list_cvs,
        get_cv_markdown_fn=_get_md,
    )
    rows = store.list_cvs_for_run("rid-1")
    assert rows == [{"version_id": "v1"}]
    assert store.get_cv_markdown("v1") == "# CV"

def test_control_plane_store_uses_injected_pipeline_runs_schema_status_fn() -> None:
    def _schema_status(bq, *, project, dataset):
        assert bq is None
        assert project == "p"
        assert dataset == "d"
        return {"status": "complete", "missing_columns": [], "warning": None}

    store = ControlPlaneStore(
        bq=None,
        project="p",
        dataset="d",
        get_pipeline_runs_schema_status_fn=_schema_status,
    )
    status = store.get_pipeline_runs_schema_status()
    assert status["status"] == "complete"

def test_control_plane_store_uses_injected_event_and_snapshot_write_fns() -> None:
    captured: dict[str, object] = {}

    def _append(event, bq, *, project, dataset):
        captured["event_id"] = event.event_id
        return {"persistence_status": "persisted", "degradation_reason": "none"}

    def _effective(run_id, effective_settings_json, bq, *, project, dataset):
        captured["effective_run_id"] = run_id
        captured["effective_json"] = effective_settings_json

    def _synonyms(run_id, synonym_proposals_json, bq, *, project, dataset):
        captured["syn_run_id"] = run_id
        return {"persistence_status": "persisted", "degradation_reason": ""}

    def _cv_debug(run_id, cv_generation_debug_json, bq, *, project, dataset):
        captured["cv_debug_run_id"] = run_id

    def _insert_cv(row, bq, *, project, dataset):
        captured["cv_row_version_id"] = row.get("version_id")
        return []

    store = ControlPlaneStore(
        bq=None,
        project="p",
        dataset="d",
        append_event_fn=_append,
        update_run_effective_settings_fn=_effective,
        update_run_synonym_proposals_fn=_synonyms,
        update_run_cv_generation_debug_fn=_cv_debug,
        insert_cv_version_row_fn=_insert_cv,
    )
    event = RunEvent(
        run_id="rid-1",
        event_id="ev-1",
        stage="test",
        level="info",
        message="ok",
        created_at=datetime.datetime.now(datetime.timezone.utc),
    )
    response = store.append_event(event)
    store.update_run_effective_settings("rid-1", "{}")
    store.update_run_synonym_proposals("rid-1", "{}")
    store.update_run_cv_generation_debug("rid-1", "{}")
    store.insert_cv_version_row({"version_id": "v1"})

    assert response["persistence_status"] == "persisted"
    assert captured["effective_run_id"] == "rid-1"
    assert captured["syn_run_id"] == "rid-1"
    assert captured["cv_debug_run_id"] == "rid-1"
    assert captured["cv_row_version_id"] == "v1"
