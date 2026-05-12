import datetime
import json
from dataclasses import asdict
from typing import Any

from fitcv_cp.bq_store import (
    append_event,
    get_events,
    get_run,
    insert_run,
    update_run_results_export,
    update_run_stage_transition_artifacts,
)
import fitcv_cp.app as app_module
from fitcv_cp.models import PipelineRun, RunEvent, RunStatus


class _FakeQueryJob:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def result(self):
        return iter(self._rows)


class _FakeBigQuery:
    def __init__(self) -> None:
        self.pipeline_runs: dict[str, dict[str, Any]] = {}
        self.pipeline_events: dict[str, list[dict[str, Any]]] = {}

    def insert_rows_json(self, table: str, rows: list[dict[str, Any]]) -> list[Any]:
        if table.endswith(".pipeline_run_events"):
            for row in rows:
                run_id = str(row.get("run_id") or "")
                self.pipeline_events.setdefault(run_id, []).append(dict(row))
        return []

    def query(self, sql: str, job_config: Any):
        params = {param.name: param.value for param in (job_config.query_parameters or [])}
        run_id = str(params.get("run_id") or "")
        normalized_sql = " ".join(sql.split())
        if "FROM `p.d.pipeline_run_events`" in normalized_sql:
            rows = list(self.pipeline_events.get(run_id, []))
            rows.sort(key=lambda row: str(row.get("created_at") or ""))
            return _FakeQueryJob(rows)
        if "FROM `p.d.pipeline_runs`" in normalized_sql:
            row = self.pipeline_runs.get(run_id)
            return _FakeQueryJob([row] if row is not None else [])
        raise AssertionError(f"Unexpected query SQL in parity fake: {sql}")


def _make_run(run_id: str) -> PipelineRun:
    return PipelineRun(
        run_id=run_id,
        status=RunStatus.QUEUED,
        triggered_by="operator",
        trigger_source="web",
        jobs_path="data/sample_data_engineer_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.datetime.now(datetime.timezone.utc),
    )


def _normalize_run_for_contract(run: PipelineRun) -> dict[str, Any]:
    payload = asdict(run)
    payload["status"] = run.status.value
    for key in ("created_at", "started_at", "finished_at"):
        value = payload.get(key)
        if isinstance(value, datetime.datetime):
            payload[key] = value.isoformat()
    # Contract comparison scope for Wave 3 surfaces.
    return {
        "run_id": payload["run_id"],
        "status": payload["status"],
        "results_export_json": payload.get("results_export_json"),
        "stage_transition_artifacts_json": payload.get("stage_transition_artifacts_json"),
    }


def _normalize_events_for_contract(events: list[RunEvent]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for ev in events:
        normalized.append(
            {
                "run_id": ev.run_id,
                "event_id": ev.event_id,
                "stage": ev.stage,
                "level": ev.level,
                "message": ev.message,
                "payload_json": ev.payload_json,
            }
        )
    return normalized


def test_run_snapshot_contract_parity_sqlite_vs_bigquery(tmp_path, monkeypatch) -> None:
    run_id = "run-parity-1"
    run = _make_run(run_id)
    results_export_json = json.dumps({"jobs": [{"job_url": "https://example.com/1"}]}, ensure_ascii=False)
    stage_artifacts_json = json.dumps(
        {"artifacts": {"stages": {"enrich": {"status": "completed"}}}},
        ensure_ascii=False,
    )

    monkeypatch.setenv("FITCV_CP_SQLITE_PATH", str(tmp_path / "fitcv_cp.sqlite3"))
    insert_run(run, None, project="p", dataset="d")
    update_run_results_export(run_id, results_export_json, None, project="p", dataset="d")
    update_run_stage_transition_artifacts(run_id, stage_artifacts_json, None, project="p", dataset="d")
    sqlite_run = get_run(run_id, None, project="p", dataset="d")

    fake_bq = _FakeBigQuery()
    fake_bq.pipeline_runs[run_id] = {
        "run_id": run_id,
        "status": RunStatus.QUEUED.value,
        "triggered_by": "operator",
        "trigger_source": "web",
        "jobs_path": "data/sample_data_engineer_jobs.json",
        "config_path": ".env.yaml",
        "created_at": run.created_at,
        "results_export_json": results_export_json,
        "stage_transition_artifacts_json": stage_artifacts_json,
    }
    bq_run = get_run(run_id, fake_bq, project="p", dataset="d")

    assert sqlite_run is not None
    assert bq_run is not None
    assert _normalize_run_for_contract(sqlite_run) == _normalize_run_for_contract(bq_run)


def test_run_events_contract_parity_sqlite_vs_bigquery(tmp_path, monkeypatch) -> None:
    run_id = "run-parity-2"
    created_at = datetime.datetime.now(datetime.timezone.utc)
    event = RunEvent(
        run_id=run_id,
        event_id="ev-1",
        stage="enrich",
        level="info",
        message="enrichment complete",
        created_at=created_at,
        payload_json=json.dumps({"fresh": 1, "reused": 2}, ensure_ascii=False),
    )

    monkeypatch.setenv("FITCV_CP_LOCAL_EVENT_HISTORY_DIR", str(tmp_path / "events"))
    append_event(event, None, project="p", dataset="d")
    sqlite_events = get_events(run_id, None, project="p", dataset="d")

    fake_bq = _FakeBigQuery()
    append_event(event, fake_bq, project="p", dataset="d")
    bq_events = get_events(run_id, fake_bq, project="p", dataset="d")

    assert _normalize_events_for_contract(sqlite_events) == _normalize_events_for_contract(bq_events)


def test_enriched_tab_visibility_contract_parity_fallback_vs_structured(monkeypatch) -> None:
    run_id = "run-parity-enriched-1"
    results_export_json = json.dumps(
        {
            "results": [
                {
                    "job_url": "https://example.com/1",
                    "title": "Data Engineer",
                    "domain": "fintech",
                    "pipeline_status": "passed",
                }
            ]
        },
        ensure_ascii=False,
    )
    run = PipelineRun(
        run_id=run_id,
        status=RunStatus.SUCCEEDED,
        triggered_by="operator",
        trigger_source="web",
        jobs_path="data/sample_data_engineer_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.datetime.now(datetime.timezone.utc),
        results_export_json=results_export_json,
    )
    filter_rows = [{"job_url": "https://example.com/1", "passed": True, "reasons": []}]
    structured_row = {
        "job_url": "https://example.com/1",
        "title": "Data Engineer",
        "domain": "fintech",
        "job_family": "data_engineering",
        "location_type": "remote",
        "seniority": "senior",
    }

    # sqlite-like path: no run_structured_jobs rows -> fallback to results export rows
    monkeypatch.setattr(app_module, "list_run_structured_jobs", lambda *args, **kwargs: [])
    monkeypatch.setattr(app_module, "list_filter_results_for_run", lambda *args, **kwargs: filter_rows)
    sqlite_ctx = app_module._build_enriched_tab_context(
        run,
        run_id=run_id,
        project="p",
        dataset="d",
        bq=None,
        filter_name="all",
        query="",
        page=1,
        page_size=25,
    )

    # bigquery-like path: run_structured_jobs available
    monkeypatch.setattr(app_module, "list_run_structured_jobs", lambda *args, **kwargs: [structured_row])
    monkeypatch.setattr(app_module, "list_filter_results_for_run", lambda *args, **kwargs: filter_rows)
    bq_ctx = app_module._build_enriched_tab_context(
        run,
        run_id=run_id,
        project="p",
        dataset="d",
        bq=object(),
        filter_name="all",
        query="",
        page=1,
        page_size=25,
    )

    assert sqlite_ctx["enriched_total_count"] == 1
    assert bq_ctx["enriched_total_count"] == 1
    assert sqlite_ctx["enriched_passed_count"] == 1
    assert bq_ctx["enriched_passed_count"] == 1
    assert sqlite_ctx["enriched_jobs"][0]["job_url"] == bq_ctx["enriched_jobs"][0]["job_url"]


def test_artifact_bundle_contract_parity_sqlite_vs_bigquery(tmp_path, monkeypatch) -> None:
    run_id = "run-parity-artifacts-1"
    run = _make_run(run_id)
    results_export_json = json.dumps({"rows": [{"job_url": "https://example.com/1"}]}, ensure_ascii=False)
    stage_artifacts_json = json.dumps(
        {"created_at": "2026-05-04T00:00:00+00:00", "artifacts": {"stages": {"enrich": {"status": "completed"}}}},
        ensure_ascii=False,
    )

    monkeypatch.setenv("FITCV_CP_SQLITE_PATH", str(tmp_path / "fitcv_cp.sqlite3"))
    insert_run(run, None, project="p", dataset="d")
    update_run_results_export(run_id, results_export_json, None, project="p", dataset="d")
    update_run_stage_transition_artifacts(run_id, stage_artifacts_json, None, project="p", dataset="d")
    sqlite_run = get_run(run_id, None, project="p", dataset="d")

    fake_bq = _FakeBigQuery()
    fake_bq.pipeline_runs[run_id] = {
        "run_id": run_id,
        "status": RunStatus.QUEUED.value,
        "triggered_by": "operator",
        "trigger_source": "web",
        "jobs_path": "data/sample_data_engineer_jobs.json",
        "config_path": ".env.yaml",
        "created_at": run.created_at,
        "results_export_json": results_export_json,
        "stage_transition_artifacts_json": stage_artifacts_json,
    }
    bq_run = get_run(run_id, fake_bq, project="p", dataset="d")
    assert sqlite_run is not None and bq_run is not None

    sqlite_files = app_module._build_available_run_artifact_files(sqlite_run)
    bq_files = app_module._build_available_run_artifact_files(bq_run)
    sqlite_manifest = app_module._build_run_artifact_bundle_manifest(sqlite_run, sqlite_files)
    bq_manifest = app_module._build_run_artifact_bundle_manifest(bq_run, bq_files)

    assert sorted(file.filename for file in sqlite_files) == sorted(file.filename for file in bq_files)
    assert sqlite_manifest["artifact_states"] == bq_manifest["artifact_states"]
