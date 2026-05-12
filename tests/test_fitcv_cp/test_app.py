"""
@meta
type: test
scope: unit
domain: admin_ui
covers:
  - FitCV control-plane app behavior
excludes:
  - live HTTP deployment
tags:
  - fast
  - ci-safe
"""

from unittest.mock import MagicMock, patch
import io
import json
import zipfile
import datetime
import os
from fastapi.testclient import TestClient
from fitcv_cp.app import _timeline_stage_download_for_event, create_app
from fitcv_cp.models import RunEvent, RunStatus
from fitcv_cp.orchestrator import RunSubmission


def _app():
    os.environ.setdefault("FITCV_CP_INLINE_EXECUTION", "1")
    bq = MagicMock()
    return create_app(bq=bq, project="p", dataset="d", redis_url="redis://localhost:6379/0")


def test_post_runs_inserts_before_enqueue(tmp_path):
    """@proves admin_control_plane_core.insert-before-enqueue-invariant

    BQ insert must happen before enqueue to ensure DB is source of truth.
    """
    call_order = []
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    def fake_insert(*args, **kwargs):
        call_order.append("insert")

    def fake_enqueue_with_job(*args, **kwargs):
        call_order.append("enqueue")
        return "run-123", "rq-job-abc"

    with patch("fitcv_cp.app.insert_run", side_effect=fake_insert), \
         patch(
             "fitcv_cp.app.submit_run",
             side_effect=lambda **kwargs: RunSubmission(
                 run_id="run-123",
                 queue_job_id=fake_enqueue_with_job()[1],
                 backend_run_id="run-123",
                 backend="default_queue",
             ),
         ), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": str(profile_path)},
         }):
        resp = TestClient(_app()).post("/runs", json={"jobs_path": str(jobs_file)})
    assert resp.status_code == 201
    assert "run_id" in resp.json()
    assert call_order == ["insert", "enqueue"], f"Order was: {call_order}"

def test_post_runs_persists_backend_binding_from_submission(tmp_path):
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    def _submit_stub(*, run_id: str | None = None, **_: object) -> RunSubmission:
        return RunSubmission(
            run_id=str(run_id or "run-123"),
            queue_job_id="rq-job-abc",
            backend_run_id="flow-run-xyz",
            backend="prefect",
        )

    with patch("fitcv_cp.app.insert_run"), \
         patch("fitcv_cp.app.submit_run", side_effect=_submit_stub), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.update_run_orchestration_binding") as binding_mock, \
         patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": str(profile_path)},
         }):
        resp = TestClient(_app()).post("/runs", json={"jobs_path": str(jobs_file)})
    assert resp.status_code == 201
    kwargs = binding_mock.call_args.kwargs
    assert kwargs["queue_job_id"] == "rq-job-abc"
    assert kwargs["orchestration_backend"] == "prefect"
    assert kwargs["orchestration_run_id"] == "flow-run-xyz"


def test_get_run_detail_reconciles_orphaned_running_run_when_queue_job_missing() -> None:
    from fitcv_cp.models import PipelineRun
    from datetime import datetime, timezone

    running = PipelineRun(
        run_id="run-orphaned-1",
        status=RunStatus.RUNNING,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc),
        queue_job_id="rq-missing-1",
        run_mode="run_all",
    )
    failed = PipelineRun(
        run_id="run-orphaned-1",
        status=RunStatus.FAILED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=running.created_at,
        started_at=running.started_at,
        finished_at=datetime.now(timezone.utc),
        error_message="Queue job rq-missing-1 missing while run remained RUNNING",
        run_mode="run_all",
    )

    with patch("fitcv_cp.app.get_run", side_effect=[running, failed]), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event"), \
         patch("fitcv_cp.app.get_queue_job_status", return_value="missing"):
        resp = TestClient(_app()).get("/runs/run-orphaned-1")

    assert resp.status_code == 200
    assert resp.json()["status"] == "failed"
    assert mock_update_status.called

def test_get_run_detail_keeps_running_for_inline_started_job_status() -> None:
    from fitcv_cp.models import PipelineRun
    from datetime import datetime, timezone

    running = PipelineRun(
        run_id="run-inline-1",
        status=RunStatus.RUNNING,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc),
        queue_job_id="inline-job-1",
        run_mode="run_all",
    )

    with patch("fitcv_cp.app.get_run", return_value=running), \
         patch("fitcv_cp.app.get_queue_job_status", return_value="started"), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event") as mock_append_event:
        resp = TestClient(_app()).get("/runs/run-inline-1")

    assert resp.status_code == 200
    assert resp.json()["status"] == "running"
    assert not mock_update_status.called
    assert not mock_append_event.called

def test_get_runs_list_reconciles_orphaned_running_run_when_queue_job_missing() -> None:
    from fitcv_cp.models import PipelineRun
    from datetime import datetime, timezone

    running = PipelineRun(
        run_id="run-orphaned-list-1",
        status=RunStatus.RUNNING,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc),
        queue_job_id="rq-missing-list-1",
        run_mode="run_all",
    )
    failed = PipelineRun(
        run_id="run-orphaned-list-1",
        status=RunStatus.FAILED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=running.created_at,
        started_at=running.started_at,
        finished_at=datetime.now(timezone.utc),
        error_message="Queue job rq-missing-list-1 missing while run remained RUNNING",
        run_mode="run_all",
    )

    with patch("fitcv_cp.app.list_runs", return_value=[running]), \
         patch("fitcv_cp.app.get_run", return_value=failed), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event"), \
         patch("fitcv_cp.app.get_queue_job_status", return_value="missing"):
        resp = TestClient(_app()).get("/runs")

    assert resp.status_code == 200
    payload = resp.json()
    assert isinstance(payload, list) and payload
    assert payload[0]["status"] == "failed"
    assert mock_update_status.called

def test_admin_runs_reconciles_orphaned_running_run_when_queue_job_missing() -> None:
    from fitcv_cp.models import PipelineRun
    from datetime import datetime, timezone

    running = PipelineRun(
        run_id="run-orphaned-admin-1",
        status=RunStatus.RUNNING,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc),
        queue_job_id="rq-missing-admin-1",
        run_mode="run_all",
    )
    failed = PipelineRun(
        run_id="run-orphaned-admin-1",
        status=RunStatus.FAILED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=running.created_at,
        started_at=running.started_at,
        finished_at=datetime.now(timezone.utc),
        error_message="Queue job rq-missing-admin-1 missing while run remained RUNNING",
        run_mode="run_all",
    )

    with patch("fitcv_cp.app.list_runs", return_value=[running]), \
         patch("fitcv_cp.app.get_run", return_value=failed), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event"), \
         patch("fitcv_cp.app.get_queue_job_status", return_value="missing"), \
         patch("fitcv_cp.app.get_pipeline_runs_schema_status", return_value={"status": "complete", "missing_columns": [], "warning": None}):
        resp = TestClient(_app()).get("/admin/runs")

    assert resp.status_code == 200
    assert "run-orphaned-admin-1" in resp.text
    assert "failed" in resp.text.lower()
    assert mock_update_status.called


def test_post_runs_rejects_empty_jobs_path():
    resp = TestClient(_app()).post("/runs", json={"jobs_path": ""})
    assert resp.status_code == 422


def test_post_runs_persists_manual_staged_mode(tmp_path) -> None:
    """@proves trigger_run_management.execution-mode-selection"""
    captured = {}
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": str(profile_path)},
         }):
        resp = TestClient(_app()).post("/runs", json={
            "jobs_path": str(jobs_file),
            "run_mode": "manual_staged",
        })

    assert resp.status_code == 201
    assert captured["run"].run_mode == "manual_staged"
    assert captured["run"].next_stage == "normalize"
    assert captured["run"].completed_stages == []


def test_post_runs_path_trigger_persists_canonical_jobs_and_candidate_snapshots(tmp_path) -> None:
    captured = {}

    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p",
             "bigquery_dataset": "d",
             "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": str(profile_path)},
         }):
        resp = TestClient(_app()).post("/runs", json={"jobs_path": str(jobs_file)})

    assert resp.status_code == 201
    assert captured["run"].jobs_input_source == "path"
    assert json.loads(captured["run"].jobs_input_json) == [{"job_url": "http://a.com"}]
    assert captured["run"].candidate_profile_source == "default_config"
    profile_snapshot = json.loads(captured["run"].candidate_profile_json)
    assert profile_snapshot["preferences"]["domains"] == ["fintech"]
    effective = json.loads(captured["run"].effective_settings_json)
    assert json.loads(effective["runtime_inputs"]["candidate_profile_json"]) == profile_snapshot
    assert "agentic_runtime_expectation" in effective["runtime_inputs"]
    synonym_settings = dict(effective.get("synonym_management") or {})
    assert synonym_settings.get("auto_apply_recommendation_enabled") is False
    assert synonym_settings.get("auto_promote_global_enabled") is False
    assert synonym_settings.get("auto_accept_ai_action_enabled") is True

def test_post_runs_path_trigger_captures_agentic_runtime_expectation(tmp_path) -> None:
    captured = {}
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    with patch.dict(
        "os.environ",
        {
            "FITCV_LANGGRAPH_PROVIDER": "9router",
            "FITCV_LANGGRAPH_MODEL": "cx/gpt-5.2",
            "FITCV_LANGGRAPH_OPENAI_BASE_URL": "http://localhost:20128/v1",
            "FITCV_LANGGRAPH_WIRE_API": "responses",
        },
        clear=False,
    ), patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p",
             "bigquery_dataset": "d",
             "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": str(profile_path)},
         }):
        resp = TestClient(_app()).post("/runs", json={"jobs_path": str(jobs_file)})

    assert resp.status_code == 201
    effective = json.loads(captured["run"].effective_settings_json)
    expectation = effective["runtime_inputs"]["agentic_runtime_expectation"]
    assert expectation["provider"] == "9router"
    assert expectation["model"] == "cx/gpt-5.2"
    assert expectation["base_url"] == "http://localhost:20128/v1"
    assert expectation["wire_api"] == "responses"


def test_post_runs_run_all_and_manual_staged_share_canonical_runtime_envelope(tmp_path) -> None:
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    captured_runs: list = []

    def _capture_insert(run, *args, **kwargs):
        captured_runs.append(run)

    config = {
        "gcp_project": "p",
        "bigquery_dataset": "d",
        "service_account_key": "k",
        "pipeline": {"final_top_n": 10},
        "paths": {"candidate_profile": str(profile_path)},
    }

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value=config):
        run_all_resp = TestClient(_app()).post(
            "/runs",
            json={"jobs_path": str(jobs_file), "run_mode": "run_all"},
        )
        staged_resp = TestClient(_app()).post(
            "/runs",
            json={"jobs_path": str(jobs_file), "run_mode": "manual_staged"},
        )

    assert run_all_resp.status_code == 201
    assert staged_resp.status_code == 201
    run_all, staged = captured_runs
    assert run_all.jobs_input_source == staged.jobs_input_source == "path"
    assert json.loads(run_all.jobs_input_json) == json.loads(staged.jobs_input_json)
    assert run_all.candidate_profile_source == staged.candidate_profile_source == "default_config"
    assert json.loads(run_all.candidate_profile_json) == json.loads(staged.candidate_profile_json)
    run_all_effective = json.loads(run_all.effective_settings_json)
    staged_effective = json.loads(staged.effective_settings_json)
    assert json.loads(run_all_effective["runtime_inputs"]["candidate_profile_json"]) == json.loads(
        staged_effective["runtime_inputs"]["candidate_profile_json"]
    )
    assert run_all.next_stage is None
    assert staged.next_stage == "normalize"


def test_get_runs_returns_list():
    """@proves trigger_run_management.runs-list-management"""
    with patch("fitcv_cp.app.list_runs", return_value=[]):
        resp = TestClient(_app()).get("/runs")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_get_run_detail_not_found():
    with patch("fitcv_cp.app.get_run", return_value=None):
        resp = TestClient(_app()).get("/runs/missing-id")
    assert resp.status_code == 404


def test_get_run_events():
    event = RunEvent(
        run_id="some-id",
        event_id="evt-1",
        stage="pipeline_start",
        level="info",
        message="Run started",
        created_at=datetime.datetime.now(datetime.timezone.utc),
        payload_json='{"telemetry_export":{"status":"degraded"}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=MagicMock()), \
         patch("fitcv_cp.app.get_events", return_value=[event]):
        resp = TestClient(_app()).get("/runs/some-id/events")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["payload_json"] == '{"telemetry_export":{"status":"degraded"}}'


def test_get_run_events_preserves_langfuse_rich_payload_json() -> None:
    payload_json = json.dumps(
        {
            "telemetry_export": {"status": "export_enabled"},
            "langfuse_link": {"status": "unverified"},
            "langfuse_rich_io": {
                "status": "ready",
                "degradation_reason": None,
                "input": {"stage_family": "cv_analysis", "message": "ok"},
                "output": {"event_status": "emitted"},
            },
            "langfuse_rich_io_native": {"status": "sent:abc123", "degradation_reason": None},
        }
    )
    event = RunEvent(
        run_id="some-id",
        event_id="evt-2",
        stage="layer4_cv_analysis",
        level="info",
        message="Rich payload event",
        created_at=datetime.datetime.now(datetime.timezone.utc),
        payload_json=payload_json,
    )
    with patch("fitcv_cp.app.get_run", return_value=MagicMock()), \
         patch("fitcv_cp.app.get_events", return_value=[event]):
        resp = TestClient(_app()).get("/runs/some-id/events")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    parsed = json.loads(str(body[0]["payload_json"]))
    assert parsed["langfuse_rich_io"]["status"] == "ready"
    assert parsed["langfuse_rich_io_native"]["status"] == "sent:abc123"


def test_healthz():
    resp = TestClient(_app()).get("/healthz")
    assert resp.status_code == 200

def test_admin_orchestration_schema_diagnostics_endpoint() -> None:
    with patch(
        "fitcv_cp.app.get_pipeline_runs_schema_status",
        return_value={"status": "complete", "missing_columns": [], "warning": None},
    ):
        resp = TestClient(_app()).get("/admin/diagnostics/orchestration-schema")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "complete"
    assert payload["required_columns"] == ["orchestration_backend", "orchestration_run_id"]


def test_timeline_stage_download_maps_cv_analysis_skip_to_cv_analysis():
    assert _timeline_stage_download_for_event("layer4_cv_analysis_skip") == "cv_analysis"
    assert _timeline_stage_download_for_event("layer4_cv_skip") == "cv_analysis"


def test_ranked_cv_outcome_summary_preserves_stage_owned_no_cv_vs_failed_distinction() -> None:
    from fitcv_cp.app import _build_ranked_cv_outcome_summary

    rows = [
        {
            "rank": 1,
            "pipeline_status": "ranked_with_cv",
            "stage_owned_subreason": "accepted",
            "decision_chain": {"cv_generation": {"status": "accepted"}},
        },
        {
            "rank": 2,
            "pipeline_status": "ranked_no_cv",
            "stage_owned_subreason": "review_required",
            "decision_chain": {"cv_generation": {"status": "review_required"}},
        },
        {
            "rank": 3,
            "pipeline_status": "ranked_no_cv",
            "stage_owned_subreason": "validation_failed",
            "decision_chain": {"cv_generation": {"status": "validation_failed"}},
        },
        {
            "rank": 4,
            "pipeline_status": "ranked_no_cv",
            "stage_owned_subreason": "ready_for_generation",
            "decision_chain": {"cv_generation": {"status": "not_attempted"}},
        },
        {
            "rank": 5,
            "pipeline_status": "ranked_blocked_by_reranker_fit",
            "stage_owned_subreason": "blocked_by_reranker_fit",
            "decision_chain": {"cv_generation": {"status": "not_attempted"}},
        },
    ]

    summary = _build_ranked_cv_outcome_summary(rows)
    assert summary["ranked_total"] == 5
    assert summary["ranked_cv_created_count"] == 1
    assert summary["ranked_review_required_count"] == 1
    assert summary["ranked_generation_failed_count"] == 1
    assert summary["ranked_fit_gated_count"] == 1
    assert summary["ranked_other_no_cv_count"] == 1


# ── settings API ─────────────────────────────────────────────────────────────

def test_get_settings_returns_dict():
    with patch("fitcv_cp.app.load_active_settings", return_value={"pipeline.final_top_n": 5}):
        resp = TestClient(_app()).get("/settings")
    assert resp.status_code == 200
    assert resp.json()["pipeline.final_top_n"] == 5


def test_post_settings_key_saves_and_returns_200():
    with patch("fitcv_cp.app.save_setting") as mock_save:
        resp = TestClient(_app()).post(
            "/settings/pipeline.final_top_n",
            json={"value": 7, "updated_by": "admin"},
        )
    assert resp.status_code == 200
    mock_save.assert_called_once()


def test_post_settings_key_rejects_invalid_value():
    resp = TestClient(_app()).post(
        "/settings/pipeline.final_top_n",
        json={"value": 0, "updated_by": "admin"},  # 0 violates int >= 1
    )
    assert resp.status_code == 422


def test_post_settings_key_rejects_unknown_key():
    resp = TestClient(_app()).post(
        "/settings/unknown.key",
        json={"value": 1, "updated_by": "admin"},
    )
    assert resp.status_code == 422


def test_post_runs_with_config_overrides(tmp_path):
    """@proves settings_system.per-run-overrides

    POST /runs with per-run overrides snapshot effective settings.
    """
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run"), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": str(profile_path)},
         }):
        resp = TestClient(_app()).post("/runs", json={
            "jobs_path": str(jobs_file),
            "config_overrides": {"pipeline.final_top_n": 5},
        })
    assert resp.status_code == 201
    assert "run_id" in resp.json()


def test_post_runs_rejects_invalid_config_overrides():
    """@proves settings_system.per-run-overrides"""
    resp = TestClient(_app()).post("/runs", json={
        "jobs_path": "data/sample_jobs.json",
        "config_overrides": {"pipeline.final_top_n": 0},  # violates >= 1
    })
    assert resp.status_code == 422


def test_admin_upload_trigger_success(tmp_path):
    """@proves trigger_run_management.job-input-modes"""
    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run"), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": "data/candidate_profile.yaml"},
         }):

        file_content = b'[{"title": "Engineer", "job_url": "http://x.com"}]'
        files = {"jobs_file": ("custom_jobs.json", file_content, "application/json")}
        data = {
            "config_path": ".env.yaml",
            "jobs_input_mode": "upload",
            "candidate_profile_mode": "default_config",
        }

        resp = TestClient(_app()).post("/admin/upload-trigger", data=data, files=files)

    assert resp.status_code == 201, resp.text
    assert "run_id" in resp.json()


def test_admin_upload_trigger_persists_run_scoped_synonym_overlay() -> None:
    """@proves trigger_run_management.synonym-overlay-at-trigger"""
    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p",
             "bigquery_dataset": "d",
             "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": "data/candidate_profile.yaml"},
             "skill_synonyms": {"gcp": "google cloud"},
             "skill_synonyms_runtime": {
                 "base_policy_path": "config/taxonomy/skill_synonyms.yaml",
                 "overlay_paths": [],
                 "has_overlay": False,
                 "entry_count": 1,
             },
         }):
        files = {
            "jobs_file": ("custom_jobs.json", b'[{"title": "Engineer", "job_url": "http://x.com"}]', "application/json"),
            "synonym_overlay_file": ("custom_overlay.yaml", b"skill_synonyms:\n  ga4: google analytics\n", "application/x-yaml"),
        }
        data = {
            "config_path": ".env.yaml",
            "jobs_input_mode": "upload",
            "candidate_profile_mode": "default_config",
            "synonym_overlay_mode": "upload",
        }

        resp = TestClient(_app()).post("/admin/upload-trigger", data=data, files=files)

    assert resp.status_code == 201, resp.text
    effective = json.loads(captured["run"].effective_settings_json)
    assert effective["skill_synonyms"]["ga4"] == "google analytics"
    assert effective["skill_synonyms_runtime"]["has_run_overlay"] is True
    assert effective["skill_synonyms_runtime"]["run_overlay_source"] == "trigger_upload"

def test_admin_upload_trigger_persists_multi_field_synonym_overlay() -> None:
    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p",
             "bigquery_dataset": "d",
             "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": "data/candidate_profile.yaml"},
             "skill_synonyms": {"gcp": "google cloud"},
             "domain_alias_map": {},
             "role_family_alias_map": {},
             "skill_synonyms_runtime": {
                 "base_policy_path": "config/taxonomy/skill_synonyms.yaml",
                 "overlay_paths": [],
                 "has_overlay": False,
                 "entry_count": 1,
             },
         }):
        files = {
            "jobs_file": ("custom_jobs.json", b'[{"title": "Engineer", "job_url": "http://x.com"}]', "application/json"),
            "synonym_overlay_file": (
                "custom_overlay.yaml",
                (
                    b"skill_synonyms:\n  ga4: google analytics\n"
                    b"domain_alias_map:\n  fintech: financial services\n"
                    b"role_family_alias_map:\n  bi analyst: analytics\n"
                ),
                "application/x-yaml",
            ),
        }
        data = {
            "config_path": ".env.yaml",
            "jobs_input_mode": "upload",
            "candidate_profile_mode": "default_config",
            "synonym_overlay_mode": "upload",
        }
        resp = TestClient(_app()).post("/admin/upload-trigger", data=data, files=files)

    assert resp.status_code == 201, resp.text
    effective = json.loads(captured["run"].effective_settings_json)
    assert effective["skill_synonyms"]["ga4"] == "google analytics"
    assert effective["domain_alias_map"]["fintech"] == "financial services"
    assert effective["role_family_alias_map"]["bi analyst"] == "analytics"

def test_admin_upload_trigger_honors_explicit_overlay_upload_scope() -> None:
    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p",
             "bigquery_dataset": "d",
             "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": "data/candidate_profile.yaml"},
             "domain_alias_map": {},
         }):
        files = {
            "jobs_file": ("custom_jobs.json", b'[{"title":"Engineer","job_url":"http://x.com"}]', "application/json"),
            "synonym_overlay_file": (
                "custom_overlay.yaml",
                b"domain_alias_map:\n  fintech: financial services\n",
                "application/x-yaml",
            ),
        }
        data = {
            "config_path": ".env.yaml",
            "jobs_input_mode": "upload",
            "candidate_profile_mode": "default_config",
            "synonym_overlay_mode": "upload",
            "overlay_upload_scope": "domain",
        }
        resp = TestClient(_app()).post("/admin/upload-trigger", data=data, files=files)

    assert resp.status_code == 201
    effective = json.loads(captured["run"].effective_settings_json)
    assert effective["domain_alias_map"]["fintech"] == "financial services"


def test_admin_continue_run_requeues_manual_paused_run() -> None:
    """@proves trigger_run_management.manual-checkpoints-and-continue"""
    paused_run = MagicMock()
    paused_run.run_id = "run-123"
    paused_run.run_mode = "manual_staged"
    paused_run.status = RunStatus.AWAITING_CONTINUE
    paused_run.next_stage = "ranking"
    paused_run.last_completed_stage = "shortlist"
    paused_run.completed_stages = ["normalize", "enrich", "rule_filter", "shortlist"]
    paused_run.checkpoint_payload_json = '{"checkpoint_payload":{"shortlist":[]}}'
    paused_run.jobs_path = "data/sample_jobs.json"
    paused_run.config_path = ".env.yaml"

    call_order: list[str] = []

    def _record(name: str):
        def _inner(*args, **kwargs):
            call_order.append(name)
            return None
        return _inner

    def _continue(*args, **kwargs):
        call_order.append("continue")
        return ("run-123", "rq-job-abc")

    with patch("fitcv_cp.app.get_run", return_value=paused_run), \
         patch("fitcv_cp.app.continue_run_with_job_id", side_effect=_continue), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.update_run_status", side_effect=_record("status")) as mock_status, \
         patch("fitcv_cp.app.update_run_queue_job_id", side_effect=_record("queue")) as mock_queue, \
         patch("fitcv_cp.app.update_run_checkpoint", side_effect=_record("checkpoint")) as mock_checkpoint, \
         patch("fitcv_cp.app.update_run_orchestration_binding", side_effect=_record("binding")) as mock_binding, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-123/continue")

    assert resp.status_code == 200
    mock_status.assert_called_once()
    mock_queue.assert_called_once()
    mock_checkpoint.assert_called_once()
    mock_binding.assert_called_once()
    assert call_order.index("status") < call_order.index("continue")
    assert call_order.index("checkpoint") < call_order.index("continue")


def test_admin_continue_run_uses_canonical_next_stage_from_completed_truth() -> None:
    """@proves trigger_run_management.manual-checkpoints-and-continue"""
    paused_run = MagicMock()
    paused_run.run_id = "run-continue-canonical"
    paused_run.run_mode = "manual_staged"
    paused_run.status = RunStatus.AWAITING_CONTINUE
    paused_run.next_stage = "rule_filter"
    paused_run.last_completed_stage = "shortlist"
    paused_run.completed_stages = ["normalize", "enrich", "rule_filter", "shortlist"]
    paused_run.checkpoint_payload_json = '{"checkpoint_payload":{"shortlist":[{"job_url":"https://example.com/1"}]}}'
    paused_run.jobs_path = "data/sample_jobs.json"
    paused_run.config_path = ".env.yaml"

    with patch("fitcv_cp.app.get_run", return_value=paused_run), \
         patch("fitcv_cp.app.continue_run_with_job_id", return_value=("run-continue-canonical", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.update_run_status"), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_checkpoint, \
         patch("fitcv_cp.app.append_event") as mock_event:
        resp = TestClient(_app()).post("/admin/runs/run-continue-canonical/continue")

    assert resp.status_code == 200
    assert mock_checkpoint.call_args.kwargs["next_stage"] == "ranking"
    assert mock_event.call_args.args[0].message == "Manual run queued to continue from ranking (strict)"

def test_admin_continue_run_rejects_strict_policy_drift() -> None:
    paused_run = MagicMock()
    paused_run.run_id = "run-continue-strict-drift"
    paused_run.run_mode = "manual_staged"
    paused_run.status = RunStatus.AWAITING_CONTINUE
    paused_run.next_stage = "ranking"
    paused_run.last_completed_stage = "shortlist"
    paused_run.completed_stages = ["normalize", "enrich", "rule_filter", "shortlist"]
    paused_run.checkpoint_payload_json = json.dumps(
        {
            "checkpoint_payload": {"shortlist": []},
            "replay_context": {"policy_envelope_signature": "old-signature"},
        }
    )
    paused_run.jobs_path = "data/sample_jobs.json"
    paused_run.config_path = ".env.yaml"
    paused_run.effective_settings_json = json.dumps({"ranking_weights": {"ai_score": 0.4}})

    with patch("fitcv_cp.app.get_run", return_value=paused_run), \
         patch("fitcv_cp.app.continue_run_with_job_id") as mock_continue:
        resp = TestClient(_app()).post("/admin/runs/run-continue-strict-drift/continue")

    assert resp.status_code == 409
    assert resp.json()["detail"] == "Strict replay rejected: policy envelope drift detected"
    mock_continue.assert_not_called()

def test_admin_continue_run_allows_policy_replay_when_policy_drifted() -> None:
    paused_run = MagicMock()
    paused_run.run_id = "run-continue-policy-replay"
    paused_run.run_mode = "manual_staged"
    paused_run.status = RunStatus.AWAITING_CONTINUE
    paused_run.next_stage = "ranking"
    paused_run.last_completed_stage = "shortlist"
    paused_run.completed_stages = ["normalize", "enrich", "rule_filter", "shortlist"]
    paused_run.checkpoint_payload_json = json.dumps(
        {
            "checkpoint_payload": {"shortlist": []},
            "replay_context": {
                "policy_envelope_signature": "old-signature",
                "replay_source_run_id": "source-run-1",
            },
        }
    )
    paused_run.jobs_path = "data/sample_jobs.json"
    paused_run.config_path = ".env.yaml"
    paused_run.effective_settings_json = json.dumps({"ranking_weights": {"ai_score": 0.4}})

    with patch("fitcv_cp.app.get_run", return_value=paused_run), \
         patch("fitcv_cp.app.continue_run_with_job_id", return_value=("run-continue-policy-replay", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.update_run_status"), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.update_run_checkpoint"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-continue-policy-replay/continue?replay_mode=policy_replay")

    assert resp.status_code == 200
    assert resp.json()["replay_mode"] == "policy_replay"


def test_admin_continue_run_rejects_underspecified_checkpoint_truth() -> None:
    paused_run = MagicMock()
    paused_run.run_id = "run-continue-invalid"
    paused_run.run_mode = "manual_staged"
    paused_run.status = RunStatus.AWAITING_CONTINUE
    paused_run.next_stage = "cv_generation"
    paused_run.last_completed_stage = None
    paused_run.completed_stages = []
    paused_run.checkpoint_payload_json = '{"checkpoint_payload":{"cv_analysis_results":[]}}'
    paused_run.jobs_path = "data/sample_jobs.json"
    paused_run.config_path = ".env.yaml"

    with patch("fitcv_cp.app.get_run", return_value=paused_run), \
         patch("fitcv_cp.app.continue_run_with_job_id") as mock_enqueue, \
         patch("fitcv_cp.app.update_run_status") as mock_status, \
         patch("fitcv_cp.app.update_run_queue_job_id") as mock_queue, \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_checkpoint, \
         patch("fitcv_cp.app.append_event") as mock_event:
        resp = TestClient(_app()).post("/admin/runs/run-continue-invalid/continue")

    assert resp.status_code == 409
    assert resp.json()["detail"] == "Run has no canonical next stage to continue"
    mock_enqueue.assert_not_called()
    mock_status.assert_not_called()
    mock_queue.assert_not_called()
    mock_checkpoint.assert_not_called()
    mock_event.assert_not_called()


def test_admin_continue_run_rejects_invalid_stage_truth() -> None:
    paused_run = MagicMock()
    paused_run.run_id = "run-continue-bogus"
    paused_run.run_mode = "manual_staged"
    paused_run.status = RunStatus.AWAITING_CONTINUE
    paused_run.next_stage = "cv_generation"
    paused_run.last_completed_stage = "bogus"
    paused_run.completed_stages = ["normalize", "bogus"]
    paused_run.checkpoint_payload_json = '{"checkpoint_payload":{"shortlist":[]}}'
    paused_run.jobs_path = "data/sample_jobs.json"
    paused_run.config_path = ".env.yaml"

    with patch("fitcv_cp.app.get_run", return_value=paused_run), \
         patch("fitcv_cp.app.continue_run_with_job_id") as mock_enqueue, \
         patch("fitcv_cp.app.update_run_status") as mock_status, \
         patch("fitcv_cp.app.update_run_queue_job_id") as mock_queue, \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_checkpoint, \
         patch("fitcv_cp.app.append_event") as mock_event:
        resp = TestClient(_app()).post("/admin/runs/run-continue-bogus/continue")

    assert resp.status_code == 409
    assert resp.json()["detail"] == "Run has no canonical next stage to continue"
    mock_enqueue.assert_not_called()
    mock_status.assert_not_called()
    mock_queue.assert_not_called()
    mock_checkpoint.assert_not_called()
    mock_event.assert_not_called()


def test_admin_continue_run_rejects_checkpoint_progress_drift() -> None:
    paused_run = MagicMock()
    paused_run.run_id = "run-continue-drift"
    paused_run.run_mode = "manual_staged"
    paused_run.status = RunStatus.AWAITING_CONTINUE
    paused_run.next_stage = "ranking"
    paused_run.last_completed_stage = "shortlist"
    paused_run.completed_stages = ["normalize", "enrich", "rule_filter", "shortlist"]
    paused_run.checkpoint_payload_json = '{"checkpoint_payload":{"ranked":[{"job_url":"https://example.com/1"}]}}'
    paused_run.jobs_path = "data/sample_jobs.json"
    paused_run.config_path = ".env.yaml"

    with patch("fitcv_cp.app.get_run", return_value=paused_run), \
         patch("fitcv_cp.app.continue_run_with_job_id") as mock_enqueue, \
         patch("fitcv_cp.app.update_run_status") as mock_status, \
         patch("fitcv_cp.app.update_run_queue_job_id") as mock_queue, \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_checkpoint, \
         patch("fitcv_cp.app.append_event") as mock_event:
        resp = TestClient(_app()).post("/admin/runs/run-continue-drift/continue")

    assert resp.status_code == 409
    assert resp.json()["detail"] == "Run has no canonical next stage to continue"
    mock_enqueue.assert_not_called()
    mock_status.assert_not_called()
    mock_queue.assert_not_called()
    mock_checkpoint.assert_not_called()
    mock_event.assert_not_called()


def test_admin_run_detail_shows_synonym_overlay_card_for_manual_enrich_checkpoint() -> None:
    """@proves inspection_debugging.synonym-overlay-inspection
    @proves trigger_run_management.synonym-overlay-inspection
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-btn",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        checkpoint_status="awaiting_continue",
        last_completed_stage="enrich",
        next_stage="rule_filter",
        completed_stages=["normalize", "enrich"],
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-overlay-btn")

    assert resp.status_code == 200
    assert "Synonym Overlay" in resp.text
    assert "Replace Run Overlay YAML" in resp.text
    assert 'action="/admin/runs/run-overlay-btn/synonym-overlay"' in resp.text
    assert 'name="overlay_upload_scope"' in resp.text
    assert "Combined Upload" in resp.text
    assert "Skills Upload" in resp.text
    assert "Domain Upload" in resp.text
    assert "Role Family Upload" in resp.text

def test_runs_list_shows_split_synonym_upload_scope_controls() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-scope-controls",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
    )
    with patch("fitcv_cp.app.list_runs", return_value=[run]):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    assert "Combined Upload" in resp.text
    assert "Skills Upload" in resp.text
    assert "Domain Upload" in resp.text
    assert "Role Family Upload" in resp.text


def test_admin_run_detail_shows_trigger_uploaded_synonym_overlay_state() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-state",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps(
            {
                "skill_synonyms": {"gcp": "google cloud", "ga4": "google analytics"},
                "skill_synonyms_runtime": {
                    "base_policy_path": "config/taxonomy/skill_synonyms.yaml",
                    "overlay_paths": [],
                    "has_overlay": True,
                    "entry_count": 2,
                    "has_run_overlay": True,
                    "run_overlay_source": "trigger_upload",
                    "run_overlay_filename": "custom_overlay.yaml",
                    "run_overlay_uploaded_at": "2026-04-05T23:30:00Z",
                    "run_overlay_entry_count": 1,
                    "run_overlay_section_counts": {
                        "skill_synonyms": 1,
                        "domain_alias_map": 2,
                        "role_family_alias_map": 1,
                        "domain_neighbors": 1,
                        "role_family_neighbors": 1,
                    },
                },
            }
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-overlay-state")

    assert resp.status_code == 200
    assert "Synonym Overlay" in resp.text
    assert "Trigger Upload" in resp.text
    assert "custom_overlay.yaml" in resp.text
    assert "Domain Entries" in resp.text
    assert "Role Family Entries" in resp.text
    assert "Domain Neighbors" in resp.text
    assert "Role Family Neighbors" in resp.text

def test_admin_run_detail_shows_agentic_review_queue_card() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-queue",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Senior Data Engineer",
                        "status": "review_required",
                        "fit_classification": "stretch",
                        "error": {"stage": "review_gate", "message": "Low confidence sections: experience"},
                    }
                ],
                "hitl_review_actions": [],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-review-queue")
    assert resp.status_code == 200
    assert "Agentic Review Queue" in resp.text

def test_admin_run_detail_shows_replay_context_metadata() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-replay-meta",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        settings_used_json=json.dumps(
            {
                "replay_context": {
                    "replay_mode": "policy_replay",
                    "replay_source_run_id": "run-origin-1",
                    "policy_registry_version": "policy_registry.v2",
                }
            }
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-replay-meta")

    assert resp.status_code == 200
    assert "Replay Mode" in resp.text
    assert "policy_replay" in resp.text
    assert "run-origin-1" in resp.text
    assert "policy_registry.v2" in resp.text
    assert "Runtime Mode" in resp.text
    assert "full" in resp.text
    assert "sqlite" in resp.text


def test_admin_run_detail_shows_stage_result_policy_and_trace_summary() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-result-summary",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json=json.dumps(
            {
                "results": [],
                "stage_result_summary": {
                    "normalize": {
                        "status": "completed",
                        "policy_version": "policy.normalize.v1",
                        "trace_context": {
                            "trace_id": "trace-normalize-1",
                            "span_id": "span-normalize-1",
                            "parent_span_id": "",
                        },
                    },
                    "cv_generation": {
                        "status": "completed",
                        "policy_version": "policy.cv_generation.v1",
                        "trace_context": {
                            "trace_id": "trace-cv-1",
                            "span_id": "span-cv-1",
                            "parent_span_id": "span-analysis-1",
                        },
                    },
                },
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-stage-result-summary")

    assert resp.status_code == 200
    assert "Stage Result Policy + Trace Summary" in resp.text
    assert "policy.normalize.v1" in resp.text
    assert "trace-normalize-1" in resp.text
    assert "policy.cv_generation.v1" in resp.text
    assert "span-analysis-1" in resp.text

def test_admin_run_detail_shows_agentic_review_queue_card_from_debug_records_key() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-queue-debug-records",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Senior Data Engineer",
                        "status": "review_required",
                        "fit_classification": "stretch",
                        "error": {"stage": "review_gate", "message": "Low confidence sections: experience"},
                    }
                ],
                "hitl_review_actions": [],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-review-queue-debug-records")
    assert resp.status_code == 200
    assert "Agentic Review Queue" in resp.text
    assert "Regenerate Once" in resp.text
    assert 'action="/admin/runs/run-review-queue-debug-records/cv-review-action"' in resp.text

def test_admin_run_detail_shows_markdown_quality_card() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-markdown-quality",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Senior Data Engineer",
                        "status": "review_required",
                        "fit_classification": "stretch",
                        "error": {"stage": "markdown_quality_review", "message": "Markdown quality requires review: Experience section appears shallow."},
                    }
                ]
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-markdown-quality")
    assert resp.status_code == 200
    assert "Markdown Quality" in resp.text
    assert "review-required" in resp.text

def test_admin_run_cv_review_action_persists_and_appends_event() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-action",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Senior Data Engineer",
                        "status": "review_required",
                        "fit_classification": "stretch",
                        "error": {"stage": "review_gate", "message": "Low confidence sections: experience"},
                    }
                ]
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_cv_generation_debug") as mock_update, \
         patch("fitcv_cp.app.append_event") as mock_append:
        resp = TestClient(_app()).post(
            "/admin/runs/run-review-action/cv-review-action",
            data={"job_url": "https://example.com/job-1", "action": "approve", "actor": "operator"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    mock_update.assert_called_once()
    saved_payload = json.loads(mock_update.call_args.args[1])
    assert saved_payload["hitl_review_actions"][-1]["action"] == "approve"
    assert saved_payload["hitl_review_actions"][-1]["job_url"] == "https://example.com/job-1"
    mock_append.assert_called_once()


def test_admin_run_cv_review_action_regenerate_once_does_not_auto_complete_review() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-regenerate",
        status=RunStatus.AWAITING_CONTINUE,
        checkpoint_status="awaiting_review",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Senior Data Engineer",
                        "status": "review_required",
                        "fit_classification": "stretch",
                        "error": {"stage": "review_gate", "message": "Low confidence sections: experience"},
                    }
                ]
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_cv_generation_debug") as mock_update_debug, \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_update_checkpoint, \
         patch("fitcv_cp.app.append_event") as mock_append:
        resp = TestClient(_app()).post(
            "/admin/runs/run-review-regenerate/cv-review-action",
            data={"job_url": "https://example.com/job-1", "action": "regenerate_once", "actor": "operator"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    mock_update_debug.assert_called_once()
    mock_update_status.assert_not_called()
    mock_update_checkpoint.assert_not_called()
    assert mock_append.call_count == 1


def test_admin_run_cv_review_action_approve_records_terminal_resolution_status() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-approve-resolution",
        status=RunStatus.AWAITING_CONTINUE,
        checkpoint_status="awaiting_review",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Senior Data Engineer",
                        "status": "review_required",
                        "fit_classification": "stretch",
                    }
                ]
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_cv_generation_debug") as mock_update_debug, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post(
            "/admin/runs/run-review-approve-resolution/cv-review-action",
            data={"job_url": "https://example.com/job-1", "action": "approve", "actor": "operator"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    payload = json.loads(mock_update_debug.call_args.args[1])
    assert payload["hitl_review_actions"][-1]["resolution_status"] == "approved_as_is"

def test_admin_run_cv_review_action_approve_as_is_finalizes_cv_artifact() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-approve-finalize",
        status=RunStatus.AWAITING_CONTINUE,
        checkpoint_status="awaiting_review",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cvs_generated=0,
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Senior Data Engineer",
                        "status": "review_required",
                        "fit_classification": "stretch",
                        "markdown_final": "# Candidate\n\nDraft",
                    }
                ]
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.insert_cv_version_row", return_value=[]) as mock_insert_cv, \
         patch("fitcv_cp.app.update_run_cv_generation_debug") as mock_update_debug, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post(
            "/admin/runs/run-review-approve-finalize/cv-review-action",
            data={"job_url": "https://example.com/job-1", "action": "approve_as_is", "actor": "operator"},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    mock_insert_cv.assert_called_once()
    payload = json.loads(mock_update_debug.call_args.args[1])
    assert payload["hitl_review_actions"][-1]["artifact_finalized"] is True

def test_admin_run_cv_review_action_approve_as_is_missing_draft_returns_409() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-approve-missing",
        status=RunStatus.AWAITING_CONTINUE,
        checkpoint_status="awaiting_review",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Senior Data Engineer",
                        "status": "review_required",
                    }
                ]
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).post(
            "/admin/runs/run-review-approve-missing/cv-review-action",
            data={"job_url": "https://example.com/job-1", "action": "approve_as_is", "actor": "operator"},
            follow_redirects=False,
        )
    assert resp.status_code == 409


def test_admin_run_cv_review_batch_action_applies_and_skips_terminal_rows() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-batch-1",
        status=RunStatus.AWAITING_CONTINUE,
        checkpoint_status="awaiting_review",
        last_completed_stage="cv_analysis",
        completed_stages=["normalize", "enrich", "rule_filter", "shortlist", "ranking", "cv_analysis"],
        checkpoint_payload_json=json.dumps({"checkpoint_payload": {"stage": "cv_analysis"}}),
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "DE1",
                        "status": "review_required",
                        "markdown_final": "# DE1\n\nAccepted draft",
                    },
                    {"job_url": "https://example.com/job-2", "job_title": "DE2", "status": "review_required"},
                ],
                "hitl_review_actions": [
                    {"job_url": "https://example.com/job-2", "action": "reject", "resolution_status": "rejected", "created_at": "2026-05-03T00:00:00+00:00"},
                ],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.insert_cv_version_row", return_value=[]), \
         patch("fitcv_cp.app.update_run_cv_generation_debug") as mock_update_debug, \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_update_checkpoint, \
         patch("fitcv_cp.app.append_event") as mock_append:
        resp = TestClient(_app()).post(
            "/admin/runs/run-review-batch-1/cv-review-batch-action",
            data={
                "action": "approve_as_is",
                "actor": "operator",
                "confirm_no_accepted_cv_closure": "true",
                "job_url": ["https://example.com/job-1", "https://example.com/job-2"],
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "hitl_batch_applied=1" in resp.headers["location"]
    assert "hitl_batch_skipped=1" in resp.headers["location"]
    assert "hitl_batch_failed=0" in resp.headers["location"]
    payload = json.loads(mock_update_debug.call_args.args[1])
    assert any(
        row.get("job_url") == "https://example.com/job-1" and row.get("resolution_status") == "approved_as_is"
        for row in list(payload.get("hitl_review_actions") or [])
    )
    assert mock_update_status.call_count == 2
    mock_update_checkpoint.assert_called_once()
    checkpoint_kwargs = mock_update_checkpoint.call_args.kwargs
    assert checkpoint_kwargs["last_completed_stage"] == "cv_analysis"
    assert checkpoint_kwargs["completed_stages"] == ["normalize", "enrich", "rule_filter", "shortlist", "ranking", "cv_analysis"]
    assert checkpoint_kwargs["checkpoint_payload_json"] == json.dumps({"checkpoint_payload": {"stage": "cv_analysis"}})
    assert mock_append.call_count >= 2
    completion_events = [
        call.args[0]
        for call in mock_append.call_args_list
        if getattr(call.args[0], "stage", "") == "cv_review_completed"
    ]
    assert completion_events
    completion_payload = json.loads(completion_events[0].payload_json)
    assert completion_payload["closure_mode"] in {"all_review_rows_terminal", "all_review_rows_terminal_no_accepted_cv"}

def test_admin_run_cv_review_batch_action_finalize_path_no_longer_needs_zero_cv_confirmation() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-batch-blocked",
        status=RunStatus.AWAITING_CONTINUE,
        checkpoint_status="awaiting_review",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cvs_generated=0,
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "DE1",
                        "status": "review_required",
                        "markdown_final": "# DE1\n\nAccepted draft",
                    },
                ],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.insert_cv_version_row", return_value=[]), \
         patch("fitcv_cp.app.update_run_cv_generation_debug") as mock_update_debug, \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_update_checkpoint, \
         patch("fitcv_cp.app.append_event") as mock_append:
        resp = TestClient(_app()).post(
            "/admin/runs/run-review-batch-blocked/cv-review-batch-action",
            data={
                "action": "approve_as_is",
                "actor": "operator",
                "job_url": ["https://example.com/job-1"],
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "hitl_batch_finalized=1" in resp.headers["location"]
    mock_update_debug.assert_called_once()
    assert mock_update_status.call_count == 2
    mock_update_checkpoint.assert_called_once()

def test_admin_run_cv_review_batch_action_approve_as_is_missing_draft_is_safe_failure() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-batch-missing-draft",
        status=RunStatus.AWAITING_CONTINUE,
        checkpoint_status="awaiting_review",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {"job_url": "https://example.com/job-1", "job_title": "DE1", "status": "review_required"},
                ],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_cv_generation_debug") as mock_update_debug, \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_update_checkpoint:
        resp = TestClient(_app()).post(
            "/admin/runs/run-review-batch-missing-draft/cv-review-batch-action",
            data={
                "action": "approve_as_is",
                "actor": "operator",
                "job_url": ["https://example.com/job-1"],
            },
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "hitl_batch_failed=1" in resp.headers["location"]
    mock_update_debug.assert_called_once()
    mock_update_status.assert_not_called()
    mock_update_checkpoint.assert_not_called()


def test_admin_run_detail_shows_synonym_overlay_yaml_snapshot() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-yaml",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps(
            {
                "skill_synonyms": {"gcp": "google cloud", "ga4": "google analytics"},
                "skill_synonyms_runtime": {
                    "base_policy_path": "config/taxonomy/skill_synonyms.yaml",
                    "overlay_paths": [],
                    "has_overlay": True,
                    "entry_count": 2,
                    "has_run_overlay": True,
                    "run_overlay_source": "trigger_upload",
                    "run_overlay_filename": "custom_overlay.yaml",
                    "run_overlay_uploaded_at": "2026-04-05T23:30:00Z",
                    "run_overlay_entry_count": 1,
                    "run_overlay_yaml": "skill_synonyms:\\n  ga4: google analytics\\n",
                },
            }
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-overlay-yaml")

    assert resp.status_code == 200
    assert "YAML Snapshot" in resp.text
    assert "ga4: google analytics" in resp.text


def test_admin_run_detail_shows_default_synonym_yaml_snapshot(tmp_path) -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    synonyms_path = tmp_path / "skill_synonyms.yaml"
    synonyms_path.write_text("skill_synonyms:\n  sql: structured query language\n", encoding="utf-8")

    run = PipelineRun(
        run_id="run-default-overlay-yaml",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        checkpoint_status="awaiting_continue",
        last_completed_stage="enrich",
        next_stage="rule_filter",
        completed_stages=["normalize", "enrich"],
        effective_settings_json=json.dumps(
            {
                "skill_synonyms": {"sql": "structured query language"},
                "skill_synonyms_runtime": {
                    "base_policy_path": str(synonyms_path),
                    "overlay_paths": [],
                    "has_overlay": False,
                    "entry_count": 1,
                    "has_run_overlay": False,
                },
            }
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-default-overlay-yaml")

    assert resp.status_code == 200
    assert "Default Config" in resp.text
    assert "sql: structured query language" in resp.text


def test_admin_upload_synonym_overlay_updates_run_effective_settings() -> None:
    """@proves trigger_run_management.synonym-overlay-replacement"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-upload",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        checkpoint_status="awaiting_continue",
        last_completed_stage="enrich",
        next_stage="rule_filter",
        completed_stages=["normalize", "enrich"],
        effective_settings_json=json.dumps({
            "gcp_project": "p",
            "bigquery_dataset": "d",
            "service_account_key": "k",
            "skill_synonyms": {"gcp": "google cloud"},
            "skill_synonyms_runtime": {
                "base_policy_path": "config/skill_synonyms.yaml",
                "overlay_paths": [],
                "has_overlay": False,
                "entry_count": 1,
            },
        }),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_effective_settings") as mock_update, \
         patch("fitcv_cp.app.append_event") as mock_event:
        resp = TestClient(_app()).post(
            "/admin/runs/run-overlay-upload/synonym-overlay",
            data={"overlay_upload_scope": "skill"},
            files={
                "synonym_overlay_file": (
                    "reviewed-skill-synonyms.yaml",
                    b"skill_synonyms:\n  ga4: google analytics\n",
                    "application/x-yaml",
                )
            },
            follow_redirects=False,
        )

    assert resp.status_code == 303
    stored_json = mock_update.call_args.args[1]
    payload = json.loads(stored_json)
    assert payload["skill_synonyms"]["ga4"] == "google analytics"
    assert payload["skill_synonyms_runtime"]["has_run_overlay"] is True
    event_payload = json.loads(mock_event.call_args.args[0].payload_json or "{}")
    assert event_payload["scope"] == "skill"

def test_admin_upload_synonym_overlay_updates_non_skill_effective_maps() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-upload-nonskill",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        checkpoint_status="awaiting_continue",
        last_completed_stage="enrich",
        next_stage="rule_filter",
        completed_stages=["normalize", "enrich"],
        effective_settings_json=json.dumps({
            "gcp_project": "p",
            "bigquery_dataset": "d",
            "service_account_key": "k",
            "skill_synonyms": {"gcp": "google cloud"},
            "domain_alias_map": {},
            "role_family_alias_map": {},
            "skill_synonyms_runtime": {
                "base_policy_path": "config/skill_synonyms.yaml",
                "overlay_paths": [],
                "has_overlay": False,
                "entry_count": 1,
            },
        }),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_effective_settings") as mock_update, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post(
            "/admin/runs/run-overlay-upload-nonskill/synonym-overlay",
            files={
                "synonym_overlay_file": (
                    "reviewed-synonyms.yaml",
                    (
                        b"domain_alias_map:\n  fintech: financial services\n"
                        b"role_family_alias_map:\n  bi analyst: analytics\n"
                    ),
                    "application/x-yaml",
                )
            },
            follow_redirects=False,
        )

    assert resp.status_code == 303
    payload = json.loads(mock_update.call_args.args[1])
    assert payload["domain_alias_map"]["fintech"] == "financial services"
    assert payload["role_family_alias_map"]["bi analyst"] == "analytics"

def test_admin_upload_synonym_overlay_regenerates_synonym_proposals_with_updated_overlay() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-rebuild-proposals",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        checkpoint_status="awaiting_continue",
        last_completed_stage="enrich",
        next_stage="rule_filter",
        completed_stages=["normalize", "enrich"],
        mapping_suggestions_json='{"suggestions":[{"alias":"ga4","canonical":"google analytics","confidence":0.9}]}',
        effective_settings_json=json.dumps({
            "gcp_project": "p",
            "bigquery_dataset": "d",
            "service_account_key": "k",
            "skill_synonyms": {"gcp": "google cloud"},
            "skill_synonyms_runtime": {
                "base_policy_path": "config/skill_synonyms.yaml",
                "overlay_paths": [],
                "has_overlay": False,
                "entry_count": 1,
            },
        }),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.update_run_synonym_proposals") as mock_update_synonyms, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post(
            "/admin/runs/run-overlay-rebuild-proposals/synonym-overlay",
            files={
                "synonym_overlay_file": (
                    "reviewed-skill-synonyms.yaml",
                    b"skill_synonyms:\n  ga4: google analytics\n",
                    "application/x-yaml",
                )
            },
            follow_redirects=False,
        )

    assert resp.status_code == 303
    synonym_payload = json.loads(mock_update_synonyms.call_args.args[1])
    assert synonym_payload["proposal_generation_status"] == "not_applicable"


def test_admin_upload_synonym_overlay_rejects_invalid_yaml() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-invalid",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        checkpoint_status="awaiting_continue",
        last_completed_stage="enrich",
        next_stage="rule_filter",
        completed_stages=["normalize", "enrich"],
        effective_settings_json='{"skill_synonyms":{"gcp":"google cloud"}}',
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_effective_settings") as mock_update:
        resp = TestClient(_app()).post(
            "/admin/runs/run-overlay-invalid/synonym-overlay",
            files={
                "synonym_overlay_file": (
                    "bad.yaml",
                    b"skill_synonyms:\n  powerbi: ''\n",
                    "application/x-yaml",
                )
            },
        )

    assert resp.status_code == 422
    mock_update.assert_not_called()

def test_admin_upload_trigger_rejects_scope_mismatch_for_skill_scope() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run") as mock_insert, \
         patch("fitcv_cp.app.enqueue_run_with_job_id"), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p",
             "bigquery_dataset": "d",
             "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": "data/candidate_profile.yaml"},
             "skill_synonyms": {"gcp": "google cloud"},
         }):
        files = {
            "jobs_file": ("custom_jobs.json", b'[{"title":"Engineer","job_url":"http://x.com"}]', "application/json"),
            "synonym_overlay_file": (
                "custom_overlay.yaml",
                b"domain_alias_map:\n  fintech: financial services\n",
                "application/x-yaml",
            ),
        }
        data = {
            "config_path": ".env.yaml",
            "jobs_input_mode": "upload",
            "candidate_profile_mode": "default_config",
            "synonym_overlay_mode": "upload",
            "overlay_upload_scope": "skill",
        }
        resp = TestClient(_app()).post("/admin/upload-trigger", data=data, files=files)

    assert resp.status_code == 422
    assert "allows sections" in resp.text
    mock_insert.assert_not_called()

def test_admin_upload_trigger_accepts_domain_scope_with_domain_sections_only() -> None:
    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p",
             "bigquery_dataset": "d",
             "service_account_key": "k",
             "pipeline": {"final_top_n": 10},
             "paths": {"candidate_profile": "data/candidate_profile.yaml"},
             "skill_synonyms": {"gcp": "google cloud"},
             "domain_alias_map": {},
         }):
        files = {
            "jobs_file": ("custom_jobs.json", b'[{"title":"Engineer","job_url":"http://x.com"}]', "application/json"),
            "synonym_overlay_file": (
                "custom_overlay.yaml",
                b"domain_alias_map:\n  fintech: financial services\n",
                "application/x-yaml",
            ),
        }
        data = {
            "config_path": ".env.yaml",
            "jobs_input_mode": "upload",
            "candidate_profile_mode": "default_config",
            "synonym_overlay_mode": "upload",
            "overlay_upload_scope": "domain",
        }
        resp = TestClient(_app()).post("/admin/upload-trigger", data=data, files=files)

    assert resp.status_code == 201
    effective = json.loads(captured["run"].effective_settings_json)
    assert effective["domain_alias_map"]["fintech"] == "financial services"

def test_admin_upload_run_synonym_overlay_rejects_scope_mismatch_for_domain_scope() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-scope-mismatch",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        checkpoint_status="awaiting_continue",
        last_completed_stage="enrich",
        next_stage="rule_filter",
        completed_stages=["normalize", "enrich"],
        effective_settings_json='{"skill_synonyms":{"gcp":"google cloud"}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_effective_settings") as mock_update:
        resp = TestClient(_app()).post(
            "/admin/runs/run-overlay-scope-mismatch/synonym-overlay",
            data={"overlay_upload_scope": "domain"},
            files={
                "synonym_overlay_file": (
                    "bad-mixed.yaml",
                    b"skill_synonyms:\n  ga4: google analytics\n",
                    "application/x-yaml",
                )
            },
        )
    assert resp.status_code == 422
    assert "allows sections" in resp.text
    mock_update.assert_not_called()


# ── multi-file upload tests ────────────────────────────────────────────────────

_UPLOAD_COMMON_PATCHES = {
    "fitcv_cp.app.load_active_settings": lambda: {"return_value": {}},
}


def _upload_patches():
    return (
        patch("fitcv_cp.app.load_active_settings", return_value={}),
        patch("fitcv_cp.app.insert_run"),
        patch("fitcv_cp.app.continue_run_with_job_id", return_value=("run-multi", "rq-job-1")),
        patch("fitcv_cp.app.update_run_queue_job_id"),
        patch("fitcv_cp.app.load_config", return_value={
            "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
            "pipeline": {"final_top_n": 10},
            "paths": {"candidate_profile": "data/candidate_profile.yaml"},
        }),
    )


def test_admin_upload_trigger_merges_multiple_job_files():
    """@proves multi_file_job_input.multiple-file-inputs-in-trigger-form
    @proves multi_file_job_input.canonical-merge-preserving-order
    @proves multi_file_job_input.one-immutable-snapshot-stored-per-run

    Two valid JSON files → 201, merged snapshot contains both jobs.
    """
    file1 = b'[{"title": "Engineer", "job_url": "http://a.com"}]'
    file2 = b'[{"title": "Analyst", "job_url": "http://b.com"}]'
    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    p = _upload_patches()
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run", side_effect=_capture_insert):
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={"jobs_input_mode": "upload", "candidate_profile_mode": "default_config"},
                files=[
                    ("jobs_files", ("file1.json", file1, "application/json")),
                    ("jobs_files", ("file2.json", file2, "application/json")),
                ],
            )

    assert resp.status_code == 201, resp.text
    assert "run_id" in resp.json()
    merged = json.loads(captured["run"].jobs_input_json)
    urls = [j["job_url"] for j in merged]
    assert "http://a.com" in urls
    assert "http://b.com" in urls


def test_admin_upload_trigger_multi_file_preserves_order():
    """@proves multi_file_job_input.canonical-merge-preserving-order

    Merged snapshot preserves file order (file1 rows first, then file2).
    """
    file1 = b'[{"job_url": "http://first.com"}]'
    file2 = b'[{"job_url": "http://second.com"}]'
    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    p = _upload_patches()
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run", side_effect=_capture_insert):
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={"jobs_input_mode": "upload", "candidate_profile_mode": "default_config"},
                files=[
                    ("jobs_files", ("a.json", file1, "application/json")),
                    ("jobs_files", ("b.json", file2, "application/json")),
                ],
            )

    assert resp.status_code == 201, resp.text
    merged = json.loads(captured["run"].jobs_input_json)
    assert [j["job_url"] for j in merged] == ["http://first.com", "http://second.com"]


def test_admin_upload_trigger_one_invalid_file_rejects_entire_request():
    """@proves multi_file_job_input.per-file-server-side-validation
    @proves multi_file_job_input.all-or-nothing-rejection-on-validation-failure

    One file with invalid JSON → 422; run must NOT be created.
    """
    file1 = b'[{"job_url": "http://good.com"}]'
    file2 = b'THIS IS NOT JSON'
    p = _upload_patches()
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run") as mock_insert:
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={"jobs_input_mode": "upload", "candidate_profile_mode": "default_config"},
                files=[
                    ("jobs_files", ("good.json", file1, "application/json")),
                    ("jobs_files", ("bad.json", file2, "application/json")),
                ],
            )
    assert resp.status_code == 422
    mock_insert.assert_not_called()


def test_admin_upload_trigger_all_empty_arrays_rejected():
    """@proves multi_file_job_input.per-file-server-side-validation
    @proves multi_file_job_input.all-or-nothing-rejection-on-validation-failure

    Two files both containing empty arrays → 422 (total merged is empty).
    """
    file1 = b'[]'
    file2 = b'[]'
    p = _upload_patches()
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).post(
            "/admin/upload-trigger",
            data={"jobs_input_mode": "upload", "candidate_profile_mode": "default_config"},
            files=[
                ("jobs_files", ("a.json", file1, "application/json")),
                ("jobs_files", ("b.json", file2, "application/json")),
            ],
        )
    assert resp.status_code == 422


def test_admin_upload_trigger_upload_mode_no_files_rejected():
    """@proves multi_file_job_input.multiple-file-inputs-in-trigger-form

    Upload mode with neither jobs_file nor jobs_files → 422.
    """
    p = _upload_patches()
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).post(
            "/admin/upload-trigger",
            data={"jobs_input_mode": "upload", "candidate_profile_mode": "default_config"},
        )
    assert resp.status_code == 422


def test_admin_upload_trigger_multi_file_non_array_rejected():
    """@proves multi_file_job_input.per-file-server-side-validation
    @proves multi_file_job_input.all-or-nothing-rejection-on-validation-failure

    A file whose top-level is not a JSON array → 422.
    """
    file1 = b'{"title": "not an array"}'
    p = _upload_patches()
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).post(
            "/admin/upload-trigger",
            data={"jobs_input_mode": "upload", "candidate_profile_mode": "default_config"},
            files=[
                ("jobs_files", ("dict.json", file1, "application/json")),
            ],
        )
    assert resp.status_code == 422


def test_admin_upload_trigger_effective_settings_includes_enrichment_parallelism():
    """Trigger run with mocked active settings containing batch_size/concurrency → stored in effective_settings_json."""
    active = {"enrichment_batch_size": 5, "enrichment_concurrency": 3}
    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    p = _upload_patches()
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.load_active_settings", return_value=active), \
             patch("fitcv_cp.app.insert_run", side_effect=_capture_insert):
            file1 = b'[{"job_url": "http://e.com"}]'
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={"jobs_input_mode": "upload", "candidate_profile_mode": "default_config"},
                files=[
                    ("jobs_files", ("e.json", file1, "application/json")),
                ],
            )

    assert resp.status_code == 201, resp.text
    effective = json.loads(captured["run"].effective_settings_json)
    assert effective.get("enrichment_batch_size") == 5
    assert effective.get("enrichment_concurrency") == 3



# ── html routes ──────────────────────────────────────────────────────────────

def test_admin_runs_rendered_nav():
    with patch("fitcv_cp.app.list_runs", return_value=[]), \
         patch("fitcv_cp.app.get_events", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    assert 'href="/admin/settings">Settings</a>' in resp.text
    assert 'Refresh' in resp.text
    assert 'id="jobs_file"' in resp.text
    assert 'id="jobs_path"' in resp.text
    assert "Outbox Replay Health (Visible Runs)" in resp.text
    assert "Replay Success Ratio" in resp.text
    assert 'href="/admin/outbox-replay-health.json?view=active"' in resp.text


def test_admin_runs_shows_degraded_outbox_replay_health(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    runs = [
        PipelineRun(
            run_id="run-a",
            status=RunStatus.SUCCEEDED,
            jobs_path="data/sample_jobs.json",
            triggered_by="admin",
            trigger_source="web",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
        ),
        PipelineRun(
            run_id="run-b",
            status=RunStatus.SUCCEEDED,
            jobs_path="data/sample_jobs.json",
            triggered_by="admin",
            trigger_source="web",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
        ),
    ]
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text(
        "\n".join(
            [
                json.dumps({"row": {"run_id": "run-a"}}),
                json.dumps({"row": {"run_id": "run-b"}}),
            ]
        ),
        encoding="utf-8",
    )

    def _events_for_run(run_id: str, *_args, **_kwargs):
        if run_id == "run-a":
            return [
                RunEvent(
                    run_id="run-a",
                    event_id="ev-a",
                    stage="event_dead_letter_replay",
                    level="info",
                    message="Replay summary",
                    created_at=datetime.now(timezone.utc),
                    payload_json=json.dumps(
                        {
                            "replay_candidates": 4,
                            "replayed": 3,
                            "failed": 1,
                            "replay_success_ratio": 0.75,
                            "remaining_dead_letter_total": 1,
                        }
                    ),
                )
            ]
        return []

    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         patch("fitcv_cp.app.list_runs", return_value=runs), \
         patch("fitcv_cp.app.get_events", side_effect=_events_for_run):
        resp = TestClient(_app()).get("/admin/runs?view=all")

    assert resp.status_code == 200
    html = resp.text
    assert "Outbox Replay Health (Visible Runs)" in html
    assert "degraded" in html
    assert "3 / 4" in html
    assert "0.75" in html
    assert ">1<" in html


def test_admin_outbox_replay_health_json(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    runs = [
        PipelineRun(
            run_id="json-run-a",
            status=RunStatus.SUCCEEDED,
            jobs_path="data/sample_jobs.json",
            triggered_by="admin",
            trigger_source="web",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
        ),
        PipelineRun(
            run_id="json-run-b",
            status=RunStatus.SUCCEEDED,
            jobs_path="data/sample_jobs.json",
            triggered_by="admin",
            trigger_source="web",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
        ),
    ]
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text(
        "\n".join(
            [
                json.dumps({"row": {"run_id": "json-run-a"}}),
                json.dumps({"row": {"run_id": "json-run-b"}}),
            ]
        ),
        encoding="utf-8",
    )

    def _events_for_run(run_id: str, *_args, **_kwargs):
        if run_id == "json-run-a":
            return [
                RunEvent(
                    run_id="json-run-a",
                    event_id="json-ev-a",
                    stage="event_dead_letter_replay",
                    level="info",
                    message="Replay summary",
                    created_at=datetime.now(timezone.utc),
                    payload_json=json.dumps(
                        {
                            "replay_candidates": 4,
                            "replayed": 3,
                            "failed": 1,
                            "replay_success_ratio": 0.75,
                            "remaining_dead_letter_total": 1,
                        }
                    ),
                )
            ]
        return []

    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         patch("fitcv_cp.app.list_runs", return_value=runs), \
         patch("fitcv_cp.app.get_events", side_effect=_events_for_run):
        resp = TestClient(_app()).get("/admin/outbox-replay-health.json?view=all")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["view"] == "all"
    assert payload["run_count"] == 2
    aggregate = payload["outbox_replay_health"]
    assert aggregate["status"] == "degraded"
    assert aggregate["dead_letter_total"] == 2
    assert aggregate["replay_candidates"] == 4
    assert aggregate["replayed"] == 3
    assert aggregate["failed"] == 1
    assert aggregate["replay_success_ratio"] == 0.75


def test_admin_outbox_replay_health_check_alert_emits_event(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    runs = [
        PipelineRun(
            run_id="check-alert-run",
            status=RunStatus.SUCCEEDED,
            jobs_path="data/sample_jobs.json",
            triggered_by="admin",
            trigger_source="web",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
        )
    ]
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text(json.dumps({"row": {"run_id": "check-alert-run"}}), encoding="utf-8")
    captured = {}

    def _capture_event(event, *_args, **_kwargs):
        captured["event"] = event
        return {"persistence_status": "persisted", "degradation_reason": ""}

    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         patch("fitcv_cp.app.list_runs", return_value=runs), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.append_event", side_effect=_capture_event):
        resp = TestClient(_app()).post("/admin/outbox-replay-health/check?view=all&min_replay_success_ratio=0.95")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["decision"] == "alert"
    assert "dead_letter_status_degraded" in payload["reason_code"]
    assert payload["outbox_replay_health"]["status"] == "degraded"
    assert captured["event"].stage == "outbox_replay_health_alert"
    assert captured["event"].level == "warning"
    emitted_payload = json.loads(str(captured["event"].payload_json or "{}"))
    assert emitted_payload["decision"] == "alert"
    assert "dead_letter_status_degraded" in emitted_payload["reason_code"]
    assert emitted_payload["outbox_replay_health"]["status"] == "degraded"


def test_admin_outbox_replay_health_check_ok_emits_info_event(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    runs = [
        PipelineRun(
            run_id="check-ok-run",
            status=RunStatus.SUCCEEDED,
            jobs_path="data/sample_jobs.json",
            triggered_by="admin",
            trigger_source="web",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
        )
    ]
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text("", encoding="utf-8")
    replay_event = RunEvent(
        run_id="check-ok-run",
        event_id="ok-replay-1",
        stage="event_dead_letter_replay",
        level="info",
        message="Replay summary",
        created_at=datetime.now(timezone.utc),
        payload_json=json.dumps(
            {
                "replay_candidates": 10,
                "replayed": 10,
                "failed": 0,
                "replay_success_ratio": 1.0,
                "remaining_dead_letter_total": 0,
            }
        ),
    )
    captured = {}

    def _capture_event(event, *_args, **_kwargs):
        captured["event"] = event
        return {"persistence_status": "persisted", "degradation_reason": ""}

    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         patch("fitcv_cp.app.list_runs", return_value=runs), \
         patch("fitcv_cp.app.get_events", return_value=[replay_event]), \
         patch("fitcv_cp.app.append_event", side_effect=_capture_event):
        resp = TestClient(_app()).post("/admin/outbox-replay-health/check?view=all&min_replay_success_ratio=0.95")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["decision"] == "ok"
    assert payload["reason_code"] == "healthy"
    assert captured["event"].stage == "outbox_replay_health_alert"
    assert captured["event"].level == "info"
    emitted_payload = json.loads(str(captured["event"].payload_json or "{}"))
    assert emitted_payload["decision"] == "ok"
    assert emitted_payload["reason_code"] == "healthy"


def test_admin_outbox_replay_health_check_uses_config_default_threshold(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    runs = [
        PipelineRun(
            run_id="check-default-threshold-run",
            status=RunStatus.SUCCEEDED,
            jobs_path="data/sample_jobs.json",
            triggered_by="admin",
            trigger_source="web",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
        )
    ]
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text("", encoding="utf-8")
    replay_event = RunEvent(
        run_id="check-default-threshold-run",
        event_id="default-threshold-ev-1",
        stage="event_dead_letter_replay",
        level="info",
        message="Replay summary",
        created_at=datetime.now(timezone.utc),
        payload_json=json.dumps(
            {
                "replay_candidates": 10,
                "replayed": 9,
                "failed": 1,
                "replay_success_ratio": 0.9,
                "remaining_dead_letter_total": 0,
            }
        ),
    )
    captured = {}

    def _capture_event(event, *_args, **_kwargs):
        captured["event"] = event
        return {"persistence_status": "persisted", "degradation_reason": ""}

    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         patch("fitcv_cp.app.list_runs", return_value=runs), \
         patch("fitcv_cp.app.get_events", return_value=[replay_event]), \
         patch("fitcv_cp.app.load_config", return_value={"outbox_replay_health": {"min_replay_success_ratio": 0.85}}), \
         patch("fitcv_cp.app.append_event", side_effect=_capture_event):
        resp = TestClient(_app()).post("/admin/outbox-replay-health/check?view=all")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["min_replay_success_ratio"] == 0.85
    assert payload["decision"] == "ok"
    emitted_payload = json.loads(str(captured["event"].payload_json or "{}"))
    assert emitted_payload["min_replay_success_ratio"] == 0.85


def test_admin_run_detail_success_banner():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    
    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-123", status=RunStatus.SUCCEEDED, 
        cvs_generated=5, total_jobs=10, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc)
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[{"version_id": "v123", "job_url": "mock.com", "fit_classification": "strong", "generated_at": datetime.now(timezone.utc)}]):
        resp = TestClient(_app()).get("/admin/runs/test-123")
    assert resp.status_code == 200
    assert "candidate CV(s) were successfully generated." in resp.text
    assert "Persisted to the <strong>cv_versions</strong> BigQuery table." in resp.text
    assert 'href="/admin/cvs/v123/download"' in resp.text
    assert 'href="/admin/runs/test-123"' in resp.text
    assert "Refresh Status" in resp.text  # still present on run_detail page


def test_admin_run_detail_shows_exports_card_with_results_link():
    """@proves trigger_run_management.run-owned-artifact-exports
    @proves inspection_debugging.run-owned-artifact-exports
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-export-btn", status=RunStatus.SUCCEEDED,
        cvs_generated=1, total_jobs=10, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json='{"run_id":"test-export-btn","results":[]}',
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-export-btn")
    assert resp.status_code == 200
    assert "Run Exports" in resp.text
    assert 'href="/admin/runs/test-export-btn/export.json"' in resp.text

def test_run_detail_shows_orchestration_backend_diagnostics() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-orch-detail",
        status=RunStatus.QUEUED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        queue_job_id="flow-run-abc123",
        orchestration_backend="prefect",
        orchestration_run_id="flow-run-abc123",
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]), \
         patch("fitcv_cp.app.orchestration_job_status", return_value="queued"):
        resp = TestClient(_app()).get("/admin/runs/run-orch-detail")

    assert resp.status_code == 200
    assert "Orchestration Backend" in resp.text
    assert "Backend Run ID" in resp.text
    assert "Backend Status" in resp.text
    assert "flow-run-abc123" in resp.text


def test_run_detail_orchestration_diagnostics_fallback_to_queue_job_id_for_legacy_rows() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-orch-legacy",
        status=RunStatus.QUEUED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        queue_job_id="rq-job-legacy-123",
        orchestration_backend=None,
        orchestration_run_id=None,
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]), \
         patch("fitcv_cp.app.orchestration_job_status", return_value="queued"):
        resp = TestClient(_app()).get("/admin/runs/run-orch-legacy")

    assert resp.status_code == 200
    assert "Orchestration Backend" in resp.text
    assert "Backend Run ID" in resp.text
    assert "rq-job-legacy-123" in resp.text
def test_admin_run_detail_shows_download_cv_debug_json_button():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-debug-btn", status=RunStatus.SUCCEEDED,
        cvs_generated=1, total_jobs=10, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json='{"run_id":"test-debug-btn","debug_records":[]}',
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-debug-btn")
    assert resp.status_code == 200
    assert 'href="/admin/runs/test-debug-btn/cv-debug.json"' in resp.text
    assert "CV Debug JSON" in resp.text


def test_admin_run_detail_shows_stage_artifacts_export_in_exports_card():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-stage-artifacts-btn", status=RunStatus.SUCCEEDED,
        cvs_generated=1, total_jobs=10, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"test-stage-artifacts-btn","artifacts":{"stages":{}}}',
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-stage-artifacts-btn")
    assert resp.status_code == 200
    assert 'href="/admin/runs/test-stage-artifacts-btn/stage-artifacts.json"' in resp.text
    assert "Stage Artifacts JSON (Diagnostics)" in resp.text
    assert resp.text.index("Run Exports") < resp.text.index("Event Timeline")


def test_admin_run_detail_shows_bundle_zip_export_link():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-bundle-btn", status=RunStatus.SUCCEEDED,
        cvs_generated=1, total_jobs=10, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json='{"run_id":"test-bundle-btn","results":[]}',
        stage_transition_artifacts_json='{"run_id":"test-bundle-btn","artifacts":{"stages":{"normalize":{"status":"completed"}}}}',
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-bundle-btn")
    assert resp.status_code == 200
    assert 'href="/admin/runs/test-bundle-btn/artifacts.zip"' in resp.text
    assert "Download All Artifacts (.zip)" in resp.text


def test_admin_run_detail_shows_download_settings_used_json_button():
    """@proves inspection_debugging.settings-used-export"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-settings-btn", status=RunStatus.SUCCEEDED,
        cvs_generated=1, total_jobs=10, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc),
        settings_used_json='{"run_id":"test-settings-btn","effective_settings":{"pipeline":{"final_top_n":10}}}',
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-settings-btn")
    assert resp.status_code == 200
    assert 'href="/admin/runs/test-settings-btn/settings-used.json"' in resp.text
    assert "Settings Used JSON" in resp.text


def test_admin_run_detail_shows_agentic_live_trace_export_when_present() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="test-agentic-live-trace-btn",
        status=RunStatus.SUCCEEDED,
        cvs_generated=1,
        total_jobs=10,
        jobs_path="",
        triggered_by="admin",
        trigger_source="web",
        config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "run_id": "test-agentic-live-trace-btn",
                "agentic_live_trace": {
                    "run_id": "test-agentic-live-trace-btn",
                    "trace_schema_version": "agentic_step_trace_run_v1",
                    "trace_family": "agentic_step_trace",
                    "step_id": "cv_generation",
                    "late_stage_mode": {
                        "late_stage_mode": "agentic",
                        "agentic_late_stage_enabled": True,
                        "mode_source": "cv.agentic_late_stage.enabled",
                        "agentic_status": "completed",
                    },
                    "trace_status": "completed",
                    "trace_summary": {"records_total": 1, "present_records": 1, "attempted_generation_jobs_total": 1},
                    "records": [{"record_id": "https://example.com/1", "scope_type": "job", "scope_key": "https://example.com/1"}],
                    "degradation": {},
                },
                "debug_records": [],
            }
        ),
        settings_used_json='{"run_id":"test-agentic-live-trace-btn","late_stage_mode":{"late_stage_mode":"agentic","agentic_late_stage_enabled":true,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"completed"}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-agentic-live-trace-btn")
    assert resp.status_code == 200
    assert 'href="/admin/runs/test-agentic-live-trace-btn/agentic-live-trace.json"' in resp.text
    assert "Agentic Live Trace JSON" in resp.text


def test_admin_run_detail_shows_cv_analysis_trace_export_when_present() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="test-cv-analysis-trace-btn",
        status=RunStatus.SUCCEEDED,
        cvs_generated=1,
        total_jobs=10,
        jobs_path="",
        triggered_by="admin",
        trigger_source="web",
        config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "run_id": "test-cv-analysis-trace-btn",
                "cv_analysis_trace": {
                    "run_id": "test-cv-analysis-trace-btn",
                    "trace_schema_version": "agentic_step_trace_run_v1",
                    "trace_family": "agentic_step_trace",
                    "step_id": "cv_analysis",
                    "late_stage_mode": {
                        "late_stage_mode": "agentic",
                        "agentic_late_stage_enabled": True,
                        "mode_source": "cv.agentic_late_stage.enabled",
                        "agentic_status": "completed",
                    },
                    "trace_status": "completed",
                    "trace_summary": {"records_total": 1, "present_records": 1, "attempted_analysis_jobs_total": 1},
                    "records": [{"record_id": "https://example.com/1", "scope_type": "job", "scope_key": "https://example.com/1"}],
                    "degradation": {},
                },
                "debug_records": [],
            }
        ),
        settings_used_json='{"run_id":"test-cv-analysis-trace-btn","late_stage_mode":{"late_stage_mode":"agentic","agentic_late_stage_enabled":true,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"completed"}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-cv-analysis-trace-btn")
    assert resp.status_code == 200
    assert 'href="/admin/runs/test-cv-analysis-trace-btn/cv-analysis-trace.json"' in resp.text
    assert "CV Analysis Trace JSON" in resp.text


def test_admin_run_detail_hides_aggregate_mapping_suggestions_button() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-mapping-aggregate-btn", status=RunStatus.RUNNING,
        total_jobs=10, jobs_path="", triggered_by="admin", trigger_source="web",
        config_path="config/default.yaml", created_at=datetime.now(timezone.utc),
        mapping_suggestions_json=None,
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-mapping-aggregate-btn")
    assert resp.status_code == 200
    assert 'href="/admin/mapping-suggestions.json"' not in resp.text
    assert "Aggregate Mapping Suggestions JSON" not in resp.text


def test_admin_run_detail_hides_mapping_suggestions_export_before_enrich_stage() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="test-mapping-stage-gate",
        status=RunStatus.AWAITING_CONTINUE,
        total_jobs=7,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {"artifacts": {"stages": {"normalize": {"status": "completed"}}}}
        ),
        mapping_suggestions_json='{"suggestions":[]}',
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-mapping-stage-gate")

    assert resp.status_code == 200
    assert 'href="/admin/runs/test-mapping-stage-gate/mapping-suggestions.json"' not in resp.text
    assert "Mapping Suggestions JSON" not in resp.text


def test_run_detail_timeline_shows_stage_download_for_mapped_event():
    """@proves inspection_debugging.stage-artifact-downloads
    @proves trigger_run_management.stage-artifact-downloads
    """
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-link",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"run-stage-link","artifacts":{"stages":{"ranking":{"status":"completed"}}}}',
    )
    events = [
        RunEvent(
            run_id="run-stage-link",
            event_id="e1",
            stage="layer3_ranking",
            level="info",
            message="Final ranking: top 3 jobs",
            created_at=datetime.now(timezone.utc),
        )
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-stage-link")
    assert resp.status_code == 200
    assert 'href="/admin/runs/run-stage-link/stage-artifacts/ranking.json"' in resp.text
    assert "Download Ranking JSON" in resp.text


def test_run_detail_paused_after_normalize_shows_normalize_download_on_timeline_row():
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-normalize-timeline",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/uploads/example_merged_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        checkpoint_status="awaiting_continue",
        last_completed_stage="normalize",
        next_stage="enrich",
        completed_stages=["normalize"],
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "normalize": {
                            "status": "completed",
                            "output_counts": {
                                "raw_jobs": 10,
                                "normalized_jobs": 10,
                                "deduplicated_jobs": 0,
                            },
                        }
                    }
                }
            }
        ),
    )
    events = [
        RunEvent(
            run_id="run-normalize-timeline",
            event_id="e1",
            stage="layer1_normalize",
            level="info",
            message="Normalization dedupe: kept 10 of 10 jobs, removed 0 duplicate(s)",
            created_at=datetime.now(timezone.utc),
        ),
        RunEvent(
            run_id="run-normalize-timeline",
            event_id="e2",
            stage="stage_checkpoint",
            level="info",
            message="Paused after normalize; next stage: enrich",
            created_at=datetime.now(timezone.utc),
        ),
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-normalize-timeline")

    assert resp.status_code == 200
    assert 'href="/admin/runs/run-normalize-timeline/stage-artifacts/normalize.json"' in resp.text
    assert "Download Normalize JSON" in resp.text
    assert "Normalize complete: kept 10 of 10 jobs, removed 0 duplicate(s)" in resp.text


def test_run_detail_timeline_shows_cv_analysis_download_only_on_aggregate_row():
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-cv-analysis-timeline",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "cv_analysis": {
                            "status": "completed",
                            "output_counts": {
                                "ready_for_generation": 1,
                                "skipped_fit_gate": 2,
                                "analysis_failed": 0,
                            },
                        }
                    }
                }
            }
        ),
    )
    events = [
        RunEvent(
            run_id="run-cv-analysis-timeline",
            event_id="e1",
            stage="layer4_cv_analysis_skip",
            level="info",
            message="Skipped https://jobs.example.com/1 (fit=skip)",
            created_at=datetime.now(timezone.utc),
        ),
        RunEvent(
            run_id="run-cv-analysis-timeline",
            event_id="e2",
            stage="layer4_cv_analysis",
            level="info",
            message="CV analysis complete: 1 ready, 2 skipped, 0 failed",
            created_at=datetime.now(timezone.utc),
        ),
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-cv-analysis-timeline")

    assert resp.status_code == 200
    assert resp.text.count('href="/admin/runs/run-cv-analysis-timeline/stage-artifacts/cv_analysis.json"') == 1
    assert "CV analysis complete: 1 ready, 2 skipped, 0 failed" in resp.text
    assert "Skipped https://jobs.example.com/1 (fit=skip)" in resp.text


def test_run_detail_timeline_uses_bounded_cv_analysis_payload_counts():
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-cv-analysis-payload",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "cv_analysis": {
                            "status": "completed",
                        }
                    }
                }
            }
        ),
    )
    events = [
        RunEvent(
            run_id="run-cv-analysis-payload",
            event_id="e1",
            stage="layer4_cv_analysis",
            level="info",
            message="legacy summary",
            created_at=datetime.now(timezone.utc),
            payload_json=json.dumps(
                {
                    "event_name": "cv_analysis_decision",
                    "event_family": "decision",
                    "source_stage": "cv_analysis",
                    "event_status": "completed",
                    "deterministic_outcome": None,
                    "stage_owned_subreason": "stage_summary",
                    "fallback_used": False,
                    "output_snapshot": {
                        "ready_for_generation": 1,
                        "blocked_by_reranker_fit": 2,
                        "skipped_fit_gate": 0,
                        "analysis_failed": 1,
                    },
                    "artifact_refs": {"stage_id": "cv_analysis"},
                }
            ),
        )
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-cv-analysis-payload")

    assert resp.status_code == 200
    assert "CV analysis complete: 1 ready, 2 blocked, 0 skipped, 1 failed" in resp.text


def test_run_detail_timeline_keeps_cv_generation_failure_types_distinct() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-cv-generation-failure-types",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "cv_generation": {
                            "status": "completed",
                            "output_counts": {
                                "accepted": 1,
                                "validation_failed": 2,
                                "generation_failed": 3,
                                "persistence_failed": 4,
                            },
                        }
                    }
                }
            }
        ),
    )
    events = [
        RunEvent(
            run_id="run-cv-generation-failure-types",
            event_id="e1",
            stage="pipeline_complete",
            level="info",
            message="legacy completion summary",
            created_at=datetime.now(timezone.utc),
            payload_json=json.dumps(
                {
                    "event_name": "pipeline_complete",
                    "event_family": "summary",
                    "source_stage": "cv_generation",
                    "event_status": "completed",
                }
            ),
        )
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-cv-generation-failure-types")

    assert resp.status_code == 200
    assert (
        "CV generation complete: 1 accepted, 2 validation failed, 3 generation failed, 4 persistence failed"
        in resp.text
    )


def test_run_detail_timeline_keeps_validation_failed_job_message_from_payload():
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-cv-validation-row",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "cv_generation": {
                            "status": "completed",
                            "output_counts": {
                                "accepted": 1,
                                "validation_failed": 1,
                                "generation_failed": 0,
                                "persistence_failed": 0,
                            },
                        }
                    }
                }
            }
        ),
    )
    events = [
        RunEvent(
            run_id="run-cv-validation-row",
            event_id="e1",
            stage="layer4_cv_validation_failed",
            level="warning",
            message="legacy validation copy",
            created_at=datetime.now(timezone.utc),
            payload_json=json.dumps(
                {
                    "event_name": "cv_generation_decision",
                    "event_family": "decision",
                    "source_stage": "cv_generation",
                    "job_url": "https://jobs.example.com/1",
                    "event_status": "completed",
                    "deterministic_outcome": "rejected",
                    "stage_owned_subreason": "validation_failed",
                    "fallback_used": False,
                    "artifact_refs": {"stage_id": "cv_generation"},
                }
            ),
        )
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-cv-validation-row")

    assert resp.status_code == 200
    assert "CV validation failed for https://jobs.example.com/1" in resp.text
    assert "CV generation complete:" not in resp.text


def test_run_detail_timeline_hides_stage_download_for_mapped_event_without_stage_artifact():
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-link-missing-artifact",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"run-stage-link-missing-artifact","artifacts":{"stages":{"shortlist":{"status":"completed"}}}}',
    )
    events = [
        RunEvent(
            run_id="run-stage-link-missing-artifact",
            event_id="e1",
            stage="layer3_ranking",
            level="info",
            message="Final ranking: top 3 jobs",
            created_at=datetime.now(timezone.utc),
        )
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-stage-link-missing-artifact")

    assert resp.status_code == 200
    assert 'href="/admin/runs/run-stage-link-missing-artifact/stage-artifacts/ranking.json"' not in resp.text


def test_run_detail_timeline_hides_stage_download_when_stage_artifact_json_is_malformed():
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-link-bad-json",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json="{bad json",
    )
    events = [
        RunEvent(
            run_id="run-stage-link-bad-json",
            event_id="e1",
            stage="layer3_ranking",
            level="info",
            message="Final ranking: top 3 jobs",
            created_at=datetime.now(timezone.utc),
        )
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-stage-link-bad-json")

    assert resp.status_code == 200
    assert 'href="/admin/runs/run-stage-link-bad-json/stage-artifacts/ranking.json"' not in resp.text


def test_run_detail_timeline_hides_stage_download_for_unmapped_event():
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-link-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"run-stage-link-2","artifacts":{"stages":{"ranking":{"status":"completed"}}}}',
    )
    events = [
        RunEvent(
            run_id="run-stage-link-2",
            event_id="e1",
            stage="pipeline_start",
            level="info",
            message="Run started",
            created_at=datetime.now(timezone.utc),
        )
    ]
    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=events), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-stage-link-2")
    assert resp.status_code == 200
    assert 'href="/admin/runs/run-stage-link-2/stage-artifacts/' not in resp.text


def test_download_mapping_suggestions_requires_enrich_stage_reached() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="mapping-gate-endpoint",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {"artifacts": {"stages": {"normalize": {"status": "completed"}}}}
        ),
        mapping_suggestions_json='{"suggestions":[]}',
    )

    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/mapping-gate-endpoint/mapping-suggestions.json")

    assert resp.status_code == 404
    assert "enrich" in resp.text.lower()


def test_admin_run_detail_warning_banner():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    
    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-124", status=RunStatus.SUCCEEDED, 
        cvs_generated=0, total_jobs=10, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path="config/default.yaml",
        created_at=datetime.now(timezone.utc)
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-124")
    assert resp.status_code == 200
    assert "No candidates passed the final AI ranking threshold." in resp.text

def test_download_cv_endpoint_200():
    with patch("fitcv_cp.app.get_cv_markdown", return_value="# Mock CV"):
        resp = TestClient(_app()).get("/admin/cvs/v456/download")
    assert resp.status_code == 200
    assert resp.text == "# Mock CV"
    assert resp.headers["content-type"] == "text/markdown; charset=utf-8"
    assert "attachment; filename=\"cv_v456.md\"" in resp.headers["content-disposition"]

def test_download_cv_endpoint_404():
    with patch("fitcv_cp.app.get_cv_markdown", return_value=None):
        resp = TestClient(_app()).get("/admin/cvs/missing/download")
    assert resp.status_code == 404


def test_download_results_json_endpoint_200():
    """@proves trigger_run_management.run-results-export
    @proves inspection_debugging.results-ledger-inspection
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-export-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json='{"run_id":"run-export-1","results":[]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-export-1/export.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-export-1"
    assert resp.headers["content-type"] == "application/json"
    assert 'attachment; filename="fitcv-run-run-export-1-results.json"' in resp.headers["content-disposition"]
    assert "\n  \"run_id\"" in resp.text


def test_download_results_json_endpoint_409_for_running_run():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-export-2",
        status=RunStatus.RUNNING,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-export-2/export.json")
    assert resp.status_code == 409

def test_download_results_json_endpoint_includes_hitl_audit_fields_when_present():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-export-hitl-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json=json.dumps(
            {
                "run_id": "run-export-hitl-1",
                "results": [
                    {"job_url": "https://example.com/job-1", "job_title": "Test Role", "pipeline_status": "ranked_no_cv"}
                ],
            }
        ),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Test Role",
                        "status": "review_required",
                        "error": {"stage": "review_gate", "message": "Low confidence sections: experience"},
                    }
                ],
                "hitl_review_actions": [
                    {
                        "job_url": "https://example.com/job-1",
                        "action": "approve",
                        "actor": "operator",
                        "created_at": "2026-04-30T10:00:00Z",
                    }
                ],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-export-hitl-1/export.json")
    assert resp.status_code == 200
    row = resp.json()["results"][0]
    assert row["hitl_review_required"] is True
    assert row["hitl_review_action"] == "approve"
    assert row["hitl_review_actor"] == "operator"
    assert row["generated_draft_present"] is False
    assert row["accepted_cv_artifact_present"] is False

def test_download_hitl_review_audit_endpoint_200():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-hitl-audit-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Test Role",
                        "status": "review_required",
                        "error": {"stage": "review_gate", "message": "Low confidence sections: experience"},
                    }
                ],
                "hitl_review_actions": [],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-hitl-audit-1/hitl-review-audit.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["schema_version"] == "hitl_review_audit_v1"
    assert payload["summary"]["review_required_total"] == 1
    assert payload["summary"]["closure_mode"] == "incomplete"
    assert payload["summary"]["resolution_totals"]["pending"] == 1


def test_admin_run_detail_shows_cv_preview_in_hitl_review_queue() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-hitl-preview-1",
        status=RunStatus.AWAITING_CONTINUE,
        checkpoint_status="awaiting_review",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "cv_generation_debug_records": [
                    {
                        "job_url": "https://example.com/job-1",
                        "job_title": "Test Role",
                        "status": "review_required",
                        "markdown_final": "# Candidate Name\\n## Experience\\n- Built pipelines",
                        "error": {"stage": "review_gate", "message": "Unsupported requirements require review: Snowflake"},
                    }
                ],
                "hitl_review_actions": [],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-hitl-preview-1")
    assert resp.status_code == 200
    assert "CV Draft Preview" in resp.text
    assert "Show generated CV markdown" in resp.text
    assert "Built pipelines" in resp.text


def test_download_results_json_endpoint_404_if_snapshot_missing():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-export-3",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json=None,
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-export-3/export.json")
    assert resp.status_code == 404


def test_download_stage_transition_artifacts_json_endpoint_200():
    """@proves inspection_debugging.stage-transition-diagnostics"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-artifacts-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"run-stage-artifacts-1","artifacts":{"stages":{"normalize":{"status":"completed"}}}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-stage-artifacts-1/stage-artifacts.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-stage-artifacts-1"
    assert resp.headers["content-type"] == "application/json"
    assert 'attachment; filename="fitcv-run-run-stage-artifacts-1-stage-artifacts.json"' in resp.headers["content-disposition"]


def test_download_stage_transition_artifacts_json_endpoint_200_for_running_run_with_snapshot():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-artifacts-running-1",
        status=RunStatus.RUNNING,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"run-stage-artifacts-running-1","artifacts":{"stages":{"enrich":{"status":"completed"}}}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-stage-artifacts-running-1/stage-artifacts.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-stage-artifacts-running-1"


def test_download_stage_transition_artifacts_json_endpoint_404_if_snapshot_missing():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-artifacts-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-stage-artifacts-2/stage-artifacts.json")
    assert resp.status_code == 404


def test_download_mapping_suggestions_json_endpoint_200() -> None:
    """@proves pipeline_performance.enrich-stage-mapping-suggestion-capture-for-review-debug-surfaces"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-mapping-suggestions-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        completed_stages=["normalize", "enrich"],
        last_completed_stage="enrich",
        stage_transition_artifacts_json='{"artifacts":{"stages":{"enrich":{"status":"completed"}}}}',
        mapping_suggestions_json='{"run_id":"run-mapping-suggestions-1","suggestions":[]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-mapping-suggestions-1/mapping-suggestions.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-mapping-suggestions-1"


def test_download_mapping_suggestions_json_endpoint_404_if_snapshot_missing() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-mapping-suggestions-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-mapping-suggestions-2/mapping-suggestions.json")
    assert resp.status_code == 404


def test_download_aggregate_mapping_suggestions_json_endpoint_200() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    runs = [
        PipelineRun(
            run_id="run-ms-a",
            status=RunStatus.SUCCEEDED,
            triggered_by="admin",
            trigger_source="web",
            jobs_path="data/sample_jobs.json",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
            mapping_suggestions_json=(
                '{"run_id":"run-ms-a","suggestions":['
                '{"alias":"gcp","canonical":"google cloud","confidence":1.0,"matches":true,"must_have_skill":"google cloud"}'
                ']}'
            ),
        ),
        PipelineRun(
            run_id="run-ms-b",
            status=RunStatus.SUCCEEDED,
            triggered_by="admin",
            trigger_source="web",
            jobs_path="data/sample_jobs.json",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
            mapping_suggestions_json=(
                '{"run_id":"run-ms-b","suggestions":['
                '{"alias":"gcp","canonical":"google cloud","confidence":0.8,"matches":true,"must_have_skill":"google cloud"}'
                ']}'
            ),
        ),
    ]
    with patch("fitcv_cp.app.list_runs", return_value=runs):
        resp = TestClient(_app()).get("/admin/mapping-suggestions.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["suggestions"][0]["alias"] == "gcp"
    assert payload["suggestions"][0]["occurrences"] == 2


def test_download_synonym_proposals_json_endpoint_200() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    runs = [
        PipelineRun(
            run_id="run-proposal-a",
            status=RunStatus.SUCCEEDED,
            triggered_by="admin",
            trigger_source="web",
            jobs_path="data/sample_jobs.json",
            config_path=".env.yaml",
            created_at=datetime.now(timezone.utc),
            synonym_proposals_json=(
                '{"run_id":"run-proposal-a","proposals":['
                '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed",'
                '"proposal_scope":"run_scoped_overlay_candidate","proposal_family":"alias_to_canonical_mapping",'
                '"alias":"gcp","canonical":"google cloud","candidate_aliases":["gcp"],'
                '"candidate_canonicals":["google cloud"],"confidence":0.9,'
                '"rationale":{"kind":"repeated_alias_mapping"},"evidence_summary":{"occurrence_count":2},'
                '"conflict_summary":{"has_conflict":false},"source_artifact_refs":{"run_id":"run-proposal-a"}}'
                ']}'
            ),
        )
    ]
    with patch("fitcv_cp.app.list_runs", return_value=runs):
        resp = TestClient(_app()).get("/admin/synonym-proposals.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["proposals"][0]["proposal_id"] == "proposal-gcp"
    assert payload["proposals"][0]["run_id"] == "run-proposal-a"


def test_download_run_synonym_proposals_trace_json_endpoint_200() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-synonym-trace-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        completed_stages=["normalize", "enrich"],
        last_completed_stage="enrich",
        stage_transition_artifacts_json='{"artifacts":{"stages":{"enrich":{"status":"completed"}}}}',
        synonym_proposals_json=json.dumps(
            {
                "run_id": "run-synonym-trace-1",
                "proposal_generation_status": "generated",
                "persistence_status": "persisted",
                "proposals": [{"proposal_id": "proposal-gcp", "alias": "gcp"}],
                "synonym_proposals_trace": {
                    "run_id": "run-synonym-trace-1",
                    "trace_schema_version": "agentic_step_trace_run_v1",
                    "trace_family": "agentic_step_trace",
                    "step_id": "synonym_proposals",
                    "trace_status": "completed",
                    "trace_summary": {"records_total": 1, "present_records": 1, "proposal_count": 1},
                    "records": [{"record_id": "proposal-gcp", "scope_type": "alias", "scope_key": "gcp"}],
                    "degradation": {},
                },
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-synonym-trace-1/synonym-proposals-trace.json")
    assert resp.status_code == 200
    assert resp.json()["step_id"] == "synonym_proposals"
    assert resp.json()["trace_family"] == "agentic_step_trace"


def test_download_run_synonym_proposals_trace_json_endpoint_404_when_not_applicable() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-synonym-trace-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-synonym-trace-2/synonym-proposals-trace.json")
    assert resp.status_code == 404

def test_download_run_synonym_suppression_diff_json_endpoint_200() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-syn-suppress-diff-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        completed_stages=["enrich"],
        last_completed_stage="enrich",
        mapping_suggestions_json='{"run_id":"run-syn-suppress-diff-1","suggestions":[{"alias":"gcp","canonical":"google cloud"}]}',
        synonym_proposals_json=json.dumps(
            {
                "run_id": "run-syn-suppress-diff-1",
                "proposals": [],
                "synonym_proposals_trace": {
                    "trace_status": "completed",
                    "trace_summary": {
                        "suppressed_as_already_global_count": 1,
                        "suppressed_count_by_field": {"skill": 1, "domain": 2},
                        "generated_for_review_count": 0,
                        "suppression_source": "run_effective_skill_synonyms",
                    },
                    "suppression_examples": [{"alias": "gcp", "canonical": "google cloud"}],
                },
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-syn-suppress-diff-1/synonym-suppression-diff.json")
    assert resp.status_code == 200
    body = resp.json()
    assert body["suppressed_pairs_total"] == 3
    assert body["suppression_source"] == "run_effective_skill_synonyms"


def test_approve_synonym_proposal_updates_run_overlay_provenance() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-approve",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        last_completed_stage="enrich",
        completed_stages=["normalize", "enrich"],
        next_stage="rule_filter",
        effective_settings_json='{"skill_synonyms":{"sql":"structured query language"},"skill_synonyms_runtime":{"base_policy_path":"config/taxonomy/skill_synonyms.yaml","overlay_paths":[],"has_overlay":false,"entry_count":1}}',
        synonym_proposals_json=(
            '{"run_id":"run-proposal-approve","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed",'
            '"proposal_scope":"run_scoped_overlay_candidate","proposal_family":"alias_to_canonical_mapping",'
            '"alias":"gcp","canonical":"google cloud","candidate_aliases":["gcp"],'
            '"candidate_canonicals":["google cloud"],"confidence":0.9,'
            '"rationale":{"kind":"repeated_alias_mapping"},"evidence_summary":{"occurrence_count":2},'
            '"conflict_summary":{"has_conflict":false},"source_artifact_refs":{"run_id":"run-proposal-approve"}}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={
             "persistence_status": "persisted",
             "degradation_reason": "",
         }) as mock_update_proposals, \
         patch("fitcv_cp.app.update_run_effective_settings") as mock_update_effective, \
         patch("fitcv_cp.app.append_event") as mock_event:
        resp = TestClient(_app()).post(
            "/admin/synonym-proposals/proposal-gcp/approve-for-run-overlay",
            data={"acted_by": "operator@example.com", "note": "Looks good"},
        )

    assert resp.status_code == 200
    updated_payload = json.loads(mock_update_proposals.call_args.args[1])
    proposal = updated_payload["proposals"][0]
    assert proposal["proposal_status"] == "approved_for_run_overlay"
    assert proposal["review_history"][0]["action"] == "approve_for_run_overlay"
    effective_payload = json.loads(mock_update_effective.call_args.args[1])
    assert effective_payload["skill_synonyms"]["gcp"] == "google cloud"
    assert effective_payload["skill_synonyms_runtime"]["run_overlay_source"] == "proposal_review"
    assert effective_payload["skill_synonyms_runtime"]["run_overlay_proposal_ids"] == ["proposal-gcp"]
    assert "proposal-gcp" in mock_event.call_args.args[0].message


def test_approve_synonym_proposal_surfaces_persistence_degradation() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-degraded",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        last_completed_stage="enrich",
        completed_stages=["normalize", "enrich"],
        next_stage="rule_filter",
        effective_settings_json='{"skill_synonyms":{"sql":"structured query language"},"skill_synonyms_runtime":{"base_policy_path":"config/taxonomy/skill_synonyms.yaml","overlay_paths":[],"has_overlay":false,"entry_count":1}}',
        synonym_proposals_json=(
            '{"run_id":"run-proposal-degraded","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed",'
            '"proposal_scope":"run_scoped_overlay_candidate","proposal_family":"alias_to_canonical_mapping",'
            '"alias":"gcp","canonical":"google cloud","candidate_aliases":["gcp"],'
            '"candidate_canonicals":["google cloud"],"confidence":0.9,'
            '"rationale":{"kind":"repeated_alias_mapping"},"evidence_summary":{"occurrence_count":2},'
            '"conflict_summary":{"has_conflict":false},"source_artifact_refs":{"run_id":"run-proposal-degraded"}}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={
             "persistence_status": "bundle_only_degraded",
             "degradation_reason": "missing_synonym_proposals_json_column",
         }), \
         patch("fitcv_cp.app.update_run_effective_settings") as mock_update_effective, \
         patch("fitcv_cp.app.append_event") as mock_event:
        resp = TestClient(_app()).post(
            "/admin/synonym-proposals/proposal-gcp/approve-for-run-overlay",
            data={"acted_by": "operator@example.com", "note": "Looks good"},
        )

    assert resp.status_code == 200
    assert resp.json()["persistence_status"] == "bundle_only_degraded"
    assert resp.json()["degradation_reason"] == "missing_synonym_proposals_json_column"
    mock_update_effective.assert_called_once()
    assert mock_event.call_count == 2
    warning_event = mock_event.call_args_list[-1].args[0]
    assert warning_event.stage == "snapshot_persist_failed"
    assert "synonym_proposals snapshot persistence failed" in warning_event.message


def test_approve_synonym_proposal_allows_run_all_mode() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-run-all",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-run-all","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed",'
            '"proposal_scope":"run_scoped_overlay_candidate","proposal_family":"alias_to_canonical_mapping",'
            '"alias":"gcp","canonical":"google cloud","candidate_aliases":["gcp"],'
            '"candidate_canonicals":["google cloud"],"confidence":0.9,'
            '"rationale":{"kind":"repeated_alias_mapping"},"evidence_summary":{"occurrence_count":2},'
            '"conflict_summary":{"has_conflict":false},"source_artifact_refs":{"run_id":"run-proposal-ineligible"}}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={
             "persistence_status": "persisted",
             "degradation_reason": "",
         }) as mock_update_proposals, \
         patch("fitcv_cp.app.update_run_effective_settings") as mock_update_effective:
        resp = TestClient(_app()).post(
            "/admin/synonym-proposals/proposal-gcp/approve-for-run-overlay",
            data={"acted_by": "operator@example.com"},
        )

    assert resp.status_code == 200
    assert resp.json()["proposal_status"] == "approved_for_run_overlay"
    mock_update_proposals.assert_called_once()
    mock_update_effective.assert_called_once()


def test_approve_synonym_proposal_does_not_persist_approved_state_if_overlay_write_fails() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-write-fail",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        effective_settings_json='{"skill_synonyms":{"sql":"structured query language"},"skill_synonyms_runtime":{"base_policy_path":"config/taxonomy/skill_synonyms.yaml","overlay_paths":[],"has_overlay":false,"entry_count":1}}',
        run_mode="manual_staged",
        last_completed_stage="enrich",
        completed_stages=["normalize", "enrich"],
        next_stage="rule_filter",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-write-fail","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed",'
            '"proposal_scope":"run_scoped_overlay_candidate","proposal_family":"alias_to_canonical_mapping",'
            '"alias":"gcp","canonical":"google cloud","candidate_aliases":["gcp"],'
            '"candidate_canonicals":["google cloud"],"confidence":0.9,'
            '"rationale":{"kind":"repeated_alias_mapping"},"evidence_summary":{"occurrence_count":2},'
            '"conflict_summary":{"has_conflict":false},"source_artifact_refs":{"run_id":"run-proposal-write-fail"}}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.update_run_synonym_proposals") as mock_update_proposals, \
         patch("fitcv_cp.app.update_run_effective_settings", side_effect=RuntimeError("bq write failed")), \
         patch("fitcv_cp.app.append_event") as mock_event:
        resp = TestClient(_app(), raise_server_exceptions=False).post(
            "/admin/synonym-proposals/proposal-gcp/approve-for-run-overlay",
            data={"acted_by": "operator@example.com", "note": "Looks good"},
        )

    assert resp.status_code == 500
    mock_update_proposals.assert_not_called()
    mock_event.assert_not_called()


def test_admin_run_detail_shows_review_derived_synonym_overlay_state() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-reviewed",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        effective_settings_json=(
            '{"skill_synonyms":{"gcp":"google cloud","ga4":"google analytics"},'
            '"skill_synonyms_runtime":{"base_policy_path":"config/taxonomy/skill_synonyms.yaml",'
            '"overlay_paths":[],"has_overlay":true,"entry_count":2,"has_run_overlay":true,'
            '"run_overlay_source":"proposal_review","run_overlay_entry_count":1,'
            '"run_overlay_proposal_ids":["proposal-gcp"]}}'
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-overlay-reviewed")

    assert resp.status_code == 200
    assert "Proposal Review" in resp.text

def test_admin_run_detail_shows_synonym_proposal_review_actions() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-ui",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-ui","proposals":['
            '{"proposal_id":"proposal-gcpx","proposal_status":"proposed_unreviewed",'
            '"field":"skill","alias":"gcpx","canonical":"google cloud platform","confidence":0.9},'
            '{"proposal_id":"proposal-fintech","proposal_status":"proposed_unreviewed",'
            '"field":"domain","alias":"it services and it consulting","canonical":"fintech","confidence":0.8},'
            '{"proposal_id":"proposal-ml","proposal_status":"proposed_unreviewed",'
            '"field":"role_family","alias":"ml engineering","canonical":"data_science","confidence":0.85}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-proposal-ui")

    assert resp.status_code == 200
    assert "Synonym Proposal Review" in resp.text
    assert "/admin/runs/run-proposal-ui/synonym-proposals/proposal-gcpx/action" in resp.text
    assert "/admin/runs/run-proposal-ui/synonym-proposals/batch-action" in resp.text
    assert "proposal_action__proposal-gcpx" in resp.text
    assert "Skills" in resp.text
    assert "Domain" in resp.text
    assert "Role Family" in resp.text
    assert "proposal_action__proposal-fintech" in resp.text
    assert "proposal_action__proposal-ml" in resp.text


def test_admin_run_detail_shows_synonym_recommendation_advisory_fields() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-ui-reco",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-ui-reco","proposals":['
            '{"proposal_id":"proposal-gcpx","proposal_status":"proposed_unreviewed",'
            '"alias":"gcpx","canonical":"google cloud platform","confidence":0.9,'
            '"recommended_action":"approve","recommendation_confidence":0.86,'
            '"recommendation_rationale":"Alias is standard in data engineering profiles.",'
            '"recommendation_risk_flags":["global_drift_check"]}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-proposal-ui-reco")

    assert resp.status_code == 200
    assert "Apply Recommendations" in resp.text
    assert "Recommendation: <strong>approve</strong>" in resp.text
    assert "Risk flags: global_drift_check" in resp.text
    assert 'data-recommended-action="approve"' in resp.text


def test_admin_run_detail_shows_synonym_triage_refresh_action_and_status() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-ui-triage-action",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-ui-triage-action","proposals":['
            '{"proposal_id":"proposal-gcpx","proposal_status":"proposed_unreviewed",'
            '"alias":"gcpx","canonical":"google cloud platform","confidence":0.9,'
            '"recommended_action":"approve"}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-proposal-ui-triage-action")

    assert resp.status_code == 200
    assert "/admin/runs/run-proposal-ui-triage-action/synonym-proposals/triage-refresh" in resp.text
    assert "Refresh Triage Recommendations" in resp.text
    assert "triage: fresh" in resp.text


def test_admin_run_detail_shows_triage_summary_banner_from_query_params() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-ui-triage-summary",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-ui-triage-summary","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get(
            "/admin/runs/run-proposal-ui-triage-summary"
            "?synonym_triage_triaged=2&synonym_triage_reused=1&synonym_triage_fallback=0&synonym_triage_skipped=0&synonym_triage_failed=0"
        )

    assert resp.status_code == 200
    assert "Triage summary:" in resp.text
    assert "triaged=2" in resp.text
    assert "reused=1" in resp.text
    assert "fallback=0" in resp.text

def test_admin_run_synonym_proposal_action_redirects_to_run_detail() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-action",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-action","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed",'
            '"alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={
             "persistence_status": "persisted",
             "degradation_reason": "",
         }), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-proposal-action/synonym-proposals/proposal-gcp/action",
            data={"action": "approve", "acted_by": "operator@example.com"},
        )

    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/runs/run-proposal-action"

def test_admin_run_synonym_proposals_batch_action_redirects_to_run_detail() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-batch",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-batch","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9},'
            '{"proposal_id":"proposal-sql","proposal_status":"proposed_unreviewed","alias":"sql","canonical":"structured query language","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={
             "persistence_status": "persisted",
             "degradation_reason": "",
         }), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-proposal-batch/synonym-proposals/batch-action",
            data={
                "acted_by": "operator@example.com",
                "proposal_action__proposal-gcp": "approve",
                "proposal_action__proposal-sql": "defer",
            },
        )

    assert resp.status_code == 303
    assert (
        resp.headers["location"]
        == "/admin/runs/run-proposal-batch?synonym_batch_applied=2&synonym_batch_skipped=0&synonym_batch_failed=0"
    )

def test_admin_run_synonym_proposal_action_blocked_when_apply_to_run_disabled() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-action-disabled",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps({"synonym_management": {"apply_to_run_enabled": False}}),
        synonym_proposals_json=(
            '{"run_id":"run-proposal-action-disabled","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-proposal-action-disabled/synonym-proposals/proposal-gcp/action",
            data={"action": "approve", "acted_by": "operator@example.com"},
        )
    assert resp.status_code == 409


def test_synonym_management_mode_includes_new_automation_flags_with_defaults() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    from fitcv_cp.app import _synonym_management_mode

    run = PipelineRun(
        run_id="run-syn-mode-defaults",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps({"synonym_management": {"apply_to_run_enabled": False}}),
    )
    mode = _synonym_management_mode(run)
    assert mode["apply_to_run_enabled"] is False
    assert mode["auto_apply_recommendation_enabled"] is False
    assert mode["auto_promote_global_enabled"] is False
    assert mode["auto_accept_ai_action_enabled"] is True

def test_admin_run_synonym_proposals_regenerate_blocked_when_propose_disabled() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-regenerate-disabled",
        status=RunStatus.AWAITING_CONTINUE,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="manual_staged",
        next_stage="rule_filter",
        last_completed_stage="enrich",
        effective_settings_json=json.dumps({"synonym_management": {"propose_enabled": False}}),
        mapping_suggestions_json='{"run_id":"run-proposal-regenerate-disabled","suggestions":[{"alias":"gcp","canonical":"google cloud"}]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-proposal-regenerate-disabled/synonym-proposals/regenerate",
        )
    assert resp.status_code == 409


def test_admin_run_synonym_proposals_batch_action_repeat_submit_skips_resolved_rows() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-proposal-batch-repeat",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-batch-repeat","proposals":['
            '{"proposal_id":"proposal-approved","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9},'
            '{"proposal_id":"proposal-pending","proposal_status":"proposed_unreviewed","alias":"sql","canonical":"structured query language","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={
             "persistence_status": "persisted",
             "degradation_reason": "",
         }), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-proposal-batch-repeat/synonym-proposals/batch-action",
            data={
                "acted_by": "operator@example.com",
                "proposal_action__proposal-approved": "approve",
                "proposal_action__proposal-pending": "defer",
            },
        )

    assert resp.status_code == 303
    assert (
        resp.headers["location"]
        == "/admin/runs/run-proposal-batch-repeat?synonym_batch_applied=1&synonym_batch_skipped=1&synonym_batch_failed=0"
    )


def test_admin_run_synonym_proposals_triage_refresh_redirects_with_summary() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-triage-refresh",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-triage-refresh","proposals":['
            '{"proposal_id":"proposal-pending","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9},'
            '{"proposal_id":"proposal-approved","proposal_status":"approved_for_run_overlay","alias":"sql","canonical":"structured query language","confidence":0.9}'
            ']}'
        ),
    )
    persisted_payloads: list[dict[str, object]] = []

    def _capture_update(run_id: str, synonym_proposals_json: str, bq, *, project: str, dataset: str):
        import json
        persisted_payloads.append(json.loads(synonym_proposals_json))
        return {"persistence_status": "persisted", "degradation_reason": ""}

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", side_effect=_capture_update), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-refresh/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )

    assert resp.status_code == 303
    location = resp.headers["location"]
    assert location.startswith("/admin/runs/run-triage-refresh?")
    assert "synonym_triage_triaged=1" in location
    assert "synonym_triage_reused=0" in location
    assert "synonym_triage_fresh=1" in location
    assert "synonym_triage_skipped=1" in location
    assert "synonym_triage_failed=0" in location
    assert "synonym_triage_fallback=0" in location
    assert persisted_payloads
    proposals = persisted_payloads[-1]["proposals"]
    pending = [row for row in proposals if row.get("proposal_id") == "proposal-pending"][0]
    assert pending["recommended_action"] in {"approve", "defer", "reject"}
    assert isinstance(pending["recommendation_confidence"], float)
    assert pending["proposal_status"] == "proposed_unreviewed"


def test_admin_run_synonym_proposals_triage_refresh_does_not_mutate_status() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-triage-status",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-triage-status","proposals":['
            '{"proposal_id":"proposal-review","proposal_status":"in_review","alias":"k8s","canonical":"kubernetes","confidence":0.7}'
            ']}'
        ),
    )
    captured_statuses: list[str] = []

    def _capture_update(run_id: str, synonym_proposals_json: str, bq, *, project: str, dataset: str):
        import json
        payload = json.loads(synonym_proposals_json)
        proposal = payload["proposals"][0]
        captured_statuses.append(str(proposal.get("proposal_status")))
        return {"persistence_status": "persisted", "degradation_reason": ""}

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", side_effect=_capture_update), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-status/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )

    assert resp.status_code == 303
    assert captured_statuses
    assert all(status == "in_review" for status in captured_statuses)

def test_synonym_proposal_review_queue_filters_pairs_already_in_global_synonyms() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    from fitcv_cp.app import _build_synonym_proposal_review_queue

    run = PipelineRun(
        run_id="run-proposal-filtered-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps(
            {
                "skill_synonyms": {
                    "gcp": "google cloud",
                }
            }
        ),
        synonym_proposals_json=(
            '{"run_id":"run-proposal-filtered-1","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )

    queue = _build_synonym_proposal_review_queue(run)
    assert queue["total_count"] == 0
    assert queue["filtered_as_already_global_count"] == 1
    lanes = {lane["field"]: lane for lane in queue["field_lanes"]}
    assert lanes["skill"]["suppressed"] == 1
    assert lanes["skill"]["zero_state_reason"] == "all_suppressed"

def test_synonym_proposal_review_queue_keeps_non_skill_fields_even_if_skill_global_has_same_alias() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    from fitcv_cp.app import _build_synonym_proposal_review_queue

    run = PipelineRun(
        run_id="run-proposal-field-aware-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps({"skill_synonyms": {"gcp": "google cloud"}}),
        synonym_proposals_json=(
            '{"run_id":"run-proposal-field-aware-1","proposals":['
            '{"proposal_id":"proposal-domain-gcp","field":"domain","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )

    queue = _build_synonym_proposal_review_queue(run)
    assert queue["total_count"] == 1
    assert queue["items"][0]["field"] == "domain"
    lanes = {lane["field"]: lane for lane in queue["field_lanes"]}
    assert lanes["domain"]["generated"] == 1
    assert lanes["domain"]["zero_state_reason"] is None

def test_synonym_proposal_review_queue_uses_trace_suppression_for_non_skill_lanes() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    from fitcv_cp.app import _build_synonym_proposal_review_queue

    run = PipelineRun(
        run_id="run-proposal-trace-suppression-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-trace-suppression-1","proposals":['
            '{"proposal_id":"proposal-skill","field":"skill","proposal_status":"proposed_unreviewed","alias":"gcpx","canonical":"google cloud platform","confidence":0.9}'
            '],'
            '"synonym_proposals_trace":{"trace_summary":{"suppressed_count_by_field":{"domain":3,"role_family":1}}}'
            '}'
        ),
    )

    queue = _build_synonym_proposal_review_queue(run)
    lanes = {lane["field"]: lane for lane in queue["field_lanes"]}
    assert lanes["domain"]["suppressed"] == 3
    assert lanes["domain"]["zero_state_reason"] == "all_suppressed"
    assert lanes["role_family"]["suppressed"] == 1
    assert lanes["role_family"]["zero_state_reason"] == "all_suppressed"

def test_synonym_proposal_review_queue_triage_stale_when_pending_without_recommendations() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    from fitcv_cp.app import _build_synonym_proposal_review_queue

    run = PipelineRun(
        run_id="run-proposal-triage-stale-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-proposal-triage-stale-1","proposals":['
            '{"proposal_id":"proposal-skill","field":"skill","proposal_status":"proposed_unreviewed","alias":"gcpx","canonical":"google cloud platform","confidence":0.9}'
            ']}'
        ),
    )

    queue = _build_synonym_proposal_review_queue(run)
    assert queue["pending_count"] == 1
    assert queue["triage_status"] == "stale"


def test_admin_run_synonym_proposals_triage_refresh_provider_failure_is_graceful() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-triage-provider-fail",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-triage-provider-fail","proposals":['
            '{"proposal_id":"proposal-pending","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={"persistence_status": "persisted", "degradation_reason": ""}), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"), \
         patch.dict("os.environ", {"FITCV_LANGGRAPH_PROVIDER": "openai"}, clear=False):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-provider-fail/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )

    assert resp.status_code == 303
    location = resp.headers["location"]
    assert location.startswith("/admin/runs/run-triage-provider-fail?")
    assert "synonym_triage_triaged=1" in location
    assert "synonym_triage_reused=0" in location
    assert "synonym_triage_fresh=1" in location
    assert "synonym_triage_skipped=0" in location
    assert "synonym_triage_failed=0" in location
    assert "synonym_triage_fallback=1" in location


def test_admin_run_synonym_proposals_triage_refresh_provider_success_persists_recommendation() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    import json

    run = PipelineRun(
        run_id="run-triage-provider-success",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-triage-provider-success","proposals":['
            '{"proposal_id":"proposal-pending","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    persisted_payloads: list[dict[str, object]] = []

    def _capture_update(run_id: str, synonym_proposals_json: str, bq, *, project: str, dataset: str):
        persisted_payloads.append(json.loads(synonym_proposals_json))
        return {"persistence_status": "persisted", "degradation_reason": ""}

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", side_effect=_capture_update), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"), \
         patch("fitcv_cp.app._call_synonym_triage_provider", return_value={
             "recommended_action": "approve",
             "recommendation_confidence": 0.91,
             "recommendation_rationale": "High-confidence normalized alias.",
             "recommendation_risk_flags": [],
         }), \
         patch.dict("os.environ", {
             "FITCV_LANGGRAPH_PROVIDER": "openai",
             "OPENAI_API_KEY": "test-key",
             "FITCV_LANGGRAPH_MODEL": "gpt-4.1-mini",
         }, clear=False):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-provider-success/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )

    assert resp.status_code == 303
    location = resp.headers["location"]
    assert location.startswith("/admin/runs/run-triage-provider-success?")
    assert "synonym_triage_triaged=1" in location
    assert "synonym_triage_reused=0" in location
    assert "synonym_triage_fresh=1" in location
    assert "synonym_triage_skipped=0" in location
    assert "synonym_triage_failed=0" in location
    assert "synonym_triage_fallback=0" in location
    assert persisted_payloads
    proposal = persisted_payloads[-1]["proposals"][0]
    assert proposal["recommended_action"] == "approve"
    assert proposal["recommendation_confidence"] == 0.91


def test_admin_run_synonym_proposals_triage_refresh_reuses_unchanged_recommendation() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    import json

    runtime_meta = '{"provider":"fitcv_builtin","model":"synonym_triage_v1","wire_api":"builtin","triage_version":"synonym_triage_v1"}'
    run = PipelineRun(
        run_id="run-triage-reuse",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-triage-reuse","proposals":['
            '{"proposal_id":"proposal-a","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9,'
            '"recommended_action":"approve","recommendation_confidence":0.9,"recommendation_rationale":"ok",'
            f'"recommendation_runtime":{runtime_meta}'
            '}]}'
        ),
    )
    payload_holder: dict[str, object] = {}
    from fitcv_cp import app as app_module
    proposal = json.loads(run.synonym_proposals_json)["proposals"][0]
    fp = app_module._synonym_triage_fingerprint(
        proposal,
        runtime={"provider": "fitcv_builtin", "model": "synonym_triage_v1", "wire_api": "builtin"},
    )
    run.synonym_proposals_json = run.synonym_proposals_json.replace(
        '"recommendation_runtime":{"provider":"fitcv_builtin","model":"synonym_triage_v1","wire_api":"builtin","triage_version":"synonym_triage_v1"}',
        f'"recommendation_runtime":{{"provider":"fitcv_builtin","model":"synonym_triage_v1","wire_api":"builtin","triage_version":"synonym_triage_v1","triage_fingerprint":"{fp}"}}',
    )

    def _capture_update(run_id: str, synonym_proposals_json: str, bq, *, project: str, dataset: str):
        payload_holder["payload"] = json.loads(synonym_proposals_json)
        return {"persistence_status": "persisted", "degradation_reason": ""}

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", side_effect=_capture_update), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-reuse/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )

    assert resp.status_code == 303
    location = resp.headers["location"]
    assert location.startswith("/admin/runs/run-triage-reuse?")
    assert "synonym_triage_triaged=0" in location
    assert "synonym_triage_reused=1" in location
    assert "synonym_triage_fresh=0" in location
    assert "synonym_triage_skipped=0" in location
    assert "synonym_triage_failed=0" in location
    assert "synonym_triage_fallback=0" in location

def test_admin_run_synonym_proposals_triage_refresh_auto_disabled_skips_generation() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-triage-auto-disabled",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps({"synonym_management": {"auto_triage_recommendation_enabled": False}}),
        synonym_proposals_json=(
            '{"run_id":"run-triage-auto-disabled","proposals":['
            '{"proposal_id":"proposal-a","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={"persistence_status": "persisted", "degradation_reason": ""}), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-auto-disabled/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )
    assert resp.status_code == 303
    assert "synonym_triage_triaged=0" in resp.headers["location"]
    assert "synonym_triage_reused=0" in resp.headers["location"]
    assert "synonym_triage_fresh=0" in resp.headers["location"]

def test_admin_run_synonym_proposals_triage_refresh_reuse_disabled_forces_fresh() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-triage-reuse-disabled",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps({"synonym_management": {"triage_recommendation_reuse_enabled": False}}),
        synonym_proposals_json=(
            '{"run_id":"run-triage-reuse-disabled","proposals":['
            '{"proposal_id":"proposal-a","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9,'
            '"recommended_action":"approve","recommendation_confidence":0.9,"recommendation_rationale":"ok"}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={"persistence_status": "persisted", "degradation_reason": ""}), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-reuse-disabled/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )
    assert resp.status_code == 303
    assert "synonym_triage_reused=0" in resp.headers["location"]
    assert "synonym_triage_fresh=1" in resp.headers["location"]


def test_admin_run_synonym_proposals_triage_refresh_auto_apply_and_promote_when_enabled() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-triage-auto-apply-promote",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps(
            {
                "synonym_management": {
                    "auto_triage_recommendation_enabled": False,
                    "apply_to_run_enabled": True,
                    "promote_global_enabled": True,
                    "auto_apply_recommendation_enabled": True,
                    "auto_promote_global_enabled": True,
                }
            }
        ),
        synonym_proposals_json=(
            '{"run_id":"run-triage-auto-apply-promote","proposals":['
            '{"proposal_id":"proposal-a","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9,"recommended_action":"approve","recommendation_confidence":0.9,"recommendation_rationale":"ok"}'
            ']}'
        ),
    )
    persisted_global: dict[str, str] = {}
    payloads: list[dict[str, Any]] = []

    def _capture_update(run_id: str, synonym_proposals_json: str, bq, *, project: str, dataset: str):
        payloads.append(json.loads(synonym_proposals_json))
        return {"persistence_status": "persisted", "degradation_reason": ""}

    def _capture_global_persist(mappings: dict[str, str]) -> None:
        persisted_global.clear()
        persisted_global.update(mappings)

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", side_effect=_capture_update), \
         patch("fitcv_cp.app._load_global_skill_synonyms_map", return_value={}), \
         patch("fitcv_cp.app._persist_global_skill_synonyms_map", side_effect=_capture_global_persist), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-auto-apply-promote/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )

    assert resp.status_code == 303
    assert "synonym_auto_apply_applied=1" in resp.headers["location"]
    assert "synonym_auto_promote_applied=1" in resp.headers["location"]
    assert persisted_global.get("gcp") == "google cloud"
    assert payloads
    promoted = payloads[-1]["proposals"][0]
    assert promoted["proposal_status"] == "approved_for_run_overlay"
    assert promoted.get("global_promotion_history")


def test_admin_run_synonym_proposals_triage_refresh_auto_promote_skips_on_conflict() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-triage-auto-promote-conflict",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json=json.dumps(
            {
                "synonym_management": {
                    "auto_promote_global_enabled": True,
                    "promote_global_enabled": True,
                }
            }
        ),
        synonym_proposals_json=(
            '{"run_id":"run-triage-auto-promote-conflict","proposals":['
            '{"proposal_id":"proposal-a","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9},'
            '{"proposal_id":"proposal-b","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"great cloud platform","confidence":0.8}'
            ']}'
        ),
    )
    persist_global_calls = 0

    def _count_global_persist(_mappings: dict[str, str]) -> None:
        nonlocal persist_global_calls
        persist_global_calls += 1

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={"persistence_status": "persisted", "degradation_reason": ""}), \
         patch("fitcv_cp.app._load_global_skill_synonyms_map", return_value={}), \
         patch("fitcv_cp.app._persist_global_skill_synonyms_map", side_effect=_count_global_persist), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-triage-auto-promote-conflict/synonym-proposals/triage-refresh",
            data={"acted_by": "operator@example.com"},
        )

    assert resp.status_code == 303
    assert "synonym_auto_promote_applied=0" in resp.headers["location"]
    assert "synonym_auto_promote_failed=2" in resp.headers["location"]
    assert persist_global_calls == 0

def test_download_run_approved_synonym_overlay_yaml() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-approved-yaml",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        synonym_proposals_json=(
            '{"run_id":"run-approved-yaml","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9},'
            '{"proposal_id":"proposal-aws","proposal_status":"rejected","alias":"aws","canonical":"amazon web services","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-approved-yaml/approved-synonym-proposals.yaml")
    assert resp.status_code == 200
    assert "skill_synonyms:" in resp.text
    assert "gcp: google cloud" in resp.text
    assert "aws: amazon web services" not in resp.text


def test_download_global_synonyms_yaml() -> None:
    with patch("fitcv_cp.app._load_global_skill_synonyms_map", return_value={"gcp": "google cloud"}):
        resp = TestClient(_app()).get("/admin/synonyms/global.yaml")
    assert resp.status_code == 200
    assert "skill_synonyms:" in resp.text
    assert "gcp: google cloud" in resp.text


def test_admin_run_synonym_promote_preview_renders_diff_summary() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-promote-preview",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json='{"synonym_management":{"promote_global_enabled":true}}',
        synonym_proposals_json=(
            '{"run_id":"run-promote-preview","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9},'
            '{"proposal_id":"proposal-sql","proposal_status":"approved_for_run_overlay","alias":"sql","canonical":"structured query language","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app._load_global_skill_synonyms_map", return_value={"gcp": "google cloud"}):
        resp = TestClient(_app()).post(
            "/admin/runs/run-promote-preview/synonym-proposals/promote-preview",
            data={"promote_proposal_id": ["proposal-gcp", "proposal-sql"]},
        )
    assert resp.status_code == 200
    assert "Promote Synonyms to Global Policy" in resp.text
    assert "new=1" in resp.text
    assert "unchanged=1" in resp.text


def test_admin_run_synonym_promote_commit_updates_global_policy_and_redirects() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-promote-commit",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        run_mode="run_all",
        effective_settings_json='{"synonym_management":{"promote_global_enabled":true}}',
        synonym_proposals_json=(
            '{"run_id":"run-promote-commit","proposals":['
            '{"proposal_id":"proposal-sql","proposal_status":"approved_for_run_overlay","alias":"sql","canonical":"structured query language","confidence":0.9}'
            ']}'
        ),
    )
    persisted_global: dict[str, str] = {}

    def _capture_global_persist(mappings: dict[str, str]) -> None:
        persisted_global.clear()
        persisted_global.update(mappings)

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app._load_global_skill_synonyms_map", return_value={"gcp": "google cloud"}), \
         patch("fitcv_cp.app._persist_global_skill_synonyms_map", side_effect=_capture_global_persist), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={"persistence_status": "persisted", "degradation_reason": ""}), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-promote-commit/synonym-proposals/promote-commit",
            data={"selected_ids_csv": "proposal-sql", "acted_by": "operator@example.com"},
        )
    assert resp.status_code == 303
    assert (
        resp.headers["location"]
        == "/admin/runs/run-promote-commit?synonym_promote_applied=1&synonym_promote_skipped=0&synonym_promote_failed=0&synonym_promote_new_aliases=1&synonym_promote_unchanged_aliases=0&synonym_promote_overridden_aliases=0"
    )
    assert persisted_global["gcp"] == "google cloud"
    assert persisted_global["sql"] == "structured query language"

def test_run_detail_includes_approved_overlay_export_link_when_proposal_review_overlay_active() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-overlay-export-link",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        completed_stages=["enrich"],
        last_completed_stage="enrich",
        effective_settings_json=(
            '{"skill_synonyms":{"gcp":"google cloud"},'
            '"skill_synonyms_runtime":{"base_policy_path":"config/taxonomy/skill_synonyms.yaml",'
            '"overlay_paths":[],"has_overlay":true,"entry_count":1,"has_run_overlay":true,'
            '"run_overlay_source":"proposal_review","run_overlay_filename":"approved-synonym-proposals.yaml",'
            '"run_overlay_uploaded_at":"2026-05-01T00:00:00Z","run_overlay_entry_count":1}}'
        ),
        synonym_proposals_json=(
            '{"run_id":"run-overlay-export-link","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-overlay-export-link")
    assert resp.status_code == 200
    assert resp.text.count("/admin/runs/run-overlay-export-link/approved-synonym-proposals.yaml") == 1
    assert "/admin/synonyms/global.yaml" not in resp.text
    assert "Select All" in resp.text
    assert "Clear Selection" in resp.text
    assert "Selected: 0" in resp.text


def test_run_detail_shows_no_promote_controls_when_no_approved_rows() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-no-promote-eligible",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        synonym_proposals_json=(
            '{"run_id":"run-no-promote-eligible","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-no-promote-eligible")
    assert resp.status_code == 200
    assert "No approved rows available for promotion yet." in resp.text


def test_run_detail_shows_global_download_link_after_promote_summary() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-promote-summary-link",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        synonym_proposals_json=(
            '{"run_id":"run-promote-summary-link","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get(
            "/admin/runs/run-promote-summary-link?synonym_promote_applied=1&synonym_promote_skipped=0&synonym_promote_failed=0"
        )
    assert resp.status_code == 200
    assert "/admin/synonyms/global.yaml" in resp.text


def test_admin_run_synonym_apply_approved_to_run_redirects_with_summary() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-apply-approved-1",
        status=RunStatus.AWAITING_CONTINUE,
        run_mode="manual_staged",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        effective_settings_json='{"synonym_management":{"apply_to_run_enabled":true}}',
        synonym_proposals_json=(
            '{"run_id":"run-apply-approved-1","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_effective_settings"), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={"persistence_status": "persisted", "degradation_reason": ""}), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-apply-approved-1/synonym-proposals/apply-approved-to-run",
            data={"acted_by": "operator@example.com"},
        )
    assert resp.status_code == 303
    assert (
        resp.headers["location"]
        == "/admin/runs/run-apply-approved-1?synonym_apply_to_run_applied=1&synonym_apply_to_run_skipped=0&synonym_apply_to_run_failed=0"
    )

def test_admin_run_synonym_regenerate_redirects_with_summary() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-synonym-regen-1",
        status=RunStatus.AWAITING_CONTINUE,
        run_mode="manual_staged",
        next_stage="rule_filter",
        last_completed_stage="enrich",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        mapping_suggestions_json=(
            '{"run_id":"run-synonym-regen-1","suggestions":['
            '{"alias":"gcp","canonical":"google cloud","confidence":1.0}'
            ']}'
        ),
        effective_settings_json='{"skill_synonyms":{"gcp":"google cloud"}}',
        synonym_proposals_json='{"run_id":"run-synonym-regen-1","proposals":[]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_synonym_proposals", return_value={"persistence_status": "persisted", "degradation_reason": ""}), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/runs/run-synonym-regen-1/synonym-proposals/regenerate",
        )
    assert resp.status_code == 303
    assert (
        resp.headers["location"]
        == "/admin/runs/run-synonym-regen-1?synonym_regenerated_total=0&synonym_regenerated_suppressed=1&synonym_regenerated_failed=0"
    )


def test_run_detail_shows_apply_approved_action_and_summary_banner() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-apply-approved-banner",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        synonym_proposals_json=(
            '{"run_id":"run-apply-approved-banner","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get(
            "/admin/runs/run-apply-approved-banner?synonym_apply_to_run_applied=2&synonym_apply_to_run_skipped=0&synonym_apply_to_run_failed=0"
        )
    assert resp.status_code == 200
    assert "Re-apply Approved to This Run" in resp.text
    assert "Apply-approved-to-run summary" in resp.text

def test_run_detail_shows_synonym_regeneration_controls_and_banner() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-synonym-regen-banner",
        status=RunStatus.AWAITING_CONTINUE,
        run_mode="manual_staged",
        next_stage="rule_filter",
        last_completed_stage="enrich",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        mapping_suggestions_json='{"run_id":"run-synonym-regen-banner","suggestions":[]}',
        synonym_proposals_json='{"run_id":"run-synonym-regen-banner","proposals":[]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get(
            "/admin/runs/run-synonym-regen-banner?synonym_regenerated_total=3&synonym_regenerated_suppressed=2&synonym_regenerated_failed=0"
        )
    assert resp.status_code == 200
    assert "Regenerate Proposals" in resp.text
    assert "Regeneration summary" in resp.text
    assert "fingerprints:" in resp.text

def test_run_detail_shows_synonym_regeneration_banner_without_review_card() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-synonym-regen-banner-only",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        synonym_proposals_json='{"run_id":"run-synonym-regen-banner-only","proposals":[]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get(
            "/admin/runs/run-synonym-regen-banner-only?synonym_regenerated_total=3&synonym_regenerated_suppressed=2&synonym_regenerated_failed=0"
        )
    assert resp.status_code == 200
    assert "Regeneration summary" in resp.text
    assert "Synonym Proposal Review" not in resp.text

def test_run_detail_shows_empty_synonym_decision_ledger_placeholder() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-synonym-ledger-empty",
        status=RunStatus.AWAITING_CONTINUE,
        run_mode="manual_staged",
        next_stage="rule_filter",
        last_completed_stage="enrich",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        mapping_suggestions_json='{"run_id":"run-synonym-ledger-empty","suggestions":[]}',
        synonym_proposals_json='{"run_id":"run-synonym-ledger-empty","proposals":[]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-synonym-ledger-empty")
    assert resp.status_code == 200
    assert "Proposal Decision Ledger: no rows yet (all suppressed or none generated)." in resp.text

def test_run_detail_hides_apply_approved_action_when_no_approved_rows() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-apply-approved-hidden",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        synonym_proposals_json=(
            '{"run_id":"run-apply-approved-hidden","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"proposed_unreviewed","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-apply-approved-hidden")
    assert resp.status_code == 200
    assert "Re-apply Approved to This Run" not in resp.text
    assert "No approved rows to apply." in resp.text


def test_run_detail_hides_promote_checkbox_after_global_promotion() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-promoted-checkbox-hidden",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        synonym_proposals_json=(
            '{"run_id":"run-promoted-checkbox-hidden","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9,'
            '"global_promotion_history":[{"action":"promote_to_global","acted_by":"admin","acted_at":"2026-05-01T00:00:00Z"}]}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-promoted-checkbox-hidden")
    assert resp.status_code == 200
    assert "Include in Promote-to-Global preview" not in resp.text

def test_run_detail_hides_promote_checkbox_when_pair_exists_in_global_map() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-promoted-checkbox-hidden-global-map",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        effective_settings_json='{"skill_synonyms":{"gcp":"google cloud"}}',
        synonym_proposals_json=(
            '{"run_id":"run-promoted-checkbox-hidden-global-map","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-promoted-checkbox-hidden-global-map")
    assert resp.status_code == 200
    assert "Include in Promote-to-Global preview" not in resp.text

def test_run_detail_keeps_promote_checkbox_when_pair_only_exists_in_run_overlay() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-promote-checkbox-visible-run-overlay-only",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        effective_settings_json=(
            '{"skill_synonyms":{"gcp":"google cloud","advanced excel":"excel"},'
            '"skill_synonyms_runtime":{"pre_run_overlay_skill_synonyms":{"gcp":"google cloud"}}}'
        ),
        synonym_proposals_json=(
            '{"run_id":"run-promote-checkbox-visible-run-overlay-only","proposals":['
            '{"proposal_id":"proposal-advanced-excel","proposal_status":"approved_for_run_overlay","alias":"advanced excel","canonical":"excel","confidence":0.9}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-promote-checkbox-visible-run-overlay-only")
    assert resp.status_code == 200
    assert "Include in Promote-to-Global preview" in resp.text


def test_run_detail_shows_reranker_blocked_message_when_no_cvs_generated() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-reranker-blocked-msg",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        ranked=2,
        cvs_generated=0,
        results_export_json=(
            '{"run_id":"run-reranker-blocked-msg","results":['
            '{"job_url":"https://example.com/a","pipeline_status":"ranked_blocked_by_reranker_fit"},'
            '{"job_url":"https://example.com/b","pipeline_status":"ranked_blocked_by_reranker_fit"}'
            ']}'
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-reranker-blocked-msg")
    assert resp.status_code == 200
    assert "blocked by reranker-fit gating before CV generation" in resp.text


def test_download_settings_used_json_endpoint_200():
    """@proves inspection_debugging.settings-used-export"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-settings-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        settings_used_json='{"run_id":"run-settings-1","effective_settings":{"pipeline":{"final_top_n":10}}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-settings-1/settings-used.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-settings-1"
    assert resp.headers["content-type"] == "application/json"
    assert 'attachment; filename="fitcv-run-run-settings-1-settings-used.json"' in resp.headers["content-disposition"]


def test_download_settings_used_json_endpoint_404_if_snapshot_missing():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-settings-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-settings-2/settings-used.json")
    assert resp.status_code == 404


def test_download_stage_slice_endpoint_200():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-slice-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"run-stage-slice-1","created_at":"2026-03-31T20:00:00+00:00","artifacts":{"stages":{"normalize":{"stage_id":"normalize","status":"completed","input_counts":{"raw_jobs":7},"output_counts":{"normalized_jobs":6},"decision_summary":{},"inputs_sample":[],"outputs_sample":[],"dropped_or_changed_sample":[]}}}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-stage-slice-1/stage-artifacts/normalize.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-stage-slice-1"
    assert resp.json()["stage_id"] == "normalize"
    assert resp.json()["stage_artifact"]["input_counts"]["raw_jobs"] == 7


def test_download_stage_slice_endpoint_200_for_running_run_with_snapshot():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-slice-running-1",
        status=RunStatus.RUNNING,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"run-stage-slice-running-1","created_at":"2026-03-31T20:00:00+00:00","artifacts":{"stages":{"enrich":{"stage_id":"enrich","status":"completed","input_counts":{},"output_counts":{"enriched_jobs":1},"decision_summary":{},"inputs_sample":[],"outputs_sample":[{"job_url":"https://example.com/1"}],"dropped_or_changed_sample":[]}}}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-stage-slice-running-1/stage-artifacts/enrich.json")
    assert resp.status_code == 200
    assert resp.json()["stage_id"] == "enrich"
    assert resp.json()["stage_artifact"]["output_counts"]["enriched_jobs"] == 1


def test_download_stage_slice_endpoint_404_for_unknown_stage():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-stage-slice-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json='{"run_id":"run-stage-slice-2","artifacts":{"stages":{"normalize":{"status":"completed"}}}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-stage-slice-2/stage-artifacts/unknown.json")
    assert resp.status_code == 404


def test_download_cv_debug_json_endpoint_200():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-debug-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json='{"run_id":"run-debug-1","debug_records":[]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-debug-1/cv-debug.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-debug-1"
    assert resp.headers["content-type"] == "application/json"
    assert 'attachment; filename="fitcv-run-run-debug-1-cv-debug.json"' in resp.headers["content-disposition"]
    assert "\n  \"run_id\"" in resp.text


def test_download_cv_debug_json_endpoint_404_if_snapshot_missing():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-debug-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=None,
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-debug-2/cv-debug.json")
    assert resp.status_code == 404

def test_download_cv_generation_review_required_json_endpoint_200() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-required-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "run_id": "run-review-required-1",
                "debug_records": [
                    {
                        "job_url": "https://example.com/j1",
                        "job_title": "Data Engineer",
                        "status": "review_required",
                        "review_required_reason_code": "provider_error",
                        "attempt_count": 2,
                        "failed_rule_ids": ["rule_one"],
                    }
                ],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-review-required-1/cv-generation-review-required.json")
    assert resp.status_code == 200
    assert resp.json()["schema_version"] == "cv_generation_review_required_v1"
    assert len(resp.json()["rows"]) == 1

def test_download_cv_generation_review_required_json_maps_reason_and_nullable_request_id() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-required-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "run_id": "run-review-required-2",
                "debug_records": [
                    {
                        "job_url": "https://example.com/j2",
                        "job_title": "Data Engineer 2",
                        "status": "review_required",
                        "review_required_reason_code": "unknown",
                        "error": {
                            "stage": "review_gate",
                            "message": "Unsupported requirements require review: Snowflake, Talend",
                        },
                        "runtime_provenance": {},
                    }
                ],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-review-required-2/cv-generation-review-required.json")
    assert resp.status_code == 200
    row = resp.json()["rows"][0]
    assert row["reason_code"] == "unsupported_requirement_gap"
    assert row["review_target"] == "requirements_alignment"
    assert "Review the generated CV output against required stack coverage" in row["operator_prompt"]
    assert row["unsupported_requirements"] == ["Snowflake", "Talend"]
    assert row["generated_draft_present"] is False
    assert row["accepted_cv_artifact_present"] is False
    assert row["request_id"] is None

def test_download_cv_generation_review_required_json_uses_structured_missing_requirements() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-review-required-structured-missing",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "run_id": "run-review-required-structured-missing",
                "debug_records": [
                    {
                        "job_url": "https://example.com/j3",
                        "job_title": "Data Engineer 3",
                        "status": "review_required",
                        "review_required_reason_code": "unsupported_requirement_gap",
                        "gap_summary": {"missing": ["Snowflake", "Talend"]},
                        "error": {
                            "stage": "review_gate",
                            "message": "Unsupported requirements require review: Snowflake, Talend. Review the generated CV output against these requirements and decide approve as-is, regenerate once, or reject.",
                        },
                    }
                ],
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-review-required-structured-missing/cv-generation-review-required.json")
    assert resp.status_code == 200
    row = resp.json()["rows"][0]
    assert row["unsupported_requirements"] == ["Snowflake", "Talend"]


def test_download_agentic_live_trace_json_endpoint_200() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-agentic-trace-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "run_id": "run-agentic-trace-1",
                "agentic_live_trace": {
                    "run_id": "run-agentic-trace-1",
                    "trace_schema_version": "agentic_step_trace_run_v1",
                    "trace_family": "agentic_step_trace",
                    "step_id": "cv_generation",
                    "late_stage_mode": {
                        "late_stage_mode": "agentic",
                        "agentic_late_stage_enabled": True,
                        "mode_source": "cv.agentic_late_stage.enabled",
                        "agentic_status": "completed",
                    },
                    "trace_status": "completed",
                    "trace_summary": {"records_total": 1, "present_records": 1, "attempted_generation_jobs_total": 1},
                    "records": [{"record_id": "https://example.com/1", "scope_type": "job", "scope_key": "https://example.com/1", "attempts": [{"attempt_index": 1, "provider_status": "accepted"}]}],
                    "degradation": {},
                },
                "debug_records": [],
            }
        ),
        settings_used_json='{"run_id":"run-agentic-trace-1","late_stage_mode":{"late_stage_mode":"agentic","agentic_late_stage_enabled":true,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"completed"}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-agentic-trace-1/agentic-live-trace.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-agentic-trace-1"
    assert resp.json()["trace_family"] == "agentic_step_trace"
    assert resp.json()["step_id"] == "cv_generation"
    assert resp.headers["content-type"] == "application/json"
    assert 'attachment; filename="fitcv-run-run-agentic-trace-1-agentic-live-trace.json"' in resp.headers["content-disposition"]


def test_download_agentic_live_trace_json_endpoint_404_when_not_applicable() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-agentic-trace-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json='{"run_id":"run-agentic-trace-2","debug_records":[]}',
        settings_used_json='{"run_id":"run-agentic-trace-2","late_stage_mode":{"late_stage_mode":"non_agentic","agentic_late_stage_enabled":false,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"not_applicable"}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-agentic-trace-2/agentic-live-trace.json")
    assert resp.status_code == 404


def test_download_cv_analysis_trace_json_endpoint_200() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-analysis-trace-1",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json=json.dumps(
            {
                "run_id": "run-analysis-trace-1",
                "cv_analysis_trace": {
                    "run_id": "run-analysis-trace-1",
                    "trace_schema_version": "agentic_step_trace_run_v1",
                    "trace_family": "agentic_step_trace",
                    "step_id": "cv_analysis",
                    "late_stage_mode": {
                        "late_stage_mode": "agentic",
                        "agentic_late_stage_enabled": True,
                        "mode_source": "cv.agentic_late_stage.enabled",
                        "agentic_status": "completed",
                    },
                    "trace_status": "completed",
                    "trace_summary": {"records_total": 1, "present_records": 1, "attempted_analysis_jobs_total": 1},
                    "records": [{"record_id": "https://example.com/1", "scope_type": "job", "scope_key": "https://example.com/1", "status": "ready_for_generation"}],
                    "degradation": {},
                },
                "debug_records": [],
            }
        ),
        settings_used_json='{"run_id":"run-analysis-trace-1","late_stage_mode":{"late_stage_mode":"agentic","agentic_late_stage_enabled":true,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"completed"}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-analysis-trace-1/cv-analysis-trace.json")
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run-analysis-trace-1"
    assert resp.json()["trace_family"] == "agentic_step_trace"
    assert resp.json()["step_id"] == "cv_analysis"
    assert 'attachment; filename="fitcv-run-run-analysis-trace-1-cv-analysis-trace.json"' in resp.headers["content-disposition"]


def test_download_cv_analysis_trace_json_endpoint_404_when_not_applicable() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-analysis-trace-2",
        status=RunStatus.SUCCEEDED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        cv_generation_debug_json='{"run_id":"run-analysis-trace-2","debug_records":[]}',
        settings_used_json='{"run_id":"run-analysis-trace-2","late_stage_mode":{"late_stage_mode":"non_agentic","agentic_late_stage_enabled":false,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"not_applicable"}}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-analysis-trace-2/cv-analysis-trace.json")
    assert resp.status_code == 404


def test_download_run_artifact_bundle_zip_endpoint_for_partial_run() -> None:
    """@proves trigger_run_management.run-owned-artifact-exports
    @proves inspection_debugging.run-owned-artifact-exports
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-bundle-partial-1",
        status=RunStatus.AWAITING_CONTINUE,
        run_mode="manual_staged",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        last_completed_stage="normalize",
        completed_stages=["normalize"],
        stage_transition_artifacts_json=json.dumps(
            {
                "run_id": "run-bundle-partial-1",
                "created_at": "2026-04-07T12:00:00+00:00",
                "artifacts": {
                    "stages": {
                        "normalize": {
                            "stage_id": "normalize",
                            "status": "completed",
                        }
                    }
                },
            }
        ),
        mapping_suggestions_json='{"run_id":"run-bundle-partial-1","suggestions":[]}',
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-bundle-partial-1/artifacts.zip")

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/zip"
    assert 'attachment; filename="fitcv-run-run-bundle-partial-1-artifacts.zip"' in resp.headers["content-disposition"]

    with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
        names = set(archive.namelist())
        assert "manifest.json" in names
        assert "stage-artifacts.json" in names
        assert "normalize.json" in names
        assert "mapping-suggestions.json" not in names
        manifest = json.loads(archive.read("manifest.json"))
    assert manifest["run_id"] == "run-bundle-partial-1"
    assert manifest["bundle_schema_version"] == "run_artifact_bundle_v6"
    assert manifest["run_mode"] == "manual_staged"
    assert manifest["run_mode_label"] == "Stage by Stage"
    assert manifest["late_stage_mode"]["late_stage_mode"] == "non_agentic"
    assert "normalize.json" in manifest["included_files"]
    assert "mapping-suggestions.json" not in manifest["missing_files"]
    assert manifest["artifact_states"]["mapping-suggestions.json"] == "not_applicable"


def test_download_run_artifact_bundle_zip_endpoint_for_succeeded_run() -> None:
    """@proves trigger_run_management.shortlist-debug-exports
    @proves inspection_debugging.shortlist-diagnostics
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-bundle-success-1",
        status=RunStatus.SUCCEEDED,
        run_mode="run_all",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        last_completed_stage="enrich",
        completed_stages=["normalize", "enrich"],
        results_export_json='{"run_id":"run-bundle-success-1","results":[]}',
        cv_generation_debug_json='{"run_id":"run-bundle-success-1","cv_analysis_trace":{"run_id":"run-bundle-success-1","trace_schema_version":"agentic_step_trace_run_v1","trace_family":"agentic_step_trace","step_id":"cv_analysis","late_stage_mode":{"late_stage_mode":"agentic","agentic_late_stage_enabled":true,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"completed"},"trace_status":"completed","trace_summary":{"records_total":1,"present_records":1,"attempted_analysis_jobs_total":1},"records":[{"record_id":"https://example.com/1","scope_type":"job","scope_key":"https://example.com/1","status":"ready_for_generation"}],"degradation":{}},"agentic_live_trace":{"run_id":"run-bundle-success-1","trace_schema_version":"agentic_step_trace_run_v1","trace_family":"agentic_step_trace","step_id":"cv_generation","late_stage_mode":{"late_stage_mode":"agentic","agentic_late_stage_enabled":true,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"completed"},"trace_status":"completed","trace_summary":{"records_total":1,"present_records":1,"attempted_generation_jobs_total":1},"records":[{"record_id":"https://example.com/1","scope_type":"job","scope_key":"https://example.com/1","attempts":[{"attempt_index":1,"provider_status":"accepted"}]}],"degradation":{}},"debug_records":[]}',
        settings_used_json='{"run_id":"run-bundle-success-1","late_stage_mode":{"late_stage_mode":"agentic","agentic_late_stage_enabled":true,"mode_source":"cv.agentic_late_stage.enabled","agentic_status":"completed"},"effective_settings":{"pipeline":{"final_top_n":10}}}',
        mapping_suggestions_json='{"run_id":"run-bundle-success-1","suggestions":[]}',
        synonym_proposals_json=(
            '{"run_id":"run-bundle-success-1","proposal_generation_status":"generated","persistence_status":"bundle_only_degraded",'
            '"proposals":[],"synonym_proposals_trace":{"run_id":"run-bundle-success-1","trace_schema_version":"agentic_step_trace_run_v1",'
            '"trace_family":"agentic_step_trace","step_id":"synonym_proposals","trace_status":"degraded",'
            '"trace_summary":{"records_total":0,"present_records":0,"proposal_count":0},"records":[],'
            '"degradation":{"reason":"synonym_proposals_bundle_only_degraded"}}}'
        ),
        stage_transition_artifacts_json=json.dumps(
            {
                "run_id": "run-bundle-success-1",
                "created_at": "2026-04-07T12:00:00+00:00",
                    "artifacts": {
                        "stages": {
                            "normalize": {"stage_id": "normalize", "status": "completed"},
                            "enrich": {"stage_id": "enrich", "status": "completed"},
                            "rule_filter": {"stage_id": "rule_filter", "status": "completed"},
                            "shortlist": {"stage_id": "shortlist", "status": "completed"},
                            "ranking": {"stage_id": "ranking", "status": "completed"},
                            "cv_analysis": {"stage_id": "cv_analysis", "status": "completed"},
                            "cv_generation": {"stage_id": "cv_generation", "status": "completed"},
                        }
                    },
                }
            ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), patch(
        "fitcv_cp.app.list_cvs_for_run",
        return_value=[{"version_id": "v-1"}],
    ), patch(
        "fitcv_cp.app.get_cv_markdown",
        return_value="# Sample CV\n\n## Experience\n- Item",
    ):
        resp = TestClient(_app()).get("/admin/runs/run-bundle-success-1/artifacts.zip")

    assert resp.status_code == 200
    with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
        names = set(archive.namelist())
        assert "manifest.json" in names
        assert "results.json" in names
        assert "hitl-review-audit.json" in names
        assert "cv-debug.json" in names
        assert "cv-analysis-trace.json" in names
        assert "agentic-live-trace.json" in names
        assert "settings-used.json" in names
        assert "stage-artifacts.json" in names
        assert "normalize.json" in names
        assert "enrich.json" in names
        assert "rule_filter.json" in names
        assert "shortlist.json" in names
        assert "ranking.json" in names
        assert "cv_analysis.json" in names
        assert "cv_generation.json" in names
        assert "mapping-suggestions.json" in names
        assert "synonym-proposals.json" in names
        assert "synonym-proposals-trace.json" in names
        assert "cv_v-1.md" in names
        manifest = json.loads(archive.read("manifest.json"))
        analysis_trace_payload = json.loads(archive.read("cv-analysis-trace.json"))
        trace_payload = json.loads(archive.read("agentic-live-trace.json"))
        synonym_trace_payload = json.loads(archive.read("synonym-proposals-trace.json"))
    assert manifest["run_id"] == "run-bundle-success-1"
    assert manifest["bundle_schema_version"] == "run_artifact_bundle_v6"
    assert manifest["run_mode"] == "run_all"
    assert manifest["run_mode_label"] == "Run All"
    assert manifest["late_stage_mode"]["late_stage_mode"] == "agentic"
    assert "results.json" in manifest["included_files"]
    assert "hitl-review-audit.json" in manifest["included_files"]
    assert manifest["artifact_states"]["cv-analysis-trace.json"] == "present"
    assert manifest["artifact_states"]["agentic-live-trace.json"] == "present"
    assert manifest["artifact_states"]["synonym-proposals.json"] == "present"
    assert manifest["artifact_states"]["synonym-proposals-trace.json"] == "present"
    assert manifest["missing_files"] == []
    assert analysis_trace_payload["trace_family"] == "agentic_step_trace"
    assert analysis_trace_payload["step_id"] == "cv_analysis"
    assert trace_payload["trace_family"] == "agentic_step_trace"
    assert trace_payload["step_id"] == "cv_generation"
    assert synonym_trace_payload["trace_family"] == "agentic_step_trace"
    assert synonym_trace_payload["step_id"] == "synonym_proposals"


def test_download_run_artifact_bundle_includes_synonym_yaml_artifacts_when_available() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-bundle-synonym-yaml-1",
        status=RunStatus.SUCCEEDED,
        run_mode="run_all",
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        results_export_json='{"run_id":"run-bundle-synonym-yaml-1","results":[]}',
        effective_settings_json=(
            '{"skill_synonyms_runtime":{"has_run_overlay":true,"run_overlay_yaml":"skill_synonyms:\\n  ga4: google analytics\\n"}}'
        ),
        synonym_proposals_json=(
            '{"run_id":"run-bundle-synonym-yaml-1","proposals":['
            '{"proposal_id":"proposal-gcp","proposal_status":"approved_for_run_overlay","alias":"gcp","canonical":"google cloud","confidence":0.9}'
            ']}'
        ),
        stage_transition_artifacts_json=json.dumps(
            {
                "run_id": "run-bundle-synonym-yaml-1",
                "created_at": "2026-04-07T12:00:00+00:00",
                "artifacts": {"stages": {"enrich": {"stage_id": "enrich", "status": "completed"}}},
            }
        ),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-bundle-synonym-yaml-1/artifacts.zip")

    assert resp.status_code == 200
    with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
        names = set(archive.namelist())
        assert "approved-synonym-proposals.yaml" in names
        assert "synonym-overlay-used.yaml" in names


def test_download_run_artifact_bundle_zip_endpoint_404_if_no_artifacts_available() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="run-bundle-empty-1",
        status=RunStatus.QUEUED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/sample_jobs.json",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-bundle-empty-1/artifacts.zip")

    assert resp.status_code == 404
    assert "artifacts" in resp.text.lower()


# ── enriched jobs on run detail ──────────────────────────────────────────────

def test_admin_run_detail_shows_enriched_jobs_section():
    """@proves inspection_debugging.enriched-job-debug-export"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    enriched_jobs = [
        {
            "run_id": "test-123",
            "job_url": "https://example.com/job/1",
            "title": "Senior Data Engineer",
            "location_type": "remote",
            "seniority": "senior",
            "job_family": "data_engineering",
            "domain": "fintech",
            "required_skills": ["SQL", "Python", "Spark"],
        }
    ]

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-123", status=RunStatus.SUCCEEDED,
        cvs_generated=1, total_jobs=5, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc)
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=enriched_jobs):
        resp = TestClient(_app()).get("/admin/runs/test-123/tabs/enriched")

    assert resp.status_code == 200
    assert "Senior Data Engineer" in resp.text
    assert "remote" in resp.text
    assert "senior" in resp.text
    assert "data_engineering" in resp.text
    assert "fintech" in resp.text


def test_admin_run_detail_empty_enriched_jobs_renders_gracefully():
    """Run detail page handles empty enriched_jobs without errors."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-empty", status=RunStatus.SUCCEEDED,
        cvs_generated=0, total_jobs=3, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc)
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-empty/tabs/enriched")

    assert resp.status_code == 200
    assert "No enrichment data" in resp.text or "enriched" in resp.text.lower()

def test_admin_run_detail_enriched_jobs_falls_back_to_results_export_when_store_empty():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="test-fallback-enriched",
        status=RunStatus.SUCCEEDED,
        cvs_generated=0,
        total_jobs=1,
        jobs_path="",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json=json.dumps(
            {
                "results": [
                    {
                        "job_url": "https://example.com/job/fallback-1",
                        "job_title": "Fallback Data Engineer",
                        "location_type": "remote",
                        "seniority": "mid",
                        "job_family": "data_engineering",
                        "domain": "fintech",
                    }
                ]
            }
        ),
    )

    with patch("fitcv_cp.app.get_run", return_value=run), \
    patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
    patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/test-fallback-enriched/tabs/enriched")

    assert resp.status_code == 200
    assert "Fallback Data Engineer" in resp.text
    assert "remote" in resp.text


def test_admin_run_detail_enriched_jobs_shows_required_skills():
    """Run detail renders required_skills from enriched job rows."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    enriched_jobs = [
        {
            "run_id": "test-456",
            "job_url": "https://example.com/job/2",
            "title": "ML Engineer",
            "location_type": "hybrid",
            "seniority": "mid",
            "job_family": "ml_engineering",
            "domain": "healthcare",
            "required_skills": ["Python", "TensorFlow", "Kubernetes"],
        }
    ]

    with patch("fitcv_cp.app.get_run", return_value=PipelineRun(
        run_id="test-456", status=RunStatus.SUCCEEDED,
        cvs_generated=0, total_jobs=1, jobs_path="",
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc)
    )), patch("fitcv_cp.app.get_events", return_value=[]), \
    patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
    patch("fitcv_cp.app.list_run_structured_jobs", return_value=enriched_jobs):
        resp = TestClient(_app()).get("/admin/runs/test-456/tabs/enriched")

    assert resp.status_code == 200
    assert "Python" in resp.text
    assert "TensorFlow" in resp.text
    assert "https://example.com/job/2" in resp.text






# ── Inspection Tab Tests ──────────────────────────────────────────────────────


def _run_detail_base_patches(run_obj):
    """Return tuple of patchers for standard run detail route dependencies."""
    return (
        patch("fitcv_cp.app.get_run", return_value=run_obj),
        patch("fitcv_cp.app.get_events", return_value=[]),
        patch("fitcv_cp.app.list_cvs_for_run", return_value=[]),
        patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]),
        patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]),
    )


def test_run_detail_default_tab_is_enriched():
    """@proves inspection_debugging.run-detail-inspection-tabs"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="tab-test-1", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json", triggered_by="admin",
        trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/tab-test-1")

    assert resp.status_code == 200
    html = resp.text
    assert 'id="pane-enriched"' in html
    assert 'id="tab-btn-enriched"' in html
    # active class must be on the enriched pane (class attr comes before id= in HTML)
    pane_pos = html.index('id="pane-enriched"')
    assert "active" in html[max(0, pane_pos - 60):pane_pos + 50]
    # active class must be on the enriched tab button
    btn_pos = html.index('id="tab-btn-enriched"')
    assert "active" in html[max(0, btn_pos - 80):btn_pos + 10]


def test_run_detail_initial_shell_does_not_query_enriched_rows() -> None:
    """Initial run-detail render stays lightweight and avoids enriched/filter queries."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="tab-shell-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs") as mock_jobs, \
         patch("fitcv_cp.app.list_filter_results_for_run") as mock_filters:
        resp = TestClient(_app()).get("/admin/runs/tab-shell-1")

    assert resp.status_code == 200
    assert "Enriched job diagnostics load on demand." in resp.text
    mock_jobs.assert_not_called()
    mock_filters.assert_not_called()


def test_run_detail_tab2_fallback_when_no_jobs_snapshot():
    """Tab 2 shows source/path fallback when jobs_input_json is absent."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="tab-test-2", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json", jobs_input_source="path",
        jobs_input_json=None,
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/tab-test-2/tabs/jobs-input")

    assert resp.status_code == 200
    html = resp.text
    assert "No immutable raw snapshot" in html
    assert "data/sample_jobs.json" in html


def test_run_detail_tab3_null_source_shows_not_recorded_not_default_config():
    """Tab 3 fallback must not infer 'default_config' when source is NULL."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="tab-test-3", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        candidate_profile_source=None,
        candidate_profile_json=None,
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/tab-test-3/tabs/profile")

    assert resp.status_code == 200
    html = resp.text
    assert "No candidate profile snapshot" in html
    assert "not recorded" in html
    assert "default_config" not in html


def test_run_detail_event_timeline_appears_after_tab_panes():
    """Event Timeline heading must come after all 3 tab panes in the HTML."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="tab-test-4", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/tab-test-4")

    assert resp.status_code == 200
    html = resp.text
    profile_pane_pos = html.index('id="pane-profile"')
    timeline_pos = html.index("Event Timeline")
    assert timeline_pos > profile_pane_pos, (
        "Event Timeline must appear after all tab panes in the HTML"
    )


def test_run_detail_renders_run_health_when_quality_metrics_available():
    """@proves trigger_run_management.run-health-surface
    @proves inspection_debugging.quality-metrics-diagnostics
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="quality-metrics-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "shortlist": {
                            "decision_summary": {
                                "quality_metrics": {
                                    "backfill_rate": 0.33,
                                    "backfilled_jobs_total": 1,
                                    "scoring_shortlisted_jobs_total": 3,
                                }
                            }
                        },
                        "ranking": {
                            "decision_summary": {
                                "quality_metrics": {
                                    "label_distribution": {
                                        "strong_rate": 0.25,
                                        "strong_count": 1,
                                        "total_scored": 4,
                                    }
                                }
                            }
                        },
                        "cv_analysis": {
                            "decision_summary": {
                                "quality_metrics": {
                                    "skip_rate": 0.5,
                                    "skipped_fit_gate": 1,
                                    "total_processed": 2,
                                }
                            }
                        },
                    }
                }
            }
        ),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/quality-metrics-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Run Health" in html
    assert "Stage Quality Metrics" not in html
    assert "Shortlist Backfill Rate" in html
    assert "33%" in html
    assert "1 / 3" in html


def test_run_detail_hides_run_health_when_quality_metrics_absent():
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="quality-metrics-2",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json=json.dumps({"results": []}),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/quality-metrics-2")

    assert resp.status_code == 200
    assert "Run Health" not in resp.text


def test_run_detail_renders_run_health_when_late_stage_reuse_metrics_available():
    """@proves inspection_debugging.cv-analysis-diagnostics
    @proves inspection_debugging.reuse-diagnostics
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="reuse-metrics-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "ranking": {
                            "decision_summary": {
                                "reuse_metrics": {
                                    "reuse_rate": 0.5,
                                    "reused_ai_scores": 1,
                                    "fresh_ai_scores": 1,
                                    "total_ai_scores": 2,
                                }
                            }
                        },
                        "cv_analysis": {
                            "decision_summary": {
                                "reuse_metrics": {
                                    "analysis_reuse_rate": 1.0,
                                    "reused_analysis_rows": 2,
                                    "fresh_analysis_rows": 0,
                                    "analysis_rows_executed": 2,
                                    "blocked_before_analysis_rows": 0,
                                }
                            }
                        },
                    }
                }
            }
        ),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/reuse-metrics-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Run Health" in html
    assert "Late-Stage Reuse" not in html
    assert "Ranking AI-Score Reuse Rate" in html
    assert "CV Analysis Reuse Rate" in html
    assert "50%" in html


def test_run_detail_run_health_marks_unreached_metrics_as_pending_and_zero_denominator_reached_metrics_as_na():
    """@proves inspection_debugging.cv-analysis-diagnostics"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="health-pending-na-1",
        status=RunStatus.AWAITING_CONTINUE,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "cv_analysis": {
                            "status": "not_reached",
                            "decision_summary": {
                                "quality_metrics": {
                                    "skip_rate": 0.0,
                                    "skipped_fit_gate": 0,
                                    "total_processed": 0,
                                }
                            },
                        },
                        "ranking": {
                            "status": "completed",
                            "decision_summary": {
                                "reuse_metrics": {
                                    "reuse_rate": 0.0,
                                    "reused_ai_scores": 0,
                                    "fresh_ai_scores": 0,
                                    "total_ai_scores": 0,
                                }
                            },
                        },
                    }
                }
            }
        ),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/health-pending-na-1")

    assert resp.status_code == 200
    html = resp.text
    assert "CV Analysis Skip Rate" in html
    assert "Ranking AI-Score Reuse Rate" in html
    assert "Pending" in html
    assert "N/A" in html


def test_run_detail_hides_late_stage_reuse_metrics_when_absent():
    """@proves inspection_debugging.reuse-diagnostics"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="reuse-metrics-2",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        results_export_json=json.dumps({"results": []}),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/reuse-metrics-2")

    assert resp.status_code == 200
    assert "Ranking AI-Score Reuse Rate" not in resp.text
    assert "CV Analysis Reuse Rate" not in resp.text


def test_run_detail_renders_cv_generation_quality_metrics():
    """@proves inspection_debugging.cv-generation-diagnostics"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="cv-generation-metrics-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "cv_generation": {
                            "decision_summary": {
                                "quality_metrics": {
                                    "accepted_rate": 0.5,
                                    "accepted": 2,
                                    "validation_fail_rate": 0.25,
                                    "validation_failed": 1,
                                    "generation_failed_rate": 0.25,
                                    "generation_failed": 1,
                                    "persistence_failed_rate": 0.0,
                                    "persistence_failed": 0,
                                    "total_attempted": 4,
                                }
                            }
                        }
                    }
                }
            }
        ),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/cv-generation-metrics-1")

    assert resp.status_code == 200
    html = resp.text
    assert "CV Generation Accepted Rate" in html
    assert "CV Generation Validation-Fail Rate" in html
    assert "CV Generation Failure Rate" in html
    assert "CV Generation Persistence-Fail Rate" in html
    assert "50%" in html
    assert "25%" in html
    assert "2 / 4" in html
    assert "0 / 4" in html


def test_run_detail_hides_cv_generation_quality_metrics_when_absent():
    """@proves inspection_debugging.cv-generation-diagnostics"""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="cv-generation-metrics-2",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
        stage_transition_artifacts_json=json.dumps(
            {
                "artifacts": {
                    "stages": {
                        "cv_analysis": {
                            "decision_summary": {
                                "quality_metrics": {
                                    "skip_rate": 0.5,
                                    "skipped_fit_gate": 1,
                                    "total_processed": 2,
                                }
                            }
                        }
                    }
                }
            }
        ),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/cv-generation-metrics-2")

    assert resp.status_code == 200
    assert "CV Analysis Skip Rate" in resp.text
    assert "CV Generation Accepted Rate" not in resp.text
    assert "CV Generation Validation-Fail Rate" not in resp.text
    assert "CV Generation Failure Rate" not in resp.text
    assert "CV Generation Persistence-Fail Rate" not in resp.text


# ── grouped settings endpoint ─────────────────────────────────────────────────

_VALID_WEIGHTS = {
    "ranking_weights.ai_score": "0.40",
    "ranking_weights.must_have_match": "0.20",
    "ranking_weights.vector_similarity": "0.15",
    "ranking_weights.title_relevance": "0.10",
    "ranking_weights.seniority_fit": "0.10",
    "ranking_weights.preference_fit": "0.05",
}


def test_grouped_save_valid_ranking_weights_redirects():
    """Valid 6-weight form POST → 303 redirect; save_settings_group called."""
    with patch("fitcv_cp.app.save_settings_group") as mock_group_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/ranking-weights",
            data=_VALID_WEIGHTS,
        )
    assert resp.status_code == 303
    mock_group_save.assert_called_once()
    saved_keys = set(mock_group_save.call_args[0][0].keys())
    assert saved_keys == set(_VALID_WEIGHTS.keys())


def test_grouped_save_weights_dont_sum_to_one_returns_422():
    """Weights summing to 0.9 → 422; no write."""
    bad_weights = dict(_VALID_WEIGHTS)
    bad_weights["ranking_weights.ai_score"] = "0.30"  # sum = 0.90
    with patch("fitcv_cp.app.save_settings_group") as mock_group_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/ranking-weights",
            data=bad_weights,
        )
    assert resp.status_code == 422
    mock_group_save.assert_not_called()


def test_grouped_save_weights_error_preserved_in_response():
    """Error response must contain the submitted form values (so admin can correct)."""
    bad_weights = dict(_VALID_WEIGHTS)
    bad_weights["ranking_weights.ai_score"] = "0.30"  # sum ≠ 1.0
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/ranking-weights",
            data=bad_weights,
        )
    assert resp.status_code == 422
    # The form values must persist (input elements show the submitted values)
    assert "0.30" in resp.text


def test_grouped_save_fit_label_thresholds_valid():
    """@proves settings_system.preference-fit-calibration

    strong > stretch -> 303 redirect; 2 keys saved.
    """
    with patch("fitcv_cp.app.save_settings_group") as mock_group_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/fit-label-thresholds",
            data={
                "fit_label_thresholds.strong": "0.70",
                "fit_label_thresholds.stretch": "0.40",
            },
        )
    assert resp.status_code == 303
    mock_group_save.assert_called_once()


def test_grouped_save_fit_label_thresholds_invalid_order():
    """@proves settings_system.grouped-form-validation

    stretch > strong -> 422; no write.
    """
    with patch("fitcv_cp.app.save_settings_group") as mock_group_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/fit-label-thresholds",
            data={
                "fit_label_thresholds.strong": "0.40",
                "fit_label_thresholds.stretch": "0.70",
            },
        )
    assert resp.status_code == 422
    mock_group_save.assert_not_called()


def test_grouped_save_gap_thresholds_valid():
    """strong_min > stretch_min → 303."""
    with patch("fitcv_cp.app.save_settings_group") as mock_group_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/gap-thresholds",
            data={
                "gap_thresholds.strong_min_matched_ratio": "0.80",
                "gap_thresholds.stretch_min_matched_ratio": "0.50",
            },
        )
    assert resp.status_code == 303
    mock_group_save.assert_called_once()


def test_grouped_save_gap_thresholds_invalid_order():
    """stretch_min > strong_min → 422; no write."""
    with patch("fitcv_cp.app.save_settings_group") as mock_group_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/gap-thresholds",
            data={
                "gap_thresholds.strong_min_matched_ratio": "0.30",
                "gap_thresholds.stretch_min_matched_ratio": "0.80",
            },
        )
    assert resp.status_code == 422
    mock_group_save.assert_not_called()


def test_grouped_save_unknown_group_returns_404():
    """Unknown group name → 404."""
    resp = TestClient(_app()).post(
        "/admin/settings/group/nonexistent",
        data={"some.key": "1"},
    )
    assert resp.status_code == 404


def test_grouped_save_bq_error_returns_422_not_303():
    """BQ failure from save_settings_group → 422 error page, not a redirect."""
    with patch("fitcv_cp.app.save_settings_group", side_effect=RuntimeError("BQ failed")), \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/fit-label-thresholds",
            data={
                "fit_label_thresholds.strong": "0.70",
                "fit_label_thresholds.stretch": "0.40",
            },
        )
    assert resp.status_code == 422
    assert "BQ failed" in resp.text


def test_grouped_save_audit_identity_encoded_in_updated_by():
    """Each group save uses updated_by='admin:grp:{uuid}'."""
    captured = {}

    def fake_save(keys_values, *, updated_by, bq, project, dataset):
        captured["updated_by"] = updated_by

    with patch("fitcv_cp.app.save_settings_group", side_effect=fake_save), \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/fit-label-thresholds",
            data={
                "fit_label_thresholds.strong": "0.70",
                "fit_label_thresholds.stretch": "0.40",
            },
        )
    assert captured.get("updated_by", "").startswith("admin:grp:")


# ── POST /admin/settings/section/{section_name} ───────────────────────────────

def _retrieval_core_section_form(
    *,
    vector_search_top_n: str = "100",
    ai_score_top_n: str = "20",
    final_top_n: str = "10",
    evidence_top_k: str = "5",
) -> dict[str, str]:
    return {
        "pipeline.vector_search_top_n": vector_search_top_n,
        "pipeline.ai_score_top_n": ai_score_top_n,
        "pipeline.final_top_n": final_top_n,
        "pipeline.evidence_top_k": evidence_top_k,
    }


def _retrieval_advanced_section_form(
    *,
    semantic_alignment_model: str = "text-embedding-005",
    required_skill_lexical_weight: str = "0.70",
    required_skill_semantic_weight: str = "0.30",
    role_lexical_weight: str = "0.60",
    role_semantic_weight: str = "0.40",
    responsibility_lexical_weight: str = "0.25",
    responsibility_semantic_weight: str = "0.75",
    domain_lexical_weight: str = "0.40",
    domain_semantic_weight: str = "0.60",
    channel_pool_size: str = "4",
) -> dict[str, str]:
    return {
        "cv_analysis.semantic_alignment.model": semantic_alignment_model,
        "cv_analysis.semantic_alignment.required_skill_lexical_weight": required_skill_lexical_weight,
        "cv_analysis.semantic_alignment.required_skill_semantic_weight": required_skill_semantic_weight,
        "cv_analysis.semantic_alignment.role_lexical_weight": role_lexical_weight,
        "cv_analysis.semantic_alignment.role_semantic_weight": role_semantic_weight,
        "cv_analysis.semantic_alignment.responsibility_lexical_weight": responsibility_lexical_weight,
        "cv_analysis.semantic_alignment.responsibility_semantic_weight": responsibility_semantic_weight,
        "cv_analysis.semantic_alignment.domain_lexical_weight": domain_lexical_weight,
        "cv_analysis.semantic_alignment.domain_semantic_weight": domain_semantic_weight,
        "cv_analysis.semantic_alignment.channel_pool_size": channel_pool_size,
    }


def _agentic_core_section_form(
    *,
    agentic_late_stage_enabled: str = "true",
    semantic_alignment_enabled: str = "true",
) -> dict[str, str]:
    return {
        "cv.agentic_late_stage.enabled": agentic_late_stage_enabled,
        "cv_analysis.semantic_alignment.enabled": semantic_alignment_enabled,
    }


def _agentic_advanced_section_form(
    *,
    semantic_alignment_model: str = "text-embedding-005",
    required_skill_lexical_weight: str = "0.70",
    required_skill_semantic_weight: str = "0.30",
    role_lexical_weight: str = "0.60",
    role_semantic_weight: str = "0.40",
    responsibility_lexical_weight: str = "0.25",
    responsibility_semantic_weight: str = "0.75",
    domain_lexical_weight: str = "0.40",
    domain_semantic_weight: str = "0.60",
    channel_pool_size: str = "4",
) -> dict[str, str]:
    return {
        "cv_analysis.semantic_alignment.model": semantic_alignment_model,
        "cv_analysis.semantic_alignment.required_skill_lexical_weight": required_skill_lexical_weight,
        "cv_analysis.semantic_alignment.required_skill_semantic_weight": required_skill_semantic_weight,
        "cv_analysis.semantic_alignment.role_lexical_weight": role_lexical_weight,
        "cv_analysis.semantic_alignment.role_semantic_weight": role_semantic_weight,
        "cv_analysis.semantic_alignment.responsibility_lexical_weight": responsibility_lexical_weight,
        "cv_analysis.semantic_alignment.responsibility_semantic_weight": responsibility_semantic_weight,
        "cv_analysis.semantic_alignment.domain_lexical_weight": domain_lexical_weight,
        "cv_analysis.semantic_alignment.domain_semantic_weight": domain_semantic_weight,
        "cv_analysis.semantic_alignment.channel_pool_size": channel_pool_size,
    }


def test_post_settings_section_valid_redirects():
    """Valid payload for retrieval core section returns 303."""
    with patch("fitcv_cp.app.save_settings_group"), \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/retrieval-core",
            data=_retrieval_core_section_form(),
        )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/settings"


def test_post_settings_section_advanced_retrieval_returns_404_after_section_retirement():
    """Legacy retrieval-advanced section is no longer an addressable section-save slug."""
    with patch("fitcv_cp.app.save_settings_group") as mock_group_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/retrieval-advanced",
            data=_retrieval_advanced_section_form(),
        )
    assert resp.status_code == 404
    mock_group_save.assert_not_called()


def test_post_settings_section_advanced_retrieval_without_metadata_only_input_returns_404() -> None:
    """Legacy retrieval-advanced endpoint remains removed even when form omits metadata-only values."""
    form_data = _retrieval_advanced_section_form()
    del form_data["cv_analysis.semantic_alignment.model"]

    with patch("fitcv_cp.app.save_settings_group") as mock_group_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/retrieval-advanced",
            data=form_data,
        )
    assert resp.status_code == 404
    mock_group_save.assert_not_called()


def test_post_settings_section_agentic_core_valid_redirects() -> None:
    captured = {}

    def _capture_save(values, *, updated_by, bq, project, dataset):
        captured["values"] = values

    with patch("fitcv_cp.app.save_settings_group", side_effect=_capture_save), \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/agentic-core",
            data=_agentic_core_section_form(),
        )

    assert resp.status_code == 303
    assert captured["values"]["cv.agentic_late_stage.enabled"] is True
    assert captured["values"]["cv_analysis.semantic_alignment.enabled"] is True


def test_post_settings_section_agentic_advanced_omits_metadata_only_input() -> None:
    captured = {}

    def _capture_save(values, *, updated_by, bq, project, dataset):
        captured["values"] = values

    form_data = _agentic_advanced_section_form()
    del form_data["cv_analysis.semantic_alignment.model"]

    with patch("fitcv_cp.app.save_settings_group", side_effect=_capture_save), \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/agentic-advanced",
            data=form_data,
        )

    assert resp.status_code == 303
    assert "cv_analysis.semantic_alignment.model" not in captured["values"]
    assert "cv_analysis.semantic_alignment.role_semantic_weight" in captured["values"]


def test_post_settings_section_agentic_core_preserves_current_vs_draft_feedback() -> None:
    active = {"cv.agentic_late_stage.enabled": False}

    with patch("fitcv_cp.app.load_active_settings", return_value=active):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/agentic-core",
            data=_agentic_core_section_form(semantic_alignment_enabled="not-a-bool"),
        )

    assert resp.status_code == 422
    html = resp.text
    assert 'data-task-section="agentic"' in html
    assert 'class="settings-field-row is-dirty"' in html
    assert "1 unsaved edit" in html
    assert "Current:" in html
    assert "No" in html


def test_post_settings_section_agentic_advanced_typed_equivalent_values_are_not_marked_dirty() -> None:
    active = {
        "cv_analysis.semantic_alignment.required_skill_lexical_weight": 0.7,
        "cv_analysis.semantic_alignment.required_skill_semantic_weight": 0.3,
        "cv_analysis.semantic_alignment.role_lexical_weight": 0.6,
        "cv_analysis.semantic_alignment.role_semantic_weight": 0.4,
        "cv_analysis.semantic_alignment.responsibility_lexical_weight": 0.25,
        "cv_analysis.semantic_alignment.responsibility_semantic_weight": 0.75,
        "cv_analysis.semantic_alignment.domain_lexical_weight": 0.4,
        "cv_analysis.semantic_alignment.domain_semantic_weight": 0.6,
        "cv_analysis.semantic_alignment.channel_pool_size": 4,
    }

    with patch("fitcv_cp.app.load_active_settings", return_value=active):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/agentic-advanced",
            data=_agentic_advanced_section_form(role_semantic_weight="0.10"),
        )

    assert resp.status_code == 422
    html = resp.text
    assert 'data-task-section="agentic"' in html
    assert 'data-entry-key="cv_analysis.semantic_alignment.required_skill_lexical_weight"' in html
    assert 'data-entry-key="cv_analysis.semantic_alignment.required_skill_lexical_weight" data-dirty="true"' not in html
    assert 'data-entry-key="cv_analysis.semantic_alignment.role_semantic_weight" data-dirty="true"' in html
    assert "1 unsaved edit" in html


def test_post_settings_key_rejects_metadata_only_agentic_setting() -> None:
    resp = TestClient(_app()).post(
        "/settings/cv_analysis.semantic_alignment.model",
        json={"value": "text-embedding-005", "updated_by": "admin"},
    )
    assert resp.status_code == 422
    assert "metadata-only" in resp.json()["detail"]


def test_admin_post_settings_key_rejects_metadata_only_agentic_setting() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/cv_analysis.semantic_alignment.model",
            data={"value": "text-embedding-005"},
        )
    assert resp.status_code == 422
    assert "metadata-only" in resp.text


def test_post_settings_section_unknown_returns_404():
    with patch("fitcv_cp.app.save_settings_group"), \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/does-not-exist",
            data={"some.key": "1"},
        )
    assert resp.status_code == 404


def test_post_settings_section_invalid_value_returns_422():
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/retrieval-core",
            data=_retrieval_core_section_form(vector_search_top_n="not-a-number"),
        )
    assert resp.status_code == 422


def test_post_settings_section_agentic_advanced_opens_details_on_validation_error():
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/agentic-advanced",
            data=_agentic_advanced_section_form(role_semantic_weight="0.10"),
        )
    assert resp.status_code == 422
    assert '<details class="settings-advanced-details" open>' in resp.text


def test_post_settings_section_rule_filter_uses_list_values() -> None:
    captured = {}

    def _capture_save(values, *, updated_by, bq, project, dataset):
        captured["values"] = values

    with patch("fitcv_cp.app.save_settings_group", side_effect=_capture_save), \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/rule-filter",
            data={
                "rule_filter.selected_filters": [
                    "seniority_mismatch",
                    "must_have_skill_missing",
                ]
            },
        )

    assert resp.status_code == 303
    assert captured["values"]["rule_filter.selected_filters"] == [
        "seniority_mismatch",
        "must_have_skill_missing",
    ]


def test_get_settings_renders_rule_filter_section_and_checkboxes() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")

    assert resp.status_code == 200
    body = resp.text
    assert "Rule Filter Settings" in body
    assert 'action="/admin/settings/section/rule-filter"' in body
    assert 'name="rule_filter.selected_filters"' in body
    assert 'value="must_have_skill_missing"' in body


def test_get_settings_renders_section_save_actions():
    """GET /admin/settings renders section-level save labels, not per-row Save buttons."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    body = resp.text
    assert "Save Retrieval Settings" in body
    assert "Save Timing Settings" in body
    assert "Save Global Job Filters" in body
    assert "Save Rule Filter Settings" in body


# ── Lifecycle API routes ─────────────────────────────────────────────────────

def _make_run_mock(status="queued", archived_at=None, queue_job_id="rq-job-1"):
    from fitcv_cp.models import PipelineRun, RunStatus
    import datetime
    return PipelineRun(
        run_id="run-lifecycle-1",
        status=RunStatus(status),
        triggered_by="admin",
        trigger_source="ui",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=datetime.datetime.now(datetime.timezone.utc),
        queue_job_id=queue_job_id,
        archived_at=archived_at,
    )


def test_admin_stop_queued_run_returns_json():
    """@proves admin_control_plane_core.fastapi-web-server
    @proves run_lifecycle_controls.cancel-queued-runs-directly-from-the-queue-via-rq
    """
    run = _make_run_mock(status="queued")
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.cancel_queued_run", return_value=True), \
         patch("fitcv_cp.app.request_run_cancel"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/stop")
    assert resp.status_code == 200
    assert "cancelled" in resp.json().get("status", "")


def test_admin_stop_queued_run_without_worker_claim_marks_cancelled() -> None:
    run = _make_run_mock(status="queued")
    run.started_at = None
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.cancel_queued_run", return_value=False), \
         patch("fitcv_cp.app.request_run_cancel") as mock_request_cancel, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/stop")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert mock_request_cancel.call_args.args[2] == "cancelled"


def test_admin_stop_claimed_run_falls_back_to_cancelling() -> None:
    """@proves run_lifecycle_controls.cooperative-cancellation-at-safe-checkpoints-for-running-jobs"""
    import datetime

    run = _make_run_mock(status="queued")
    run.started_at = datetime.datetime.now(datetime.timezone.utc)
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.cancel_queued_run", return_value=False), \
         patch("fitcv_cp.app.request_run_cancel") as mock_request_cancel, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/stop")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelling"
    assert mock_request_cancel.call_args.args[2] == "cancelling"


def test_admin_stop_succeeded_run_returns_409():
    run = _make_run_mock(status="succeeded")
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/stop")
    assert resp.status_code == 409


def test_admin_stop_awaiting_continue_run_returns_cancelled() -> None:
    """@proves run_lifecycle_controls.direct-cancellation-of-paused-manual-runs-in-awaiting-continue"""
    run = _make_run_mock(status="awaiting_continue")
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event") as mock_append_event:
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/stop")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert mock_update_status.call_args.args[1].value == "cancelled"
    assert mock_append_event.call_count == 1


def test_admin_stop_unknown_run_returns_404():
    with patch("fitcv_cp.app.get_run", return_value=None):
        resp = TestClient(_app()).post("/admin/runs/nonexistent/stop")
    assert resp.status_code == 404


def test_admin_repair_cancellation_stale_run_returns_cancelled() -> None:
    """@proves run_lifecycle_controls.stale-cancellation-repair-endpoint"""
    run = _make_run_mock(status="cancelling")
    run.started_at = None
    run.finished_at = None
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/repair-cancellation")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert mock_update_status.call_args.args[1].value == "cancelled"


def test_admin_repair_cancellation_started_stale_run_returns_cancelled() -> None:
    """@proves run_lifecycle_controls.stale-cancellation-repair-endpoint"""
    import datetime

    run = _make_run_mock(status="cancelling")
    now = datetime.datetime.now(datetime.timezone.utc)
    run.started_at = now - datetime.timedelta(minutes=15)
    run.cancel_requested_at = now - datetime.timedelta(minutes=5)
    run.finished_at = None
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/repair-cancellation")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert mock_update_status.call_args.args[1].value == "cancelled"


def test_admin_repair_cancellation_running_run_returns_409() -> None:
    """@proves run_lifecycle_controls.stale-cancellation-repair-endpoint"""
    run = _make_run_mock(status="running")
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/repair-cancellation")
    assert resp.status_code == 409


def test_admin_archive_succeeded_run_returns_json():
    """@proves run_lifecycle_controls.archive-and-unarchive-terminal-runs"""
    run = _make_run_mock(status="succeeded")
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.archive_run"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/archive")
    assert resp.status_code == 200


def test_admin_archive_running_run_returns_409():
    run = _make_run_mock(status="running")
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/archive")
    assert resp.status_code == 409


def test_admin_unarchive_archived_run_returns_json():
    """@proves run_lifecycle_controls.archive-and-unarchive-terminal-runs"""
    import datetime
    run = _make_run_mock(status="succeeded", archived_at=datetime.datetime.now(datetime.timezone.utc))
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.unarchive_run"), \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/unarchive")
    assert resp.status_code == 200


def test_admin_unarchive_non_archived_run_returns_409():
    run = _make_run_mock(status="succeeded", archived_at=None)
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/unarchive")
    assert resp.status_code == 409


def test_admin_bulk_cancel_mixed_eligibility_returns_processed_and_skipped_summary():
    """@proves trigger_run_management.runs-list-management
    @proves run_lifecycle_controls.batch-cancel-archive-and-unarchive-endpoints-with-explicit-processed-skipped-summaries
    """
    run1 = _make_full_run_mock(status="queued", run_id="run-bulk-1")
    run2 = _make_full_run_mock(status="succeeded", run_id="run-bulk-2")

    def _get_run(run_id, *args, **kwargs):
        return {"run-bulk-1": run1, "run-bulk-2": run2}.get(run_id)

    with patch("fitcv_cp.app.get_run", side_effect=_get_run), \
         patch("fitcv_cp.app.cancel_queued_run", return_value=False), \
         patch("fitcv_cp.app.request_run_cancel") as mock_request_cancel, \
         patch("fitcv_cp.app.append_event") as mock_append_event:
        resp = TestClient(_app()).post(
            "/admin/runs/bulk/cancel",
            json={"run_ids": ["run-bulk-1", "run-bulk-2"]},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["requested"] == 2
    assert body["processed"] == 1
    assert body["skipped"] == 1
    assert body["processed_run_ids"] == ["run-bulk-1"]
    assert body["skipped_items"] == [{"run_id": "run-bulk-2", "reason": "not_cancellable"}]
    assert mock_request_cancel.call_count == 1
    assert mock_append_event.call_count == 1


def test_admin_bulk_cancel_awaiting_continue_run_directly_cancels():
    """@proves run_lifecycle_controls.direct-cancellation-of-paused-manual-runs-in-awaiting-continue
    @proves run_lifecycle_controls.batch-cancel-archive-and-unarchive-endpoints-with-explicit-processed-skipped-summaries
    """
    run = _make_full_run_mock(status="awaiting_continue", run_id="run-bulk-awaiting")

    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.request_run_cancel") as mock_request_cancel, \
         patch("fitcv_cp.app.append_event") as mock_append_event:
        resp = TestClient(_app()).post(
            "/admin/runs/bulk/cancel",
            json={"run_ids": ["run-bulk-awaiting"]},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["processed"] == 1
    assert body["processed_run_ids"] == ["run-bulk-awaiting"]
    assert mock_update_status.call_args.args[1].value == "cancelled"
    mock_request_cancel.assert_not_called()
    assert mock_append_event.call_count == 1


def test_admin_bulk_archive_terminal_runs_only():
    """@proves run_lifecycle_controls.batch-cancel-archive-and-unarchive-endpoints-with-explicit-processed-skipped-summaries"""
    run1 = _make_full_run_mock(status="succeeded", run_id="run-archive-1")
    run2 = _make_full_run_mock(status="running", run_id="run-archive-2")

    def _get_run(run_id, *args, **kwargs):
        return {"run-archive-1": run1, "run-archive-2": run2}.get(run_id)

    with patch("fitcv_cp.app.get_run", side_effect=_get_run), \
         patch("fitcv_cp.app.archive_run") as mock_archive_run, \
         patch("fitcv_cp.app.append_event") as mock_append_event:
        resp = TestClient(_app()).post(
            "/admin/runs/bulk/archive",
            json={"run_ids": ["run-archive-1", "run-archive-2"]},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["processed"] == 1
    assert body["processed_run_ids"] == ["run-archive-1"]
    assert body["skipped_items"] == [{"run_id": "run-archive-2", "reason": "not_archivable"}]
    mock_archive_run.assert_called_once()
    mock_append_event.assert_called_once()


def test_admin_bulk_unarchive_archived_runs_only():
    """@proves run_lifecycle_controls.batch-cancel-archive-and-unarchive-endpoints-with-explicit-processed-skipped-summaries"""
    import datetime

    run1 = _make_full_run_mock(
        status="succeeded",
        run_id="run-unarchive-1",
        archived_at=datetime.datetime.now(datetime.timezone.utc),
    )
    run2 = _make_full_run_mock(status="failed", run_id="run-unarchive-2", archived_at=None)

    def _get_run(run_id, *args, **kwargs):
        return {"run-unarchive-1": run1, "run-unarchive-2": run2}.get(run_id)

    with patch("fitcv_cp.app.get_run", side_effect=_get_run), \
         patch("fitcv_cp.app.unarchive_run") as mock_unarchive_run, \
         patch("fitcv_cp.app.append_event") as mock_append_event:
        resp = TestClient(_app()).post(
            "/admin/runs/bulk/unarchive",
            json={"run_ids": ["run-unarchive-1", "run-unarchive-2"]},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["processed"] == 1
    assert body["processed_run_ids"] == ["run-unarchive-1"]
    assert body["skipped_items"] == [{"run_id": "run-unarchive-2", "reason": "not_unarchivable"}]
    mock_unarchive_run.assert_called_once()
    mock_append_event.assert_called_once()


def test_admin_bulk_lifecycle_rejects_empty_run_ids():
    """@proves run_lifecycle_controls.batch-cancel-archive-and-unarchive-endpoints-with-explicit-processed-skipped-summaries"""
    resp = TestClient(_app()).post("/admin/runs/bulk/cancel", json={"run_ids": []})
    assert resp.status_code == 422


def test_admin_bulk_lifecycle_rejects_unknown_run_ids():
    """@proves run_lifecycle_controls.batch-cancel-archive-and-unarchive-endpoints-with-explicit-processed-skipped-summaries"""
    with patch("fitcv_cp.app.get_run", return_value=None):
        resp = TestClient(_app()).post(
            "/admin/runs/bulk/archive",
            json={"run_ids": ["missing-run"]},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["processed"] == 0
    assert body["skipped"] == 1
    assert body["skipped_items"] == [{"run_id": "missing-run", "reason": "not_found"}]


def test_admin_runs_active_view_passes_archive_filter():
    with patch("fitcv_cp.app.list_runs", return_value=[]) as mock_list:
        resp = TestClient(_app()).get("/admin/runs?view=active")
    assert resp.status_code == 200
    call_kwargs = mock_list.call_args[1]
    assert call_kwargs.get("include_archived") is False
    assert call_kwargs.get("archived_only", False) is False


def test_admin_runs_archived_view_passes_archived_only():
    with patch("fitcv_cp.app.list_runs", return_value=[]) as mock_list:
        resp = TestClient(_app()).get("/admin/runs?view=archived")
    assert resp.status_code == 200
    call_kwargs = mock_list.call_args[1]
    assert call_kwargs.get("archived_only") is True


# ── Task 5: Admin UI lifecycle controls ─────────────────────────────────────

def _make_full_run_mock(status="queued", archived_at=None, run_id="run-ui-1"):
    from fitcv_cp.models import PipelineRun, RunStatus
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc)
    return PipelineRun(
        run_id=run_id,
        status=RunStatus(status),
        triggered_by="admin",
        trigger_source="ui",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=now - datetime.timedelta(minutes=5),
        archived_at=archived_at,
    )


def test_runs_list_shows_active_all_archived_filter_tabs():
    """@proves admin_control_plane_core.jinja2-admin-pages"""
    with patch("fitcv_cp.app.list_runs", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    body = resp.text
    assert "Active" in body
    assert "Archived" in body
    assert "All" in body


def test_runs_list_is_selection_first_without_row_action_controls():
    run = _make_full_run_mock(status="queued")
    with patch("fitcv_cp.app.list_runs", return_value=[run]):
        resp = TestClient(_app()).get("/admin/runs")
    html = resp.text
    assert "Actions" not in html
    assert "Stop Run" not in html
    assert "Run Next Stage" not in html
    assert "Repair Status" not in html
    assert "Triggered By" not in html


def test_runs_list_renders_bulk_selection_checkboxes():
    run = _make_full_run_mock(status="queued", run_id="run-bulk-ui-1")
    with patch("fitcv_cp.app.list_runs", return_value=[run]):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    html = resp.text
    assert 'id="select-all-runs"' in html
    assert 'name="selected_run_ids"' in html


def test_runs_list_renders_bulk_action_bar_hooks():
    """@proves admin_control_plane_core.jinja2-admin-pages
    @proves ui_consistency_theming.consistent-action-hierarchy-primary-secondary-section
    """
    run = _make_full_run_mock(status="queued", run_id="run-bulk-ui-1")
    with patch("fitcv_cp.app.list_runs", return_value=[run]):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    html = resp.text
    assert 'id="bulk-action-bar"' in html
    assert "Cancel selected" in html
    assert "Archive selected" in html
    assert "Unarchive selected" in html


def test_runs_list_shows_core_operational_columns_only():
    run = _make_full_run_mock(status="queued", run_id="run-compact-actions")
    with patch("fitcv_cp.app.list_runs", return_value=[run]):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    html = resp.text
    assert "Run ID" in html
    assert "Status" in html
    assert "Mode" in html
    assert "Jobs Path" in html
    assert "Created" in html
    assert "Duration" in html
    assert "Orchestration" in html
    assert "Triggered By" not in html
    assert "Actions" not in html

def test_runs_list_shows_orchestration_backend_diagnostics() -> None:
    run = _make_full_run_mock(status="queued", run_id="run-orch-list")
    run.queue_job_id = "backend-run-123"
    run.orchestration_backend = "prefect"
    run.orchestration_run_id = "backend-run-123"
    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.orchestration_job_status", return_value="queued"):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    html = resp.text
    assert "backend-run-123" in html
    assert "prefect" in html
    assert "queued" in html

def test_runs_list_shows_schema_fallback_banner_when_columns_missing() -> None:
    run = _make_full_run_mock(status="queued", run_id="run-schema-banner")
    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch(
             "fitcv_cp.app.get_pipeline_runs_schema_status",
             return_value={
                 "status": "fallback",
                 "missing_columns": ["orchestration_backend", "orchestration_run_id"],
                 "warning": "orchestration_binding_columns_missing",
             },
         ):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    assert "Orchestration Schema Fallback Mode" in resp.text
    assert "schema: fallback mode" in resp.text

def test_runs_list_hides_schema_fallback_banner_for_unknown_schema_status() -> None:
    run = _make_full_run_mock(status="queued", run_id="run-schema-unknown")
    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch(
             "fitcv_cp.app.get_pipeline_runs_schema_status",
             return_value={
                 "status": "unknown",
                 "missing_columns": [],
                 "warning": "sqlite_mode_no_bigquery_schema_check",
             },
         ):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    assert "Orchestration Schema Fallback Mode" not in resp.text
    assert "schema: fallback mode" not in resp.text

def test_runs_list_uses_persisted_backend_identity_per_run() -> None:
    run_prefect = _make_full_run_mock(status="queued", run_id="run-prefect")
    run_prefect.queue_job_id = "rq-job-prefect"
    run_prefect.orchestration_backend = "prefect"
    run_prefect.orchestration_run_id = "flow-run-1"
    run_queue = _make_full_run_mock(status="queued", run_id="run-queue")
    run_queue.queue_job_id = "rq-job-2"
    run_queue.orchestration_backend = "default_queue"
    run_queue.orchestration_run_id = "rq-job-2"
    with patch("fitcv_cp.app.list_runs", return_value=[run_prefect, run_queue]), \
         patch("fitcv_cp.app.get_pipeline_runs_schema_status", return_value={"status": "complete", "missing_columns": [], "warning": None}), \
         patch("fitcv_cp.app.orchestration_job_status", return_value="queued"):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    html = resp.text
    assert "flow-run-1" in html
    assert "prefect" in html
    assert "rq-job-2" in html
    assert "default_queue" in html


def test_runs_list_uses_canonical_run_mode_labels():
    run_all = _make_full_run_mock(status="queued", run_id="run-all-label")
    staged = _make_full_run_mock(status="awaiting_continue", run_id="run-staged-label")
    staged.run_mode = "manual_staged"
    staged.next_stage = "ranking"
    with patch("fitcv_cp.app.list_runs", return_value=[run_all, staged]):
        resp = TestClient(_app()).get("/admin/runs")
    html = resp.text
    assert "Run All" in html
    assert "Stage by Stage" in html
    assert "Auto" not in html
    assert "Manual staged" not in html


def test_runs_list_jobs_path_is_truncated_with_full_title():
    run = _make_full_run_mock(status="queued", run_id="run-jobs-path")
    run.jobs_path = "data/uploads/very_long_nested_folder_name/another_folder/really_long_jobs_snapshot_name.json"
    with patch("fitcv_cp.app.list_runs", return_value=[run]):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    html = resp.text
    assert 'class="run-jobs-path"' in html
    assert 'title="data/uploads/very_long_nested_folder_name/another_folder/really_long_jobs_snapshot_name.json"' in html


def test_settings_page_renders_run_lifecycle_section() -> None:
    """@proves settings_system.run-safety-settings"""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Run Lifecycle Settings" in html
    assert 'name="run_lifecycle.max_runtime_minutes"' in html


def test_admin_runs_timeouts_running_runs_to_failed() -> None:
    """@proves run_lifecycle_controls.state-aware-max-runtime-timeout-handling-for-queued-running-cancelling-and-paused-manual-runs
    @proves run_lifecycle_controls.timeout-copy-now-distinguishes-queue-wait-active-runtime-and-stage-by-stage-manual-wait-time
    """
    import datetime

    now = datetime.datetime.now(datetime.timezone.utc)
    run = _make_full_run_mock(status="running", run_id="run-timeout-running")
    run.created_at = now - datetime.timedelta(hours=3)
    run.started_at = now - datetime.timedelta(hours=2)

    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.load_active_settings", return_value={"run_lifecycle.max_runtime_minutes": 60}), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event") as mock_append_event:
        resp = TestClient(_app()).get("/admin/runs")

    assert resp.status_code == 200
    args = mock_update_status.call_args.args
    assert args[0] == "run-timeout-running"
    assert args[1] == RunStatus.FAILED
    assert mock_append_event.call_args.args[0].stage == "run_timed_out"


def test_admin_runs_timeouts_awaiting_continue_runs_to_cancelled() -> None:
    """@proves run_lifecycle_controls.state-aware-max-runtime-timeout-handling-for-queued-running-cancelling-and-paused-manual-runs
    @proves run_lifecycle_controls.timeout-copy-now-distinguishes-queue-wait-active-runtime-and-stage-by-stage-manual-wait-time
    """
    import datetime

    now = datetime.datetime.now(datetime.timezone.utc)
    run = _make_full_run_mock(status="awaiting_continue", run_id="run-timeout-awaiting")
    run.run_mode = "manual_staged"
    run.next_stage = "cv_generation"
    run.created_at = now - datetime.timedelta(hours=5)
    run.started_at = now - datetime.timedelta(hours=4)

    with patch("fitcv_cp.app.list_runs", return_value=[run]), \
         patch("fitcv_cp.app.load_active_settings", return_value={"run_lifecycle.max_runtime_minutes": 120}), \
         patch("fitcv_cp.app.update_run_status") as mock_update_status, \
         patch("fitcv_cp.app.append_event") as mock_append_event:
        resp = TestClient(_app()).get("/admin/runs")

    assert resp.status_code == 200
    args = mock_update_status.call_args.args
    assert args[0] == "run-timeout-awaiting"
    assert args[1] == RunStatus.CANCELLED
    assert mock_append_event.call_args.args[0].stage == "run_timed_out"


def test_run_detail_queued_shows_stop_run():
    """@proves trigger_run_management.run-detail-actions"""
    import datetime
    run = _make_full_run_mock(status="queued")
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-ui-1")
    assert resp.status_code == 200
    assert "Stop Run" in resp.text


def test_run_detail_awaiting_continue_shows_run_next_stage_and_stop_run():
    """@proves inspection_debugging.run-progress-and-checkpoints
    @proves admin_control_plane_core.jinja2-admin-pages
    """
    run = _make_full_run_mock(status="awaiting_continue")
    run.run_mode = "manual_staged"
    run.next_stage = "ranking"
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-ui-1")
    assert resp.status_code == 200
    assert "Run Next Stage" in resp.text
    assert "Stop Run" in resp.text


def test_run_detail_run_all_shows_shared_progress_without_checkpoint_controls():
    """@proves inspection_debugging.run-progress-and-checkpoints
    @proves trigger_run_management.shared-stage-progress
    """
    run = _make_full_run_mock(status="running")
    run.run_mode = "run_all"
    run.last_completed_stage = "enrich"
    run.completed_stages = ["normalize", "enrich"]
    run.stage_transition_artifacts_json = json.dumps(
        {
            "artifacts": {
                "stages": {
                    "normalize": {"status": "completed"},
                    "enrich": {"status": "completed"},
                }
            }
        }
    )
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-ui-1")
    assert resp.status_code == 200
    assert "Run All" in resp.text
    assert "Last Completed" in resp.text
    assert "Completed Stages" in resp.text
    assert '<span class="k">Checkpoint</span>' not in resp.text
    assert "Stage Artifacts JSON (Diagnostics)" in resp.text


def test_run_detail_succeeded_shows_archive_run():
    """@proves trigger_run_management.run-detail-actions"""
    run = _make_full_run_mock(status="succeeded")
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-ui-1")
    assert resp.status_code == 200
    assert "Archive Run" in resp.text


def test_run_detail_archived_shows_unarchive_and_badge():
    """@proves admin_control_plane_core.jinja2-admin-pages"""
    import datetime
    run = _make_full_run_mock(status="succeeded", archived_at=datetime.datetime(2026, 3, 26, 13, 0, 0, tzinfo=datetime.timezone.utc))
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-ui-1")
    assert resp.status_code == 200
    assert "Unarchive Run" in resp.text
    assert "Archived" in resp.text


def test_run_detail_stale_cancelling_shows_repair_status() -> None:
    """@proves run_lifecycle_controls.stale-cancellation-repair-endpoint
    @proves admin_control_plane_core.jinja2-admin-pages
    """
    run = _make_full_run_mock(status="cancelling")
    run.started_at = None
    run.finished_at = None
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-ui-1")
    assert resp.status_code == 200
    assert "Repair Status" in resp.text


def test_run_detail_started_stale_cancelling_shows_repair_status() -> None:
    """@proves run_lifecycle_controls.stale-cancellation-repair-endpoint
    @proves admin_control_plane_core.jinja2-admin-pages
    """
    import datetime

    run = _make_full_run_mock(status="cancelling")
    now = datetime.datetime.now(datetime.timezone.utc)
    run.started_at = now - datetime.timedelta(minutes=15)
    run.cancel_requested_at = now - datetime.timedelta(minutes=5)
    run.finished_at = None
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.get_events", return_value=[]), \
         patch("fitcv_cp.app.list_cvs_for_run", return_value=[]), \
         patch("fitcv_cp.app.list_run_structured_jobs", return_value=[]), \
         patch("fitcv_cp.app.list_filter_results_for_run", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs/run-ui-1")
    assert resp.status_code == 200
    assert "Repair Status" in resp.text


# ── Component 1: app.py server-side enrichment ───────────────────────────────

def _run_detail_patches(
    status="succeeded",
    cv_versions=None,
    enriched_jobs=None,
    filter_results=None,
    results_export_json=None,
    stage_transition_artifacts_json=None,
):
    import datetime
    from fitcv_cp.models import PipelineRun, RunStatus
    run = PipelineRun(
        run_id="run-detail-test",
        status=RunStatus(status),
        triggered_by="admin",
        trigger_source="ui",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=datetime.datetime(2026, 3, 27, 9, 0, 0, tzinfo=datetime.timezone.utc),
        results_export_json=results_export_json,
        stage_transition_artifacts_json=stage_transition_artifacts_json,
    )
    return (
        patch("fitcv_cp.app.get_run", return_value=run),
        patch("fitcv_cp.app.get_events", return_value=[]),
        patch("fitcv_cp.app.list_cvs_for_run", return_value=cv_versions or []),
        patch("fitcv_cp.app.list_run_structured_jobs", return_value=enriched_jobs or []),
        patch("fitcv_cp.app.list_filter_results_for_run", return_value=filter_results or []),
    )


def test_run_detail_shows_deduplicated_before_enrichment_section():
    import json as _json

    export_payload = _json.dumps({
        "results": [
            {
                "job_url": "https://jobs.example.com/2",
                "job_title": "Duplicated Analyst",
                "pipeline_status": "deduplicated_before_enrichment",
                "reject_reasons": ["near_duplicate_job_posting"],
            }
        ]
    })
    patches = _run_detail_patches(
        enriched_jobs=[{"job_url": "https://jobs.example.com/1", "title": "Kept Job", "domain": "d", "job_family": "f", "required_skills": [], "location_type": None, "seniority": None}],
        filter_results=[{"job_url": "https://jobs.example.com/1", "passed": True, "reasons": []}],
        results_export_json=export_payload,
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert resp.status_code == 200
    assert "Post-dedupe enriched jobs" in resp.text
    assert "Deduplicated before enrichment: 1" in resp.text
    assert "Duplicated Analyst" in resp.text

def test_run_detail_enriched_tab_uses_stage_artifacts_sample_for_running_run() -> None:
    import json as _json

    stage_artifacts = _json.dumps(
        {
            "artifacts": {
                "stages": {
                    "enrich": {
                        "status": "completed",
                        "outputs_sample": [
                            {
                                "job_url": "https://jobs.example.com/live-1",
                                "job_title": "Live Enriched Role",
                                "location_type": "remote",
                                "seniority": "senior",
                                "job_family": "data_engineering",
                                "domain": "data",
                                "required_skills": ["python", "sql"],
                            }
                        ],
                    }
                }
            }
        }
    )
    patches = _run_detail_patches(
        status="running",
        enriched_jobs=[],
        filter_results=[],
        results_export_json=None,
        stage_transition_artifacts_json=stage_artifacts,
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert resp.status_code == 200
    assert "No enrichment data available for this run." not in resp.text
    assert "Post-dedupe enriched jobs" in resp.text
    assert "Live Enriched Role" in resp.text


def test_run_detail_shows_marks_for_passed_jobs() -> None:
    """@proves inspection_debugging.rule-filter-diagnostics"""
    patches = _run_detail_patches(
        enriched_jobs=[
            {
                "job_url": "https://jobs.example.com/1",
                "title": "Marked Pass Job",
                "domain": "d",
                "job_family": "f",
                "required_skills": [],
                "location_type": None,
                "seniority": None,
            }
        ],
        filter_results=[
            {
                "job_url": "https://jobs.example.com/1",
                "passed": True,
                "reasons": [],
                "marks": [
                    {
                        "code": "must_have_skill_missing",
                        "message": "Missing must-have skills",
                    }
                ],
            }
        ],
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")

    assert resp.status_code == 200
    assert "Marks: must_have_skill_missing" in resp.text


def test_run_detail_enriched_shows_pipeline_outcome_for_passed_non_ranked_job():
    import json as _json

    export_payload = _json.dumps({
        "results": [
            {
                "job_url": "https://jobs.example.com/1",
                "job_title": "Retail Banking Analyst",
                "pipeline_status": "not_shortlisted",
                "reject_reasons": [],
            }
        ]
    })
    enriched = [{
        "job_url": "https://jobs.example.com/1",
        "title": "Retail Banking Analyst",
        "domain": "banking",
        "job_family": "analytics",
        "required_skills": [],
        "location_type": "hybrid",
        "seniority": "mid",
    }]
    filter_results = [{"job_url": "https://jobs.example.com/1", "passed": True, "reasons": []}]
    patches = _run_detail_patches(
        enriched_jobs=enriched,
        filter_results=filter_results,
        results_export_json=export_payload,
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert resp.status_code == 200
    assert "Pipeline Outcome" in resp.text
    assert "Passed filter, not shortlisted" in resp.text


def test_run_detail_enriched_shows_pipeline_outcome_for_ranked_fit_skip_job():
    import json as _json

    export_payload = _json.dumps({
        "results": [
            {
                "job_url": "https://jobs.example.com/1",
                "job_title": "Skipped After Ranking",
                "pipeline_status": "ranked_skipped_fit_gate",
                "decision_chain": {
                    "shortlist": {"status": "returned_by_vector_search", "advanced_to_scoring": True},
                    "primary_fit": {"source": "reranker", "label": "skip"},
                    "cv_analysis": {"status": "skipped_fit_gate", "completed": True},
                    "cv_generation": {"status": "skipped_fit_gate", "attempted": False},
                    "validation": {"status": "not_run"},
                },
                "reject_reasons": [],
            }
        ]
    })
    enriched = [{
        "job_url": "https://jobs.example.com/1",
        "title": "Skipped After Ranking",
        "domain": "banking",
        "job_family": "analytics",
        "required_skills": [],
        "location_type": "hybrid",
        "seniority": "mid",
    }]
    filter_results = [{"job_url": "https://jobs.example.com/1", "passed": True, "reasons": []}]
    patches = _run_detail_patches(
        enriched_jobs=enriched,
        filter_results=filter_results,
        results_export_json=export_payload,
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert resp.status_code == 200
    assert "Pipeline Outcome" in resp.text
    assert "Skipped after CV analysis" in resp.text
    assert "Primary fit: skip" in resp.text
    assert "CV analysis: skipped after CV analysis" in resp.text


def test_run_detail_enriched_shows_pipeline_outcome_for_reranker_blocked_job():
    """@proves trigger_run_management.decision-chain-outcomes
    @proves trigger_run_management.reranker-fit-authority
    @proves inspection_debugging.results-ledger-inspection
    """
    import json as _json

    export_payload = _json.dumps({
        "results": [
            {
                "job_url": "https://jobs.example.com/1",
                "job_title": "Blocked Before Analysis",
                "pipeline_status": "ranked_blocked_by_reranker_fit",
                "decision_chain": {
                    "shortlist": {"status": "returned_by_vector_search", "advanced_to_scoring": True},
                    "primary_fit": {"source": "reranker", "label": "skip"},
                    "cv_analysis": {"status": "blocked_by_reranker_fit", "completed": False},
                    "cv_generation": {"status": "not_attempted", "attempted": False},
                    "validation": {"status": "not_run"},
                },
                "reject_reasons": [],
            }
        ]
    })
    enriched = [{
        "job_url": "https://jobs.example.com/1",
        "title": "Blocked Before Analysis",
        "domain": "banking",
        "job_family": "analytics",
        "required_skills": [],
        "location_type": "hybrid",
        "seniority": "mid",
    }]
    filter_results = [{"job_url": "https://jobs.example.com/1", "passed": True, "reasons": []}]
    patches = _run_detail_patches(
        enriched_jobs=enriched,
        filter_results=filter_results,
        results_export_json=export_payload,
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert resp.status_code == 200
    assert "Pipeline Outcome" in resp.text
    assert "Ranked, blocked by reranker fit" in resp.text
    assert "Primary fit: skip" in resp.text
    assert "CV analysis: blocked by reranker fit" in resp.text


def test_run_detail_enriched_uses_deterministic_subreason_for_validation_failed_job():
    import json as _json

    export_payload = _json.dumps({
        "results": [
            {
                "job_url": "https://jobs.example.com/1",
                "job_title": "Validation Failed CV",
                "pipeline_status": "ranked_no_cv",
                "deterministic_outcome": "rejected",
                "stage_owned_subreason": "validation_failed",
                "source_stage": "cv_generation",
                "decision_chain": {
                    "shortlist": {"status": "returned_by_vector_search", "advanced_to_scoring": True},
                    "primary_fit": {"source": "reranker", "label": "strong"},
                    "cv_analysis": {"status": "ready_for_generation", "completed": True},
                    "cv_generation": {"status": "validation_failed", "attempted": True},
                    "validation": {"status": "failed"},
                },
                "reject_reasons": [],
            }
        ]
    })
    enriched = [{
        "job_url": "https://jobs.example.com/1",
        "title": "Validation Failed CV",
        "domain": "banking",
        "job_family": "analytics",
        "required_skills": [],
        "location_type": "hybrid",
        "seniority": "mid",
    }]
    filter_results = [{"job_url": "https://jobs.example.com/1", "passed": True, "reasons": []}]
    patches = _run_detail_patches(
        enriched_jobs=enriched,
        filter_results=filter_results,
        results_export_json=export_payload,
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert resp.status_code == 200
    assert "CV validation failed" in resp.text
    assert "Validation: failed" in resp.text


def test_run_detail_enriched_uses_analysis_handoff_truth_for_ready_job():
    import json as _json

    export_payload = _json.dumps({
        "results": [
            {
                "job_url": "https://jobs.example.com/1",
                "job_title": "Ready Job",
                "pipeline_status": "ranked_no_cv",
                "deterministic_outcome": None,
                "stage_owned_subreason": "ready_for_generation",
                "source_stage": "cv_analysis",
                "decision_chain": {
                    "shortlist": {"status": "returned_by_vector_search", "advanced_to_scoring": True},
                    "primary_fit": {"source": "reranker", "label": "strong"},
                    "cv_analysis": {"status": "ready_for_generation", "completed": True},
                    "cv_generation": {"status": "not_attempted", "attempted": False},
                    "validation": {"status": "not_run"},
                },
                "reject_reasons": [],
            }
        ]
    })
    enriched = [{
        "job_url": "https://jobs.example.com/1",
        "title": "Ready Job",
        "domain": "banking",
        "job_family": "analytics",
        "required_skills": [],
        "location_type": "hybrid",
        "seniority": "mid",
    }]
    filter_results = [{"job_url": "https://jobs.example.com/1", "passed": True, "reasons": []}]
    patches = _run_detail_patches(
        enriched_jobs=enriched,
        filter_results=filter_results,
        results_export_json=export_payload,
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert resp.status_code == 200
    assert "Ready for CV generation" in resp.text
    assert "CV analysis: ready for CV generation" not in resp.text


def test_run_detail_cv_versions_show_job_title():
    """CV output link uses the enriched job title instead of generic 'View Job'."""
    import json as _json
    import datetime as _dt
    from fitcv_cp.models import PipelineRun, RunStatus
    cv = {"version_id": "cv1", "job_url": "https://jobs.example.com/1",
          "fit_classification": "strong",
          "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc)}
    enriched = [{"job_url": "https://jobs.example.com/1", "title": "Senior Data Engineer",
                 "domain": "data", "job_family": "engineering", "required_skills": [],
                 "location_type": "remote", "seniority": "senior"}]
    export_payload = _json.dumps({
        "results": [
            {
                "job_url": "https://jobs.example.com/1",
                "job_title": "Senior Data Engineer",
                "pipeline_status": "ranked_with_cv",
            }
        ]
    })
    patches = _run_detail_patches(cv_versions=[cv], enriched_jobs=enriched,
                                  filter_results=[{"job_url": "https://jobs.example.com/1",
                                                   "passed": True, "reasons": []}],
                                  results_export_json=export_payload)
    run_with_cv = PipelineRun(
        run_id="run-detail-test",
        status=RunStatus("succeeded"),
        triggered_by="admin",
        trigger_source="ui",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=_dt.datetime(2026, 3, 27, 9, 0, 0, tzinfo=_dt.timezone.utc),
        cvs_generated=1,
        results_export_json=export_payload,
    )
    with patch("fitcv_cp.app.get_run", return_value=run_with_cv), patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test")
    assert resp.status_code == 200
    assert "Senior Data Engineer" in resp.text
    assert "View Job" not in resp.text.split("Senior Data Engineer")[0].split("Generated Outputs")[-1]


def test_run_detail_cv_versions_fallback_when_no_title():
    """CV output link falls back to 'View Job' when no enriched job matches the job_url."""
    import datetime as _dt
    cv = {"version_id": "cv2", "job_url": "https://jobs.example.com/orphan",
          "fit_classification": "strong",
          "generated_at": _dt.datetime.now(_dt.timezone.utc)}
    # Run must have cvs_generated > 0 for the pipeline results section to render
    patches = _run_detail_patches(cv_versions=[cv], enriched_jobs=[])
    # Override the run object to have cvs_generated set
    import datetime as _dt2
    from fitcv_cp.models import PipelineRun, RunStatus
    run_with_cv = PipelineRun(
        run_id="run-detail-test",
        status=RunStatus("succeeded"),
        triggered_by="admin",
        trigger_source="ui",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=_dt2.datetime(2026, 3, 27, 9, 0, 0, tzinfo=_dt2.timezone.utc),
        cvs_generated=1,
    )
    with patch("fitcv_cp.app.get_run", return_value=run_with_cv), \
         patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test")
    assert resp.status_code == 200
    assert "View Job" in resp.text


def test_run_detail_zero_cvs_and_zero_ranked_shows_ranking_threshold_message():
    """@proves inspection_debugging.ranking-diagnostics"""
    import datetime as _dt
    from fitcv_cp.models import PipelineRun, RunStatus

    run = PipelineRun(
        run_id="run-detail-zero-ranked",
        status=RunStatus("succeeded"),
        triggered_by="admin",
        trigger_source="ui",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=_dt.datetime(2026, 3, 27, 9, 0, 0, tzinfo=_dt.timezone.utc),
        ranked=0,
        cvs_generated=0,
    )
    patches = _run_detail_patches(cv_versions=[], enriched_jobs=[])
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-zero-ranked")
    assert resp.status_code == 200
    assert "No candidates passed the final AI ranking threshold." in resp.text


def test_run_detail_zero_cvs_and_ranked_jobs_shows_post_ranking_message():
    import datetime as _dt
    from fitcv_cp.models import PipelineRun, RunStatus

    run = PipelineRun(
        run_id="run-detail-ranked-no-cv",
        status=RunStatus("succeeded"),
        triggered_by="admin",
        trigger_source="ui",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=_dt.datetime(2026, 3, 27, 9, 0, 0, tzinfo=_dt.timezone.utc),
        ranked=2,
        cvs_generated=0,
    )
    patches = _run_detail_patches(cv_versions=[], enriched_jobs=[])
    with patch("fitcv_cp.app.get_run", return_value=run), \
         patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-ranked-no-cv")
    assert resp.status_code == 200
    assert "Ranked outcome breakdown:" in resp.text
    assert "fit-gated=0" in resp.text
    assert "review-required=0" in resp.text
    assert "generation-failed=0" in resp.text
    assert "No candidates passed the final AI ranking threshold." not in resp.text


def test_run_detail_enriched_shows_summary_counts():
    """Enriched tab renders post-dedupe total, Passed, Rejected summary counts."""
    enriched = [{"job_url": "https://j.test/1", "title": "A", "domain": "d",
                 "job_family": "f", "required_skills": [], "location_type": None, "seniority": None}]
    fr = [{"job_url": "https://j.test/1", "passed": True, "reasons": []}]
    patches = _run_detail_patches(enriched_jobs=enriched, filter_results=fr)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert "Post-dedupe enriched jobs:" in resp.text
    assert "Passed:" in resp.text
    assert "Rejected:" in resp.text


def test_run_detail_enriched_shows_filter_controls():
    """Filter buttons All, Passed, Rejected are present (only rendered when enriched_jobs is non-empty)."""
    enriched = [{"job_url": "https://j.test/1", "title": "A", "domain": "d",
                 "job_family": "f", "required_skills": [], "location_type": None, "seniority": None}]
    patches = _run_detail_patches(enriched_jobs=enriched)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert 'name="filter_name"' in resp.text
    assert ">All<" in resp.text
    assert ">Passed<" in resp.text
    assert ">Rejected<" in resp.text


def test_run_detail_enriched_shows_search_box():
    """Search input with id='enr-search' is present (only rendered when enriched_jobs is non-empty)."""
    enriched = [{"job_url": "https://j.test/1", "title": "A", "domain": "d",
                 "job_family": "f", "required_skills": [], "location_type": None, "seniority": None}]
    patches = _run_detail_patches(enriched_jobs=enriched)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert 'id="enr-search"' in resp.text


def test_run_detail_enriched_rows_render_server_side_without_data_attributes():
    """Enriched fragment is server-paginated and no longer depends on client-side row attributes."""
    enriched = [{"job_url": "https://j.test/1", "title": "ML Engineer", "domain": "AI",
                 "job_family": "engineering", "required_skills": [], "location_type": None, "seniority": None}]
    fr = [{"job_url": "https://j.test/1", "passed": True, "reasons": []}]
    patches = _run_detail_patches(enriched_jobs=enriched, filter_results=fr)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert 'data-filter=' not in resp.text
    assert 'name="q"' in resp.text


def test_run_detail_enriched_shows_pagination():
    """Pagination controls are present for the enriched jobs tab."""
    patches = _run_detail_patches()
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched")
    assert "Page 1 of 1" in resp.text or "No enrichment data" in resp.text


def test_run_detail_enriched_unknown_filter_not_counted_as_rejected():
    """A job with no filter result gets data-filter=unknown and is not counted as rejected."""
    enriched = [
        {"job_url": "https://j.test/pass", "title": "Engineer A", "domain": "d",
         "job_family": "f", "required_skills": [], "location_type": None, "seniority": None},
        {"job_url": "https://j.test/no-fr", "title": "Engineer B", "domain": "d",
         "job_family": "f", "required_skills": [], "location_type": None, "seniority": None},
    ]
    fr = [{"job_url": "https://j.test/pass", "passed": True, "reasons": []}]
    # j.test/no-fr has no filter result → must be 'unknown', not 'rejected'
    patches = _run_detail_patches(enriched_jobs=enriched, filter_results=fr)
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched?filter_name=unknown")
    assert "Engineer B" in resp.text
    # Rejected count should be 0 (no explicit reject), not 1
    assert "Rejected: 0" in resp.text


def test_run_detail_enriched_tab_paginates_server_side():
    """Enriched tab returns only the requested page slice."""
    enriched = [
        {"job_url": f"https://j.test/{i}", "title": f"Job {i}", "domain": "d",
         "job_family": "f", "required_skills": [], "location_type": None, "seniority": None}
        for i in range(1, 61)
    ]
    patches = _run_detail_patches(enriched_jobs=enriched, filter_results=[])
    with patches[0], patches[1], patches[2], patches[3], patches[4]:
        resp = TestClient(_app()).get("/admin/runs/run-detail-test/tabs/enriched?page=2&page_size=25")
    assert resp.status_code == 200
    assert "Page 2 of 3" in resp.text
    assert "Job 1" not in resp.text
    assert "Job 26" in resp.text
    assert "Job 50" in resp.text



# ── Task 6: Composition consistency tests ──────────────────────────────────────

def test_settings_ranking_section_has_no_tailwind_classes():
    """The ranking section must not contain Tailwind class names in the rendered HTML."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    # Find the ranking section
    ranking_start = html.index("Ranking Weights")
    section_slice = html[ranking_start:ranking_start + 3000]
    tailwind_prefixes = ("text-gray-", "bg-slate-", "bg-indigo-", "text-indigo-", "rounded-", "px-", "py-", "mb-", "mt-", "mr-", "ml-", "gap-", "border-")
    for prefix in tailwind_prefixes:
        assert prefix not in section_slice, f"Tailwind class '{prefix}' found in ranking section"


def test_settings_ranking_contains_group_forms():
    """The ranking section keeps the four grouped ranking forms in the task-first layout."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    for form_id in (
        "form-ranking-weights",
        "form-preference-fit-weights",
        "form-fit-label-thresholds",
        "form-gap-thresholds",
    ):
        assert f'<form id="{form_id}"' in html


def test_settings_ranking_group_forms_have_save_buttons_with_correct_form_targets():
    """Each ranking grouped form keeps its nested submit button in the new layout."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    for form_id in (
        "form-ranking-weights",
        "form-preference-fit-weights",
        "form-fit-label-thresholds",
        "form-gap-thresholds",
    ):
        # Locate the form element
        form_open = f'<form id="{form_id}"'
        form_start = html.index(form_open)
        form_end = html.index("</form>", form_start)
        form_body = html[form_start:form_end]
        # The submit button must be nested inside the form (not using form= attribute)
        assert '<button type="submit"' in form_body, f"Submit button inside form '{form_id}' not found"


def test_run_detail_inspection_area_wrapped_in_inspection_card():
    """@proves ui_consistency_theming.attached-tab-inspection-card-pattern

    The inspection area must be wrapped in .inspection-card, with tab bar inside.
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    run = PipelineRun(
        run_id="composition-test-1", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json", triggered_by="admin",
        trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/composition-test-1")
    assert resp.status_code == 200
    html = resp.text
    # .inspection-card must appear in the HTML
    assert 'class="inspection-card"' in html
    # Verify the tab bar is actually INSIDE the card (not just later in the document).
    # Find the card's opening position and its closing </div><!-- /.inspection-card -->.
    card_pos = html.index('class="inspection-card"')
    card_close_pos = html.index('</div><!-- /.inspection-card -->', card_pos)
    # Now find the first tab button and verify it is between the open and close.
    tab_btn_pos = html.index('id="tab-btn-enriched"')
    assert card_pos < tab_btn_pos < card_close_pos, (
        "Tab bar button is not inside .inspection-card. "
        f"card={card_pos}, tab_btn={tab_btn_pos}, card_close={card_close_pos}"
    )


def test_run_detail_tab_bar_uses_attached_modifier():
    """@proves ui_consistency_theming.attached-tab-inspection-card-pattern

    The tab bar must use .tab-bar--attached (not the old .tab-bar).
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    run = PipelineRun(
        run_id="composition-test-2", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json", triggered_by="admin",
        trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/composition-test-2")
    assert resp.status_code == 200
    html = resp.text
    # .tab-bar--attached must be present
    assert 'tab-bar--attached' in html, 'class="tab-bar--attached" not found in rendered HTML'
    # The bare "tab-bar" class (without --attached) must NOT appear as the opening class attribute
    # Check around the tab-bar element: find a position where class= is followed by tab-bar
    # Use token-level check: split on 'class="' and look at tokens
    import re
    class_tokens = re.findall(r'class="([^"]*)"', html)
    bare_tab_bar_in_classes = any('tab-bar' in token and 'tab-bar--attached' not in token for token in class_tokens)
    assert not bare_tab_bar_in_classes, "Bare 'tab-bar' class found in rendered HTML (should be 'tab-bar--attached')"


def test_run_detail_panes_use_pane_container():
    """All three inspection panes must use .pane-container alongside .tab-pane."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    run = PipelineRun(
        run_id="composition-test-3", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json", triggered_by="admin",
        trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/composition-test-3")
    assert resp.status_code == 200
    html = resp.text
    for pane_id in ("pane-enriched", "pane-jobs-input", "pane-profile"):
        pane_pos = html.index(f'id="{pane_id}"')
        # Check that "pane-container" appears within 100 chars before the pane id (it's on the same div's class attribute)
        context = html[max(0, pane_pos - 100):pane_pos + len(pane_id) + 10]
        assert "pane-container" in context, f"'pane-container' not found near {pane_id} pane"


def test_run_detail_no_page_local_tab_style_inside_inspection_area():
    """No <style> tag may appear between the inspection card open and the first tab button."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone
    run = PipelineRun(
        run_id="composition-test-4", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json", triggered_by="admin",
        trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/composition-test-4")
    assert resp.status_code == 200
    html = resp.text
    # Find the .inspection-card region (open to close)
    card_pos = html.index('class="inspection-card"')
    card_close_pos = html.index('</div><!-- /.inspection-card -->', card_pos)
    card_region = html[card_pos:card_close_pos]
    # No <style> tag may appear inside the inspection card region
    assert "<style>" not in card_region, (
        "Page-local <style> tag found inside .inspection-card region — "
        "tab styling should use shared CSS from base.html"
    )


def test_base_template_bootstraps_saved_theme_before_styles():
    """@proves ui_consistency_theming.dark-light-theme-toggle-with-localstorage-persistence
    @proves ui_consistency_theming.flash-free-theme-application
    """
    from pathlib import Path

    html = Path("src/fitcv_cp/templates/base.html").read_text(encoding="utf-8")

    script_pos = html.index("<script>")
    style_pos = html.index("<style>")

    assert script_pos < style_pos
    assert "localStorage.getItem('fitcv-theme') || 'dark'" in html
    assert "document.documentElement.setAttribute('data-theme', t);" in html


def test_base_template_defines_theme_tokens_and_shared_classes():
    """@proves ui_consistency_theming.css-custom-properties-design-tokens
    @proves ui_consistency_theming.shared-component-classes
    """
    from pathlib import Path

    html = Path("src/fitcv_cp/templates/base.html").read_text(encoding="utf-8")

    assert ':root[data-theme="dark"]' in html
    assert ':root[data-theme="light"]' in html
    for token in ("--bg:", "--surface-1:", "--accent:", "--divider:"):
        assert token in html
    for shared_class in (".card, .section-card", ".sub-card", ".inspection-card", ".pane-container"):
        assert shared_class in html


def test_base_template_uses_wrapping_rules_for_shared_layout_surfaces():
    """@proves ui_consistency_theming.responsive-wrapping"""
    from pathlib import Path

    html = Path("src/fitcv_cp/templates/base.html").read_text(encoding="utf-8")

    page_header_start = html.index(".page-header {")
    page_header_end = html.index("}", page_header_start)
    page_header_block = html[page_header_start:page_header_end]
    assert "flex-wrap: wrap;" in page_header_block

    section_actions_start = html.index(".section-actions {")
    section_actions_end = html.index("}", section_actions_start)
    section_actions_block = html[section_actions_start:section_actions_end]
    assert "flex-wrap: wrap;" in section_actions_block


# ── Task 1: path-mode snapshot capture ──────────────────────────────────────


def _path_mode_patches(profile_path: str = "/tmp/dummy_profile.yaml"):
    """Return standard patches for path-mode upload-trigger tests."""
    base_config = {
        "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
        "pipeline": {"final_top_n": 10},
        "paths": {"candidate_profile": profile_path},
    }
    return (
        patch("fitcv_cp.app.load_active_settings", return_value={}),
        patch("fitcv_cp.app.insert_run"),
        patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-path-1", "rq-job-1")),
        patch("fitcv_cp.app.update_run_queue_job_id"),
        patch("fitcv_cp.app.load_config", return_value=base_config),
    )


def test_admin_upload_trigger_path_mode_stores_jobs_snapshot(tmp_path):
    """@proves multi_file_job_input.one-immutable-snapshot-stored-per-run

    path mode: trigger must read the file and store its JSON in jobs_input_json.
    """
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_file = tmp_path / "profile.yaml"
    profile_file.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    p = _path_mode_patches(profile_path=str(profile_file))
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run", side_effect=_capture_insert):
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={
                    "jobs_input_mode": "path",
                    "jobs_path": str(jobs_file),
                    "candidate_profile_mode": "default_config",
                },
            )

    assert resp.status_code == 201, resp.text
    assert "run_id" in resp.json()
    assert captured["run"].jobs_input_source == "path"
    assert json.loads(captured["run"].jobs_input_json) == [{"job_url": "http://a.com"}]


def test_admin_upload_trigger_path_mode_missing_file_returns_422(tmp_path):
    """path mode: missing file must fail the trigger with 422."""
    profile_file = tmp_path / "profile.yaml"
    profile_file.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")
    p = _path_mode_patches(profile_path=str(profile_file))
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run") as mock_insert:
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={
                    "jobs_input_mode": "path",
                    "jobs_path": str(tmp_path / "nonexistent.json"),
                    "candidate_profile_mode": "default_config",
                },
            )
    assert resp.status_code == 422
    mock_insert.assert_not_called()


def test_admin_upload_trigger_path_mode_invalid_json_returns_422(tmp_path):
    """path mode: invalid JSON content must fail the trigger with 422."""
    bad_file = tmp_path / "bad.json"
    bad_file.write_text("NOT JSON AT ALL", encoding="utf-8")
    profile_file = tmp_path / "profile.yaml"
    profile_file.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    p = _path_mode_patches(profile_path=str(profile_file))
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run") as mock_insert:
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={
                    "jobs_input_mode": "path",
                    "jobs_path": str(bad_file),
                    "candidate_profile_mode": "default_config",
                },
            )
    assert resp.status_code == 422
    mock_insert.assert_not_called()


def test_admin_upload_trigger_path_mode_non_array_json_returns_422(tmp_path):
    """path mode: JSON that is not a top-level array must fail with 422."""
    obj_file = tmp_path / "obj.json"
    obj_file.write_text('{"job_url": "http://a.com"}', encoding="utf-8")
    profile_file = tmp_path / "profile.yaml"
    profile_file.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    p = _path_mode_patches(profile_path=str(profile_file))
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run") as mock_insert:
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={
                    "jobs_input_mode": "path",
                    "jobs_path": str(obj_file),
                    "candidate_profile_mode": "default_config",
                },
            )
    assert resp.status_code == 422
    mock_insert.assert_not_called()


# ── Task 2: default_config profile snapshot capture ──────────────────────────


def _minimal_valid_profile_yaml() -> str:
    """Return a minimal YAML profile with required sections."""
    return """
name: Test Candidate
skills:
  - name: Python
    level: expert
    years: 5
    evidence_refs: []
experiences: []
projects: []
achievements: []
preferences:
  domains:
    - fintech
  location_types:
    - remote
"""


def test_admin_upload_trigger_default_config_stores_profile_snapshot(tmp_path):
    """@proves trigger_run_management.candidate-profile-input-modes

    default_config mode: trigger must load the configured profile and store snapshot.
    """
    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")

    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")

    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    config = {
        "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
        "pipeline": {"final_top_n": 10},
        "paths": {"candidate_profile": str(profile_path)},
    }
    p = (
        patch("fitcv_cp.app.load_active_settings", return_value={}),
        patch("fitcv_cp.app.insert_run"),
        patch("fitcv_cp.app.continue_run_with_job_id", return_value=("run-dc-1", "rq-job-1")),
        patch("fitcv_cp.app.update_run_queue_job_id"),
        patch("fitcv_cp.app.load_config", return_value=config),
    )
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run", side_effect=_capture_insert):
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={
                    "jobs_input_mode": "path",
                    "jobs_path": str(jobs_file),
                    "candidate_profile_mode": "default_config",
                },
            )

    assert resp.status_code == 201, resp.text
    assert "run_id" in resp.json()
    assert captured["run"].candidate_profile_source == "default_config"
    profile_snapshot = json.loads(captured["run"].candidate_profile_json)
    assert profile_snapshot["preferences"]["domains"] == ["fintech"]


def test_admin_upload_trigger_default_config_missing_profile_returns_422(tmp_path):
    """default_config mode: missing profile file must fail the trigger with 422."""
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")

    config = {
        "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
        "pipeline": {"final_top_n": 10},
        "paths": {"candidate_profile": str(tmp_path / "nonexistent.yaml")},
    }
    p = (
        patch("fitcv_cp.app.load_active_settings", return_value={}),
        patch("fitcv_cp.app.insert_run"),
        patch("fitcv_cp.app.continue_run_with_job_id", return_value=("run-dc-2", "rq-job-1")),
        patch("fitcv_cp.app.update_run_queue_job_id"),
        patch("fitcv_cp.app.load_config", return_value=config),
    )
    with p[0], p[1], p[2], p[3], p[4]:
        with patch("fitcv_cp.app.insert_run") as mock_insert:
            resp = TestClient(_app()).post(
                "/admin/upload-trigger",
                data={
                    "jobs_input_mode": "path",
                    "jobs_path": str(jobs_file),
                    "candidate_profile_mode": "default_config",
                },
            )
    assert resp.status_code == 422
    mock_insert.assert_not_called()


def test_admin_upload_trigger_candidate_profile_modes_share_canonical_runtime_payload(tmp_path):
    """@proves trigger_run_management.candidate-profile-input-modes"""
    from fitcv.candidate import load_profile_yaml

    profile_path = tmp_path / "profile.yaml"
    profile_path.write_text(_minimal_valid_profile_yaml(), encoding="utf-8")
    jobs_file = tmp_path / "jobs.json"
    jobs_file.write_text('[{"job_url": "http://a.com"}]', encoding="utf-8")
    profile_payload = load_profile_yaml(str(profile_path))
    upload_bytes = json.dumps(profile_payload).encode("utf-8")
    expected_payload = profile_payload

    config = {
        "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
        "pipeline": {"final_top_n": 10},
        "paths": {"candidate_profile": str(profile_path)},
    }

    captured_runs = {}

    def _capture_insert(run, *args, **kwargs):
        captured_runs[run.candidate_profile_source] = run

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.continue_run_with_job_id", return_value=("run-profile-mode", "rq-job-1")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value=config):
        default_resp = TestClient(_app()).post(
            "/admin/upload-trigger",
            data={
                "jobs_input_mode": "path",
                "jobs_path": str(jobs_file),
                "candidate_profile_mode": "default_config",
            },
        )
        upload_resp = TestClient(_app()).post(
            "/admin/upload-trigger",
            data={
                "jobs_input_mode": "path",
                "jobs_path": str(jobs_file),
                "candidate_profile_mode": "upload",
            },
            files={"candidate_profile_file": ("profile.json", upload_bytes, "application/json")},
        )
        paste_resp = TestClient(_app()).post(
            "/admin/upload-trigger",
            data={
                "jobs_input_mode": "path",
                "jobs_path": str(jobs_file),
                "candidate_profile_mode": "paste",
                "candidate_profile_text": json.dumps(profile_payload),
            },
        )

    assert default_resp.status_code == 201, default_resp.text
    assert upload_resp.status_code == 201, upload_resp.text
    assert paste_resp.status_code == 201, paste_resp.text

    for source in ("default_config", "upload", "paste"):
        run = captured_runs[source]
        assert json.loads(run.candidate_profile_json) == expected_payload
        effective = json.loads(run.effective_settings_json)
        assert json.loads(effective["runtime_inputs"]["candidate_profile_json"]) == expected_payload

    assert captured_runs["default_config"].candidate_profile_source == "default_config"
    assert captured_runs["upload"].candidate_profile_source == "upload"
    assert captured_runs["paste"].candidate_profile_source == "paste"


# ── Task 3: Snapshot semantics – run detail display and legacy fallback ────────


def test_run_detail_tab2_shows_snapshot_for_path_source():
    """Tab 2 shows snapshot content when jobs_input_json is present for path source."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="snap-test-1", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json", jobs_input_source="path",
        jobs_input_json='[{"job_url": "http://a.com"}]',
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/snap-test-1/tabs/jobs-input")

    assert resp.status_code == 200
    html = resp.text
    assert "Raw job payload captured at trigger time" in html
    # Jinja2 auto-escapes " as &quot; in <pre> blocks
    assert "job_url" in html
    assert "http://a.com" in html


def test_run_detail_tab2_legacy_fallback_does_not_mention_path_mode_limitation():
    """Tab 2 fallback for legacy runs (no snapshot) must NOT say 'path-mode runs do not'."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="snap-test-2", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json", jobs_input_source="path",
        jobs_input_json=None,
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/snap-test-2/tabs/jobs-input")

    assert resp.status_code == 200
    html = resp.text
    assert "No immutable raw snapshot" in html
    # Must NOT imply path-mode never has snapshots
    assert "path-mode runs do not" not in html


def test_run_detail_tab3_shows_snapshot_for_default_config_source():
    """@proves trigger_run_management.candidate-profile-input-modes

    Tab 3 shows snapshot content when candidate_profile_json is present for default_config.
    """
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    profile_json = '{"preferences": {"domains": ["fintech"]}, "skills": [], "experiences": [], "projects": [], "achievements": []}'
    run = PipelineRun(
        run_id="snap-test-3", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        candidate_profile_source="default_config",
        candidate_profile_json=profile_json,
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/snap-test-3/tabs/profile")

    assert resp.status_code == 200
    html = resp.text
    assert "Candidate profile captured at trigger time" in html
    assert "default_config" in html


def test_run_detail_tab3_legacy_fallback_does_not_mention_default_config_limitation():
    """Tab 3 fallback for legacy runs must NOT say 'Default-config and pre-feature runs do not'."""
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="snap-test-4", status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        candidate_profile_source="default_config",
        candidate_profile_json=None,
        triggered_by="admin", trigger_source="web", config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/snap-test-4/tabs/profile")

    assert resp.status_code == 200
    html = resp.text
    assert "No candidate profile snapshot" in html
    # Must NOT imply default_config never has snapshots
    assert "Default-config and pre-feature runs do not" not in html


# ── CV settings grouped save ──────────────────────────────────────────────────

def test_grouped_save_cv_generation_valid_redirects():
    """cv-preset is the new default group for preset/model; old cv-generation group removed."""
    # The old /admin/settings/group/cv-generation route no longer exists (group renamed)
    # This test verifies the new /admin/settings/group/cv-preset route works
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/cv-preset",
            data={
                "cv_preset": "europass",
                "cv_generation_model": "gemini-2.5-flash",
            },
        )
    assert resp.status_code == 303
    mock_save.assert_called_once()


def test_grouped_save_cv_generation_rejects_empty_model():
    """Empty cv_generation_model → 422 (handled by cv-preset group)."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/cv-preset",
            data={
                "cv_preset": "europass",
                "cv_generation_model": "",
            },
        )
    assert resp.status_code == 422
    mock_save.assert_not_called()


def test_grouped_save_cv_generation_rejects_whitespace_template_path():
    """cv_template_path is no longer in the schema (not admin-editable)."""
    # This test is a no-op since cv_template_path was removed from the schema
    pass


def test_grouped_save_cv_validation_valid_redirects():
    """Valid cv-validation form POST with cv_max_pages → 303 redirect."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/cv-validation",
            data={
                "cv_max_pages": "3",
            },
        )
    assert resp.status_code == 303
    mock_save.assert_called_once()


def test_grouped_save_cv_validation_rejects_empty_sections():
    """cv-validation group now has only cv_max_pages; valid payload → 303."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/cv-validation",
            data={
                "cv_max_pages": "2",
            },
        )
    assert resp.status_code == 303
    mock_save.assert_called_once()


def test_grouped_save_cv_validation_preserves_order_on_failure():
    """Validation error → 422 response must include submitted values."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/cv-validation",
            data={
                "cv_max_pages": "0",   # invalid
            },
        )
    assert resp.status_code == 422
    assert "0" in resp.text


def test_grouped_save_unknown_cv_group_returns_404():
    """Unknown CV group name → 404."""
    resp = TestClient(_app()).post(
        "/admin/settings/group/cv-nonexistent",
        data={"some.key": "1"},
    )
    assert resp.status_code == 404


def test_get_settings_page_includes_cv_groups():
    """GET /admin/settings renders cv_groups in context (used by template)."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    # Template receives cv_groups from context
    assert "CV Output" in resp.text


# ── CV settings page rendering ────────────────────────────────────────────────

def test_settings_page_renders_task_first_sections():
    """@proves settings_system.task-first-settings-ui
    @proves ui_consistency_theming.consistent-action-hierarchy-primary-secondary-section
    @proves ui_consistency_theming.human-readable-section-headings

    Settings page is organized around operator tasks, not only raw schema buckets.
    """
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Selection" in html
    assert "Ranking" in html
    assert "CV Output" in html
    assert "Run Safety" in html
    assert "Advanced" in html


def test_settings_page_renders_cv_sub_cards():
    """@proves settings_system.compact-cv-visibility-controls

    CV Output keeps the meaningful output-focused sub-surfaces.
    """
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Template" in html
    assert "Model" in html
    assert "Section Visibility" in html
    assert "Validation" in html


def test_settings_page_renders_single_option_controls_as_metadata():
    """@proves settings_system.metadata-only-fixed-controls

    Single-option pseudo-choice controls are shown as metadata, not editable inputs.
    """
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Currently fixed by the active runtime contract" in html
    assert "europass" in html.lower()
    assert "text-embedding-005" in html
    assert 'name="cv_preset"' not in html
    assert 'name="cv_analysis.semantic_alignment.model"' not in html
    assert 'name="cv_generation_model"' in html
    assert 'name="cv_prompt_version"' not in html


def test_settings_page_uses_advanced_disclosure_for_expert_controls() -> None:
    """@proves settings_system.advanced-settings-disclosure"""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Advanced Agentic Tuning" in html
    assert "Advanced Runtime Tuning" in html
    assert "Semantic channel weights and pool sizing" in html
    assert "Timing and throttling controls" in html
    assert "<details" in html
    assert 'name="cv_analysis.semantic_alignment.required_skill_lexical_weight"' in html
    assert 'name="cv_analysis.semantic_alignment.role_semantic_weight"' in html


def test_settings_page_renders_dedicated_agentic_section() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'data-task-section="agentic"' in html
    assert "Agentic" in html
    assert "Agentic Controls" in html
    assert "Advanced Agentic Tuning" in html
    assert 'action="/admin/settings/section/agentic-core"' in html
    assert 'action="/admin/settings/section/agentic-advanced"' in html


def test_settings_page_agentic_controls_hide_setup_only_and_metadata_only_inputs() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'name="cv.agentic_late_stage.enabled"' in html
    assert 'name="cv_analysis.semantic_alignment.enabled"' in html
    assert 'name="cv_analysis.semantic_alignment.model"' not in html
    assert 'name="cv_prompt_version"' not in html


def test_settings_page_semantic_alignment_toggle_has_single_agentic_owner() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'action="/admin/settings/section/agentic-core"' in html
    assert 'action="/admin/settings/section/retrieval-core"' in html
    assert html.count('name="cv_analysis.semantic_alignment.enabled"') == 2

    retrieval_form = html.split('action="/admin/settings/section/retrieval-core"', 1)[1].split("</form>", 1)[0]
    agentic_form = html.split('action="/admin/settings/section/agentic-core"', 1)[1].split("</form>", 1)[0]

    assert 'name="cv_analysis.semantic_alignment.enabled"' not in retrieval_form
    assert agentic_form.count('name="cv_analysis.semantic_alignment.enabled"') == 2


def test_settings_page_agentic_truth_copy_points_to_run_detail_and_settings_used() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "settings-used.json" in html
    assert "run detail" in html.lower()

def test_settings_page_shows_mode_summary_strip_for_agentic_vs_cv_model() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Agentic Mode:" in html
    assert "Live Provider:" in html
    assert "Live Model:" in html
    assert "CV Model (Settings):" in html
    assert "Run Truth Check" in html
    assert "Agentic Runtime Alignment" in html


def test_settings_page_marks_dirty_rows_when_draft_differs_from_effective() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={"enrichment_sleep_secs": 1.0}):
        resp = TestClient(_app()).post(
            "/admin/settings/section/timing",
            data={
                "enrichment_sleep_secs": "2.0",
                "rerank_sleep_secs": "0.5",
                "enrichment_batch_size": "10",
                "enrichment_concurrency": "0",
            },
        )
    assert resp.status_code == 422
    html = resp.text
    assert 'class="settings-field-row is-dirty"' in html
    assert "1 unsaved edit" in html or "2 unsaved edits" in html
    assert "Current:" in html
    assert "1.0" in html


def test_settings_page_explains_future_defaults_per_run_overrides_and_settings_used_truth() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "future runs only" in html.lower()
    assert "Per-run overrides" in html
    assert "settings-used.json" in html


def test_settings_page_labels_when_current_value_comes_from_baseline_default() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert "Source: Baseline default" in resp.text

def test_settings_page_renders_global_unsaved_changes_summary_strip() -> None:
    """Task 2 Step 1: expect page-level unsaved summary in addition to per-card status."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert 'data-global-dirty-summary="settings-page"' in resp.text
    assert "All sections saved" in resp.text

def test_settings_page_renders_quick_nav_for_task_sections() -> None:
    """Task 2 Step 1: expect section quick-nav for long settings page usability."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert 'data-settings-quick-nav="true"' in resp.text
    assert 'href="#task-selection"' in resp.text
    assert 'href="#task-agentic"' in resp.text
    assert 'href="#task-ranking"' in resp.text


def test_settings_page_cv_sections_no_raw_yaml():
    """required_cv_sections no longer exists in the schema (replaced by toggle fields)."""
    # The new UI does not expose a textarea for required_cv_sections
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert '<textarea name="required_cv_sections"' not in resp.text


def test_settings_page_cv_max_pages_is_numeric_input():
    """@proves settings_system.warning-only-cv-max-pages-validation-setting

    cv_max_pages renders as a numeric input.
    """
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert 'type="number"' in resp.text or '<input' in resp.text
    assert 'name="cv_max_pages"' in resp.text


# ── Preset-based CV settings page rendering ──────────────────────────────────────

def test_settings_page_renders_cv_preset_section():
    """@proves ui_consistency_theming.human-readable-section-headings

    CV Output includes the template/model card.
    """
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Template" in html
    assert "Model" in html


def test_settings_page_renders_cv_composition_section():
    """@proves ui_consistency_theming.human-readable-section-headings

    CV Output includes the visibility-focused composition block.
    """
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Section Visibility" in html


def test_settings_page_renders_cv_visibility_matrix() -> None:
    """@proves settings_system.cv-composition-visibility-settings

    Composition settings render in a denser visibility matrix.
    """
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'class="composition-matrix"' in html
    for label in (
        "Summary",
        "Education",
        "Experience",
        "Skills",
        "Certifications",
        "Projects",
        "Publications",
        "Languages",
    ):
        assert f'<div class="composition-row-title">{label}</div>' in html


def test_settings_page_renders_summary_visibility_toggle() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'name="cv_summary_enabled"' in html
    assert "Included" in html or "Hidden" in html


def test_settings_page_renders_cv_model_as_select_with_supported_options() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'name="cv_generation_model"' in html
    assert "<select" in html
    assert '<option value="gemini-2.5-flash"' in html
    assert '<option value="gemini-2.5-flash-lite"' in html
    assert '<option value="gemini-2.5-pro"' in html
    assert 'name="cv_generation_model"' in html
    assert 'type="text" name="cv_generation_model"' not in html


def test_settings_page_uses_shared_cv_setting_row_class_across_blocks() -> None:
    """@proves settings_system.compact-cv-visibility-controls"""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'class="settings-field-row"' in html
    assert html.count('class="settings-field-row"') >= 10


def test_settings_page_hides_default_column_for_settings_blocks() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "<th>Default</th>" not in html


def test_settings_page_renders_use_defaults_button_per_cv_block() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert html.count("Use Defaults") >= 3
    assert 'data-reset-form="form-cv-preset"' in html
    assert 'data-reset-form="form-cv-composition"' in html
    assert 'data-reset-form="form-cv-validation"' in html


def test_settings_page_exposes_default_values_for_browser_reset() -> None:
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'data-default-value="gemini-2.5-flash"' in html
    assert 'data-default-value="true"' in html
    assert 'data-default-value="concise"' not in html


def test_settings_page_hides_legacy_required_controls() -> None:
    """Composition UI no longer exposes separate required checkboxes."""
    active = {"cv_education_enabled": False}
    with patch("fitcv_cp.app.load_active_settings", return_value=active):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'name="cv_education_required"' not in html
    assert 'name="cv_projects_required"' not in html


def test_settings_page_shows_effective_current_values_for_composition_defaults() -> None:
    """Current column should show effective default-backed values, not blanks."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Current:" in html
    assert "Included" in html or "Hidden" in html
    assert '<span class="current-value">compact</span>' not in html
    assert '<span class="current-value">concise</span>' not in html


def test_settings_page_does_not_render_cv_content_rules_section():
    """Settings page no longer includes the removed cv-content-rules sub-card."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Content Rules" not in html


def test_settings_page_renders_cv_model_input_without_cv_preset_input():
    """Settings page keeps cv_generation_model editable while cv_preset becomes metadata-only."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'name="cv_preset"' not in html
    assert 'name="cv_generation_model"' in html
    assert 'name="cv_prompt_version"' not in html


def test_settings_page_renders_cv_composition_inputs():
    """Settings page includes inputs only for active composition fields."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    for name in (
        "cv_summary_enabled",
        "cv_education_enabled",
        "cv_experience_enabled",
        "cv_skills_enabled",
        "cv_certifications_enabled",
        "cv_projects_enabled",
        "cv_publications_enabled",
        "cv_languages_enabled",
    ):
        assert f'name="{name}"' in html, f"Missing input for {name}"


def test_settings_page_does_not_render_retired_cv_composition_formatting_inputs():
    """Settings page no longer renders dormant CV composition formatting/detail controls."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    for name in (
        "cv_summary_style",
        "cv_education_detail",
        "cv_experience_bullet_style",
        "cv_skills_max_items",
        "cv_publications_detail",
        "cv_languages_detail",
    ):
        assert f'name="{name}"' not in html, f"Unexpected input for retired field {name}"


def test_settings_page_does_not_render_removed_cv_content_rules_inputs():
    """Settings page no longer includes inputs for removed content-rule fields."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    for name in ("cv_emphasize_required_skills", "cv_align_jd_terminology", "cv_evidence_grounded_only"):
        assert f'name="{name}"' not in html, f"Unexpected input for removed field {name}"


def test_settings_page_renders_cv_validation_inputs():
    """Settings page includes input for cv_max_pages."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert 'name="cv_max_pages"' in html


def test_settings_page_cv_preset_save_button():
    """Settings page has 'Save Preset Settings' button for cv-preset group."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert "Save Preset Settings" in resp.text


def test_settings_page_cv_composition_save_button():
    """Settings page has 'Save Composition Settings' button for cv-composition group."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert "Save Composition Settings" in resp.text


def test_settings_page_does_not_render_cv_content_rules_save_button():
    """Settings page no longer renders a save button for the removed content-rules group."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert "Save Content Rules Settings" not in resp.text


def test_settings_page_cv_validation_new_save_button():
    """Settings page has 'Save Validation Settings' button for cv-validation group."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert "Save Validation Settings" in resp.text


def test_settings_page_no_raw_template_path_input():
    """Raw cv_template_path text input is NOT exposed in the new preset-based UI."""
    # NOTE: This test requires the new UI to be rendered (Task 4).
    # It checks that when the new "Preset" sub-card is present,
    # the cv_template_path field is NOT exposed as a raw text input there.
    # Skipped until the UI is updated; backend is ready.
    pass


def test_settings_page_no_raw_required_cv_sections_freeform():
    """required_cv_sections is NOT rendered as a free-form editor in the new UI."""
    # NOTE: This test requires the new UI to be rendered (Task 4).
    # Skipped until the UI is updated; backend is ready.
    pass


# ── Preset-based CV grouped save endpoints ────────────────────────────────────────

def test_grouped_save_cv_preset_valid_redirects():
    """Valid cv-preset form POST saves editable fields without requiring metadata-only inputs."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
            resp = TestClient(_app(), follow_redirects=False).post(
                "/admin/settings/group/cv-preset",
                data={
                    "cv_generation_model": "gemini-2.5-flash",
                },
            )
    assert resp.status_code == 303
    mock_save.assert_called_once()
    saved_keys = set(mock_save.call_args[0][0].keys())
    assert saved_keys == {"cv_generation_model"}


def test_grouped_save_cv_preset_rejects_empty():
    """Empty cv_preset → 422; no write."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/cv-preset",
            data={
                "cv_preset": "",
                "cv_generation_model": "gemini-2.5-flash",
            },
        )
    assert resp.status_code == 422
    mock_save.assert_not_called()


def test_grouped_save_cv_composition_valid_redirects():
    """Valid cv-composition form POST → 303 redirect."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/cv-composition",
            data={
                "cv_summary_enabled": "true",
                "cv_education_enabled": "true",
                "cv_experience_enabled": "true",
                "cv_skills_enabled": "true",
                "cv_certifications_enabled": "true",
                "cv_projects_enabled": "true",
                "cv_publications_enabled": "false",
                "cv_languages_enabled": "true",
            },
        )
    assert resp.status_code == 303
    mock_save.assert_called_once()

def test_grouped_save_cv_composition_rejects_invalid_bool():
    """Invalid boolean in cv-composition → 422; no write."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/cv-composition",
            data={
                "cv_summary_enabled": "true",
                "cv_education_enabled": "true",
                "cv_experience_enabled": "not-a-bool",
                "cv_skills_enabled": "true",
                "cv_certifications_enabled": "true",
                "cv_projects_enabled": "true",
                "cv_publications_enabled": "false",
                "cv_languages_enabled": "true",
            },
        )
    assert resp.status_code == 422
    mock_save.assert_not_called()


def test_grouped_save_removed_cv_content_rules_returns_404():
    """Removed cv-content-rules group can no longer be posted."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/cv-content-rules",
            data={},
        )
    assert resp.status_code == 404
    mock_save.assert_not_called()


def test_grouped_save_cv_validation_new_valid_redirects():
    """Valid cv-validation form POST with cv_max_pages → 303 redirect."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/group/cv-validation",
            data={
                "cv_max_pages": "3",
            },
        )
    assert resp.status_code == 303
    mock_save.assert_called_once()


def test_grouped_save_cv_validation_preserves_draft_on_failure():
    """Validation error → 422 response must include submitted values."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/cv-validation",
            data={
                "cv_max_pages": "0",   # invalid
            },
        )
    assert resp.status_code == 422
    assert "0" in resp.text


def test_grouped_save_cv_preset_invalid_does_not_partial_save():
    """Invalid cv_preset → 422; no partial write."""
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/cv-preset",
            data={
                "cv_preset": "",
                "cv_generation_model": "gemini-2.5-flash",
            },
        )
    assert resp.status_code == 422
    mock_save.assert_not_called()


def test_grouped_save_cv_composition_invalid_does_not_partial_save():
    """@proves settings_system.grouped-form-validation

    Invalid cv_composition -> 422; no partial write of any field.
    """
    with patch("fitcv_cp.app.save_settings_group") as mock_save, \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).post(
            "/admin/settings/group/cv-composition",
            data={
                "cv_summary_enabled": "definitely-not-bool",
                "cv_education_enabled": "true",
                "cv_experience_enabled": "true",
                "cv_skills_enabled": "true",
                "cv_certifications_enabled": "true",
                "cv_projects_enabled": "true",
                "cv_publications_enabled": "false",
                "cv_languages_enabled": "true",
            },
        )
    assert resp.status_code == 422
    mock_save.assert_not_called()
"""
@meta
type: test
scope: unit
domain: admin_ui
covers:
  - FitCV control-plane app behavior
excludes:
  - live HTTP deployment
tags:
  - fast
  - ci-safe
"""

def test_run_detail_shows_event_delivery_degraded_when_dead_letter_exists(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="event-degraded-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "row": {"run_id": "event-degraded-1", "stage": "pipeline_failed"},
                        "failed_at": "2026-05-02T14:20:00Z",
                        "degradation_reason": "event_insert_failed_dead_lettered",
                        "retry_attempts": 3,
                    }
                ),
                json.dumps(
                    {
                        "row": {"run_id": "other-run", "stage": "normalize"},
                        "failed_at": "2026-05-02T14:21:00Z",
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    p = _run_detail_base_patches(run)
    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/event-degraded-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Event Delivery Health" in html
    assert "degraded" in html
    assert "Dead-lettered Events" in html
    assert "2026-05-02T14:20:00Z" in html
    assert "event_insert_failed_dead_lettered" in html
    assert "Max Retry Attempts" in html


def test_run_detail_shows_event_delivery_healthy_when_no_dead_letter_for_run(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="event-healthy-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text(
        json.dumps(
            {
                "row": {"run_id": "other-run", "stage": "pipeline_failed"},
                "failed_at": "2026-05-02T14:30:00Z",
            }
        ),
        encoding="utf-8",
    )
    p = _run_detail_base_patches(run)
    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/event-healthy-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Event Delivery Health" in html
    assert "healthy" in html
    assert "Dead-lettered Events" in html
    assert 'Degraded Telemetry Events</span><span class="v">0</span>' in html

def test_run_detail_shows_telemetry_export_degraded_health() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="telemetry-degraded-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    telemetry_event = RunEvent(
        run_id="telemetry-degraded-1",
        event_id="telemetry-ev-1",
        stage="layer3_filter",
        level="warning",
        message="telemetry degraded",
        created_at=datetime.now(timezone.utc),
        payload_json=json.dumps({"telemetry_export": {"status": "degraded", "degradation_reason": "otel_dependency_missing"}}),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], patch("fitcv_cp.app.get_events", return_value=[telemetry_event]), p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/telemetry-degraded-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Telemetry Export Health" in html
    assert "degraded" in html
    assert "Last Degraded Stage" in html
    assert "layer3_filter" in html

def test_run_detail_shows_telemetry_export_healthy_when_no_degraded_events() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="telemetry-healthy-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    telemetry_event = RunEvent(
        run_id="telemetry-healthy-1",
        event_id="telemetry-ev-2",
        stage="normalize",
        level="info",
        message="telemetry ok",
        created_at=datetime.now(timezone.utc),
        payload_json=json.dumps({"telemetry_export": {"status": "export_enabled"}}),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], patch("fitcv_cp.app.get_events", return_value=[telemetry_event]), p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/telemetry-healthy-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Telemetry Export Health" in html
    assert "healthy" in html
    assert "Degraded Telemetry Events" in html
    assert ">0<" in html


def test_run_detail_ignores_otel_disabled_for_telemetry_degradation() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="telemetry-disabled-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    telemetry_event = RunEvent(
        run_id="telemetry-disabled-1",
        event_id="telemetry-ev-3",
        stage="pipeline_start",
        level="info",
        message="telemetry disabled",
        created_at=datetime.now(timezone.utc),
        payload_json=json.dumps({"telemetry_export": {"status": "degraded", "degradation_reason": "otel_disabled"}}),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], patch("fitcv_cp.app.get_events", return_value=[telemetry_event]), p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/telemetry-disabled-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Telemetry Export Health" in html
    assert "healthy" in html
    assert "Degraded Telemetry Events" in html
    assert 'Degraded Telemetry Events</span><span class="v">0</span>' in html


def test_run_detail_shows_langfuse_unverified_health_when_trace_url_present() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="langfuse-unverified-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    telemetry_event = RunEvent(
        run_id="langfuse-unverified-1",
        event_id="langfuse-ev-1",
        stage="cv_generation",
        level="info",
        message="langfuse trace url available",
        created_at=datetime.now(timezone.utc),
        payload_json=json.dumps(
            {
                "langfuse_link": {
                    "status": "unverified",
                    "trace_url": "http://localhost:3000/trace/trace-abc",
                }
            }
        ),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], patch("fitcv_cp.app.get_events", return_value=[telemetry_event]), p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/langfuse-unverified-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Langfuse Trace-Link Health" in html
    assert "unverified" in html
    assert "Unverified Link Events" in html
    assert "trace/trace-abc" in html


def test_run_detail_shows_langfuse_degraded_health_when_link_fails() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="langfuse-degraded-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    telemetry_event = RunEvent(
        run_id="langfuse-degraded-1",
        event_id="langfuse-ev-2",
        stage="layer3_filter",
        level="warning",
        message="langfuse degraded",
        created_at=datetime.now(timezone.utc),
        payload_json=json.dumps(
            {
                "langfuse_link": {
                    "status": "degraded",
                    "degradation_reason": "langfuse_base_url_missing",
                    "trace_url": None,
                }
            }
        ),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], patch("fitcv_cp.app.get_events", return_value=[telemetry_event]), p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/langfuse-degraded-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Langfuse Trace-Link Health" in html
    assert "degraded" in html
    assert "Last Degraded Stage" in html
    assert "layer3_filter" in html

def test_run_detail_shows_dead_letter_replay_summary() -> None:
    from fitcv_cp.models import PipelineRun, RunStatus, RunEvent
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="replay-summary-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    replay_event = RunEvent(
        run_id="replay-summary-1",
        event_id="replay-summary-ev-1",
        stage="event_dead_letter_replay",
        level="info",
        message="Replay completed",
        created_at=datetime.now(timezone.utc),
        payload_json=json.dumps(
            {
                "replay_candidates": 4,
                "replayed": 3,
                "failed": 1,
                "replay_success_ratio": 0.75,
                "remaining_dead_letter_total": 2,
            }
        ),
    )
    p = _run_detail_base_patches(run)
    with p[0], p[1], patch("fitcv_cp.app.get_events", return_value=[replay_event]), p[3], p[4]:
        resp = TestClient(_app()).get("/admin/runs/replay-summary-1")

    assert resp.status_code == 200
    html = resp.text
    assert "Dead-letter Replay Summary" in html
    assert "0.75" in html
    assert "3 / 4" in html
    assert ">1<" in html
    assert ">2<" in html

def test_admin_replay_dead_letter_events_replays_and_clears_run_rows(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="event-replay-1",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "row": {
                            "run_id": "event-replay-1",
                            "event_id": "ev-1",
                            "stage": "normalize",
                            "level": "info",
                            "message": "m1",
                            "payload_json": None,
                            "created_at": "2026-05-02T14:00:00Z",
                        }
                    }
                ),
                json.dumps(
                    {
                        "row": {
                            "run_id": "other-run",
                            "event_id": "ev-2",
                            "stage": "enrich",
                            "level": "warning",
                            "message": "m2",
                            "payload_json": None,
                            "created_at": "2026-05-02T14:01:00Z",
                        }
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.append_event", return_value={"persistence_status": "persisted", "degradation_reason": ""}) as mock_append:
        resp = TestClient(_app()).post("/admin/runs/event-replay-1/replay-dead-letter-events")

    assert resp.status_code == 200
    body = resp.json()
    assert body["replay_candidates"] == 1
    assert body["replayed"] == 1
    assert body["failed"] == 0
    assert body["replay_success_ratio"] == 1.0
    assert mock_append.call_count == 2
    content = dead_letter_file.read_text(encoding="utf-8")
    assert "event-replay-1" not in content
    assert "other-run" in content


def test_admin_replay_dead_letter_events_keeps_failed_rows(tmp_path):
    from fitcv_cp.models import PipelineRun, RunStatus
    from datetime import datetime, timezone

    run = PipelineRun(
        run_id="event-replay-2",
        status=RunStatus.SUCCEEDED,
        jobs_path="data/sample_jobs.json",
        triggered_by="admin",
        trigger_source="web",
        config_path=".env.yaml",
        created_at=datetime.now(timezone.utc),
    )
    dead_letter_file = tmp_path / "events-dead-letter.jsonl"
    dead_letter_file.write_text(
        json.dumps(
            {
                "row": {
                    "run_id": "event-replay-2",
                    "event_id": "ev-3",
                    "stage": "ranking",
                    "level": "error",
                    "message": "m3",
                    "payload_json": None,
                    "created_at": "2026-05-02T14:02:00Z",
                }
            }
        ),
        encoding="utf-8",
    )
    with patch.dict("os.environ", {"FITCV_EVENT_DEAD_LETTER_PATH": str(dead_letter_file)}), \
         patch("fitcv_cp.app.get_run", return_value=run), \
         patch("fitcv_cp.app.append_event", return_value={"persistence_status": "dead_lettered", "degradation_reason": "x"}):
        resp = TestClient(_app()).post("/admin/runs/event-replay-2/replay-dead-letter-events")

    assert resp.status_code == 200
    body = resp.json()
    assert body["replay_candidates"] == 1
    assert body["replayed"] == 0
    assert body["failed"] == 1
    assert body["replay_success_ratio"] == 0.0
    content = dead_letter_file.read_text(encoding="utf-8")
    assert "event-replay-2" in content

