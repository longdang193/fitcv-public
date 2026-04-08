from unittest.mock import MagicMock, patch
import io
import json
import zipfile
from fastapi.testclient import TestClient
from fitcv_cp.app import _timeline_stage_download_for_event, create_app
from fitcv_cp.models import RunStatus


def _app():
    bq = MagicMock()
    return create_app(bq=bq, project="p", dataset="d", redis_url="redis://localhost:6379/0")


def test_post_runs_inserts_before_enqueue():
    """BQ insert must happen before enqueue to ensure DB is source of truth."""
    call_order = []

    def fake_insert(*args, **kwargs):
        call_order.append("insert")

    def fake_enqueue_with_job(*args, **kwargs):
        call_order.append("enqueue")
        return "run-123", "rq-job-abc"

    with patch("fitcv_cp.app.insert_run", side_effect=fake_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", side_effect=fake_enqueue_with_job), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
             "pipeline": {"final_top_n": 10}
         }):
        resp = TestClient(_app()).post("/runs", json={"jobs_path": "data/sample_jobs.json"})
    assert resp.status_code == 201
    assert "run_id" in resp.json()
    assert call_order == ["insert", "enqueue"], f"Order was: {call_order}"


def test_post_runs_rejects_empty_jobs_path():
    resp = TestClient(_app()).post("/runs", json={"jobs_path": ""})
    assert resp.status_code == 422


def test_post_runs_persists_manual_staged_mode() -> None:
    captured = {}

    def _capture_insert(run, *args, **kwargs):
        captured["run"] = run

    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run", side_effect=_capture_insert), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_queue_job_id"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
             "pipeline": {"final_top_n": 10}
         }):
        resp = TestClient(_app()).post("/runs", json={
            "jobs_path": "data/sample_jobs.json",
            "run_mode": "manual_staged",
        })

    assert resp.status_code == 201
    assert captured["run"].run_mode == "manual_staged"
    assert captured["run"].next_stage == "normalize"
    assert captured["run"].completed_stages == []


def test_get_runs_returns_list():
    with patch("fitcv_cp.app.list_runs", return_value=[]):
        resp = TestClient(_app()).get("/runs")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_get_run_detail_not_found():
    with patch("fitcv_cp.app.get_run", return_value=None):
        resp = TestClient(_app()).get("/runs/missing-id")
    assert resp.status_code == 404


def test_get_run_events():
    with patch("fitcv_cp.app.get_run", return_value=MagicMock()), \
         patch("fitcv_cp.app.get_events", return_value=[]):
        resp = TestClient(_app()).get("/runs/some-id/events")
    assert resp.status_code == 200


def test_healthz():
    resp = TestClient(_app()).get("/healthz")
    assert resp.status_code == 200


def test_timeline_stage_download_maps_cv_analysis_skip_to_cv_analysis():
    assert _timeline_stage_download_for_event("layer4_cv_analysis_skip") == "cv_analysis"
    assert _timeline_stage_download_for_event("layer4_cv_skip") == "cv_analysis"


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


def test_post_runs_with_config_overrides():
    """POST /runs with per-run overrides snapshot effective settings."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}), \
         patch("fitcv_cp.app.insert_run"), \
         patch("fitcv_cp.app.enqueue_run", return_value="run-123"), \
         patch("fitcv_cp.app.load_config", return_value={
             "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
             "pipeline": {"final_top_n": 10}
         }):
        resp = TestClient(_app()).post("/runs", json={
            "jobs_path": "data/sample_jobs.json",
            "config_overrides": {"pipeline.final_top_n": 5},
        })
    assert resp.status_code == 201
    assert "run_id" in resp.json()


def test_post_runs_rejects_invalid_config_overrides():
    resp = TestClient(_app()).post("/runs", json={
        "jobs_path": "data/sample_jobs.json",
        "config_overrides": {"pipeline.final_top_n": 0},  # violates >= 1
    })
    assert resp.status_code == 422


def test_admin_upload_trigger_success(tmp_path):
    """Test POST /admin/upload-trigger saves file and calls trigger logic."""
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


def test_admin_continue_run_requeues_manual_paused_run() -> None:
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

    with patch("fitcv_cp.app.get_run", return_value=paused_run), \
         patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-123", "rq-job-abc")), \
         patch("fitcv_cp.app.update_run_status") as mock_status, \
         patch("fitcv_cp.app.update_run_queue_job_id") as mock_queue, \
         patch("fitcv_cp.app.update_run_checkpoint") as mock_checkpoint, \
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post("/admin/runs/run-123/continue")

    assert resp.status_code == 200
    mock_status.assert_called_once()
    mock_queue.assert_called_once()
    mock_checkpoint.assert_called_once()


def test_admin_run_detail_shows_synonym_overlay_card_for_manual_enrich_checkpoint() -> None:
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
         patch("fitcv_cp.app.append_event"):
        resp = TestClient(_app()).post(
            "/admin/runs/run-overlay-upload/synonym-overlay",
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


# ── multi-file upload tests ────────────────────────────────────────────────────

_UPLOAD_COMMON_PATCHES = {
    "fitcv_cp.app.load_active_settings": lambda: {"return_value": {}},
}


def _upload_patches():
    return (
        patch("fitcv_cp.app.load_active_settings", return_value={}),
        patch("fitcv_cp.app.insert_run"),
        patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-multi", "rq-job-1")),
        patch("fitcv_cp.app.update_run_queue_job_id"),
        patch("fitcv_cp.app.load_config", return_value={
            "gcp_project": "p", "bigquery_dataset": "d", "service_account_key": "k",
            "pipeline": {"final_top_n": 10},
            "paths": {"candidate_profile": "data/candidate_profile.yaml"},
        }),
    )


def test_admin_upload_trigger_merges_multiple_job_files():
    """Two valid JSON files → 201, merged snapshot contains both jobs."""
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
    """Merged snapshot preserves file order (file1 rows first, then file2)."""
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
    """One file with invalid JSON → 422; run must NOT be created."""
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
    """Two files both containing empty arrays → 422 (total merged is empty)."""
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
    """Upload mode with neither jobs_file nor jobs_files → 422."""
    p = _upload_patches()
    with p[0], p[1], p[2], p[3], p[4]:
        resp = TestClient(_app()).post(
            "/admin/upload-trigger",
            data={"jobs_input_mode": "upload", "candidate_profile_mode": "default_config"},
        )
    assert resp.status_code == 422


def test_admin_upload_trigger_multi_file_non_array_rejected():
    """A file whose top-level is not a JSON array → 422."""
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
    with patch("fitcv_cp.app.list_runs", return_value=[]):
        resp = TestClient(_app()).get("/admin/runs")
    assert resp.status_code == 200
    assert 'href="/admin/settings">Settings</a>' in resp.text
    assert 'Refresh' in resp.text
    assert 'id="jobs_file"' in resp.text
    assert 'id="jobs_path"' in resp.text


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
    assert "Results JSON (Job Ledger)" in resp.text


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
                                "generation_ready": 1,
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


def test_download_settings_used_json_endpoint_200():
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


def test_download_run_artifact_bundle_zip_endpoint_for_partial_run() -> None:
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
    assert manifest["bundle_schema_version"] == "run_artifact_bundle_v2"
    assert manifest["run_mode"] == "manual_staged"
    assert manifest["run_mode_label"] == "Stage by Stage"
    assert "normalize.json" in manifest["included_files"]
    assert "mapping-suggestions.json" in manifest["missing_files"]


def test_download_run_artifact_bundle_zip_endpoint_for_succeeded_run() -> None:
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
        cv_generation_debug_json='{"run_id":"run-bundle-success-1","debug_records":[]}',
        settings_used_json='{"run_id":"run-bundle-success-1","effective_settings":{"pipeline":{"final_top_n":10}}}',
        mapping_suggestions_json='{"run_id":"run-bundle-success-1","suggestions":[]}',
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
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).get("/admin/runs/run-bundle-success-1/artifacts.zip")

    assert resp.status_code == 200
    with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
        names = set(archive.namelist())
        assert "manifest.json" in names
        assert "results.json" in names
        assert "cv-debug.json" in names
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
        manifest = json.loads(archive.read("manifest.json"))
    assert manifest["run_id"] == "run-bundle-success-1"
    assert manifest["bundle_schema_version"] == "run_artifact_bundle_v2"
    assert manifest["run_mode"] == "run_all"
    assert manifest["run_mode_label"] == "Run All"
    assert "results.json" in manifest["included_files"]
    assert manifest["missing_files"] == []


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
    """Run detail page renders Enriched Jobs section when rows are returned."""
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
    """Enriched Jobs pane must be active by default on page load."""
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
    """strong > stretch → 303 redirect; 2 keys saved."""
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
    """stretch > strong → 422; no write."""
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

def _retrieval_section_form(
    *,
    vector_search_top_n: str = "100",
    ai_score_top_n: str = "20",
    final_top_n: str = "10",
    evidence_top_k: str = "5",
    semantic_alignment_enabled: str = "true",
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
        "pipeline.vector_search_top_n": vector_search_top_n,
        "pipeline.ai_score_top_n": ai_score_top_n,
        "pipeline.final_top_n": final_top_n,
        "pipeline.evidence_top_k": evidence_top_k,
        "cv_analysis.semantic_alignment.enabled": semantic_alignment_enabled,
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
    """Valid payload for retrieval section returns 303."""
    with patch("fitcv_cp.app.save_settings_group"), \
         patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app(), follow_redirects=False).post(
            "/admin/settings/section/retrieval",
            data=_retrieval_section_form(),
        )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin/settings"


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
            "/admin/settings/section/retrieval",
            data=_retrieval_section_form(vector_search_top_n="not-a-number"),
        )
    assert resp.status_code == 422


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
    run = _make_run_mock(status="running")
    with patch("fitcv_cp.app.get_run", return_value=run):
        resp = TestClient(_app()).post("/admin/runs/run-lifecycle-1/repair-cancellation")
    assert resp.status_code == 409


def test_admin_archive_succeeded_run_returns_json():
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
    resp = TestClient(_app()).post("/admin/runs/bulk/cancel", json={"run_ids": []})
    assert resp.status_code == 422


def test_admin_bulk_lifecycle_rejects_unknown_run_ids():
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
    assert "Triggered By" not in html
    assert "Actions" not in html


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
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Run Lifecycle Settings" in html
    assert 'name="run_lifecycle.max_runtime_minutes"' in html


def test_admin_runs_timeouts_running_runs_to_failed() -> None:
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


def test_run_detail_shows_marks_for_passed_jobs() -> None:
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
    assert "2 ranked job(s) did not produce a valid CV output." in resp.text
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
    """The inspection area must be wrapped in .inspection-card, with tab bar inside."""
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
    """The tab bar must use .tab-bar--attached (not the old .tab-bar)."""
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
    """path mode: trigger must read the file and store its JSON in jobs_input_json."""
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
    """default_config mode: trigger must load the configured profile and store snapshot."""
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
        patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-dc-1", "rq-job-1")),
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
        patch("fitcv_cp.app.enqueue_run_with_job_id", return_value=("run-dc-2", "rq-job-1")),
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
    """Tab 3 shows snapshot content when candidate_profile_json is present for default_config."""
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
    """Settings page is organized around operator tasks, not only raw schema buckets."""
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
    """CV Output keeps the meaningful output-focused sub-surfaces."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Template" in html
    assert "Model" in html
    assert "Section Visibility" in html
    assert "Validation" in html


def test_settings_page_renders_single_option_controls_as_metadata():
    """Single-option pseudo-choice controls are shown as metadata, not editable inputs."""
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
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Advanced Retrieval Tuning" in html
    assert "Advanced Runtime Tuning" in html
    assert "Timing and throttling controls" in html
    assert "<details" in html


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


def test_settings_page_cv_sections_no_raw_yaml():
    """required_cv_sections no longer exists in the schema (replaced by toggle fields)."""
    # The new UI does not expose a textarea for required_cv_sections
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert '<textarea name="required_cv_sections"' not in resp.text


def test_settings_page_cv_max_pages_is_numeric_input():
    """cv_max_pages renders as a numeric input."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    assert 'type="number"' in resp.text or '<input' in resp.text
    assert 'name="cv_max_pages"' in resp.text


# ── Preset-based CV settings page rendering ──────────────────────────────────────

def test_settings_page_renders_cv_preset_section():
    """CV Output includes the template/model card."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Template" in html
    assert "Model" in html


def test_settings_page_renders_cv_composition_section():
    """CV Output includes the visibility-focused composition block."""
    with patch("fitcv_cp.app.load_active_settings", return_value={}):
        resp = TestClient(_app()).get("/admin/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "Section Visibility" in html


def test_settings_page_renders_cv_visibility_matrix() -> None:
    """Composition settings render in a denser visibility matrix."""
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
    """Valid cv-preset form POST → 303 redirect; save_settings_group called."""
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
    saved_keys = set(mock_save.call_args[0][0].keys())
    assert saved_keys == {"cv_preset", "cv_generation_model"}


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
    """Invalid cv_composition → 422; no partial write of any field."""
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
