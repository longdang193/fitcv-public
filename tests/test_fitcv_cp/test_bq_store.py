from unittest.mock import MagicMock
from fitcv_cp.bq_store import insert_run, update_run_status, append_event, get_run, list_runs, get_events, list_cvs_for_run, get_cv_markdown, list_run_structured_jobs, list_filter_results_for_run, update_run_results_export, update_run_cv_generation_debug, update_run_stage_transition_artifacts, update_run_settings_used, update_run_checkpoint, update_run_mapping_suggestions, update_run_effective_settings
from fitcv_cp.models import PipelineRun, RunEvent, RunStatus
import datetime
import uuid
from google.api_core.exceptions import BadRequest


def _make_run() -> PipelineRun:
    return PipelineRun(
        run_id=str(uuid.uuid4()), status=RunStatus.QUEUED, triggered_by="admin",
        trigger_source="ui", jobs_path="data/sample_jobs.json",
        config_path=".env.yaml", created_at=datetime.datetime.now(datetime.timezone.utc),
    )


def test_insert_run_calls_bq():
    bq = MagicMock()
    insert_run(_make_run(), bq, project="p", dataset="d")
    bq.query.assert_called_once()


def test_update_run_status_uses_parameterized_query():
    bq = MagicMock()
    update_run_status("rid", RunStatus.RUNNING, bq, project="p", dataset="d")
    bq.query.assert_called_once()
    # Verify parameterized: run_id must NOT appear literally in the SQL string
    sql_arg = bq.query.call_args[0][0]
    assert "rid" not in sql_arg, "SQL must use query parameters, not string interpolation"


def test_update_run_status_retries_on_pipeline_runs_concurrent_update(monkeypatch) -> None:
    bq = MagicMock()
    first_job = MagicMock()
    first_job.result.side_effect = BadRequest(
        "Could not serialize access to table fitcv-491123:fitcv.pipeline_runs due to concurrent update"
    )
    second_job = MagicMock()
    second_job.result.return_value = None
    bq.query.side_effect = [first_job, second_job]

    sleep_calls: list[float] = []
    monkeypatch.setattr("fitcv_cp.bq_store.time.sleep", lambda seconds: sleep_calls.append(seconds))

    update_run_status("rid", RunStatus.RUNNING, bq, project="p", dataset="d")

    assert bq.query.call_count == 2
    assert sleep_calls == [0.25]


def test_append_event_calls_bq():
    bq = MagicMock()
    ev = RunEvent(run_id="rid", event_id=str(uuid.uuid4()), stage="ingest",
                  level="info", message="done", created_at=datetime.datetime.now(datetime.timezone.utc))
    append_event(ev, bq, project="p", dataset="d")
    bq.insert_rows_json.assert_called_once()


def test_get_run_returns_none_when_not_found():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    assert get_run("missing", bq, project="p", dataset="d") is None


def test_list_runs_returns_list():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    assert isinstance(list_runs(bq, project="p", dataset="d"), list)


def test_list_runs_coerces_unknown_status_to_failed_for_admin_compatibility():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([
        {
            "run_id": "rid-1",
            "status": "future_unknown_status",
            "triggered_by": "admin",
            "trigger_source": "ui",
            "jobs_path": "data/sample_jobs.json",
            "config_path": ".env.yaml",
            "created_at": datetime.datetime.now(datetime.timezone.utc),
        }
    ])

    runs = list_runs(bq, project="p", dataset="d")

    assert len(runs) == 1
    assert runs[0].status == RunStatus.FAILED


def test_get_events_returns_list():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    assert isinstance(get_events("rid", bq, project="p", dataset="d"), list)


def test_list_cvs_for_run_parameterized():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([
        {"version_id": "v1", "job_url": "http", "fit_classification": "strong", "generated_at": datetime.datetime.now(datetime.timezone.utc)}
    ])
    result = list_cvs_for_run("rid", bq, project="p", dataset="d")
    assert len(result) == 1
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "rid" not in sql_arg, "SQL must use query parameters"


def test_list_cvs_for_run_maps_structured_cv_fields() -> None:
    bq = MagicMock()

    class FakeRow:
        def items(self):
            return [
                ("version_id", "v1"),
                ("job_url", "https://example.com/1"),
                ("fit_classification", "strong"),
                ("generated_at", datetime.datetime.now(datetime.timezone.utc)),
                ("cv_generation_model", "gemini-2.5-pro"),
                ("cv_prompt_version", "cv_prompt_v3"),
                ("cv_schema_version", "cv_doc_v1"),
                ("cv_structured_json", '{"schema_version":"cv_doc_v1","sections":{"summary":{"text":"Grounded summary."}}}'),
            ]

    bq.query.return_value.result.return_value = iter([FakeRow()])
    result = list_cvs_for_run("rid", bq, project="p", dataset="d")
    assert result[0]["cv_generation_model"] == "gemini-2.5-pro"
    assert result[0]["cv_prompt_version"] == "cv_prompt_v3"
    assert result[0]["cv_schema_version"] == "cv_doc_v1"
    assert result[0]["cv_structured"]["sections"]["summary"]["text"] == "Grounded summary."


def test_list_cvs_for_run_falls_back_when_structured_columns_missing() -> None:
    from google.api_core.exceptions import BadRequest

    bq = MagicMock()

    legacy_row = {
        "version_id": "v1",
        "job_url": "https://example.com/1",
        "fit_classification": "strong",
        "generated_at": datetime.datetime.now(datetime.timezone.utc),
    }

    first_query = MagicMock()
    first_query.result.side_effect = BadRequest("Unrecognized name: cv_generation_model")
    second_query = MagicMock()
    second_query.result.return_value = iter([legacy_row])
    bq.query.side_effect = [first_query, second_query]

    result = list_cvs_for_run("rid", bq, project="p", dataset="d")

    assert len(result) == 1
    assert result[0]["version_id"] == "v1"
    assert result[0]["cv_generation_model"] is None
    assert result[0]["cv_prompt_version"] is None
    assert result[0]["cv_schema_version"] is None
    assert result[0]["cv_structured"] is None

def test_get_cv_markdown_returns_string_or_none():
    bq = MagicMock()
    # Test not found
    bq.query.return_value.result.return_value = iter([])
    assert get_cv_markdown("missing", bq, project="p", dataset="d") is None
    
    # Test found
    bq.query.return_value.result.return_value = iter([{"cv_markdown": "my cv"}])
    assert get_cv_markdown("found", bq, project="p", dataset="d") == "my cv"


# ── list_run_structured_jobs ─────────────────────────────────────────────────

def test_list_run_structured_jobs_returns_list():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    result = list_run_structured_jobs("rid", bq, project="p", dataset="d")
    assert isinstance(result, list)


def test_list_run_structured_jobs_uses_parameterized_query():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    list_run_structured_jobs("run-secret-id", bq, project="p", dataset="d")
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "run-secret-id" not in sql_arg, "SQL must use query parameters, not string interpolation"


def test_list_run_structured_jobs_returns_rows_as_dicts():
    bq = MagicMock()

    class FakeRow:
        def items(self):
            return [
                ("run_id", "run-abc"),
                ("job_url", "https://example.com/1"),
                ("title", "Data Engineer"),
                ("location_type", "remote"),
                ("seniority", "senior"),
                ("job_family", "data_engineering"),
                ("domain", "fintech"),
                ("required_skills", ["SQL", "Python"]),
            ]

    bq.query.return_value.result.return_value = iter([FakeRow()])
    result = list_run_structured_jobs("run-abc", bq, project="p", dataset="d")
    assert len(result) == 1
    row = result[0]
    assert isinstance(row, dict)
    assert row["run_id"] == "run-abc"
    assert row["job_url"] == "https://example.com/1"
    assert row["location_type"] == "remote"
    assert row["required_skills"] == ["SQL", "Python"]


def test_list_run_structured_jobs_queries_correct_table():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    list_run_structured_jobs("run-abc", bq, project="myproject", dataset="myds")
    sql_arg = bq.query.call_args[0][0]
    assert "run_structured_jobs" in sql_arg, "SQL must reference run_structured_jobs table"


def test_list_run_structured_jobs_parses_canonical_json_companions():
    bq = MagicMock()

    class FakeRow:
        def items(self):
            return [
                ("run_id", "run-abc"),
                ("job_url", "https://example.com/1"),
                (
                    "required_skill_entities_json",
                    '[{"raw_text":"Python programming for data science","canonical":"python"}]',
                ),
                (
                    "mapping_suggestions_json",
                    '[{"must_have_skill":"google cloud","matches":true,"alias":"gcp","canonical":"google cloud","confidence":1.0}]',
                ),
            ]

    bq.query.return_value.result.return_value = iter([FakeRow()])

    result = list_run_structured_jobs("run-abc", bq, project="p", dataset="d")

    assert result[0]["required_skill_entities"] == [
        {"raw_text": "Python programming for data science", "canonical": "python"}
    ]
    assert result[0]["mapping_suggestions"] == [
        {
            "must_have_skill": "google cloud",
            "matches": True,
            "alias": "gcp",
            "canonical": "google cloud",
            "confidence": 1.0,
        }
    ]


def test_list_filter_results_for_run_parses_marks_json() -> None:
    bq = MagicMock()

    class FakeRow:
        def items(self):
            return [
                ("job_url", "https://example.com/1"),
                ("passed", True),
                ("reasons", []),
                (
                    "marks_json",
                    '[{"code":"must_have_skill_missing","message":"Missing must-have skills","details":{"missing_count":1,"missing_skills":["dbt"]}}]',
                ),
                ("run_id", "run-abc"),
                ("filtered_at", datetime.datetime.now(datetime.timezone.utc)),
            ]

    bq.query.return_value.result.return_value = iter([FakeRow()])

    result = list_filter_results_for_run("run-abc", bq, project="p", dataset="d")

    assert result == [
        {
            "job_url": "https://example.com/1",
            "passed": True,
            "reasons": [],
            "marks_json": '[{"code":"must_have_skill_missing","message":"Missing must-have skills","details":{"missing_count":1,"missing_skills":["dbt"]}}]',
            "marks": [
                {
                    "code": "must_have_skill_missing",
                    "message": "Missing must-have skills",
                    "details": {
                        "missing_count": 1,
                        "missing_skills": ["dbt"],
                    },
                }
            ],
            "run_id": "run-abc",
            "filtered_at": result[0]["filtered_at"],
        }
    ]


def test_list_filter_results_for_run_falls_back_when_marks_json_missing() -> None:
    from google.api_core.exceptions import BadRequest

    bq = MagicMock()

    legacy_row = {
        "job_url": "https://example.com/1",
        "passed": True,
        "reasons": [],
        "run_id": "run-legacy",
        "filtered_at": datetime.datetime.now(datetime.timezone.utc),
    }

    first_query = MagicMock()
    first_query.result.side_effect = BadRequest("Unrecognized name: marks_json")
    second_query = MagicMock()
    second_query.result.return_value = iter([legacy_row])
    bq.query.side_effect = [first_query, second_query]

    result = list_filter_results_for_run("run-legacy", bq, project="p", dataset="d")

    assert result == [
        {
            "job_url": "https://example.com/1",
            "passed": True,
            "reasons": [],
            "run_id": "run-legacy",
            "filtered_at": legacy_row["filtered_at"],
            "marks": [],
        }
    ]


def test_list_run_structured_jobs_preserves_reuse_provenance_fields() -> None:
    bq = MagicMock()

    class FakeRow:
        def items(self):
            return [
                ("run_id", "run-abc"),
                ("job_url", "https://example.com/1"),
                ("raw_job_fingerprint", "raw-123"),
                ("enrich_contract_fingerprint", "contract-123"),
                ("enrich_reuse_status", "reused_cached_enrichment"),
            ]

    bq.query.return_value.result.return_value = iter([FakeRow()])

    result = list_run_structured_jobs("run-abc", bq, project="p", dataset="d")

    assert result[0]["raw_job_fingerprint"] == "raw-123"
    assert result[0]["enrich_contract_fingerprint"] == "contract-123"
    assert result[0]["enrich_reuse_status"] == "reused_cached_enrichment"


# ── Task 1: run-scoped input metadata fields ──────────────────────────────────

def test_insert_run_includes_input_metadata_params() -> None:
    """insert_run sends all 4 new input metadata fields as query parameters."""
    bq = MagicMock()
    run = _make_run()
    run.jobs_input_source = "paste"
    run.jobs_input_json = '[{"title": "DE"}]'
    run.candidate_profile_source = "upload"
    run.candidate_profile_json = '{"skills": []}'
    insert_run(run, bq, project="p", dataset="d")
    call_args = bq.query.call_args
    job_config = call_args[1]["job_config"]
    param_names = {p.name for p in job_config.query_parameters}
    assert "jobs_input_source" in param_names
    assert "jobs_input_json" in param_names
    assert "candidate_profile_source" in param_names
    assert "candidate_profile_json" in param_names


def test_insert_run_includes_manual_checkpoint_params() -> None:
    bq = MagicMock()
    run = _make_run()
    run.run_mode = "manual_staged"
    run.checkpoint_status = "pending_first_stage"
    run.next_stage = "normalize"
    run.last_completed_stage = "enrich"
    run.completed_stages = ["normalize", "enrich"]
    run.checkpoint_payload_json = '{"checkpoint_payload":{"enriched":[]}}'

    insert_run(run, bq, project="p", dataset="d")

    job_config = bq.query.call_args[1]["job_config"]
    params_by_name = {p.name: p for p in job_config.query_parameters}
    assert params_by_name["run_mode"].value == "manual_staged"
    assert params_by_name["checkpoint_status"].value == "pending_first_stage"
    assert params_by_name["next_stage"].value == "normalize"
    assert params_by_name["last_completed_stage"].value == "enrich"
    assert params_by_name["completed_stages_json"].value == '["normalize", "enrich"]'
    assert params_by_name["checkpoint_payload_json"].value == '{"checkpoint_payload":{"enriched":[]}}'


def test_insert_run_input_metadata_none_values_are_included() -> None:
    """insert_run includes None input metadata params (not silently omitted)."""
    bq = MagicMock()
    run = _make_run()  # all 4 new fields default to None
    insert_run(run, bq, project="p", dataset="d")
    call_args = bq.query.call_args
    job_config = call_args[1]["job_config"]
    param_names = {p.name for p in job_config.query_parameters}
    assert "jobs_input_source" in param_names
    assert "candidate_profile_json" in param_names
    # verify value is None (not missing)
    params_by_name = {p.name: p for p in job_config.query_parameters}
    assert params_by_name["jobs_input_source"].value is None


def test_row_to_run_maps_input_metadata_fields() -> None:
    """_row_to_run correctly maps all 4 new fields from a BQ row."""
    from fitcv_cp.bq_store import _row_to_run
    import datetime
    row = {
        "run_id": "r1",
        "status": "queued",
        "triggered_by": "admin",
        "trigger_source": "ui",
        "jobs_path": "data/jobs.json",
        "config_path": ".env.yaml",
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "jobs_input_source": "paste",
        "jobs_input_json": '[{"title": "Analyst"}]',
        "candidate_profile_source": "upload",
        "candidate_profile_json": '{"skills": []}',
    }
    result = _row_to_run(row)
    assert result.jobs_input_source == "paste"
    assert result.jobs_input_json == '[{"title": "Analyst"}]'
    assert result.candidate_profile_source == "upload"
    assert result.candidate_profile_json == '{"skills": []}'


def test_row_to_run_maps_manual_checkpoint_fields() -> None:
    from fitcv_cp.bq_store import _row_to_run
    row = {
        "run_id": "r-manual",
        "status": "awaiting_continue",
        "triggered_by": "admin",
        "trigger_source": "ui",
        "jobs_path": "data/jobs.json",
        "config_path": ".env.yaml",
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "run_mode": "manual_staged",
        "checkpoint_status": "awaiting_continue",
        "next_stage": "ranking",
        "last_completed_stage": "shortlist",
        "completed_stages_json": '["normalize", "enrich", "rule_filter", "shortlist"]',
        "checkpoint_payload_json": '{"checkpoint_payload":{"shortlist":[]}}',
    }
    result = _row_to_run(row)
    assert result.run_mode == "manual_staged"
    assert result.checkpoint_status == "awaiting_continue"
    assert result.next_stage == "ranking"
    assert result.last_completed_stage == "shortlist"
    assert result.completed_stages == ["normalize", "enrich", "rule_filter", "shortlist"]
    assert result.checkpoint_payload_json == '{"checkpoint_payload":{"shortlist":[]}}'


def test_row_to_run_handles_missing_input_metadata_fields() -> None:
    """_row_to_run returns None for new fields absent from old BQ rows."""
    from fitcv_cp.bq_store import _row_to_run
    import datetime
    row = {
        "run_id": "r2",
        "status": "succeeded",
        "jobs_path": "data/jobs.json",
        "config_path": ".env.yaml",
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        # no input metadata fields — simulates old row
    }
    result = _row_to_run(row)
    assert result.jobs_input_source is None
    assert result.jobs_input_json is None
    assert result.candidate_profile_source is None
    assert result.candidate_profile_json is None
    assert result.results_export_json is None


def test_row_to_run_maps_cv_generation_debug_json() -> None:
    """_row_to_run maps the run-scoped CV generation debug snapshot field."""
    from fitcv_cp.bq_store import _row_to_run
    row = {
        "run_id": "r3",
        "status": "succeeded",
        "jobs_path": "data/jobs.json",
        "config_path": ".env.yaml",
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "cv_generation_debug_json": '{"run_id":"r3","debug_records":[]}',
    }
    result = _row_to_run(row)
    assert result.cv_generation_debug_json == '{"run_id":"r3","debug_records":[]}'


def test_row_to_run_maps_stage_transition_artifacts_json() -> None:
    """_row_to_run maps the run-scoped stage transition artifacts snapshot field."""
    from fitcv_cp.bq_store import _row_to_run
    row = {
        "run_id": "r4",
        "status": "succeeded",
        "jobs_path": "data/jobs.json",
        "config_path": ".env.yaml",
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "stage_transition_artifacts_json": '{"run_id":"r4","stages":{}}',
    }
    result = _row_to_run(row)
    assert result.stage_transition_artifacts_json == '{"run_id":"r4","stages":{}}'


def test_row_to_run_maps_settings_used_json() -> None:
    """_row_to_run maps the run-scoped settings-used snapshot field."""
    from fitcv_cp.bq_store import _row_to_run
    row = {
        "run_id": "r5",
        "status": "succeeded",
        "jobs_path": "data/jobs.json",
        "config_path": ".env.yaml",
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "settings_used_json": '{"run_id":"r5","effective_settings":{"pipeline":{"final_top_n":10}}}',
    }
    result = _row_to_run(row)
    assert result.settings_used_json == '{"run_id":"r5","effective_settings":{"pipeline":{"final_top_n":10}}}'


def test_update_run_cv_generation_debug_updates_only_debug_snapshot_field() -> None:
    """Dedicated helper updates cv_generation_debug_json without reusing results_export_json."""
    bq = MagicMock()
    update_run_cv_generation_debug(
        "rid",
        '{"run_id":"rid","debug_records":[]}',
        bq,
        project="p",
        dataset="d",
    )
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "cv_generation_debug_json" in sql_arg
    assert "results_export_json" not in sql_arg

    job_config = bq.query.call_args[1]["job_config"]
    param_names = {p.name for p in job_config.query_parameters}
    assert param_names == {"cv_generation_debug_json", "run_id"}


def test_update_run_stage_transition_artifacts_updates_only_stage_artifacts_field() -> None:
    """Dedicated helper updates stage_transition_artifacts_json without reusing other snapshot fields."""
    bq = MagicMock()
    update_run_stage_transition_artifacts(
        "rid",
        '{"run_id":"rid","stages":{}}',
        bq,
        project="p",
        dataset="d",
    )
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "stage_transition_artifacts_json" in sql_arg
    assert "results_export_json" not in sql_arg
    assert "cv_generation_debug_json" not in sql_arg

    job_config = bq.query.call_args[1]["job_config"]
    param_names = {p.name for p in job_config.query_parameters}
    assert param_names == {"stage_transition_artifacts_json", "run_id"}


def test_update_run_settings_used_updates_only_settings_snapshot_field() -> None:
    """Dedicated helper updates settings_used_json without touching other snapshot fields."""
    bq = MagicMock()
    update_run_settings_used(
        "rid",
        '{"run_id":"rid","effective_settings":{"pipeline":{"final_top_n":10}}}',
        bq,
        project="p",
        dataset="d",
    )
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "settings_used_json" in sql_arg
    assert "results_export_json" not in sql_arg
    assert "cv_generation_debug_json" not in sql_arg
    assert "stage_transition_artifacts_json" not in sql_arg

    job_config = bq.query.call_args[1]["job_config"]
    param_names = {p.name for p in job_config.query_parameters}
    assert param_names == {"settings_used_json", "run_id"}


def test_update_run_mapping_suggestions_updates_only_mapping_snapshot_field() -> None:
    bq = MagicMock()
    update_run_mapping_suggestions(
        "run-123",
        '{"run_id":"run-123","suggestions":[]}',
        bq,
        project="p",
        dataset="d",
    )

    sql_arg = bq.query.call_args[0][0]
    assert "mapping_suggestions_json" in sql_arg
    assert "results_export_json" not in sql_arg
    assert "stage_transition_artifacts_json" not in sql_arg

    job_config = bq.query.call_args[1]["job_config"]
    param_names = {p.name for p in job_config.query_parameters}
    assert param_names == {"mapping_suggestions_json", "run_id"}


def test_update_run_effective_settings_updates_only_effective_settings_field() -> None:
    bq = MagicMock()
    update_run_effective_settings(
        "run-123",
        '{"skill_synonyms":{"gcp":"google cloud"}}',
        bq,
        project="p",
        dataset="d",
    )

    sql_arg = bq.query.call_args[0][0]
    assert "effective_settings_json" in sql_arg
    assert "mapping_suggestions_json" not in sql_arg
    assert "checkpoint_payload_json" not in sql_arg

    job_config = bq.query.call_args[1]["job_config"]
    param_names = {p.name for p in job_config.query_parameters}
    assert param_names == {"effective_settings_json", "run_id"}


# ── Lifecycle fields ────────────────────────────────────────────────────────

def test_row_to_run_maps_lifecycle_fields():
    from fitcv_cp.bq_store import _row_to_run
    row = {
        "run_id": "r1",
        "status": "queued",
        "triggered_by": "admin",
        "trigger_source": "web",
        "jobs_path": "data/jobs.json",
        "config_path": ".env.yaml",
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "queue_job_id": "rq-job-1",
        "cancel_requested_at": None,
        "cancel_requested_by": None,
        "archived_at": None,
        "archived_by": None,
    }
    run = _row_to_run(row)
    assert run.queue_job_id == "rq-job-1"
    assert run.cancel_requested_at is None
    assert run.archived_at is None


def test_insert_run_includes_queue_job_id():
    from fitcv_cp.bq_store import insert_run
    bq = MagicMock()
    run = _make_run()
    run.queue_job_id = "rq-job-abc"
    insert_run(run, bq, project="p", dataset="d")
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "queue_job_id" in sql_arg


def test_update_run_results_export_uses_parameterized_query() -> None:
    bq = MagicMock()
    update_run_results_export("rid", '{"results":[]}', bq, project="p", dataset="d")
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "rid" not in sql_arg
    assert '"results":[]' not in sql_arg


def test_row_to_run_maps_results_export_json() -> None:
    from fitcv_cp.bq_store import _row_to_run

    row = {
        "run_id": "r3",
        "status": "succeeded",
        "jobs_path": "data/jobs.json",
        "config_path": ".env.yaml",
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "results_export_json": '{"run_id":"r3","results":[]}',
    }
    result = _row_to_run(row)
    assert result.results_export_json == '{"run_id":"r3","results":[]}'


# ──  Lifecycle update helpers ───────────────────────────────────────────────

def test_update_run_queue_job_id_uses_parameterized_query():
    from fitcv_cp.bq_store import update_run_queue_job_id
    bq = MagicMock()
    update_run_queue_job_id("rid", "rq-job-1", bq, project="p", dataset="d")
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "rid" not in sql_arg
    assert "rq-job-1" not in sql_arg


def test_update_run_checkpoint_uses_parameterized_query() -> None:
    bq = MagicMock()
    update_run_checkpoint(
        "rid",
        bq,
        project="p",
        dataset="d",
        checkpoint_status="awaiting_continue",
        next_stage="ranking",
        last_completed_stage="shortlist",
        completed_stages=["normalize", "enrich", "rule_filter", "shortlist"],
        checkpoint_payload_json='{"checkpoint_payload":{"shortlist":[]}}',
    )
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "rid" not in sql_arg
    job_config = bq.query.call_args[1]["job_config"]
    param_names = {p.name for p in job_config.query_parameters}
    assert param_names == {
        "checkpoint_status",
        "next_stage",
        "last_completed_stage",
        "completed_stages_json",
        "checkpoint_payload_json",
        "run_id",
    }


def test_request_run_cancel_sets_cancel_fields():
    from fitcv_cp.bq_store import request_run_cancel
    bq = MagicMock()
    request_run_cancel("rid", "admin", "cancelling", bq, project="p", dataset="d")
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "rid" not in sql_arg


def test_archive_run_uses_parameterized_query():
    from fitcv_cp.bq_store import archive_run
    bq = MagicMock()
    archive_run("rid", "admin", bq, project="p", dataset="d")
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "rid" not in sql_arg


def test_unarchive_run_uses_parameterized_query():
    from fitcv_cp.bq_store import unarchive_run
    bq = MagicMock()
    unarchive_run("rid", bq, project="p", dataset="d")
    bq.query.assert_called_once()
    sql_arg = bq.query.call_args[0][0]
    assert "rid" not in sql_arg


# ──  list_runs archive filter ───────────────────────────────────────────────

def test_list_runs_active_filters_archived():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    list_runs(bq, project="p", dataset="d", include_archived=False)
    sql_arg = bq.query.call_args[0][0]
    assert "archived_at IS NULL" in sql_arg


def test_list_runs_archived_only():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    list_runs(bq, project="p", dataset="d", archived_only=True)
    sql_arg = bq.query.call_args[0][0]
    assert "archived_at IS NOT NULL" in sql_arg


def test_list_runs_include_all():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    list_runs(bq, project="p", dataset="d", include_archived=True)
    sql_arg = bq.query.call_args[0][0]
    assert "archived_at" not in sql_arg
