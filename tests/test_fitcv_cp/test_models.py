"""
@meta
type: test
scope: unit
domain: admin_ui
covers:
  - control-plane model behavior
excludes:
  - persistence integration
tags:
  - fast
  - ci-safe
"""

import dataclasses

from fitcv_cp.models import RunStatus, EventLevel, PipelineRun, RunEvent


def test_run_status_values():
    """@proves admin_control_plane_core.pipeline-runs-bigquery-table"""
    assert set(RunStatus) == {
        RunStatus.QUEUED,
        RunStatus.RUNNING,
        RunStatus.AWAITING_CONTINUE,
        RunStatus.CANCELLING,
        RunStatus.CANCELLED,
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
    }


def test_run_status_cancelling_value():
    assert RunStatus.CANCELLING.value == "cancelling"


def test_run_status_cancelled_value():
    assert RunStatus.CANCELLED.value == "cancelled"


def test_event_level_values():
    assert set(EventLevel) == {EventLevel.INFO, EventLevel.WARNING, EventLevel.ERROR}


def test_pipeline_run_fields():
    """@proves multi_file_job_input.one-immutable-snapshot-stored-per-run"""
    fields = {f.name for f in dataclasses.fields(PipelineRun)}
    assert {
        "run_id", "status", "triggered_by", "trigger_source", "jobs_path",
        "config_path", "created_at", "error_stage",
    } <= fields


def test_pipeline_run_lifecycle_fields():
    """@proves admin_control_plane_core.pipeline-runs-bigquery-table"""
    fields = {f.name for f in dataclasses.fields(PipelineRun)}
    assert {
        "queue_job_id",
        "cancel_requested_at",
        "cancel_requested_by",
        "archived_at",
        "archived_by",
    } <= fields


def test_pipeline_run_lifecycle_fields_default_none():
    """@proves admin_control_plane_core.pipeline-runs-bigquery-table"""
    import datetime
    run = PipelineRun(
        run_id="r1",
        status=RunStatus.QUEUED,
        triggered_by="admin",
        trigger_source="web",
        jobs_path="data/jobs.json",
        config_path=".env.yaml",
        created_at=datetime.datetime.now(datetime.timezone.utc),
    )
    assert run.queue_job_id is None
    assert run.cancel_requested_at is None
    assert run.cancel_requested_by is None
    assert run.archived_at is None
    assert run.archived_by is None


def test_run_event_fields():
    """@proves admin_control_plane_core.pipeline-run-events-bigquery-table"""
    fields = {f.name for f in dataclasses.fields(RunEvent)}
    assert {"run_id", "event_id", "stage", "level", "message", "created_at"} <= fields
