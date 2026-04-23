"""
@meta
type: test
scope: unit
domain: admin_ui
covers:
  - control-plane reporting behavior
excludes:
  - live BigQuery or queue integrations
tags:
  - fast
  - ci-safe
"""

from unittest.mock import MagicMock
from fitcv_cp.reporter import PipelineReporter


def test_reporter_emits_event():
    """@proves admin_control_plane_core.pipelinereporter-integration
    @proves run_lifecycle_controls.full-audit-trail-in-pipeline-run-events
    """
    bq = MagicMock()
    reporter = PipelineReporter(run_id="r1", bq=bq, project="p", dataset="d")
    reporter.emit("pipeline_start", "info", "Run started")
    bq.insert_rows_json.assert_called_once()


def test_reporter_noop_without_bq():
    """@proves admin_control_plane_core.pipelinereporter-integration"""
    reporter = PipelineReporter(run_id="r1", bq=None, project="p", dataset="d")
    reporter.emit("pipeline_start", "info", "ok")  # must not raise


def test_reporter_payload_serialized():
    """@proves admin_control_plane_core.pipelinereporter-integration"""
    bq = MagicMock()
    reporter = PipelineReporter(run_id="r1", bq=bq, project="p", dataset="d")
    reporter.emit("layer3_filter", "error", "timeout", payload={"retries": 3})
    call_args = bq.insert_rows_json.call_args[0][1][0]
    assert call_args["level"] == "error"
    assert "retries" in call_args["payload_json"]
