from unittest.mock import MagicMock
from fitcv_cp.reporter import PipelineReporter


def test_reporter_emits_event():
    bq = MagicMock()
    reporter = PipelineReporter(run_id="r1", bq=bq, project="p", dataset="d")
    reporter.emit("pipeline_start", "info", "Run started")
    bq.insert_rows_json.assert_called_once()


def test_reporter_noop_without_bq():
    reporter = PipelineReporter(run_id="r1", bq=None, project="p", dataset="d")
    reporter.emit("pipeline_start", "info", "ok")  # must not raise


def test_reporter_payload_serialized():
    bq = MagicMock()
    reporter = PipelineReporter(run_id="r1", bq=bq, project="p", dataset="d")
    reporter.emit("layer3_filter", "error", "timeout", payload={"retries": 3})
    call_args = bq.insert_rows_json.call_args[0][1][0]
    assert call_args["level"] == "error"
    assert "retries" in call_args["payload_json"]
