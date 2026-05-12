"""
@meta
type: test
scope: unit
domain: observability
covers:
  - control-plane routing/backend observability emissions
excludes:
  - otel exporter plumbing
tags:
  - fast
  - ci-safe
"""

from __future__ import annotations

from fitcv_cp.orchestrator import RunSubmission
import fitcv_cp.app as app


def test_submit_run_emits_backend_and_routing_diagnostics(monkeypatch):
    emitted = []

    class _StubAdapter:
        name = "prefect"

        def submit(self, **kwargs):
            return RunSubmission(
                run_id="run-obs-1",
                queue_job_id="job-obs-1",
                backend_run_id="backend-obs-1",
                backend="prefect",
            )

    monkeypatch.setattr(app, "ORCHESTRATION_ADAPTER", _StubAdapter())
    monkeypatch.setattr(app, "emit_observability_event", lambda event, payload: emitted.append((event, payload)))
    monkeypatch.setattr(
        app,
        "load_control_plane_config",
        lambda: {
            "observability": {
                "emit_model_routing_diagnostics": True,
                "emit_backend_capability_diagnostics": True,
            }
        },
    )

    app.submit_run(jobs_path="a", config_path="b", triggered_by="admin")

    names = [row[0] for row in emitted]
    assert "control_plane.backend_execution" in names
    assert "control_plane.model_routing" in names
    backend_payload = dict([row for row in emitted if row[0] == "control_plane.backend_execution"][0][1])
    assert backend_payload["run_id"] == "run-obs-1"
    assert backend_payload["backend"] == "prefect"
    assert backend_payload["queue_job_id"] == "job-obs-1"


def test_resolve_submission_binding_emits_fallback_backend_event(monkeypatch):
    emitted = []
    monkeypatch.setattr(app, "emit_observability_event", lambda event, payload: emitted.append((event, payload)))
    monkeypatch.setattr(
        app,
        "load_control_plane_config",
        lambda: {
            "observability": {
                "emit_model_routing_diagnostics": False,
                "emit_backend_capability_diagnostics": True,
            }
        },
    )

    app._RUN_SUBMISSION_CACHE.clear()
    app._resolve_submission_binding("run-obs-2", "job-obs-2")

    assert emitted
    assert emitted[0][0] == "control_plane.backend_fallback_binding"
    assert emitted[0][1]["run_id"] == "run-obs-2"


def test_submit_run_respects_disabled_observability_toggles(monkeypatch):
    emitted = []

    class _StubAdapter:
        name = "default_queue"

        def submit(self, **kwargs):
            return RunSubmission(
                run_id="run-obs-3",
                queue_job_id="job-obs-3",
                backend_run_id="backend-obs-3",
                backend="queue",
            )

    monkeypatch.setattr(app, "ORCHESTRATION_ADAPTER", _StubAdapter())
    monkeypatch.setattr(app, "emit_observability_event", lambda event, payload: emitted.append((event, payload)))
    monkeypatch.setattr(
        app,
        "load_control_plane_config",
        lambda: {
            "observability": {
                "emit_model_routing_diagnostics": False,
                "emit_backend_capability_diagnostics": False,
            }
        },
    )

    app.submit_run(jobs_path="a", config_path="b", triggered_by="admin")

    assert emitted == []
