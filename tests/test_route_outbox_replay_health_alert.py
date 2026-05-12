import importlib.util
import json
from pathlib import Path
from subprocess import CompletedProcess


def _load_wrapper_module():
    script_path = Path(__file__).resolve().parent.parent / "scripts" / "route_outbox_replay_health_alert.py"
    spec = importlib.util.spec_from_file_location("route_outbox_replay_health_alert", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_wrapper_routes_alert_to_webhook_and_preserves_alert_exit(monkeypatch, capsys):
    module = _load_wrapper_module()

    checker_payload = {
        "decision": "alert",
        "reason_code": "dead_letter_status_degraded",
        "outbox_replay_health": {"status": "degraded"},
    }

    def _fake_run(*_args, **_kwargs):
        return CompletedProcess(
            args=["python", "check_outbox_replay_health.py"],
            returncode=2,
            stdout=json.dumps(checker_payload),
            stderr="",
        )

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            self.sent = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, _url, json=None):
            assert isinstance(json, dict)

            class _Resp:
                @staticmethod
                def raise_for_status():
                    return None

            return _Resp()

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    monkeypatch.setattr(module.httpx, "Client", _FakeClient)
    monkeypatch.setattr(
        "sys.argv",
        [
            "route_outbox_replay_health_alert.py",
            "--webhook-url",
            "https://example.local/alert",
        ],
    )

    exit_code = module.main()
    assert exit_code == 2
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert payload["checker_exit_code"] == 2
    assert payload["final_exit_code"] == 2
    assert payload["webhook_status"] == "sent"
    assert payload["payload"]["decision"] == "alert"


def test_wrapper_healthy_without_notify_on_ok_skips_webhook(monkeypatch, capsys):
    module = _load_wrapper_module()

    checker_payload = {
        "decision": "ok",
        "reason_code": "healthy",
        "outbox_replay_health": {"status": "healthy"},
    }

    def _fake_run(*_args, **_kwargs):
        return CompletedProcess(
            args=["python", "check_outbox_replay_health.py"],
            returncode=0,
            stdout=json.dumps(checker_payload),
            stderr="",
        )

    class _FailClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("Webhook client should not be created for healthy check without --notify-on-ok")

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    monkeypatch.setattr(module.httpx, "Client", _FailClient)
    monkeypatch.setattr("sys.argv", ["route_outbox_replay_health_alert.py"])

    exit_code = module.main()
    assert exit_code == 0
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert payload["checker_exit_code"] == 0
    assert payload["final_exit_code"] == 0
    assert payload["webhook_status"] == "skipped"
    assert payload["payload"]["decision"] == "ok"


def test_wrapper_passes_config_path_and_optional_threshold_override(monkeypatch):
    module = _load_wrapper_module()
    captured = {}

    def _fake_run(cmd, *_args, **_kwargs):
        captured["cmd"] = list(cmd)
        return CompletedProcess(
            args=cmd,
            returncode=0,
            stdout=json.dumps({"decision": "ok"}),
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    monkeypatch.setattr(
        "sys.argv",
        [
            "route_outbox_replay_health_alert.py",
            "--config-path",
            "config/env.yaml",
            "--min-replay-success-ratio",
            "0.91",
        ],
    )
    exit_code = module.main()
    assert exit_code == 0
    cmd = captured["cmd"]
    assert "--config-path" in cmd
    assert "config/env.yaml" in cmd
    assert "--min-replay-success-ratio" in cmd
    assert "0.91" in cmd
