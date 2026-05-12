import importlib.util
import json
from pathlib import Path


def _load_checker_module():
    script_path = Path(__file__).resolve().parent.parent / "scripts" / "check_outbox_replay_health.py"
    spec = importlib.util.spec_from_file_location("check_outbox_replay_health", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_checker_uses_config_default_threshold_when_cli_not_set(monkeypatch, capsys):
    module = _load_checker_module()
    captured = {}

    class _FakeResponse:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"decision": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, _endpoint, params=None):
            captured["params"] = dict(params or {})
            return _FakeResponse()

    monkeypatch.setattr(module, "load_config", lambda _path: {"outbox_replay_health": {"min_replay_success_ratio": 0.77}})
    monkeypatch.setattr(module.httpx, "Client", _FakeClient)
    monkeypatch.setattr(
        "sys.argv",
        ["check_outbox_replay_health.py", "--config-path", "config/env.yaml"],
    )

    exit_code = module.main()
    assert exit_code == 0
    assert captured["params"]["min_replay_success_ratio"] == 0.77
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["decision"] == "ok"


def test_checker_cli_override_wins_over_config_default(monkeypatch):
    module = _load_checker_module()
    captured = {}

    class _FakeResponse:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"decision": "ok"}

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, _endpoint, params=None):
            captured["params"] = dict(params or {})
            return _FakeResponse()

    monkeypatch.setattr(module, "load_config", lambda _path: {"outbox_replay_health": {"min_replay_success_ratio": 0.77}})
    monkeypatch.setattr(module.httpx, "Client", _FakeClient)
    monkeypatch.setattr(
        "sys.argv",
        [
            "check_outbox_replay_health.py",
            "--config-path",
            "config/env.yaml",
            "--min-replay-success-ratio",
            "0.88",
        ],
    )

    exit_code = module.main()
    assert exit_code == 0
    assert captured["params"]["min_replay_success_ratio"] == 0.88

