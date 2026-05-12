"""
@meta
type: test
scope: unit
domain: control_plane_config
covers:
  - control-plane runtime config loading and validation
excludes:
  - provider API calls
  - backend network connectivity
tags:
  - fast
  - ci-safe
"""

from pathlib import Path

import pytest

from fitcv.config import load_control_plane_config


def test_load_control_plane_config_defaults_from_runtime_yaml() -> None:
    cfg = load_control_plane_config()

    assert cfg["data_backend"]["type"] == "sqlite"
    assert "providers" in cfg
    assert "model_routing" in cfg
    assert "parts" in cfg["model_routing"]
    assert "observability" in cfg


def test_load_control_plane_config_env_override_backend_type(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "sqlite")

    cfg = load_control_plane_config()

    assert cfg["data_backend"]["type"] == "sqlite"


def test_load_control_plane_config_rejects_invalid_backend_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FITCV_CP_DATA_BACKEND", raising=False)
    config_path = tmp_path / "control_plane.yaml"
    config_path.write_text(
        "control_plane:\n"
        "  data_backend:\n"
        "    type: invalid_backend\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="data_backend.type"):
        load_control_plane_config(config_path)


def test_load_control_plane_config_rejects_secret_or_env_key_names(tmp_path: Path) -> None:
    config_path = tmp_path / "control_plane.yaml"
    config_path.write_text(
        "control_plane:\n"
        "  providers:\n"
        "    openai:\n"
        "      api_key_env: OPENAI_API_KEY\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="forbidden secret-oriented key names"):
        load_control_plane_config(config_path)
