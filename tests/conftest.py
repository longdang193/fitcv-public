"""Shared pytest fixtures for fitcv tests."""

import multiprocessing

# Python 3.13 on Windows does not support the 'fork' multiprocessing start method.
# rq.scheduler unconditionally calls get_context('fork') at import time, which
# raises ValueError.  Patch it here before the test module imports anything from
# fitcv_cp (which transitively imports rq).
_original_get_context = multiprocessing.get_context


def _patched_get_context(method):
    if method == "fork":
        method = "spawn"
    return _original_get_context(method)


multiprocessing.get_context = _patched_get_context

import os
from pathlib import Path

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: mark test as requiring live GCP credentials (skipped by default)",
    )


@pytest.fixture(autouse=True)
def skip_integration_without_creds(request: pytest.FixtureRequest) -> None:
    """Auto-skip any @pytest.mark.integration test when credentials are absent."""
    if request.node.get_closest_marker("integration"):
        if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            pytest.skip("Set GOOGLE_APPLICATION_CREDENTIALS to run integration tests")


@pytest.fixture
def sample_jobs_path() -> Path:
    """Absolute path to the sample jobs JSON fixture."""
    return Path(__file__).parent.parent / "data" / "sample_jobs.json"


@pytest.fixture
def sample_profile_path() -> Path:
    """Absolute path to the candidate profile YAML fixture."""
    return Path(__file__).parent.parent / "data" / "candidate_profile.yaml"


@pytest.fixture
def config() -> dict[str, object]:
    """Loaded project config from .env.yaml."""
    from fitcv.config import load_config
    return load_config(Path(__file__).parent.parent / ".env.yaml")
