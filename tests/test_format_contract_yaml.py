"""
@meta
type: test
scope: unit
domain: docs
covers:
  - YAML contract formatting normalization for docs/features and docs/stages
  - Check mode vs rewrite mode behavior for the contract YAML formatter
excludes:
  - Full-repo end-to-end normalization runs
tags:
  - fast
  - ci-safe
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import uuid
from shutil import rmtree
from pathlib import Path
from types import ModuleType

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
FORMATTER = REPO_ROOT / "scripts" / "format_contract_yaml.py"

SAMPLE_YAML = """feature_id: sample-feature
name: Sample feature
status: active
type: workflow
summary: Example contract
invariants:
  - "Training behavior is controlled by `configs/train.yaml` and the AML train component."
  - Production retraining remains distinct from the optimization workflow.
capabilities:
  - "Apply experiment and display metadata from `configs/train.yaml`."
  - Accept an explicit training config path so smoke runs can use `configs/train_smoke.yaml` without changing production defaults.
"""


def run_formatter(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(FORMATTER), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def load_formatter_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("format_contract_yaml", FORMATTER)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load formatter from {FORMATTER}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_test_root() -> Path:
    root = REPO_ROOT / ".tmp-tests" / f"format-contract-yaml-{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=False)
    return root


def test_check_mode_detects_drift_without_rewriting() -> None:
    test_root = make_test_root()
    try:
        target = test_root / "docs" / "features" / "sample" / "sample.yaml"
        target.parent.mkdir(parents=True)
        target.write_text(SAMPLE_YAML, encoding="utf-8")

        result = run_formatter("--check", str(target))

        assert result.returncode == 1
        assert target.read_text(encoding="utf-8") == SAMPLE_YAML
        assert "would reformat" in result.stdout.lower()
    finally:
        rmtree(test_root, ignore_errors=True)


def test_rewrite_mode_normalizes_optional_quotes_and_is_idempotent() -> None:
    test_root = make_test_root()
    try:
        target = test_root / "docs" / "stages" / "sample.yaml"
        target.parent.mkdir(parents=True)
        target.write_text(SAMPLE_YAML, encoding="utf-8")

        first = run_formatter(str(target))
        assert first.returncode == 0

        normalized_once = target.read_text(encoding="utf-8")
        assert (
            '"Training behavior is controlled by `configs/train.yaml` and the AML train component."'
            not in normalized_once
        )
        assert (
            '"Apply experiment and display metadata from `configs/train.yaml`."'
            not in normalized_once
        )

        second = run_formatter(str(target))
        assert second.returncode == 0
        assert target.read_text(encoding="utf-8") == normalized_once
    finally:
        rmtree(test_root, ignore_errors=True)


def test_check_mode_passes_after_normalization() -> None:
    test_root = make_test_root()
    try:
        target = test_root / "docs" / "features" / "sample" / "sample.yaml"
        target.parent.mkdir(parents=True)
        target.write_text(SAMPLE_YAML, encoding="utf-8")

        rewrite = run_formatter(str(target))
        assert rewrite.returncode == 0

        check = run_formatter("--check", str(target))
        assert check.returncode == 0
        assert "already normalized" in check.stdout.lower()
    finally:
        rmtree(test_root, ignore_errors=True)


def test_check_mode_allows_repo_without_contract_yaml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_root = make_test_root()
    try:
        (test_root / "docs" / "features").mkdir(parents=True)
        (test_root / "docs" / "stages").mkdir(parents=True)

        formatter = load_formatter_module()
        monkeypatch.setattr(formatter, "repo_root", lambda: test_root)
        monkeypatch.setattr(sys, "argv", ["format_contract_yaml.py", "--check"])

        assert formatter.main() == 0
    finally:
        rmtree(test_root, ignore_errors=True)


def test_formatter_skips_generated_contracts() -> None:
    test_root = make_test_root()
    try:
        target = test_root / "docs" / "features" / "sample" / "sample.yaml"
        target.parent.mkdir(parents=True)
        generated_text = "# GENERATED FILE - do not edit directly.\n" + SAMPLE_YAML
        target.write_text(generated_text, encoding="utf-8")

        result = run_formatter("--check", str(target))

        assert result.returncode == 0
        assert target.read_text(encoding="utf-8") == generated_text
        assert "skipped generated" in result.stdout.lower()
    finally:
        rmtree(test_root, ignore_errors=True)


def test_formatter_skips_generated_feature_local_lineage_in_check_and_rewrite_modes(
) -> None:
    test_root = make_test_root()
    try:
        target = test_root / "docs" / "features" / "sample" / "lineage.generated.yaml"
        target.parent.mkdir(parents=True)
        generated_text = "# GENERATED FILE - do not edit directly.\n" + SAMPLE_YAML
        target.write_text(generated_text, encoding="utf-8")

        check = run_formatter("--check", str(target))
        rewrite = run_formatter(str(target))

        assert check.returncode == 0
        assert rewrite.returncode == 0
        assert target.read_text(encoding="utf-8") == generated_text
        assert "skipped generated" in check.stdout.lower()
        assert "skipped generated" in rewrite.stdout.lower()
    finally:
        rmtree(test_root, ignore_errors=True)
