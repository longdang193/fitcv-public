"""
@meta
name: test_setup_hooks
type: test
scope: unit
domain: docs
covers:
  - Local hook setup scripts install the canonical repo-contract validator entrypoint
  - Architecture sync remains the narrower generated-doc refresh path
tags:
  - fast
  - ci-safe
lifecycle:
  status: active
"""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
HOOK_SCRIPT_PATHS = [
    REPO_ROOT / "scripts" / "setup_hooks.ps1",
    REPO_ROOT / "scripts" / "setup_hooks.sh",
]
SYNC_SCRIPT_PATH = REPO_ROOT / "scripts" / "sync_architecture_docs.py"


def test_hook_setup_scripts_install_architecture_metadata_checks() -> None:
    for script_path in HOOK_SCRIPT_PATHS:
        if not script_path.exists():
            continue
        script_text = script_path.read_text(encoding="utf-8")

        assert "validate_repo_contracts.py --fast" in script_text
        assert ".venv" in script_text


def test_architecture_sync_script_runs_generator_formatter_and_focused_tests() -> None:
    script_text = SYNC_SCRIPT_PATH.read_text(encoding="utf-8")

    assert "validate_adoption_shape.py" in script_text
    assert "generate_architecture_metadata.py" in script_text
    assert "audit_architecture_linkage.py" in script_text
    assert '"--strict-awareness"' in script_text
    assert '"--report-awareness"' in script_text
    assert '"--validate-only"' in script_text
    assert '"--check"' in script_text
    assert "format_contract_yaml.py" in script_text
    assert "tests/test_architecture_metadata_generation.py" in script_text
    assert "tests/test_architecture_linkage_audit.py" in script_text
    assert "tests/test_format_contract_yaml.py" in script_text
    assert "tests/test_validate_adoption_shape.py" in script_text
    assert "tests/test_setup_hooks.py" in script_text
