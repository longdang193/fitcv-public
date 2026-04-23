"""
@meta
name: test_architecture_linkage_audit
type: test
scope: unit
domain: docs
covers:
  - Feature-source manual_refs rejection in architecture linkage audit
  - Strict and report-only audit behavior
excludes:
  - Full repository-wide architecture sync execution
tags:
  - fast
  - ci-safe
lifecycle:
  status: active
"""

from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path
from shutil import rmtree


REPO_ROOT = Path(__file__).resolve().parent.parent
AUDIT_SCRIPT = REPO_ROOT / "scripts" / "audit_architecture_linkage.py"


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def make_test_root() -> Path:
    root = REPO_ROOT / ".tmp-tests" / f"architecture-linkage-audit-{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=False)
    return root


def run_audit(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(AUDIT_SCRIPT), "--repo-root", str(root), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def seed_repo(root: Path, *, include_manual_refs: bool) -> None:
    manual_refs = ""
    if include_manual_refs:
        manual_refs = """manual_refs:
  code:
    - src/sample_helper.py
"""
    write_text(
        root / "docs" / "features" / "sample-feature" / "feature.source.yaml",
        f"""feature_id: sample-feature
name: Sample feature
status: active
type: workflow
summary: Sample feature summary.
domains:
  - sample
depends_on: []
invariants:
  - invariant_id: sample.must-exist
    statement: Sample invariant.
    state: active
capabilities:
  - capability_id: sample-feature.helper-capability
    statement: Sample helper capability.
    state: active
{manual_refs}""",
    )
    write_text(root / "docs" / "features" / "sample-feature" / "history.md", "# History\n")
    write_text(
        root / "src" / "sample_helper.py",
        '''"""
@meta
name: sample_helper
type: utility
domain: sample
responsibility:
  - Provide sample helper behavior.
features:
  - sample-feature
capabilities:
  - sample-feature.helper-capability
inputs: []
outputs: []
tags:
  - sample
lifecycle:
  status: active
"""
''',
    )
    write_text(
        root / "tests" / "test_sample_helper.py",
        '''"""
@meta
type: test
scope: unit
domain: sample
"""


def test_sample_helper() -> None:
    """
    @proves sample-feature.helper-capability
    """
''',
    )


def test_linkage_audit_fails_when_manual_refs_reappear() -> None:
    test_root = make_test_root()
    try:
        seed_repo(test_root, include_manual_refs=True)
        result = run_audit(test_root, "--strict-awareness")

        assert result.returncode == 1
        assert "manual_refs is not supported" in result.stdout
        assert "docs/features/sample-feature/feature.source.yaml" in result.stdout
    finally:
        rmtree(test_root, ignore_errors=True)


def test_linkage_audit_passes_when_refs_are_metadata_derived() -> None:
    test_root = make_test_root()
    try:
        seed_repo(test_root, include_manual_refs=False)
        result = run_audit(test_root, "--report-awareness")

        assert result.returncode == 0, result.stderr + result.stdout
        assert "no feature-source manual_refs found" in result.stdout
    finally:
        rmtree(test_root, ignore_errors=True)
