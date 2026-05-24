"""
@meta
name: run_validator
type: script
domain: validation
distribution_tier: starter_kit
responsibility:
  - Run repo-contract validation from stable repo-root hook entrypoint.
inputs:
  - Optional validator CLI args
outputs:
  - Validator exit code passthrough
lifecycle:
  status: active
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main(argv: list[str]) -> int:
    root = repo_root()
    cmd = [sys.executable, str(root / "scripts" / "validate_repo_contracts.py"), *argv]
    return subprocess.run(cmd, cwd=root, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
