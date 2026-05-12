"""Add run-scoped mark persistence columns to rule_filter_results.

Run with:
    python scripts/migrations/011_add_marks_json_to_rule_filter_results.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from google.cloud import bigquery

_SCRIPT_PATH = Path(__file__).resolve()
_WORKTREE_ROOT = _SCRIPT_PATH.parents[2]
_SRC_DIR = _WORKTREE_ROOT / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from fitcv.config import load_config


def main() -> None:
    config = load_config()
    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = bigquery.Client(project=project)
    table = f"{project}.{dataset}.rule_filter_results"
    statements = [
        f"""
        ALTER TABLE `{table}`
        ADD COLUMN IF NOT EXISTS run_id STRING
        OPTIONS(description="Pipeline run identifier for run-scoped inspection")
        """,
        f"""
        ALTER TABLE `{table}`
        ADD COLUMN IF NOT EXISTS marks_json STRING
        OPTIONS(description="JSON-encoded non-blocking rule-filter marks for passed or rejected jobs")
        """,
    ]
    for sql in statements:
        client.query(sql).result()
    print(f"Updated {table}")


if __name__ == "__main__":
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(load_config()["service_account_key"]))
    main()
"""
@meta
name: add_marks_json_to_rule_filter_results
type: migration
domain: data
responsibility:
  - Add marks_json storage to rule-filter result records.
inputs:
  - Existing rule_filter_results schema
outputs:
  - Updated rule_filter_results schema with marks_json
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
