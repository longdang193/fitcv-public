"""Add synonym_proposals_json to pipeline_runs.

Run with:
    python scripts/migrations/011_add_synonym_proposals_json_to_pipeline_runs.py
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
    table = f"{project}.{dataset}.pipeline_runs"
    sql = f"""
    ALTER TABLE `{table}`
    ADD COLUMN IF NOT EXISTS synonym_proposals_json STRING
    OPTIONS(description="Mutable run-scoped synonym proposal review snapshot")
    """
    client.query(sql).result()
    print(f"Updated {table}")


if __name__ == "__main__":
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(load_config()["service_account_key"]))
    main()
"""
@meta
name: add_synonym_proposals_json_to_pipeline_runs
type: migration
domain: data
responsibility:
  - Add synonym proposal review snapshot storage to pipeline run records.
inputs:
  - Existing pipeline_runs schema
outputs:
  - Updated pipeline_runs schema with synonym_proposals_json
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
