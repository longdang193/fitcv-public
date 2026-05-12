#!/usr/bin/env python3
"""Migration: add manual staged checkpoint columns to pipeline_runs.

Run once for already-bootstrapped environments:
    python scripts/migrations/009_add_manual_checkpoint_columns_to_pipeline_runs.py
"""

import os
from pathlib import Path

import yaml


PIPELINE_RUNS_ADDITIONS = (
    "ADD COLUMN IF NOT EXISTS run_mode STRING",
    "ADD COLUMN IF NOT EXISTS checkpoint_status STRING",
    "ADD COLUMN IF NOT EXISTS next_stage STRING",
    "ADD COLUMN IF NOT EXISTS last_completed_stage STRING",
    "ADD COLUMN IF NOT EXISTS completed_stages_json STRING",
    "ADD COLUMN IF NOT EXISTS checkpoint_payload_json STRING",
)


def _load_config() -> dict:
    env_path = Path(".env.yaml")
    if not env_path.exists():
        raise FileNotFoundError("Error: .env.yaml not found. Run from repo root.")
    with env_path.open() as handle:
        return yaml.safe_load(handle) or {}


def _resolve_key_path(config: dict) -> str | None:
    env_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if env_key_path:
        return env_key_path

    compose_key_path = os.environ.get("GCP_SA_KEY_PATH", "").strip()
    if compose_key_path:
        return compose_key_path

    key_path = str(config.get("service_account_key") or "").strip()
    if not key_path:
        return None
    return key_path


def main() -> None:
    try:
        config = _load_config()
    except FileNotFoundError as exc:
        print(str(exc))
        return

    project = str(config.get("gcp_project") or "")
    dataset = str(config.get("bigquery_dataset") or "")
    key_path = _resolve_key_path(config)
    if not project or not dataset or not key_path:
        print("Error: Missing gcp_project, bigquery_dataset, or service_account_key in .env.yaml")
        return

    try:
        from google.cloud import bigquery  # type: ignore[import-untyped]
        from google.oauth2 import service_account  # type: ignore[import-untyped]

        credentials = service_account.Credentials.from_service_account_file(str(key_path))
        client = bigquery.Client(project=project, credentials=credentials)
        table_ref = f"`{project}.{dataset}.pipeline_runs`"
        for clause in PIPELINE_RUNS_ADDITIONS:
            sql = f"ALTER TABLE {table_ref} {clause}"
            print(f"Executing: {clause}")
            client.query(sql).result()
    except Exception as exc:
        print(f"❌ Migration failed: {exc}")
        return

    print("✅ Migration successful: manual checkpoint columns added/verified on pipeline_runs.")


if __name__ == "__main__":
    main()
"""
@meta
name: add_manual_checkpoint_columns_to_pipeline_runs
type: migration
domain: data
responsibility:
  - Add manual-checkpoint storage columns to pipeline run records.
inputs:
  - Existing pipeline_runs schema
outputs:
  - Updated pipeline_runs schema with staged checkpoint storage
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
