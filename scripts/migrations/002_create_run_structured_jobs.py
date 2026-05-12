#!/usr/bin/env python3
"""Migration: Create fitcv.run_structured_jobs table.

Run once for already-bootstrapped environments to add the table:
    python scripts/migrations/002_create_run_structured_jobs.py

Requires .env.yaml with gcp_project, bigquery_dataset, and service_account_key.
"""

from pathlib import Path

import yaml


def main() -> None:
    try:
        with open(".env.yaml") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: .env.yaml not found. Run from repo root.")
        return

    project = config.get("gcp_project")
    dataset = config.get("bigquery_dataset")
    key_path = config.get("service_account_key")

    if not project or not dataset or not key_path:
        print("Error: Missing gcp_project, bigquery_dataset, or service_account_key in .env.yaml")
        return

    repo_root = Path(__file__).parent.parent
    ddl_path = repo_root / "assets" / "bigquery" / "run_structured_jobs.sql"

    if not ddl_path.exists():
        print(f"Error: DDL asset not found at {ddl_path}")
        return

    ddl = ddl_path.read_text().strip()
    ddl = ddl.replace("{project}", project).replace("{dataset}", dataset)

    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)

    print(f"Executing migration on {project}.{dataset}.run_structured_jobs...")
    try:
        job = client.query(ddl)
        job.result()
        print("✅ Migration successful: run_structured_jobs table created/verified.")
    except Exception as e:
        print(f"❌ Migration failed: {e}")


if __name__ == "__main__":
    main()
"""
@meta
name: create_run_structured_jobs
type: migration
domain: data
responsibility:
  - Create the run_structured_jobs storage surface for per-run structured job state.
inputs:
  - Existing pipeline storage schema
outputs:
  - New run_structured_jobs table or equivalent schema
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
