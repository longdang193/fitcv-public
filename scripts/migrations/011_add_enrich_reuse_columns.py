#!/usr/bin/env python3
"""Migration: add enrich reuse fingerprint/provenance columns to enrichment tables.

Run once for already-bootstrapped environments:
    python scripts/migrations/011_add_enrich_reuse_columns.py

Requires .env.yaml with gcp_project, bigquery_dataset, and service_account_key.
"""

import os
from pathlib import Path

import yaml


STRUCTURED_JOBS_ADDITIONS = (
    "ADD COLUMN IF NOT EXISTS raw_job_fingerprint STRING",
    "ADD COLUMN IF NOT EXISTS enrich_contract_fingerprint STRING",
    "ADD COLUMN IF NOT EXISTS enrich_reuse_status STRING",
)

RUN_STRUCTURED_JOBS_ADDITIONS = (
    "ADD COLUMN IF NOT EXISTS raw_job_fingerprint STRING",
    "ADD COLUMN IF NOT EXISTS enrich_contract_fingerprint STRING",
    "ADD COLUMN IF NOT EXISTS enrich_reuse_status STRING",
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


def _run_alter_statements(
    *,
    project: str,
    dataset: str,
    key_path: str,
    table_name: str,
    additions: tuple[str, ...],
) -> None:
    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
    table_ref = f"`{project}.{dataset}.{table_name}`"

    for clause in additions:
        sql = f"ALTER TABLE {table_ref} {clause}"
        print(f"Executing on {table_name}: {clause}")
        client.query(sql).result()


def main() -> None:
    try:
        config = _load_config()
    except FileNotFoundError as exc:
        print(str(exc))
        return

    project = config.get("gcp_project")
    dataset = config.get("bigquery_dataset")
    key_path = _resolve_key_path(config)

    if not project or not dataset or not key_path:
        print("Error: Missing gcp_project, bigquery_dataset, or service_account_key in .env.yaml")
        return

    try:
        _run_alter_statements(
            project=str(project),
            dataset=str(dataset),
            key_path=str(key_path),
            table_name="structured_jobs",
            additions=STRUCTURED_JOBS_ADDITIONS,
        )
        _run_alter_statements(
            project=str(project),
            dataset=str(dataset),
            key_path=str(key_path),
            table_name="run_structured_jobs",
            additions=RUN_STRUCTURED_JOBS_ADDITIONS,
        )
    except Exception as exc:
        print(f"❌ Migration failed: {exc}")
        return

    print("✅ Migration successful: enrich reuse columns added/verified.")


if __name__ == "__main__":
    main()
"""
@meta
name: add_enrich_reuse_columns
type: migration
domain: data
responsibility:
  - Add enrich-reuse provenance columns to the relevant storage surfaces.
inputs:
  - Existing enrich storage schema
outputs:
  - Updated schema with enrich reuse metadata
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
