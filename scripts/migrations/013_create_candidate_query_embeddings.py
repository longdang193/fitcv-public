#!/usr/bin/env python3
"""Migration: create shortlist candidate_query_embeddings table if missing.

Run once for already-bootstrapped environments:
    python scripts/migrations/013_create_candidate_query_embeddings.py

Requires .env.yaml with gcp_project, bigquery_dataset, and service_account_key.
"""

import os
from pathlib import Path

import yaml


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.candidate_query_embeddings` (
  candidate_query_signature STRING NOT NULL OPTIONS (description = "Stable shortlist candidate-query signature"),
  candidate_query_contract_fingerprint STRING OPTIONS (description = "Candidate-query embedding contract fingerprint"),
  candidate_query_text STRING OPTIONS (description = "Deterministic shortlist candidate query text"),
  candidate_query_components_json STRING OPTIONS (description = "Serialized bounded candidate-query components for debugging"),
  embedding ARRAY<FLOAT64> OPTIONS (description = "Dense vector from Vertex AI text-embedding-005"),
  created_at TIMESTAMP
)
OPTIONS (description = "Cached embeddings for the single shortlist candidate query text")
""".strip()


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

    project = config.get("gcp_project")
    dataset = config.get("bigquery_dataset")
    key_path = _resolve_key_path(config)

    if not project or not dataset or not key_path:
        print("Error: Missing gcp_project, bigquery_dataset, or service_account_key in .env.yaml")
        return

    try:
        from google.cloud import bigquery  # type: ignore[import-untyped]
        from google.oauth2 import service_account  # type: ignore[import-untyped]

        credentials = service_account.Credentials.from_service_account_file(str(key_path))
        client = bigquery.Client(project=str(project), credentials=credentials)
        client.query(
            CREATE_TABLE_SQL.format(project=str(project), dataset=str(dataset))
        ).result()
    except Exception as exc:
        print(f"❌ Migration failed: {exc}")
        return

    print("✅ Migration successful: shortlist candidate_query_embeddings table created/verified.")


if __name__ == "__main__":
    main()
"""
@meta
name: create_candidate_query_embeddings
type: migration
domain: data
responsibility:
  - Create the candidate-query embeddings storage surface for shortlist retrieval.
inputs:
  - Existing embedding storage schema
outputs:
  - New candidate-query embeddings table or equivalent schema
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
