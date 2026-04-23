#!/usr/bin/env python3
"""
@meta
name: bootstrap_bigquery
type: script
domain: infrastructure
responsibility:
  - Bootstrap the FitCV BigQuery dataset and tables from checked-in SQL assets.
  - Create missing tables in dependency order for local setup and integration testing.
inputs:
  - assets/bigquery/*.sql
  - GOOGLE_APPLICATION_CREDENTIALS
outputs:
  - BigQuery dataset and tables
tags:
  - setup
  - bigquery
lifecycle:
  status: active
"""

"""Bootstrap BigQuery dataset and all tables for FitCV.

Run once before integration tests:
    python scripts/bootstrap_bigquery.py

Requires GOOGLE_APPLICATION_CREDENTIALS to be set.
"""

import sys
from pathlib import Path

PROJECT = "fitcv-491123"
DATASET = "fitcv"
REGION  = "US"  # BQ dataset location (multi-region)

ASSETS_DIR = Path(__file__).parent.parent / "assets" / "bigquery"

# Ordered so dependencies come first (FKs are not enforced in BQ, but logical order helps)
TABLE_ORDER = [
    "raw_jobs",
    "structured_jobs",
    "run_structured_jobs",
    "candidate_profile",
    "candidate_skills",
    "candidate_experiences",
    "candidate_education",
    "candidate_certifications",
    "candidate_projects",
    "candidate_achievements",
    "candidate_embeddings",
    "job_embeddings",
    "rule_filter_results",
    "vector_shortlist",
    "ai_score_results",
    "final_ranking",
    "evidence_retrieval",
    "gap_analysis",
    "cv_versions",
    "application_tracker",
    # Control-plane tables
    "pipeline_runs",
    "pipeline_run_events",
    "pipeline_settings",
]


def main() -> None:
    from google.cloud import bigquery  # type: ignore[import-untyped]

    client = bigquery.Client(project=PROJECT)

    # 1. Create dataset (idempotent)
    dataset_id = f"{PROJECT}.{DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"✓ Dataset {dataset_id} ready.")

    # 2. Create each table from its DDL file
    for table_name in TABLE_ORDER:
        sql_path = ASSETS_DIR / f"{table_name}.sql"
        if not sql_path.exists():
            print(f"  ⚠ {table_name}.sql not found — skipping")
            continue
        ddl = sql_path.read_text().strip()
        if not ddl:
            print(f"  ⚠ {table_name}.sql is empty — skipping")
            continue

        ddl = ddl.replace("{project}", PROJECT).replace("{dataset}", DATASET)

        try:
            job = client.query(ddl)
            job.result()
            print(f"✓ Table {DATASET}.{table_name} created/verified.")
        except Exception as exc:
            print(f"  ✗ {table_name}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
