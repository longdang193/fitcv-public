"""Add results_export_json column to pipeline_runs."""

from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    ddl = (
        "ALTER TABLE `{project}.{dataset}.pipeline_runs` "
        "ADD COLUMN IF NOT EXISTS results_export_json STRING "
        'OPTIONS(description="Immutable run-results export snapshot for completed runs")'
    )

    project = "fitcv-491123"
    dataset = "fitcv"
    key_path = repo_root / "sa_key.json"
    credentials = service_account.Credentials.from_service_account_file(str(key_path))
    client = bigquery.Client(project=project, credentials=credentials)
    client.query(ddl.format(project=project, dataset=dataset)).result()
    print("Added results_export_json to pipeline_runs")


if __name__ == "__main__":
    main()
"""
@meta
name: add_results_export_json_to_pipeline_runs
type: migration
domain: data
responsibility:
  - Add results-export JSON storage to pipeline run records.
inputs:
  - Existing pipeline_runs schema
outputs:
  - Updated pipeline_runs schema with results export storage
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
