"""Add settings_used_json to pipeline_runs if missing."""

from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / ".env.yaml"
    key_path = repo_root / "sa_key.json"

    project_id = None
    dataset = "fitcv"
    for line in config_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("gcp_project:"):
            project_id = stripped.split(":", 1)[1].strip().strip("'\"")
        if stripped.startswith("bigquery_dataset:"):
            dataset = stripped.split(":", 1)[1].strip().strip("'\"")

    if not project_id:
        raise RuntimeError("Could not determine gcp_project from .env.yaml")

    credentials = service_account.Credentials.from_service_account_file(str(key_path))
    client = bigquery.Client(project=project_id, credentials=credentials)

    table = f"`{project_id}.{dataset}.pipeline_runs`"
    sql = f"""
    ALTER TABLE {table}
    ADD COLUMN IF NOT EXISTS settings_used_json STRING
    OPTIONS(description="Immutable run-scoped effective-settings snapshot for completed runs")
    """
    client.query(sql).result()
    print("Added settings_used_json to pipeline_runs")


if __name__ == "__main__":
    main()
"""
@meta
name: add_settings_used_json_to_pipeline_runs
type: migration
domain: data
responsibility:
  - Add settings-used snapshot storage to pipeline run records.
inputs:
  - Existing pipeline_runs schema
outputs:
  - Updated pipeline_runs schema with settings_used_json
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
