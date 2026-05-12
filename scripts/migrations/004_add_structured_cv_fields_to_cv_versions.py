"""
@meta
name: add_structured_cv_fields_to_cv_versions
type: migration
domain: bigquery
responsibility:
  - Add structured CV artifact columns to fitcv.cv_versions
  - Keep historical rows valid by using nullable additive columns
inputs:
  - BigQuery table fitcv.cv_versions
outputs:
  - Extended cv_versions schema with structured CV metadata fields
tags:
  - migration
  - bigquery
  - cv
lifecycle:
  status: active
"""

from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    project = "fitcv-491123"
    dataset = "fitcv"
    key_path = repo_root / "sa_key.json"
    credentials = service_account.Credentials.from_service_account_file(str(key_path))
    client = bigquery.Client(project=project, credentials=credentials)

    statements = [
        (
            "cv_prompt_version",
            'ALTER TABLE `{project}.{dataset}.cv_versions` ADD COLUMN IF NOT EXISTS cv_prompt_version STRING OPTIONS(description="Structured CV generation prompt version identifier")',
        ),
        (
            "cv_generation_model",
            'ALTER TABLE `{project}.{dataset}.cv_versions` ADD COLUMN IF NOT EXISTS cv_generation_model STRING OPTIONS(description="Model used to generate the structured CV artifact")',
        ),
        (
            "cv_schema_version",
            'ALTER TABLE `{project}.{dataset}.cv_versions` ADD COLUMN IF NOT EXISTS cv_schema_version STRING OPTIONS(description="Structured CV schema version, e.g. cv_doc_v1")',
        ),
        (
            "cv_structured_json",
            'ALTER TABLE `{project}.{dataset}.cv_versions` ADD COLUMN IF NOT EXISTS cv_structured_json STRING OPTIONS(description="JSON-serialised structured CV document")',
        ),
    ]

    for column_name, ddl in statements:
        client.query(ddl.format(project=project, dataset=dataset)).result()
        print(f"Added {column_name} to cv_versions")


if __name__ == "__main__":
    main()
