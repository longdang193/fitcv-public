"""
@meta
name: migrate_pipeline_runs_orchestration_columns
type: script
domain: run_orchestration
responsibility:
  - Add orchestration binding columns to pipeline_runs for persisted backend identity.
  - Provide idempotent schema migration for control-plane rollout.
inputs:
  - GCP project and BigQuery dataset
outputs:
  - ALTER TABLE statements executed when columns are missing
lifecycle:
  status: active
"""

from __future__ import annotations

import argparse
import os

from google.cloud import bigquery

REQUIRED_COLUMNS = ("orchestration_backend", "orchestration_run_id")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate pipeline_runs orchestration columns.")
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT", ""), help="GCP project id")
    parser.add_argument("--dataset", default=os.environ.get("BIGQUERY_DATASET", "fitcv"), help="BigQuery dataset")
    parser.add_argument("--apply", action="store_true", help="Apply migration (default is dry-run)")
    return parser.parse_args()


def _existing_columns(client: bigquery.Client, *, project: str, dataset: str) -> set[str]:
    sql = (
        f"SELECT column_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        "WHERE table_name = 'pipeline_runs'"
    )
    rows = client.query(sql).result()
    return {str(dict(row).get("column_name") or "").strip() for row in rows}


def main() -> int:
    args = parse_args()
    if not args.project:
        raise SystemExit("--project is required (or set GCP_PROJECT)")

    client = bigquery.Client(project=args.project)
    existing = _existing_columns(client, project=args.project, dataset=args.dataset)
    missing = [column for column in REQUIRED_COLUMNS if column not in existing]

    if not missing:
        print("Schema already complete: no migration needed.")
        return 0

    statements = [
        f"ALTER TABLE `{args.project}.{args.dataset}.pipeline_runs` ADD COLUMN IF NOT EXISTS {column} STRING"
        for column in missing
    ]

    print("Missing columns:", ", ".join(missing))
    print("Planned statements:")
    for statement in statements:
        print(f"- {statement}")

    if not args.apply:
        print("Dry-run only. Re-run with --apply to execute.")
        return 0

    for statement in statements:
        client.query(statement).result()
    print("Migration applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
