#!/usr/bin/env python3
"""Migration: Add run_id to fitcv.cv_versions"""

import yaml
from google.cloud import bigquery

def main():
    try:
        with open('.env.yaml') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: .env.yaml not found.")
        return

    project = config.get('gcp_project')
    dataset = config.get('bigquery_dataset')
    key_path = config.get('service_account_key')
    
    if not project or not dataset or not key_path:
        print("Error: Missing gcp_project, bigquery_dataset, or service_account_key in .env.yaml")
        return

    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
    
    ddl = f"""
        ALTER TABLE `{project}.{dataset}.cv_versions` 
        ADD COLUMN IF NOT EXISTS run_id STRING OPTIONS(description="Logical FK to pipeline_runs");
    """
    
    print(f"Executing migration on {project}.{dataset}.cv_versions...")
    try:
        job = client.query(ddl)
        job.result()  # Wait for query to complete
        print("✅ Migration successful: run_id column added to cv_versions.")
    except Exception as e:
        print(f"❌ Migration failed: {e}")

if __name__ == "__main__":
    main()
"""
@meta
name: add_run_id_to_cvs
type: migration
domain: data
responsibility:
  - Add the run_id field to persisted CV records.
inputs:
  - Existing BigQuery or application CV schema
outputs:
  - Updated CV schema with run linkage
tags:
  - migration
  - schema
lifecycle:
  status: active
"""
