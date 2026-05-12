#!/usr/bin/env python3
"""
@meta
name: download_cvs
type: script
domain: export
responsibility:
  - Export recently generated CV markdown documents from BigQuery.
  - Materialize operator-readable CV files under the local output folder.
inputs:
  - .env.yaml
  - BigQuery cv_versions rows
outputs:
  - data/output/cvs/*.md
tags:
  - export
  - bigquery
lifecycle:
  status: active
"""

import os
import yaml
from google.cloud import bigquery
from urllib.parse import urlparse

def main():
    # Load config
    try:
        with open('.env.yaml') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: .env.yaml not found.")
        return

    # Setup BigQuery client
    project = config.get('gcp_project')
    dataset = config.get('bigquery_dataset')
    
    if not project or not dataset:
        print("Error: Missing gcp_project or bigquery_dataset in .env.yaml")
        return

    client = bigquery.Client(project=project)
    
    # Query latest CVs
    query = f"""
        SELECT version_id, job_url, fit_classification, cv_markdown, generated_at
        FROM `{project}.{dataset}.cv_versions`
        ORDER BY generated_at DESC
        LIMIT 50
    """
    
    print(f"Fetching generated CVs from BigQuery: {project}.{dataset}.cv_versions...")
    try:
        rows = list(client.query(query).result())
    except Exception as e:
        print(f"Failed to query BigQuery: {e}")
        return

    if not rows:
        print("No CVs found in the database. Run the pipeline first!")
        return

    # Create output directory
    os.makedirs('data/output/cvs', exist_ok=True)
    
    print(f"Found {len(rows)} CVs. Saving to data/output/cvs/...")
    
    for row in rows:
        # Extract sensible filename from Job URL
        parsed = urlparse(row.job_url)
        path = parsed.path.strip('/').split('/')
        company = path[0] if len(path) > 0 else "unknown"
        job = path[-1] if len(path) > 1 else "role"
            
        fit = row.fit_classification.lower() if row.fit_classification else "unknown"
        filename = f"{company}_{job}_{fit}_{row.version_id[:8]}.md"
        filepath = os.path.join('data/output/cvs', filename)
        
        with open(filepath, 'w') as f:
            f.write(f"<!-- Job URL: {row.job_url} -->\n")
            f.write(f"<!-- Generated At: {row.generated_at} -->\n")
            f.write(f"<!-- Fit Classification: {row.fit_classification} -->\n")
            f.write(f"<!-- Version ID: {row.version_id} -->\n\n")
            f.write(row.cv_markdown)
            
        print(f"Saved: {filepath}")

if __name__ == "__main__":
    main()
