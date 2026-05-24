"""@meta
name: persistence
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared persistence helpers for fitcv runtime modules.
inputs:
  - Runtime config and environment values
outputs:
  - Normalized sqlite path and BigQuery client construction
lifecycle:
  - status: active
"""

import os
from typing import Any


def get_local_sqlite_path() -> str:
    return str(os.environ.get("FITCV_CP_SQLITE_PATH") or "data/fitcv_cp.sqlite3").strip() or "data/fitcv_cp.sqlite3"


def build_bigquery_client(config: dict[str, Any]) -> Any:
    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    key_path = str(config["service_account_key"])
    if key_path:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(project=project, credentials=credentials)
    return bigquery.Client(project=project)
