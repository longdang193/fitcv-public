"""Uvicorn entrypoint for the FitCV admin web service."""
import os
from pathlib import Path

from google.cloud import bigquery

from fitcv_cp.app import create_app


def _validate_google_credentials_path() -> None:
    """Fail fast with a clear message if the mounted key path is invalid."""
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        return

    path = Path(credentials_path)
    if not path.exists():
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS does not exist: "
            f"{credentials_path}. Set GCP_SA_KEY_PATH to a real JSON key file."
        )
    if path.is_dir():
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS points to a directory, not a file: "
            f"{credentials_path}. Set GCP_SA_KEY_PATH to a real JSON key file."
        )


_validate_google_credentials_path()
bq = bigquery.Client()
app = create_app(
    bq=bq,
    project=os.environ["GCP_PROJECT"],
    dataset=os.environ.get("BIGQUERY_DATASET", "fitcv"),
    redis_url=os.environ.get("REDIS_URL", "redis://redis:6379/0"),
)
