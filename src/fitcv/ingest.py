"""@meta
name: ingest
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.ingest.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fitcv.config import sqlite_mode_enabled

# ── field mapping: LinkedIn scraper camelCase → raw_jobs snake_case ──────────

_CAMEL_TO_SNAKE: dict[str, str] = {
    "jobUrl": "job_url",
    "postedTime": "posted_time",
    "publishedAt": "published_at",
    "companyName": "company_name",
    "companyUrl": "company_url",
    "companyId": "company_id",
    "applicationsCount": "applications_count",
    "contractType": "contract_type",
    "experienceLevel": "experience_level",
    "workType": "work_type",
    "posterFullName": "poster_full_name",
    "posterProfileUrl": "poster_profile_url",
    "applyUrl": "apply_url",
    "applyType": "apply_type",
}

# Fields the scraper must always provide
_REQUIRED_SCRAPER_FIELDS: list[str] = [
    "jobUrl",
    "title",
    "companyName",
    "description",
    "contractType",
    "experienceLevel",
]


# ── parsing ──────────────────────────────────────────────────────────────────

def parse_jobs_file(path: str | Path) -> list[dict[str, Any]]:
    """Load a JSON array of LinkedIn job objects from *path*.

    Raises:
        FileNotFoundError: if the file does not exist.
        ValueError: if the file does not contain a JSON array.
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Jobs file not found: {file_path}")

    with open(file_path, encoding="utf-8") as f:
        data: Any = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"Jobs file must contain a JSON array, expected a JSON array but got {type(data).__name__}")

    return data  # type: ignore[return-value]


def validate_linkedin_schema(job: dict[str, Any]) -> list[str]:
    """Return a list of error strings for any missing required scraper fields.

    Returns an empty list when the job is valid.
    """
    return [
        f"Missing required field: '{field}'"
        for field in _REQUIRED_SCRAPER_FIELDS
        if field not in job
    ]


# ── key conversion ────────────────────────────────────────────────────────────

def snake_case_keys(job: dict[str, Any]) -> dict[str, Any]:
    """Return a new dict with camelCase LinkedIn keys mapped to snake_case.

    Fields not in the mapping are preserved as-is with their original key.
    """
    return {_CAMEL_TO_SNAKE.get(k, k): v for k, v in job.items()}


# ── row preparation ───────────────────────────────────────────────────────────

def prepare_raw_rows(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map a list of LinkedIn scraper dicts into raw_jobs BQ schema rows.

    Each row includes the full original JSON in `raw_json` for auditability
    and an `ingested_at` timestamp set to the current UTC time.
    """
    ingested_at = datetime.now(tz=timezone.utc).isoformat()

    rows: list[dict[str, Any]] = []
    for job in jobs:
        snaked = snake_case_keys(job)
        row: dict[str, Any] = {
            "job_url":            snaked.get("job_url", ""),
            "title":              snaked.get("title", ""),
            "location":           snaked.get("location", ""),
            "posted_time":        snaked.get("posted_time", ""),
            "published_at":       snaked.get("published_at", None),
            "company_name":       snaked.get("company_name", ""),
            "company_url":        snaked.get("company_url", ""),
            "company_id":         snaked.get("company_id", ""),
            "description":        snaked.get("description", ""),
            "applications_count": snaked.get("applications_count", ""),
            "contract_type":      snaked.get("contract_type", ""),
            "experience_level":   snaked.get("experience_level", ""),
            "work_type":          snaked.get("work_type", ""),
            "sector":             snaked.get("sector", ""),
            "salary":             snaked.get("salary", ""),
            "apply_url":          snaked.get("apply_url", ""),
            "apply_type":         snaked.get("apply_type", ""),
            "raw_json":           json.dumps(job, ensure_ascii=False),
            "ingested_at":        ingested_at,
        }
        rows.append(row)

    return rows


# ── BigQuery load (integration) ───────────────────────────────────────────────

def _local_sqlite_path() -> str:
    return str(os.environ.get("FITCV_CP_SQLITE_PATH") or "data/fitcv_cp.sqlite3").strip() or "data/fitcv_cp.sqlite3"



def _ensure_local_raw_jobs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_jobs (
            job_url TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            location TEXT NOT NULL,
            posted_time TEXT NOT NULL,
            published_at TEXT,
            company_name TEXT NOT NULL,
            company_url TEXT NOT NULL,
            company_id TEXT NOT NULL,
            description TEXT NOT NULL,
            applications_count TEXT NOT NULL,
            contract_type TEXT NOT NULL,
            experience_level TEXT NOT NULL,
            work_type TEXT NOT NULL,
            sector TEXT NOT NULL,
            salary TEXT NOT NULL,
            apply_url TEXT NOT NULL,
            apply_type TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        )
        """
    )
    conn.commit()



def load_to_bigquery(rows: list[dict[str, Any]], config: dict[str, Any]) -> int:
    """Insert *rows* into fitcv.raw_jobs and return the number of rows inserted.

    Requires GOOGLE_APPLICATION_CREDENTIALS to be set.
    Decorated with @pytest.mark.integration in tests.

    Args:
        rows:   Output of prepare_raw_rows().
        config: Dict from load_config() containing gcp_project, bigquery_dataset,
                and service_account_key.

    Returns:
        Number of rows successfully inserted.
    """
    if sqlite_mode_enabled(config):
        db_path = Path(_local_sqlite_path())
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            _ensure_local_raw_jobs_table(conn)
            conn.executemany(
                """
                INSERT INTO raw_jobs(
                    job_url,
                    title,
                    location,
                    posted_time,
                    published_at,
                    company_name,
                    company_url,
                    company_id,
                    description,
                    applications_count,
                    contract_type,
                    experience_level,
                    work_type,
                    sector,
                    salary,
                    apply_url,
                    apply_type,
                    raw_json,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_url) DO UPDATE SET
                    title = excluded.title,
                    location = excluded.location,
                    posted_time = excluded.posted_time,
                    published_at = excluded.published_at,
                    company_name = excluded.company_name,
                    company_url = excluded.company_url,
                    company_id = excluded.company_id,
                    description = excluded.description,
                    applications_count = excluded.applications_count,
                    contract_type = excluded.contract_type,
                    experience_level = excluded.experience_level,
                    work_type = excluded.work_type,
                    sector = excluded.sector,
                    salary = excluded.salary,
                    apply_url = excluded.apply_url,
                    apply_type = excluded.apply_type,
                    raw_json = excluded.raw_json,
                    ingested_at = excluded.ingested_at
                """,
                [
                    (
                        str(row.get("job_url") or ""),
                        str(row.get("title") or ""),
                        str(row.get("location") or ""),
                        str(row.get("posted_time") or ""),
                        row.get("published_at"),
                        str(row.get("company_name") or ""),
                        str(row.get("company_url") or ""),
                        str(row.get("company_id") or ""),
                        str(row.get("description") or ""),
                        str(row.get("applications_count") or ""),
                        str(row.get("contract_type") or ""),
                        str(row.get("experience_level") or ""),
                        str(row.get("work_type") or ""),
                        str(row.get("sector") or ""),
                        str(row.get("salary") or ""),
                        str(row.get("apply_url") or ""),
                        str(row.get("apply_type") or ""),
                        str(row.get("raw_json") or ""),
                        str(row.get("ingested_at") or ""),
                    )
                    for row in rows
                ],
            )
            conn.commit()
        return len(rows)

    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    key_path: str = str(config["service_account_key"])
    project: str = str(config["gcp_project"])
    dataset: str = str(config["bigquery_dataset"])

    if key_path:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bigquery.Client(project=project, credentials=credentials)
    else:
        client = bigquery.Client(project=project)

    table_ref = f"{project}.{dataset}.raw_jobs"
    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    return len(rows)


# ── Apify API source ──────────────────────────────────────────────────────────

def fetch_from_apify(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch all items from an Apify dataset via the REST API.

    Returns the same list[dict] format as parse_jobs_file so the rest of the
    pipeline is source-agnostic.

    Args:
        config: Must contain 'apify_dataset_id' and 'apify_token'.

    Returns:
        List of raw LinkedIn job dicts.

    Raises:
        KeyError:   If apify_dataset_id or apify_token missing from config.
        ValueError: If the API does not return a JSON array.
    """
    import urllib.request

    dataset_id: str = str(config["apify_dataset_id"])
    token: str = str(config["apify_token"])
    url = (
        f"https://api.apify.com/v2/datasets/{dataset_id}/items"
        f"?format=json&clean=true"
    )
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as resp:  # noqa: S310
        data: Any = json.loads(resp.read())

    if not isinstance(data, list):
        raise ValueError("Apify API did not return a JSON array")

    return data  # type: ignore[return-value]
