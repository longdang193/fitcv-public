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
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fitcv.config import sqlite_mode_enabled
from fitcv.contracts import REQUIRED_SCRAPER_FIELDS, SCRAPER_CAMEL_TO_SNAKE
from fitcv.persistence import build_bigquery_client, get_local_sqlite_path

# ── field mapping: LinkedIn scraper camelCase → raw_jobs snake_case ──────────

_CAMEL_TO_SNAKE: dict[str, str] = SCRAPER_CAMEL_TO_SNAKE.copy()

# Fields the scraper must always provide
_REQUIRED_SCRAPER_FIELDS: tuple[str, ...] = REQUIRED_SCRAPER_FIELDS

# ── parsing ──────────────────────────────────────────────────────────────────

def parse_jobs_file(path: str | Path) -> list[dict[str, Any]]:
    """Load a JSON array of job objects from *path*.

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

    return data

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

def _indeed_description_text(job: dict[str, Any]) -> str:
    description = job.get("description")
    if isinstance(description, dict):
        return str(description.get("text") or "")
    return str(description or "")


def _indeed_location_text(job: dict[str, Any]) -> str:
    location = job.get("location")
    if not isinstance(location, dict):
        return str(location or "")

    parts = [
        str(location.get("city") or "").strip(),
        str(location.get("countryName") or "").strip(),
    ]
    if not any(parts):
        fallback = str(location.get("streetAddress") or location.get("postalCode") or "").strip()
        return fallback
    return ", ".join(part for part in parts if part)


def _indeed_job_types_text(job: dict[str, Any]) -> str:
    job_types = job.get("jobTypes")
    if isinstance(job_types, dict):
        values = [str(value).strip() for value in job_types.values() if str(value).strip()]
        return ", ".join(dict.fromkeys(values))
    if isinstance(job_types, list):
        values = [str(value).strip() for value in job_types if str(value).strip()]
        return ", ".join(dict.fromkeys(values))
    return str(job_types or "")


def _indeed_company_name(job: dict[str, Any]) -> str:
    employer = job.get("employer")
    if isinstance(employer, dict):
        name = str(employer.get("name") or "").strip()
        if name:
            return name
    parent_employer = job.get("parentEmployer")
    if isinstance(parent_employer, dict):
        return str(parent_employer.get("name") or "")
    return ""


def _indeed_company_url(job: dict[str, Any]) -> str:
    employer = job.get("employer")
    if isinstance(employer, dict):
        url = str(employer.get("companyPageUrl") or "").strip()
        if url:
            return url
    parent_employer = job.get("parentEmployer")
    if isinstance(parent_employer, dict):
        return str(parent_employer.get("companyPageUrl") or "")
    return ""


def _indeed_job_url(job: dict[str, Any]) -> str:
    return str(job.get("url") or job.get("job_url") or "")


def _indeed_apply_url(job: dict[str, Any]) -> str:
    apply_url = str(job.get("jobUrl") or "").strip()
    if apply_url:
        return apply_url
    return _indeed_job_url(job)


def _indeed_company_id(job: dict[str, Any]) -> str:
    company_url = _indeed_company_url(job)
    if company_url:
        return company_url
    company_name = _indeed_company_name(job)
    if company_name:
        return company_name
    return _indeed_job_url(job)


def _indeed_published_at(job: dict[str, Any]) -> str | None:
    published_at = str(job.get("datePublished") or "").strip()
    if not published_at:
        return None
    return published_at.split("T", 1)[0]


def _indeed_posted_time(job: dict[str, Any]) -> str:
    return str(job.get("dateOnIndeed") or "")


def _is_indeed_job(job: dict[str, Any]) -> bool:
    return bool(job.get("url")) and (
        "dateOnIndeed" in job
        or isinstance(job.get("employer"), dict)
        or isinstance(job.get("jobTypes"), dict)
    )


def _normalize_indeed_job(job: dict[str, Any]) -> dict[str, Any]:
    apply_url = _indeed_apply_url(job)
    return {
        "job_url": _indeed_job_url(job),
        "title": str(job.get("title") or ""),
        "location": _indeed_location_text(job),
        "posted_time": _indeed_posted_time(job),
        "published_at": _indeed_published_at(job),
        "company_name": _indeed_company_name(job),
        "company_url": _indeed_company_url(job),
        "company_id": _indeed_company_id(job),
        "description": _indeed_description_text(job),
        "applications_count": "",
        "contract_type": _indeed_job_types_text(job),
        "experience_level": "",
        "work_type": "",
        "sector": "",
        "salary": "",
        "apply_url": apply_url,
        "apply_type": "EXTERNAL" if apply_url else "",
        "raw_json": json.dumps(job, ensure_ascii=False),
    }


def snake_case_keys(job: dict[str, Any]) -> dict[str, Any]:
    """Return a new dict with scraper keys mapped to canonical snake_case.

    Fields not in the mapping are preserved as-is with their original key.
    """
    if _is_indeed_job(job):
        return _normalize_indeed_job(job)
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
    return get_local_sqlite_path()



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
    project: str = str(config["gcp_project"])
    dataset: str = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)

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

    return data


