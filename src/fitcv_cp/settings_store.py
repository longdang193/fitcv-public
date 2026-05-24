"""@meta
name: settings_store
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.settings_store.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import datetime
import json
import logging
import os
import sqlite3
import shutil
from pathlib import Path
from typing import Any

from fitcv_cp.settings_schema import coerce_value, editable_settings_keys

logger = logging.getLogger(__name__)


def _local_sqlite_path() -> Path:
    raw = str(
        os.environ.get("FITCV_CP_SETTINGS_SQLITE_PATH")
        or os.environ.get("FITCV_CP_SQLITE_PATH")
        or "data/fitcv_cp.sqlite3"
    ).strip() or "data/fitcv_cp.sqlite3"
    return Path(raw)


def _ensure_local_pipeline_settings_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_settings (
            setting_key TEXT NOT NULL,
            setting_value_json TEXT NOT NULL,
            updated_by TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )

def _ensure_local_bookmarked_jobs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bookmarked_jobs (
            bookmark_key TEXT PRIMARY KEY,
            job_id TEXT,
            title TEXT NOT NULL,
            company TEXT,
            location TEXT,
            url TEXT NOT NULL,
            fit_classification TEXT,
            source_run_id TEXT,
            source TEXT,
            snapshot_json TEXT NOT NULL,
            saved_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            submitted_at TEXT,
            archived_at TEXT
        )
        """
    )
    existing_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(bookmarked_jobs)").fetchall()
        if row and len(row) > 1
    }
    missing_columns = {
        "status": "ALTER TABLE bookmarked_jobs ADD COLUMN status TEXT NOT NULL DEFAULT 'active'",
        "submitted_at": "ALTER TABLE bookmarked_jobs ADD COLUMN submitted_at TEXT",
        "archived_at": "ALTER TABLE bookmarked_jobs ADD COLUMN archived_at TEXT",
    }
    for column_name, ddl in missing_columns.items():
        if column_name in existing_columns:
            continue
        conn.execute(ddl)

def _is_recoverable_sqlite_error(exc: sqlite3.Error) -> bool:
    message = str(exc).lower()
    return (
        "disk i/o error" in message
        or "database is locked" in message
        or "file is not a database" in message
    )

def _rotate_local_sqlite_family(db_path: Path, *, reason: str) -> Path | None:
    if not db_path.exists():
        return None
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
    backup_dir = db_path.parent / f"{db_path.stem}.corrupt.{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    moved = False
    for suffix in ("", "-wal", "-shm"):
        source = Path(f"{db_path}{suffix}")
        if not source.exists():
            continue
        target = backup_dir / source.name
        try:
            shutil.move(str(source), str(target))
            moved = True
        except OSError as move_exc:
            logger.warning("Failed to rotate sqlite file %s: %s", source, move_exc)
    if moved:
        logger.warning(
            "Rotated local settings sqlite files due to recoverable sqlite failure (%s). backup_dir=%s",
            reason,
            backup_dir,
        )
        return backup_dir
    return None


def _save_local_settings_rows(rows: list[dict[str, str]]) -> None:
    db_path = _local_sqlite_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in (1, 2):
        try:
            with sqlite3.connect(db_path, timeout=30) as conn:
                _ensure_local_pipeline_settings_table(conn)
                conn.executemany(
                    """
                    INSERT INTO pipeline_settings (
                        setting_key,
                        setting_value_json,
                        updated_by,
                        updated_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    [
                        (
                            str(row["setting_key"]),
                            str(row["setting_value_json"]),
                            str(row.get("updated_by") or ""),
                            str(row["updated_at"]),
                        )
                        for row in rows
                    ],
                )
                conn.commit()
                return
        except sqlite3.Error as exc:
            if attempt == 1 and _is_recoverable_sqlite_error(exc):
                _rotate_local_sqlite_family(db_path, reason=str(exc))
                continue
            raise


def _load_local_settings_rows() -> list[sqlite3.Row]:
    db_path = _local_sqlite_path()
    if not db_path.exists():
        return []
    for attempt in (1, 2):
        try:
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.row_factory = sqlite3.Row
                _ensure_local_pipeline_settings_table(conn)
                rows = conn.execute(
                    """
                    SELECT setting_key, setting_value_json
                    FROM pipeline_settings
                    ORDER BY updated_at DESC, rowid DESC
                    """
                ).fetchall()
            return rows
        except sqlite3.Error as exc:
            if attempt == 1 and _is_recoverable_sqlite_error(exc):
                _rotate_local_sqlite_family(db_path, reason=str(exc))
                return []
            raise
    return []


def _normalize_bookmark_key(
    *,
    job_id: str | None,
    url: str | None,
    title: str | None,
    company: str | None,
    location: str | None,
) -> str:
    if job_id and str(job_id).strip():
        return f"job_id:{str(job_id).strip()}"
    if url and str(url).strip():
        return f"url:{str(url).strip()}"
    fallback_parts = [
        str(title or "").strip().lower(),
        str(company or "").strip().lower(),
        str(location or "").strip().lower(),
    ]
    fallback = "|".join(fallback_parts)
    if fallback.strip("|"):
        return f"fallback:{fallback}"
    raise ValueError("bookmark identity requires job_id, url, or title/company/location fallback")


def bookmark_key_for_job(
    *,
    job_id: str | None,
    url: str | None,
    title: str | None,
    company: str | None = None,
    location: str | None = None,
) -> str:
    return _normalize_bookmark_key(
        job_id=job_id,
        url=url,
        title=title,
        company=company,
        location=location,
    )


def upsert_bookmarked_job(
    *,
    job_id: str | None,
    title: str,
    url: str,
    company: str | None = None,
    location: str | None = None,
    fit_classification: str | None = None,
    source_run_id: str | None = None,
    source: str | None = None,
    snapshot: dict[str, Any] | None = None,
    saved_at: str | None = None,
) -> str:
    bookmark_key = _normalize_bookmark_key(
        job_id=job_id,
        url=url,
        title=title,
        company=company,
        location=location,
    )
    timestamp = saved_at or datetime.datetime.now(datetime.timezone.utc).isoformat()
    snapshot_payload = {
        "bookmark_key": bookmark_key,
        "job_id": job_id,
        "title": title,
        "company": company,
        "location": location,
        "url": url,
        "fit_classification": fit_classification,
        "source_run_id": source_run_id,
        "source": source,
        "saved_at": timestamp,
    }
    if snapshot:
        snapshot_payload.update(snapshot)
    db_path = _local_sqlite_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_bookmarked_jobs_table(conn)
        conn.execute(
            """
            INSERT INTO bookmarked_jobs (
                bookmark_key,
                job_id,
                title,
                company,
                location,
                url,
                fit_classification,
                source_run_id,
                source,
                snapshot_json,
                saved_at,
                status,
                submitted_at,
                archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bookmark_key) DO UPDATE SET
                job_id=excluded.job_id,
                title=excluded.title,
                company=excluded.company,
                location=excluded.location,
                url=excluded.url,
                fit_classification=excluded.fit_classification,
                source_run_id=excluded.source_run_id,
                source=excluded.source,
                snapshot_json=excluded.snapshot_json,
                saved_at=excluded.saved_at
            """,
            (
                bookmark_key,
                str(job_id).strip() if job_id else None,
                str(title).strip(),
                str(company).strip() if company else None,
                str(location).strip() if location else None,
                str(url).strip(),
                str(fit_classification).strip() if fit_classification else None,
                str(source_run_id).strip() if source_run_id else None,
                str(source).strip() if source else None,
                json.dumps(snapshot_payload),
                timestamp,
                "active",
                None,
                None,
            ),
        )
        conn.commit()
    return bookmark_key


def delete_bookmarked_job(bookmark_key: str) -> bool:
    db_path = _local_sqlite_path()
    if not db_path.exists():
        return False
    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_bookmarked_jobs_table(conn)
        cursor = conn.execute(
            "DELETE FROM bookmarked_jobs WHERE bookmark_key = ?",
            (str(bookmark_key).strip(),),
        )
        conn.commit()
        return int(cursor.rowcount or 0) > 0


def set_bookmarked_job_status(
    bookmark_key: str,
    status: str,
    *,
    at: str | None = None,
) -> bool:
    normalized_key = str(bookmark_key).strip()
    normalized_status = str(status).strip().lower()
    allowed = {"active", "submitted", "archived"}
    if normalized_status not in allowed:
        raise ValueError(f"Unsupported bookmark status: {normalized_status}")

    db_path = _local_sqlite_path()
    if not db_path.exists():
        return False

    timestamp = at or datetime.datetime.now(datetime.timezone.utc).isoformat()
    submitted_at: str | None = None
    archived_at: str | None = None
    if normalized_status == "submitted":
        submitted_at = timestamp
    elif normalized_status == "archived":
        archived_at = timestamp

    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_bookmarked_jobs_table(conn)
        cursor = conn.execute(
            """
            UPDATE bookmarked_jobs
            SET
                status = ?,
                submitted_at = ?,
                archived_at = ?
            WHERE bookmark_key = ?
            """,
            (
                normalized_status,
                submitted_at,
                archived_at,
                normalized_key,
            ),
        )
        conn.commit()
        return int(cursor.rowcount or 0) > 0


def list_bookmarked_jobs() -> list[dict[str, Any]]:
    db_path = _local_sqlite_path()
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        _ensure_local_bookmarked_jobs_table(conn)
        rows = conn.execute(
            """
            SELECT
                bookmark_key,
                job_id,
                title,
                company,
                location,
                url,
                fit_classification,
                source_run_id,
                source,
                snapshot_json,
                saved_at,
                status,
                submitted_at,
                archived_at
            FROM bookmarked_jobs
            ORDER BY saved_at DESC, bookmark_key ASC
            """
        ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        snapshot_raw = row["snapshot_json"]
        snapshot = {}
        if snapshot_raw:
            try:
                snapshot = json.loads(str(snapshot_raw))
            except (TypeError, json.JSONDecodeError):
                snapshot = {}
        result.append(
            {
                "bookmark_key": str(row["bookmark_key"]),
                "job_id": str(row["job_id"]) if row["job_id"] is not None else None,
                "title": str(row["title"]),
                "company": str(row["company"]) if row["company"] is not None else None,
                "location": str(row["location"]) if row["location"] is not None else None,
                "url": str(row["url"]),
                "fit_classification": (
                    str(row["fit_classification"]) if row["fit_classification"] is not None else None
                ),
                "source_run_id": str(row["source_run_id"]) if row["source_run_id"] is not None else None,
                "source": str(row["source"]) if row["source"] is not None else None,
                "saved_at": str(row["saved_at"]),
                "status": str(row["status"]) if row["status"] is not None else "active",
                "submitted_at": (
                    str(row["submitted_at"]) if row["submitted_at"] is not None else None
                ),
                "archived_at": str(row["archived_at"]) if row["archived_at"] is not None else None,
                "snapshot": snapshot,
            }
        )
    return result


def is_job_bookmarked(
    *,
    job_id: str | None,
    url: str | None,
    title: str | None,
    company: str | None = None,
    location: str | None = None,
) -> bool:
    bookmark_key = _normalize_bookmark_key(
        job_id=job_id,
        url=url,
        title=title,
        company=company,
        location=location,
    )
    db_path = _local_sqlite_path()
    if not db_path.exists():
        return False
    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_bookmarked_jobs_table(conn)
        row = conn.execute(
            "SELECT 1 FROM bookmarked_jobs WHERE bookmark_key = ? LIMIT 1",
            (bookmark_key,),
        ).fetchone()
    return row is not None


def save_setting(
    key: str,
    value: Any,
    *,
    updated_by: str,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    """Append a new row for this key. Current value = latest row per key."""
    row = {
        "setting_key": key,
        "setting_value_json": json.dumps(value),
        "updated_by": updated_by,
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    if bq is None:
        _save_local_settings_rows([row])
        return
    table = f"{project}.{dataset}.pipeline_settings"
    errors = bq.insert_rows_json(table, [row])
    if errors:
        logger.error("BQ save_setting errors: %s", errors)


def save_settings_group(
    keys_values: dict[str, Any],
    *,
    updated_by: str,
    bq: Any,
    project: str,
    dataset: str,
) -> None:
    """Write all keys in the group with a shared updated_at timestamp.

    All rows are submitted in a single insert_rows_json batch call.
    Raises RuntimeError if BigQuery rejects the batch, so callers can surface
    the failure to the user rather than silently reporting success.

    WARNING: BigQuery streaming inserts are not transactional. Validation must
    always be completed before calling this function. Partial writes on BQ-level
    partial failures are possible but accepted for this admin tool.
    """
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows = [
        {
            "setting_key": key,
            "setting_value_json": json.dumps(value),
            "updated_by": updated_by,
            "updated_at": now,
        }
        for key, value in keys_values.items()
    ]
    if bq is None:
        _save_local_settings_rows(rows)
        return
    table = f"{project}.{dataset}.pipeline_settings"
    errors = bq.insert_rows_json(table, rows)
    if errors:
        logger.error("BQ save_settings_group errors: %s", errors)
        raise RuntimeError(f"Failed to save settings group: {errors}")


def load_active_settings(*, bq: Any, project: str, dataset: str) -> dict[str, Any]:
    """Return the current active settings dict (latest row per key, coerced to Python types).

    Returns an empty dict if no settings have been saved yet.
    """
    if bq is None:
        rows = _load_local_settings_rows()
    else:
        sql = (
            f"SELECT setting_key, setting_value_json "
            f"FROM `{project}.{dataset}.pipeline_settings` "
            f"ORDER BY updated_at DESC"
        )
        rows = list(bq.query(sql).result())

    seen_valid: set[str] = set()
    result: dict[str, Any] = {}
    for row in rows:
        key = str(row["setting_key"])
        if key in seen_valid:
            continue  # older value for same key — skip
        raw = json.loads(str(row["setting_value_json"]))
        try:
            result[key] = coerce_value(key, raw)
            seen_valid.add(key)
        except (KeyError, ValueError) as exc:
            logger.warning("Skipping unknown/invalid setting key=%s: %s", key, exc)

    return result


def load_active_editable_settings(*, bq: Any, project: str, dataset: str) -> dict[str, Any]:
    """Return only schema-backed editable settings from the active settings snapshot."""
    active_settings = load_active_settings(bq=bq, project=project, dataset=dataset)
    editable_keys = editable_settings_keys()
    return {
        key: value
        for key, value in active_settings.items()
        if key in editable_keys
    }
