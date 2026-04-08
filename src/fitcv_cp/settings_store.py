"""BigQuery persistence for pipeline_settings table.

All reads use a single query that returns the latest value per key (ORDER BY updated_at DESC).
No in-process caching — reads hit BQ each time. This is acceptable for an internal admin tool.
"""
import datetime
import json
import logging
from typing import Any

from fitcv_cp.settings_schema import coerce_value

logger = logging.getLogger(__name__)


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
    table = f"{project}.{dataset}.pipeline_settings"
    row = {
        "setting_key": key,
        "setting_value_json": json.dumps(value),
        "updated_by": updated_by,
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
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
    table = f"{project}.{dataset}.pipeline_settings"
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
    errors = bq.insert_rows_json(table, rows)
    if errors:
        logger.error("BQ save_settings_group errors: %s", errors)
        raise RuntimeError(f"Failed to save settings group: {errors}")


def load_active_settings(*, bq: Any, project: str, dataset: str) -> dict[str, Any]:
    """Return the current active settings dict (latest row per key, coerced to Python types).

    Returns an empty dict if no settings have been saved yet.
    """
    sql = (
        f"SELECT setting_key, setting_value_json "
        f"FROM `{project}.{dataset}.pipeline_settings` "
        f"ORDER BY updated_at DESC"
    )
    rows = list(bq.query(sql).result())

    seen: set[str] = set()
    result: dict[str, Any] = {}
    for row in rows:
        key = str(row["setting_key"])
        if key in seen:
            continue  # older value for same key — skip
        seen.add(key)
        raw = json.loads(str(row["setting_value_json"]))
        try:
            result[key] = coerce_value(key, raw)
        except (KeyError, ValueError) as exc:
            logger.warning("Skipping unknown/invalid setting key=%s: %s", key, exc)

    return result
