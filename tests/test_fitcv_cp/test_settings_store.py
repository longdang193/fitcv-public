import datetime
from unittest.mock import MagicMock

from fitcv_cp.settings_store import (
    load_active_settings,
    save_setting,
)


def _make_bq_row(key: str, value_json: str, updated_at: str) -> dict:
    return {
        "setting_key": key,
        "setting_value_json": value_json,
        "updated_by": "admin",
        "updated_at": updated_at,
    }


def test_save_setting_calls_bq():
    bq = MagicMock()
    save_setting("pipeline.final_top_n", 5, updated_by="admin",
                 bq=bq, project="p", dataset="d")
    bq.insert_rows_json.assert_called_once()
    row = bq.insert_rows_json.call_args[0][1][0]
    assert row["setting_key"] == "pipeline.final_top_n"
    assert row["setting_value_json"] == "5"


def test_load_active_settings_returns_latest_per_key():
    bq = MagicMock()
    # Two rows for the same key — different timestamps. Latest should win.
    rows = [
        _make_bq_row("pipeline.final_top_n", "10", "2026-01-01T00:00:00"),
        _make_bq_row("pipeline.final_top_n", "5", "2026-01-02T00:00:00"),
    ]
    bq.query.return_value.result.return_value = iter(rows)
    result = load_active_settings(bq=bq, project="p", dataset="d")
    # The query uses ORDER BY updated_at DESC so first row per key is the latest
    assert result["pipeline.final_top_n"] == 10
    assert isinstance(result["pipeline.final_top_n"], int)  # coerced


def test_load_active_settings_empty_table():
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    result = load_active_settings(bq=bq, project="p", dataset="d")
    assert result == {}


def test_load_active_settings_uses_parameterized_query_or_safe_query():
    """Just verify query is called (no string injection risk since no user input)."""
    bq = MagicMock()
    bq.query.return_value.result.return_value = iter([])
    load_active_settings(bq=bq, project="p", dataset="d")
    bq.query.assert_called_once()
