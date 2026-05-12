"""
@meta
type: test
scope: unit
domain: settings_store
covers:
  - sqlite-safe local fallback when bq client is absent
excludes:
  - live bigquery operations
tags:
  - fast
  - ci-safe
"""

from fitcv_cp import settings_store as ss


def test_local_settings_fallback_round_trip_without_bq():
    ss._LOCAL_SETTINGS.clear()

    ss.save_setting(
        "ai_score_top_n",
        20,
        updated_by="local",
        bq=None,
        project="local",
        dataset="local",
    )

    active = ss.load_active_settings(bq=None, project="local", dataset="local")

    assert active["ai_score_top_n"] == 20


def test_local_settings_group_save_without_bq():
    ss._LOCAL_SETTINGS.clear()

    ss.save_settings_group(
        {"vector_search_top_n": 25, "final_top_n": 10},
        updated_by="local",
        bq=None,
        project="local",
        dataset="local",
    )

    active = ss.load_active_settings(bq=None, project="local", dataset="local")

    assert active["vector_search_top_n"] == 25
    assert active["final_top_n"] == 10
