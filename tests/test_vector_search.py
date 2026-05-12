"""Tests for fitcv.vector_search — all pure unit tests (no cloud calls)."""

import pytest
from unittest.mock import patch

from fitcv.vector_search import (
    _dedupe_shortlist_rows,
    build_candidate_query_components,
    build_candidate_query_embedding_contract_fingerprint,
    build_candidate_query_signature_record,
    build_candidate_query_text,
    build_vector_search_query,
    resolve_candidate_query_embedding,
    run_vector_search,
)


# ── build_candidate_query_text ────────────────────────────────────────────────

def test_build_candidate_query_text_includes_headline() -> None:
    profile = {
        "headline": "Data Engineer",
        "skills": [{"name": "SQL"}, {"name": "Python"}],
        "preferences": {"domains": ["data_engineering"]},
    }
    text = build_candidate_query_text(profile)
    assert "Data Engineer" in text


def test_build_candidate_query_text_includes_skills() -> None:
    profile = {
        "headline": "Data Engineer",
        "skills": [{"name": "SQL"}, {"name": "Python"}, {"name": "BigQuery"}],
        "preferences": {"domains": []},
    }
    text = build_candidate_query_text(profile)
    assert "SQL" in text
    assert "Python" in text


def test_build_candidate_query_text_uses_flattened_skills_from_experiences_and_projects() -> None:
    profile = {
        "headline": "Data Analyst",
        "skills": [{"name": "SQL"}],
        "experiences": [
            {
                "role": "BI Analyst",
                "bullets": [
                    {"text": "Built dashboards", "skills": ["Power BI", "Looker"]},
                ],
            }
        ],
        "projects": [
            {"name": "Warehouse Migration", "skills": ["BigQuery", "dbt"]},
        ],
        "preferences": {"domains": []},
    }

    text = build_candidate_query_text(profile, {"vector_max_candidate_skills": 10})

    assert "Power BI" in text
    assert "Looker" in text
    assert "BigQuery" in text
    assert "dbt" in text


def test_build_candidate_query_components_include_role_family_and_domain_hints() -> None:
    profile = {
        "headline": "Data Analyst",
        "skills": [{"name": "SQL"}],
        "preferences": {
            "target_role": "Data Analyst",
            "domains": ["banking"],
            "role_families": ["analytics"],
        },
        "experiences": [
            {
                "role": "Business Intelligence Analyst",
                "role_family": "analytics",
                "domain_tags": ["retail_banking"],
            },
            {
                "role": "Data Scientist",
                "domain_tags": ["fraud_detection"],
            },
        ],
        "projects": [
            {"name": "Fraud Dashboard", "domain_tags": ["fintech"]},
        ],
    }

    components = build_candidate_query_components(profile, {"vector_max_candidate_skills": 10})

    assert components["target_role"] == "Data Analyst"
    assert components["recent_roles"] == ["Business Intelligence Analyst", "Data Scientist"]
    assert components["role_family_hints"] == ["analytics", "data_science"]
    assert components["domain_hints"] == ["banking", "retail_banking", "fraud_detection", "fintech"]


def test_build_candidate_query_components_bound_skill_count() -> None:
    profile = {
        "headline": "Data Engineer",
        "skills": [{"name": "SQL"}, {"name": "Python"}],
        "experiences": [
            {
                "role": "Data Engineer",
                "bullets": [
                    {"skills": ["BigQuery", "dbt", "Airflow"]},
                ],
            }
        ],
        "projects": [{"skills": ["Looker", "Power BI"]}],
        "preferences": {"domains": []},
    }

    components = build_candidate_query_components(profile, {"vector_max_candidate_skills": 3})

    assert components["flattened_skills"] == ["SQL", "Python", "BigQuery"]


def test_build_candidate_query_text_includes_preferred_domains() -> None:
    profile = {
        "headline": "DE",
        "skills": [],
        "preferences": {"domains": ["data_engineering", "analytics"]},
    }
    text = build_candidate_query_text(profile)
    assert "data_engineering" in text or "analytics" in text


def test_build_candidate_query_text_handles_missing_fields() -> None:
    """Should not crash when optional fields are absent."""
    profile: dict = {}
    text = build_candidate_query_text(profile)
    assert isinstance(text, str)


def test_build_candidate_query_text_is_deterministic() -> None:
    """Same profile always produces the same query text."""
    profile = {
        "headline": "Data Engineer",
        "skills": [{"name": "SQL"}, {"name": "dbt"}],
        "preferences": {"domains": ["analytics"]},
    }
    assert build_candidate_query_text(profile) == build_candidate_query_text(profile)


def test_build_candidate_query_text_includes_target_role() -> None:
    profile = {
        "headline": "Senior Data Engineer",
        "skills": [{"name": "SQL"}],
        "preferences": {"target_role": "Data Analyst", "domains": ["analytics"]},
    }
    text = build_candidate_query_text(profile)
    assert "Target role: Data Analyst" in text


def test_build_candidate_query_text_includes_recent_roles() -> None:
    profile = {
        "headline": "Senior Data Engineer",
        "skills": [{"name": "SQL"}],
        "experiences": [
            {"role": "Senior Data Engineer"},
            {"role": "Data Engineer"},
            {"role": "Junior Data Analyst"},
        ],
        "preferences": {"domains": ["analytics"]},
    }
    text = build_candidate_query_text(profile)
    assert "Recent roles:" in text
    assert "Junior Data Analyst" in text


def test_build_candidate_query_text_includes_role_family_and_domain_hints() -> None:
    profile = {
        "headline": "Data Analyst",
        "skills": [{"name": "SQL"}],
        "experiences": [
            {
                "role": "Business Intelligence Analyst",
                "role_family": "analytics",
                "domain_tags": ["retail_banking"],
            }
        ],
        "projects": [{"name": "BI Project", "domain_tags": ["fintech"]}],
        "preferences": {
            "target_role": "Data Analyst",
            "domains": ["banking"],
            "role_families": ["analytics"],
        },
    }

    text = build_candidate_query_text(profile)

    assert "Role families: analytics" in text
    assert "Domain hints: banking, retail_banking, fintech" in text


def test_build_candidate_query_signature_record_is_stable_for_same_effective_components() -> None:
    first = {
        "headline": "Data Analyst",
        "target_role": "Data Analyst",
        "recent_roles": ["BI Analyst", "Data Analyst"],
        "role_family_hints": ["analytics"],
        "flattened_skills": ["SQL", "Python", "Power BI"],
        "domain_hints": ["banking", "retail_banking"],
    }
    second = {
        "headline": "Data Analyst",
        "target_role": "Data Analyst",
        "recent_roles": ["BI Analyst", "Data Analyst"],
        "role_family_hints": ["analytics"],
        "flattened_skills": ["SQL", "Python", "Power BI"],
        "domain_hints": ["banking", "retail_banking"],
    }

    assert build_candidate_query_signature_record(first) == build_candidate_query_signature_record(second)


def test_build_candidate_query_embedding_contract_fingerprint_changes_with_model() -> None:
    first = build_candidate_query_embedding_contract_fingerprint({})
    second = build_candidate_query_embedding_contract_fingerprint(
        {"shortlist_embedding_model": "text-embedding-004"}
    )

    assert first["fingerprint"] != second["fingerprint"]


@pytest.mark.parametrize(
    ("cached_signature", "cached_contract", "expected_status", "should_generate"),
    [
        ("matching", "matching", "reused_cached_query_embedding", False),
        ("matching", "stale", "fresh_query_embedding", True),
    ],
)
def test_resolve_candidate_query_embedding_reuses_or_refreshes_cache(
    cached_signature: str,
    cached_contract: str,
    expected_status: str,
    should_generate: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")
    profile = {
        "headline": "Data Analyst",
        "skills": [{"name": "SQL"}, {"name": "Python"}],
        "preferences": {"target_role": "Data Analyst", "domains": ["banking"]},
    }
    config = {
        "gcp_project": "fitcv-test",
        "bigquery_dataset": "fitcv",
        "service_account_key": "/tmp/fake.json",
    }

    expected_text = build_candidate_query_text(profile, config)
    expected_components = build_candidate_query_components(profile, config)
    expected_signature = build_candidate_query_signature_record(expected_components)["signature"]
    expected_contract = build_candidate_query_embedding_contract_fingerprint(config)["fingerprint"]

    row_signature = expected_signature if cached_signature == "matching" else "stale-signature"
    row_contract = expected_contract if cached_contract == "matching" else "stale-contract"

    with (
        patch("google.cloud.bigquery.Client") as mock_bigquery_client,
        patch("google.oauth2.service_account.Credentials.from_service_account_file"),
        patch("fitcv.vector_search.generate_embedding") as mock_generate_embedding,
    ):
        client = mock_bigquery_client.return_value
        client.query.return_value.result.return_value = [
            type(
                "Row",
                (),
                {
                    "candidate_query_signature": row_signature,
                    "candidate_query_contract_fingerprint": row_contract,
                    "candidate_query_text": expected_text,
                    "candidate_query_components_json": "{}",
                    "embedding": [0.11, 0.22],
                },
            )()
        ]
        client.insert_rows_json.return_value = []
        mock_generate_embedding.return_value = [0.33, 0.44]

        record = resolve_candidate_query_embedding(profile, config)

    assert record["text"] == expected_text
    assert record["components"] == expected_components
    assert record["candidate_query_signature"] == expected_signature
    assert record["candidate_query_contract_fingerprint"] == expected_contract
    assert record["candidate_query_reuse_status"] == expected_status
    if should_generate:
        mock_generate_embedding.assert_called_once_with(expected_text, config)
        client.insert_rows_json.assert_called_once()
    else:
        mock_generate_embedding.assert_not_called()
        client.insert_rows_json.assert_not_called()


# ── build_vector_search_query ─────────────────────────────────────────────────

def test_build_vector_search_query_contains_vector_search() -> None:
    query = build_vector_search_query(top_n=50, passed_job_urls=["url1"])
    assert "VECTOR_SEARCH" in query


def test_build_vector_search_query_targets_job_summary_chunk() -> None:
    """Query must filter to chunk_type = 'job_summary' only."""
    query = build_vector_search_query(top_n=50, passed_job_urls=["url1", "url2"])
    assert "job_summary" in query


def test_build_vector_search_query_enforces_top_n() -> None:
    """top_n must appear in the query."""
    query = build_vector_search_query(top_n=50, passed_job_urls=["url1"])
    assert "50" in query


def test_build_vector_search_query_filters_passed_universe() -> None:
    """Query must restrict to the rule-filtered job universe."""
    query = build_vector_search_query(top_n=50, passed_job_urls=["url1", "url2"])
    # Must embed the passed URLs directly or reference a filtered subquery
    assert "url1" in query or "rule_filter_results" in query or "passed" in query.lower()


def test_build_vector_search_query_filters_job_universe_inside_vector_search() -> None:
    """Universe restriction must happen inside VECTOR_SEARCH, not only afterward."""
    query = build_vector_search_query(top_n=50, passed_job_urls=["url1", "url2"])
    assert "CREATE TEMP TABLE _latest_job_embeddings AS" in query
    assert "VECTOR_SEARCH(\n    TABLE _latest_job_embeddings" in query
    assert "chunk_type = 'job_summary' AND job_url IN ('url1', 'url2')" in query


def test_build_vector_search_query_materializes_latest_rows_before_vector_search() -> None:
    query = build_vector_search_query(top_n=50, passed_job_urls=["url1", "url2"])

    assert "ROW_NUMBER() OVER (" in query
    assert "PARTITION BY job_url" in query
    assert "ORDER BY created_at DESC" in query
    assert "WHERE rn = 1" in query


def test_build_vector_search_query_outputs_job_url() -> None:
    query = build_vector_search_query(top_n=50, passed_job_urls=["url1"])
    assert "job_url" in query


def test_build_vector_search_query_references_job_embeddings_table() -> None:
    query = build_vector_search_query(top_n=50, passed_job_urls=["url1"])
    assert "job_embeddings" in query


def test_build_vector_search_query_empty_passed_urls() -> None:
    """Empty passed_job_urls → query should still be a valid SQL string."""
    query = build_vector_search_query(top_n=50, passed_job_urls=[])
    assert isinstance(query, str)
    assert "VECTOR_SEARCH" in query


def test_dedupe_shortlist_rows_keeps_best_rank_per_job_url() -> None:
    rows = [
        {"job_url": "https://example.com/1", "vector_similarity": 0.9, "vector_rank": 1},
        {"job_url": "https://example.com/1", "vector_similarity": 0.8, "vector_rank": 2},
        {"job_url": "https://example.com/2", "vector_similarity": 0.7, "vector_rank": 3},
    ]

    deduped = _dedupe_shortlist_rows(rows)

    assert deduped == [
        {"job_url": "https://example.com/1", "vector_similarity": 0.9, "vector_rank": 1},
        {"job_url": "https://example.com/2", "vector_similarity": 0.7, "vector_rank": 3},
    ]


# ── integration tests ─────────────────────────────────────────────────────────

@pytest.mark.integration
def test_run_vector_search_returns_shortlist(config: dict, sample_profile_path) -> None:
    """Integration — runs VECTOR_SEARCH against real BQ and returns ranked rows."""
    from pathlib import Path
    from fitcv.candidate import load_profile_yaml
    from fitcv.rule_filter import apply_rule_filters
    from fitcv.vector_search import run_vector_search

    profile = load_profile_yaml(sample_profile_path)
    # Use empty passed_job_urls to test graceful short-circuit
    result = run_vector_search(profile, passed_job_urls=[], config=config, top_n=10)
    assert isinstance(result, list)


def test_run_vector_search_prefers_nested_pipeline_top_n_over_legacy_flat_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")
    profile = {
        "headline": "Data Analyst",
        "skills": [{"name": "SQL"}],
        "preferences": {"domains": ["banking"]},
    }

    with (
        patch("google.cloud.bigquery.Client") as mock_bigquery_client,
        patch("google.oauth2.service_account.Credentials.from_service_account_file"),
        patch("fitcv.vector_search.resolve_candidate_query_embedding") as mock_resolve_embedding,
    ):
        mock_resolve_embedding.return_value = {
            "embedding": [0.1, 0.2],
            "candidate_query_signature": "sig",
            "candidate_query_contract_fingerprint": "contract",
            "candidate_query_reuse_status": "fresh_query_embedding",
            "text": "query",
            "components": {},
        }
        client = mock_bigquery_client.return_value
        client.query.return_value.result.return_value = []

        run_vector_search(
            profile=profile,
            passed_job_urls=["https://example.com/job-1"],
            config={
                "gcp_project": "fitcv-test",
                "bigquery_dataset": "fitcv",
                "service_account_key": "/tmp/fake.json",
                "pipeline": {"vector_search_top_n": 7},
                "vector_top_n": 99,
            },
        )

    rendered_query = client.query.call_args.args[0]
    assert "top_k => 7" in rendered_query
    assert "top_k => 99" not in rendered_query
"""
@meta
type: test
scope: unit
domain: shortlist
covers:
  - vector search behavior
excludes:
  - external vector services
tags:
  - fast
  - ci-safe
"""
