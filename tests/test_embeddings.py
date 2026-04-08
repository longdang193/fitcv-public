"""Tests for fitcv.embeddings — all pure unit tests (no cloud calls)."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from fitcv.embeddings import (
    FRESH_EMBEDDING_STATUS,
    REUSED_CACHED_EMBEDDING_STATUS,
    build_embedding_contract_fingerprint,
    build_candidate_chunks,
    build_job_summary_chunk,
    build_job_summary_signature_payload,
    build_job_summary_signature_record,
    build_job_summary_text,
)


# ── unit tests (no cloud calls) ───────────────────────────────────────────────

class TestBuildJobSummaryText:
    def test_uses_labelled_sections(self) -> None:
        """Summary text must use labelled field prefixes, not free concatenation."""
        jd = {
            "title": "Data Engineer",
            "seniority": "mid",
            "job_family": "data_engineering",
            "required_skills": ["SQL", "Python"],
            "responsibilities": ["Build pipelines"],
        }
        text = build_job_summary_text(jd)
        assert "Title:" in text
        assert "Data Engineer" in text
        assert "Required skills:" in text
        assert "SQL" in text
        assert "Seniority:" in text

    def test_includes_job_family(self) -> None:
        jd = {"title": "DE", "job_family": "data_engineering", "required_skills": [], "responsibilities": []}
        text = build_job_summary_text(jd)
        assert "Job family:" in text
        assert "data_engineering" in text

    def test_handles_empty_optional_fields(self) -> None:
        """Should not crash when optional fields are absent."""
        jd = {"title": "Data Engineer"}
        text = build_job_summary_text(jd)
        assert "Title:" in text
        assert "Data Engineer" in text

    def test_joins_skills_comma_separated(self) -> None:
        jd = {"title": "DE", "required_skills": ["SQL", "Python", "BigQuery"]}
        text = build_job_summary_text(jd)
        assert "Required skills:" in text
        assert "BigQuery" in text
        assert "Python" in text
        assert "SQL" in text

    def test_includes_location_type_when_present(self) -> None:
        jd = {"title": "DE", "location_type": "remote"}
        text = build_job_summary_text(jd)
        assert "Location type: remote" in text

    def test_omits_responsibilities_from_stable_shortlist_summary_contract(self) -> None:
        jd = {"title": "DE", "responsibilities": ["Build pipelines", "Write tests"]}
        text = build_job_summary_text(jd)
        assert "Responsibilities:" not in text


class TestBuildJobSummarySignaturePayload:
    def test_prefers_canonical_skill_lists_and_sorts_them(self) -> None:
        jd = {
            "title": "Senior Data Analyst",
            "location_type": "remote",
            "seniority": "senior",
            "job_family": "analytics",
            "required_skills": ["PowerBI", "SQL"],
            "required_skills_canonical": ["sql", "power bi", "sql"],
            "preferred_skills": ["dbt", "Airflow"],
            "preferred_skills_canonical": ["apache airflow", "dbt"],
        }

        payload = build_job_summary_signature_payload(jd)

        assert payload == {
            "title": "Senior Data Analyst",
            "location_type": "remote",
            "seniority": "senior",
            "job_family": "analytics",
            "required_skills": ["power bi", "sql"],
            "preferred_skills": ["apache airflow", "dbt"],
        }

    def test_falls_back_to_raw_skills_when_canonical_lists_missing(self) -> None:
        jd = {
            "title": "Data Engineer",
            "required_skills": ["Python", "SQL", "python"],
            "preferred_skills": ["dbt", "Airflow"],
        }

        payload = build_job_summary_signature_payload(jd)

        assert payload["required_skills"] == ["Python", "SQL"]
        assert payload["preferred_skills"] == ["Airflow", "dbt"]


class TestBuildJobSummarySignatureRecord:
    def test_signature_is_stable_when_skill_order_changes(self) -> None:
        first = {
            "title": "Data Engineer",
            "location_type": "remote",
            "required_skills_canonical": ["sql", "python"],
            "preferred_skills_canonical": ["dbt", "apache airflow"],
            "seniority": "mid",
            "job_family": "analytics",
        }
        second = {
            "title": "Data Engineer",
            "location_type": "remote",
            "required_skills_canonical": ["python", "sql"],
            "preferred_skills_canonical": ["apache airflow", "dbt"],
            "seniority": "mid",
            "job_family": "analytics",
        }

        assert build_job_summary_signature_record(first) == build_job_summary_signature_record(second)


class TestBuildEmbeddingContractFingerprint:
    def test_contract_changes_when_embedding_model_changes(self) -> None:
        first = build_embedding_contract_fingerprint({})
        second = build_embedding_contract_fingerprint({"shortlist_embedding_model": "text-embedding-004"})

        assert first["fingerprint"] != second["fingerprint"]


class TestBuildJobSummaryChunk:
    def test_returns_exactly_one_summary_chunk(self) -> None:
        """v1: must produce exactly one job_summary chunk for VECTOR_SEARCH ranking."""
        structured_jd = {
            "title": "Data Engineer",
            "responsibilities": ["Build pipelines"],
            "required_skills": ["SQL"],
        }
        chunks = build_job_summary_chunk(structured_jd)
        summary_chunks = [c for c in chunks if c["chunk_type"] == "job_summary"]
        assert len(summary_chunks) == 1

    def test_chunk_text_contains_title(self) -> None:
        chunks = build_job_summary_chunk({"title": "Data Engineer", "required_skills": []})
        assert "Data Engineer" in chunks[0]["chunk_text"]

    def test_chunk_has_correct_keys(self) -> None:
        chunks = build_job_summary_chunk({"title": "DE"})
        assert set(chunks[0].keys()) == {"chunk_type", "chunk_text"}


class TestBuildCandidateChunks:
    def test_one_chunk_per_project(self) -> None:
        profile = {
            "projects": [
                {"id": "proj_1", "name": "GA4 Pipeline", "skills": ["SQL"], "business_value": "analytics"},
                {"id": "proj_2", "name": "FitCV", "skills": ["Python"], "business_value": "AI matching"},
            ],
            "experiences": [],
            "achievements": [],
        }
        chunks = build_candidate_chunks(profile)
        proj_chunks = [c for c in chunks if c["evidence_type"] == "project"]
        assert len(proj_chunks) == 2

    def test_project_chunk_has_correct_shape(self) -> None:
        profile = {
            "projects": [{"id": "proj_1", "name": "GA4 Pipeline", "skills": ["SQL"], "business_value": "analytics"}],
            "experiences": [],
            "achievements": [],
        }
        chunks = build_candidate_chunks(profile)
        proj_chunk = next(c for c in chunks if c["evidence_type"] == "project")
        assert proj_chunk["evidence_id"] == "proj_1"
        assert proj_chunk["source_ref_id"] == "proj_1"
        assert "GA4 Pipeline" in proj_chunk["chunk_text"]
        # All 4 required keys must be present
        assert {"evidence_id", "source_ref_id", "evidence_type", "chunk_text"} <= set(proj_chunk.keys())

    def test_one_chunk_per_experience_bullet(self) -> None:
        profile = {
            "experiences": [{
                "id": "exp_1", "role": "Data Engineer", "company": "ACME",
                "bullets": [
                    {"text": "Built pipelines", "skills": ["SQL"]},
                    {"text": "Automated tests", "skills": ["Python"]},
                ],
            }],
            "projects": [],
            "achievements": [],
        }
        chunks = build_candidate_chunks(profile)
        bullet_chunks = [c for c in chunks if c["evidence_type"] == "experience_bullet"]
        assert len(bullet_chunks) == 2

    def test_experience_bullet_chunk_has_source_ref_id(self) -> None:
        profile = {
            "experiences": [{
                "id": "exp_1", "role": "Data Engineer", "company": "ACME",
                "bullets": [{"text": "Built pipelines", "skills": ["SQL"]}],
            }],
            "projects": [],
            "achievements": [],
        }
        chunks = build_candidate_chunks(profile)
        bullet = next(c for c in chunks if c["evidence_type"] == "experience_bullet")
        assert bullet["source_ref_id"] == "exp_1"  # must trace back to parent exp

    def test_one_chunk_per_achievement(self) -> None:
        profile = {
            "achievements": [
                {"id": "ach_1", "text": "Reduced latency by 40%", "category": "performance"},
                {"id": "ach_2", "text": "Reduced requests by 60%", "category": "productivity"},
            ],
            "projects": [],
            "experiences": [],
        }
        chunks = build_candidate_chunks(profile)
        ach_chunks = [c for c in chunks if c["evidence_type"] == "achievement"]
        assert len(ach_chunks) == 2

    def test_achievement_chunk_shape(self) -> None:
        profile = {
            "achievements": [{"id": "ach_1", "text": "Reduced latency by 40%", "category": "performance"}],
            "projects": [],
            "experiences": [],
        }
        chunks = build_candidate_chunks(profile)
        ach = next(c for c in chunks if c["evidence_type"] == "achievement")
        assert ach["evidence_id"] == "ach_1"
        assert ach["source_ref_id"] == "ach_1"
        assert "40%" in ach["chunk_text"]

    def test_empty_profile_returns_empty_list(self) -> None:
        profile: dict = {"projects": [], "experiences": [], "achievements": []}
        assert build_candidate_chunks(profile) == []


@patch("google.cloud.bigquery.Client")
@patch("google.oauth2.service_account.Credentials.from_service_account_file")
@patch("fitcv.embeddings.generate_embedding")
def test_embed_and_store_jobs_returns_zero_for_empty_batch(
    mock_generate_embedding: object,
    mock_from_service_account_file: object,
    mock_bigquery_client: object,
) -> None:
    from fitcv.embeddings import embed_and_store_jobs

    config = {
        "gcp_project": "fitcv-test",
        "bigquery_dataset": "fitcv",
        "service_account_key": "/tmp/fake.json",
    }

    inserted = embed_and_store_jobs([], config)

    assert inserted == 0
    mock_generate_embedding.assert_not_called()
    mock_from_service_account_file.assert_not_called()
    mock_bigquery_client.assert_not_called()


@patch("google.cloud.bigquery.Client")
@patch("google.oauth2.service_account.Credentials.from_service_account_file")
@patch("fitcv.embeddings.generate_embedding")
def test_embed_and_store_jobs_does_not_delete_existing_rows_before_insert(
    mock_generate_embedding: object,
    mock_from_service_account_file: object,
    mock_bigquery_client: object,
) -> None:
    from fitcv.embeddings import embed_and_store_jobs

    mock_generate_embedding.return_value = [0.1, 0.2]
    client = mock_bigquery_client.return_value
    client.insert_rows_json.return_value = []

    config = {
        "gcp_project": "fitcv-test",
        "bigquery_dataset": "fitcv",
        "service_account_key": "/tmp/fake.json",
    }
    jobs = [{"job_url": "https://example.com/1", "title": "DE", "required_skills": []}]

    inserted = embed_and_store_jobs(jobs, config)

    assert inserted == 1
    client.query.assert_called_once()
    assert "DELETE" not in client.query.call_args.args[0]
    client.insert_rows_json.assert_called_once()


@patch("google.cloud.bigquery.Client")
@patch("google.oauth2.service_account.Credentials.from_service_account_file")
@patch("fitcv.embeddings.generate_embedding")
def test_embed_and_store_jobs_reuses_matching_latest_embeddings_and_only_inserts_misses(
    mock_generate_embedding: object,
    mock_from_service_account_file: object,
    mock_bigquery_client: object,
) -> None:
    from fitcv.embeddings import embed_and_store_jobs

    client = mock_bigquery_client.return_value
    client.insert_rows_json.return_value = []

    config = {
        "gcp_project": "fitcv-test",
        "bigquery_dataset": "fitcv",
        "service_account_key": "/tmp/fake.json",
    }
    jobs = [
        {
            "job_url": "https://example.com/1",
            "title": "Data Engineer",
            "location_type": "remote",
            "required_skills_canonical": ["sql", "python"],
            "preferred_skills_canonical": ["dbt"],
            "seniority": "mid",
            "job_family": "analytics",
        },
        {
            "job_url": "https://example.com/2",
            "title": "Analytics Engineer",
            "location_type": "hybrid",
            "required_skills_canonical": ["sql", "dbt"],
            "preferred_skills_canonical": ["apache airflow"],
            "seniority": "mid",
            "job_family": "analytics",
        },
    ]

    reused_signature = build_job_summary_signature_record(jobs[0])
    contract = build_embedding_contract_fingerprint(config)
    client.query.return_value.result.return_value = [
        SimpleNamespace(
            job_url=jobs[0]["job_url"],
            embedding_input_signature=reused_signature["signature"],
            embedding_contract_fingerprint=contract["fingerprint"],
        ),
        SimpleNamespace(
            job_url=jobs[1]["job_url"],
            embedding_input_signature="stale-signature",
            embedding_contract_fingerprint=contract["fingerprint"],
        ),
    ]
    mock_generate_embedding.return_value = [0.1, 0.2]

    inserted = embed_and_store_jobs(jobs, config)

    assert inserted == 1
    mock_generate_embedding.assert_called_once()
    client.insert_rows_json.assert_called_once()
    inserted_rows = client.insert_rows_json.call_args.args[1]
    assert inserted_rows == [
        {
            "job_url": "https://example.com/2",
            "chunk_type": "job_summary",
            "chunk_text": build_job_summary_chunk(jobs[1])[0]["chunk_text"],
            "embedding": [0.1, 0.2],
            "created_at": inserted_rows[0]["created_at"],
            "embedding_input_signature": build_job_summary_signature_record(jobs[1])["signature"],
            "embedding_contract_fingerprint": contract["fingerprint"],
            "embedding_input_signature_payload_json": inserted_rows[0]["embedding_input_signature_payload_json"],
        }
    ]
    assert jobs[0]["embedding_reuse_status"] == REUSED_CACHED_EMBEDDING_STATUS
    assert jobs[0]["embedding_input_signature"] == reused_signature["signature"]
    assert jobs[0]["embedding_contract_fingerprint"] == contract["fingerprint"]
    assert jobs[1]["embedding_reuse_status"] == FRESH_EMBEDDING_STATUS
    assert jobs[1]["embedding_contract_fingerprint"] == contract["fingerprint"]


# ── integration tests (require GOOGLE_APPLICATION_CREDENTIALS) ────────────────

@pytest.mark.integration
def test_generate_embedding_returns_floats(config: dict) -> None:
    """Integration — calls Vertex AI text-embedding-005."""
    from fitcv.embeddings import generate_embedding
    result = generate_embedding("Data Engineer with SQL and Python skills", config)
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(v, float) for v in result)


@pytest.mark.integration
def test_embed_and_store_jobs_integration(config: dict) -> None:
    """Integration — embeds one job and inserts into job_embeddings."""
    from fitcv.embeddings import embed_and_store_jobs
    jobs = [{"job_url": "http://test.url", "title": "DE", "required_skills": ["SQL"], "responsibilities": [], "seniority": "mid", "job_family": "data_engineering"}]
    count = embed_and_store_jobs(jobs, config)
    assert count == 1
