"""Tests for fitcv.ai_score — all pure unit tests (no cloud calls)."""

import json
import sqlite3
import sys
import time
import types
from pathlib import Path
from unittest.mock import patch

import pytest

from fitcv.ai_score import build_scoring_prompt, parse_score_response


# ── helpers ───────────────────────────────────────────────────────────────────

_VALID_RESPONSE = json.dumps({
    "ai_score": 0.85,
    "fit_label": "strong",
    "score_reasoning": "Candidate has SQL, Python, and BigQuery experience matching JD.",
    "matched_strengths": ["SQL", "Python", "BigQuery"],
    "key_risks": [],
})


# ── build_scoring_prompt ──────────────────────────────────────────────────────

def test_build_scoring_prompt_includes_jd_summary() -> None:
    prompt = build_scoring_prompt(
        jd_summary="Data Engineer role requiring SQL, Python",
        candidate_summary="3 years experience in SQL, Python, BigQuery",
        top_evidence=["Built GA4 pipeline reducing latency 40%"],
    )
    assert "Data Engineer" in prompt
    assert "SQL" in prompt


def test_build_scoring_prompt_includes_candidate_summary() -> None:
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="3 years experience in SQL, Python, BigQuery",
        top_evidence=[],
    )
    assert "BigQuery" in prompt or "3 years" in prompt


def test_build_scoring_prompt_includes_score_in_rubric() -> None:
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="candidate",
        top_evidence=[],
    )
    assert "score" in prompt.lower()


def test_build_scoring_prompt_includes_rubric_range() -> None:
    """Rubric range 0.0-1.0 must appear in prompt."""
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="candidate",
        top_evidence=[],
    )
    assert "0.0" in prompt or "1.0" in prompt


def test_build_scoring_prompt_includes_fit_labels() -> None:
    """strong / stretch / skip must be present so model knows the classification."""
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="candidate",
        top_evidence=[],
    )
    assert "strong" in prompt.lower()
    assert "stretch" in prompt.lower()
    assert "skip" in prompt.lower()


def test_build_scoring_prompt_includes_top_evidence() -> None:
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="candidate",
        top_evidence=["Built GA4 pipeline reducing latency 40%"],
    )
    assert "GA4" in prompt


def test_build_scoring_prompt_contains_required_skills_in_rubric() -> None:
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="mid-level engineer",
        top_evidence=[],
    )
    assert "required skills" in prompt.lower() or "required_skills" in prompt


def test_build_scoring_prompt_makes_preferences_secondary() -> None:
    prompt = build_scoring_prompt(
        jd_summary="Analytics role",
        candidate_summary="candidate",
        top_evidence=[],
    )
    assert "secondary" in prompt.lower()
    assert "preferences" in prompt.lower()


def test_build_scoring_prompt_contains_seniority_in_rubric() -> None:
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="mid-level engineer",
        top_evidence=[],
    )
    assert "seniority" in prompt.lower()


def test_build_scoring_prompt_specifies_json_output() -> None:
    """Prompt must tell model to return JSON only, no prose."""
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="candidate",
        top_evidence=[],
    )
    assert "json" in prompt.lower()


def test_build_scoring_prompt_uses_configured_fit_thresholds() -> None:
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="candidate",
        top_evidence=[],
        strong_threshold=0.8,
        stretch_threshold=0.55,
    )
    assert "0.8" in prompt
    assert "0.55" in prompt


def test_build_scoring_prompt_empty_evidence_does_not_crash() -> None:
    prompt = build_scoring_prompt(
        jd_summary="DE role",
        candidate_summary="candidate",
        top_evidence=[],
    )
    assert isinstance(prompt, str)
    assert len(prompt) > 0


# ── parse_score_response ──────────────────────────────────────────────────────

def test_parse_score_response_valid_json() -> None:
    result = parse_score_response(_VALID_RESPONSE)
    assert result["ai_score"] == 0.85
    assert result["fit_label"] == "strong"
    assert result["matched_strengths"] == ["SQL", "Python", "BigQuery"]
    assert isinstance(result["key_risks"], list)


def test_parse_score_response_returns_all_required_keys() -> None:
    result = parse_score_response(_VALID_RESPONSE)
    assert {"ai_score", "fit_label", "score_reasoning", "matched_strengths", "key_risks"} <= set(result.keys())


def test_parse_score_response_score_clamped_below_upper_bound() -> None:
    raw = json.dumps({
        "ai_score": 1.5, "fit_label": "strong",
        "score_reasoning": "", "matched_strengths": [], "key_risks": [],
    })
    result = parse_score_response(raw)
    assert result["ai_score"] <= 1.0


def test_parse_score_response_score_clamped_above_lower_bound() -> None:
    raw = json.dumps({
        "ai_score": -0.5, "fit_label": "skip",
        "score_reasoning": "", "matched_strengths": [], "key_risks": [],
    })
    result = parse_score_response(raw)
    assert result["ai_score"] >= 0.0


def test_parse_score_response_bad_fit_label_mapped_to_skip() -> None:
    raw = json.dumps({
        "ai_score": 0.3, "fit_label": "maybe",
        "score_reasoning": "", "matched_strengths": [], "key_risks": [],
    })
    result = parse_score_response(raw)
    assert result["fit_label"] in ("strong", "stretch", "skip")


def test_parse_score_response_malformed_json_returns_defaults() -> None:
    result = parse_score_response("not json at all")
    assert result["ai_score"] == 0.0
    assert result["fit_label"] == "skip"
    assert result["score_reasoning"] == "Scoring response parse failure: malformed_json"
    assert result["parser_status"] == "malformed_json"
    assert result["matched_strengths"] == []
    assert result["key_risks"] == []


def test_parse_score_response_markdown_fenced_json() -> None:
    """Model sometimes wraps response in ```json ... ``` fences."""
    raw = '```json\n{"ai_score": 0.75, "fit_label": "strong", "score_reasoning": "good", "matched_strengths": [], "key_risks": []}\n```'
    result = parse_score_response(raw)
    assert result["ai_score"] == 0.75
    assert result["fit_label"] == "strong"


def test_parse_score_response_fit_label_derived_from_score_if_missing() -> None:
    """If fit_label missing, derive from ai_score thresholds."""
    raw = json.dumps({
        "ai_score": 0.8,
        "score_reasoning": "good match",
        "matched_strengths": ["SQL"],
        "key_risks": [],
    })
    result = parse_score_response(raw)
    assert result["fit_label"] == "strong"


def test_make_genai_client_requires_supported_routing_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.ai_score import _make_genai_client

    monkeypatch.setattr(
        "fitcv.ai_score.resolve_model_routing_part",
        lambda part, model_fallback=None: {
            "provider": "",
            "model": "",
            "base_url": "",
        },
    )
    with pytest.raises(RuntimeError, match="ranking_ai_score provider must be configured"):
        _make_genai_client({"gemini_model": "gemini-2.5-flash"})


def test_make_genai_client_openai_compatible_requires_env_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.ai_score import _make_genai_client

    monkeypatch.setattr(
        "fitcv.ai_score.resolve_model_routing_part",
        lambda part, model_fallback=None: {
            "provider": "openai_compatible",
            "model": "cx/gpt-5.2",
            "base_url": "http://localhost:20128/v1",
        },
    )
    monkeypatch.delenv("FITCV_LANGGRAPH_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_COMPATIBLE_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="requires API key in env"):
        _make_genai_client({"gemini_model": "gemini-2.5-flash"})


def test_make_genai_client_openai_compatible_falls_back_to_chat_completions_on_responses_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.ai_score import _make_genai_client

    class FakeHTTPError(Exception):
        def __init__(self, status_code: int) -> None:
            self.response = types.SimpleNamespace(status_code=status_code)

    calls: list[str] = []

    class FakeResponse:
        def __init__(self, endpoint: str) -> None:
            self._endpoint = endpoint

        def raise_for_status(self) -> None:
            if self._endpoint.endswith("/responses"):
                raise FakeHTTPError(404)

        def json(self) -> dict[str, object]:
            if self._endpoint.endswith("/chat/completions"):
                return {"choices": [{"message": {"content": '{"ai_score":0.9,"fit_label":"strong"}'}}]}
            return {}

    class FakeHTTPClient:
        def __enter__(self) -> "FakeHTTPClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def post(self, url: str, headers: dict[str, str], json: dict[str, object]) -> FakeResponse:
            calls.append(url)
            return FakeResponse(url)

    fake_httpx = types.SimpleNamespace(Client=lambda timeout=None: FakeHTTPClient(), HTTPStatusError=FakeHTTPError)
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    monkeypatch.setattr(
        "fitcv.ai_score.resolve_model_routing_part",
        lambda part, model_fallback=None: {
            "provider": "openai_compatible",
            "model": "cx/gpt-5.2",
            "base_url": "http://localhost:20128/v1",
        },
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("FITCV_LANGGRAPH_WIRE_API", "responses")

    client = _make_genai_client({"gemini_model": "gemini-2.5-flash"})
    result = client.models.generate_content(model="any", contents="hello")

    assert '"fit_label":"strong"' in result.text
    assert calls[0].endswith("/responses")
    assert calls[1].endswith("/chat/completions")


def test_make_genai_client_openai_compatible_uses_routed_model_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.ai_score import _make_genai_client

    captured_models: list[str] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"output_text": '{"ai_score":0.8,"fit_label":"strong"}'}

    class FakeHTTPClient:
        def __enter__(self) -> "FakeHTTPClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def post(self, url: str, headers: dict[str, str], json: dict[str, object]) -> FakeResponse:
            captured_models.append(str(json.get("model")))
            return FakeResponse()

    fake_httpx = types.SimpleNamespace(Client=lambda timeout=None: FakeHTTPClient(), HTTPStatusError=Exception)
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    monkeypatch.setattr(
        "fitcv.ai_score.resolve_model_routing_part",
        lambda part, model_fallback=None: {
            "provider": "openai_compatible",
            "model": "cx/gpt-5.2",
            "base_url": "http://localhost:20128/v1",
        },
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("FITCV_LANGGRAPH_WIRE_API", "responses")
    monkeypatch.setenv("FITCV_LANGGRAPH_MODEL", "cx/gpt-5.2")

    client = _make_genai_client({"gemini_model": "gemini-2.5-flash"})
    _ = client.models.generate_content(model="gemini-2.5-flash", contents="hello")
    assert captured_models == ["cx/gpt-5.2"]


def test_score_job_uses_versioned_default_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.ai_score import score_job

    class FakeResponse:
        text = '{"ai_score": 0.8, "fit_label": "strong", "score_reasoning": "good", "matched_strengths": [], "key_risks": []}'

    captured: dict[str, object] = {}

    class FakeModels:
        def generate_content(self, *, model: str, contents: str) -> FakeResponse:
            captured["model"] = model
            captured["contents"] = contents
            return FakeResponse()

    class FakeClient:
        models = FakeModels()

    fake_embeddings = types.SimpleNamespace(
        build_job_summary_text=lambda job: "summary"
    )

    monkeypatch.setitem(sys.modules, "fitcv.embeddings", fake_embeddings)
    monkeypatch.setattr("fitcv.ai_score._make_genai_client", lambda config: FakeClient())

    result = score_job(
        job={"job_url": "http://test.url/1", "title": "Data Engineer"},
        candidate_summary="candidate summary",
        top_evidence=[],
        config={},
    )

    assert captured["model"] == "gemini-2.5-flash"


def test_run_ai_scoring_prefers_nested_pipeline_top_n_over_legacy_flat_key() -> None:
    from fitcv.ai_score import run_ai_scoring

    shortlist = [
        {"job_url": "https://example.com/1"},
        {"job_url": "https://example.com/2"},
        {"job_url": "https://example.com/3"},
    ]

    with patch("fitcv.ai_score.score_job") as mock_score_job, patch.object(time, "sleep"):
        mock_score_job.side_effect = lambda **kwargs: {
            "job_url": kwargs["job"]["job_url"],
            "ai_score": 0.5,
            "fit_label": "stretch",
            "score_reasoning": "ok",
            "matched_strengths": [],
            "key_risks": [],
        }
        results = run_ai_scoring(
            shortlist=shortlist,
            candidate_summary="candidate",
            config={
                "pipeline": {"ai_score_top_n": 1},
                "rerank_top_n": 3,
                "rerank_sleep_secs": 0.0,
            },
        )

    assert len(results) == 1
    assert mock_score_job.call_count == 1
    assert results[0]["job_url"] == "https://example.com/1"


# ── store_ai_scores ───────────────────────────────────────────────────────────


def test_store_ai_scores_writes_sqlite_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.ai_score import store_ai_scores

    db_path = tmp_path / "fitcv_cp.sqlite3"
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "sqlite")
    monkeypatch.setenv("FITCV_CP_SQLITE_PATH", str(db_path))

    scores = [
        {
            "job_url": "https://example.com/job-1",
            "ai_score": 0.91,
            "fit_label": "strong",
            "score_reasoning": "Strong SQL and Python fit",
            "matched_strengths": ["SQL", "Python"],
            "key_risks": ["No dbt"],
        },
        {
            "job_url": "https://example.com/job-2",
            "ai_score": 0.55,
            "fit_label": "stretch",
            "score_reasoning": "Partial analytics overlap",
            "matched_strengths": ["Looker"],
            "key_risks": [],
        },
    ]

    store_ai_scores(scores, config={})

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT job_url, ai_score, fit_label, score_reasoning,
                   matched_strengths_json, key_risks_json
            FROM ai_score_results
            ORDER BY job_url
            """
        ).fetchall()

    assert len(rows) == 2
    assert rows[0][0] == "https://example.com/job-1"
    assert float(rows[0][1]) == 0.91
    assert rows[0][2] == "strong"
    assert json.loads(str(rows[0][4])) == ["SQL", "Python"]
    assert json.loads(str(rows[0][5])) == ["No dbt"]
    assert rows[1][0] == "https://example.com/job-2"
    assert float(rows[1][1]) == 0.55
    assert rows[1][2] == "stretch"
    assert json.loads(str(rows[1][4])) == ["Looker"]
    assert json.loads(str(rows[1][5])) == []


def test_store_ai_scores_bigquery_mode_uses_bigquery_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.ai_score import store_ai_scores

    calls: dict[str, object] = {}

    class FakeCredentials:
        @staticmethod
        def from_service_account_file(path: str) -> str:
            calls["credential_path"] = path
            return "fake-creds"

    class FakeClient:
        def __init__(self, *, project: str, credentials: object) -> None:
            calls["project"] = project
            calls["credentials"] = credentials

        def insert_rows_json(self, table_ref: str, rows: list[dict[str, object]]) -> list[object]:
            calls["table_ref"] = table_ref
            calls["rows"] = rows
            return []

    fake_bigquery = types.SimpleNamespace(Client=FakeClient)
    fake_service_account = types.SimpleNamespace(Credentials=FakeCredentials)
    monkeypatch.setitem(sys.modules, "google.cloud", types.SimpleNamespace(bigquery=fake_bigquery))
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", types.SimpleNamespace(service_account=fake_service_account))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    store_ai_scores(
        [
            {
                "job_url": "https://example.com/job-9",
                "ai_score": 0.88,
                "fit_label": "strong",
                "score_reasoning": "Good fit",
                "matched_strengths": ["SQL"],
                "key_risks": [],
            }
        ],
        config={
            "gcp_project": "demo-project",
            "bigquery_dataset": "fitcv",
            "service_account_key": "C:/fake/key.json",
        },
    )

    assert calls["credential_path"] == "C:/fake/key.json"
    assert calls["project"] == "demo-project"
    assert calls["table_ref"] == "demo-project.fitcv.ai_score_results"
    rows = calls["rows"]
    assert isinstance(rows, list)
    assert rows[0]["job_url"] == "https://example.com/job-9"
    assert rows[0]["matched_strengths"] == ["SQL"]
    assert rows[0]["key_risks"] == []
    assert float(rows[0]["ai_score"]) == 0.88


# ── integration tests ─────────────────────────────────────────────────────────

@pytest.mark.integration
def test_score_job_integration(config: dict) -> None:
    """Integration — calls Vertex AI ML.GENERATE_TEXT and returns a parsed score."""
    from fitcv.ai_score import score_job
    job = {
        "job_url": "http://test.url/1",
        "title": "Data Engineer",
        "required_skills": ["SQL", "Python"],
        "seniority": "mid",
        "job_family": "data_engineering",
        "responsibilities": ["Build pipelines", "Write tests"],
    }
    result = score_job(
        job=job,
        candidate_summary="Experienced data engineer with 4 years SQL and Python.",
        top_evidence=["Built GA4 pipeline reducing latency 40%."],
        config=config,
    )
    assert 0.0 <= result["ai_score"] <= 1.0
    assert result["fit_label"] in ("strong", "stretch", "skip")
"""
@meta
type: test
scope: unit
domain: ranking
covers:
  - AI scoring behavior
excludes:
  - live model calls
tags:
  - fast
  - ci-safe
"""
