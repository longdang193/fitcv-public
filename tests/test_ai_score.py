"""Tests for fitcv.ai_score — all pure unit tests (no cloud calls)."""

import json
import sys
import time
import types
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
    assert result["score_reasoning"] == ""
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


def test_make_genai_client_uses_vertex_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.ai_score import _make_genai_client

    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    fake_genai = types.SimpleNamespace(Client=FakeClient)
    fake_google = types.SimpleNamespace(
        auth=types.SimpleNamespace(default=lambda scopes=None: ("creds", "project"))
    )

    monkeypatch.setenv("GEMINI_API_KEY", "")
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.auth", fake_google.auth)
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)
    setattr(fake_google, "genai", fake_genai)

    _make_genai_client({
        "gcp_project": "fitcv-491123",
        "location": "US",
        "vertex_location": "us-central1",
    })

    assert captured["vertexai"] is True
    assert captured["location"] == "us-central1"


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
