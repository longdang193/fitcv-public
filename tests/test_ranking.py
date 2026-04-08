import pytest

from fitcv.ranking import (
    compute_feature_contributions,
    get_active_missing_value_defaults,
    get_preference_fit_weights,
    get_active_ranking_weights,
    compute_final_score,
    compute_must_have_match,
    compute_preference_fit,
    compute_seniority_fit,
    compute_title_relevance,
    rank_jobs,
    store_final_ranking,
)


_DEFAULT_WEIGHTS = {
    "ai_score": 0.40,
    "must_have_match": 0.20,
    "vector_similarity": 0.15,
    "title_relevance": 0.10,
    "seniority_fit": 0.10,
    "preference_fit": 0.05,
}

_NULL_DEFAULTS = {
    "ai_score": 0.0,
    "must_have_match": 0.5,
    "vector_similarity": 0.0,
    "title_relevance": 0.5,
    "seniority_fit": 0.5,
    "preference_fit": 0.5,
}


# ── compute_final_score ───────────────────────────────────────────────────────

def test_compute_final_score_weighted():
    features = {
        "ai_score": 0.8,
        "must_have_match": 1.0,
        "vector_similarity": 0.7,
        "title_relevance": 0.5,
        "seniority_fit": 1.0,
        "preference_fit": 0.0,
    }
    score = compute_final_score(features, _DEFAULT_WEIGHTS, _NULL_DEFAULTS)
    expected = (
        0.40 * 0.8
        + 0.20 * 1.0
        + 0.15 * 0.7
        + 0.10 * 0.5
        + 0.10 * 1.0
        + 0.05 * 0.0
    )
    assert abs(score - expected) < 0.001


def test_compute_final_score_handles_missing_ai_score():
    """Missing ai_score → fallback 0.0 (conservative)."""
    features = {
        "must_have_match": 1.0,
        "vector_similarity": 1.0,
        "title_relevance": 1.0,
        "seniority_fit": 1.0,
        "preference_fit": 1.0,
    }
    score = compute_final_score(features, _DEFAULT_WEIGHTS, _NULL_DEFAULTS)
    expected = 0.20 + 0.15 + 0.10 + 0.10 + 0.05
    assert abs(score - expected) < 0.001


def test_compute_final_score_handles_missing_vector_similarity():
    """Missing vector_similarity → fallback 0.0 (conservative)."""
    features = {
        "ai_score": 0.8,
        "must_have_match": 1.0,
        "title_relevance": 1.0,
        "seniority_fit": 1.0,
        "preference_fit": 1.0,
    }
    score = compute_final_score(features, _DEFAULT_WEIGHTS, _NULL_DEFAULTS)
    expected = (0.40 * 0.8) + 0.20 + 0.10 + 0.10 + 0.05
    assert abs(score - expected) < 0.001


def test_compute_final_score_accepts_config_weights():
    """Weights must come from the weights dict, not hardcoded."""
    features = {
        "ai_score": 1.0,
        "must_have_match": 0.0,
        "vector_similarity": 0.0,
        "title_relevance": 0.0,
        "seniority_fit": 0.0,
        "preference_fit": 0.0,
    }
    custom_weights = {
        "ai_score": 1.0,
        "must_have_match": 0.0,
        "vector_similarity": 0.0,
        "title_relevance": 0.0,
        "seniority_fit": 0.0,
        "preference_fit": 0.0,
    }
    # With weight fully on ai_score=1.0, final score should be 1.0
    assert abs(compute_final_score(features, custom_weights, _NULL_DEFAULTS) - 1.0) < 0.001


def test_get_active_ranking_weights_returns_full_six_feature_contract() -> None:
    config = {
        "ranking_weights": {
            "ai_score": 0.4,
            "must_have_match": 0.2,
            "vector_similarity": 0.15,
            "title_relevance": 0.1,
            "seniority_fit": 0.1,
            "preference_fit": 0.05,
        }
    }

    weights = get_active_ranking_weights(config)

    assert weights == {
        "ai_score": 0.4,
        "must_have_match": 0.2,
        "vector_similarity": 0.15,
        "title_relevance": 0.1,
        "seniority_fit": 0.1,
        "preference_fit": 0.05,
    }


def test_get_active_ranking_weights_preserves_zero_weight_features() -> None:
    config = {
        "ranking_weights": {
            "ai_score": 0.73,
            "must_have_match": 0.0,
            "vector_similarity": 0.27,
            "title_relevance": 0.0,
            "seniority_fit": 0.0,
            "preference_fit": 0.0,
        }
    }

    weights = get_active_ranking_weights(config)

    assert weights == {
        "ai_score": 0.73,
        "must_have_match": 0.0,
        "vector_similarity": 0.27,
        "title_relevance": 0.0,
        "seniority_fit": 0.0,
        "preference_fit": 0.0,
    }


def test_get_active_missing_value_defaults_prefers_canonical_key() -> None:
    config = {
        "missing_value_defaults": {
            "ai_score": 0.0,
            "must_have_match": 0.5,
            "vector_similarity": 0.25,
            "title_relevance": 0.5,
            "seniority_fit": 0.5,
            "preference_fit": 0.25,
        },
        "ranking_null_defaults": {
            "ai_score": 0.0,
            "vector_similarity": 0.99,
        },
    }

    defaults = get_active_missing_value_defaults(config)

    assert defaults == {
        "ai_score": 0.0,
        "must_have_match": 0.5,
        "vector_similarity": 0.25,
        "title_relevance": 0.5,
        "seniority_fit": 0.5,
        "preference_fit": 0.25,
    }


def test_get_preference_fit_weights_uses_runtime_config() -> None:
    weights = get_preference_fit_weights(
        {
            "preference_fit_weights": {
                "domain": 0.6,
                "role_family": 0.25,
                "location_type": 0.15,
            }
        }
    )

    assert weights == {
        "domain": 0.6,
        "role_family": 0.25,
        "location_type": 0.15,
    }


# ── compute_must_have_match ───────────────────────────────────────────────────

def test_compute_must_have_match_ratio():
    score = compute_must_have_match(
        job_skills=["SQL", "Python", "BigQuery"],
        candidate_skills=["SQL", "BigQuery"],
    )
    assert abs(score - (2 / 3)) < 0.001


def test_compute_must_have_match_synonym_canonicalization():
    """GCP == Google Cloud via synonym map."""
    config = {"skill_synonyms": {"gcp": "google cloud"}}
    score = compute_must_have_match(
        job_skills=["Google Cloud"],
        candidate_skills=["GCP"],
        config=config,
    )
    assert score == 1.0


def test_compute_must_have_match_empty_job_skills():
    """No required skills → neutral 0.5 (not a penalty)."""
    assert compute_must_have_match(job_skills=[], candidate_skills=["SQL"]) == 0.5


def test_compute_must_have_match_empty_candidate_skills():
    """Candidate has no skills → 0.0 (cannot satisfy any requirement)."""
    assert compute_must_have_match(job_skills=["SQL"], candidate_skills=[]) == 0.0


def test_compute_must_have_match_case_insensitive():
    score = compute_must_have_match(job_skills=["bigquery"], candidate_skills=["BigQuery"])
    assert score == 1.0


# ── compute_seniority_fit ─────────────────────────────────────────────────────

def test_compute_seniority_fit():
    cfg = {"seniority": {"ladder": ["entry", "mid", "senior"]}}
    assert compute_seniority_fit("mid", "mid", cfg) == 1.0
    assert compute_seniority_fit("entry", "mid", cfg) == 0.5  # target=mid, job=entry (distance 1)
    assert compute_seniority_fit("entry", "senior", cfg) == 0.0  # target=senior, job=entry (distance 2)
    assert compute_seniority_fit(None, "mid", cfg) == 0.5  # unknown target
    assert compute_seniority_fit("mid", None, cfg) == 0.5  # unknown job


# ── compute_title_relevance ───────────────────────────────────────────────────

def test_compute_title_relevance():
    # overlap = 2 (data, engineer) / len(target)=2 → 1.0
    assert compute_title_relevance("Data Engineer", "Data Engineer") == 1.0
    assert compute_title_relevance("Senior Data Engineer", "Data Engineer") == 1.0
    # overlap = 1 (engineer) / len(target)=2 → 0.5
    assert compute_title_relevance("Software Engineer", "Data Engineer") == 0.5
    # overlap = 0 → 0.0
    assert compute_title_relevance("Product Manager", "Data Engineer") == 0.0
    # missing → 0.5 neutral
    assert compute_title_relevance(None, "Data") == 0.5
    assert compute_title_relevance("Data", None) == 0.5


def test_compute_title_relevance_uses_semantic_role_alignment() -> None:
    config = {
        "role_taxonomy": {
            "canonical_role_by_alias": {
                "business intelligence analyst": "data analyst",
                "data analyst": "data analyst",
                "analytics engineer": "analytics engineer",
                "data engineer": "data engineer",
                "machine learning engineer": "machine learning engineer",
            },
            "role_family_by_role": {
                "data analyst": "analytics",
                "analytics engineer": "data_engineering",
                "data engineer": "data_engineering",
                "machine learning engineer": "ml_engineering",
            },
            "role_family_neighbors": {
                "analytics": ("data_science",),
                "data_engineering": ("ml_engineering",),
                "ml_engineering": ("data_engineering",),
            },
        }
    }
    assert compute_title_relevance("Business Intelligence Analyst", "Data Analyst", config=config) == 1.0
    assert compute_title_relevance("Analytics Engineer", "Data Engineer", config=config) == 1.0
    assert compute_title_relevance("Machine Learning Engineer", "Data Analyst", config=config) == 0.0


# ── compute_preference_fit ────────────────────────────────────────────────────

def test_compute_preference_fit():
    prefs = {"domains": ["fintech", "health"], "location_types": ["remote"]}
    config = {"preference_fit_weights": {"domain": 0.5, "role_family": 0.3, "location_type": 0.2}}
    assert compute_preference_fit({"domain": "fintech", "location_type": "remote"}, prefs, config) == 0.85
    assert compute_preference_fit({"domain": "fintech", "location_type": "onsite"}, prefs, config) == 0.65
    assert compute_preference_fit({"domain": "retail", "location_type": "onsite"}, prefs, config) == 0.15
    # no preferences = 0.5 neutral
    assert compute_preference_fit({"domain": "fintech"}, {}, config) == 0.5


def test_compute_preference_fit_weights_domain_role_family_and_location_separately() -> None:
    prefs = {
        "domains": ["fintech"],
        "role_families": ["analytics"],
        "location_types": ["remote"],
    }
    config = {"preference_fit_weights": {"domain": 0.5, "role_family": 0.3, "location_type": 0.2}}

    assert compute_preference_fit(
        {"domain": "telecommunications", "job_family": "analytics", "location_type": "remote"},
        prefs,
        config,
    ) == 0.5


def test_compute_feature_contributions_sum_to_final_score() -> None:
    features = {
        "ai_score": 0.8,
        "must_have_match": 1.0,
        "vector_similarity": 0.7,
        "title_relevance": 0.9,
        "seniority_fit": 1.0,
        "preference_fit": 0.5,
    }
    contributions = compute_feature_contributions(features, _DEFAULT_WEIGHTS, _NULL_DEFAULTS)

    assert contributions == {
        "ai_score": pytest.approx(0.32),
        "must_have_match": pytest.approx(0.2),
        "vector_similarity": pytest.approx(0.105),
        "title_relevance": pytest.approx(0.09),
        "seniority_fit": pytest.approx(0.1),
        "preference_fit": pytest.approx(0.025),
    }


# ── rank_jobs ─────────────────────────────────────────────────────────────────

def test_rank_jobs_sorts_descending():
    jobs = [
        {"job_url": "u1", "final_score": 0.5, "ai_score": 0.5, "vector_similarity": 0.5},
        {"job_url": "u2", "final_score": 0.9, "ai_score": 0.9, "vector_similarity": 0.9},
    ]
    ranked = rank_jobs(jobs, top_n=2)
    assert ranked[0]["job_url"] == "u2"


def test_rank_jobs_respects_top_n():
    jobs = [
        {"job_url": "u1", "final_score": 0.9, "ai_score": 0.9, "vector_similarity": 0.9},
        {"job_url": "u2", "final_score": 0.8, "ai_score": 0.8, "vector_similarity": 0.8},
        {"job_url": "u3", "final_score": 0.7, "ai_score": 0.7, "vector_similarity": 0.7},
    ]
    ranked = rank_jobs(jobs, top_n=2)
    assert len(ranked) == 2


def test_rank_jobs_breaks_ties_by_ai_score_then_vector():
    """Tie in final_score → higher ai_score wins; tie in ai_score → higher vector_similarity wins."""
    jobs = [
        {"job_url": "u1", "final_score": 0.8, "ai_score": 0.7, "vector_similarity": 0.8},
        {"job_url": "u2", "final_score": 0.8, "ai_score": 0.9, "vector_similarity": 0.6},
        {"job_url": "u3", "final_score": 0.8, "ai_score": 0.9, "vector_similarity": 0.7},
    ]
    ranked = rank_jobs(jobs, top_n=3)
    # tie break 1: u3 and u2 have higher ai_score than u1 (0.9 vs 0.7)
    # tie break 2: u3 has higher vector_similarity than u2 (0.7 vs 0.6)
    assert ranked[0]["job_url"] == "u3"
    assert ranked[1]["job_url"] == "u2"
    assert ranked[2]["job_url"] == "u1"


def test_rank_jobs_assigns_final_rank():
    """rank_jobs must add a final_rank field (1-indexed)."""
    jobs = [
        {"job_url": "u1", "final_score": 0.5, "ai_score": 0.5, "vector_similarity": 0.5},
        {"job_url": "u2", "final_score": 0.9, "ai_score": 0.9, "vector_similarity": 0.9},
    ]
    ranked = rank_jobs(jobs, top_n=2)
    assert ranked[0]["final_rank"] == 1
    assert ranked[1]["final_rank"] == 2
