"""@meta
name: ranking
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.ranking.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import re
from datetime import datetime, timezone
from typing import Any

from fitcv.candidate import canonicalize_role_title, infer_role_family
from fitcv.config import sqlite_mode_enabled
from fitcv.persistence import build_bigquery_client
from fitcv.ranking_contract import (
    validate_missing_defaults_contract,
    validate_weight_contract,
)

SUPPORTED_RANKING_FEATURES = (
    "ai_score",
    "must_have_match",
    "vector_similarity",
    "title_relevance",
    "seniority_fit",
    "preference_fit",
)
DEFAULT_ACTIVE_RANKING_WEIGHTS = {
    "ai_score": 0.40,
    "must_have_match": 0.20,
    "vector_similarity": 0.15,
    "title_relevance": 0.10,
    "seniority_fit": 0.10,
    "preference_fit": 0.05,
}
DEFAULT_ACTIVE_MISSING_VALUE_DEFAULTS = {
    "ai_score": 0.0,
    "must_have_match": 0.5,
    "vector_similarity": 0.0,
    "title_relevance": 0.5,
    "seniority_fit": 0.5,
    "preference_fit": 0.5,
}
LEGACY_MISSING_VALUE_DEFAULTS_KEY = "ranking_null_defaults"
DEFAULT_PREFERENCE_FIT_WEIGHTS = {
    "domain": 0.50,
    "role_family": 0.30,
    "location_type": 0.20,
}


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9_]+", " ", value.lower())).strip()

def _canonicalize_with_alias_map(value: str | None, alias_map: dict[str, str] | None) -> str:
    normalized_value = _normalize_text(value)
    if not normalized_value:
        return ""
    if not isinstance(alias_map, dict):
        return normalized_value
    canonical = alias_map.get(normalized_value)
    return _normalize_text(canonical) if canonical else normalized_value

def _canonical_domain(value: str | None, config: dict[str, Any] | None = None) -> str:
    alias_map = (config or {}).get("domain_alias_map")
    return _canonicalize_with_alias_map(value, alias_map if isinstance(alias_map, dict) else None)

def _canonical_role_family(value: str | None, config: dict[str, Any] | None = None) -> str:
    alias_map = (config or {}).get("role_family_alias_map")
    return _canonicalize_with_alias_map(value, alias_map if isinstance(alias_map, dict) else None)

def _domain_neighbors(config: dict[str, Any] | None = None) -> dict[str, frozenset[str]]:
    raw_neighbors = (config or {}).get("domain_neighbors")
    if not isinstance(raw_neighbors, dict):
        return {}
    return {
        _normalize_text(str(domain)): frozenset(
            _normalize_text(str(neighbor))
            for neighbor in neighbors
            if _normalize_text(str(neighbor))
        )
        for domain, neighbors in raw_neighbors.items()
        if isinstance(neighbors, (list, tuple))
    }


def _role_family_neighbors(config: dict[str, Any] | None = None) -> dict[str, frozenset[str]]:
    raw_neighbors = ((config or {}).get("role_taxonomy") or {}).get("role_family_neighbors")
    if not isinstance(raw_neighbors, dict):
        return {}
    return {
        _normalize_text(str(family)): frozenset(
            _normalize_text(str(neighbor))
            for neighbor in neighbors
            if _normalize_text(str(neighbor))
        )
        for family, neighbors in raw_neighbors.items()
        if isinstance(neighbors, (list, tuple))
    }


def get_active_ranking_weights(config: dict[str, Any] | None = None) -> dict[str, float]:
    """Return the supported runtime ranking weights."""
    configured = (config or {}).get("ranking_weights") or {}
    if not isinstance(configured, dict):
        return dict(DEFAULT_ACTIVE_RANKING_WEIGHTS)

    resolved = dict(DEFAULT_ACTIVE_RANKING_WEIGHTS)
    for feature_name in SUPPORTED_RANKING_FEATURES:
        raw_weight = configured.get(feature_name)
        if raw_weight is not None:
            resolved[feature_name] = float(raw_weight)
    validate_weight_contract(resolved)
    return resolved


def get_active_missing_value_defaults(config: dict[str, Any] | None = None) -> dict[str, float]:
    """Return missing-value defaults for the supported runtime ranking contract."""
    cfg = config or {}
    configured = cfg.get("missing_value_defaults")
    if configured is None:
        configured = cfg.get(LEGACY_MISSING_VALUE_DEFAULTS_KEY)
    if not isinstance(configured, dict):
        return dict(DEFAULT_ACTIVE_MISSING_VALUE_DEFAULTS)

    resolved = dict(DEFAULT_ACTIVE_MISSING_VALUE_DEFAULTS)
    for feature_name in SUPPORTED_RANKING_FEATURES:
        raw_default = configured.get(feature_name)
        if raw_default is not None:
            resolved[feature_name] = float(raw_default)
    validate_missing_defaults_contract(resolved, supported_features=SUPPORTED_RANKING_FEATURES)
    return resolved


def get_preference_fit_weights(config: dict[str, Any] | None = None) -> dict[str, float]:
    configured = (config or {}).get("preference_fit_weights") or {}
    if not isinstance(configured, dict):
        return dict(DEFAULT_PREFERENCE_FIT_WEIGHTS)

    resolved = dict(DEFAULT_PREFERENCE_FIT_WEIGHTS)
    for key in DEFAULT_PREFERENCE_FIT_WEIGHTS:
        raw_weight = configured.get(key)
        if raw_weight is not None:
            resolved[key] = float(raw_weight)
    return resolved


# ── feature computation ───────────────────────────────────────────────────────

def compute_must_have_match(
    job_skills: list[str],
    candidate_skills: list[str],
    config: dict[str, Any] | None = None,
) -> float:
    """Compute ratio of required skills matched by the candidate.

    - Uses the synonym map via config (or default if None) for canonical matching.
    - If job has no required skills, returns 0.5 (neutral, no penalty).
    - If candidate has no skills but job does, returns 0.0.
    """
    if not job_skills:
        return 0.5
    if not candidate_skills:
        return 0.0

    raw_synonyms = (config or {}).get("skill_synonyms", {})
    synonyms = raw_synonyms if isinstance(raw_synonyms, dict) else {}

    def canonical(s: str) -> str:
        lower = s.strip().lower()
        return synonyms.get(lower, lower)

    reqs = {canonical(s) for s in job_skills}
    cands = {canonical(s) for s in candidate_skills}

    matched = len(reqs & cands)
    return matched / len(reqs)


def compute_seniority_fit(
    job_seniority: str | None,
    target_seniority: str | None,
    config: dict[str, Any] | None = None,
) -> float:
    """Map seniority closeness to a score in [0.0, 1.0].

    Rules:
    - exact match: 1.0
    - off by ±1 step: 0.5
    - off by ±2+ steps: 0.0
    - unknown (either side): 0.5 (neutral)
    """
    if not job_seniority or not target_seniority:
        return 0.5

    ladder = (config or {}).get("seniority", {}).get("ladder", [])
    if not ladder:
        # Fallback if config is missing
        ladder = ["intern", "entry", "associate", "mid", "senior", "lead", "manager", "director"]

    try:
        job_idx = ladder.index(job_seniority.lower())
        tgt_idx = ladder.index(target_seniority.lower())
    except ValueError:
        return 0.5

    diff = abs(job_idx - tgt_idx)
    if diff == 0:
        return 1.0
    if diff == 1:
        return 0.5
    return 0.0


def compute_title_relevance(
    job_title: str | None,
    candidate_target_role: str | None,
    *,
    job_family: str | None = None,
    config: dict[str, Any] | None = None,
) -> float:
    """Compute semantic role alignment between target role and job title.

    Prefer deterministic role-family normalization when possible, then fall back
    to lexical token overlap. The exposed score remains bounded in [0.0, 1.0].
    """
    if not job_title or not candidate_target_role:
        return 0.5

    target_family = infer_role_family(candidate_target_role, config=config)
    resolved_job_family = infer_role_family(job_title, explicit_family=job_family, config=config)
    neighbors = _role_family_neighbors(config)
    if target_family and resolved_job_family:
        if target_family == resolved_job_family:
            return 1.0
        if resolved_job_family in neighbors.get(target_family, frozenset()):
            return 0.75
        return 0.0

    canonical_target_role = canonicalize_role_title(candidate_target_role, config)
    canonical_job_role = canonicalize_role_title(job_title, config)
    if canonical_target_role and canonical_job_role:
        return 1.0 if canonical_target_role == canonical_job_role else 0.0

    tgt_tokens = set(candidate_target_role.lower().split())
    job_tokens = set(job_title.lower().split())

    if not tgt_tokens:
        return 0.5

    matched = len(tgt_tokens & job_tokens)
    return matched / len(tgt_tokens)


def _normalized_preferences(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [_normalize_text(str(value)) for value in values if _normalize_text(str(value))]


def _preference_dimension_score(job_value: str | None, preferred_values: list[str]) -> float:
    if not preferred_values:
        return 0.5
    normalized_job_value = _normalize_text(job_value)
    if not normalized_job_value:
        return 0.0
    return 1.0 if normalized_job_value in preferred_values else 0.0

def _preference_neighbor_score(config: dict[str, Any] | None = None) -> float:
    raw_score = (config or {}).get("preference_fit_neighbor_score")
    if raw_score is None:
        return 0.7
    try:
        score = float(raw_score)
    except (TypeError, ValueError):
        return 0.7
    return max(0.0, min(1.0, score))

def _preference_dimension_score_with_neighbors(
    *,
    job_value: str,
    preferred_values: list[str],
    neighbors: dict[str, frozenset[str]] | None = None,
    neighbor_score: float = 0.7,
) -> tuple[float, str]:
    if not preferred_values:
        return 0.5, "neutral"
    if not job_value:
        return 0.0, "none"
    if job_value in preferred_values:
        return 1.0, "exact"
    neighbor_map = neighbors or {}
    preferred_set = set(preferred_values)
    for preferred in preferred_set:
        if job_value in neighbor_map.get(preferred, frozenset()):
            return neighbor_score, "neighbor"
        if preferred in neighbor_map.get(job_value, frozenset()):
            return neighbor_score, "neighbor"
    return 0.0, "none"


def compute_preference_fit_details(
    job: dict[str, Any],
    prefs: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pref_domains = [_canonical_domain(value, config) for value in _normalized_preferences(prefs.get("domains", []))]
    pref_role_families = [_canonical_role_family(value, config) for value in _normalized_preferences(prefs.get("role_families", []))]
    pref_locations = _normalized_preferences(prefs.get("location_types", []))
    domain_neighbors = _domain_neighbors(config)
    role_family_neighbors = _role_family_neighbors(config)
    neighbor_score = _preference_neighbor_score(config)

    if not (pref_domains or pref_role_families or pref_locations):
        return {
            "score": 0.5,
            "weights": get_preference_fit_weights(config),
            "components": {
                "domain": 0.5,
                "role_family": 0.5,
                "location_type": 0.5,
            },
            "match_details": {
                "domain": "neutral",
                "role_family": "neutral",
                "location_type": "neutral",
            },
        }

    weights = get_preference_fit_weights(config)
    canonical_domain = _canonical_domain(str(job.get("domain") or ""), config)
    canonical_role_family = _canonical_role_family(str(job.get("job_family") or ""), config)
    domain_score, domain_match_type = _preference_dimension_score_with_neighbors(
        job_value=canonical_domain,
        preferred_values=[value for value in pref_domains if value],
        neighbors=domain_neighbors,
        neighbor_score=neighbor_score,
    )
    role_family_score, role_family_match_type = _preference_dimension_score_with_neighbors(
        job_value=canonical_role_family,
        preferred_values=[value for value in pref_role_families if value],
        neighbors=role_family_neighbors,
        neighbor_score=neighbor_score,
    )
    location_score = _preference_dimension_score(str(job.get("location_type") or ""), pref_locations)
    components = {
        "domain": domain_score,
        "role_family": role_family_score,
        "location_type": location_score,
    }
    location_match_type = (
        "neutral"
        if not pref_locations
        else ("exact" if location_score == 1.0 else "none")
    )
    score = sum(components[key] * weights[key] for key in DEFAULT_PREFERENCE_FIT_WEIGHTS)
    return {
        "score": score,
        "weights": weights,
        "components": components,
        "match_details": {
            "domain": domain_match_type,
            "role_family": role_family_match_type,
            "location_type": location_match_type,
        },
        "canonical_values": {
            "job": {
                "domain": canonical_domain,
                "role_family": canonical_role_family,
                "location_type": _normalize_text(str(job.get("location_type") or "")),
            },
            "preferences": {
                "domains": [value for value in pref_domains if value],
                "role_families": [value for value in pref_role_families if value],
                "location_types": [value for value in pref_locations if value],
            },
        },
    }


def compute_preference_fit(
    job: dict[str, Any],
    prefs: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> float:
    """Compute weighted alignment of explicit preferences.

    Dimensions:
    - domain
    - role_family
    - location_type
    """
    return float(compute_preference_fit_details(job, prefs, config)["score"])


# ── composite score ───────────────────────────────────────────────────────────

def compute_final_score(
    features: dict[str, float],
    weights: dict[str, float],
    null_defaults: dict[str, float],
) -> float:
    """Compute the weighted composite score, applying missing-value fallbacks.

    Args:
        features: Dictionary of scores (e.g. ai_score, title_relevance, etc.)
        weights: Dictionary of weights summing to 1.0
        null_defaults: Dictionary of fallback values when a feature is missing
    """
    validate_weight_contract(weights)
    validate_missing_defaults_contract(null_defaults, supported_features=SUPPORTED_RANKING_FEATURES)
    score = 0.0
    for feature_name, weight in weights.items():
        val = features.get(feature_name)
        if val is None:
            val = null_defaults.get(feature_name, 0.0)
        score += val * weight
    return score


def compute_feature_contributions(
    features: dict[str, float],
    weights: dict[str, float],
    null_defaults: dict[str, float],
) -> dict[str, float]:
    validate_weight_contract(weights)
    validate_missing_defaults_contract(null_defaults, supported_features=SUPPORTED_RANKING_FEATURES)
    contributions: dict[str, float] = {}
    for feature_name, weight in weights.items():
        value = features.get(feature_name)
        if value is None:
            value = null_defaults.get(feature_name, 0.0)
        contributions[feature_name] = float(value) * weight
    return contributions


# ── sorting and ranking ───────────────────────────────────────────────────────

def rank_jobs(jobs: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    """Sort jobs by final score and assign a final_rank.

    Tie-breaking order:
    1. final_score DESC
    2. ai_score DESC
    3. vector_similarity DESC
    """
    sorted_jobs = sorted(
        jobs,
        key=lambda j: (
            float(j.get("final_score", 0.0)),
            float(j.get("ai_score", 0.0)),
            float(j.get("vector_similarity", 0.0)),
        ),
        reverse=True,
    )

    ranked = sorted_jobs[:top_n]
    for i, job in enumerate(ranked):
        job["final_rank"] = i + 1

    return ranked


# ── integration: store to bigquery ────────────────────────────────────────────

def store_final_ranking(
    ranked_jobs: list[dict[str, Any]],
    config: dict[str, Any],
) -> None:
    """Insert final ranking rows into fitcv.final_ranking.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.
    """
    if not ranked_jobs:
        return
    if sqlite_mode_enabled(config):
        return

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)
    table_ref = f"{project}.{dataset}.final_ranking"
    now = datetime.now(tz=timezone.utc).isoformat()

    rows = []
    for job in ranked_jobs:
        rows.append({
            "job_url": str(job["job_url"]),
            "final_rank": int(job["final_rank"]),
            "final_score": float(job["final_score"]),
            "ai_score": float(job.get("ai_score", 0.0)),
            "must_have_match": float(job.get("must_have_match", 0.0)),
            "vector_similarity": float(job.get("vector_similarity", 0.0)),
            "title_relevance": float(job.get("title_relevance", 0.5)),
            "seniority_fit": float(job.get("seniority_fit", 0.5)),
            "preference_fit": float(job.get("preference_fit", 0.5)),
            "fit_label": str(job.get("fit_label", "skip")),
            "ranked_at": now,
        })

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for final_ranking: {errors}")
