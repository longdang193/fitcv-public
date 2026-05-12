"""@meta
name: evidence
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.evidence.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

"""Evidence retrieval and final evidence selection for CV analysis.

Public API
----------
normalise_evidence_item  : convert a raw profile entry to the canonical evidence schema
score_evidence_item      : compatibility skill-support score for one evidence item
retrieve_evidence_bundle : retrieve channel pools, merge/dedupe, and select final evidence
retrieve_evidence        : compatibility wrapper that returns final selected evidence only
store_evidence_selection : persist selected evidence to BigQuery (integration)
"""

import hashlib
import json
import math
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fitcv.config import get_embedding_model, sqlite_mode_enabled
from fitcv.contracts import (
    ANALYSIS_CHANNEL_IDS,
    CV_ANALYSIS_REUSE_SCHEMA_VERSION,
    DOMAIN_ALIGNMENT_CHANNEL,
    REQUIRED_SKILL_SUPPORT_CHANNEL,
    RESPONSIBILITY_ALIGNMENT_CHANNEL,
    ROLE_ALIGNMENT_CHANNEL,
)
from fitcv.embeddings import generate_embedding
from fitcv.ranking import _normalize_text, _role_family_neighbors, infer_role_family


SKILL_OVERLAP_WEIGHT: float = 0.60
TYPE_WEIGHT_FACTOR: float = 0.25
BUSINESS_VALUE_WEIGHT: float = 0.15

TYPE_WEIGHTS: dict[str, float] = {
    "experience_entry": 1.1,
    "project_entry": 1.0,
    "project": 1.0,
    "experience_bullet": 0.7,
    "achievement": 0.4,
}
RETRIEVAL_CHANNELS = ANALYSIS_CHANNEL_IDS
DEFAULT_CHANNEL_POOL_SIZE = 4
DEFAULT_SEMANTIC_ALIGNMENT_ENABLED = False
DEFAULT_REQUIRED_SKILL_LEXICAL_WEIGHT = 0.70
DEFAULT_REQUIRED_SKILL_SEMANTIC_WEIGHT = 0.30
DEFAULT_ROLE_LEXICAL_WEIGHT = 0.60
DEFAULT_ROLE_SEMANTIC_WEIGHT = 0.40
DEFAULT_RESPONSIBILITY_LEXICAL_WEIGHT = 0.25
DEFAULT_RESPONSIBILITY_SEMANTIC_WEIGHT = 0.75
DEFAULT_DOMAIN_LEXICAL_WEIGHT = 0.40
DEFAULT_DOMAIN_SEMANTIC_WEIGHT = 0.60
SEMANTIC_METHOD_DISABLED = "disabled"
SEMANTIC_METHOD_EMBEDDING = "embedding_similarity"
ROLE_ALIGNMENT_NEIGHBOR_SCORE = 0.75

_UUID_NAMESPACE = uuid.NAMESPACE_OID
DEFAULT_EXPERIENCE_ENTRY_TOP_K = 2
DEFAULT_PROJECT_ENTRY_TOP_K = 2
DEFAULT_ACHIEVEMENT_TOP_K = 1
DEFAULT_BULLETS_PER_EXPERIENCE = 2
DEFAULT_HIGHLIGHTS_PER_PROJECT = 2
DEFAULT_STACK_LINES_PER_PROJECT = 2
_STOPWORDS = {
    "a",
    "an",
    "and",
    "for",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}


def _cv_analysis_policy_settings(config: dict[str, Any] | None) -> dict[str, Any]:
    selection_policy: dict[str, Any] = {}
    if isinstance(config, dict):
        selection_policy = dict((config.get("cv_analysis") or {}).get("selection_policy") or {})
    channel_weights = dict(selection_policy.get("channel_weights") or {})
    return {
        "channel_weights": {
            REQUIRED_SKILL_SUPPORT_CHANNEL: float(channel_weights.get(REQUIRED_SKILL_SUPPORT_CHANNEL, 0.40)),
            RESPONSIBILITY_ALIGNMENT_CHANNEL: float(channel_weights.get(RESPONSIBILITY_ALIGNMENT_CHANNEL, 0.30)),
            ROLE_ALIGNMENT_CHANNEL: float(channel_weights.get(ROLE_ALIGNMENT_CHANNEL, 0.15)),
            DOMAIN_ALIGNMENT_CHANNEL: float(channel_weights.get(DOMAIN_ALIGNMENT_CHANNEL, 0.15)),
        },
        "multi_channel_bonus": float(selection_policy.get("multi_channel_bonus", 0.05)),
        "type_weight_factor": float(selection_policy.get("type_weight_factor", 0.10)),
        "residual_score_factor": float(selection_policy.get("residual_score_factor", 0.05)),
        "new_type_bonus": float(selection_policy.get("new_type_bonus", 0.03)),
        "same_type_penalty": float(selection_policy.get("same_type_penalty", 0.02)),
        "quotas": {
            "experience_entry_top_k": int(dict(selection_policy.get("quotas") or {}).get("experience_entry_top_k", DEFAULT_EXPERIENCE_ENTRY_TOP_K)),
            "project_entry_top_k": int(dict(selection_policy.get("quotas") or {}).get("project_entry_top_k", DEFAULT_PROJECT_ENTRY_TOP_K)),
            "achievement_top_k": int(dict(selection_policy.get("quotas") or {}).get("achievement_top_k", DEFAULT_ACHIEVEMENT_TOP_K)),
        },
        "trimming": {
            "bullets_per_experience": int(dict(selection_policy.get("trimming") or {}).get("bullets_per_experience", DEFAULT_BULLETS_PER_EXPERIENCE)),
            "highlights_per_project": int(dict(selection_policy.get("trimming") or {}).get("highlights_per_project", DEFAULT_HIGHLIGHTS_PER_PROJECT)),
            "stack_lines_per_project": int(dict(selection_policy.get("trimming") or {}).get("stack_lines_per_project", DEFAULT_STACK_LINES_PER_PROJECT)),
        },
    }


def _normalize_optional_text(value: Any) -> str:
    return str(value or "").strip()


def _canonicalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _canonicalize_json_value(value[key])
            for key in sorted(value)
        }
    if isinstance(value, list):
        return [_canonicalize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_canonicalize_json_value(item) for item in value]
    if isinstance(value, set):
        return sorted(_canonicalize_json_value(item) for item in value)
    if isinstance(value, str):
        return value.strip()
    return value


def _stable_json_fingerprint(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(
        _canonicalize_json_value(payload),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _normalize_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        text = _normalize_optional_text(value)
        if not text:
            continue
        if text in seen_values:
            continue
        seen_values.add(text)
        normalized.append(text)
    return normalized


def _canonicalize_terms(values: list[str]) -> list[str]:
    canonical: list[str] = []
    seen_terms: set[str] = set()
    for value in values:
        normalized = _normalize_text(value)
        if not normalized or normalized in seen_terms:
            continue
        seen_terms.add(normalized)
        canonical.append(normalized)
    return canonical


def _canonicalize_term_set(values: list[str]) -> set[str]:
    return set(_canonicalize_terms(values))


def _build_evidence_id(*parts: str) -> str:
    seed = "|".join(_normalize_optional_text(part) for part in parts)
    return str(uuid.uuid5(_UUID_NAMESPACE, seed))


def _extract_canonical_entities(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    extracted: list[str] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        canonical = _normalize_optional_text(value.get("canonical"))
        if canonical:
            extracted.append(canonical)
    return extracted


def _tokenize(value: str) -> set[str]:
    normalized = _normalize_text(value)
    if not normalized:
        return set()
    return {
        token
        for token in normalized.split()
        if len(token) > 1 and token not in _STOPWORDS
    }


def _overlap_ratio(lhs: set[str], rhs: set[str]) -> float:
    if not lhs or not rhs:
        return 0.0
    return len(lhs & rhs) / len(rhs)


def _clamp_score(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


def _cosine_similarity(lhs: list[float], rhs: list[float]) -> float:
    if not lhs or not rhs or len(lhs) != len(rhs):
        return 0.0
    numerator = sum(left * right for left, right in zip(lhs, rhs, strict=False))
    lhs_norm = math.sqrt(sum(value * value for value in lhs))
    rhs_norm = math.sqrt(sum(value * value for value in rhs))
    if lhs_norm <= 0.0 or rhs_norm <= 0.0:
        return 0.0
    return _clamp_score(numerator / (lhs_norm * rhs_norm))


def _semantic_alignment_settings(config: dict[str, Any] | None) -> dict[str, Any]:
    semantic_alignment: dict[str, Any] = {}
    if isinstance(config, dict):
        cv_analysis_config = dict(config.get("cv_analysis") or {})
        semantic_alignment = dict(cv_analysis_config.get("semantic_alignment") or {})
    return {
        "enabled": bool(
            semantic_alignment.get("enabled", DEFAULT_SEMANTIC_ALIGNMENT_ENABLED)
            if isinstance(config, dict)
            else False
        ),
        "model": str(semantic_alignment.get("model") or get_embedding_model(config or {})),
        "required_skill_lexical_weight": float(
            semantic_alignment.get(
                "required_skill_lexical_weight",
                DEFAULT_REQUIRED_SKILL_LEXICAL_WEIGHT,
            )
        ),
        "required_skill_semantic_weight": float(
            semantic_alignment.get(
                "required_skill_semantic_weight",
                DEFAULT_REQUIRED_SKILL_SEMANTIC_WEIGHT,
            )
        ),
        "role_lexical_weight": float(
            semantic_alignment.get(
                "role_lexical_weight",
                DEFAULT_ROLE_LEXICAL_WEIGHT,
            )
        ),
        "role_semantic_weight": float(
            semantic_alignment.get(
                "role_semantic_weight",
                DEFAULT_ROLE_SEMANTIC_WEIGHT,
            )
        ),
        "responsibility_lexical_weight": float(
            semantic_alignment.get(
                "responsibility_lexical_weight",
                DEFAULT_RESPONSIBILITY_LEXICAL_WEIGHT,
            )
        ),
        "responsibility_semantic_weight": float(
            semantic_alignment.get(
                "responsibility_semantic_weight",
                DEFAULT_RESPONSIBILITY_SEMANTIC_WEIGHT,
            )
        ),
        "domain_lexical_weight": float(
            semantic_alignment.get("domain_lexical_weight", DEFAULT_DOMAIN_LEXICAL_WEIGHT)
        ),
        "domain_semantic_weight": float(
            semantic_alignment.get("domain_semantic_weight", DEFAULT_DOMAIN_SEMANTIC_WEIGHT)
        ),
        "channel_pool_size": int(
            semantic_alignment.get("channel_pool_size", DEFAULT_CHANNEL_POOL_SIZE)
        ),
    }


def _cv_analysis_profile_payload(profile: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "skills": list(profile.get("skills") or []),
        "years_experience": profile.get("years_experience"),
        "preferences": dict(profile.get("preferences") or {}),
        "experiences": list(profile.get("experiences") or []),
        "projects": list(profile.get("projects") or []),
        "achievements": list(profile.get("achievements") or []),
    }
    return {
        key: value
        for key, value in payload.items()
        if value not in (None, "", [], {})
    }


def build_cv_analysis_contract_fingerprint(config: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_version": CV_ANALYSIS_REUSE_SCHEMA_VERSION,
        "evidence_top_k": int(config.get("pipeline", {}).get("evidence_top_k", 0) or 0),
        "semantic_alignment": _semantic_alignment_settings(config),
        "selection_policy": _cv_analysis_policy_settings(config),
        "fit_label_thresholds": dict(config.get("fit_label_thresholds") or {}),
        "role_taxonomy": dict(config.get("role_taxonomy") or {}),
        "skill_synonyms_runtime": dict(config.get("skill_synonyms_runtime") or {}),
    }
    return {
        "payload": payload,
        "fingerprint": _stable_json_fingerprint(payload),
    }


def build_cv_analysis_input_fingerprint(
    profile: dict[str, Any],
    job_context: dict[str, Any] | list[str],
    config: dict[str, Any],
) -> dict[str, Any]:
    coerced_job_context = _coerce_job_context(job_context)
    job_payload = {
        "job_url": str(coerced_job_context.get("job_url") or ""),
        "job_title": str(coerced_job_context.get("job_title") or ""),
        "job_family": str(coerced_job_context.get("job_family") or ""),
        "domain": str(coerced_job_context.get("domain") or ""),
        "required_skills": list(coerced_job_context.get("required_skills") or []),
        "preferred_skills": list(coerced_job_context.get("preferred_skills") or []),
        "responsibilities": list(coerced_job_context.get("responsibilities") or []),
        "years_experience_min": job_context.get("years_experience_min") if isinstance(job_context, dict) else None,
        "years_experience_max": job_context.get("years_experience_max") if isinstance(job_context, dict) else None,
        "fit_label": str(job_context.get("fit_label") or "") if isinstance(job_context, dict) else "",
    }
    payload = {
        "profile": _cv_analysis_profile_payload(profile),
        "job": {
            key: value
            for key, value in job_payload.items()
            if value not in (None, "", [], {})
        },
        "contract_fingerprint": build_cv_analysis_contract_fingerprint(config)["fingerprint"],
    }
    return {
        "payload": payload,
        "fingerprint": _stable_json_fingerprint(payload),
    }


def _semantic_runtime_state() -> dict[str, Any]:
    return {
        "embedding_cache": {},
        "candidate_embedding_fresh_count": 0,
        "candidate_embedding_reused_count": 0,
        "job_embedding_fresh_count": 0,
        "job_embedding_reused_count": 0,
    }


def _semantic_reuse_state(runtime_state: dict[str, Any]) -> dict[str, str]:
    candidate_fresh = int(runtime_state.get("candidate_embedding_fresh_count") or 0)
    candidate_reused = int(runtime_state.get("candidate_embedding_reused_count") or 0)
    job_fresh = int(runtime_state.get("job_embedding_fresh_count") or 0)
    job_reused = int(runtime_state.get("job_embedding_reused_count") or 0)

    def _status_for(fresh_count: int, reused_count: int) -> str:
        if fresh_count > 0 and reused_count > 0:
            return "mixed_fresh_and_reused"
        if fresh_count > 0:
            return "fresh_embedding"
        if reused_count > 0:
            return "reused_cached_embedding"
        return "not_requested"

    return {
        "candidate_evidence": _status_for(candidate_fresh, candidate_reused),
        "job_context": _status_for(job_fresh, job_reused),
    }


def _semantic_embedding_counts(runtime_state: dict[str, Any]) -> dict[str, dict[str, int]]:
    return {
        "candidate_evidence": {
            "fresh": int(runtime_state.get("candidate_embedding_fresh_count") or 0),
            "reused": int(runtime_state.get("candidate_embedding_reused_count") or 0),
        },
        "job_context": {
            "fresh": int(runtime_state.get("job_embedding_fresh_count") or 0),
            "reused": int(runtime_state.get("job_embedding_reused_count") or 0),
        },
    }


def _semantic_methods(enabled: bool) -> dict[str, str]:
    method = SEMANTIC_METHOD_EMBEDDING if enabled else SEMANTIC_METHOD_DISABLED
    return {
        REQUIRED_SKILL_SUPPORT_CHANNEL: method,
        ROLE_ALIGNMENT_CHANNEL: method,
        RESPONSIBILITY_ALIGNMENT_CHANNEL: method,
        DOMAIN_ALIGNMENT_CHANNEL: method,
    }


def _embed_text_cached(
    text: str,
    *,
    config: dict[str, Any],
    model_name: str,
    runtime_state: dict[str, Any],
    cache_namespace: str,
) -> list[float]:
    normalized_text = " ".join(str(text).split()).strip()
    if not normalized_text:
        return []
    cache_key = f"{cache_namespace}:{normalized_text.casefold()}"
    embedding_cache = runtime_state["embedding_cache"]
    cached = embedding_cache.get(cache_key)
    if cached is not None:
        if cache_namespace == "candidate":
            runtime_state["candidate_embedding_reused_count"] = int(
                runtime_state.get("candidate_embedding_reused_count") or 0
            ) + 1
        elif cache_namespace == "job":
            runtime_state["job_embedding_reused_count"] = int(
                runtime_state.get("job_embedding_reused_count") or 0
            ) + 1
        return list(cached)
    vector = list(generate_embedding(normalized_text, config, model_name=model_name))
    embedding_cache[cache_key] = vector
    if cache_namespace == "candidate":
        runtime_state["candidate_embedding_fresh_count"] = int(
            runtime_state.get("candidate_embedding_fresh_count") or 0
        ) + 1
    elif cache_namespace == "job":
        runtime_state["job_embedding_fresh_count"] = int(
            runtime_state.get("job_embedding_fresh_count") or 0
        ) + 1
    return list(vector)


def _context_terms(job_context: dict[str, Any]) -> list[str]:
    terms: list[str] = []
    terms.extend(list(job_context.get("required_skills") or []))
    terms.extend(list(job_context.get("preferred_skills") or []))
    title = _normalize_optional_text(job_context.get("job_title"))
    if title:
        terms.append(title)
    domain = _normalize_optional_text(job_context.get("domain"))
    if domain:
        terms.append(domain)
    job_family = _normalize_optional_text(job_context.get("job_family"))
    if job_family:
        terms.append(job_family)
    terms.extend(list(job_context.get("responsibilities") or []))
    return terms


def _job_context_tokens(job_context: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for value in _context_terms(job_context):
        tokens |= _tokenize(value)
    return tokens


def _coerce_job_context(job_context: dict[str, Any] | list[str]) -> dict[str, Any]:
    if isinstance(job_context, list):
        required_skills = _canonicalize_terms(job_context)
        return {
            "job_url": "",
            "job_title": "",
            "job_family": "",
            "domain": "",
            "required_skills": required_skills,
            "preferred_skills": [],
            "responsibilities": [],
            "context_tokens": set().union(*(_tokenize(skill) for skill in required_skills)) if required_skills else set(),
        }

    required_skills = _canonicalize_terms(
        _extract_canonical_entities(job_context.get("required_skill_entities"))
        or list(job_context.get("required_skills_canonical") or [])
        or list(job_context.get("required_skills") or [])
    )
    preferred_skills = _canonicalize_terms(
        _extract_canonical_entities(job_context.get("preferred_skill_entities"))
        or list(job_context.get("preferred_skills_canonical") or [])
        or list(job_context.get("preferred_skills") or [])
    )
    responsibilities = _normalize_text_list(job_context.get("responsibilities"))
    context: dict[str, Any] = {
        "job_url": _normalize_optional_text(job_context.get("job_url")),
        "job_title": _normalize_optional_text(job_context.get("title") or job_context.get("job_title")),
        "job_family": _normalize_optional_text(job_context.get("job_family")),
        "domain": _normalize_optional_text(job_context.get("domain")),
        "required_skills": required_skills,
        "preferred_skills": preferred_skills,
        "responsibilities": responsibilities,
    }
    context["context_tokens"] = _job_context_tokens(context)
    return context


def _preferred_evidence_id(raw: dict[str, Any], fallback_id: str) -> str:
    explicit_id = _normalize_optional_text(raw.get("id"))
    return explicit_id or fallback_id


def _job_domain_text(job_context: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in (
            _normalize_text(job_context.get("domain")),
            _normalize_text(job_context.get("job_family")),
        )
        if part
    )


def _job_required_skill_texts(job_context: dict[str, Any]) -> list[str]:
    required_skills = [skill for skill in list(job_context.get("required_skills") or []) if skill]
    if not required_skills:
        return []
    texts = list(required_skills)
    combined = " ".join(required_skills)
    if combined and combined not in texts:
        texts.append(combined)
    return texts


def _item_required_skill_text(item: dict[str, Any]) -> str:
    parts = [
        *[_normalize_text(value) for value in list(item.get("skills") or [])],
        _normalize_text(item.get("name")),
        _normalize_text(item.get("scoring_context")),
    ]
    return " ".join(part for part in parts if part)


def _job_role_texts(job_context: dict[str, Any]) -> list[str]:
    parts = [
        _normalize_text(job_context.get("job_title")),
        _normalize_text(job_context.get("job_family")),
    ]
    texts = [part for part in parts if part]
    combined = " ".join(texts)
    if combined and combined not in texts:
        texts.append(combined)
    return texts


def _item_role_text(item: dict[str, Any]) -> str:
    parts = [
        _normalize_text(item.get("role") or item.get("name")),
        _normalize_text(item.get("role_family")),
        _normalize_text(item.get("scoring_context")),
    ]
    return " ".join(part for part in parts if part)


def _item_domain_text(item: dict[str, Any]) -> str:
    scoring_context = _normalize_text(item.get("scoring_context"))
    if scoring_context:
        return scoring_context
    return " ".join(
        part
        for part in [
            _normalize_text(item.get("name")),
            *[_normalize_text(value) for value in list(item.get("domain_tags") or [])],
        ]
        if part
    )


def _item_responsibility_text(item: dict[str, Any]) -> str:
    scoring_context = _normalize_text(item.get("scoring_context"))
    if scoring_context:
        return scoring_context
    return " ".join(
        part
        for part in [_normalize_text(value) for value in list(item.get("responsibility_themes") or [])]
        if part
    )


def _semantic_similarity(
    *,
    job_texts: list[str],
    item_text: str,
    config: dict[str, Any],
    model_name: str,
    runtime_state: dict[str, Any],
) -> float:
    if not item_text or not job_texts:
        return 0.0
    item_vector = _embed_text_cached(
        item_text,
        config=config,
        model_name=model_name,
        runtime_state=runtime_state,
        cache_namespace="candidate",
    )
    if not item_vector:
        return 0.0
    best_score = 0.0
    for job_text in job_texts:
        if not job_text:
            continue
        job_vector = _embed_text_cached(
            job_text,
            config=config,
            model_name=model_name,
            runtime_state=runtime_state,
            cache_namespace="job",
        )
        if not job_vector:
            continue
        best_score = max(best_score, _cosine_similarity(item_vector, job_vector))
    return _clamp_score(best_score)


def normalise_evidence_item(
    raw: dict[str, Any],
    evidence_type: str,
    source_ref: str,
) -> dict[str, Any]:
    """Convert a raw profile entry into the canonical evidence item schema."""
    name = _normalize_optional_text(raw.get("name") or raw.get("text"))
    business_value = _normalize_optional_text(raw.get("business_value"))
    skills = _normalize_text_list(raw.get("skills"))
    domain_tags = _canonicalize_terms(_normalize_text_list(raw.get("domain_tags")))
    responsibility_themes = _canonicalize_terms(_normalize_text_list(raw.get("responsibility_themes")))
    role_family = _normalize_text(raw.get("role_family")) or None

    evidence_id = _preferred_evidence_id(raw, _build_evidence_id(evidence_type, source_ref, name))
    return {
        "evidence_id": evidence_id,
        "evidence_type": evidence_type,
        "name": name,
        "skills": skills,
        "business_value": business_value,
        "score": 0.0,
        "source_ref": source_ref,
        "domain_tags": domain_tags,
        "responsibility_themes": responsibility_themes,
        "role_family": role_family,
        "scoring_context": " ".join(part for part in (name, business_value, *domain_tags, *responsibility_themes) if part),
    }


def _sort_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        sorted(items, key=lambda item: str(item.get("name") or "")),
        key=lambda item: (
            float(item.get("score") or 0.0),
            TYPE_WEIGHTS.get(str(item.get("evidence_type") or "achievement"), 0.4),
        ),
        reverse=True,
    )


def _normalise_experience_entry(
    experience: dict[str, Any],
    *,
    experience_index: int,
) -> dict[str, Any]:
    role = _normalize_optional_text(experience.get("role"))
    company = _normalize_optional_text(experience.get("company"))
    name = " — ".join(part for part in (role, company) if part)
    source_ref = f"experiences[{experience_index}]"

    bullet_texts: list[str] = []
    aggregated_skills: list[str] = []
    seen_skills: set[str] = set()
    for bullet in experience.get("bullets") or []:
        if not isinstance(bullet, dict):
            continue
        text = _normalize_optional_text(bullet.get("text") or bullet.get("name"))
        if text:
            bullet_texts.append(text)
        for skill in _normalize_text_list(bullet.get("skills")):
            if skill not in seen_skills:
                seen_skills.add(skill)
                aggregated_skills.append(skill)

    domain_tags = _canonicalize_terms(_normalize_text_list(experience.get("domain_tags")))
    responsibility_themes = _canonicalize_terms(_normalize_text_list(experience.get("responsibility_themes")))
    role_family = _normalize_text(experience.get("role_family")) or infer_role_family(role)
    scoring_parts = [role, company, *bullet_texts, *domain_tags, *responsibility_themes]
    evidence_id = _preferred_evidence_id(
        experience,
        _build_evidence_id("experience_entry", source_ref, name),
    )
    return {
        "evidence_id": evidence_id,
        "evidence_type": "experience_entry",
        "name": name,
        "role": role,
        "company": company,
        "location": _normalize_optional_text(experience.get("location")) or None,
        "start": _normalize_optional_text(experience.get("start")) or None,
        "end": _normalize_optional_text(experience.get("end")) or None,
        "skills": aggregated_skills,
        "bullets": bullet_texts,
        "business_value": " ".join(bullet_texts),
        "score": 0.0,
        "source_ref": source_ref,
        "role_family": role_family,
        "domain_tags": domain_tags,
        "responsibility_themes": responsibility_themes,
        "scoring_context": " ".join(part for part in scoring_parts if part),
    }

def _build_project_scoring_context(
    *,
    name: str,
    business_value: str,
    tech_stack: list[str],
    highlights: list[str],
    domain_tags: list[str],
    responsibility_themes: list[str],
) -> str:
    parts: list[str] = [name]
    if business_value:
        parts.append(business_value)
    parts.extend(tech_stack)
    parts.extend(highlights)
    parts.extend(domain_tags)
    parts.extend(responsibility_themes)
    return " ".join(parts)


def _normalise_project_entry(
    project: dict[str, Any],
    *,
    project_index: int,
) -> dict[str, Any]:
    name = _normalize_optional_text(project.get("name"))
    source_ref = f"projects[{project_index}]"
    business_value = _normalize_optional_text(project.get("business_value"))
    tech_stack = _normalize_text_list(project.get("tech_stack"))
    highlights = _normalize_text_list(project.get("highlights"))
    skills = _normalize_text_list(project.get("skills"))
    domain_tags = _canonicalize_terms(_normalize_text_list(project.get("domain_tags")))
    responsibility_themes = _canonicalize_terms(_normalize_text_list(project.get("responsibility_themes")))
    evidence_id = _preferred_evidence_id(
        project,
        _build_evidence_id("project_entry", source_ref, name),
    )
    scoring_context = _build_project_scoring_context(
        name=name,
        business_value=business_value,
        tech_stack=tech_stack,
        highlights=highlights,
        domain_tags=domain_tags,
        responsibility_themes=responsibility_themes,
    )
    return {
        "evidence_id": evidence_id,
        "evidence_type": "project_entry",
        "name": name,
        "duration": _normalize_optional_text(project.get("duration")) or None,
        "url": _normalize_optional_text(project.get("url")) or None,
        "skills": skills,
        "tech_stack": tech_stack,
        "business_value": business_value,
        "highlights": highlights,
        "scoring_context": scoring_context,
        "score": 0.0,
        "source_ref": source_ref,
        "role_family": infer_role_family(name),
        "domain_tags": domain_tags,
        "responsibility_themes": responsibility_themes,
    }


def score_evidence_item(item: dict[str, Any], jd_skills: list[str]) -> float:
    """Compute a weighted score in [0.0, 1.0] for one normalised evidence item."""
    item_skills = _canonicalize_term_set(list(item.get("skills") or []))
    jd_lower = _canonicalize_term_set(jd_skills)

    if jd_lower and item_skills:
        skill_ratio = len(item_skills & jd_lower) / len(jd_lower)
    else:
        skill_ratio = 0.0

    type_score = TYPE_WEIGHTS.get(str(item.get("evidence_type") or "achievement"), 0.4)

    biz_value = _tokenize(str(item.get("scoring_context") or item.get("business_value") or ""))
    if jd_lower and biz_value:
        biz_ratio = min(len(biz_value & jd_lower) / len(jd_lower), 1.0)
    else:
        biz_ratio = 0.0

    return (
        SKILL_OVERLAP_WEIGHT * skill_ratio
        + TYPE_WEIGHT_FACTOR * type_score
        + BUSINESS_VALUE_WEIGHT * biz_ratio
    )


def _collect_experience_entries(profile: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for experience_index, experience in enumerate(profile.get("experiences") or []):
        if not isinstance(experience, dict):
            continue
        items.append(
            _normalise_experience_entry(
                experience,
                experience_index=experience_index,
            )
        )
    return items


def _collect_project_entries(profile: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for project_index, project in enumerate(profile.get("projects") or []):
        if not isinstance(project, dict):
            continue
        items.append(
            _normalise_project_entry(
                project,
                project_index=project_index,
            )
        )
    return items


def _collect_achievement_entries(profile: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for achievement_index, achievement in enumerate(profile.get("achievements") or []):
        if not isinstance(achievement, dict):
            continue
        items.append(
            normalise_evidence_item(
                achievement,
                "achievement",
                f"achievements[{achievement_index}]",
            )
        )
    return items


def _text_overlap_score(text: str, reference_terms: list[str]) -> int:
    lowered_text = _normalize_text(text)
    return sum(1 for term in reference_terms if _normalize_text(term) in lowered_text)


def _select_relevant_texts(values: list[str], reference_terms: list[str], limit: int) -> list[str]:
    if limit <= 0 or not values:
        return []
    ranked = sorted(
        enumerate(values),
        key=lambda pair: (-_text_overlap_score(pair[1], reference_terms), pair[0]),
    )
    selected = [values[index] for index, _ in ranked[:limit]]
    return selected


def _trim_selected_project_entry(
    item: dict[str, Any],
    reference_terms: list[str],
    *,
    policy: dict[str, Any],
) -> dict[str, Any]:
    trimmed = dict(item)
    trimming = dict(policy.get("trimming") or {})
    trimmed["tech_stack"] = _select_relevant_texts(
        list(item.get("tech_stack") or []),
        reference_terms,
        int(trimming.get("stack_lines_per_project", DEFAULT_STACK_LINES_PER_PROJECT)),
    )
    trimmed["highlights"] = _select_relevant_texts(
        list(item.get("highlights") or []),
        reference_terms,
        int(trimming.get("highlights_per_project", DEFAULT_HIGHLIGHTS_PER_PROJECT)),
    )
    return trimmed


def _trim_selected_experience_entry(
    item: dict[str, Any],
    reference_terms: list[str],
    *,
    policy: dict[str, Any],
) -> dict[str, Any]:
    trimmed = dict(item)
    trimming = dict(policy.get("trimming") or {})
    trimmed["bullets"] = _select_relevant_texts(
        list(item.get("bullets") or []),
        reference_terms,
        int(trimming.get("bullets_per_experience", DEFAULT_BULLETS_PER_EXPERIENCE)),
    )
    return trimmed


def _collect_base_items(profile: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        *_collect_experience_entries(profile),
        *_collect_project_entries(profile),
        *_collect_achievement_entries(profile),
    ]


def _select_budgeted_items(
    *,
    top_k: int,
    reference_terms: list[str],
    experience_items: list[dict[str, Any]],
    project_items: list[dict[str, Any]],
    achievement_items: list[dict[str, Any]],
    policy: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if top_k <= 0:
        return []
    effective_policy = policy or _cv_analysis_policy_settings(None)
    quotas = dict(effective_policy.get("quotas") or {})

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    remaining_slots = top_k

    minimum_experience = 1 if experience_items and remaining_slots > 0 else 0
    minimum_projects = 1 if project_items and remaining_slots > minimum_experience else 0
    reserved_experience = minimum_experience
    remaining_slots -= minimum_experience
    reserved_projects = minimum_projects
    remaining_slots -= minimum_projects

    additional_experience = min(
        max(int(quotas.get("experience_entry_top_k", DEFAULT_EXPERIENCE_ENTRY_TOP_K)) - reserved_experience, 0),
        len(experience_items) - reserved_experience,
        remaining_slots,
    )
    reserved_experience += max(additional_experience, 0)
    remaining_slots -= max(additional_experience, 0)

    additional_projects = min(
        max(int(quotas.get("project_entry_top_k", DEFAULT_PROJECT_ENTRY_TOP_K)) - reserved_projects, 0),
        len(project_items) - reserved_projects,
        remaining_slots,
    )
    reserved_projects += max(additional_projects, 0)
    remaining_slots -= max(additional_projects, 0)

    reserved_achievements = min(
        int(quotas.get("achievement_top_k", DEFAULT_ACHIEVEMENT_TOP_K)),
        len(achievement_items),
        remaining_slots,
    )
    remaining_slots -= reserved_achievements

    for items, limit in (
        (experience_items, reserved_experience),
        (project_items, reserved_projects),
        (achievement_items, reserved_achievements),
        ):
        for item in items[:limit]:
            selected_item = item
            if str(item.get("evidence_type") or "") == "project_entry":
                selected_item = _trim_selected_project_entry(item, reference_terms, policy=effective_policy)
            if str(item.get("evidence_type") or "") == "experience_entry":
                selected_item = _trim_selected_experience_entry(item, reference_terms, policy=effective_policy)
            selected.append(selected_item)
            selected_ids.add(str(item["evidence_id"]))

    if remaining_slots > 0:
        fallback_pool = _sort_items(
            [
                item
                for item in [*experience_items, *project_items, *achievement_items]
                if str(item["evidence_id"]) not in selected_ids
            ]
        )
        for item in fallback_pool[:remaining_slots]:
            selected_item = item
            if str(item.get("evidence_type") or "") == "project_entry":
                selected_item = _trim_selected_project_entry(item, reference_terms, policy=effective_policy)
            if str(item.get("evidence_type") or "") == "experience_entry":
                selected_item = _trim_selected_experience_entry(item, reference_terms, policy=effective_policy)
            selected.append(selected_item)

    return _sort_items(selected)


def _retrieve_evidence_legacy(
    profile: dict[str, Any],
    jd_skills: list[str],
    top_k: int,
) -> list[dict[str, Any]]:
    experience_items = _collect_experience_entries(profile)
    project_items = _collect_project_entries(profile)
    achievement_items = _collect_achievement_entries(profile)

    for item in [*experience_items, *project_items, *achievement_items]:
        item["score"] = score_evidence_item(item, jd_skills)

    return _select_budgeted_items(
        top_k=top_k,
        reference_terms=jd_skills,
        experience_items=_sort_items(experience_items),
        project_items=_sort_items(project_items),
        achievement_items=_sort_items(achievement_items),
    )


def _item_role_family(item: dict[str, Any]) -> str | None:
    explicit_family = _normalize_text(item.get("role_family"))
    if explicit_family:
        return explicit_family
    role_text = _normalize_optional_text(item.get("role") or item.get("name"))
    inferred_family = infer_role_family(role_text)
    return inferred_family


def _score_required_skill_support(item: dict[str, Any], job_context: dict[str, Any]) -> float:
    required_skills = _canonicalize_term_set(list(job_context.get("required_skills") or []))
    if not required_skills:
        return 0.0
    item_skills = _canonicalize_term_set(list(item.get("skills") or []))
    context_tokens = _tokenize(str(item.get("scoring_context") or ""))
    return max(
        _overlap_ratio(item_skills, required_skills),
        _overlap_ratio(context_tokens, required_skills),
    )


def _score_required_skill_support_components(
    item: dict[str, Any],
    job_context: dict[str, Any],
    *,
    config: dict[str, Any] | None,
    semantic_settings: dict[str, Any],
    runtime_state: dict[str, Any],
) -> dict[str, float]:
    lexical_score = _score_required_skill_support(item, job_context)
    semantic_score = 0.0
    if semantic_settings["enabled"] and config is not None:
        semantic_score = _semantic_similarity(
            job_texts=_job_required_skill_texts(job_context),
            item_text=_item_required_skill_text(item),
            config=config,
            model_name=str(semantic_settings["model"]),
            runtime_state=runtime_state,
        )
    return {
        "lexical": round(lexical_score, 6),
        "semantic": round(semantic_score, 6),
        "combined": round(
            _hybrid_score(
                lexical_score,
                semantic_score,
                float(semantic_settings["required_skill_lexical_weight"]),
                float(semantic_settings["required_skill_semantic_weight"]),
            ),
            6,
        ),
    }


def _score_role_alignment(item: dict[str, Any], job_context: dict[str, Any]) -> float:
    job_title = _normalize_optional_text(job_context.get("job_title"))
    job_family = _normalize_text(job_context.get("job_family")) or infer_role_family(job_title)
    item_family = _item_role_family(item)

    family_score = 0.0
    if job_family and item_family:
        if job_family == item_family:
            family_score = 1.0
        elif item_family in _role_family_neighbors().get(job_family, frozenset()):
            family_score = ROLE_ALIGNMENT_NEIGHBOR_SCORE

    lexical_score = _overlap_ratio(
        _tokenize(_normalize_optional_text(item.get("role") or item.get("name"))),
        _tokenize(job_title),
    )
    return max(family_score, lexical_score)


def _score_role_alignment_components(
    item: dict[str, Any],
    job_context: dict[str, Any],
    *,
    config: dict[str, Any] | None,
    semantic_settings: dict[str, Any],
    runtime_state: dict[str, Any],
) -> dict[str, float]:
    lexical_score = _score_role_alignment(item, job_context)
    semantic_score = 0.0
    if semantic_settings["enabled"] and config is not None:
        semantic_score = _semantic_similarity(
            job_texts=_job_role_texts(job_context),
            item_text=_item_role_text(item),
            config=config,
            model_name=str(semantic_settings["model"]),
            runtime_state=runtime_state,
        )
    return {
        "lexical": round(lexical_score, 6),
        "semantic": round(semantic_score, 6),
        "combined": round(
            _hybrid_score(
                lexical_score,
                semantic_score,
                float(semantic_settings["role_lexical_weight"]),
                float(semantic_settings["role_semantic_weight"]),
            ),
            6,
        ),
    }


def _score_domain_alignment_lexical(item: dict[str, Any], job_context: dict[str, Any]) -> float:
    domain_terms = _canonicalize_term_set(
        [
            _normalize_optional_text(job_context.get("domain")),
            _normalize_optional_text(job_context.get("job_family")),
        ]
    )
    if not domain_terms:
        return 0.0
    item_domain_tags = _canonicalize_term_set(list(item.get("domain_tags") or []))
    if item_domain_tags:
        return max(
            _overlap_ratio(item_domain_tags, domain_terms),
            _overlap_ratio(domain_terms, item_domain_tags),
        )
    return _overlap_ratio(_tokenize(str(item.get("scoring_context") or "")), domain_terms)


def _score_responsibility_alignment_lexical(item: dict[str, Any], job_context: dict[str, Any]) -> float:
    responsibilities = list(job_context.get("responsibilities") or [])
    if not responsibilities:
        return 0.0
    responsibility_tokens = set().union(*(_tokenize(text) for text in responsibilities))
    if not responsibility_tokens:
        return 0.0
    theme_tokens = set().union(*(_tokenize(theme) for theme in item.get("responsibility_themes") or []))
    context_tokens = _tokenize(str(item.get("scoring_context") or ""))
    return max(
        _overlap_ratio(theme_tokens, responsibility_tokens),
        _overlap_ratio(context_tokens, responsibility_tokens),
    )


def _hybrid_score(lexical_score: float, semantic_score: float, lexical_weight: float, semantic_weight: float) -> float:
    return _clamp_score((lexical_score * lexical_weight) + (semantic_score * semantic_weight))


def _score_domain_alignment_components(
    item: dict[str, Any],
    job_context: dict[str, Any],
    *,
    config: dict[str, Any] | None,
    semantic_settings: dict[str, Any],
    runtime_state: dict[str, Any],
) -> dict[str, float]:
    lexical_score = _score_domain_alignment_lexical(item, job_context)
    semantic_score = 0.0
    if semantic_settings["enabled"] and config is not None:
        semantic_score = _semantic_similarity(
            job_texts=[_job_domain_text(job_context)],
            item_text=_item_domain_text(item),
            config=config,
            model_name=str(semantic_settings["model"]),
            runtime_state=runtime_state,
        )
    return {
        "lexical": round(lexical_score, 6),
        "semantic": round(semantic_score, 6),
        "combined": round(
            _hybrid_score(
                lexical_score,
                semantic_score,
                float(semantic_settings["domain_lexical_weight"]),
                float(semantic_settings["domain_semantic_weight"]),
            ),
            6,
        ),
    }


def _score_responsibility_alignment_components(
    item: dict[str, Any],
    job_context: dict[str, Any],
    *,
    config: dict[str, Any] | None,
    semantic_settings: dict[str, Any],
    runtime_state: dict[str, Any],
) -> dict[str, float]:
    lexical_score = _score_responsibility_alignment_lexical(item, job_context)
    semantic_score = 0.0
    if semantic_settings["enabled"] and config is not None:
        semantic_score = _semantic_similarity(
            job_texts=[str(value) for value in list(job_context.get("responsibilities") or []) if value],
            item_text=_item_responsibility_text(item),
            config=config,
            model_name=str(semantic_settings["model"]),
            runtime_state=runtime_state,
        )
    return {
        "lexical": round(lexical_score, 6),
        "semantic": round(semantic_score, 6),
        "combined": round(
            _hybrid_score(
                lexical_score,
                semantic_score,
                float(semantic_settings["responsibility_lexical_weight"]),
                float(semantic_settings["responsibility_semantic_weight"]),
            ),
            6,
        ),
    }


def _channel_score_components(
    item: dict[str, Any],
    channel: str,
    job_context: dict[str, Any],
    *,
    config: dict[str, Any] | None,
    semantic_settings: dict[str, Any],
    runtime_state: dict[str, Any],
) -> dict[str, float]:
    if channel == REQUIRED_SKILL_SUPPORT_CHANNEL:
        return _score_required_skill_support_components(
            item,
            job_context,
            config=config,
            semantic_settings=semantic_settings,
            runtime_state=runtime_state,
        )
    if channel == ROLE_ALIGNMENT_CHANNEL:
        return _score_role_alignment_components(
            item,
            job_context,
            config=config,
            semantic_settings=semantic_settings,
            runtime_state=runtime_state,
        )
    if channel == DOMAIN_ALIGNMENT_CHANNEL:
        return _score_domain_alignment_components(
            item,
            job_context,
            config=config,
            semantic_settings=semantic_settings,
            runtime_state=runtime_state,
        )
    if channel == RESPONSIBILITY_ALIGNMENT_CHANNEL:
        return _score_responsibility_alignment_components(
            item,
            job_context,
            config=config,
            semantic_settings=semantic_settings,
            runtime_state=runtime_state,
        )
    return {"lexical": 0.0, "semantic": 0.0, "combined": 0.0}


def _channel_rationale(channel: str, item: dict[str, Any], job_context: dict[str, Any]) -> list[str]:
    if channel == REQUIRED_SKILL_SUPPORT_CHANNEL:
        required_skills = _canonicalize_term_set(list(job_context.get("required_skills") or []))
        item_skills = _canonicalize_term_set(list(item.get("skills") or []))
        matched = sorted(required_skills & item_skills)
        return matched[:3]
    if channel == ROLE_ALIGNMENT_CHANNEL:
        item_family = _item_role_family(item)
        job_family = _normalize_text(job_context.get("job_family"))
        reasons: list[str] = []
        if item_family and job_family and item_family == job_family:
            reasons.append(f"role_family:{job_family}")
        role_name = _normalize_optional_text(item.get("role") or item.get("name"))
        if role_name:
            reasons.append(role_name)
        return reasons[:3]
    if channel == DOMAIN_ALIGNMENT_CHANNEL:
        matched_domains = sorted(
            _canonicalize_term_set(list(item.get("domain_tags") or []))
            & _canonicalize_term_set(
                [
                    _normalize_optional_text(job_context.get("domain")),
                    _normalize_optional_text(job_context.get("job_family")),
                ]
            )
        )
        return matched_domains[:3]
    if channel == RESPONSIBILITY_ALIGNMENT_CHANNEL:
        themes = [str(theme) for theme in list(item.get("responsibility_themes") or []) if theme]
        if themes:
            return themes[:3]
        return _select_relevant_texts(
            list(item.get("bullets") or item.get("highlights") or []),
            list(job_context.get("responsibilities") or []),
            1,
        )
    return []


def _select_channel_candidates(
    *,
    items: list[dict[str, Any]],
    channel: str,
    job_context: dict[str, Any],
    pool_size: int,
    config: dict[str, Any] | None,
    semantic_settings: dict[str, Any],
    runtime_state: dict[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for item in items:
        channel_subscore = _channel_score_components(
            item,
            channel,
            job_context,
            config=config,
            semantic_settings=semantic_settings,
            runtime_state=runtime_state,
        )
        channel_score = float(channel_subscore["combined"])
        if channel_score <= 0.0:
            continue
        candidates.append(
            {
                **item,
                "channel": channel,
                "channel_score": channel_score,
                "channel_subscore": channel_subscore,
                "channel_rationale": _channel_rationale(channel, item, job_context),
            }
        )
    return sorted(
        candidates,
        key=lambda item: (
            float(item.get("channel_score") or 0.0),
            TYPE_WEIGHTS.get(str(item.get("evidence_type") or ""), 0.0),
            str(item.get("name") or ""),
        ),
        reverse=True,
    )[:pool_size]


def _merge_channel_pools(channel_pools: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    merged_by_id: dict[str, dict[str, Any]] = {}
    for channel, pool in channel_pools.items():
        for item in pool:
            evidence_id = str(item.get("evidence_id") or "")
            if not evidence_id:
                continue
            existing = merged_by_id.get(evidence_id)
            if existing is None:
                merged_by_id[evidence_id] = {
                    key: value
                    for key, value in item.items()
                    if key not in {"channel", "channel_score", "channel_subscore", "channel_rationale"}
                }
                existing = merged_by_id[evidence_id]
                existing["matched_channels"] = []
                existing["channel_scores"] = {}
                existing["channel_subscores"] = {}
                existing["channel_rationales"] = {}
            matched_channels = list(existing.get("matched_channels") or [])
            if channel not in matched_channels:
                matched_channels.append(channel)
            existing["matched_channels"] = matched_channels
            channel_scores = dict(existing.get("channel_scores") or {})
            channel_scores[channel] = float(item.get("channel_score") or 0.0)
            existing["channel_scores"] = channel_scores
            channel_subscores = dict(existing.get("channel_subscores") or {})
            channel_subscores[channel] = dict(item.get("channel_subscore") or {})
            existing["channel_subscores"] = channel_subscores
            channel_rationales = dict(existing.get("channel_rationales") or {})
            channel_rationales[channel] = list(item.get("channel_rationale") or [])
            existing["channel_rationales"] = channel_rationales
    return sorted(
        merged_by_id.values(),
        key=lambda item: (
            sum(float(score) for score in dict(item.get("channel_scores") or {}).values()),
            len(list(item.get("matched_channels") or [])),
            TYPE_WEIGHTS.get(str(item.get("evidence_type") or ""), 0.0),
            str(item.get("name") or ""),
        ),
        reverse=True,
    )


def _base_selection_score(item: dict[str, Any], *, policy: dict[str, Any]) -> float:
    channel_weights = dict(policy.get("channel_weights") or {})
    channel_scores = dict(item.get("channel_scores") or {})
    weighted_score = sum(
        float(channel_scores.get(channel) or 0.0) * float(channel_weights.get(channel, 0.0))
        for channel in RETRIEVAL_CHANNELS
    )
    matched_channels = list(item.get("matched_channels") or [])
    multi_channel_bonus = max(len(matched_channels) - 1, 0) * float(policy.get("multi_channel_bonus", 0.0))
    type_bonus = TYPE_WEIGHTS.get(str(item.get("evidence_type") or ""), 0.0) * float(policy.get("type_weight_factor", 0.0))
    return weighted_score + multi_channel_bonus + type_bonus


def _coverage_gain(
    item: dict[str, Any],
    covered_channel_scores: dict[str, float],
    *,
    policy: dict[str, Any],
) -> float:
    channel_weights = dict(policy.get("channel_weights") or {})
    channel_scores = dict(item.get("channel_scores") or {})
    return sum(
        max(float(channel_scores.get(channel) or 0.0) - covered_channel_scores.get(channel, 0.0), 0.0)
        * float(channel_weights.get(channel, 0.0))
        for channel in RETRIEVAL_CHANNELS
    )


def _selection_reasons(item: dict[str, Any]) -> list[str]:
    channel_scores = dict(item.get("channel_scores") or {})
    ordered_channels = sorted(
        list(item.get("matched_channels") or []),
        key=lambda channel: (-float(channel_scores.get(channel) or 0.0), channel),
    )
    return ordered_channels[:3]


def _reference_terms(job_context: dict[str, Any]) -> list[str]:
    terms: list[str] = []
    terms.extend(list(job_context.get("required_skills") or []))
    terms.extend(list(job_context.get("preferred_skills") or []))
    terms.extend(list(job_context.get("responsibilities") or []))
    title = _normalize_optional_text(job_context.get("job_title"))
    if title:
        terms.append(title)
    domain = _normalize_optional_text(job_context.get("domain"))
    if domain:
        terms.append(domain)
    job_family = _normalize_optional_text(job_context.get("job_family"))
    if job_family:
        terms.append(job_family)
    return terms


def _finalize_selected_item(item: dict[str, Any], job_context: dict[str, Any], *, policy: dict[str, Any]) -> dict[str, Any]:
    reference_terms = _reference_terms(job_context)
    finalized = dict(item)
    if str(item.get("evidence_type") or "") == "project_entry":
        finalized = _trim_selected_project_entry(finalized, reference_terms, policy=policy)
    if str(item.get("evidence_type") or "") == "experience_entry":
        finalized = _trim_selected_experience_entry(finalized, reference_terms, policy=policy)
    return finalized


def _debug_candidate_sample(item: dict[str, Any]) -> dict[str, Any]:
    sample: dict[str, Any] = {
        "evidence_id": str(item.get("evidence_id") or ""),
        "evidence_type": str(item.get("evidence_type") or ""),
        "source_ref": str(item.get("source_ref") or ""),
        "name": str(item.get("name") or ""),
        "matched_channels": list(item.get("matched_channels") or []),
        "selection_reasons": list(item.get("selection_reasons") or []),
        "selection_score": round(float(item.get("selection_score") or 0.0), 6),
    }
    channel_subscores = dict(item.get("channel_subscores") or {})
    if channel_subscores:
        sample["channel_subscores"] = channel_subscores
    return {
        key: value
        for key, value in sample.items()
        if value not in ("", None, [], {})
    }


def _select_final_evidence(
    merged_pool: list[dict[str, Any]],
    *,
    top_k: int,
    job_context: dict[str, Any],
    policy: dict[str, Any],
) -> list[dict[str, Any]]:
    if top_k <= 0:
        return []

    selected: list[dict[str, Any]] = []
    selected_types: list[str] = []
    covered_channel_scores = {channel: 0.0 for channel in RETRIEVAL_CHANNELS}
    remaining = list(merged_pool)
    while remaining and len(selected) < top_k:
        best_index = -1
        best_score = -1.0
        for index, item in enumerate(remaining):
            evidence_type = str(item.get("evidence_type") or "")
            dynamic_score = (
                _coverage_gain(item, covered_channel_scores, policy=policy)
                + (_base_selection_score(item, policy=policy) * float(policy.get("residual_score_factor", 0.0)))
            )
            if evidence_type and evidence_type not in selected_types:
                dynamic_score += float(policy.get("new_type_bonus", 0.0))
            dynamic_score -= selected_types.count(evidence_type) * float(policy.get("same_type_penalty", 0.0))
            if dynamic_score > best_score:
                best_score = dynamic_score
                best_index = index
        if best_index < 0:
            break
        chosen = dict(remaining.pop(best_index))
        evidence_type = str(chosen.get("evidence_type") or "")
        selected_types.append(evidence_type)
        channel_scores = dict(chosen.get("channel_scores") or {})
        for channel in RETRIEVAL_CHANNELS:
            covered_channel_scores[channel] = max(
                covered_channel_scores[channel],
                float(channel_scores.get(channel) or 0.0),
            )
        chosen["selection_score"] = round(best_score, 6)
        chosen["selection_reasons"] = _selection_reasons(chosen)
        selected.append(_finalize_selected_item(chosen, job_context, policy=policy))
    return selected


def _top_unselected_candidates(
    merged_pool: list[dict[str, Any]],
    selected_evidence: list[dict[str, Any]],
    *,
    limit: int = 3,
    policy: dict[str, Any],
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    selected_ids = {
        str(item.get("evidence_id") or "")
        for item in selected_evidence
        if str(item.get("evidence_id") or "")
    }
    unselected = [
        item for item in merged_pool
        if str(item.get("evidence_id") or "") not in selected_ids
    ]
    ranked_unselected = sorted(
        unselected,
        key=lambda item: (
            float(item.get("selection_score") or _base_selection_score(item, policy=policy)),
            len(list(item.get("matched_channels") or [])),
            TYPE_WEIGHTS.get(str(item.get("evidence_type") or ""), 0.0),
            str(item.get("name") or ""),
        ),
        reverse=True,
    )
    return [_debug_candidate_sample(item) for item in ranked_unselected[:limit]]


def retrieve_evidence_bundle(
    profile: dict[str, Any],
    job_context: dict[str, Any] | list[str],
    top_k: int,
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Retrieve evidence via separate channels, then merge/dedupe/select."""
    coerced_job_context = _coerce_job_context(job_context)
    base_items = _collect_base_items(profile)
    selection_policy = _cv_analysis_policy_settings(config)
    semantic_settings = _semantic_alignment_settings(config)
    runtime_state = _semantic_runtime_state()
    channel_pools = {
        channel: _select_channel_candidates(
            items=base_items,
            channel=channel,
            job_context=coerced_job_context,
            pool_size=int(semantic_settings["channel_pool_size"]),
            config=config,
            semantic_settings=semantic_settings,
            runtime_state=runtime_state,
        )
        for channel in RETRIEVAL_CHANNELS
    }
    merged_pool = _merge_channel_pools(channel_pools)
    selected_evidence = _select_final_evidence(
        merged_pool,
        top_k=top_k,
        job_context=coerced_job_context,
        policy=selection_policy,
    )
    semantic_alignment = {
        "enabled": bool(semantic_settings["enabled"]),
        "semantic_methods": _semantic_methods(bool(semantic_settings["enabled"])),
        "reuse_state": _semantic_reuse_state(runtime_state),
        "embedding_counts": _semantic_embedding_counts(runtime_state),
    }
    hybrid_alignment = {
        "required_skill_support": {
            "lexical_weight": round(float(semantic_settings["required_skill_lexical_weight"]), 6),
            "semantic_weight": round(float(semantic_settings["required_skill_semantic_weight"]), 6),
        },
        "role_alignment": {
            "lexical_weight": round(float(semantic_settings["role_lexical_weight"]), 6),
            "semantic_weight": round(float(semantic_settings["role_semantic_weight"]), 6),
        },
        "responsibility": {
            "lexical_weight": round(float(semantic_settings["responsibility_lexical_weight"]), 6),
            "semantic_weight": round(float(semantic_settings["responsibility_semantic_weight"]), 6),
        },
        "domain": {
            "lexical_weight": round(float(semantic_settings["domain_lexical_weight"]), 6),
            "semantic_weight": round(float(semantic_settings["domain_semantic_weight"]), 6),
        },
    }
    for item in selected_evidence:
        item["semantic_alignment"] = dict(semantic_alignment)
    return {
        "selected_evidence": selected_evidence,
        "selected_evidence_ids": [str(item.get("evidence_id") or "") for item in selected_evidence],
        "channel_counts": {
            channel: len(channel_pools.get(channel, []))
            for channel in RETRIEVAL_CHANNELS
        },
        "effective_channel_pool_size": int(semantic_settings["channel_pool_size"]),
        "merged_pool_size": sum(len(pool) for pool in channel_pools.values()),
        "deduped_pool_size": len(merged_pool),
        "selected_evidence_count": len(selected_evidence),
        "unselected_top_candidates": _top_unselected_candidates(
            merged_pool,
            selected_evidence,
            policy=selection_policy,
        ),
        "hybrid_alignment": hybrid_alignment,
        "semantic_alignment": semantic_alignment,
        "selection_policy": selection_policy,
    }


def retrieve_evidence(
    profile: dict[str, Any],
    job_context: dict[str, Any] | list[str] | None = None,
    top_k: int = 0,
    *,
    jd_skills: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Compatibility wrapper that returns the selected evidence list only."""
    if jd_skills is not None or isinstance(job_context, list) or job_context is None:
        resolved_skills = list(jd_skills or job_context or [])
        return _retrieve_evidence_legacy(profile, resolved_skills, top_k)

    resolved_context: dict[str, Any] | list[str]
    resolved_context = job_context
    return list(
        retrieve_evidence_bundle(
            profile,
            resolved_context,
            top_k,
        ).get("selected_evidence")
        or []
    )


def _local_sqlite_path() -> str:
    return str(os.environ.get("FITCV_CP_SQLITE_PATH") or "data/fitcv_cp.sqlite3").strip() or "data/fitcv_cp.sqlite3"



def _ensure_local_evidence_selections_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS evidence_selections (
            job_url TEXT NOT NULL,
            evidence_id TEXT NOT NULL,
            evidence_type TEXT NOT NULL,
            name TEXT NOT NULL,
            skills_json TEXT NOT NULL,
            business_value TEXT NOT NULL,
            score REAL NOT NULL,
            source_ref TEXT NOT NULL,
            selected_at TEXT NOT NULL,
            PRIMARY KEY (job_url, evidence_id)
        )
        """
    )
    conn.commit()



def store_evidence_selection(
    job_url: str,
    evidence: list[dict[str, Any]],
    config: dict[str, Any],
) -> None:
    """Insert evidence selection rows into fitcv.evidence_selections."""
    if not evidence:
        return

    now = datetime.now(tz=timezone.utc).isoformat()

    if sqlite_mode_enabled(config):
        db_path = Path(_local_sqlite_path())
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            _ensure_local_evidence_selections_table(conn)
            conn.executemany(
                """
                INSERT INTO evidence_selections(
                    job_url,
                    evidence_id,
                    evidence_type,
                    name,
                    skills_json,
                    business_value,
                    score,
                    source_ref,
                    selected_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_url, evidence_id) DO UPDATE SET
                    evidence_type = excluded.evidence_type,
                    name = excluded.name,
                    skills_json = excluded.skills_json,
                    business_value = excluded.business_value,
                    score = excluded.score,
                    source_ref = excluded.source_ref,
                    selected_at = excluded.selected_at
                """,
                [
                    (
                        str(job_url),
                        str(item["evidence_id"]),
                        str(item["evidence_type"]),
                        str(item["name"]),
                        json.dumps(list(item.get("skills") or []), ensure_ascii=False),
                        str(item.get("business_value") or ""),
                        float(item.get("selection_score") or item.get("score") or 0.0),
                        str(item["source_ref"]),
                        now,
                    )
                    for item in evidence
                ],
            )
            conn.commit()
        return

    from google.cloud import bigquery  # type: ignore[import-not-found]
    from google.oauth2 import service_account  # type: ignore[import-not-found]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    if key_path:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bigquery.Client(project=project, credentials=credentials)
    else:
        client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.evidence_selections"

    rows = [
        {
            "job_url": str(job_url),
            "evidence_id": str(item["evidence_id"]),
            "evidence_type": str(item["evidence_type"]),
            "name": str(item["name"]),
            "skills": list(item.get("skills") or []),
            "business_value": str(item.get("business_value") or ""),
            "score": float(item.get("selection_score") or item.get("score") or 0.0),
            "source_ref": str(item["source_ref"]),
            "selected_at": now,
        }
        for item in evidence
    ]

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for evidence_selections: {errors}")
