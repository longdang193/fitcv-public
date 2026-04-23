"""
@meta
name: fitcv_enrich
type: utility
domain: enrich
responsibility:
  - Parse structured job extraction responses and normalize enriched job records.
  - Fingerprint raw jobs and enrich contracts for safe structured job reuse.
inputs:
  - scraped job records
  - enrich prompt/config contracts
  - structured LLM extraction responses
outputs:
  - enriched structured job records
  - reusable structured-job lookup and persistence payloads
capabilities:
  - pipeline_performance.gemini-structured-output-with-response-schema-and-pydantic
  - pipeline_performance.fallback-path-for-unparseable-responses
  - pipeline_performance.enrich-extraction-prompt-text-now-comes-from-a-centralized-prompt-registry-with-config-selected-prompt-ids
  - pipeline_performance.enrich-stage-raw-plus-canonical-semantic-companions-for-repeated-downstream-fields
  - pipeline_performance.canonical-skill-companion-lists-and-entity-payloads-for-required-preferred-skills
  - pipeline_performance.enrich-stage-mapping-suggestion-capture-for-review-debug-surfaces
  - pipeline_performance.fingerprint-based-enrich-result-reuse-happens-before-llm-enrichment-using-normalized-raw-job-inputs
  - pipeline_performance.enrich-contract-fingerprinting-invalidates-reuse-automatically-when-prompt-model-schema-behavior-changes
  - pipeline_performance.shared-structured-jobs-reuse-lookup-avoids-redundant-enrich-calls-while-only-fresh-rows-are-upserted-back-into-the-shared-table
tags:
  - enrich
  - reuse
lifecycle:
  status: active
"""

import json
import hashlib
import logging
import os
import re
import threading
from datetime import datetime, timezone
from typing import Any, TypedDict

from pydantic import BaseModel as _BaseModel, Field as _Field
from fitcv.config import get_gemini_model
from fitcv.prompts import get_prompt_definition, render_prompt

logger = logging.getLogger(__name__)

# ── global rate limiter ──────────────────────────────────────────────────────
# Acquired around every enrich_job call so that concurrent chunks cannot
# exceed one API request per enrichment_sleep_secs interval globally.
# Per-chunk sleep alone is NOT a true rate limiter when concurrency > 1.
_ENRICH_RATE_LOCK: threading.Lock = threading.Lock()

# ── enum definitions (fallbacks — overridden by taxonomy.yaml via config) ──────

_FALLBACK_LOCATION_TYPES: frozenset[str] = frozenset({"remote", "hybrid", "onsite"})
_FALLBACK_SENIORITY_ENRICH: frozenset[str] = frozenset({"junior", "mid", "senior", "lead"})


def _get_valid_location_types(config: dict | None) -> frozenset[str]:
    if config:
        vals = config.get("valid_location_types")
        if vals:
            return frozenset(str(v).lower() for v in vals)
    return _FALLBACK_LOCATION_TYPES


def _get_valid_seniority_enrich(config: dict | None) -> frozenset[str]:
    if config:
        vals = config.get("valid_seniority_enrich")
        if vals:
            return frozenset(str(v).lower() for v in vals)
    return _FALLBACK_SENIORITY_ENRICH

# ── schema: which fields are arrays vs scalars ─────────────────────────────────

_ARRAY_FIELDS: frozenset[str] = frozenset({
    "required_skills",
    "preferred_skills",
    "responsibilities",
    "tech_stack",
    "keywords",
})
_CANONICAL_SKILL_FIELDS: frozenset[str] = frozenset({
    "required_skills",
    "preferred_skills",
})

_SCALAR_FIELDS: frozenset[str] = frozenset({
    "location_type",
    "seniority",
    "domain",
    "job_family",
    "years_experience_min",
    "years_experience_max",
})

_KNOWN_FIELDS: frozenset[str] = _ARRAY_FIELDS | _SCALAR_FIELDS
_LANGUAGE_CANONICALS: frozenset[str] = frozenset({
    "english",
    "german",
    "french",
    "spanish",
    "italian",
    "dutch",
    "portuguese",
    "polish",
})
_NON_SKILL_CANONICAL_EXACT: frozenset[str] = frozenset({
    "analytical thinking",
    "analytical skills",
    "attention to detail",
    "proactiveness",
    "solution-oriented approach",
    "communication skills",
    "ownership",
    "data presentation",
    "actionable insights",
    "business model analysis",
    "value chain analysis",
    "technical systems understanding",
    "complex operations management",
    "performance driver analysis",
    "telecommunications domain knowledge",
    "mvne domain knowledge",
    "mvno domain knowledge",
    "sme financing",
    "data handling",
    "business case development",
    "benchmarking",
    "strategic analysis",
    "end-to-end project management",
})
_NON_SKILL_PHRASE_MARKERS: tuple[str, ...] = (
    "domain experience",
    "domain knowledge",
    "language",
    "communication",
    "attention to detail",
    "ownership",
    "proactive",
    "solution-oriented",
    "analytical focus",
    "analytical ability",
    "analytical skills",
    "problem-solving",
    "business model",
    "value chain",
    "performance driver",
    "technical systems",
    "complex operations",
    "actionable recommendation",
    "actionable insight",
    "present complex insights",
    "data presentation",
    "high degree of ownership",
    "soft skill",
)


class SkillEntity(TypedDict):
    raw_text: str
    canonical: str
    confidence: float


class SkillEntityOutput(_BaseModel):
    raw_text: str
    canonical: str
    confidence: float | None = None


class MappingSuggestion(TypedDict):
    must_have_skill: str
    matches: bool
    alias: str
    canonical: str
    confidence: float


class RawJobFingerprintPayload(TypedDict):
    fingerprint_version: str
    job_url: str
    title: str | None
    company_name: str | None
    location: str | None
    description_cleaned: str | None
    contract_type: str | None
    experience_level: str | None
    source: str | None


class RawJobFingerprintResult(TypedDict):
    payload: RawJobFingerprintPayload
    fingerprint: str


class EnrichContractFingerprintPayload(TypedDict):
    contract_version: str
    prompt_id: str
    prompt_version: str
    template_path: str
    model: str
    response_schema_version: str
    skill_postprocessing_version: str


class EnrichContractFingerprintResult(TypedDict):
    payload: EnrichContractFingerprintPayload
    fingerprint: str


RAW_JOB_FINGERPRINT_VERSION = "raw_job_fingerprint_v1"
ENRICH_RESPONSE_SCHEMA_VERSION = "enrichment_output_v1"
ENRICH_SKILL_POSTPROCESSING_VERSION = "canonical_skill_entities_v1"
ENRICH_CONTRACT_VERSION = "enrich_contract_v1"
FRESH_ENRICHMENT_STATUS = "fresh_enrichment"
REUSED_CACHED_ENRICHMENT_STATUS = "reused_cached_enrichment"

# ── Markdown fence stripper ───────────────────────────────────────────────────

_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)\s*```", re.IGNORECASE)
_MISSING_STRING_COMMA_RE = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"\s+"([^"\\]*(?:\\.[^"\\]*)*)"')


def _strip_markdown_fences(text: str) -> str:
    """Strip ```json ... ``` or ``` ... ``` fences if present."""
    match = _FENCE_RE.search(text)
    return match.group(1).strip() if match else text.strip()


def _repair_common_json_issues(text: str) -> str:
    """Repair a small set of common model JSON mistakes before giving up."""
    repaired = text
    while True:
        updated = _MISSING_STRING_COMMA_RE.sub(r'"\1", "\2"', repaired)
        if updated == repaired:
            return repaired
        repaired = updated


# ── field coercion ────────────────────────────────────────────────────────────

def _normalize_enum(value: Any, valid: frozenset[str]) -> str | None:
    if not isinstance(value, str):
        return None
    lowered = value.lower().strip()
    return lowered if lowered in valid else None


def _normalize_text_item(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _get_skill_synonyms(config: dict | None) -> dict[str, str]:
    raw_synonyms = (config or {}).get("skill_synonyms")
    if not isinstance(raw_synonyms, dict):
        return {}
    return {
        str(alias).strip().lower(): str(canonical).strip().lower()
        for alias, canonical in raw_synonyms.items()
        if str(alias).strip() and str(canonical).strip()
    }


def _canonicalize_text_item(field_name: str, raw_text: str, config: dict | None) -> str:
    normalized = raw_text.strip().lower()
    if field_name in _CANONICAL_SKILL_FIELDS:
        return _get_skill_synonyms(config).get(normalized, normalized)
    return normalized


def _normalize_array_values(values: list[Any] | None) -> list[str]:
    normalized: list[str] = []
    for value in values or []:
        text = _normalize_text_item(value)
        if text is not None:
            normalized.append(text)
    return normalized


def _build_canonical_list(raw_values: list[str], field_name: str, config: dict | None) -> list[str]:
    return [_canonicalize_text_item(field_name, raw_value, config) for raw_value in raw_values]


def _normalise_confidence(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        numeric = float(value)
        if 0.0 <= numeric <= 1.0:
            return numeric
    return None


def _canonical_from_entities(entities: list[SkillEntity]) -> list[str]:
    seen: set[str] = set()
    canonical_values: list[str] = []
    for entity in entities:
        canonical = str(entity.get("canonical") or "").strip().lower()
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        canonical_values.append(canonical)
    return canonical_values


def _is_generic_ai_concept_overreach(raw_text: str, canonical: str) -> bool:
    raw_lower = raw_text.strip().lower()
    canonical_lower = canonical.strip().lower()
    if canonical_lower not in {"genai", "generative ai"}:
        return False
    return "genai" not in raw_lower and "generative ai" not in raw_lower


def _is_allowed_skill_entity(raw_text: str, canonical: str) -> bool:
    raw_lower = raw_text.strip().lower()
    canonical_lower = canonical.strip().lower()
    if not raw_lower or not canonical_lower:
        return False
    if canonical_lower in _LANGUAGE_CANONICALS:
        return False
    if canonical_lower in _NON_SKILL_CANONICAL_EXACT:
        return False
    if any(marker in raw_lower or marker in canonical_lower for marker in _NON_SKILL_PHRASE_MARKERS):
        return False
    if _is_generic_ai_concept_overreach(raw_text, canonical):
        return False
    return True


def _is_reusable_skill_alias(raw_text: str, canonical: str) -> bool:
    normalized_alias = raw_text.strip().lower()
    normalized_canonical = canonical.strip().lower()
    if not normalized_alias or not normalized_canonical or normalized_alias == normalized_canonical:
        return False
    if len(normalized_alias) > 40:
        return False
    if len(normalized_alias.split()) > 4:
        return False
    if any(char in normalized_alias for char in ",;:()[]{}"):
        return False
    return True


def _normalise_skill_entities(
    raw_entities: list[Any] | None,
    *,
    config: dict | None,
) -> list[SkillEntity]:
    entities: list[SkillEntity] = []
    for raw_entity in raw_entities or []:
        if not isinstance(raw_entity, dict):
            continue
        raw_text = _normalize_text_item(raw_entity.get("raw_text"))
        canonical_raw = _normalize_text_item(raw_entity.get("canonical"))
        if raw_text is None or canonical_raw is None:
            continue
        canonical = _get_skill_synonyms(config).get(canonical_raw.strip().lower(), canonical_raw.strip().lower())
        if not _is_allowed_skill_entity(raw_text, canonical):
            continue
        confidence = _normalise_confidence(raw_entity.get("confidence"))
        entities.append(
            {
                "raw_text": raw_text,
                "canonical": canonical,
                "confidence": confidence if confidence is not None else 1.0,
            }
        )
    return entities


def _build_skill_entities(
    raw_values: list[str],
    field_name: str,
    config: dict | None,
    *,
    raw_entities: list[Any] | None = None,
) -> list[SkillEntity]:
    if field_name not in {"required_skills", "preferred_skills"}:
        return []
    normalized_entities = _normalise_skill_entities(raw_entities, config=config)
    if normalized_entities:
        return normalized_entities
    fallback_entities: list[SkillEntity] = []
    for raw_value in raw_values:
        normalized_alias = raw_value.strip().lower()
        canonical = _canonicalize_text_item(field_name, raw_value, config)
        if not _is_reusable_skill_alias(raw_value, canonical):
            continue
        if not _is_allowed_skill_entity(raw_value, canonical):
            continue
        fallback_entities.append(
            {
                "raw_text": raw_value,
                "canonical": canonical,
                "confidence": 1.0,
            }
        )
    return fallback_entities


def _build_mapping_suggestions(
    *,
    entities: list[SkillEntity],
) -> list[MappingSuggestion]:
    suggestions: list[MappingSuggestion] = []
    seen_pairs: set[tuple[str, str]] = set()
    for entity in entities:
        raw_value = str(entity.get("raw_text") or "")
        canonical = str(entity.get("canonical") or "").strip().lower()
        normalized_alias = raw_value.strip().lower()
        if not _is_reusable_skill_alias(raw_value, canonical):
            continue
        pair = (normalized_alias, canonical)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        suggestions.append(
            {
                "must_have_skill": canonical,
                "matches": True,
                "alias": normalized_alias,
                "canonical": canonical,
                "confidence": float(entity.get("confidence") or 1.0),
            }
        )
    return suggestions


def _build_array_companions(
    *,
    field_name: str,
    raw_values: list[str],
    config: dict | None,
    raw_entities: list[Any] | None = None,
) -> dict[str, Any]:
    companions: dict[str, Any] = {field_name: raw_values}
    if field_name in _CANONICAL_SKILL_FIELDS:
        skill_entities = _build_skill_entities(
            raw_values,
            field_name,
            config,
            raw_entities=raw_entities,
        )
        companions[f"{field_name}_canonical"] = _canonical_from_entities(skill_entities)
        singular_prefix = "required_skill" if field_name == "required_skills" else "preferred_skill"
        companions[f"{singular_prefix}_entities"] = skill_entities
    return companions


def _coerce_field(key: str, value: Any, config: dict | None = None) -> Any:
    """Coerce a raw LLM value to its canonical Python type."""
    if key in _ARRAY_FIELDS:
        if value is None or not isinstance(value, list):
            return []
        return _normalize_array_values(value)

    if key == "location_type":
        return _normalize_enum(value, _get_valid_location_types(config))

    if key == "seniority":
        return _normalize_enum(value, _get_valid_seniority_enrich(config))

    if key in ("years_experience_min", "years_experience_max"):
        if isinstance(value, (int, float)):
            return int(value)
        return None

    if key in ("job_family", "domain"):
        if isinstance(value, str) and value.strip():
            return value.lower().strip()
        return None

    return value


# ── Pydantic model for structured output ─────────────────────────────────────

class EnrichmentOutput(_BaseModel):
    """Structured output schema for Gemini enrichment extraction.

    Used as response_schema in generate_content to guarantee valid JSON
    from the API. Post-processing via _apply_structured_normalization
    preserves the same field semantics as the text-path coercion.
    """
    required_skills: list[str] = _Field(default_factory=list)
    preferred_skills: list[str] = _Field(default_factory=list)
    required_skill_entities: list[SkillEntityOutput] = _Field(default_factory=list)
    preferred_skill_entities: list[SkillEntityOutput] = _Field(default_factory=list)
    responsibilities: list[str] = _Field(default_factory=list)
    tech_stack: list[str] = _Field(default_factory=list)
    keywords: list[str] = _Field(default_factory=list)
    location_type: str | None = None
    seniority: str | None = None
    domain: str | None = None
    job_family: str | None = None
    years_experience_min: int | None = None
    years_experience_max: int | None = None


def _apply_structured_normalization(
    output: EnrichmentOutput,
    config: dict | None,
) -> dict[str, Any]:
    """Convert EnrichmentOutput to a normalized dict preserving existing field semantics.

    Applies the same canonicalization as _coerce_field on the text path:
    - enum fields (location_type, seniority): validated against valid sets, unknown → None
    - domain, job_family: lowercased and stripped
    - list fields: None values removed, items coerced to str
    """
    required_skills = _normalize_array_values(output.required_skills)
    preferred_skills = _normalize_array_values(output.preferred_skills)
    responsibilities = _normalize_array_values(output.responsibilities)
    tech_stack = _normalize_array_values(output.tech_stack)
    keywords = _normalize_array_values(output.keywords)
    required_skill_entities = _build_skill_entities(
        required_skills,
        "required_skills",
        config,
        raw_entities=[entity.model_dump(mode="python") for entity in output.required_skill_entities],
    )
    preferred_skill_entities = _build_skill_entities(
        preferred_skills,
        "preferred_skills",
        config,
        raw_entities=[entity.model_dump(mode="python") for entity in output.preferred_skill_entities],
    )
    mapping_suggestions = [
        *_build_mapping_suggestions(entities=required_skill_entities),
        *_build_mapping_suggestions(entities=preferred_skill_entities),
    ]

    return {
        "location_type_raw": _normalize_text_item(output.location_type),
        "location_type": _normalize_enum(
            output.location_type, _get_valid_location_types(config)
        ),
        "seniority_raw": _normalize_text_item(output.seniority),
        "seniority": _normalize_enum(
            output.seniority, _get_valid_seniority_enrich(config)
        ),
        "domain_raw": output.domain,
        "domain": output.domain.lower().strip() if output.domain else None,
        "job_family_raw": output.job_family,
        "job_family": output.job_family.lower().strip() if output.job_family else None,
        "years_experience_min": output.years_experience_min,
        "years_experience_max": output.years_experience_max,
        **_build_array_companions(
            field_name="required_skills",
            raw_values=required_skills,
            config=config,
            raw_entities=required_skill_entities,
        ),
        **_build_array_companions(
            field_name="preferred_skills",
            raw_values=preferred_skills,
            config=config,
            raw_entities=preferred_skill_entities,
        ),
        **_build_array_companions(field_name="responsibilities", raw_values=responsibilities, config=config),
        **_build_array_companions(field_name="tech_stack", raw_values=tech_stack, config=config),
        **_build_array_companions(field_name="keywords", raw_values=keywords, config=config),
        "mapping_suggestions": mapping_suggestions,
    }

# ── prompt construction ───────────────────────────────────────────────────────

_EXTRACTION_SCHEMA = """\
{
  "required_skills":      ["list", "of", "required", "skills"],
  "preferred_skills":     ["nice-to-have skills"],
  "required_skill_entities": [
    {"raw_text": "raw requirement phrase", "canonical": "normalized skill", "confidence": 0.95}
  ],
  "preferred_skill_entities": [
    {"raw_text": "raw preferred phrase", "canonical": "normalized skill", "confidence": 0.95}
  ],
  "responsibilities":     ["key responsibilities"],
  "tech_stack":           ["specific tools and technologies"],
  "keywords":             ["searchable keywords"],
  "location_type":        "remote | hybrid | onsite",
  "seniority":            "junior | mid | senior | lead",
  "domain":               "business/industry domain, e.g. banking, fintech, healthcare",
  "job_family":           "role category, e.g. data_engineering, analytics, data_science, ml_engineering",
  "years_experience_min": 0,
  "years_experience_max": null
}"""


def build_extraction_prompt(
    description: str,
    scraped_metadata: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> str:
    """Build a Gemini extraction prompt for structured JD fields.

    The prompt is designed to extract only fields NOT already available in
    the scraped metadata. The LLM is instructed to return valid JSON only.

    Important field definitions embedded in the prompt:
    - job_family = role category (what you do)
    - domain = business/industry domain (what industry you do it in)
    - seniority = normalized from JD TEXT, distinct from scraped experience_level
    """
    metadata_block = json.dumps(scraped_metadata, ensure_ascii=False, indent=2)
    prompt_id = get_enrich_extraction_prompt_id(config)
    rendered = render_prompt(
        prompt_id,
        {
            "metadata_block": metadata_block,
            "extraction_schema": _EXTRACTION_SCHEMA,
            "description": description,
        },
    )
    return rendered.text


def get_enrich_extraction_prompt_id(config: dict[str, Any] | None = None) -> str:
    prompt_id = str(
        ((((config or {}).get("prompts") or {}).get("enrich") or {}).get("extraction") or {}
    ).get("prompt_id") or "enrich.extraction.v1")
    return prompt_id.strip() or "enrich.extraction.v1"


def get_enrich_prompt_provenance(config: dict[str, Any] | None = None) -> dict[str, str]:
    prompt_id = get_enrich_extraction_prompt_id(config)
    definition = get_prompt_definition(prompt_id)
    model_name = get_gemini_model(config or {})
    return {
        "prompt_id": definition.prompt_id,
        "prompt_version": definition.version,
        "template_path": str(definition.template_path),
        "model": model_name,
    }


def _normalize_fingerprint_text(value: Any) -> str | None:
    text = _normalize_text_item(value)
    if text is None:
        return None
    return re.sub(r"\s+", " ", text).strip().lower()


def _fingerprint_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_raw_job_fingerprint(job: dict[str, Any]) -> RawJobFingerprintResult:
    payload: RawJobFingerprintPayload = {
        "fingerprint_version": RAW_JOB_FINGERPRINT_VERSION,
        "job_url": str(job.get("job_url") or "").strip(),
        "title": _normalize_fingerprint_text(job.get("title")),
        "company_name": _normalize_fingerprint_text(job.get("company_name")),
        "location": _normalize_fingerprint_text(job.get("location")),
        "description_cleaned": _normalize_fingerprint_text(
            job.get("description_cleaned") or job.get("description")
        ),
        "contract_type": _normalize_fingerprint_text(job.get("contract_type")),
        "experience_level": _normalize_fingerprint_text(job.get("experience_level")),
        "source": _normalize_fingerprint_text(job.get("source")),
    }
    return {
        "payload": payload,
        "fingerprint": _fingerprint_hash(payload),
    }


def build_enrich_contract_fingerprint(
    config: dict[str, Any] | None = None,
) -> EnrichContractFingerprintResult:
    prompt_provenance = get_enrich_prompt_provenance(config)
    payload: EnrichContractFingerprintPayload = {
        "contract_version": ENRICH_CONTRACT_VERSION,
        "prompt_id": prompt_provenance["prompt_id"],
        "prompt_version": prompt_provenance["prompt_version"],
        "template_path": prompt_provenance["template_path"],
        "model": prompt_provenance["model"],
        "response_schema_version": ENRICH_RESPONSE_SCHEMA_VERSION,
        "skill_postprocessing_version": ENRICH_SKILL_POSTPROCESSING_VERSION,
    }
    return {
        "payload": payload,
        "fingerprint": _fingerprint_hash(payload),
    }


# ── response parsing ──────────────────────────────────────────────────────────

def parse_extraction_response(response_text: str, config: dict | None = None) -> dict[str, Any]:
    """Parse LLM extraction response with explicit fallback contract.

    Returns:
        {
            "parsed": dict of validated/coerced known fields,
            "errors": list of error strings (empty on full success),
            "raw_response": original response_text unchanged,
        }

    Contract:
        - Strips Markdown code fences before parsing
        - Invalid JSON → parsed = {}, error recorded, no crash
        - Missing field → [] for arrays, None for scalars
        - Unknown keys → silently ignored
        - Null list values → coerced to []
        - Enum fields (location_type, seniority) → lowercased; unrecognized → None
        - job_family, domain → lowercased free strings
    """
    errors: list[str] = []
    cleaned = _strip_markdown_fences(response_text)

    try:
        raw: Any = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        repaired = _repair_common_json_issues(cleaned)
        if repaired != cleaned:
            try:
                raw = json.loads(repaired)
            except json.JSONDecodeError:
                raw = None
            else:
                cleaned = repaired
        else:
            raw = None
        if raw is not None:
            pass
        else:
        # Thinking models (e.g. gemini-2.5-flash) sometimes emit malformed JSON
        # (missing commas, trailing commas, etc.). Try json_repair before giving up.
            try:
                from json_repair import repair_json  # type: ignore[import-untyped]
                raw = json.loads(repair_json(cleaned))
            except Exception:
                return {
                    "parsed": {},
                    "errors": [f"JSON parse error: {exc}"],
                    "raw_response": response_text,
                }

    if not isinstance(raw, dict):
        return {
            "parsed": {},
            "errors": ["LLM response was valid JSON but not an object"],
            "raw_response": response_text,
        }

    parsed: dict[str, Any] = {}
    for field in _KNOWN_FIELDS:
        raw_value = raw.get(field)
        parsed[field] = _coerce_field(field, raw_value, config)

    parsed["location_type_raw"] = _normalize_text_item(raw.get("location_type"))
    parsed["seniority_raw"] = _normalize_text_item(raw.get("seniority"))
    parsed["domain_raw"] = raw.get("domain") if isinstance(raw.get("domain"), str) else None
    parsed["job_family_raw"] = raw.get("job_family") if isinstance(raw.get("job_family"), str) else None

    required_skill_entities = _build_skill_entities(
        list(parsed.get("required_skills") or []),
        "required_skills",
        config,
        raw_entities=raw.get("required_skill_entities") if isinstance(raw.get("required_skill_entities"), list) else None,
    )
    preferred_skill_entities = _build_skill_entities(
        list(parsed.get("preferred_skills") or []),
        "preferred_skills",
        config,
        raw_entities=raw.get("preferred_skill_entities") if isinstance(raw.get("preferred_skill_entities"), list) else None,
    )
    parsed["required_skill_entities"] = required_skill_entities
    parsed["preferred_skill_entities"] = preferred_skill_entities
    parsed["required_skills_canonical"] = _canonical_from_entities(required_skill_entities)
    parsed["preferred_skills_canonical"] = _canonical_from_entities(preferred_skill_entities)
    mapping_suggestions: list[MappingSuggestion] = [
        *_build_mapping_suggestions(entities=required_skill_entities),
        *_build_mapping_suggestions(entities=preferred_skill_entities),
    ]
    parsed["mapping_suggestions"] = mapping_suggestions

    return {
        "parsed": parsed,
        "errors": errors,
        "raw_response": response_text,
    }


# ── merge ─────────────────────────────────────────────────────────────────────

def merge_scraped_and_enriched(
    scraped: dict[str, Any],
    enriched: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Combine scraper metadata and LLM-parsed dict into structured_jobs schema.

    Args:
        scraped:  Normalized scraped job dict (snake_case keys).
        enriched: The `parsed` dict from parse_extraction_response().
        config:   Project config; used for enrichment_version and ai_score_model.

    Returns:
        Merged dict matching fitcv.structured_jobs schema including audit fields.
    """
    cfg = config or {}
    model = str(enriched.get("enrichment_model") or cfg.get("gemini_model") or cfg.get("ai_score_model") or "")
    version = str(enriched.get("enrichment_version") or cfg.get("enrichment_version", "v1"))

    merged: dict[str, Any] = {
        # ── scraped fields ────────────────────────────────────────────
        "job_url":            scraped.get("job_url", ""),
        "title":              scraped.get("title", ""),
        "company_name":       scraped.get("company_name", ""),
        "company_id":         scraped.get("company_id", ""),
        "location":           scraped.get("location", ""),
        "contract_type":      scraped.get("contract_type", ""),
        "experience_level":   scraped.get("experience_level", ""),
        "sector":             scraped.get("sector", ""),
        "salary_min":         scraped.get("salary_min"),
        "salary_max":         scraped.get("salary_max"),
        "salary_currency":    scraped.get("salary_currency"),
        "applications_count": scraped.get("applications_count_int"),
        "published_at":       scraped.get("published_at"),
        "description_cleaned": scraped.get("description", ""),
        # ── LLM-enriched fields ───────────────────────────────────────
        "location_type_raw":    enriched.get("location_type_raw"),
        "location_type":        enriched.get("location_type"),
        "seniority_raw":        enriched.get("seniority_raw"),
        "seniority":            enriched.get("seniority"),
        "required_skills":      enriched.get("required_skills", []),
        "required_skills_canonical": enriched.get("required_skills_canonical", []),
        "required_skill_entities": enriched.get("required_skill_entities", []),
        "preferred_skills":     enriched.get("preferred_skills", []),
        "preferred_skills_canonical": enriched.get("preferred_skills_canonical", []),
        "preferred_skill_entities": enriched.get("preferred_skill_entities", []),
        "responsibilities":     enriched.get("responsibilities", []),
        "domain_raw":           enriched.get("domain_raw"),
        "domain":               enriched.get("domain"),
        "tech_stack":           enriched.get("tech_stack", []),
        "years_experience_min": enriched.get("years_experience_min"),
        "years_experience_max": enriched.get("years_experience_max"),
        "keywords":             enriched.get("keywords", []),
        "job_family_raw":       enriched.get("job_family_raw"),
        "job_family":           enriched.get("job_family"),
        "mapping_suggestions":  enriched.get("mapping_suggestions", []),
        # ── audit fields ──────────────────────────────────────────────
        "enrichment_version": version,
        "enrichment_model":   model,
        "enriched_at":        _normalise_enriched_at(enriched.get("enriched_at")) or datetime.now(tz=timezone.utc).isoformat(),
        "raw_job_fingerprint": enriched.get("raw_job_fingerprint"),
        "enrich_contract_fingerprint": enriched.get("enrich_contract_fingerprint"),
        "enrich_reuse_status": enriched.get("enrich_reuse_status"),
    }
    return merged


def _parse_json_field(raw_value: Any) -> Any:
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError:
            return None
    return None


def _normalise_enriched_at(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return str(value)


def _cached_structured_row_to_enriched_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "location_type_raw": row.get("location_type_raw"),
        "location_type": row.get("location_type"),
        "seniority_raw": row.get("seniority_raw"),
        "seniority": row.get("seniority"),
        "required_skills": list(row.get("required_skills") or []),
        "required_skills_canonical": list(row.get("required_skills_canonical") or []),
        "required_skill_entities": _parse_json_field(row.get("required_skill_entities_json")) or [],
        "preferred_skills": list(row.get("preferred_skills") or []),
        "preferred_skills_canonical": list(row.get("preferred_skills_canonical") or []),
        "preferred_skill_entities": _parse_json_field(row.get("preferred_skill_entities_json")) or [],
        "responsibilities": list(row.get("responsibilities") or []),
        "domain_raw": row.get("domain_raw"),
        "domain": row.get("domain"),
        "tech_stack": list(row.get("tech_stack") or []),
        "years_experience_min": row.get("years_experience_min"),
        "years_experience_max": row.get("years_experience_max"),
        "keywords": list(row.get("keywords") or []),
        "job_family_raw": row.get("job_family_raw"),
        "job_family": row.get("job_family"),
        "mapping_suggestions": _parse_json_field(row.get("mapping_suggestions_json")) or [],
        "enrichment_version": row.get("enrichment_version"),
        "enrichment_model": row.get("enrichment_model"),
        "enriched_at": _normalise_enriched_at(row.get("enriched_at")),
        "raw_job_fingerprint": row.get("raw_job_fingerprint"),
        "enrich_contract_fingerprint": row.get("enrich_contract_fingerprint"),
        "enrich_reuse_status": row.get("enrich_reuse_status"),
    }


def lookup_reusable_structured_jobs(
    normalized_jobs: list[dict[str, Any]],
    config: dict[str, Any],
    *,
    raw_job_fingerprints: dict[str, str],
    enrich_contract_fingerprint: str,
) -> dict[str, dict[str, Any]]:
    if not normalized_jobs:
        return {}

    project = str(config.get("gcp_project") or "").strip()
    dataset = str(config.get("bigquery_dataset") or "").strip()
    key_path = str(config.get("service_account_key") or "").strip()
    if not project or not dataset or not key_path:
        logger.info(
            "Skipping enrich reuse lookup because BigQuery reuse config is incomplete",
            extra={
                "has_gcp_project": bool(project),
                "has_bigquery_dataset": bool(dataset),
                "has_service_account_key": bool(key_path),
            },
        )
        return {}

    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)

    job_urls = [
        str(job.get("job_url") or "")
        for job in normalized_jobs
        if str(job.get("job_url") or "")
    ]
    if not job_urls:
        return {}

    table = f"`{project}.{dataset}.structured_jobs`"
    sql = f"""
        SELECT *
        FROM {table}
        WHERE job_url IN UNNEST(@job_urls)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("job_urls", "STRING", job_urls),
        ],
        use_query_cache=False,
    )
    try:
        rows = client.query(sql, job_config=job_config).result()
    except Exception as exc:
        logger.warning("Enrich reuse lookup failed; falling back to fresh enrichment: %s", exc)
        return {}

    normalized_by_url = {
        str(job.get("job_url") or ""): job
        for job in normalized_jobs
        if str(job.get("job_url") or "")
    }
    reusable_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_dict = dict(row.items())
        job_url = str(row_dict.get("job_url") or "")
        if not job_url or job_url in reusable_rows:
            continue
        if row_dict.get("raw_job_fingerprint") != raw_job_fingerprints.get(job_url):
            continue
        if row_dict.get("enrich_contract_fingerprint") != enrich_contract_fingerprint:
            continue
        normalized_job = normalized_by_url.get(job_url)
        if normalized_job is None:
            continue
        reusable_rows[job_url] = merge_scraped_and_enriched(
            normalized_job,
            {
                **_cached_structured_row_to_enriched_payload(row_dict),
                "raw_job_fingerprint": raw_job_fingerprints[job_url],
                "enrich_contract_fingerprint": enrich_contract_fingerprint,
                "enrich_reuse_status": REUSED_CACHED_ENRICHMENT_STATUS,
            },
            config,
        )
    return reusable_rows


# ── integration: LLM call ─────────────────────────────────────────────────────

def _make_genai_client(config: dict[str, Any]) -> Any:
    """Return a google.genai client using API key first, then Vertex AI."""
    import google.auth  # type: ignore[import-untyped]
    from google import genai  # type: ignore[import-untyped]
    from fitcv.config import get_vertex_location

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if api_key:
        return genai.Client(api_key=api_key)

    creds, _ = google.auth.default(  # type: ignore[misc]
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return genai.Client(
        vertexai=True,
        project=str(config["gcp_project"]),
        location=get_vertex_location(config),
        credentials=creds,
    )


def _build_extraction_generation_config() -> "Any":
    """Return structured-output config using EnrichmentOutput Pydantic schema.

    Both response_mime_type and response_schema are required: Vertex AI
    rejects response_schema when mime type defaults to 'text/plain'.
    """
    from google.genai import types as _genai_types  # type: ignore[import-untyped]
    return _genai_types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=EnrichmentOutput,
    )


def enrich_job(job: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    """Call Gemini to extract structured fields from one normalized job.

    Primary path: uses response_schema structured output (EnrichmentOutput),
    which the API guarantees to be valid JSON matching the schema.
    Fallback: response.text + json_repair when response.parsed is None.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.

    Returns:
        Merged dict ready for load_structured_jobs().
    """
    import logging as _logging
    _log = _logging.getLogger(__name__)

    model_name = get_gemini_model(config)
    client = _make_genai_client(config)
    title_for_log = job.get("title") or job.get("job_url")

    prompt = build_extraction_prompt(
        description=str(job.get("description", "")),
        scraped_metadata={
            "title": job.get("title", ""),
            "experienceLevel": job.get("experience_level", ""),
            "contractType": job.get("contract_type", ""),
            "sector": job.get("sector", ""),
            "location": job.get("location", ""),
        },
        config=config,
    )

    response = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=_build_extraction_generation_config(),
    )

    # ── Primary path: structured output ──────────────────────────────────────
    if response.parsed is not None:
        parsed = _apply_structured_normalization(response.parsed, config)
        return merge_scraped_and_enriched(job, parsed, config)

    # ── Fallback: text + json_repair ─────────────────────────────────────────
    _log.warning(
        "Structured output unavailable for %r — falling back to json_repair",
        title_for_log,
    )
    extraction = parse_extraction_response(str(response.text or ""), config)
    if extraction["errors"]:
        _log.warning(
            "Enrichment parse errors for job %r: %s",
            title_for_log,
            "; ".join(extraction["errors"]),
        )
    return merge_scraped_and_enriched(job, extraction["parsed"], config)


def _enrich_chunk(
    chunk: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Enrich one bounded chunk of normalized jobs with global rate limiting and retry.

    @capability bounded_parallel_enrichment.per-job-failure-isolation

    Uses the module-level _ENRICH_RATE_LOCK to serialize API calls across all
    concurrent chunks. This makes enrichment_sleep_secs a true global rate limit
    rather than a per-thread-only delay, regardless of enrichment_concurrency.

    Raises:
        Any exception that enrich_job raises after exhausting retries (ResourceExhausted,
        ClientError, etc.) — non-recoverable failures propagate to the caller.
    """
    import time
    from google.api_core.exceptions import ResourceExhausted  # type: ignore[import-untyped]
    from google.genai.errors import ClientError  # type: ignore[import-untyped]

    sleep_secs = float(config.get("enrichment_sleep_secs", 1.0))
    max_retries = int(config.get("enrichment_max_retries", 2))
    results: list[dict[str, Any]] = []
    for job in chunk:
        attempts = 0
        while True:
            with _ENRICH_RATE_LOCK:
                # Hold the lock for the API call + inter-request sleep so that
                # no other chunk thread can issue an API call during this window.
                try:
                    enriched = enrich_job(job, config)
                    results.append(enriched)
                    time.sleep(sleep_secs)  # global rate limit: one req per sleep_secs
                    break
                except ResourceExhausted:
                    if attempts >= max_retries:
                        raise
                    attempts += 1
                    time.sleep(sleep_secs * (2 ** (attempts - 1)))
                except ClientError as exc:
                    if getattr(exc, "status_code", None) != 429 or attempts >= max_retries:
                        raise
                    attempts += 1
                    time.sleep(sleep_secs * (2 ** (attempts - 1)))
    return results


def enrich_batch(
    normalized_jobs: list[dict[str, Any]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Enrich a batch of normalized jobs with bounded parallel execution.

    @capability bounded_parallel_enrichment.deterministic-output-order

    Splits normalized_jobs into chunks of enrichment_batch_size, then submits
    up to enrichment_concurrency chunks in parallel via ThreadPoolExecutor.
    Results are collected BY ORIGINAL CHUNK INDEX (not completion order) to
    guarantee deterministic output ordering matching the input.

    Fail-fast semantics: any non-recoverable exception from a chunk propagates
    immediately — the parallel version does not silently degrade to partial success
    when a chunk raises an exception that the sequential path would have raised.

    Config keys (read at call time with safe defaults):
        enrichment_batch_size  (int, default 10)
        enrichment_concurrency (int, default 1)

    Rate limiting: all API calls across all concurrent chunks are serialized
    through the module-level _ENRICH_RATE_LOCK, making enrichment_sleep_secs
    a true global rate limiter. Higher concurrency values speed up wall-clock
    time only when chunk processing overhead (not API latency) dominates.
    """
    from concurrent.futures import ThreadPoolExecutor

    batch_size = int(config.get("enrichment_batch_size", 10))
    concurrency = int(config.get("enrichment_concurrency", 1))

    # Split into chunks of batch_size
    chunks = [
        normalized_jobs[i:i + batch_size]
        for i in range(0, len(normalized_jobs), batch_size)
    ]

    if not chunks:
        return []

    # Submit all chunks; collect futures in original order
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(_enrich_chunk, chunk, config) for chunk in chunks]

        # Collect results by original chunk index, not completion order.
        # This preserves deterministic merged output ordering.
        chunk_results: list[list[dict[str, Any]]] = [None] * len(futures)  # type: ignore[list-item]
        for idx, future in enumerate(futures):
            # Calling .result() re-raises any exception from the chunk.
            # This preserves fail-fast semantics: if any chunk raises a
            # non-recoverable exception, it propagates here immediately.
            chunk_results[idx] = future.result()

    # Flatten chunk results in original chunk order
    results: list[dict[str, Any]] = []
    for chunk_result in chunk_results:
        results.extend(chunk_result)
    return results


# ── integration: BigQuery upsert ──────────────────────────────────────────────

_MERGE_COLUMNS = [
    "title", "company_name", "company_id", "location", "contract_type",
    "experience_level", "sector", "salary_min", "salary_max", "salary_currency",
    "applications_count", "published_at", "location_type_raw", "location_type",
    "seniority_raw", "seniority", "required_skills", "required_skills_canonical",
    "required_skill_entities_json", "preferred_skills", "preferred_skills_canonical",
    "preferred_skill_entities_json", "responsibilities", "responsibilities_canonical",
    "domain_raw", "domain", "tech_stack", "tech_stack_canonical",
    "years_experience_min", "years_experience_max", "keywords", "keywords_canonical",
    "job_family_raw", "job_family", "mapping_suggestions_json", "description_cleaned",
    "enrichment_version", "enrichment_model", "enriched_at",
    "raw_job_fingerprint", "enrich_contract_fingerprint", "enrich_reuse_status",
]

_STAGING_SCHEMA_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("job_url", "STRING", "REQUIRED"),
    ("title", "STRING", "NULLABLE"),
    ("company_name", "STRING", "NULLABLE"),
    ("company_id", "STRING", "NULLABLE"),
    ("location", "STRING", "NULLABLE"),
    ("contract_type", "STRING", "NULLABLE"),
    ("experience_level", "STRING", "NULLABLE"),
    ("sector", "STRING", "NULLABLE"),
    ("salary_min", "FLOAT64", "NULLABLE"),
    ("salary_max", "FLOAT64", "NULLABLE"),
    ("salary_currency", "STRING", "NULLABLE"),
    ("applications_count", "INT64", "NULLABLE"),
    ("published_at", "DATE", "NULLABLE"),
    ("location_type_raw", "STRING", "NULLABLE"),
    ("location_type", "STRING", "NULLABLE"),
    ("seniority_raw", "STRING", "NULLABLE"),
    ("seniority", "STRING", "NULLABLE"),
    ("required_skills", "STRING", "REPEATED"),
    ("required_skills_canonical", "STRING", "REPEATED"),
    ("required_skill_entities_json", "STRING", "NULLABLE"),
    ("preferred_skills", "STRING", "REPEATED"),
    ("preferred_skills_canonical", "STRING", "REPEATED"),
    ("preferred_skill_entities_json", "STRING", "NULLABLE"),
    ("responsibilities", "STRING", "REPEATED"),
    ("responsibilities_canonical", "STRING", "REPEATED"),
    ("domain_raw", "STRING", "NULLABLE"),
    ("domain", "STRING", "NULLABLE"),
    ("tech_stack", "STRING", "REPEATED"),
    ("tech_stack_canonical", "STRING", "REPEATED"),
    ("years_experience_min", "INT64", "NULLABLE"),
    ("years_experience_max", "INT64", "NULLABLE"),
    ("keywords", "STRING", "REPEATED"),
    ("keywords_canonical", "STRING", "REPEATED"),
    ("job_family_raw", "STRING", "NULLABLE"),
    ("job_family", "STRING", "NULLABLE"),
    ("mapping_suggestions_json", "STRING", "NULLABLE"),
    ("description_cleaned", "STRING", "NULLABLE"),
    ("enrichment_version", "STRING", "NULLABLE"),
    ("enrichment_model", "STRING", "NULLABLE"),
    ("enriched_at", "TIMESTAMP", "NULLABLE"),
    ("raw_job_fingerprint", "STRING", "NULLABLE"),
    ("enrich_contract_fingerprint", "STRING", "NULLABLE"),
    ("enrich_reuse_status", "STRING", "NULLABLE"),
)

_STRUCTURED_JSON_LIST_FIELDS: tuple[tuple[str, str], ...] = (
    ("required_skill_entities", "required_skill_entities_json"),
    ("preferred_skill_entities", "preferred_skill_entities_json"),
    ("mapping_suggestions", "mapping_suggestions_json"),
)

_STRUCTURED_SCHEMA_KEYS: frozenset[str] = frozenset(
    ["job_url", *_MERGE_COLUMNS]
)


def _map_to_structured_jobs_row(row: dict[str, Any]) -> dict[str, Any]:
    mapped: dict[str, Any] = {}
    for key in _STRUCTURED_SCHEMA_KEYS:
        if key in row:
            mapped[key] = row[key]
    for source_key, target_key in _STRUCTURED_JSON_LIST_FIELDS:
        if source_key in row:
            mapped[target_key] = json.dumps(row.get(source_key, []), ensure_ascii=False)
    return mapped


def load_structured_jobs(
    enriched: list[dict[str, Any]],
    config: dict[str, Any],
) -> int:
    """Upsert enriched job rows into fitcv.structured_jobs via MERGE on job_url.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.

    Returns:
        Number of rows upserted.
    """
    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)

    target = f"`{project}.{dataset}.structured_jobs`"
    update_set = ",\n    ".join(
        f"T.{col} = S.{col}" for col in _MERGE_COLUMNS
    )
    insert_cols = ", ".join(["job_url"] + _MERGE_COLUMNS)
    insert_vals = ", ".join([f"S.{c}" for c in ["job_url"] + _MERGE_COLUMNS])

    temp_table = f"`{project}.{dataset}._enrich_staging`"
    schema = [
        bigquery.SchemaField(name, field_type, mode=mode)
        for name, field_type, mode in _STAGING_SCHEMA_FIELDS
    ]

    # Load to a temp table first, then MERGE
    staging_ref = f"{project}.{dataset}._enrich_staging"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
    )
    load_rows = [_map_to_structured_jobs_row(row) for row in enriched]
    load_job = client.load_table_from_json(
        load_rows,
        staging_ref,
        job_config=job_config,
    )
    load_job.result()

    merge_sql = f"""
    MERGE {target} AS T
    USING {temp_table} AS S
    ON T.job_url = S.job_url
    WHEN MATCHED THEN UPDATE SET
        {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols})
    VALUES ({insert_vals})
    """
    client.query(merge_sql).result()
    return len(enriched)


# ── integration: run-scoped append ───────────────────────────────────────────────

# Ordered columns for run_structured_jobs (same order as DDL).
_RUN_SCHEMA_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("run_id",               "STRING",    "REQUIRED"),
    ("job_url",              "STRING",    "REQUIRED"),
    ("title",                "STRING",    "NULLABLE"),
    ("company_name",         "STRING",    "NULLABLE"),
    ("location",             "STRING",    "NULLABLE"),
    ("contract_type",        "STRING",    "NULLABLE"),
    ("experience_level",     "STRING",    "NULLABLE"),
    ("published_at",         "DATE",      "NULLABLE"),
    ("location_type_raw",    "STRING",    "NULLABLE"),
    ("location_type",        "STRING",    "NULLABLE"),
    ("seniority_raw",        "STRING",    "NULLABLE"),
    ("seniority",            "STRING",    "NULLABLE"),
    ("required_skills",      "STRING",    "REPEATED"),
    ("required_skills_canonical", "STRING", "REPEATED"),
    ("required_skill_entities_json", "STRING", "NULLABLE"),
    ("preferred_skills",     "STRING",    "REPEATED"),
    ("preferred_skills_canonical", "STRING", "REPEATED"),
    ("preferred_skill_entities_json", "STRING", "NULLABLE"),
    ("responsibilities",     "STRING",    "REPEATED"),
    ("responsibilities_canonical", "STRING", "REPEATED"),
    ("domain_raw",           "STRING",    "NULLABLE"),
    ("domain",               "STRING",    "NULLABLE"),
    ("tech_stack",           "STRING",    "REPEATED"),
    ("tech_stack_canonical", "STRING",    "REPEATED"),
    ("years_experience_min", "INT64",     "NULLABLE"),
    ("years_experience_max", "INT64",     "NULLABLE"),
    ("keywords",             "STRING",    "REPEATED"),
    ("keywords_canonical",   "STRING",    "REPEATED"),
    ("job_family_raw",       "STRING",    "NULLABLE"),
    ("job_family",           "STRING",    "NULLABLE"),
    ("mapping_suggestions_json", "STRING", "NULLABLE"),
    ("description_cleaned",  "STRING",    "NULLABLE"),
    ("enrichment_version",   "STRING",    "NULLABLE"),
    ("enrichment_model",     "STRING",    "NULLABLE"),
    ("enriched_at",          "TIMESTAMP", "NULLABLE"),
    ("raw_job_fingerprint",  "STRING",    "NULLABLE"),
    ("enrich_contract_fingerprint", "STRING", "NULLABLE"),
    ("enrich_reuse_status",  "STRING",    "NULLABLE"),
)

_RUN_SCHEMA_KEYS: frozenset[str] = frozenset(name for name, _, _ in _RUN_SCHEMA_FIELDS)


def _map_to_run_structured_jobs_row(
    row: dict[str, Any],
    run_id: str,
) -> dict[str, Any]:
    """Project an enriched row into the run_structured_jobs schema, injecting run_id."""
    mapped: dict[str, Any] = {"run_id": run_id}
    for key in _RUN_SCHEMA_KEYS - {"run_id"}:
        if key in row:
            mapped[key] = row[key]
    for source_key, target_key in _STRUCTURED_JSON_LIST_FIELDS:
        if source_key in row:
            mapped[target_key] = json.dumps(row.get(source_key, []), ensure_ascii=False)
    return mapped


def load_run_structured_jobs(
    enriched: list[dict[str, Any]],
    run_id: str,
    config: dict[str, Any],
) -> int:
    """Append run-scoped enriched job rows into fitcv.run_structured_jobs.

    Uses WRITE_APPEND semantics — no MERGE, no staging table.  One job can
    appear multiple times across different runs (that is intentional).

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.

    Returns:
        Number of rows appended.
    """
    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)

    rows = [_map_to_run_structured_jobs_row(row, run_id) for row in enriched]

    schema = [
        bigquery.SchemaField(name, field_type, mode=mode)
        for name, field_type, mode in _RUN_SCHEMA_FIELDS
    ]
    table_ref = f"{project}.{dataset}.run_structured_jobs"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
    )
    load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    load_job.result()
    return len(rows)
