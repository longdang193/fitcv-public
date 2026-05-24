"""@meta
name: contracts
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.contracts.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


RANKING_AI_SCORE_PROMPT_SCHEMA_VERSION = "ranking_ai_score_prompt_v1"
STRUCTURED_CV_SCHEMA_VERSION = "cv_doc_v1"
CV_ANALYSIS_REUSE_SCHEMA_VERSION = "cv_analysis_reuse_v1"
STAGE_TRANSITION_ARTIFACTS_PIPELINE_SCHEMA_VERSION = "stage_transition_artifacts_v6"
STAGE_TRANSITION_ARTIFACTS_RUN_SCHEMA_VERSION = "stage_transition_artifacts_run_v1"
STAGE_TRANSITION_ARTIFACTS_STAGE_SCHEMA_VERSION = "stage_transition_artifacts_stage_v1"
MAPPING_SUGGESTIONS_SCHEMA_VERSION = "mapping_suggestions_v1"
MAPPING_SUGGESTIONS_AGGREGATE_SCHEMA_VERSION = "mapping_suggestions_aggregate_v1"
SETTINGS_USED_SCHEMA_VERSION = "settings_used_v2"
SYNONYM_PROPOSALS_SCHEMA_VERSION = "synonym_proposals_v1"
SYNONYM_PROPOSALS_QUEUE_SCHEMA_VERSION = "synonym_proposals_queue_v1"

REQUIRED_SKILL_SUPPORT_CHANNEL = "required_skill_support"
ROLE_ALIGNMENT_CHANNEL = "role_alignment"
DOMAIN_ALIGNMENT_CHANNEL = "domain_alignment"
RESPONSIBILITY_ALIGNMENT_CHANNEL = "responsibility_alignment"


@dataclass(frozen=True)
class AnalysisChannelDefinition:
    channel_id: str
    display_label: str
    intended_usage: str


ANALYSIS_CHANNEL_DEFINITIONS = {
    REQUIRED_SKILL_SUPPORT_CHANNEL: AnalysisChannelDefinition(
        channel_id=REQUIRED_SKILL_SUPPORT_CHANNEL,
        display_label="Required Skill Support",
        intended_usage="Use as grounding for concrete technical and skill claims.",
    ),
    ROLE_ALIGNMENT_CHANNEL: AnalysisChannelDefinition(
        channel_id=ROLE_ALIGNMENT_CHANNEL,
        display_label="Role Alignment",
        intended_usage="Use to shape summary positioning and headline alignment.",
    ),
    DOMAIN_ALIGNMENT_CHANNEL: AnalysisChannelDefinition(
        channel_id=DOMAIN_ALIGNMENT_CHANNEL,
        display_label="Domain Alignment",
        intended_usage="Use only for grounded business-context and domain familiarity claims.",
    ),
    RESPONSIBILITY_ALIGNMENT_CHANNEL: AnalysisChannelDefinition(
        channel_id=RESPONSIBILITY_ALIGNMENT_CHANNEL,
        display_label="Responsibility Alignment",
        intended_usage="Use to support experience bullets around similar work and outcomes.",
    ),
}

ANALYSIS_CHANNEL_IDS = tuple(ANALYSIS_CHANNEL_DEFINITIONS.keys())

ANALYSIS_CHANNEL_ALIASES = {
    "responsibility": RESPONSIBILITY_ALIGNMENT_CHANNEL,
    "domain": DOMAIN_ALIGNMENT_CHANNEL,
}


def canonical_analysis_channel_id(channel_id: str) -> str:
    normalized = str(channel_id or "").strip()
    if not normalized:
        return normalized
    return ANALYSIS_CHANNEL_ALIASES.get(normalized, normalized)


def normalize_analysis_channel_mapping(mapping: Mapping[str, Any] | None) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for raw_key, value in dict(mapping or {}).items():
        canonical_key = canonical_analysis_channel_id(str(raw_key))
        if not canonical_key:
            continue
        if canonical_key in normalized and isinstance(normalized[canonical_key], dict) and isinstance(value, Mapping):
            merged = dict(normalized[canonical_key])
            merged.update(dict(value))
            normalized[canonical_key] = merged
            continue
        normalized[canonical_key] = value
    return normalized

# Shared ingest/normalize/tracker contracts
SCRAPER_CAMEL_TO_SNAKE: dict[str, str] = {
    "jobUrl": "job_url",
    "postedTime": "posted_time",
    "publishedAt": "published_at",
    "companyName": "company_name",
    "companyUrl": "company_url",
    "companyId": "company_id",
    "applicationsCount": "applications_count",
    "contractType": "contract_type",
    "experienceLevel": "experience_level",
    "workType": "work_type",
    "posterFullName": "poster_full_name",
    "posterProfileUrl": "poster_profile_url",
    "applyUrl": "apply_url",
    "applyType": "apply_type",
}

REQUIRED_SCRAPER_FIELDS: tuple[str, ...] = (
    "jobUrl",
    "title",
    "companyName",
    "description",
    "contractType",
    "experienceLevel",
)

DEFAULT_APPLICATION_STATUSES: tuple[str, ...] = (
    "applied",
    "not_applied",
    "interview",
    "rejected",
    "no_response",
)
