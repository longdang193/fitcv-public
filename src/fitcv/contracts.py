from __future__ import annotations

from dataclasses import dataclass


RANKING_AI_SCORE_PROMPT_SCHEMA_VERSION = "ranking_ai_score_prompt_v1"
STRUCTURED_CV_SCHEMA_VERSION = "cv_doc_v1"
CV_ANALYSIS_REUSE_SCHEMA_VERSION = "cv_analysis_reuse_v1"

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
