"""@meta
name: settings_schema
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.settings_schema.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

from typing import Any

from fitcv.config import load_config
from fitcv.cv_presets import SUPPORTED_PRESETS


class ValidationError(ValueError):
    pass


_CV_GENERATION_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-pro",
]
_CV_PRESET_OPTIONS = sorted(SUPPORTED_PRESETS)
_RULE_FILTER_SELECTABLE_OPTIONS = [
    "seniority_mismatch",
    "location_type_excluded",
    "contract_type_excluded",
    "experience_level_excluded",
    "must_have_skill_missing",
    "domain_not_preferred",
]
_RESPONSIBILITY_ALIGNMENT_WEIGHT_KEYS = {
    "cv_analysis.semantic_alignment.responsibility_lexical_weight",
    "cv_analysis.semantic_alignment.responsibility_semantic_weight",
}
_DOMAIN_ALIGNMENT_WEIGHT_KEYS = {
    "cv_analysis.semantic_alignment.domain_lexical_weight",
    "cv_analysis.semantic_alignment.domain_semantic_weight",
}
_REQUIRED_SKILL_ALIGNMENT_WEIGHT_KEYS = {
    "cv_analysis.semantic_alignment.required_skill_lexical_weight",
    "cv_analysis.semantic_alignment.required_skill_semantic_weight",
}
_ROLE_ALIGNMENT_WEIGHT_KEYS = {
    "cv_analysis.semantic_alignment.role_lexical_weight",
    "cv_analysis.semantic_alignment.role_semantic_weight",
}
_UI_SURFACE_EDITABLE = "editable"
_UI_SURFACE_METADATA_ONLY = "metadata_only"
_AGENTIC_SECTION_CORE = "agentic-core"
_AGENTIC_SECTION_ADVANCED = "agentic-advanced"
_EXCLUDED_AGENTIC_KEYS: frozenset[str] = frozenset(
    {
        "cv_prompt_version",
        "cv_template_path",
        "skill_synonyms_runtime",
    }
)


# ── schema registry ──────────────────────────────────────────────────────────

SETTINGS_SCHEMA: list[dict[str, Any]] = [
    # ── Retrieval ─────────────────────────────────────────────────────────────
    {
        "key": "pipeline.vector_search_top_n",
        "type": "int",
        "default": 50,
        "label": "Initial Candidate Pool Size",
        "description": "Higher values broaden recall after deterministic filtering, but they also increase shortlist and downstream ranking latency.",
        "group": "retrieval",
        "config_path": ["pipeline", "vector_search_top_n"],
    },
    {
        "key": "pipeline.ai_score_top_n",
        "type": "int",
        "default": 50,
        "label": "AI Reranking Pool Size",
        "description": "Higher values improve semantic coverage, but they increase LLM reranking time and cost.",
        "group": "retrieval",
        "config_path": ["pipeline", "ai_score_top_n"],
    },
    {
        "key": "pipeline.final_top_n",
        "type": "int",
        "default": 10,
        "label": "Final Output Count",
        "description": "Bounds how many ranked jobs reach the final output set and late CV stages.",
        "group": "retrieval",
        "config_path": ["pipeline", "final_top_n"],
    },
    {
        "key": "pipeline.evidence_top_k",
        "type": "int",
        "default": 5,
        "label": "Final Evidence Items Per Job",
        "description": "Controls how much bounded evidence cv_analysis keeps per ranked job after merge, dedupe, and final selection.",
        "group": "retrieval",
        "config_path": ["pipeline", "evidence_top_k"],
    },
    {
        "key": "cv.agentic_late_stage.enabled",
        "type": "bool",
        "default": False,
        "label": "Agentic Late Stage Enabled",
        "description": "Enable the bounded agentic late-stage analysis and generation path for future runs.",
        "group": "agentic",
        "config_path": ["cv", "agentic_late_stage", "enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.propose_enabled",
        "type": "bool",
        "default": True,
        "label": "Synonym Proposals Enabled",
        "description": "Enable generation and regeneration of synonym proposals from mapping suggestions.",
        "group": "agentic",
        "config_path": ["synonym_management", "propose_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.apply_to_run_enabled",
        "type": "bool",
        "default": False,
        "label": "Synonym Apply-to-Run Enabled",
        "description": "Allow review actions and apply-approved-to-run overlay writes for synonym proposals.",
        "group": "agentic",
        "config_path": ["synonym_management", "apply_to_run_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.promote_global_enabled",
        "type": "bool",
        "default": False,
        "label": "Synonym Promote-Global Enabled",
        "description": "Allow promotion of approved synonym proposals into the global synonym map.",
        "group": "agentic",
        "config_path": ["synonym_management", "promote_global_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.auto_triage_recommendation_enabled",
        "type": "bool",
        "default": True,
        "label": "Auto Triage Recommendation",
        "description": "Automatically generate synonym triage recommendations for pending proposals.",
        "group": "agentic",
        "config_path": ["synonym_management", "auto_triage_recommendation_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.triage_recommendation_reuse_enabled",
        "type": "bool",
        "default": True,
        "label": "Reuse Triage Recommendation",
        "description": "Reuse compatible prior triage recommendations; disable to force fresh recompute.",
        "group": "agentic",
        "config_path": ["synonym_management", "triage_recommendation_reuse_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.auto_apply_recommendation_enabled",
        "type": "bool",
        "default": False,
        "label": "Auto Apply Recommendation",
        "description": "Automatically apply recommended synonym proposal actions after triage refresh when safety checks pass.",
        "group": "agentic",
        "config_path": ["synonym_management", "auto_apply_recommendation_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.auto_promote_global_enabled",
        "type": "bool",
        "default": False,
        "label": "Auto Promote to Global",
        "description": "Automatically promote approved synonym proposals to global map only after validation and conflict checks pass.",
        "group": "agentic",
        "config_path": ["synonym_management", "auto_promote_global_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.auto_accept_ai_action_enabled",
        "type": "bool",
        "default": True,
        "label": "Auto Accept AI Action",
        "description": "Allow run-all low-risk AI review-required records to be auto-accepted as final CV artifacts by policy.",
        "group": "agentic",
        "config_path": ["synonym_management", "auto_accept_ai_action_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "cv_analysis.semantic_alignment.enabled",
        "type": "bool",
        "default": False,
        "label": "Semantic Alignment Enabled",
        "description": "Enable hybrid lexical-plus-semantic scoring for cv_analysis required-skill, role, domain, and responsibility alignment.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "cv_analysis.semantic_alignment.model",
        "type": "str",
        "default": "text-embedding-005",
        "label": "Semantic Alignment Model",
        "description": "Embedding model used for cv_analysis semantic skill, role, domain, and responsibility similarity.",
        "options": ["text-embedding-005"],
        "ui_surface": _UI_SURFACE_METADATA_ONLY,
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "model"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.required_skill_lexical_weight",
        "type": "float",
        "default": 0.70,
        "label": "Required Skill Lexical Weight",
        "description": "Relative weight of lexical overlap inside cv_analysis required-skill support.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "required_skill_lexical_weight"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.required_skill_semantic_weight",
        "type": "float",
        "default": 0.30,
        "label": "Required Skill Semantic Weight",
        "description": "Relative weight of embedding similarity inside cv_analysis required-skill support.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "required_skill_semantic_weight"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.role_lexical_weight",
        "type": "float",
        "default": 0.60,
        "label": "Role Lexical Weight",
        "description": "Relative weight of lexical overlap and role-family heuristics inside cv_analysis role alignment.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "role_lexical_weight"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.role_semantic_weight",
        "type": "float",
        "default": 0.40,
        "label": "Role Semantic Weight",
        "description": "Relative weight of embedding similarity inside cv_analysis role alignment.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "role_semantic_weight"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.responsibility_lexical_weight",
        "type": "float",
        "default": 0.25,
        "label": "Responsibility Lexical Weight",
        "description": "Relative weight of lexical overlap inside cv_analysis responsibility alignment.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "responsibility_lexical_weight"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.responsibility_semantic_weight",
        "type": "float",
        "default": 0.75,
        "label": "Responsibility Semantic Weight",
        "description": "Relative weight of embedding similarity inside cv_analysis responsibility alignment.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "responsibility_semantic_weight"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.domain_lexical_weight",
        "type": "float",
        "default": 0.40,
        "label": "Domain Lexical Weight",
        "description": "Relative weight of lexical overlap inside cv_analysis domain alignment.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "domain_lexical_weight"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.domain_semantic_weight",
        "type": "float",
        "default": 0.60,
        "label": "Domain Semantic Weight",
        "description": "Relative weight of embedding similarity inside cv_analysis domain alignment.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "domain_semantic_weight"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    {
        "key": "cv_analysis.semantic_alignment.channel_pool_size",
        "type": "int",
        "default": 4,
        "label": "Semantic Channel Pool Size",
        "description": "Maximum number of candidates retained per cv_analysis retrieval channel before merge and final bounded selection.",
        "group": "retrieval",
        "config_path": ["cv_analysis", "semantic_alignment", "channel_pool_size"],
        "agentic_section": _AGENTIC_SECTION_ADVANCED,
    },
    # ── Timing / Throttling ───────────────────────────────────────────────────
    {
        "key": "enrichment_sleep_secs",
        "type": "float",
        "default": 1.0,
        "label": "API Delay: Data Enrichment",
        "description": "Seconds to wait between calls to the web scraping/enrichment API to avoid rate limiting.",
        "group": "timing",
        "config_path": ["enrichment_sleep_secs"],
    },
    {
        "key": "rerank_sleep_secs",
        "type": "float",
        "default": 0.5,
        "label": "API Delay: AI Reranking",
        "description": "Seconds to wait between concurrent/sequential LLM calls during candidate scoring.",
        "group": "timing",
        "config_path": ["rerank_sleep_secs"],
    },
    {
        "key": "enrichment_batch_size",
        "type": "int",
        "default": 10,
        "label": "Enrichment Batch Size",
        "description": "How many jobs each enrich worker batch handles at once before the next scheduling boundary.",
        "group": "timing",
        "config_path": ["enrichment_batch_size"],
    },
    {
        "key": "enrichment_concurrency",
        "type": "int",
        "default": 1,
        "label": "Enrichment Concurrency",
        "description": "How many enrich batches may run concurrently. Higher values can improve throughput, but the stage still uses shared rate limiting so gains are not linear.",
        "group": "timing",
        "config_path": ["enrichment_concurrency"],
    },
    # ── Run Lifecycle ─────────────────────────────────────────────────────────
    {
        "key": "run_lifecycle.max_runtime_minutes",
        "type": "int",
        "default": 240,
        "label": "Maximum Run Duration (Minutes)",
        "description": "Safety guard for unfinished runs. Lower values fail or cancel stuck runs sooner; higher values allow longer recovery windows.",
        "group": "run_lifecycle",
        "config_path": ["run_lifecycle", "max_runtime_minutes"],
    },
    # ── Ranking Policy ────────────────────────────────────────────────────────
    {
        "key": "ranking_weights.ai_score",
        "type": "float",
        "default": 0.40,
        "label": "Weight: AI Score",
        "description": "How much influence the LLM-evaluated fit score has on the final candidate ranking.",
        "group": "ranking",
        "config_path": ["ranking_weights", "ai_score"],
    },
    {
        "key": "ranking_weights.must_have_match",
        "type": "float",
        "default": 0.20,
        "label": "Weight: Must-Have Skills",
        "description": "How much influence the strict matching of required skills has on the final ranking.",
        "group": "ranking",
        "config_path": ["ranking_weights", "must_have_match"],
    },
    {
        "key": "ranking_weights.vector_similarity",
        "type": "float",
        "default": 0.15,
        "label": "Weight: Vector Similarity",
        "description": "How much influence the embedding-based vector similarity score has on the final ranking.",
        "group": "ranking",
        "config_path": ["ranking_weights", "vector_similarity"],
    },
    {
        "key": "ranking_weights.title_relevance",
        "type": "float",
        "default": 0.10,
        "label": "Weight: Title Relevance",
        "description": "How much influence semantic role alignment between the job title and the candidate's target role has on the final ranking.",
        "group": "ranking",
        "config_path": ["ranking_weights", "title_relevance"],
    },
    {
        "key": "ranking_weights.seniority_fit",
        "type": "float",
        "default": 0.10,
        "label": "Weight: Seniority Alignment",
        "description": "How much influence the match between job seniority requirements and candidate experience has.",
        "group": "ranking",
        "config_path": ["ranking_weights", "seniority_fit"],
    },
    {
        "key": "ranking_weights.preference_fit",
        "type": "float",
        "default": 0.05,
        "label": "Weight: Preference Alignment",
        "description": "How much influence weighted candidate preference alignment across domain, role family, and location type has on the final candidate ranking.",
        "group": "ranking",
        "config_path": ["ranking_weights", "preference_fit"],
    },
    {
        "key": "preference_fit_weights.domain",
        "type": "float",
        "default": 0.50,
        "label": "Preference Weight: Domain",
        "description": "Relative importance of explicit domain preference alignment within the preference-fit feature.",
        "group": "ranking",
        "config_path": ["preference_fit_weights", "domain"],
    },
    {
        "key": "preference_fit_weights.role_family",
        "type": "float",
        "default": 0.30,
        "label": "Preference Weight: Role Family",
        "description": "Relative importance of explicit role-family preference alignment within the preference-fit feature.",
        "group": "ranking",
        "config_path": ["preference_fit_weights", "role_family"],
    },
    {
        "key": "preference_fit_weights.location_type",
        "type": "float",
        "default": 0.20,
        "label": "Preference Weight: Location Type",
        "description": "Relative importance of explicit location-type preference alignment within the preference-fit feature.",
        "group": "ranking",
        "config_path": ["preference_fit_weights", "location_type"],
    },
    {
        "key": "fit_label_thresholds.strong",
        "type": "float",
        "default": 0.70,
        "label": "Threshold: Strong Overall Fit",
        "description": "The minimum AI reranker score required to categorize a shortlisted job as a 'Strong' fit.",
        "group": "ranking",
        "config_path": ["fit_label_thresholds", "strong"],
    },
    {
        "key": "fit_label_thresholds.stretch",
        "type": "float",
        "default": 0.40,
        "label": "Threshold: Stretch Overall Fit",
        "description": "The minimum AI reranker score required to categorize a shortlisted job as a 'Stretch' fit.",
        "group": "ranking",
        "config_path": ["fit_label_thresholds", "stretch"],
    },
    {
        "key": "gap_thresholds.strong_min_matched_ratio",
        "type": "float",
        "default": 0.80,
        "label": "Skill Ratio Limit: Strong Match",
        "description": "The minimum percentage of required skills a candidate must possess to avoid a 'Strong' gap penalty.",
        "group": "ranking",
        "config_path": ["gap_thresholds", "strong_min_matched_ratio"],
    },
    {
        "key": "gap_thresholds.stretch_min_matched_ratio",
        "type": "float",
        "default": 0.50,
        "label": "Skill Ratio Limit: Stretch Match",
        "description": "The minimum percentage of required skills a candidate must possess to avoid a 'Stretch' gap penalty.",
        "group": "ranking",
        "config_path": ["gap_thresholds", "stretch_min_matched_ratio"],
    },
    # ── Global Job Filters ──────────────────────────────────────────────────────────────────────
    {
        "key": "global_job_filters.applications_count_max",
        "type": "int",
        "default": 200,
        "label": "Maximum Applicant Count",
        "description": "Reject jobs before enrichment when the applicant count exceeds this threshold.",
        "group": "global_job_filters",
        "config_path": ["global_job_filters", "applications_count_max"],
    },
    {
        "key": "global_job_filters.max_age_days",
        "type": "int",
        "default": 30,
        "label": "Maximum Posting Age (Days)",
        "description": "Reject jobs before enrichment when the posting is older than this many days. Missing posted date still passes.",
        "group": "global_job_filters",
        "config_path": ["global_job_filters", "max_age_days"],
    },
    {
        "key": "rule_filter.selected_filters",
        "type": "list[str]",
        "default": [
            "seniority_mismatch",
            "location_type_excluded",
            "contract_type_excluded",
            "experience_level_excluded",
        ],
        "label": "Blocking Rule Filters",
        "description": "Choose which post-enrichment deterministic rule filters reject jobs. Unselected filters are still evaluated and recorded as marks.",
        "options": _RULE_FILTER_SELECTABLE_OPTIONS,
        "group": "rule_filter",
        "config_path": ["rule_filter", "selected_filters"],
    },
    # ── CV Generation ──────────────────────────────────────────────────────
    {
        "key": "cv_generation_model",
        "type": "str",
        "default": "gemini-2.5-flash",
        "label": "CV Generation Model",
        "description": "Choose the model that writes final CV content for future runs.",
        "options": _CV_GENERATION_MODELS,
        "ui_surface": _UI_SURFACE_EDITABLE,
        "group": "cv_composition",
        "config_path": ["cv", "generation", "model"],
    },
    {
        "key": "cv_preset",
        "type": "str",
        "default": "europass",
        "label": "CV Preset",
        "description": "The CV preset to use for generation. Controls template, section order, and supported composition options.",
        "options": _CV_PRESET_OPTIONS,
        "ui_surface": _UI_SURFACE_METADATA_ONLY,
        "group": "cv_preset",
        "config_path": ["cv", "preset"],
    },
    # ── CV Composition ─────────────────────────────────────────────────────────
    {
        "key": "cv_summary_enabled",
        "type": "bool",
        "default": True,
        "label": "Include Summary",
        "description": "Whether to include a professional summary section in generated CVs.",
        "group": "cv_composition",
        "config_path": ["cv", "composition", "summary", "enabled"],
    },
    {
        "key": "cv_education_enabled",
        "type": "bool",
        "default": True,
        "label": "Include Education",
        "description": "Whether to include an Education section in generated CVs.",
        "group": "cv_composition",
        "config_path": ["cv", "composition", "education", "enabled"],
    },
    {
        "key": "cv_experience_enabled",
        "type": "bool",
        "default": True,
        "label": "Include Experience",
        "description": "Whether to include a Work Experience section in generated CVs.",
        "group": "cv_composition",
        "config_path": ["cv", "composition", "experience", "enabled"],
    },
    {
        "key": "cv_skills_enabled",
        "type": "bool",
        "default": True,
        "label": "Include Skills",
        "description": "Whether to include a Skills section in generated CVs.",
        "group": "cv_composition",
        "config_path": ["cv", "composition", "skills", "enabled"],
    },
    {
        "key": "cv_certifications_enabled",
        "type": "bool",
        "default": True,
        "label": "Include Certifications",
        "description": "Whether to include a Certifications section in generated CVs.",
        "group": "cv_composition",
        "config_path": ["cv", "composition", "certifications", "enabled"],
    },
    {
        "key": "cv_projects_enabled",
        "type": "bool",
        "default": True,
        "label": "Include Projects",
        "description": "Whether to include a Projects section in generated CVs.",
        "group": "cv_composition",
        "config_path": ["cv", "composition", "projects", "enabled"],
    },
    {
        "key": "cv_publications_enabled",
        "type": "bool",
        "default": False,
        "label": "Include Publications",
        "description": "Whether to include a Publications section in generated CVs.",
        "group": "cv_composition",
        "config_path": ["cv", "composition", "publications", "enabled"],
    },
    {
        "key": "cv_languages_enabled",
        "type": "bool",
        "default": True,
        "label": "Include Languages",
        "description": "Whether to include a Languages section in generated CVs.",
        "group": "cv_composition",
        "config_path": ["cv", "composition", "languages", "enabled"],
    },
    # ── CV Validation ────────────────────────────────────────────────────────
    {
        "key": "cv_max_pages",
        "type": "int",
        "default": 2,
        "label": "CV Maximum Pages",
        "description": "Warning-only page budget for generated CV documents.",
        "group": "cv_validation",
        "config_path": ["cv", "validation", "max_pages"],
    },
]


def _resolve_config_path_default(
    config: dict[str, Any],
    path: list[str],
) -> Any:
    current: Any = config
    for part in path:
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _hydrate_schema_defaults_from_config() -> None:
    try:
        baseline_config = load_config()
    except Exception:
        return

    for entry in SETTINGS_SCHEMA:
        config_path = entry.get("config_path")
        if not isinstance(config_path, list) or not config_path:
            continue
        resolved_default = _resolve_config_path_default(baseline_config, config_path)
        if resolved_default is None:
            continue
        if isinstance(entry.get("default"), list) and isinstance(resolved_default, list):
            entry["default"] = [str(value) for value in resolved_default]
            continue
        entry["default"] = resolved_default


_hydrate_schema_defaults_from_config()

# ── Ranking group registry ────────────────────────────────────────────────────
# Maps URL group slug → ordered list of schema keys in that group.
# Used by the grouped-edit endpoint and the settings template.

RANKING_GROUPS: dict[str, list[str]] = {
    "ranking-weights": [
        "ranking_weights.ai_score",
        "ranking_weights.must_have_match",
        "ranking_weights.vector_similarity",
        "ranking_weights.title_relevance",
        "ranking_weights.seniority_fit",
        "ranking_weights.preference_fit",
    ],
    "preference-fit-weights": [
        "preference_fit_weights.domain",
        "preference_fit_weights.role_family",
        "preference_fit_weights.location_type",
    ],
    "fit-label-thresholds": [
        "fit_label_thresholds.strong",
        "fit_label_thresholds.stretch",
    ],
    "gap-thresholds": [
        "gap_thresholds.strong_min_matched_ratio",
        "gap_thresholds.stretch_min_matched_ratio",
    ],
}

# ── Independent settings section registry ─────────────────────────────────────
# Maps URL section slug → ordered list of schema keys in that section.
# Used by the section-save endpoint (/admin/settings/section/{name}).
# Each section uses one form with one save action; keys are validated
# individually (no cross-key constraints within a section).

SETTINGS_SECTIONS: dict[str, list[str]] = {
    "retrieval-core": [
        "pipeline.vector_search_top_n",
        "pipeline.ai_score_top_n",
        "pipeline.final_top_n",
        "pipeline.evidence_top_k",
    ],
    "timing": [
        "enrichment_sleep_secs",
        "rerank_sleep_secs",
        "enrichment_batch_size",
        "enrichment_concurrency",
    ],
    "run-lifecycle": [
        "run_lifecycle.max_runtime_minutes",
    ],
    "global-job-filters": [
        "global_job_filters.applications_count_max",
        "global_job_filters.max_age_days",
    ],
    "rule-filter": [
        "rule_filter.selected_filters",
    ],
}


def _build_agentic_settings_sections() -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    for entry in SETTINGS_SCHEMA:
        section = entry.get("agentic_section")
        if not isinstance(section, str) or not section:
            continue
        sections.setdefault(section, []).append(entry["key"])
    return sections


AGENTIC_SETTINGS_SECTIONS: dict[str, list[str]] = _build_agentic_settings_sections()

# ── CV Generation settings schema ──────────────────────────────────────────
# Kept for reference and documentation only.  The actual schema entries live
# inside SETTINGS_SCHEMA so they appear alongside all other settings.
# _CV_GENERATION_SCHEMA was removed to avoid duplication.

# ── CV group registry ───────────────────────────────────────────────────────
# Maps URL group slug (used in /admin/settings/group/{slug}) → ordered list
# of schema keys.  CV groups are validated and saved together, just like
# ranking groups, but are kept in a separate namespace.
CV_GROUPS: dict[str, list[str]] = {
    "cv-preset": [
        "cv_preset",
        "cv_generation_model",
    ],
    "cv-composition": [
        "cv_summary_enabled",
        "cv_education_enabled",
        "cv_experience_enabled",
        "cv_skills_enabled",
        "cv_certifications_enabled",
        "cv_projects_enabled",
        "cv_publications_enabled",
        "cv_languages_enabled",
    ],
    "cv-validation": [
        "cv_max_pages",
    ],
}

# ── Combined grouped-registry lookup ───────────────────────────────────────
# Used by the grouped-save endpoint to validate any group request.
ALL_GROUP_REGISTRIES: dict[str, dict[str, list[str]]] = {
    "ranking": RANKING_GROUPS,
    "cv": CV_GROUPS,
}

# Build lookup maps once
_ALL_SCHEMA_BY_KEY: dict[str, dict[str, Any]] = {s["key"]: s for s in SETTINGS_SCHEMA}
_METADATA_ONLY_KEYS: frozenset[str] = frozenset(
    entry["key"]
    for entry in SETTINGS_SCHEMA
    if entry.get("ui_surface") == _UI_SURFACE_METADATA_ONLY
)
_EDITABLE_KEYS: frozenset[str] = frozenset(
    entry["key"]
    for entry in SETTINGS_SCHEMA
    if entry.get("ui_surface", _UI_SURFACE_EDITABLE) == _UI_SURFACE_EDITABLE
)
_AGENTIC_KEYS: frozenset[str] = frozenset(
    entry["key"]
    for entry in SETTINGS_SCHEMA
    if isinstance(entry.get("agentic_section"), str)
)
_EDITABLE_AGENTIC_KEYS: frozenset[str] = frozenset(
    key for key in _AGENTIC_KEYS if key in _EDITABLE_KEYS
)
_METADATA_ONLY_AGENTIC_KEYS: frozenset[str] = frozenset(
    key for key in _AGENTIC_KEYS if key in _METADATA_ONLY_KEYS
)
_WEIGHT_KEYS: frozenset[str] = frozenset(
    s["key"] for s in SETTINGS_SCHEMA if s["key"].startswith("ranking_weights.")
)
_PREFERENCE_WEIGHT_KEYS: frozenset[str] = frozenset(
    s["key"] for s in SETTINGS_SCHEMA if s["key"].startswith("preference_fit_weights.")
)


def metadata_only_settings_keys() -> set[str]:
    return set(_METADATA_ONLY_KEYS)


def editable_settings_keys() -> set[str]:
    return set(_EDITABLE_KEYS)


def editable_agentic_settings_keys() -> set[str]:
    return set(_EDITABLE_AGENTIC_KEYS)


def metadata_only_agentic_settings_keys() -> set[str]:
    return set(_METADATA_ONLY_AGENTIC_KEYS)


def excluded_agentic_settings_keys() -> set[str]:
    return set(_EXCLUDED_AGENTIC_KEYS)


# ── coercion ──────────────────────────────────────────────────────────────────

def coerce_value(key: str, raw: Any) -> int | float | str | bool | list[str]:
    """Cast raw value (string or numeric) to the type declared in the schema."""
    entry = _ALL_SCHEMA_BY_KEY[key]  # raises KeyError for unknown keys
    if entry["type"] == "int":
        return int(raw)
    elif entry["type"] == "float":
        return float(raw)
    elif entry["type"] == "str":
        return str(raw).strip()
    elif entry["type"] == "bool":
        if isinstance(raw, bool):
            return raw
        s = str(raw).strip().lower()
        if s in ("true", "1", "yes", "on"):
            return True
        if s in ("false", "0", "no", "off", ""):
            return False
        raise ValueError(f"{key} must be a boolean value, got {raw!r}")
    elif entry["type"] == "list[str]":
        if isinstance(raw, list):
            return [str(v).strip() for v in raw]
        return [str(raw).strip()]
    raise TypeError(f"Unsupported type {entry['type']!r} for key {key!r}")


# ── validation ────────────────────────────────────────────────────────────────

def validate_settings(settings: dict[str, Any]) -> None:
    """Validate a (possibly partial) settings dict.

    Raises ValidationError with a descriptive message on any violation.
    settings values must already be coerced to their declared Python types.
    """
    for key, value in settings.items():
        if key not in _ALL_SCHEMA_BY_KEY:
            raise ValidationError(f"Unknown setting key: '{key}'")
        entry = _ALL_SCHEMA_BY_KEY[key]

        if entry["type"] == "int":
            if not isinstance(value, int) or value < 1:
                raise ValidationError(f"{key} must be an integer >= 1, got {value!r}")
        elif entry["type"] == "float":
            fval = float(value)
            if key.endswith("_secs"):
                if fval < 0.0:
                    raise ValidationError(f"{key} must be >= 0.0, got {fval}")
            else:
                if not (0.0 <= fval <= 1.0):
                    raise ValidationError(
                        f"{key} must be in range [0.0, 1.0], got {fval}"
                    )
        elif entry["type"] == "str":
            if not value or not value.strip():
                raise ValidationError(f"{key} must not be empty or whitespace-only")
            options = entry.get("options")
            if options is not None and value not in options:
                raise ValidationError(
                    f"{key} must be one of {', '.join(options)}, got {value!r}"
                )
        elif entry["type"] == "bool":
            if not isinstance(value, bool):
                raise ValidationError(f"{key} must be a boolean, got {value!r}")
        elif entry["type"] == "list[str]":
            if not isinstance(value, list):
                raise ValidationError(f"{key} must be a list of strings, got {type(value).__name__}")
            if len(value) == 0:
                raise ValidationError(f"{key} must not be empty")
            for item in value:
                if not isinstance(item, str) or not item.strip():
                    raise ValidationError(f"{key} contains a blank entry: {value!r}")
            seen: list[str] = []
            for item in value:
                if item in seen:
                    raise ValidationError(
                        f"{key} contains duplicate entries (order preserved, duplicates rejected): {value!r}"
                    )
                seen.append(item)
            options = entry.get("options")
            if options is not None:
                unknown = [item for item in value if item not in options]
                if unknown:
                    raise ValidationError(
                        f"{key} must be one of {', '.join(options)}, got invalid entries: {unknown!r}"
                    )

    # ── relational constraints ────────────────────────────────────────────────
    vs = settings.get("pipeline.vector_search_top_n")
    ai = settings.get("pipeline.ai_score_top_n")
    fn = settings.get("pipeline.final_top_n")
    if isinstance(vs, int) and isinstance(ai, int) and ai > vs:
        raise ValidationError(
            f"pipeline.ai_score_top_n ({ai}) must be <= pipeline.vector_search_top_n ({vs})"
        )
    if isinstance(ai, int) and isinstance(fn, int) and fn > ai:
        raise ValidationError(
            f"pipeline.final_top_n ({fn}) must be <= pipeline.ai_score_top_n ({ai})"
        )

    strong = settings.get("fit_label_thresholds.strong")
    stretch = settings.get("fit_label_thresholds.stretch")
    if isinstance(strong, float) and isinstance(stretch, float) and strong <= stretch:
        raise ValidationError(
            f"fit_label_thresholds.strong ({strong}) must be > stretch ({stretch})"
        )

    g_strong = settings.get("gap_thresholds.strong_min_matched_ratio")
    g_stretch = settings.get("gap_thresholds.stretch_min_matched_ratio")
    if isinstance(g_strong, float) and isinstance(g_stretch, float) and g_strong <= g_stretch:
        raise ValidationError(
            f"gap_thresholds.strong_min_matched_ratio ({g_strong}) must be > stretch ({g_stretch})"
        )

    # Ranking weights sum-to-1 only checked when all 6 are present
    if _WEIGHT_KEYS <= set(settings.keys()):
        total = sum(float(settings[k]) for k in _WEIGHT_KEYS)
        if abs(total - 1.0) > 0.01:
            raise ValidationError(
                f"ranking_weights must sum to 1.0 (± 0.01), got {total:.4f}"
            )
    if _PREFERENCE_WEIGHT_KEYS <= set(settings.keys()):
        total = sum(float(settings[k]) for k in _PREFERENCE_WEIGHT_KEYS)
        if abs(total - 1.0) > 0.01:
            raise ValidationError(
                f"preference_fit_weights must sum to 1.0 (± 0.01), got {total:.4f}"
            )
    if _RESPONSIBILITY_ALIGNMENT_WEIGHT_KEYS <= set(settings.keys()):
        total = sum(float(settings[key]) for key in _RESPONSIBILITY_ALIGNMENT_WEIGHT_KEYS)
        if abs(total - 1.0) > 0.01:
            raise ValidationError(
                f"cv_analysis responsibility semantic alignment weights must sum to 1.0 (± 0.01), got {total:.4f}"
            )
    if _REQUIRED_SKILL_ALIGNMENT_WEIGHT_KEYS <= set(settings.keys()):
        total = sum(float(settings[key]) for key in _REQUIRED_SKILL_ALIGNMENT_WEIGHT_KEYS)
        if abs(total - 1.0) > 0.01:
            raise ValidationError(
                f"cv_analysis required-skill semantic alignment weights must sum to 1.0 (± 0.01), got {total:.4f}"
            )
    if _ROLE_ALIGNMENT_WEIGHT_KEYS <= set(settings.keys()):
        total = sum(float(settings[key]) for key in _ROLE_ALIGNMENT_WEIGHT_KEYS)
        if abs(total - 1.0) > 0.01:
            raise ValidationError(
                f"cv_analysis role semantic alignment weights must sum to 1.0 (± 0.01), got {total:.4f}"
            )
    if _DOMAIN_ALIGNMENT_WEIGHT_KEYS <= set(settings.keys()):
        total = sum(float(settings[key]) for key in _DOMAIN_ALIGNMENT_WEIGHT_KEYS)
        if abs(total - 1.0) > 0.01:
            raise ValidationError(
                f"cv_analysis domain semantic alignment weights must sum to 1.0 (± 0.01), got {total:.4f}"
            )


# ── config application ────────────────────────────────────────────────────────

def apply_settings_to_config(config: dict[str, Any], settings: dict[str, Any]) -> None:
    """Write settings values into a config dict in-place.

    Uses config_path from the schema registry to navigate nested dicts.
    settings values must already be coerced to their declared Python types.
    """
    for key, value in settings.items():
        path = _ALL_SCHEMA_BY_KEY[key]["config_path"]
        target = config
        for part in path[:-1]:
            target = target.setdefault(part, {})
        target[path[-1]] = value
