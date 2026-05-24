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
_RESPONSIBILITY_ALIGNMENT_WEIGHT_KEYS: frozenset[str] = frozenset(
    {
    "cv_analysis.semantic_alignment.responsibility_lexical_weight",
    "cv_analysis.semantic_alignment.responsibility_semantic_weight",
    }
)
_DOMAIN_ALIGNMENT_WEIGHT_KEYS: frozenset[str] = frozenset(
    {
    "cv_analysis.semantic_alignment.domain_lexical_weight",
    "cv_analysis.semantic_alignment.domain_semantic_weight",
    }
)
_REQUIRED_SKILL_ALIGNMENT_WEIGHT_KEYS: frozenset[str] = frozenset(
    {
    "cv_analysis.semantic_alignment.required_skill_lexical_weight",
    "cv_analysis.semantic_alignment.required_skill_semantic_weight",
    }
)
_ROLE_ALIGNMENT_WEIGHT_KEYS: frozenset[str] = frozenset(
    {
    "cv_analysis.semantic_alignment.role_lexical_weight",
    "cv_analysis.semantic_alignment.role_semantic_weight",
    }
)
_UI_SURFACE_EDITABLE = "editable"
_UI_SURFACE_METADATA_ONLY = "metadata_only"
_UI_DEPRECATION_ACTIVE = "active"
_UI_DEPRECATION_HIDDEN = "hidden_deprecated"
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
        "default": True,
        "label": "Synonym Apply-to-Run (Manual Capability Gate)",
        "description": "Permission gate for apply-to-run capability. OFF blocks both manual and automatic apply actions. ON allows manual apply. Automatic apply still needs its own automation toggle.",
        "group": "agentic",
        "config_path": ["synonym_management", "apply_to_run_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.promote_global_enabled",
        "type": "bool",
        "default": True,
        "label": "Synonym Promote-Global (Manual Capability Gate)",
        "description": "Permission gate for promote-global capability. OFF blocks both manual and automatic promote actions. ON allows manual promote. Automatic promote still needs its own automation toggle.",
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
        "key": "reuse.enrich.enabled",
        "type": "bool",
        "default": True,
        "label": "Reuse Enrichment Results",
        "description": "Reuse cached enrichment results on exact-match fingerprints; disable to force fresh enrichment.",
        "group": "agentic",
        "config_path": ["reuse", "enrich", "enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "reuse.ranking.enabled",
        "type": "bool",
        "default": True,
        "label": "Reuse Ranking Scores",
        "description": "Reuse exact-match ranking AI scores; disable to force fresh reranking compute.",
        "group": "agentic",
        "config_path": ["reuse", "ranking", "enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "reuse.cv_analysis.enabled",
        "type": "bool",
        "default": True,
        "label": "Reuse CV Analysis",
        "description": "Reuse exact-match cv_analysis outputs; disable to force fresh late-stage analysis.",
        "group": "agentic",
        "config_path": ["reuse", "cv_analysis", "enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "reuse.cv_generation.enabled",
        "type": "bool",
        "default": True,
        "label": "Reuse CV Generation",
        "description": "Reuse exact-match CV generation artifacts; disable to force fresh markdown generation.",
        "group": "agentic",
        "config_path": ["reuse", "cv_generation", "enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "reuse.synonym_triage.enabled",
        "type": "bool",
        "default": True,
        "label": "Reuse Synonym Triage",
        "description": "Canonical synonym triage reuse gate; when unset, legacy synonym-management reuse key remains backward compatible.",
        "group": "agentic",
        "config_path": ["reuse", "synonym_triage", "enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.auto_apply_recommendation_enabled",
        "type": "bool",
        "default": False,
        "label": "Auto Apply Recommendation (Automatic Execution)",
        "description": "Automation policy toggle. When ON, system can auto-apply recommended actions after safety checks pass. Requires Synonym Apply-to-Run gate ON and never bypasses that gate.",
        "group": "agentic",
        "config_path": ["synonym_management", "auto_apply_recommendation_enabled"],
        "agentic_section": _AGENTIC_SECTION_CORE,
    },
    {
        "key": "synonym_management.auto_promote_global_enabled",
        "type": "bool",
        "default": False,
        "label": "Auto Promote to Global (Automatic Execution)",
        "description": "Automation policy toggle. When ON, system can auto-promote approved actions after validation and conflict checks pass. Requires Synonym Promote-Global gate ON and never bypasses that gate.",
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
        "key": "stage_runtime.enrich.sleep_secs",
        "type": "float",
        "default": 0.0,
        "label": "API Delay: Enrichment Stage",
        "description": "Canonical delay between enrich-stage API calls for shared throttling.",
        "group": "timing",
        "config_path": ["stage_runtime", "enrich", "sleep_secs"],
    },
    {
        "key": "stage_runtime.enrich.batch_size",
        "type": "int",
        "default": 10,
        "label": "Batch Size: Enrichment Stage",
        "description": "Canonical enrich-stage batch size before each scheduling boundary.",
        "group": "timing",
        "config_path": ["stage_runtime", "enrich", "batch_size"],
    },
    {
        "key": "stage_runtime.enrich.concurrency",
        "type": "int",
        "default": 8,
        "label": "Concurrency: Enrichment Stage",
        "description": "Canonical enrich-stage concurrent batch worker count.",
        "group": "timing",
        "config_path": ["stage_runtime", "enrich", "concurrency"],
    },
    {
        "key": "stage_runtime.ranking.sleep_secs",
        "type": "float",
        "default": 0.0,
        "label": "API Delay: Ranking Stage",
        "description": "Canonical delay between ranking-stage AI scoring calls.",
        "group": "timing",
        "config_path": ["stage_runtime", "ranking", "sleep_secs"],
    },
    {
        "key": "stage_runtime.ranking.concurrency",
        "type": "int",
        "default": 4,
        "label": "Concurrency: Ranking Stage",
        "description": "Canonical ranking-stage concurrent AI scoring worker count.",
        "group": "timing",
        "config_path": ["stage_runtime", "ranking", "concurrency"],
    },
    {
        "key": "stage_runtime.cv_analysis.sleep_secs",
        "type": "float",
        "default": 0.0,
        "label": "API Delay: CV Analysis Stage",
        "description": "Canonical delay between cv_analysis stage AI calls when enabled.",
        "group": "timing",
        "config_path": ["stage_runtime", "cv_analysis", "sleep_secs"],
    },
    {
        "key": "stage_runtime.cv_analysis.concurrency",
        "type": "int",
        "default": 4,
        "label": "Concurrency: CV Analysis Stage",
        "description": "Canonical cv_analysis stage concurrent worker count.",
        "group": "timing",
        "config_path": ["stage_runtime", "cv_analysis", "concurrency"],
    },
    {
        "key": "stage_runtime.cv_generation.sleep_secs",
        "type": "float",
        "default": 0.0,
        "label": "API Delay: CV Generation Stage",
        "description": "Canonical delay between cv_generation stage AI calls when enabled.",
        "group": "timing",
        "config_path": ["stage_runtime", "cv_generation", "sleep_secs"],
    },
    {
        "key": "stage_runtime.cv_generation.concurrency",
        "type": "int",
        "default": 4,
        "label": "Concurrency: CV Generation Stage",
        "description": "Canonical cv_generation stage concurrent worker count.",
        "group": "timing",
        "config_path": ["stage_runtime", "cv_generation", "concurrency"],
    },
    {
        "key": "enrichment_sleep_secs",
        "type": "float",
        "default": 0.0,
        "label": "API Delay: Data Enrichment",
        "description": "Seconds to wait between calls to the web scraping/enrichment API to avoid rate limiting.",
        "group": "timing",
        "config_path": ["enrichment_sleep_secs"],
        "compatibility_alias_for": "stage_runtime.enrich.sleep_secs",
    },
    {
        "key": "rerank_sleep_secs",
        "type": "float",
        "default": 0.0,
        "label": "API Delay: AI Reranking",
        "description": "Seconds to wait between concurrent/sequential LLM calls during candidate scoring.",
        "group": "timing",
        "config_path": ["rerank_sleep_secs"],
        "compatibility_alias_for": "stage_runtime.ranking.sleep_secs",
    },
    {
        "key": "enrichment_batch_size",
        "type": "int",
        "default": 10,
        "label": "Enrichment Batch Size",
        "description": "How many jobs each enrich worker batch handles at once before the next scheduling boundary.",
        "group": "timing",
        "config_path": ["enrichment_batch_size"],
        "compatibility_alias_for": "stage_runtime.enrich.batch_size",
    },
    {
        "key": "enrichment_concurrency",
        "type": "int",
        "default": 8,
        "label": "Enrichment Concurrency",
        "description": "How many enrich batches may run concurrently. Higher values can improve throughput, but the stage still uses shared rate limiting so gains are not linear.",
        "group": "timing",
        "config_path": ["enrichment_concurrency"],
        "compatibility_alias_for": "stage_runtime.enrich.concurrency",
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
        "ui_deprecation_state": _UI_DEPRECATION_HIDDEN,
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


def _copy_schema_entries(schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    for entry in schema:
        cloned = dict(entry)
        if isinstance(cloned.get("default"), list):
            cloned["default"] = list(cloned["default"])
        copied.append(cloned)
    return copied

def settings_schema_with_runtime_defaults(
    baseline_config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    schema = _copy_schema_entries(SETTINGS_SCHEMA)
    if baseline_config is None:
        try:
            baseline_config = load_config()
        except Exception:
            baseline_config = {}

    for entry in schema:
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
    return schema

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
        "stage_runtime.enrich.sleep_secs",
        "stage_runtime.enrich.batch_size",
        "stage_runtime.enrich.concurrency",
        "stage_runtime.ranking.sleep_secs",
        "stage_runtime.ranking.concurrency",
        "stage_runtime.cv_analysis.sleep_secs",
        "stage_runtime.cv_analysis.concurrency",
        "stage_runtime.cv_generation.sleep_secs",
        "stage_runtime.cv_generation.concurrency",
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


AGENTIC_ENABLEMENT_SECTION_KEYS: list[str] = [
    "cv.agentic_late_stage.enabled",
    "cv_analysis.semantic_alignment.enabled",
    "synonym_management.propose_enabled",
    "synonym_management.apply_to_run_enabled",
    "synonym_management.promote_global_enabled",
    "reuse.enrich.enabled",
    "reuse.ranking.enabled",
    "reuse.cv_analysis.enabled",
    "reuse.cv_generation.enabled",
    "reuse.synonym_triage.enabled",
]

AGENTIC_REUSE_SECTION_KEYS: list[str] = [
    "reuse.enrich.enabled",
    "reuse.ranking.enabled",
    "reuse.cv_analysis.enabled",
    "reuse.cv_generation.enabled",
    "reuse.synonym_triage.enabled",
]

AGENTIC_AUTOMATION_SECTION_KEYS: list[str] = [
    "synonym_management.auto_triage_recommendation_enabled",
    "synonym_management.triage_recommendation_reuse_enabled",
    "synonym_management.auto_apply_recommendation_enabled",
    "synonym_management.auto_promote_global_enabled",
    "synonym_management.auto_accept_ai_action_enabled",
]

AGENTIC_ADVANCED_SECTION_KEYS: list[str] = [
    "cv_analysis.semantic_alignment.model",
    "cv_analysis.semantic_alignment.required_skill_lexical_weight",
    "cv_analysis.semantic_alignment.required_skill_semantic_weight",
    "cv_analysis.semantic_alignment.role_lexical_weight",
    "cv_analysis.semantic_alignment.role_semantic_weight",
    "cv_analysis.semantic_alignment.responsibility_lexical_weight",
    "cv_analysis.semantic_alignment.responsibility_semantic_weight",
    "cv_analysis.semantic_alignment.domain_lexical_weight",
    "cv_analysis.semantic_alignment.domain_semantic_weight",
    "cv_analysis.semantic_alignment.channel_pool_size",
]

AGENTIC_SETTINGS_SECTIONS: dict[str, list[str]] = {
    "agentic-enablement": list(AGENTIC_ENABLEMENT_SECTION_KEYS),
    "agentic-automation": list(AGENTIC_AUTOMATION_SECTION_KEYS),
    "agentic-advanced": list(AGENTIC_ADVANCED_SECTION_KEYS),
}

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
_HIDDEN_DEPRECATED_KEYS: frozenset[str] = frozenset(
    entry["key"]
    for entry in SETTINGS_SCHEMA
    if entry.get("ui_deprecation_state") == _UI_DEPRECATION_HIDDEN
)
# Transitional overlap contract: hidden-deprecated keys must not remain editable unless explicitly allowlisted.
_EDITABLE_HIDDEN_DEPRECATED_ALLOWLIST: frozenset[str] = frozenset({"cv_generation_model"})
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

# Declarative constraint registry (Task 4 Step 1): behavior still enforced by legacy checks below.
_RELATIONAL_ORDER_CONSTRAINTS: tuple[tuple[str, str, str], ...] = (
    (
        "pipeline.vector_search_top_n",
        "pipeline.ai_score_top_n",
        "pipeline.ai_score_top_n ({rhs}) must be <= pipeline.vector_search_top_n ({lhs})",
    ),
    (
        "pipeline.ai_score_top_n",
        "pipeline.final_top_n",
        "pipeline.final_top_n ({rhs}) must be <= pipeline.ai_score_top_n ({lhs})",
    ),
    (
        "fit_label_thresholds.strong",
        "fit_label_thresholds.stretch",
        "fit_label_thresholds.strong ({lhs}) must be > stretch ({rhs})",
    ),
    (
        "gap_thresholds.strong_min_matched_ratio",
        "gap_thresholds.stretch_min_matched_ratio",
        "gap_thresholds.strong_min_matched_ratio ({lhs}) must be > stretch ({rhs})",
    ),
)

_WEIGHT_SUM_CONSTRAINTS: tuple[tuple[frozenset[str], str], ...] = (
    (_WEIGHT_KEYS, "ranking_weights"),
    (_PREFERENCE_WEIGHT_KEYS, "preference_fit_weights"),
    (_RESPONSIBILITY_ALIGNMENT_WEIGHT_KEYS, "cv_analysis responsibility semantic alignment weights"),
    (_REQUIRED_SKILL_ALIGNMENT_WEIGHT_KEYS, "cv_analysis required-skill semantic alignment weights"),
    (_ROLE_ALIGNMENT_WEIGHT_KEYS, "cv_analysis role semantic alignment weights"),
    (_DOMAIN_ALIGNMENT_WEIGHT_KEYS, "cv_analysis domain semantic alignment weights"),
)


_IA_DOMAIN_GENERAL = "general"
_IA_DOMAIN_LAYERS = "layers"
_IA_DOMAIN_STAGES = "stages"
_IA_DOMAIN_RULES = "rules"
_IA_DOMAIN_INTEGRATIONS = "integrations"
_IA_DOMAIN_ADVANCED = "advanced"
COMPLEXITY_VIEW_BASIC = "basic"
COMPLEXITY_VIEW_ADVANCED = "advanced"
COMPLEXITY_VIEW_ALL = "all"

DECISION_STATUS_NEEDS_REVIEW = "needs_review"
DECISION_STATUS_RECOMMENDED = "recommended"
DECISION_STATUS_CONFIGURED = "configured"
DECISION_STATUS_ADVANCED = "advanced"
DECISION_STATUS_ALL = "all"

REASON_CODE_MISSING_REQUIRED = "missing_required"
REASON_CODE_CONFLICT = "conflict"
REASON_CODE_LOW_CONFIDENCE = "low_confidence"
REASON_CODE_QUALITY_RISK = "quality_risk"
REASON_CODE_CHANGED_FROM_DEFAULT = "changed_from_default"
REASON_CODE_RECOMMENDED_DELTA = "recommended_delta"
REASON_CODE_ADVANCED_ONLY = "advanced_only"
REASON_CODE_UNUSED = "unused"

_DECISION_STATUS_PRIORITY: dict[str, int] = {
    DECISION_STATUS_NEEDS_REVIEW: 0,
    DECISION_STATUS_RECOMMENDED: 1,
    DECISION_STATUS_CONFIGURED: 2,
    DECISION_STATUS_ADVANCED: 3,
    DECISION_STATUS_ALL: 4,
}

_BLOCKING_REASON_CODES: frozenset[str] = frozenset(
    {
        REASON_CODE_MISSING_REQUIRED,
        REASON_CODE_CONFLICT,
        REASON_CODE_LOW_CONFIDENCE,
        REASON_CODE_QUALITY_RISK,
    }
)

_WORKFLOW_STAGE_NORMALIZE = "normalize"
_WORKFLOW_STAGE_ENRICH = "enrich"
_WORKFLOW_STAGE_RULE_FILTER = "rule_filter"
_WORKFLOW_STAGE_SHORTLIST = "shortlist"
_WORKFLOW_STAGE_RANKING = "ranking"
_WORKFLOW_STAGE_CV_ANALYSIS = "cv_analysis"
_WORKFLOW_STAGE_CV_GENERATION = "cv_generation"

_GROUP_TO_IA_DOMAIN: dict[str, str] = {
    "retrieval": _IA_DOMAIN_STAGES,
    "global_job_filters": _IA_DOMAIN_RULES,
    "rule_filter": _IA_DOMAIN_RULES,
    "run_lifecycle": _IA_DOMAIN_STAGES,
    "ranking": _IA_DOMAIN_RULES,
    "timing": _IA_DOMAIN_ADVANCED,
    "agentic": _IA_DOMAIN_INTEGRATIONS,
    "cv_composition": _IA_DOMAIN_GENERAL,
    "cv_validation": _IA_DOMAIN_STAGES,
    "cv_preset": _IA_DOMAIN_LAYERS,
}

_GROUP_TO_WORKFLOW_STAGES: dict[str, tuple[str, ...]] = {
    "retrieval": (
        _WORKFLOW_STAGE_NORMALIZE,
        _WORKFLOW_STAGE_ENRICH,
        _WORKFLOW_STAGE_RULE_FILTER,
        _WORKFLOW_STAGE_SHORTLIST,
    ),
    "global_job_filters": (
        _WORKFLOW_STAGE_ENRICH,
        _WORKFLOW_STAGE_RULE_FILTER,
    ),
    "rule_filter": (
        _WORKFLOW_STAGE_RULE_FILTER,
    ),
    "run_lifecycle": (
        _WORKFLOW_STAGE_NORMALIZE,
        _WORKFLOW_STAGE_ENRICH,
        _WORKFLOW_STAGE_RULE_FILTER,
        _WORKFLOW_STAGE_SHORTLIST,
        _WORKFLOW_STAGE_RANKING,
        _WORKFLOW_STAGE_CV_ANALYSIS,
        _WORKFLOW_STAGE_CV_GENERATION,
    ),
    "ranking": (
        _WORKFLOW_STAGE_SHORTLIST,
        _WORKFLOW_STAGE_RANKING,
        _WORKFLOW_STAGE_CV_ANALYSIS,
    ),
    "timing": (
        _WORKFLOW_STAGE_ENRICH,
        _WORKFLOW_STAGE_RANKING,
        _WORKFLOW_STAGE_CV_ANALYSIS,
        _WORKFLOW_STAGE_CV_GENERATION,
    ),
    "agentic": (
        _WORKFLOW_STAGE_CV_ANALYSIS,
        _WORKFLOW_STAGE_CV_GENERATION,
    ),
    "cv_composition": (
        _WORKFLOW_STAGE_CV_GENERATION,
    ),
    "cv_validation": (
        _WORKFLOW_STAGE_CV_GENERATION,
    ),
    "cv_preset": (
        _WORKFLOW_STAGE_CV_ANALYSIS,
        _WORKFLOW_STAGE_CV_GENERATION,
    ),
}

_GROUP_TO_APPLIES_WHEN: dict[str, str] = {
    "retrieval": "Used while constructing and narrowing candidate sets before final scoring and synthesis.",
    "global_job_filters": "Used when deterministic global filters evaluate enriched jobs.",
    "rule_filter": "Used when rule-filter stage decides reject vs pass marks.",
    "run_lifecycle": "Used by control-plane timeout guard for queued/running/manual-wait runs.",
    "ranking": "Used during reranking, fit labeling, and gap classification.",
    "timing": "Used by enrich, ranking, cv_analysis, and cv_generation runtime throttling/concurrency controls.",
    "agentic": "Used only when agentic late-stage path or synonym-management controls are active.",
    "cv_composition": "Used when CV generation decides section visibility and output composition intent.",
    "cv_validation": "Used by post-generation CV validation checks.",
    "cv_preset": "Used when resolving CV preset/model defaults for generation.",
}

_STAGE_CROSS_STAGE = "cross_stage"

_KEY_TO_STAGE_ID: dict[str, str] = {
    # shortlist
    "pipeline.vector_search_top_n": _WORKFLOW_STAGE_SHORTLIST,
    # ranking
    "pipeline.ai_score_top_n": _WORKFLOW_STAGE_RANKING,
    "pipeline.final_top_n": _WORKFLOW_STAGE_RANKING,
    "reuse.ranking.enabled": _WORKFLOW_STAGE_RANKING,
    "stage_runtime.ranking.sleep_secs": _WORKFLOW_STAGE_RANKING,
    "stage_runtime.ranking.concurrency": _WORKFLOW_STAGE_RANKING,
    "rerank_sleep_secs": _WORKFLOW_STAGE_RANKING,
    "ranking_weights.ai_score": _WORKFLOW_STAGE_RANKING,
    "ranking_weights.must_have_match": _WORKFLOW_STAGE_RANKING,
    "ranking_weights.vector_similarity": _WORKFLOW_STAGE_RANKING,
    "ranking_weights.title_relevance": _WORKFLOW_STAGE_RANKING,
    "ranking_weights.seniority_fit": _WORKFLOW_STAGE_RANKING,
    "ranking_weights.preference_fit": _WORKFLOW_STAGE_RANKING,
    "preference_fit_weights.domain": _WORKFLOW_STAGE_RANKING,
    "preference_fit_weights.role_family": _WORKFLOW_STAGE_RANKING,
    "preference_fit_weights.location_type": _WORKFLOW_STAGE_RANKING,
    "fit_label_thresholds.strong": _WORKFLOW_STAGE_RANKING,
    "fit_label_thresholds.stretch": _WORKFLOW_STAGE_RANKING,
    "gap_thresholds.strong_min_matched_ratio": _WORKFLOW_STAGE_RANKING,
    "gap_thresholds.stretch_min_matched_ratio": _WORKFLOW_STAGE_RANKING,
    # cv_analysis
    "pipeline.evidence_top_k": _WORKFLOW_STAGE_CV_ANALYSIS,
    "reuse.cv_analysis.enabled": _WORKFLOW_STAGE_CV_ANALYSIS,
    "stage_runtime.cv_analysis.sleep_secs": _WORKFLOW_STAGE_CV_ANALYSIS,
    "stage_runtime.cv_analysis.concurrency": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv.agentic_late_stage.enabled": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.enabled": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.model": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.required_skill_lexical_weight": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.required_skill_semantic_weight": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.role_lexical_weight": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.role_semantic_weight": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.responsibility_lexical_weight": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.responsibility_semantic_weight": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.domain_lexical_weight": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.domain_semantic_weight": _WORKFLOW_STAGE_CV_ANALYSIS,
    "cv_analysis.semantic_alignment.channel_pool_size": _WORKFLOW_STAGE_CV_ANALYSIS,
    # enrich
    "global_job_filters.applications_count_max": _WORKFLOW_STAGE_ENRICH,
    "global_job_filters.max_age_days": _WORKFLOW_STAGE_ENRICH,
    "stage_runtime.enrich.sleep_secs": _WORKFLOW_STAGE_ENRICH,
    "stage_runtime.enrich.batch_size": _WORKFLOW_STAGE_ENRICH,
    "stage_runtime.enrich.concurrency": _WORKFLOW_STAGE_ENRICH,
    "enrichment_sleep_secs": _WORKFLOW_STAGE_ENRICH,
    "enrichment_batch_size": _WORKFLOW_STAGE_ENRICH,
    "enrichment_concurrency": _WORKFLOW_STAGE_ENRICH,
    "synonym_management.propose_enabled": _WORKFLOW_STAGE_ENRICH,
    "synonym_management.apply_to_run_enabled": _WORKFLOW_STAGE_ENRICH,
    "synonym_management.promote_global_enabled": _WORKFLOW_STAGE_ENRICH,
    "synonym_management.auto_triage_recommendation_enabled": _WORKFLOW_STAGE_ENRICH,
    "synonym_management.triage_recommendation_reuse_enabled": _WORKFLOW_STAGE_ENRICH,
    "reuse.enrich.enabled": _WORKFLOW_STAGE_ENRICH,
    "reuse.synonym_triage.enabled": _WORKFLOW_STAGE_ENRICH,
    "synonym_management.auto_apply_recommendation_enabled": _WORKFLOW_STAGE_ENRICH,
    "synonym_management.auto_promote_global_enabled": _WORKFLOW_STAGE_ENRICH,
    # rule_filter
    "rule_filter.selected_filters": _WORKFLOW_STAGE_RULE_FILTER,
    # cv_generation
    "cv_generation_model": _WORKFLOW_STAGE_CV_GENERATION,
    "stage_runtime.cv_generation.sleep_secs": _WORKFLOW_STAGE_CV_GENERATION,
    "stage_runtime.cv_generation.concurrency": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_preset": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_summary_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_education_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_experience_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_skills_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_certifications_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_projects_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_publications_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_languages_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "cv_max_pages": _WORKFLOW_STAGE_CV_GENERATION,
    "reuse.cv_generation.enabled": _WORKFLOW_STAGE_CV_GENERATION,
    "synonym_management.auto_accept_ai_action_enabled": _WORKFLOW_STAGE_CV_GENERATION,
    # cross-stage runtime guardrail
    "run_lifecycle.max_runtime_minutes": _STAGE_CROSS_STAGE,
}

_CONTROL_SURFACE_STANDARD_PIPELINE = "standard_pipeline"
_CONTROL_SURFACE_AGENTIC_RUNTIME = "agentic_runtime"
_CONTROL_SURFACE_SHARED = "shared"

_GROUP_TO_CONTROL_SURFACE: dict[str, str] = {
    "retrieval": _CONTROL_SURFACE_STANDARD_PIPELINE,
    "global_job_filters": _CONTROL_SURFACE_STANDARD_PIPELINE,
    "rule_filter": _CONTROL_SURFACE_STANDARD_PIPELINE,
    "ranking": _CONTROL_SURFACE_STANDARD_PIPELINE,
    "cv_composition": _CONTROL_SURFACE_STANDARD_PIPELINE,
    "cv_validation": _CONTROL_SURFACE_STANDARD_PIPELINE,
    "cv_preset": _CONTROL_SURFACE_STANDARD_PIPELINE,
    "run_lifecycle": _CONTROL_SURFACE_SHARED,
    "timing": _CONTROL_SURFACE_SHARED,
    "agentic": _CONTROL_SURFACE_AGENTIC_RUNTIME,
}

_DECISION_AREA_ENABLEMENT = "enablement"
_DECISION_AREA_BEHAVIOR = "behavior"
_DECISION_AREA_QUALITY_TARGETS = "quality_targets"
_DECISION_AREA_THROUGHPUT = "throughput"
_DECISION_AREA_AUTOMATION = "automation"
_DECISION_AREA_SAFEGUARDS = "safeguards"
_DECISION_AREA_DIAGNOSTICS = "diagnostics"

_HIGH_RISK_GROUPS: frozenset[str] = frozenset({"ranking", "timing"})
_MEDIUM_RISK_GROUPS: frozenset[str] = frozenset(
    {"retrieval", "agentic", "run_lifecycle", "global_job_filters", "rule_filter"}
)
_DANGER_ZONE_GROUPS: frozenset[str] = frozenset({"timing", "run_lifecycle"})

def _risk_for_entry(entry: dict[str, Any]) -> str:
    group = str(entry.get("group") or "")
    key = str(entry.get("key") or "")
    entry_type = str(entry.get("type") or "")
    if key in _METADATA_ONLY_KEYS:
        return "low"
    if group in _HIGH_RISK_GROUPS:
        return "high"
    if group in _MEDIUM_RISK_GROUPS:
        return "medium"
    if entry_type == "float":
        return "high"
    return "low"

def _default_ia_domain(entry: dict[str, Any]) -> str:
    key = str(entry.get("key") or "")
    if key in _METADATA_ONLY_KEYS:
        return _IA_DOMAIN_LAYERS
    group = str(entry.get("group") or "")
    return _GROUP_TO_IA_DOMAIN.get(group, _IA_DOMAIN_ADVANCED)

def _default_stage_id(entry: dict[str, Any]) -> str:
    key = str(entry.get("key") or "")
    return _KEY_TO_STAGE_ID.get(key, _STAGE_CROSS_STAGE)


def _default_workflow_stages(entry: dict[str, Any], stage_id: str) -> list[str]:
    group = str(entry.get("group") or "")
    group_stages = list(_GROUP_TO_WORKFLOW_STAGES.get(group, ()))
    if not group_stages:
        return [stage_id]
    if stage_id not in group_stages:
        group_stages.append(stage_id)
    return group_stages

def _default_control_surface(entry: dict[str, Any]) -> str:
    key = str(entry.get("key") or "")
    if key.startswith("cv_analysis.semantic_alignment."):
        return _CONTROL_SURFACE_AGENTIC_RUNTIME
    group = str(entry.get("group") or "")
    return _GROUP_TO_CONTROL_SURFACE.get(group, _CONTROL_SURFACE_SHARED)

def _default_decision_area(entry: dict[str, Any]) -> str:
    key = str(entry.get("key") or "")
    group = str(entry.get("group") or "")
    if key in _METADATA_ONLY_KEYS:
        return _DECISION_AREA_DIAGNOSTICS
    if group == "agentic":
        if key.startswith("synonym_management.auto_") or key.endswith("_recommendation_enabled"):
            return _DECISION_AREA_AUTOMATION
        if key in {
            "cv.agentic_late_stage.enabled",
            "synonym_management.propose_enabled",
            "synonym_management.apply_to_run_enabled",
            "synonym_management.promote_global_enabled",
        }:
            return _DECISION_AREA_ENABLEMENT
    if key.startswith("cv_analysis.semantic_alignment."):
        if key.endswith("_weight"):
            return _DECISION_AREA_QUALITY_TARGETS
        if key.endswith("channel_pool_size"):
            return _DECISION_AREA_THROUGHPUT
        if key.endswith(".model") or key.endswith(".enabled"):
            return _DECISION_AREA_ENABLEMENT if key.endswith(".enabled") else _DECISION_AREA_DIAGNOSTICS
    if group == "retrieval":
        return _DECISION_AREA_THROUGHPUT
    if group == "ranking":
        return _DECISION_AREA_QUALITY_TARGETS
    if group in {"global_job_filters", "rule_filter", "run_lifecycle", "cv_validation"}:
        return _DECISION_AREA_SAFEGUARDS
    if group in {"cv_composition", "cv_preset"}:
        return _DECISION_AREA_BEHAVIOR
    if group == "timing":
        return _DECISION_AREA_THROUGHPUT
    return _DECISION_AREA_BEHAVIOR

def _build_settings_ia_metadata() -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for entry in SETTINGS_SCHEMA:
        key = str(entry["key"])
        group = str(entry.get("group") or "")
        risk = _risk_for_entry(entry)
        stage_id = _default_stage_id(entry)
        metadata[key] = {
            "domain": _default_ia_domain(entry),
            "stage": stage_id,
            "control_surface": _default_control_surface(entry),
            "decision_area": _default_decision_area(entry),
            # Keep per-key canonical stage and workflow participation explicitly separated.
            "workflow_stages": _default_workflow_stages(entry, stage_id),
            "risk": risk,
            "runtime_used": key not in _METADATA_ONLY_KEYS,
            "metadata_only": key in _METADATA_ONLY_KEYS,
            "override_policy": "hidden_until_enabled" if key not in _METADATA_ONLY_KEYS else "disabled",
            "can_override": key not in _METADATA_ONLY_KEYS,
            "is_dangerous": risk == "high" or group in _DANGER_ZONE_GROUPS,
            "advanced": _default_ia_domain(entry) == _IA_DOMAIN_ADVANCED,
            "complexity_view": COMPLEXITY_VIEW_ADVANCED if _default_ia_domain(entry) == _IA_DOMAIN_ADVANCED else COMPLEXITY_VIEW_BASIC,
            "unused": False,
            "recommended_delta": False,
            "decision_status": DECISION_STATUS_CONFIGURED,
            "reason_codes": [],
            "applies_when": _GROUP_TO_APPLIES_WHEN.get(
                group,
                "Used in advanced runtime flow according to this setting group.",
            ),
        }
    return metadata

SETTINGS_IA_METADATA_BY_KEY: dict[str, dict[str, Any]] = _build_settings_ia_metadata()

def _validate_settings_ia_metadata_coverage() -> None:
    schema_keys = {entry["key"] for entry in SETTINGS_SCHEMA}
    metadata_keys = set(SETTINGS_IA_METADATA_BY_KEY.keys())
    missing = schema_keys - metadata_keys
    extra = metadata_keys - schema_keys
    if missing:
        raise RuntimeError(f"SETTINGS_IA_METADATA_BY_KEY missing keys: {sorted(missing)!r}")
    if extra:
        raise RuntimeError(f"SETTINGS_IA_METADATA_BY_KEY has unknown keys: {sorted(extra)!r}")

def _validate_settings_surface_contract() -> None:
    overlap = (_EDITABLE_KEYS & _HIDDEN_DEPRECATED_KEYS) - _EDITABLE_HIDDEN_DEPRECATED_ALLOWLIST
    if overlap:
        raise RuntimeError(
            f"Editable hidden-deprecated overlap keys must be explicitly allowlisted: {sorted(overlap)!r}"
        )

_validate_settings_ia_metadata_coverage()
_validate_settings_surface_contract()

def settings_ia_metadata_by_key() -> dict[str, dict[str, Any]]:
    return {key: dict(value) for key, value in SETTINGS_IA_METADATA_BY_KEY.items()}

def settings_keys_for_intent_layer(layer: str) -> list[str]:
    return settings_keys_for_domain(layer)

def settings_keys_for_domain(domain: str) -> list[str]:
    return sorted(
        key
        for key, meta in SETTINGS_IA_METADATA_BY_KEY.items()
        if str(meta.get("domain")) == domain
    )

def settings_keys_for_workflow_stage(stage: str) -> list[str]:
    return sorted(
        key
        for key, meta in SETTINGS_IA_METADATA_BY_KEY.items()
        if stage in list(meta.get("workflow_stages") or [])
    )

def settings_keys_for_stage(stage: str) -> list[str]:
    return sorted(
        key
        for key, meta in SETTINGS_IA_METADATA_BY_KEY.items()
        if str(meta.get("stage") or "") == stage
    )

def settings_keys_for_control_surface(control_surface: str) -> list[str]:
    return sorted(
        key
        for key, meta in SETTINGS_IA_METADATA_BY_KEY.items()
        if str(meta.get("control_surface") or "") == control_surface
    )

def settings_ia_contract_for_key(key: str) -> dict[str, Any]:
    if key not in SETTINGS_IA_METADATA_BY_KEY:
        raise KeyError(key)
    return dict(SETTINGS_IA_METADATA_BY_KEY[key])

def decision_status_sort_key(status: str) -> int:
    return _DECISION_STATUS_PRIORITY.get(str(status), _DECISION_STATUS_PRIORITY[DECISION_STATUS_ALL])

def reason_code_is_blocking(reason_code: str) -> bool:
    return str(reason_code) in _BLOCKING_REASON_CODES

def derive_settings_decision_state(
    *,
    is_advanced: bool,
    is_unused: bool,
    is_changed_from_default: bool,
    has_recommended_delta: bool,
    has_conflict: bool = False,
    has_missing_required: bool = False,
    has_low_confidence: bool = False,
    has_quality_risk: bool = False,
) -> dict[str, Any]:
    reason_codes: list[str] = []
    if has_missing_required:
        reason_codes.append(REASON_CODE_MISSING_REQUIRED)
    if has_conflict:
        reason_codes.append(REASON_CODE_CONFLICT)
    if has_low_confidence:
        reason_codes.append(REASON_CODE_LOW_CONFIDENCE)
    if has_quality_risk:
        reason_codes.append(REASON_CODE_QUALITY_RISK)
    if is_changed_from_default:
        reason_codes.append(REASON_CODE_CHANGED_FROM_DEFAULT)
    if has_recommended_delta:
        reason_codes.append(REASON_CODE_RECOMMENDED_DELTA)
    if is_advanced:
        reason_codes.append(REASON_CODE_ADVANCED_ONLY)
    if is_unused:
        reason_codes.append(REASON_CODE_UNUSED)

    if any(code in _BLOCKING_REASON_CODES for code in reason_codes):
        status = DECISION_STATUS_NEEDS_REVIEW
    elif has_recommended_delta:
        status = DECISION_STATUS_RECOMMENDED
    elif is_advanced:
        status = DECISION_STATUS_ADVANCED
    else:
        status = DECISION_STATUS_CONFIGURED

    return {
        "decision_status": status,
        "reason_codes": reason_codes,
        "advanced": bool(is_advanced),
        "unused": bool(is_unused),
        "recommended_delta": bool(has_recommended_delta),
        "is_blocking": status == DECISION_STATUS_NEEDS_REVIEW,
    }

def danger_zone_settings_keys() -> list[str]:
    return sorted(
        key
        for key, meta in SETTINGS_IA_METADATA_BY_KEY.items()
        if bool(meta.get("is_dangerous"))
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


def hidden_deprecated_settings_keys() -> set[str]:
    return set(_HIDDEN_DEPRECATED_KEYS)


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
    normalized = _normalize_settings_aliases(settings)
    for key, value in normalized.items():
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
    for lhs_key, rhs_key, message_template in _RELATIONAL_ORDER_CONSTRAINTS:
        lhs = normalized.get(lhs_key)
        rhs = normalized.get(rhs_key)
        if isinstance(lhs, (int, float)) and isinstance(rhs, (int, float)) and rhs > lhs:
            raise ValidationError(message_template.format(lhs=lhs, rhs=rhs))

    # Weight-family sum-to-1 checks only run when each full family is present.
    for keys, label in _WEIGHT_SUM_CONSTRAINTS:
        if keys <= set(normalized.keys()):
            total = sum(float(normalized[key]) for key in keys)
            if abs(total - 1.0) > 0.01:
                raise ValidationError(
                    f"{label} must sum to 1.0 (± 0.01), got {total:.4f}"
                )

# ── config application ────────────────────────────────────────────────────────

def apply_settings_to_config(config: dict[str, Any], settings: dict[str, Any]) -> None:
    """Write settings values into a config dict in-place.

    Uses config_path from the schema registry to navigate nested dicts.
    settings values must already be coerced to their declared Python types.
    """
    normalized = _normalize_settings_aliases(settings)
    for key, value in normalized.items():
        path = _ALL_SCHEMA_BY_KEY[key]["config_path"]
        target = config
        for part in path[:-1]:
            target = target.setdefault(part, {})
        target[path[-1]] = value

_LEGACY_THROUGHPUT_ALIAS_TO_CANONICAL: dict[str, str] = {
    "enrichment_sleep_secs": "stage_runtime.enrich.sleep_secs",
    "rerank_sleep_secs": "stage_runtime.ranking.sleep_secs",
    "enrichment_batch_size": "stage_runtime.enrich.batch_size",
    "enrichment_concurrency": "stage_runtime.enrich.concurrency",
}

def _normalize_settings_aliases(settings: dict[str, Any]) -> dict[str, Any]:
    """Apply canonical-over-legacy precedence for throughput compatibility aliases."""
    normalized = dict(settings)
    for legacy_key, canonical_key in _LEGACY_THROUGHPUT_ALIAS_TO_CANONICAL.items():
        if canonical_key in normalized:
            continue
        if legacy_key in normalized:
            normalized[canonical_key] = normalized[legacy_key]
    return normalized



