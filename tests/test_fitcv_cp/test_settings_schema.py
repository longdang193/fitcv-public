import pytest
from fitcv_cp.settings_schema import (
    SETTINGS_SCHEMA,
    apply_settings_to_config,
    validate_settings,
    ValidationError,
)


# ── schema registry ───────────────────────────────────────────────────────────

def test_all_expected_keys_present():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "pipeline.final_top_n" in keys
    assert "cv_analysis.semantic_alignment.enabled" in keys
    assert "cv_analysis.semantic_alignment.required_skill_lexical_weight" in keys
    assert "cv_analysis.semantic_alignment.role_semantic_weight" in keys
    assert "cv_analysis.semantic_alignment.responsibility_lexical_weight" in keys
    assert "cv_analysis.semantic_alignment.domain_semantic_weight" in keys
    assert "run_lifecycle.max_runtime_minutes" in keys
    assert "ranking_weights.ai_score" in keys
    assert "fit_label_thresholds.strong" in keys
    assert "gap_thresholds.strong_min_matched_ratio" in keys
    # excluded key — internal fallback only, not admin-editable
    assert "rerank_top_n" not in keys


def test_schema_has_required_fields():
    for entry in SETTINGS_SCHEMA:
        assert "key" in entry
        assert "type" in entry       # "int" or "float"
        assert "default" in entry
        assert "label" in entry
        assert "description" in entry
        assert "group" in entry      # "retrieval" | "timing" | "ranking"


# ── type coercion ─────────────────────────────────────────────────────────────

def test_coerce_int_from_string():
    from fitcv_cp.settings_schema import coerce_value
    assert coerce_value("pipeline.final_top_n", "5") == 5
    assert isinstance(coerce_value("pipeline.final_top_n", "5"), int)


def test_coerce_float_from_string():
    from fitcv_cp.settings_schema import coerce_value
    assert coerce_value("ranking_weights.ai_score", "0.5") == 0.5


def test_coerce_rejects_unknown_key():
    from fitcv_cp.settings_schema import coerce_value
    with pytest.raises(KeyError):
        coerce_value("unknown.key", "1")


# ── per-field validation ──────────────────────────────────────────────────────

def test_int_top_n_must_be_positive():
    with pytest.raises(ValidationError, match="pipeline.final_top_n"):
        validate_settings({"pipeline.final_top_n": 0})


def test_float_threshold_must_be_in_range():
    with pytest.raises(ValidationError, match="fit_label_thresholds.strong"):
        validate_settings({"fit_label_thresholds.strong": 1.5})


def test_sleep_secs_may_be_zero():
    validate_settings({"enrichment_sleep_secs": 0.0})  # should not raise


# ── relational validation ─────────────────────────────────────────────────────

def test_top_n_relational_constraint():
    """final_top_n <= ai_score_top_n <= vector_search_top_n"""
    with pytest.raises(ValidationError, match="final_top_n"):
        validate_settings({
            "pipeline.vector_search_top_n": 50,
            "pipeline.ai_score_top_n": 50,
            "pipeline.final_top_n": 60,   # violates: 60 > 50
        })


def test_fit_label_strong_must_exceed_stretch():
    with pytest.raises(ValidationError, match="fit_label_thresholds"):
        validate_settings({
            "fit_label_thresholds.strong": 0.40,
            "fit_label_thresholds.stretch": 0.70,   # violates: stretch > strong
        })


def test_ranking_weights_must_sum_to_one():
    with pytest.raises(ValidationError, match="ranking_weights"):
        validate_settings({
            "ranking_weights.ai_score": 0.90,
            "ranking_weights.must_have_match": 0.20,
            "ranking_weights.vector_similarity": 0.15,
            "ranking_weights.title_relevance": 0.10,
            "ranking_weights.seniority_fit": 0.10,
            "ranking_weights.preference_fit": 0.05,
        })


def test_ranking_weights_partial_update_skips_sum_check():
    """Partial updates are allowed; sum-to-1 only checked when ALL 6 are present."""
    validate_settings({"ranking_weights.ai_score": 0.50})  # should not raise


def test_gap_thresholds_strong_must_exceed_stretch():
    with pytest.raises(ValidationError, match="gap_thresholds"):
        validate_settings({
            "gap_thresholds.strong_min_matched_ratio": 0.30,
            "gap_thresholds.stretch_min_matched_ratio": 0.50,
        })


def test_unknown_key_rejected():
    with pytest.raises(ValidationError, match="unknown"):
        validate_settings({"unknown.key": 1})


# ── config application ────────────────────────────────────────────────────────

def test_apply_settings_to_config_nested():
    config = {"pipeline": {"final_top_n": 10}, "ranking_weights": {"ai_score": 0.40}}
    apply_settings_to_config(config, {"pipeline.final_top_n": 5, "ranking_weights.ai_score": 0.50})
    assert config["pipeline"]["final_top_n"] == 5
    assert config["ranking_weights"]["ai_score"] == 0.50


def test_apply_settings_to_config_flat_key():
    config = {"enrichment_sleep_secs": 1.0}
    apply_settings_to_config(config, {"enrichment_sleep_secs": 0.5})
    assert config["enrichment_sleep_secs"] == 0.5


# ── global_job_filters settings ───────────────────────────────────────────────

def test_global_job_filters_keys_registered():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "global_job_filters.applications_count_max" in keys
    assert "global_job_filters.max_age_days" in keys


def test_global_job_filters_group_name():
    for entry in SETTINGS_SCHEMA:
        if entry["key"].startswith("global_job_filters."):
            assert entry["group"] == "global_job_filters"


def test_global_job_filters_apply_settings_to_config_writes_correct_path():
    config: dict = {}
    apply_settings_to_config(config, {
        "global_job_filters.applications_count_max": 150,
        "global_job_filters.max_age_days": 14,
    })
    assert config["global_job_filters"]["applications_count_max"] == 150
    assert config["global_job_filters"]["max_age_days"] == 14


def test_global_job_filters_validate_rejects_zero():
    with pytest.raises(ValidationError):
        validate_settings({"global_job_filters.applications_count_max": 0})


def test_global_job_filters_validate_rejects_negative():
    with pytest.raises(ValidationError):
        validate_settings({"global_job_filters.max_age_days": -1})


def test_global_job_filters_validate_accepts_positive():
    validate_settings({
        "global_job_filters.applications_count_max": 200,
        "global_job_filters.max_age_days": 30,
    })  # must not raise


def test_run_lifecycle_max_runtime_minutes_validate_accepts_positive() -> None:
    validate_settings({"run_lifecycle.max_runtime_minutes": 240})


def test_run_lifecycle_max_runtime_minutes_validate_rejects_zero() -> None:
    with pytest.raises(ValidationError):
        validate_settings({"run_lifecycle.max_runtime_minutes": 0})


def test_apply_settings_to_config_run_lifecycle_writes_nested_path() -> None:
    config: dict = {}
    apply_settings_to_config(config, {"run_lifecycle.max_runtime_minutes": 180})
    assert config["run_lifecycle"]["max_runtime_minutes"] == 180


# ── rule_filter.selected_filters settings ────────────────────────────────────

def test_rule_filter_selected_filters_key_registered() -> None:
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "rule_filter.selected_filters" in keys


def test_rule_filter_selected_filters_uses_list_str_type() -> None:
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["rule_filter.selected_filters"]["type"] == "list[str]"


def test_rule_filter_selected_filters_default_matches_spec() -> None:
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["rule_filter.selected_filters"]["default"] == [
        "seniority_mismatch",
        "location_type_excluded",
        "contract_type_excluded",
        "experience_level_excluded",
    ]


def test_retrieval_defaults_are_hydrated_from_centralized_pipeline_config() -> None:
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["pipeline.vector_search_top_n"]["default"] == 50
    assert schema_by_key["pipeline.ai_score_top_n"]["default"] == 50
    assert schema_by_key["pipeline.final_top_n"]["default"] == 10
    assert schema_by_key["pipeline.evidence_top_k"]["default"] == 5


def test_rule_filter_selected_filters_validate_accepts_known_codes() -> None:
    validate_settings({
        "rule_filter.selected_filters": [
            "seniority_mismatch",
            "must_have_skill_missing",
        ]
    })


def test_rule_filter_selected_filters_validate_rejects_duplicates() -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        validate_settings({
            "rule_filter.selected_filters": [
                "seniority_mismatch",
                "seniority_mismatch",
            ]
        })


def test_rule_filter_selected_filters_validate_rejects_unknown_code() -> None:
    with pytest.raises(ValidationError, match="must be one of"):
        validate_settings({
            "rule_filter.selected_filters": [
                "seniority_mismatch",
                "not_a_real_filter",
            ]
        })


def test_apply_settings_to_config_rule_filter_selected_filters_nested() -> None:
    config: dict = {}
    apply_settings_to_config(config, {
        "rule_filter.selected_filters": [
            "seniority_mismatch",
            "domain_not_preferred",
        ]
    })


def test_cv_analysis_semantic_alignment_validate_accepts_balanced_weight_pairs() -> None:
    validate_settings({
        "cv_analysis.semantic_alignment.required_skill_lexical_weight": 0.70,
        "cv_analysis.semantic_alignment.required_skill_semantic_weight": 0.30,
        "cv_analysis.semantic_alignment.role_lexical_weight": 0.60,
        "cv_analysis.semantic_alignment.role_semantic_weight": 0.40,
        "cv_analysis.semantic_alignment.responsibility_lexical_weight": 0.25,
        "cv_analysis.semantic_alignment.responsibility_semantic_weight": 0.75,
        "cv_analysis.semantic_alignment.domain_lexical_weight": 0.40,
        "cv_analysis.semantic_alignment.domain_semantic_weight": 0.60,
    })


def test_cv_analysis_semantic_alignment_validate_rejects_unbalanced_required_skill_weights() -> None:
    with pytest.raises(ValidationError, match="required-skill"):
        validate_settings({
            "cv_analysis.semantic_alignment.required_skill_lexical_weight": 0.50,
            "cv_analysis.semantic_alignment.required_skill_semantic_weight": 0.20,
        })


def test_cv_analysis_semantic_alignment_validate_rejects_unbalanced_role_weights() -> None:
    with pytest.raises(ValidationError, match="role"):
        validate_settings({
            "cv_analysis.semantic_alignment.role_lexical_weight": 0.20,
            "cv_analysis.semantic_alignment.role_semantic_weight": 0.20,
        })


def test_cv_analysis_semantic_alignment_validate_rejects_unbalanced_responsibility_weights() -> None:
    with pytest.raises(ValidationError, match="responsibility"):
        validate_settings({
            "cv_analysis.semantic_alignment.responsibility_lexical_weight": 0.20,
            "cv_analysis.semantic_alignment.responsibility_semantic_weight": 0.50,
        })


def test_cv_analysis_semantic_alignment_validate_rejects_unbalanced_domain_weights() -> None:
    with pytest.raises(ValidationError, match="domain"):
        validate_settings({
            "cv_analysis.semantic_alignment.domain_lexical_weight": 0.30,
            "cv_analysis.semantic_alignment.domain_semantic_weight": 0.30,
        })


# ── RANKING_GROUPS registry ───────────────────────────────────────────────────

def test_ranking_groups_has_four_slugs():
    from fitcv_cp.settings_schema import RANKING_GROUPS
    assert set(RANKING_GROUPS.keys()) == {
        "ranking-weights",
        "preference-fit-weights",
        "fit-label-thresholds",
        "gap-thresholds",
    }


def test_ranking_groups_all_keys_in_schema():
    from fitcv_cp.settings_schema import RANKING_GROUPS
    schema_keys = {s["key"] for s in SETTINGS_SCHEMA}
    for slug, keys in RANKING_GROUPS.items():
        for key in keys:
            assert key in schema_keys, f"{key!r} from group {slug!r} not found in SETTINGS_SCHEMA"


def test_ranking_weights_group_has_six_keys():
    from fitcv_cp.settings_schema import RANKING_GROUPS
    assert len(RANKING_GROUPS["ranking-weights"]) == 6


def test_preference_fit_weights_group_has_three_keys() -> None:
    from fitcv_cp.settings_schema import RANKING_GROUPS
    assert len(RANKING_GROUPS["preference-fit-weights"]) == 3


def test_ranking_weight_copy_matches_runtime_semantics():
    schema_by_key = {entry["key"]: entry for entry in SETTINGS_SCHEMA}
    assert schema_by_key["ranking_weights.title_relevance"]["description"] == (
        "How much influence semantic role alignment between the job title and the candidate's target role has on the final ranking."
    )
    assert schema_by_key["ranking_weights.preference_fit"]["label"] == "Weight: Preference Alignment"
    assert schema_by_key["ranking_weights.preference_fit"]["description"] == (
        "How much influence weighted candidate preference alignment across domain, role family, and location type has on the final candidate ranking."
    )


def test_preference_fit_weight_keys_registered() -> None:
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "preference_fit_weights.domain" in keys
    assert "preference_fit_weights.role_family" in keys
    assert "preference_fit_weights.location_type" in keys


def test_preference_fit_weight_copy_matches_runtime_semantics() -> None:
    schema_by_key = {entry["key"]: entry for entry in SETTINGS_SCHEMA}
    assert schema_by_key["preference_fit_weights.domain"]["description"] == (
        "Relative importance of explicit domain preference alignment within the preference-fit feature."
    )
    assert schema_by_key["preference_fit_weights.role_family"]["description"] == (
        "Relative importance of explicit role-family preference alignment within the preference-fit feature."
    )
    assert schema_by_key["preference_fit_weights.location_type"]["description"] == (
        "Relative importance of explicit location-type preference alignment within the preference-fit feature."
    )


def test_preference_fit_weights_must_sum_to_one() -> None:
    with pytest.raises(ValidationError, match="preference_fit_weights"):
        validate_settings({
            "preference_fit_weights.domain": 0.70,
            "preference_fit_weights.role_family": 0.20,
            "preference_fit_weights.location_type": 0.20,
        })


def test_ranking_groups_threshold_groups_have_two_keys_each():
    from fitcv_cp.settings_schema import RANKING_GROUPS
    assert len(RANKING_GROUPS["fit-label-thresholds"]) == 2
    assert len(RANKING_GROUPS["gap-thresholds"]) == 2


# ── SETTINGS_SECTIONS registry ────────────────────────────────────────────────

def test_settings_sections_has_expected_slugs():
    from fitcv_cp.settings_schema import SETTINGS_SECTIONS
    assert set(SETTINGS_SECTIONS.keys()) == {
        "retrieval",
        "timing",
        "run-lifecycle",
        "global-job-filters",
        "rule-filter",
    }


def test_settings_sections_all_keys_in_schema():
    from fitcv_cp.settings_schema import SETTINGS_SECTIONS, SETTINGS_SCHEMA
    schema_keys = {s["key"] for s in SETTINGS_SCHEMA}
    for slug, keys in SETTINGS_SECTIONS.items():
        for key in keys:
            assert key in schema_keys, (
                f"{key!r} from SETTINGS_SECTIONS[{slug!r}] not found in SETTINGS_SCHEMA"
            )


def test_settings_sections_no_key_appears_twice():
    from fitcv_cp.settings_schema import SETTINGS_SECTIONS
    seen: set[str] = set()
    for slug, keys in SETTINGS_SECTIONS.items():
        for key in keys:
            assert key not in seen, f"{key!r} appears in multiple sections"
            seen.add(key)


def test_settings_sections_retrieval_has_semantic_alignment_keys():
    from fitcv_cp.settings_schema import SETTINGS_SECTIONS
    assert "pipeline.evidence_top_k" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.enabled" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.model" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.required_skill_lexical_weight" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.required_skill_semantic_weight" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.role_lexical_weight" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.role_semantic_weight" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.responsibility_lexical_weight" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.responsibility_semantic_weight" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.domain_lexical_weight" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.domain_semantic_weight" in SETTINGS_SECTIONS["retrieval"]
    assert "cv_analysis.semantic_alignment.channel_pool_size" in SETTINGS_SECTIONS["retrieval"]


def test_settings_sections_global_job_filters_has_two_keys():
    from fitcv_cp.settings_schema import SETTINGS_SECTIONS
    assert len(SETTINGS_SECTIONS["global-job-filters"]) == 2


# ── enrichment parallelism settings ───────────────────────────────────────────

def test_enrichment_parallelism_keys_registered():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "enrichment_batch_size" in keys
    assert "enrichment_concurrency" in keys


def test_enrichment_parallelism_defaults():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["enrichment_batch_size"]["default"] == 10
    assert schema_by_key["enrichment_concurrency"]["default"] == 1


def test_enrichment_parallelism_group_is_timing():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["enrichment_batch_size"]["group"] == "timing"
    assert schema_by_key["enrichment_concurrency"]["group"] == "timing"


def test_enrichment_batch_size_validate_rejects_zero():
    with pytest.raises(ValidationError):
        validate_settings({"enrichment_batch_size": 0})


def test_enrichment_batch_size_validate_rejects_negative():
    with pytest.raises(ValidationError):
        validate_settings({"enrichment_batch_size": -5})


def test_enrichment_concurrency_validate_rejects_zero():
    with pytest.raises(ValidationError):
        validate_settings({"enrichment_concurrency": 0})


def test_enrichment_concurrency_validate_accepts_one():
    validate_settings({"enrichment_concurrency": 1})  # must not raise


def test_enrichment_batch_size_apply_writes_correct_path():
    config: dict = {}
    apply_settings_to_config(config, {"enrichment_batch_size": 5})
    assert config["enrichment_batch_size"] == 5


def test_enrichment_concurrency_apply_writes_correct_path():
    config: dict = {}
    apply_settings_to_config(config, {"enrichment_concurrency": 3})
    assert config["enrichment_concurrency"] == 3


def test_enrichment_parallelism_in_settings_sections_timing():
    from fitcv_cp.settings_schema import SETTINGS_SECTIONS
    assert "enrichment_batch_size" in SETTINGS_SECTIONS["timing"]
    assert "enrichment_concurrency" in SETTINGS_SECTIONS["timing"]


# ── CV settings schema ────────────────────────────────────────────────────────

def test_cv_settings_keys_registered():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "cv_preset" in keys
    assert "cv_generation_model" in keys
    assert "cv_max_pages" in keys
    assert "cv_template_path" not in keys
    assert "cv_prompt_version" not in keys


def test_cv_settings_have_correct_group():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    # preset and composition fields are in new groups
    assert schema_by_key["cv_preset"]["group"] == "cv_preset"
    assert schema_by_key["cv_generation_model"]["group"] == "cv_composition"
    assert schema_by_key["cv_summary_enabled"]["group"] == "cv_composition"
    assert schema_by_key["cv_max_pages"]["group"] == "cv_validation"


def test_cv_settings_defaults():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_preset"]["default"] == "europass"
    assert schema_by_key["cv_generation_model"]["default"] == "gemini-2.5-flash"
    assert schema_by_key["cv_max_pages"]["default"] == 2


def test_cv_settings_types():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_preset"]["type"] == "str"
    assert schema_by_key["cv_generation_model"]["type"] == "str"
    assert schema_by_key["cv_max_pages"]["type"] == "int"
    assert schema_by_key["cv_summary_enabled"]["type"] == "bool"
    assert schema_by_key["cv_education_enabled"]["type"] == "bool"
    assert schema_by_key["cv_experience_enabled"]["type"] == "bool"
    assert schema_by_key["cv_skills_enabled"]["type"] == "bool"
    assert schema_by_key["cv_certifications_enabled"]["type"] == "bool"
    assert schema_by_key["cv_projects_enabled"]["type"] == "bool"
    assert schema_by_key["cv_publications_enabled"]["type"] == "bool"
    assert schema_by_key["cv_languages_enabled"]["type"] == "bool"
    assert "cv_emphasize_required_skills" not in schema_by_key
    assert "cv_align_jd_terminology" not in schema_by_key
    assert "cv_evidence_grounded_only" not in schema_by_key


def test_pipeline_evidence_top_k_not_in_cv_group():
    """evidence_top_k stays in retrieval, not in the CV section."""
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["pipeline.evidence_top_k"]["group"] != "cv_generation"


def test_cv_generation_keys_in_cv_groups():
    from fitcv_cp.settings_schema import CV_GROUPS
    assert "cv_preset" in CV_GROUPS["cv-preset"]
    assert "cv_generation_model" in CV_GROUPS["cv-preset"]


def test_cv_groups_all_keys_in_schema():
    from fitcv_cp.settings_schema import CV_GROUPS
    schema_keys = {s["key"] for s in SETTINGS_SCHEMA}
    for slug, keys in CV_GROUPS.items():
        for key in keys:
            assert key in schema_keys, f"{key!r} from CV_GROUPS[{slug!r}] not found in SETTINGS_SCHEMA"


def test_cv_groups_no_key_appears_twice():
    from fitcv_cp.settings_schema import CV_GROUPS
    seen: set[str] = set()
    for slug, keys in CV_GROUPS.items():
        for key in keys:
            assert key not in seen, f"{key!r} appears in multiple CV groups"
            seen.add(key)


# ── Preset-based CV settings schema ─────────────────────────────────────────────

def test_cv_preset_key_registered():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "cv_preset" in keys


def test_cv_preset_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_preset"]["default"] == "europass"


def test_cv_preset_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_preset"]["type"] == "str"


def test_cv_preset_group():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_preset"]["group"] == "cv_preset"


# ── Generation fields ─────────────────────────────────────────────────────────────

def test_cv_generation_fields_registered():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "cv_generation_model" in keys
    assert "cv_prompt_version" not in keys


def test_cv_generation_model_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_generation_model"]["type"] == "str"


def test_cv_generation_model_group():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_generation_model"]["group"] == "cv_composition"


# ── Composition fields ───────────────────────────────────────────────────────────

def test_cv_composition_fields_registered():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "cv_summary_enabled" in keys
    assert "cv_education_enabled" in keys
    assert "cv_experience_enabled" in keys
    assert "cv_skills_enabled" in keys
    assert "cv_certifications_enabled" in keys
    assert "cv_projects_enabled" in keys
    assert "cv_publications_enabled" in keys
    assert "cv_languages_enabled" in keys
    assert "cv_summary_style" not in keys
    assert "cv_education_detail" not in keys
    assert "cv_experience_bullet_style" not in keys
    assert "cv_skills_max_items" not in keys
    assert "cv_publications_detail" not in keys
    assert "cv_languages_detail" not in keys
    assert "cv_education_required" not in keys
    assert "cv_projects_required" not in keys


def test_cv_education_enabled_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_education_enabled"]["type"] == "bool"

def test_cv_experience_enabled_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_experience_enabled"]["type"] == "bool"

def test_cv_skills_enabled_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_skills_enabled"]["type"] == "bool"


def test_cv_certifications_enabled_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_certifications_enabled"]["type"] == "bool"


def test_cv_projects_enabled_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_projects_enabled"]["type"] == "bool"


def test_cv_publications_enabled_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_publications_enabled"]["type"] == "bool"


def test_cv_languages_enabled_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_languages_enabled"]["type"] == "bool"


def test_cv_composition_fields_have_correct_group():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    for key in (
        "cv_summary_enabled",
        "cv_education_enabled",
        "cv_experience_enabled",
        "cv_skills_enabled",
        "cv_certifications_enabled",
        "cv_projects_enabled",
        "cv_publications_enabled",
        "cv_languages_enabled",
    ):
        assert schema_by_key[key]["group"] == "cv_composition", f"{key} should be in cv_composition group"


def test_cv_composition_retired_formatting_fields_removed():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    for key in (
        "cv_summary_style",
        "cv_education_detail",
        "cv_experience_bullet_style",
        "cv_skills_max_items",
        "cv_publications_detail",
        "cv_languages_detail",
    ):
        assert key not in schema_by_key


# ── Content rules fields ────────────────────────────────────────────────────────

def test_cv_content_rules_fields_removed():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "cv_emphasize_required_skills" not in keys
    assert "cv_align_jd_terminology" not in keys
    assert "cv_evidence_grounded_only" not in keys


def test_cv_content_rules_group_removed():
    from fitcv_cp.settings_schema import CV_GROUPS
    assert "cv-content-rules" not in CV_GROUPS


# ── Validation fields ────────────────────────────────────────────────────────────

def test_cv_validation_fields_registered():
    keys = {s["key"] for s in SETTINGS_SCHEMA}
    assert "cv_max_pages" in keys


def test_cv_max_pages_type():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_max_pages"]["type"] == "int"


def test_cv_max_pages_group():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_max_pages"]["group"] == "cv_validation"


# ── CV group registries ─────────────────────────────────────────────────────────

def test_cv_groups_has_expected_subgroups():
    from fitcv_cp.settings_schema import CV_GROUPS
    assert "cv-preset" in CV_GROUPS
    assert "cv-composition" in CV_GROUPS
    assert "cv-validation" in CV_GROUPS


def test_cv_groups_preset_has_correct_keys():
    from fitcv_cp.settings_schema import CV_GROUPS
    assert "cv_preset" in CV_GROUPS["cv-preset"]
    assert "cv_generation_model" in CV_GROUPS["cv-preset"]


def test_cv_groups_composition_has_all_composition_keys():
    from fitcv_cp.settings_schema import CV_GROUPS
    expected = {
        "cv_summary_enabled",
        "cv_education_enabled",
        "cv_experience_enabled",
        "cv_skills_enabled",
        "cv_certifications_enabled", "cv_projects_enabled",
        "cv_publications_enabled",
        "cv_languages_enabled",
    }
    assert set(CV_GROUPS["cv-composition"]) == expected


def test_cv_groups_validation_has_cv_max_pages():
    from fitcv_cp.settings_schema import CV_GROUPS
    assert "cv_max_pages" in CV_GROUPS["cv-validation"]


def test_cv_groups_no_key_appears_twice():
    from fitcv_cp.settings_schema import CV_GROUPS
    seen: set[str] = set()
    for slug, keys in CV_GROUPS.items():
        for key in keys:
            assert key not in seen, f"{key!r} appears in multiple CV groups"
            seen.add(key)


def test_cv_groups_all_keys_in_schema():
    from fitcv_cp.settings_schema import CV_GROUPS
    schema_keys = {s["key"] for s in SETTINGS_SCHEMA}
    for slug, keys in CV_GROUPS.items():
        for key in keys:
            assert key in schema_keys, f"{key!r} from CV_GROUPS[{slug!r}] not found in SETTINGS_SCHEMA"


# ── coerce_value for new CV types ────────────────────────────────────────────────

def test_coerce_bool_from_string():
    from fitcv_cp.settings_schema import coerce_value
    result = coerce_value("cv_education_enabled", "true")
    assert result is True
    assert isinstance(result, bool)


def test_coerce_bool_from_string_false():
    from fitcv_cp.settings_schema import coerce_value
    result = coerce_value("cv_education_enabled", "false")
    assert result is False
    assert isinstance(result, bool)


# ── validate_settings for new CV fields ─────────────────────────────────────────

def test_cv_preset_rejects_empty():
    with pytest.raises(ValidationError, match="cv_preset"):
        validate_settings({"cv_preset": ""})


def test_cv_preset_rejects_whitespace_only():
    with pytest.raises(ValidationError, match="cv_preset"):
        validate_settings({"cv_preset": "   "})


# ── apply_settings_to_config for new CV fields ──────────────────────────────────

def test_apply_settings_to_config_cv_preset():
    config: dict = {}
    apply_settings_to_config(config, {"cv_preset": "europass"})
    assert config["cv"]["preset"] == "europass"


def test_apply_settings_to_config_cv_composition_nested():
    config: dict = {}
    apply_settings_to_config(config, {
        "cv_summary_enabled": False,
        "cv_education_enabled": True,
        "cv_skills_enabled": True,
    })
    assert config["cv"]["composition"]["summary"]["enabled"] is False
    assert config["cv"]["composition"]["education"]["enabled"] is True
    assert config["cv"]["composition"]["skills"]["enabled"] is True


def test_apply_settings_to_config_cv_validation_nested():
    config: dict = {}
    apply_settings_to_config(config, {"cv_max_pages": 3})
    assert config["cv"]["validation"]["max_pages"] == 3


def test_apply_settings_to_config_cv_generation_nested():
    config: dict = {}
    apply_settings_to_config(config, {
        "cv_generation_model": "gemini-2.5-flash",
    })
    assert config["cv"]["generation"]["model"] == "gemini-2.5-flash"


def test_apply_settings_to_config_cv_preset_with_existing_cv_structure():
    """apply_settings_to_config must work when cv key already exists in config."""
    config = {"cv": {"generation": {"model": "old-model"}}}
    apply_settings_to_config(config, {"cv_preset": "europass"})
    assert config["cv"]["preset"] == "europass"
    assert config["cv"]["generation"]["model"] == "old-model"


def test_valid_cv_preset_group_payload_passes():
    """All cv-preset group fields pass validation together."""
    validate_settings({
        "cv_preset": "europass",
        "cv_generation_model": "gemini-2.5-flash",
    })  # must not raise


def test_valid_cv_composition_group_payload_passes():
    """All cv-composition group fields pass validation together."""
    validate_settings({
        "cv_summary_enabled": True,
        "cv_education_enabled": True,
        "cv_experience_enabled": True,
        "cv_skills_enabled": True,
        "cv_certifications_enabled": True,
        "cv_projects_enabled": True,
        "cv_publications_enabled": False,
        "cv_languages_enabled": True,
    })  # must not raise


# ── Preset-based CV settings defaults match cv.yaml ──────────────────────────────

def test_cv_preset_defaults_match_cv_yaml():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_preset"]["default"] == "europass"


def test_cv_generation_model_default_uses_25_flash():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_generation_model"]["default"] == "gemini-2.5-flash"


def test_cv_generation_model_options_are_constrained() -> None:
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_generation_model"]["options"] == [
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite",
        "gemini-2.5-pro",
    ]


def test_cv_summary_enabled_default() -> None:
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_summary_enabled"]["default"] is True


def test_cv_education_enabled_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_education_enabled"]["default"] is True


def test_cv_experience_enabled_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_experience_enabled"]["default"] is True


def test_cv_skills_enabled_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_skills_enabled"]["default"] is True


def test_cv_certifications_enabled_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_certifications_enabled"]["default"] is True


def test_cv_projects_enabled_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_projects_enabled"]["default"] is True


def test_cv_publications_enabled_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_publications_enabled"]["default"] is False


def test_cv_languages_enabled_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_languages_enabled"]["default"] is True


def test_cv_max_pages_default():
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert schema_by_key["cv_max_pages"]["default"] == 2


# ── ALL_GROUP_REGISTRIES ──────────────────────────────────────────────────────

def test_all_group_registries_has_all_four_cv_groups():
    from fitcv_cp.settings_schema import ALL_GROUP_REGISTRIES
    assert "cv-preset" in ALL_GROUP_REGISTRIES["cv"]
    assert "cv-composition" in ALL_GROUP_REGISTRIES["cv"]
    assert "cv-validation" in ALL_GROUP_REGISTRIES["cv"]


# ── coerce_value for CV types ─────────────────────────────────────────────────

def test_coerce_list_str_from_list():
    # required_cv_sections was removed; no list[str] fields remain in schema
    pass


def test_coerce_list_str_from_single_value():
    # required_cv_sections was removed; no list[str] fields remain in schema
    pass


def test_coerce_cv_generation_model_strips_whitespace():
    from fitcv_cp.settings_schema import coerce_value
    result = coerce_value("cv_generation_model", "  gemini-2.5-flash  ")
    assert result == "gemini-2.5-flash"
    assert isinstance(result, str)


def test_validate_settings_rejects_unknown_cv_generation_model() -> None:
    with pytest.raises(ValidationError, match="cv_generation_model"):
        validate_settings({"cv_generation_model": "gemini-3-flash"})


def test_all_group_registries_has_ranking_and_cv():
    from fitcv_cp.settings_schema import ALL_GROUP_REGISTRIES
    assert "ranking" in ALL_GROUP_REGISTRIES
    assert "cv" in ALL_GROUP_REGISTRIES
    assert ALL_GROUP_REGISTRIES["cv"] is not None


def test_legacy_cv_required_toggles_are_removed_from_schema() -> None:
    schema_by_key = {s["key"]: s for s in SETTINGS_SCHEMA}
    assert "cv_education_required" not in schema_by_key
    assert "cv_projects_required" not in schema_by_key
