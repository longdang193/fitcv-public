from pathlib import Path
from unittest.mock import MagicMock, patch

from fitcv.pipeline import run_pipeline
from fitcv.agentic_cv_generation import (
    _build_fitcv_langgraph_env_values,
    _generate_cv_with_live_provider,
    _shallow_section_repair_targets,
    generate_from_analysis,
)


def _minimal_config() -> dict:
    return {
        "paths": {"candidate_profile": "data/candidate_profile.yaml"},
        "pipeline": {
            "vector_search_top_n": 2,
            "ai_score_top_n": 2,
            "final_top_n": 2,
            "evidence_top_k": 3,
        },
        "cv": {
            "generation": {
                "model": "gemini-2.5-flash",
                "prompt_version": "v1",
            },
            "preset": "europass",
            "composition": {
                "summary": {"enabled": True},
                "experience": {"enabled": True, "required": True},
                "skills": {"enabled": True, "required": True},
            },
            "content_rules": {"evidence_grounded_only": True},
            "validation": {"max_pages": 2},
            "agentic_late_stage": {"enabled": False},
        },
        "cv_generation_model": "gemini-2.5-flash",
        "required_cv_sections": ["Experience", "Skills"],
        "cv_max_pages": 2,
        "prompt_version": "v1",
    }


def _minimal_profile() -> dict:
    return {
        "name": "Test Candidate",
        "headline": "Data Engineer",
        "skills": [{"name": "SQL"}, {"name": "Python"}],
        "years_experience": 5,
        "preferences": {"target_role": "Data Engineer", "domains": []},
        "experiences": [{"role": "DE", "company": "ACME", "start": "2020", "end": "2022"}],
        "projects": [],
        "certifications": [],
        "languages": [],
    }


def _minimal_job(url: str = "https://example.com/1") -> dict:
    return {
        "job_url": url,
        "job_title": "Data Engineer",
        "required_skills": ["SQL"],
        "years_required": 3,
        "vector_rank": 1,
        "ai_score": 0.85,
        "final_score": 0.80,
        "seniority": "senior",
        "location_type": "remote",
        "preferences": {},
    }


def _minimal_analysis_record() -> dict:
    job = _minimal_job()
    return {
        "job_url": job["job_url"],
        "job_title": job["job_title"],
        "status": "ready_for_generation",
        "fit_classification": "strong",
        "job_snapshot": {
            **job,
            "title": "Data Engineer",
        },
        "evidence_payload": [{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
        "evidence_used": [{"evidence_id": "exp-1"}],
        "evidence_selection_summary": {"selected_evidence_count": 1},
        "gap_summary": {"matched": ["SQL"], "missing": []},
    }


def _minimal_structured_cv() -> dict:
    return {
        "schema_version": "structured_cv.v1",
        "preset": "europass",
        "locale": "en",
        "job_url": "https://example.com/1",
        "fit_classification": "strong",
        "target_role": "Data Engineer",
        "sections": {
            "header": {
                "name": "Test Candidate",
                "title": "Data Engineer",
                "location": None,
                "contact": {"email": None, "phone": None, "linkedin": None},
            },
            "summary": {"text": "Grounded summary"},
            "experience": [
                {
                    "role": "Data Engineer",
                    "company": "ACME",
                    "start": None,
                    "end": None,
                    "location": None,
                    "bullets": ["Built grounded reporting workflows."],
                }
            ],
            "projects": [],
            "education": [],
            "skills": {"groups": [{"label": "Core", "items": ["SQL", "Python"]}]},
            "certifications": [],
            "publications": [],
            "languages": [],
        },
    }

def test_shallow_section_repair_targets_flags_context_only_projects() -> None:
    structured_cv = _minimal_structured_cv()
    structured_cv["sections"]["projects"] = [
        {
            "name": "Project A",
            "context": "2022-06 - 2022-10",
            "bullets": [],
        }
    ]
    assert _shallow_section_repair_targets(structured_cv) == ["projects"]

def test_shallow_section_repair_targets_flags_empty_experience_bullets() -> None:
    structured_cv = _minimal_structured_cv()
    structured_cv["sections"]["experience"][0]["bullets"] = []
    assert _shallow_section_repair_targets(structured_cv) == ["experience"]


@patch("fitcv.pipeline.store_cv_version")
@patch("fitcv.pipeline.run_all_validations")
@patch("fitcv.pipeline.generate_cv")
@patch("fitcv.pipeline.compute_gap")
@patch("fitcv.pipeline.retrieve_evidence_bundle")
@patch("fitcv.pipeline.store_final_ranking")
@patch("fitcv.pipeline.rank_jobs")
@patch("fitcv.pipeline.build_ranking_features")
@patch("fitcv.pipeline.run_ai_scoring")
@patch("fitcv.pipeline.run_vector_search")
@patch("fitcv.pipeline.embed_and_store_candidate")
@patch("fitcv.pipeline.embed_and_store_jobs")
@patch("fitcv.pipeline.store_filter_results")
@patch("fitcv.pipeline.apply_rule_filters")
@patch("fitcv.pipeline.load_candidate_to_bigquery")
@patch("fitcv.pipeline.load_profile_yaml")
@patch("fitcv.pipeline.load_structured_jobs")
@patch("fitcv.pipeline.load_run_structured_jobs")
@patch("fitcv.pipeline.enrich_batch")
@patch("fitcv.pipeline.load_to_bigquery")
@patch("fitcv.pipeline.normalize_batch")
@patch("fitcv.pipeline.parse_jobs_file")
@patch("fitcv.pipeline.load_config")
def test_run_pipeline_keeps_original_late_stage_path_by_default(
    mock_config: MagicMock,
    mock_parse: MagicMock,
    mock_normalize: MagicMock,
    mock_load_to_bigquery: MagicMock,
    mock_enrich: MagicMock,
    mock_load_run_structured: MagicMock,
    mock_load_structured: MagicMock,
    mock_load_profile: MagicMock,
    mock_load_candidate_to_bigquery: MagicMock,
    mock_apply_rule_filters: MagicMock,
    mock_store_filter_results: MagicMock,
    mock_embed_jobs: MagicMock,
    mock_embed_candidate: MagicMock,
    mock_run_vector_search: MagicMock,
    mock_run_ai_scoring: MagicMock,
    mock_build_ranking_features: MagicMock,
    mock_rank_jobs: MagicMock,
    mock_store_final_ranking: MagicMock,
    mock_retrieve_evidence_bundle: MagicMock,
    mock_compute_gap: MagicMock,
    mock_generate_cv: MagicMock,
    mock_run_all_validations: MagicMock,
    mock_store_cv_version: MagicMock,
) -> None:
    job = _minimal_job()
    profile = _minimal_profile()
    config = _minimal_config()

    mock_config.return_value = config
    mock_parse.return_value = [job]
    mock_normalize.return_value = [job]
    mock_enrich.return_value = [job]
    mock_load_run_structured.return_value = [job]
    mock_load_structured.return_value = [job]
    mock_load_profile.return_value = profile
    mock_apply_rule_filters.return_value = {"passed": [job["job_url"]], "rejected": []}
    ranked_job = {
        **job,
        "title": "Data Engineer",
        "fit_label": "strong",
        "fit_label_source": "reranker",
        "shortlist_origin": "vector_search",
    }
    mock_run_vector_search.return_value = [{"job_url": job["job_url"], "vector_similarity": 0.9, "vector_rank": 1}]
    mock_run_ai_scoring.return_value = [{"job_url": job["job_url"], "ai_score": 0.85, "fit_label": "strong"}]
    mock_build_ranking_features.return_value = [ranked_job]
    mock_rank_jobs.return_value = [ranked_job]
    mock_retrieve_evidence_bundle.return_value = {
        "selected_evidence": [{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
        "channel_counts": {"required_skill_support": 1},
        "merged_pool_size": 1,
        "deduped_pool_size": 1,
        "selected_evidence_count": 1,
    }
    mock_compute_gap.return_value = {"matched": ["SQL"], "missing": []}
    mock_generate_cv.return_value = "# Test Candidate\n## Summary\nGrounded summary"
    mock_run_all_validations.return_value = {
        "valid": True,
        "missing_sections": [],
        "grounding_violations": [],
        "deterministic_grounding_violations": [],
        "semantic_grounding_violations": [],
        "skill_violations": [],
        "warnings": [],
        "support_source_summary": {},
    }

    with patch("fitcv.pipeline.run_agentic_cv_analysis", create=True) as mock_agentic_analysis, patch(
        "fitcv.pipeline.run_agentic_cv_generation",
        create=True,
    ) as mock_agentic_generation:
        result = run_pipeline("data/sample_jobs.json", config_path="config/env.yaml", run_id="late-stage-default")

    mock_agentic_analysis.assert_not_called()
    mock_agentic_generation.assert_not_called()
    mock_generate_cv.assert_called_once()
    stage_artifacts = result["stage_transition_artifacts"]["stages"]
    assert stage_artifacts["cv_analysis"]["late_stage_mode"]["late_stage_mode"] == "non_agentic"
    assert stage_artifacts["cv_analysis"]["late_stage_mode"]["agentic_late_stage_enabled"] is False
    assert stage_artifacts["cv_analysis"]["late_stage_mode"]["agentic_status"] == "not_applicable"
    assert stage_artifacts["cv_generation"]["late_stage_mode"]["late_stage_mode"] == "non_agentic"


@patch("fitcv.pipeline.store_cv_version")
@patch("fitcv.pipeline.run_all_validations")
@patch("fitcv.pipeline.generate_cv")
@patch("fitcv.pipeline.compute_gap")
@patch("fitcv.pipeline.retrieve_evidence_bundle")
@patch("fitcv.pipeline.store_final_ranking")
@patch("fitcv.pipeline.rank_jobs")
@patch("fitcv.pipeline.build_ranking_features")
@patch("fitcv.pipeline.run_ai_scoring")
@patch("fitcv.pipeline.run_vector_search")
@patch("fitcv.pipeline.embed_and_store_candidate")
@patch("fitcv.pipeline.embed_and_store_jobs")
@patch("fitcv.pipeline.store_filter_results")
@patch("fitcv.pipeline.apply_rule_filters")
@patch("fitcv.pipeline.load_candidate_to_bigquery")
@patch("fitcv.pipeline.load_profile_yaml")
@patch("fitcv.pipeline.load_structured_jobs")
@patch("fitcv.pipeline.load_run_structured_jobs")
@patch("fitcv.pipeline.enrich_batch")
@patch("fitcv.pipeline.load_to_bigquery")
@patch("fitcv.pipeline.normalize_batch")
@patch("fitcv.pipeline.parse_jobs_file")
@patch("fitcv.pipeline.load_config")
def test_run_pipeline_routes_through_agentic_late_stage_when_enabled(
    mock_config: MagicMock,
    mock_parse: MagicMock,
    mock_normalize: MagicMock,
    mock_load_to_bigquery: MagicMock,
    mock_enrich: MagicMock,
    mock_load_run_structured: MagicMock,
    mock_load_structured: MagicMock,
    mock_load_profile: MagicMock,
    mock_load_candidate_to_bigquery: MagicMock,
    mock_apply_rule_filters: MagicMock,
    mock_store_filter_results: MagicMock,
    mock_embed_jobs: MagicMock,
    mock_embed_candidate: MagicMock,
    mock_run_vector_search: MagicMock,
    mock_run_ai_scoring: MagicMock,
    mock_build_ranking_features: MagicMock,
    mock_rank_jobs: MagicMock,
    mock_store_final_ranking: MagicMock,
    mock_retrieve_evidence_bundle: MagicMock,
    mock_compute_gap: MagicMock,
    mock_generate_cv: MagicMock,
    mock_run_all_validations: MagicMock,
    mock_store_cv_version: MagicMock,
) -> None:
    class _Reporter:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, str, dict | None]] = []

        def emit(self, stage: str, level: str, message: str, payload: dict | None = None) -> None:
            self.events.append((stage, level, message, payload))

    job = _minimal_job()
    profile = _minimal_profile()
    config = _minimal_config()
    config["cv"]["agentic_late_stage"]["enabled"] = True
    reporter = _Reporter()

    mock_config.return_value = config
    mock_parse.return_value = [job]
    mock_normalize.return_value = [job]
    mock_enrich.return_value = [job]
    mock_load_run_structured.return_value = [job]
    mock_load_structured.return_value = [job]
    mock_load_profile.return_value = profile
    mock_apply_rule_filters.return_value = {"passed": [job["job_url"]], "rejected": []}
    ranked_job = {
        **job,
        "title": "Data Engineer",
        "fit_label": "strong",
        "fit_label_source": "reranker",
        "shortlist_origin": "vector_search",
    }
    mock_run_vector_search.return_value = [{"job_url": job["job_url"], "vector_similarity": 0.9, "vector_rank": 1}]
    mock_run_ai_scoring.return_value = [{"job_url": job["job_url"], "ai_score": 0.85, "fit_label": "strong"}]
    mock_build_ranking_features.return_value = [ranked_job]
    mock_rank_jobs.return_value = [ranked_job]
    mock_retrieve_evidence_bundle.return_value = {
        "selected_evidence": [{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
        "channel_counts": {"required_skill_support": 1},
        "merged_pool_size": 1,
        "deduped_pool_size": 1,
        "selected_evidence_count": 1,
    }
    mock_compute_gap.return_value = {"matched": ["SQL"], "missing": []}
    mock_run_all_validations.return_value = {
        "valid": True,
        "missing_sections": [],
        "grounding_violations": [],
        "deterministic_grounding_violations": [],
        "semantic_grounding_violations": [],
        "skill_violations": [],
        "warnings": [],
        "support_source_summary": {},
    }

    agentic_analysis_result = {
        "status": "ready_for_generation",
        "analysis_input_fingerprint": "agentic::fingerprint",
        "evidence_payload": [{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
        "evidence_used": [{"evidence_id": "exp-1"}],
        "evidence_selection_summary": {"selected_evidence_count": 1},
        "gap_summary": {"matched": ["SQL"], "missing": []},
        "fit_classification": "strong",
        "error": None,
    }
    agentic_generation_result = {
        "status": "accepted",
        "fit_classification": "strong",
        "analysis_input_summary": {"required_skills": ["SQL"]},
        "evidence_used": [{"evidence_id": "exp-1"}],
        "evidence_selection_summary": {"selected_evidence_count": 1},
        "gap_summary": {"matched": ["SQL"], "missing": []},
        "structured_cv_initial": {"sections": {"summary": {"content": ["Grounded summary"]}}},
        "validation_initial": {
            "valid": True,
            "missing_sections": [],
            "grounding_violations": [],
            "deterministic_grounding_violations": [],
            "semantic_grounding_violations": [],
            "skill_violations": [],
            "warnings": [],
            "support_source_summary": {},
        },
        "repair_attempt": {"performed": False, "missing_sections": []},
        "structured_cv_final": {"sections": {"header": {"name": "Test Candidate"}}},
        "markdown_final": "# Test Candidate\n## Summary\nGrounded summary",
        "error": None,
        "runtime_provenance": {
            "runtime_path": "fitcv_langgraph_live",
            "provider": "openai",
            "model": "cx/gpt-5.5",
        },
        "agentic_live_trace": {
            "trace_family": "agentic_step_trace",
            "step_id": "cv_generation",
            "trace_status": "completed",
            "runtime_provenance": {
                "runtime_path": "fitcv_langgraph_live",
                "provider": "openai",
                "model": "cx/gpt-5.5",
                "prompt_contract": "fitcv_structured_generation_prompt",
                "template_path": "src/fitcv/prompts/templates/europass.md",
                "response_schema_name": "fitcv_structured_cv_document",
            },
            "attempts": [
                {
                    "attempt_index": 1,
                    "provider_status": "accepted",
                    "attempt_type": "initial_generation",
                    "input_character_count": 512,
                    "input_item_count": 1,
                }
            ],
            "input_summary": {
                "attempt_count": 1,
                "input_item_count": 1,
            },
            "output_summary": {
                "accepted_output_present": True,
                "final_status": "accepted",
            },
            "validation_summary": {
                "initial_valid": True,
                "final_valid": True,
                "initial_missing_fields": [],
                "final_missing_fields": [],
                "violation_count": 0,
                "warning_count": 0,
            },
            "repair_summary": {
                "repair_attempted": False,
                "repair_attempt_count": 0,
                "repair_targets": [],
            },
            "error_summary": None,
        },
    }

    with patch(
        "fitcv.pipeline.run_agentic_cv_analysis",
        create=True,
        return_value=agentic_analysis_result,
    ) as mock_agentic_analysis, patch(
        "fitcv.pipeline.run_agentic_cv_generation",
        create=True,
        return_value=agentic_generation_result,
    ) as mock_agentic_generation:
        result = run_pipeline(
            "data/sample_jobs.json",
            config_path="config/env.yaml",
            run_id="late-stage-agentic",
            reporter=reporter,
        )

    mock_agentic_analysis.assert_called_once()
    mock_agentic_generation.assert_called_once()
    mock_generate_cv.assert_not_called()
    assert result["cv_generation_debug_records"][0]["status"] == "accepted"
    assert result["cv_generation_debug_records"][0]["cv_generation_model"] == "cx/gpt-5.5"
    assert result["cv_generation_debug_records"][0]["runtime_provenance"]["provider"] == "openai"
    assert result["cv_generation_debug_records"][0]["agentic_live_trace"]["trace_status"] == "completed"
    assert result["cv_generation_debug_records"][0]["markdown_final"].startswith("# Test Candidate")
    assert result["agentic_live_trace"]["trace_status"] == "completed"
    assert result["agentic_live_trace"]["trace_family"] == "agentic_step_trace"
    assert result["agentic_live_trace"]["step_id"] == "cv_generation"
    assert result["agentic_live_trace"]["records"][0]["attempts"][0]["provider_status"] == "accepted"
    stage_artifacts = result["stage_transition_artifacts"]["stages"]
    assert stage_artifacts["cv_analysis"]["late_stage_mode"]["late_stage_mode"] == "agentic"
    assert stage_artifacts["cv_analysis"]["late_stage_mode"]["agentic_late_stage_enabled"] is True
    assert stage_artifacts["cv_analysis"]["late_stage_mode"]["agentic_status"] == "completed"
    assert stage_artifacts["cv_generation"]["late_stage_mode"]["late_stage_mode"] == "agentic"
    assert stage_artifacts["cv_generation"]["decision_summary"]["cv_generation_model"] == "cx/gpt-5.5"
    assert stage_artifacts["cv_generation"]["decision_summary"]["cv_generation_provider"] == "openai"
    cv_generation_invoked_event = next(event for event in reporter.events if event[0] == "layer4_cv_generation_invoked")
    assert cv_generation_invoked_event[3]["provenance"]["cv_generation_model"] == "cx/gpt-5.5"


@patch("fitcv.pipeline.store_cv_version")
@patch("fitcv.pipeline.run_all_validations")
@patch("fitcv.pipeline.generate_cv")
@patch("fitcv.pipeline.compute_gap")
@patch("fitcv.pipeline.retrieve_evidence_bundle")
@patch("fitcv.pipeline.store_final_ranking")
@patch("fitcv.pipeline.rank_jobs")
@patch("fitcv.pipeline.build_ranking_features")
@patch("fitcv.pipeline.run_ai_scoring")
@patch("fitcv.pipeline.run_vector_search")
@patch("fitcv.pipeline.embed_and_store_candidate")
@patch("fitcv.pipeline.embed_and_store_jobs")
@patch("fitcv.pipeline.store_filter_results")
@patch("fitcv.pipeline.apply_rule_filters")
@patch("fitcv.pipeline.load_candidate_to_bigquery")
@patch("fitcv.pipeline.load_profile_yaml")
@patch("fitcv.pipeline.load_structured_jobs")
@patch("fitcv.pipeline.load_run_structured_jobs")
@patch("fitcv.pipeline.enrich_batch")
@patch("fitcv.pipeline.load_to_bigquery")
@patch("fitcv.pipeline.normalize_batch")
@patch("fitcv.pipeline.parse_jobs_file")
@patch("fitcv.pipeline.load_config")
def test_run_pipeline_marks_review_required_and_skips_persist_when_agentic_gate_triggers(
    mock_config: MagicMock,
    mock_parse: MagicMock,
    mock_normalize: MagicMock,
    mock_load_to_bigquery: MagicMock,
    mock_enrich: MagicMock,
    mock_load_run_structured: MagicMock,
    mock_load_structured: MagicMock,
    mock_load_profile: MagicMock,
    mock_load_candidate_to_bigquery: MagicMock,
    mock_apply_rule_filters: MagicMock,
    mock_store_filter_results: MagicMock,
    mock_embed_jobs: MagicMock,
    mock_embed_candidate: MagicMock,
    mock_run_vector_search: MagicMock,
    mock_run_ai_scoring: MagicMock,
    mock_build_ranking_features: MagicMock,
    mock_rank_jobs: MagicMock,
    mock_store_final_ranking: MagicMock,
    mock_retrieve_evidence_bundle: MagicMock,
    mock_compute_gap: MagicMock,
    mock_generate_cv: MagicMock,
    mock_run_all_validations: MagicMock,
    mock_store_cv_version: MagicMock,
) -> None:
    job = _minimal_job()
    profile = _minimal_profile()
    config = _minimal_config()
    config["cv"]["agentic_late_stage"]["enabled"] = True
    mock_config.return_value = config
    mock_parse.return_value = [job]
    mock_normalize.return_value = [job]
    mock_enrich.return_value = [job]
    mock_load_run_structured.return_value = [job]
    mock_load_structured.return_value = [job]
    mock_load_profile.return_value = profile
    mock_apply_rule_filters.return_value = {"passed": [job["job_url"]], "rejected": []}
    ranked_job = {**job, "fit_label": "stretch", "fit_label_source": "reranker", "shortlist_origin": "vector_search"}
    mock_run_vector_search.return_value = [{"job_url": job["job_url"], "vector_similarity": 0.9, "vector_rank": 1}]
    mock_run_ai_scoring.return_value = [{"job_url": job["job_url"], "ai_score": 0.7, "fit_label": "stretch"}]
    mock_build_ranking_features.return_value = [ranked_job]
    mock_rank_jobs.return_value = [ranked_job]
    mock_retrieve_evidence_bundle.return_value = {
        "selected_evidence": [{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
        "channel_counts": {"required_skill_support": 1},
        "merged_pool_size": 1,
        "deduped_pool_size": 1,
        "selected_evidence_count": 1,
    }
    mock_compute_gap.return_value = {"matched": ["SQL"], "missing": ["Python"]}
    mock_run_all_validations.return_value = {"valid": True, "missing_sections": []}

    agentic_analysis_result = {
        "status": "ready_for_generation",
        "analysis_input_fingerprint": "agentic::fingerprint",
        "evidence_payload": [{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
        "evidence_used": [{"evidence_id": "exp-1"}],
        "evidence_selection_summary": {"selected_evidence_count": 1},
        "gap_summary": {"matched": ["SQL"], "missing": ["Python"]},
        "fit_classification": "stretch",
        "requirement_coverage": [{"requirement": "Python", "support_strength": "unsupported"}],
        "section_confidence_hints": {"experience": "low"},
        "do_not_claim": ["Python"],
        "error": None,
    }
    agentic_generation_result = {
        "status": "accepted",
        "fit_classification": "stretch",
        "analysis_input_summary": {"required_skills": ["SQL", "Python"]},
        "evidence_used": [{"evidence_id": "exp-1"}],
        "evidence_selection_summary": {"selected_evidence_count": 1},
        "gap_summary": {"matched": ["SQL"], "missing": ["Python"]},
        "structured_cv_initial": {"sections": {"summary": {"content": ["Grounded summary"]}}},
        "validation_initial": {"valid": True, "missing_sections": []},
        "repair_attempt": {"performed": False, "missing_sections": []},
        "structured_cv_final": {"sections": {"header": {"name": "Test Candidate"}}},
        "markdown_final": "# Test Candidate\n## Summary\nGrounded summary",
        "error": None,
        "runtime_provenance": {"runtime_path": "fitcv_langgraph_live", "provider": "openai", "model": "cx/gpt-5.2"},
        "agentic_live_trace": {"trace_family": "agentic_step_trace", "step_id": "cv_generation", "trace_status": "completed"},
    }

    with patch("fitcv.pipeline.run_agentic_cv_analysis", create=True, return_value=agentic_analysis_result), patch(
        "fitcv.pipeline.run_agentic_cv_generation",
        create=True,
        return_value=agentic_generation_result,
    ):
        result = run_pipeline("data/sample_jobs.json", config_path="config/env.yaml", run_id="late-stage-agentic-review")

    assert result["cvs_generated"] == 0
    assert result["cv_generation_debug_records"][0]["status"] == "review_required"
    assert result["cv_generation_debug_records"][0]["error"]["stage"] == "review_gate"
    assert "Low confidence sections" in str(result["cv_generation_debug_records"][0]["error"]["message"])
    mock_store_cv_version.assert_not_called()


@patch("fitcv.pipeline.store_cv_version")
@patch("fitcv.pipeline.run_all_validations")
@patch("fitcv.pipeline.generate_cv")
@patch("fitcv.pipeline.compute_gap")
@patch("fitcv.pipeline.retrieve_evidence_bundle")
@patch("fitcv.pipeline.store_final_ranking")
@patch("fitcv.pipeline.rank_jobs")
@patch("fitcv.pipeline.build_ranking_features")
@patch("fitcv.pipeline.run_ai_scoring")
@patch("fitcv.pipeline.run_vector_search")
@patch("fitcv.pipeline.embed_and_store_candidate")
@patch("fitcv.pipeline.embed_and_store_jobs")
@patch("fitcv.pipeline.store_filter_results")
@patch("fitcv.pipeline.apply_rule_filters")
@patch("fitcv.pipeline.load_candidate_to_bigquery")
@patch("fitcv.pipeline.load_profile_yaml")
@patch("fitcv.pipeline.load_structured_jobs")
@patch("fitcv.pipeline.load_run_structured_jobs")
@patch("fitcv.pipeline.enrich_batch")
@patch("fitcv.pipeline.load_to_bigquery")
@patch("fitcv.pipeline.normalize_batch")
@patch("fitcv.pipeline.parse_jobs_file")
@patch("fitcv.pipeline.load_config")
def test_run_pipeline_marks_review_required_from_markdown_quality_flags(
    mock_config: MagicMock,
    mock_parse: MagicMock,
    mock_normalize: MagicMock,
    mock_load_to_bigquery: MagicMock,
    mock_enrich: MagicMock,
    mock_load_run_structured: MagicMock,
    mock_load_structured: MagicMock,
    mock_load_profile: MagicMock,
    mock_load_candidate_to_bigquery: MagicMock,
    mock_apply_rule_filters: MagicMock,
    mock_store_filter_results: MagicMock,
    mock_embed_jobs: MagicMock,
    mock_embed_candidate: MagicMock,
    mock_run_vector_search: MagicMock,
    mock_run_ai_scoring: MagicMock,
    mock_build_ranking_features: MagicMock,
    mock_rank_jobs: MagicMock,
    mock_store_final_ranking: MagicMock,
    mock_retrieve_evidence_bundle: MagicMock,
    mock_compute_gap: MagicMock,
    mock_generate_cv: MagicMock,
    mock_run_all_validations: MagicMock,
    mock_store_cv_version: MagicMock,
) -> None:
    job = _minimal_job()
    profile = _minimal_profile()
    config = _minimal_config()
    config["cv"]["agentic_late_stage"]["enabled"] = True
    mock_config.return_value = config
    mock_parse.return_value = [job]
    mock_normalize.return_value = [job]
    mock_enrich.return_value = [job]
    mock_load_run_structured.return_value = [job]
    mock_load_structured.return_value = [job]
    mock_load_profile.return_value = profile
    mock_apply_rule_filters.return_value = {"passed": [job["job_url"]], "rejected": []}
    ranked_job = {**job, "fit_label": "stretch", "fit_label_source": "reranker", "shortlist_origin": "vector_search"}
    mock_run_vector_search.return_value = [{"job_url": job["job_url"], "vector_similarity": 0.9, "vector_rank": 1}]
    mock_run_ai_scoring.return_value = [{"job_url": job["job_url"], "ai_score": 0.7, "fit_label": "stretch"}]
    mock_build_ranking_features.return_value = [ranked_job]
    mock_rank_jobs.return_value = [ranked_job]
    mock_retrieve_evidence_bundle.return_value = {
        "selected_evidence": [{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
        "channel_counts": {"required_skill_support": 1},
        "merged_pool_size": 1,
        "deduped_pool_size": 1,
        "selected_evidence_count": 1,
    }
    mock_compute_gap.return_value = {"matched": ["SQL"], "missing": ["Python"]}
    mock_run_all_validations.return_value = {"valid": True, "missing_sections": []}

    agentic_analysis_result = {
        "status": "ready_for_generation",
        "analysis_input_fingerprint": "agentic::fingerprint",
        "evidence_payload": [{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
        "evidence_used": [{"evidence_id": "exp-1"}],
        "evidence_selection_summary": {"selected_evidence_count": 1},
        "gap_summary": {"matched": ["SQL"], "missing": ["Python"]},
        "fit_classification": "stretch",
        "requirement_coverage": [{"requirement": "Python", "support_strength": "supported"}],
        "section_confidence_hints": {"experience": "high"},
        "do_not_claim": [],
        "error": None,
    }
    agentic_generation_result = {
        "status": "accepted",
        "fit_classification": "stretch",
        "analysis_input_summary": {"required_skills": ["SQL", "Python"]},
        "evidence_used": [{"evidence_id": "exp-1"}],
        "evidence_selection_summary": {"selected_evidence_count": 1},
        "gap_summary": {"matched": ["SQL"], "missing": ["Python"]},
        "structured_cv_initial": {"sections": {"summary": {"content": ["Grounded summary"]}}},
        "validation_initial": {
            "valid": True,
            "missing_sections": [],
            "markdown_quality_review_flags": ["Experience section appears shallow (fewer than 2 bullets)."],
            "markdown_quality_blocking_issues": [],
        },
        "repair_attempt": {"performed": False, "missing_sections": []},
        "structured_cv_final": {"sections": {"header": {"name": "Test Candidate"}}},
        "markdown_final": "# Test Candidate\n## Summary\nGrounded summary",
        "error": None,
        "runtime_provenance": {"runtime_path": "fitcv_langgraph_live", "provider": "openai", "model": "cx/gpt-5.2"},
        "agentic_live_trace": {"trace_family": "agentic_step_trace", "step_id": "cv_generation", "trace_status": "completed"},
    }

    with patch("fitcv.pipeline.run_agentic_cv_analysis", create=True, return_value=agentic_analysis_result), patch(
        "fitcv.pipeline.run_agentic_cv_generation",
        create=True,
        return_value=agentic_generation_result,
    ):
        result = run_pipeline("data/sample_jobs.json", config_path="config/env.yaml", run_id="late-stage-agentic-markdown-review")

    assert result["cvs_generated"] == 0
    assert result["cv_generation_debug_records"][0]["status"] == "review_required"
    assert result["cv_generation_debug_records"][0]["error"]["stage"] == "review_gate"
    assert "Markdown quality requires review" in str(result["cv_generation_debug_records"][0]["error"]["message"])
    mock_store_cv_version.assert_not_called()


@patch("fitcv.agentic_cv_generation.run_all_validations")
@patch("fitcv.agentic_cv_generation.generate_cv")
def test_generate_from_analysis_uses_fitcv_langgraph_live_provider_when_env_present(
    mock_generate_cv: MagicMock,
    mock_run_all_validations: MagicMock,
) -> None:
    analysis_record = _minimal_analysis_record()
    analysis_record["do_not_claim"] = ["Python"]
    analysis_record["requirement_coverage"] = [
        {"requirement": "SQL", "support_strength": "supported"},
        {"requirement": "Python", "support_strength": "unsupported"},
    ]
    analysis_record["section_confidence_hints"] = {"experience": "high"}
    profile = _minimal_profile()
    config = _minimal_config()
    config["cv"]["agentic_late_stage"]["enabled"] = True
    mock_run_all_validations.return_value = {
        "valid": True,
        "missing_sections": [],
        "grounding_violations": [],
        "deterministic_grounding_violations": [],
        "semantic_grounding_violations": [],
        "skill_violations": [],
        "warnings": [],
        "support_source_summary": {},
    }
    fake_generated_cv = {
        "structured_cv": _minimal_structured_cv(),
        "markdown": "# Test Candidate\n## Summary\nGrounded summary\n## Skills\nSQL, Python\n## Experience\n- Built grounded reporting workflows.",
    }

    with patch(
        "fitcv.agentic_cv_generation._generate_cv_with_live_provider",
        return_value=fake_generated_cv,
    ) as mock_live_generation, patch(
        "fitcv.agentic_cv_generation._live_runtime_provenance_or_none",
        return_value={
            "runtime_path": "fitcv_langgraph_live",
            "provider": "openai",
            "model": "cx/gpt-5.4",
        },
    ):
        result = generate_from_analysis(analysis_record, profile, config)

    mock_live_generation.assert_called_once()
    call_kwargs = mock_live_generation.call_args.kwargs
    assert call_kwargs["gap"]["do_not_claim"] == ["Python"]
    assert len(call_kwargs["gap"]["requirement_coverage"]) == 2
    assert call_kwargs["gap"]["section_confidence_hints"]["experience"] == "high"
    mock_generate_cv.assert_not_called()
    assert result["status"] == "accepted"
    assert result["runtime_provenance"]["runtime_path"] == "fitcv_langgraph_live"
    assert result["agentic_live_trace"]["trace_status"] == "completed"
    assert result["agentic_live_trace"]["trace_family"] == "agentic_step_trace"
    assert result["agentic_live_trace"]["step_id"] == "cv_generation"
    assert result["agentic_live_trace"]["runtime_provenance"]["response_schema_name"] == "fitcv_structured_cv_document"
    assert result["agentic_live_trace"]["attempts"][0]["provider_status"] == "accepted"
    assert result["agentic_live_trace"]["validation_summary"]["final_valid"] is True


@patch("fitcv.agentic_cv_generation.generate_cv")
def test_generate_from_analysis_does_not_silently_fallback_when_live_runtime_returns_no_final_result(
    mock_generate_cv: MagicMock,
) -> None:
    analysis_record = _minimal_analysis_record()
    profile = _minimal_profile()
    config = _minimal_config()
    config["cv"]["agentic_late_stage"]["enabled"] = True

    with patch(
        "fitcv.agentic_cv_generation._generate_cv_with_live_provider",
        side_effect=RuntimeError("live provider broke"),
    ) as mock_live_generation, patch(
        "fitcv.agentic_cv_generation._live_runtime_provenance_or_none",
        return_value={
            "runtime_path": "fitcv_langgraph_live",
            "provider": "openai",
            "model": "cx/gpt-5.4",
        },
    ):
        result = generate_from_analysis(analysis_record, profile, config)

    mock_live_generation.assert_called_once()
    mock_generate_cv.assert_not_called()
    assert result["status"] == "generation_failed"
    assert result["runtime_provenance"]["runtime_path"] == "fitcv_langgraph_live"
    assert result["error"]["stage"] == "agentic_live_provider"
    assert result["agentic_live_trace"]["trace_status"] == "degraded"
    assert result["agentic_live_trace"]["attempts"][0]["provider_status"] == "error"
    assert result["agentic_live_trace"]["attempts"][0]["error_stage"] == "agentic_live_provider"
    assert result["agentic_live_trace"]["error_summary"]["error_stage"] == "agentic_live_provider"


def test_build_fitcv_langgraph_env_values_uses_process_env_only() -> None:
    with patch.dict(
        "fitcv.agentic_cv_generation.os.environ",
        {
            "OPENAI_API_KEY": "process-key",
            "FITCV_LANGGRAPH_MODEL": "cx/gpt-5.2",
            "FITCV_LANGGRAPH_OPENAI_BASE_URL": "http://localhost:20128/v1",
        },
        clear=True,
    ):
        env_values = _build_fitcv_langgraph_env_values(None)

    assert env_values["OPENAI_API_KEY"] == "process-key"
    assert env_values["FITCV_LANGGRAPH_MODEL"] == "cx/gpt-5.2"
    assert env_values["FITCV_LANGGRAPH_OPENAI_BASE_URL"] == "http://localhost:20128/v1"

def test_generate_from_analysis_live_provider_uses_template_rendering_and_full_validation(tmp_path: Path) -> None:
    analysis_record = _minimal_analysis_record()
    profile = _minimal_profile()
    config = _minimal_config()
    config["cv"]["agentic_late_stage"]["enabled"] = True
    config["required_cv_sections"] = ["Experience", "Certifications", "Projects"]
    config["cv"]["composition"] = {
        "summary": {"enabled": False},
        "experience": {"enabled": True, "required": True},
        "skills": {"enabled": False},
        "certifications": {"enabled": True, "required": True},
        "projects": {"enabled": True, "required": True},
    }
    template_path = tmp_path / "cv_template.md"
    template_path.write_text(
        "# {{ candidate.name }}\n"
        "**{{ headline }}**\n\n"
        "## Experience\n"
        "{% for exp in selected_experiences %}- {{ exp.role }} at {{ exp.company }}\n{% endfor %}\n"
        "## Certifications\n"
        "{% for cert in selected_certifications %}- {{ cert.name }}\n{% endfor %}\n"
        "## Projects\n"
        "{% for project in selected_projects %}- {{ project.name }}\n{% endfor %}\n",
        encoding="utf-8",
    )
    config["_template_path"] = str(template_path)
    fake_generated_cv = {
        "structured_cv": _minimal_structured_cv(),
        "markdown": "",
    }

    with patch(
        "fitcv.agentic_cv_generation._generate_cv_with_live_provider",
        return_value=fake_generated_cv,
    ), patch(
        "fitcv.agentic_cv_generation._live_runtime_provenance_or_none",
        return_value={
            "runtime_path": "fitcv_langgraph_live",
            "provider": "openai",
            "model": "cx/gpt-5.4",
        },
    ):
        result = generate_from_analysis(analysis_record, profile, config)

    assert result["status"] == "validation_failed"
    assert result["runtime_provenance"]["runtime_path"] == "fitcv_langgraph_live"
    assert set(result["validation"]["missing_sections"]) >= {"Certifications", "Projects"}
    assert result["agentic_live_trace"]["trace_status"] == "completed"
    assert set(result["agentic_live_trace"]["validation_summary"]["final_missing_fields"]) >= {"Certifications", "Projects"}


@patch("fitcv.agentic_cv_generation.run_all_validations")
@patch("fitcv.agentic_cv_generation.generate_cv")
def test_generate_from_analysis_live_provider_records_retry_trace(
    mock_generate_cv: MagicMock,
    mock_run_all_validations: MagicMock,
) -> None:
    analysis_record = _minimal_analysis_record()
    profile = _minimal_profile()
    config = _minimal_config()
    config["cv"]["agentic_late_stage"]["enabled"] = True
    mock_run_all_validations.side_effect = [
        {
            "valid": False,
            "missing_sections": ["Projects"],
            "grounding_violations": [],
            "deterministic_grounding_violations": [],
            "semantic_grounding_violations": [],
            "skill_violations": [],
            "warnings": [],
            "support_source_summary": {},
        },
        {
            "valid": True,
            "missing_sections": [],
            "grounding_violations": [],
            "deterministic_grounding_violations": [],
            "semantic_grounding_violations": [],
            "skill_violations": [],
            "warnings": [],
            "support_source_summary": {},
        },
    ]
    fake_generated_cv = {
        "structured_cv": _minimal_structured_cv(),
        "markdown": "# Test Candidate\n## Experience\n- Built grounded reporting workflows.",
    }

    with patch(
        "fitcv.agentic_cv_generation._generate_cv_with_live_provider",
        side_effect=[fake_generated_cv, fake_generated_cv],
    ) as mock_live_generation, patch(
        "fitcv.agentic_cv_generation._live_runtime_provenance_or_none",
        return_value={
            "runtime_path": "fitcv_langgraph_live",
            "provider": "openai",
            "model": "cx/gpt-5.4",
        },
    ):
        result = generate_from_analysis(analysis_record, profile, config)

    mock_live_generation.assert_called()
    mock_generate_cv.assert_not_called()
    assert result["status"] == "accepted"
    assert result["agentic_live_trace"]["repair_summary"]["repair_attempted"] is True
    assert result["agentic_live_trace"]["repair_summary"]["repair_attempt_count"] == 1
    assert result["agentic_live_trace"]["repair_summary"]["repair_targets"] == ["Projects"]
    assert result["agentic_live_trace"]["attempts"][1]["attempt_index"] == 2
    assert result["agentic_live_trace"]["attempts"][1]["retry_reason"] == "missing_or_shallow_sections"


def test_generate_cv_with_live_provider_renders_repo_template_markdown(tmp_path: Path) -> None:
    config = _minimal_config()
    config["cv"]["composition"] = {
        "summary": {"enabled": False},
        "experience": {"enabled": True, "required": True},
        "skills": {"enabled": False},
        "projects": {"enabled": True},
    }
    template_path = tmp_path / "cv_template.md"
    template_path.write_text(
        "# {{ candidate.name }}\n"
        "**{{ headline }}**\n\n"
        "## Experience\n"
        "{% for exp in selected_experiences %}- {{ exp.role }} at {{ exp.company }}\n{% endfor %}\n"
        "## Projects\n"
        "{% for project in selected_projects %}- {{ project.name }}\n{% endfor %}\n",
        encoding="utf-8",
    )
    config["_template_path"] = str(template_path)

    response_payload = {
        "sections": {
            "header": {
                "name": "Test Candidate",
                "title": "Data Engineer",
                "location": None,
                "contact": {"email": None, "phone": None, "linkedin": None},
            },
            "summary": {"text": "Should be hidden"},
            "experience": [
                {
                    "role": "Data Engineer",
                    "company": "ACME",
                    "start": None,
                    "end": None,
                    "location": None,
                    "bullets": ["Built grounded reporting workflows."],
                }
            ],
            "projects": [
                {
                    "name": "Banking KPI Project",
                    "context": None,
                    "bullets": ["Created KPI reporting assets."],
                }
            ],
            "education": [],
            "skills": {"groups": [{"label": "Core", "items": ["SQL"]}]},
            "certifications": [],
            "publications": [],
            "languages": [],
        }
    }

    class _FakeClient:
        def __init__(self, _config: object) -> None:
            pass

        def generate_json(
            self,
            *,
            instructions: str,
            input_text: str,
            schema_name: str,
            schema: dict[str, object],
        ) -> dict[str, object]:
            assert "Generate a tailored CV as a structured JSON document." in input_text
            assert schema_name == "fitcv_structured_cv_document"
            assert "sections" in schema["properties"]  # type: ignore[index]
            return response_payload

    class _FakeLiveModule:
        @staticmethod
        def load_live_provider_config_from_env(_environ: dict[str, str]) -> object:
            return object()

        OpenAIResponsesClient = _FakeClient

    with patch("fitcv.agentic_cv_generation.importlib.import_module", return_value=_FakeLiveModule()):
        generated_cv = _generate_cv_with_live_provider(
            job=_minimal_job(),
            evidence=[{"evidence_id": "exp-1", "evidence_type": "experience_entry"}],
            gap={"matched": ["SQL"], "missing": []},
            profile=_minimal_profile(),
            config=config,
            fit_classification="strong",
            evidence_selection_summary={"selected_evidence_count": 1},
            repair_missing_sections=None,
            env_values={"OPENAI_API_KEY": "test-key", "FITCV_LANGGRAPH_MODEL": "cx/gpt-5.4"},
        )

    markdown = generated_cv["markdown"]
    assert "## Experience" in markdown
    assert "## Projects" in markdown
    assert "## Summary" not in markdown
    assert "## Skills" not in markdown
    assert "Data Engineer at ACME" in markdown
    assert "Banking KPI Project" in markdown
