"""
@meta
type: test
scope: unit
domain: config
covers:
  - configuration loading and validation
excludes:
  - external service connectivity
tags:
  - fast
  - ci-safe
"""
import shutil
import uuid
import os
from pathlib import Path

import pytest

from fitcv.config import (
    apply_runtime_skill_synonym_overlay,
    get_cv_generation_structured_prompt_id,
    get_gemini_model,
    get_ranking_prompt_id,
    get_vertex_location,
    load_config,
    parse_skill_synonym_overlay_yaml,
)


def test_load_config_returns_dict() -> None:
    cfg = load_config(Path(__file__).parent.parent / ".env.yaml")
    assert isinstance(cfg, dict)
    assert "gcp_project" in cfg
    assert "bigquery_dataset" in cfg


def test_load_config_has_required_keys() -> None:
    cfg = load_config(Path(__file__).parent.parent / ".env.yaml")
    assert cfg["gcp_project"] == "fitcv-491123"
    assert cfg["bigquery_dataset"] == "fitcv"
    assert "service_account_key" in cfg


def test_load_config_raises_for_missing_file() -> None:
    import pytest
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/.env.yaml")


def test_load_config_raises_for_missing_keys(tmp_path: Path) -> None:
    import pytest
    isolated_root = tmp_path / "isolated" / "a" / "b" / "c" / "d"
    isolated_root.mkdir(parents=True)
    bad_yaml = isolated_root / ".env.yaml"
    bad_yaml.write_text("some_key: value\n")
    for env_key in ("GCP_PROJECT", "BIGQUERY_DATASET", "GOOGLE_APPLICATION_CREDENTIALS"):
        os.environ.pop(env_key, None)
    with pytest.raises(ValueError, match="Missing config keys"):
        load_config(bad_yaml)


def test_load_config_prefers_standard_env_vars_for_infra_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: file-project\n"
        "bigquery_dataset: file-dataset\n"
        "service_account_key: file-key.json\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )
    monkeypatch.setenv("GCP_PROJECT", "env-project")
    monkeypatch.setenv("BIGQUERY_DATASET", "env-dataset")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/env-key.json")

    cfg = load_config(env_yaml)

    assert cfg["gcp_project"] == "env-project"
    assert cfg["bigquery_dataset"] == "env-dataset"
    assert cfg["service_account_key"] == "/tmp/env-key.json"


def test_get_vertex_location_prefers_vertex_location() -> None:
    cfg = {"location": "US", "vertex_location": "us-central1"}
    assert get_vertex_location(cfg) == "us-central1"


def test_get_vertex_location_defaults_to_us_central1() -> None:
    cfg = {"location": "US"}
    assert get_vertex_location(cfg) == "us-central1"


def test_load_config_defaults_to_repo_config_shape() -> None:
    cfg = load_config()
    assert cfg["gcp_project"] == "fitcv-491123"
    assert cfg["gemini_model"] == "gemini-2.5-flash"
    assert cfg["vertex_location"] == "us-central1"
    assert cfg["paths"]["candidate_profile"] == "data/candidate_profile.yaml"
    assert cfg["pipeline"]["vector_search_top_n"] == 50
    assert cfg["pipeline"]["ai_score_top_n"] == 50
    assert cfg["pipeline"]["final_top_n"] == 10
    assert cfg["pipeline"]["evidence_top_k"] == 5
    assert cfg["vector_top_n"] == cfg["pipeline"]["vector_search_top_n"]
    assert cfg["rerank_top_n"] == cfg["pipeline"]["ai_score_top_n"]


def test_load_config_accepts_legacy_config_env_path_with_warning() -> None:
    legacy_path = Path(__file__).parent.parent / "config" / "env.yaml"
    with pytest.warns(UserWarning, match="legacy config path"):
        cfg = load_config(legacy_path)
    assert cfg["gemini_model"] == "gemini-2.5-flash"
    assert cfg["vertex_location"] == "us-central1"


def test_load_config_prefers_reorganized_config_subfolders_over_legacy_flat_files(tmp_path: Path) -> None:
    """@proves settings_system.baseline-default-hydration"""
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\n"
        "bigquery_dataset: ds\n"
        "service_account_key: /dev/null\n"
    )
    cfg_dir = tmp_path / "config"
    (cfg_dir / "runtime").mkdir(parents=True)
    (cfg_dir / "policy").mkdir()
    (cfg_dir / "taxonomy").mkdir()

    (cfg_dir / "runtime" / "pipeline.yaml").write_text(
        "gemini_model: new-model\n"
        "embedding_model: new-embedding\n"
        "pipeline:\n"
        "  vector_search_top_n: 12\n"
        "  ai_score_top_n: 11\n"
        "  final_top_n: 7\n"
        "  evidence_top_k: 3\n"
    )
    (cfg_dir / "policy" / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )
    (cfg_dir / "taxonomy" / "skill_synonyms.yaml").write_text(
        "skill_synonyms:\n"
        "  gcp: google cloud new\n"
    )
    (cfg_dir / "skill_synonyms.yaml").write_text(
        "skill_synonyms:\n"
        "  gcp: google cloud legacy\n"
    )
    (cfg_dir / "pipeline.yaml").write_text(
        "gemini_model: legacy-model\n"
        "embedding_model: legacy-embedding\n"
        "pipeline:\n"
        "  vector_search_top_n: 99\n"
        "  ai_score_top_n: 98\n"
        "  final_top_n: 97\n"
        "  evidence_top_k: 9\n"
    )

    cfg = load_config(env_yaml)

    assert cfg["gemini_model"] == "new-model"
    assert cfg["embedding_model"] == "new-embedding"
    assert cfg["pipeline"]["vector_search_top_n"] == 12
    assert cfg["skill_synonyms"]["gcp"] == "google cloud new"
    assert Path(cfg["skill_synonyms_runtime"]["base_policy_path"]).as_posix().endswith(
        "config/taxonomy/skill_synonyms.yaml"
    )


# ── Task 1: cv.yaml config layer tests ────────────────────────────────────────


def test_load_config_includes_cv_defaults() -> None:
    """@proves settings_system.cv-generation-settings"""
    cfg = load_config()
    assert cfg["cv_generation_model"] == "gemini-2.5-flash"
    assert cfg["cv"]["generation"]["model"] == "gemini-2.5-flash"
    assert cfg["cv"]["preset"] == "europass"
    assert cfg["cv"]["composition"]["summary"]["enabled"] is True
    assert cfg["cv"]["validation"]["max_pages"] == 2
    assert cfg["cv"]["generation"]["prompt_version"] == "v1"


def test_load_config_cv_keys_missing_raises(tmp_path: Path) -> None:
    """A config without cv.yaml keys should raise ValueError after loader validation."""
    isolated_root = tmp_path / "isolated" / "a" / "b" / "c" / "d"
    isolated_root.mkdir(parents=True)
    env_yaml = isolated_root / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\nbigquery_dataset: ds\nservice_account_key: /dev/null\n"
    )
    # No cv.yaml → missing top-level 'cv' key → ValueError
    with pytest.raises(ValueError, match="Missing top-level 'cv' key"):
        load_config(env_yaml)


def test_load_config_cv_required_sections_must_be_nonempty_list(tmp_path: Path) -> None:
    """required_cv_sections must be derivable from enabled composition sections."""
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\nbigquery_dataset: ds\nservice_account_key: /dev/null\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    # composition with an enabled section → required_cv_sections will be non-empty
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "    experience:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )
    cfg = load_config(env_yaml)
    assert "Experience" in cfg["required_cv_sections"]


def test_load_config_cv_max_pages_must_be_positive(tmp_path: Path) -> None:
    """cv.validation.max_pages must be a positive integer."""
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\nbigquery_dataset: ds\nservice_account_key: /dev/null\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 0\n"
    )
    with pytest.raises(ValueError, match="max_pages"):
        load_config(env_yaml)


def test_load_config_env_yaml_overrides_nested_cv(tmp_path: Path) -> None:
    """.env.yaml keys take precedence over nested cv values in cv.yaml."""
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\nbigquery_dataset: ds\nservice_account_key: /dev/null\n"
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: my-custom-model\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    # cv.yaml also has generation.model — but env.yaml wins
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )
    cfg = load_config(env_yaml)
    # env.yaml value wins
    assert cfg["cv_generation_model"] == "my-custom-model"
    assert cfg["cv"]["generation"]["model"] == "my-custom-model"


def test_load_config_merges_skill_synonym_overlay_paths(tmp_path: Path) -> None:
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\n"
        "bigquery_dataset: ds\n"
        "service_account_key: /dev/null\n"
        "skill_synonyms_overlay_paths:\n"
        "  - skill_synonyms.overlay.yaml\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "skill_synonyms.yaml").write_text(
        "skill_synonyms:\n"
        "  gcp: google cloud\n"
        "  powerbi: power bi\n"
    )
    (cfg_dir / "skill_synonyms.overlay.yaml").write_text(
        "skill_synonyms:\n"
        "  gcp: gcp cloud\n"
        "  ga4: google analytics\n"
    )
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )

    cfg = load_config(env_yaml)

    assert cfg["skill_synonyms"]["gcp"] == "gcp cloud"
    assert cfg["skill_synonyms"]["powerbi"] == "power bi"
    assert cfg["skill_synonyms"]["ga4"] == "google analytics"
    assert cfg["skill_synonyms_runtime"]["has_overlay"] is True
    assert len(cfg["skill_synonyms_runtime"]["overlay_paths"]) == 1


def test_load_config_normalizes_role_taxonomy_structure(tmp_path: Path) -> None:
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\n"
        "bigquery_dataset: ds\n"
        "service_account_key: /dev/null\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "taxonomy.yaml").write_text(
        "role_taxonomy:\n"
        "  canonical_roles:\n"
        "    data analyst:\n"
        "      aliases:\n"
        "        - BI Analyst\n"
        "        - Business Intelligence Analyst\n"
        "  role_families:\n"
        "    analytics:\n"
        "      roles:\n"
        "        - Data Analyst\n"
        "  role_family_neighbors:\n"
        "    analytics:\n"
        "      - data_science\n"
    )
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )

    cfg = load_config(env_yaml)

    assert cfg["role_taxonomy"]["canonical_role_by_alias"]["bi analyst"] == "data analyst"
    assert cfg["role_taxonomy"]["canonical_role_by_alias"]["business intelligence analyst"] == "data analyst"
    assert cfg["role_taxonomy"]["role_family_by_role"]["data analyst"] == "analytics"
    assert cfg["role_taxonomy"]["role_family_neighbors"]["analytics"] == ("data_science",)


def test_parse_skill_synonym_overlay_yaml_accepts_nested_skill_synonyms() -> None:
    overlay = parse_skill_synonym_overlay_yaml(
        "skill_synonyms:\n"
        "  PowerBI: power bi\n"
        "  GCP: google cloud\n"
    )

    assert overlay == {
        "powerbi": "power bi",
        "gcp": "google cloud",
    }


def test_parse_skill_synonym_overlay_yaml_rejects_invalid_mapping_values() -> None:
    with pytest.raises(ValueError, match="must be non-empty strings"):
        parse_skill_synonym_overlay_yaml(
            "skill_synonyms:\n"
            "  powerbi: ''\n"
        )


def test_apply_runtime_skill_synonym_overlay_merges_entries_and_runtime_metadata() -> None:
    cfg = {
        "skill_synonyms": {
            "gcp": "google cloud",
            "powerbi": "power bi",
        },
        "skill_synonyms_runtime": {
            "base_policy_path": "config/taxonomy/skill_synonyms.yaml",
            "overlay_paths": [],
            "has_overlay": False,
            "entry_count": 2,
        },
    }

    updated = apply_runtime_skill_synonym_overlay(
        cfg,
        {
            "gcp": "gcp cloud",
            "ga4": "google analytics",
        },
        source="upload",
        filename="reviewed-skill-synonyms.yaml",
        uploaded_at="2026-04-02T21:30:00Z",
    )

    assert updated["skill_synonyms"]["gcp"] == "gcp cloud"
    assert updated["skill_synonyms"]["ga4"] == "google analytics"
    assert updated["skill_synonyms_runtime"]["has_run_overlay"] is True
    assert updated["skill_synonyms_runtime"]["run_overlay_filename"] == "reviewed-skill-synonyms.yaml"
    assert updated["skill_synonyms_runtime"]["run_overlay_entry_count"] == 2


# ── Task 1: nested preset-based cv config ─────────────────────────────────────


def test_load_config_returns_nested_cv_object() -> None:
    """load_config() must return a nested cv dict."""
    cfg = load_config()
    assert "cv" in cfg
    assert isinstance(cfg["cv"], dict)


def test_load_config_nested_cv_has_preset() -> None:
    cfg = load_config()
    assert "preset" in cfg["cv"]


def test_load_config_nested_cv_generation_has_model_and_prompt_version() -> None:
    """@proves settings_system.cv-generation-settings"""
    cfg = load_config()
    assert "generation" in cfg["cv"]
    assert "model" in cfg["cv"]["generation"]
    assert "prompt_version" in cfg["cv"]["generation"]


def test_load_config_nested_cv_validation_has_max_pages() -> None:
    cfg = load_config()
    assert "validation" in cfg["cv"]
    assert "max_pages" in cfg["cv"]["validation"]


def test_load_config_nested_cv_composition_has_sections() -> None:
    cfg = load_config()
    assert "composition" in cfg["cv"]
    assert isinstance(cfg["cv"]["composition"], dict)


def test_load_config_compatibility_projection_cv_generation_model() -> None:
    """Legacy flat key must still be projected during the migration window."""
    cfg = load_config()
    # Compatibility projection: flat key must be present for control-plane compatibility
    assert "cv_generation_model" in cfg
    # And must match the nested value
    assert cfg["cv_generation_model"] == cfg["cv"]["generation"]["model"]


def test_load_config_compatibility_projection_cv_max_pages() -> None:
    cfg = load_config()
    assert "cv_max_pages" in cfg
    assert cfg["cv_max_pages"] == cfg["cv"]["validation"]["max_pages"]


def test_load_config_compatibility_projection_prompt_version() -> None:
    cfg = load_config()
    assert "prompt_version" in cfg
    assert cfg["prompt_version"] == cfg["cv"]["generation"]["prompt_version"]


def test_load_config_compatibility_projection_required_cv_sections() -> None:
    cfg = load_config()
    assert "required_cv_sections" in cfg
    # required_cv_sections is derived from enabled composition sections
    assert isinstance(cfg["required_cv_sections"], list)
    assert len(cfg["required_cv_sections"]) > 0


def test_load_config_nested_cv_validation_max_pages_positive(tmp_path: Path) -> None:
    """@proves settings_system.warning-only-cv-max-pages-validation-setting

    max_pages in the nested validation block must be a positive integer.
    """
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\nbigquery_dataset: ds\nservice_account_key: /dev/null\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: true\n"
        "    experience:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 0\n"
    )
    with pytest.raises(ValueError, match="max_pages"):
        load_config(env_yaml)


# ── Task 2: preset registry ─────────────────────────────────────────────────────

def test_cv_presets_module_exists() -> None:
    """cv_presets.py must exist and define the preset registry."""
    from fitcv import cv_presets
    assert hasattr(cv_presets, "PRESET_REGISTRY")
    assert hasattr(cv_presets, "SUPPORTED_PRESETS")


def test_europass_is_a_supported_preset() -> None:
    from fitcv import cv_presets
    assert "europass" in cv_presets.SUPPORTED_PRESETS


def test_preset_registry_has_sections_for_europass() -> None:
    from fitcv import cv_presets
    europass = cv_presets.PRESET_REGISTRY["europass"]
    assert "sections" in europass
    sections = europass["sections"]
    expected = {"summary", "education", "experience", "skills", "certifications", "projects", "publications", "languages"}
    assert set(sections.keys()) >= expected


def test_preset_registry_defines_section_ordering() -> None:
    from fitcv import cv_presets
    europass = cv_presets.PRESET_REGISTRY["europass"]
    assert "section_order" in europass
    assert europass["section_order"][0] == "summary"


def test_preset_registry_defines_allowed_enum_values() -> None:
    from fitcv import cv_presets
    europass = cv_presets.PRESET_REGISTRY["europass"]
    assert "allowed_values" in europass
    allowed = europass["allowed_values"]
    # summary styles
    assert "summary" in allowed
    assert "concise" in allowed["summary"].get("style", [])
    # detail levels
    assert "compact" in allowed.get("detail", [])
    assert "standard" in allowed.get("detail", [])
    assert "detailed" in allowed.get("detail", [])


def test_preset_registry_maps_template_path_for_europass() -> None:
    from fitcv import cv_presets
    europass = cv_presets.PRESET_REGISTRY["europass"]
    assert "template_path" in europass
    assert isinstance(europass["template_path"], str)
    assert europass["template_path"] == "templates/cv_template.md"


def test_get_section_order_returns_europass_order() -> None:
    from fitcv import cv_presets
    order = cv_presets.get_section_order("europass")
    assert order[0] == "summary"
    assert "experience" in order


def test_validate_composition_rejects_unknown_section() -> None:
    from fitcv import cv_presets
    bad_composition = {"unknown_section": {"enabled": True}}
    result = cv_presets.validate_composition("europass", bad_composition)
    assert result["valid"] is False
    assert any("unknown_section" in err for err in result["errors"])


def test_validate_composition_accepts_valid_europass() -> None:
    """@proves settings_system.cv-composition-visibility-settings"""
    from fitcv import cv_presets
    valid_composition = {
        "summary": {"enabled": True, "style": "concise"},
        "experience": {"enabled": True, "detail": "standard"},
    }
    result = cv_presets.validate_composition("europass", valid_composition)
    assert result["valid"] is True


def test_validate_composition_rejects_bad_enum_value() -> None:
    from fitcv import cv_presets
    bad_enum = {
        "summary": {"enabled": True, "style": "invalid_style"},
    }
    result = cv_presets.validate_composition("europass", bad_enum)
    assert result["valid"] is False
    assert any("invalid_style" in err for err in result["errors"])


def test_validate_composition_rejects_unknown_preset() -> None:
    from fitcv import cv_presets
    result = cv_presets.validate_composition("unknown_preset", {"summary": {"enabled": True}})
    assert result["valid"] is False
    assert any("unknown_preset" in err for err in result["errors"])


# ── Task 6: compatibility shim guard ───────────────────────────────────────────

def test_load_config_compatibility_flat_keys_work_after_nested_migration() -> None:
    """@proves settings_system.baseline-default-hydration"""
    cfg = load_config()
    # These are the keys the control plane (settings_schema) still reads
    assert cfg["cv_generation_model"] == cfg["cv"]["generation"]["model"]
    assert cfg["prompt_version"] == cfg["cv"]["generation"]["prompt_version"]
    assert cfg["cv_max_pages"] == cfg["cv"]["validation"]["max_pages"]
    assert isinstance(cfg["required_cv_sections"], list)
    # required_cv_sections is derived from composition
    assert len(cfg["required_cv_sections"]) > 0


def test_load_config_compatibility_required_cv_sections_from_composition() -> None:
    """required_cv_sections is derived from enabled composition sections."""
    cfg = load_config()
    # projects is enabled in cv.yaml, so it should appear in required_cv_sections
    assert "Projects" in cfg["required_cv_sections"]
    # publications has enabled:false, so it should NOT appear
    assert "Publications" not in cfg["required_cv_sections"]


def test_required_cv_sections_includes_education_when_enabled() -> None:
    """Education appears in required_cv_sections when enabled:true."""
    cfg = load_config()
    assert "Education" in cfg["required_cv_sections"]


def test_required_cv_sections_includes_summary_when_enabled() -> None:
    cfg = load_config()
    assert "Summary" in cfg["required_cv_sections"]


def test_required_cv_sections_excludes_education_when_disabled(tmp_path: Path) -> None:
    """Education must NOT appear in required_cv_sections when enabled:false."""
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\nbigquery_dataset: ds\nservice_account_key: /dev/null\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    education:\n"
        "      enabled: false\n"
        "    experience:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )
    cfg = load_config(env_yaml)
    assert "Education" not in cfg["required_cv_sections"]
    assert "Experience" in cfg["required_cv_sections"]


def test_required_cv_sections_excludes_summary_when_disabled(tmp_path: Path) -> None:
    env_yaml = tmp_path / ".env.yaml"
    env_yaml.write_text(
        "gcp_project: test\nbigquery_dataset: ds\nservice_account_key: /dev/null\n"
    )
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "cv.yaml").write_text(
        "cv:\n"
        "  preset: europass\n"
        "  generation:\n"
        "    model: gemini-2.5-flash\n"
        "    prompt_version: v1\n"
        "  composition:\n"
        "    summary:\n"
        "      enabled: false\n"
        "      style: concise\n"
        "    experience:\n"
        "      enabled: true\n"
        "  content_rules:\n"
        "    evidence_grounded_only: true\n"
        "  validation:\n"
        "    max_pages: 2\n"
    )
    cfg = load_config(env_yaml)
    assert "Summary" not in cfg["required_cv_sections"]
    assert "Experience" in cfg["required_cv_sections"]


def test_load_config_adds_default_enrich_prompt_id() -> None:
    cfg = load_config()

    assert cfg["prompts"]["enrich"]["extraction"]["prompt_id"] == "enrich.extraction.v1"


def test_load_config_adds_default_ranking_and_cv_generation_prompt_ids() -> None:
    cfg = load_config()

    assert cfg["prompts"]["ranking"]["ai_score"]["prompt_id"] == "ranking.ai_score.v1"
    assert cfg["prompts"]["cv_generation"]["structured_write"]["prompt_id"] == "cv_generation.structured_write.v1"


def test_load_config_builds_prompts_runtime_for_all_major_stages() -> None:
    """@proves cv_system.config-owned-generation-contract"""
    cfg = load_config()

    assert cfg["prompts_runtime"]["enrich"]["extraction"]["prompt_id"] == "enrich.extraction.v1"
    assert cfg["prompts_runtime"]["ranking"]["ai_score"]["prompt_id"] == "ranking.ai_score.v1"
    assert cfg["prompts_runtime"]["cv_generation"]["structured_write"]["prompt_id"] == "cv_generation.structured_write.v1"


def test_config_accessors_resolve_centralized_prompt_ids_and_model_defaults() -> None:
    """@proves pipeline_performance.enrich-extraction-prompt-text-now-comes-from-a-centralized-prompt-registry-with-config-selected-prompt-ids"""
    cfg = load_config()

    assert get_gemini_model(cfg) == "gemini-2.5-flash"
    assert get_ranking_prompt_id(cfg) == "ranking.ai_score.v1"
    assert get_cv_generation_structured_prompt_id(cfg) == "cv_generation.structured_write.v1"


def test_load_config_exposes_only_active_cv_generation_prompt_contract() -> None:
    """@proves cv_system.config-owned-generation-contract"""
    cfg = load_config()

    assert "write" not in cfg["prompts"]["cv_generation"]
    assert "write" not in cfg["prompts_runtime"]["cv_generation"]


def test_load_config_rejects_unknown_enrich_prompt_id() -> None:
    tmp_path = Path(".worktrees/Stage-by-stage-flow/tests") / f"tmp_prompt_config_{uuid.uuid4().hex}"
    tmp_path.mkdir(parents=True, exist_ok=False)
    try:
        env_yaml = tmp_path / ".env.yaml"
        env_yaml.write_text(
            "gcp_project: test\nbigquery_dataset: ds\nservice_account_key: /dev/null\n"
        )
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        (cfg_dir / "cv.yaml").write_text(
            "cv:\n"
            "  preset: europass\n"
            "  generation:\n"
            "    model: gemini-2.5-flash\n"
            "    prompt_version: v1\n"
            "  composition:\n"
            "    summary:\n"
            "      enabled: true\n"
            "      style: concise\n"
            "    experience:\n"
            "      enabled: true\n"
            "  content_rules:\n"
            "    evidence_grounded_only: true\n"
            "  validation:\n"
            "    max_pages: 2\n"
        )
        (cfg_dir / "pipeline.yaml").write_text(
            "prompts:\n"
            "  enrich:\n"
            "    extraction:\n"
            "      prompt_id: enrich.extraction.v999\n"
        )

        with pytest.raises(ValueError, match="Unknown enrich prompt_id"):
            load_config(env_yaml)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)
