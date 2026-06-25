"""@meta
name: config
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.config.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import logging
import os
import re
import warnings
from pathlib import Path
from typing import Any

import yaml
from fitcv import config_compat, config_loader, config_validators
from fitcv.cv_presets import SUPPORTED_PRESETS
from fitcv.prompts import get_prompt_definition

logger = logging.getLogger(__name__)

_REQUIRED_BIGQUERY_BRIDGE_KEYS = [
    "gcp_project",
    "bigquery_dataset",
    "service_account_key",
]

# Optional — only needed when using the Apify API source
_APIFY_KEYS = ["apify_dataset_id", "apify_token"]

# Policy YAML files merged into the base config (relative to config/).
# The new subfolder layout is canonical; flat legacy filenames are temporary
# fallbacks for migration compatibility.
_POLICY_FILE_CANDIDATES = [
    ("skill_synonyms", ("taxonomy/skill_synonyms.yaml", "skill_synonyms.yaml")),
    (
        "domain_synonyms",
        ("taxonomy/domain_synonyms.yaml", "domain_synonyms.yaml"),
    ),
    (
        "role_family_synonyms",
        ("taxonomy/role_family_synonyms.yaml", "role_family_synonyms.yaml"),
    ),
    ("taxonomy", ("taxonomy/taxonomy.yaml", "taxonomy.yaml")),
    ("shortlist_lexical", ("shortlist_lexical.yaml",)),
    ("pipeline", ("runtime/pipeline.yaml", "pipeline.yaml")),
    ("ranking", ("policy/ranking.yaml", "ranking.yaml")),
    ("cv_analysis", ("policy/cv_analysis.yaml", "cv_analysis.yaml")),
    ("prompts", ("runtime/prompts.yaml", "prompts.yaml")),
    ("cv", ("policy/cv.yaml", "cv.yaml")),
]

_DEFAULT_ENV_CANDIDATES = (".env.yaml", "config/env.yaml")
_DEFAULT_CONTROL_PLANE_CONFIG_PATH = Path("config/runtime/control_plane.yaml")
_RETIRED_CONFIG_SURFACES = (
    "live_smoke.yaml",
)
_DEFAULT_ENRICH_PROMPT_ID = "enrich.extraction.v1"
_DEFAULT_RANKING_AI_SCORE_PROMPT_ID = "ranking.ai_score.v1"
_DEFAULT_CV_GENERATION_STRUCTURED_WRITE_PROMPT_ID = "cv_generation.structured_write.v1"
_DEFAULT_CV_REQUIRED_MATCH_POLICY = {
    "required_match": {
        "min_ratio_by_fit": {
            "strong": 0.8,
            "stretch": 0.5,
        },
        "max_missing_by_fit": {
            "strong": 0,
            "stretch": 1,
        },
    },
    "force_review_when_any_required_missing_for_fits": ["stretch"],
}
_INFRA_ENV_OVERRIDES = {
    "gcp_project": "GCP_PROJECT",
    "bigquery_dataset": "BIGQUERY_DATASET",
    "service_account_key": "GOOGLE_APPLICATION_CREDENTIALS",
}
_CONTROL_PLANE_ENV_OVERRIDES = {
    "FITCV_CP_DATA_BACKEND": ("data_backend", "type"),
    # Portability override: local Windows host often needs localhost, while containers may use host.docker.internal.
    "FITCV_CP_OPENAI_COMPATIBLE_BASE_URL": ("providers", "openai_compatible", "base_url"),
    "FITCV_CP_OPENAI_COMPATIBLE_WIRE_API": ("providers", "openai_compatible", "wire_api"),
}
_CANONICAL_INFRA_KEYS = {
    "location",
    "vertex_location",
}
_CANONICAL_PIPELINE_TOP_LEVEL_KEYS = {
    "gemini_model",
    "embedding_model",
    "enrichment_version",
    "enrichment_sleep_secs",
    "enrichment_max_retries",
    "embedding_batch_size",
    "run_lifecycle",
    "outbox_replay_health",
    "vector_top_n",
    "vector_max_candidate_skills",
    "retrieval_strategy",
    "rerank_top_n",
    "rerank_sleep_secs",
    "pipeline",
}
_CANONICAL_POLICY_TOP_LEVEL_KEYS = {
    "cv",
}
_CANONICAL_TAXONOMY_TOP_LEVEL_KEYS = {
    "seniority",
    "valid_location_types",
    "valid_seniority_enrich",
    "valid_contract_types",
    "valid_experience_levels",
    "role_taxonomy",
}
# Legacy keys are still accepted during transition, but should not be used as
# canonical owners in new config edits.
_LEGACY_COMPATIBILITY_KEYS = {
    "seniority_ladder",
    "application_statuses",
}
_SSOT_ENFORCEMENT_ENV = "FITCV_CONFIG_SSOT_MODE"
_CONTROL_PLANE_FORBIDDEN_KEY_TOKENS = (
    "secret",
    "token",
    "password",
    "credential",
    "api_key",
    "key_env",
)
_DEFAULT_CV_ACCEPTANCE_POLICY: dict[str, Any] = {
    "enabled": False,
    "enforce_for_fit_labels": ["stretch"],
    "min_required_match_score": 0.50,
    "min_required_skill_support_ratio": 0.50,
    "downgrade_status": "review_required",
    "review_reason_code": "policy_acceptance",
}
CV_STRUCTURED_SECTION_KEYS = (
    "header",
    "summary",
    "experience",
    "projects",
    "education",
    "skills",
    "certifications",
    "publications",
    "languages",
)
CV_SECTION_KEY_TO_NAME = {
    "header": "Header",
    "summary": "Summary",
    "education": "Education",
    "experience": "Experience",
    "skills": "Skills",
    "certifications": "Certifications",
    "projects": "Projects",
    "publications": "Publications",
    "languages": "Languages",
}
CV_SECTION_NAME_TO_KEY = {
    display_name.lower(): section_key
    for section_key, display_name in CV_SECTION_KEY_TO_NAME.items()
}


def _load_yaml_file(path: Path) -> dict[str, Any]:
    return config_loader.load_yaml_file(path, logger=logger)

def _iter_nested_mapping_keys(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        flattened: list[str] = []
        for key, value in payload.items():
            flattened.append(str(key))
            flattened.extend(_iter_nested_mapping_keys(value))
        return flattened
    if isinstance(payload, list):
        flattened: list[str] = []
        for item in payload:
            flattened.extend(_iter_nested_mapping_keys(item))
        return flattened
    return []

def _validate_control_plane_secret_hygiene(control_plane: dict[str, Any]) -> None:
    violating_keys = [
        key_name
        for key_name in _iter_nested_mapping_keys(control_plane)
        if any(token in key_name.strip().lower() for token in _CONTROL_PLANE_FORBIDDEN_KEY_TOKENS)
    ]
    if violating_keys:
        sample = ", ".join(sorted(set(violating_keys))[:5])
        raise ValueError(
            "control_plane config contains forbidden secret-oriented key names: "
            f"{sample}"
        )

def _apply_control_plane_env_overrides(control_plane: dict[str, Any]) -> dict[str, Any]:
    updated = dict(control_plane)
    for env_var, key_path in _CONTROL_PLANE_ENV_OVERRIDES.items():
        env_value = str(os.environ.get(env_var, "")).strip()
        if not env_value:
            continue
        cursor: dict[str, Any] = updated
        for key in key_path[:-1]:
            nested = cursor.get(key)
            if not isinstance(nested, dict):
                nested = {}
                cursor[key] = nested
            cursor = nested
        cursor[key_path[-1]] = env_value
    return updated

def resolve_data_backend(config: dict[str, Any] | None = None) -> str:
    """Resolve active persistence backend from canonical control-plane settings.

    Precedence:
    1. explicit FITCV_CP_DATA_BACKEND env override
    2. passed config["control_plane"]["data_backend"]["type"]
    3. passed config["data_backend"]["type"]
    4. load_control_plane_config() when available
    5. default to bigquery for backward compatibility
    """
    env_backend = str(os.environ.get("FITCV_CP_DATA_BACKEND", "")).strip().lower()
    if env_backend:
        if env_backend not in {"bigquery", "sqlite"}:
            raise ValueError("FITCV_CP_DATA_BACKEND must be one of: bigquery, sqlite")
        return env_backend

    cfg = config or {}
    nested_control_plane = dict(cfg.get("control_plane") or {})
    nested_backend = dict(nested_control_plane.get("data_backend") or {})
    direct_backend = dict(cfg.get("data_backend") or {})
    backend = str(
        nested_backend.get("type")
        or direct_backend.get("type")
        or ""
    ).strip().lower()
    if backend:
        if backend not in {"bigquery", "sqlite"}:
            raise ValueError("data_backend.type must be one of: bigquery, sqlite")
        return backend

    try:
        control_plane_cfg = load_control_plane_config()
    except FileNotFoundError:
        control_plane_cfg = {}
    backend_from_control_plane = str(
        (control_plane_cfg.get("data_backend") or {}).get("type") or ""
    ).strip().lower()
    if backend_from_control_plane:
        if backend_from_control_plane not in {"bigquery", "sqlite"}:
            raise ValueError("control_plane.data_backend.type must be one of: bigquery, sqlite")
        return backend_from_control_plane

    return "bigquery"



def sqlite_mode_enabled(config: dict[str, Any] | None = None) -> bool:
    """Return True when sqlite persistence mode is active."""
    return resolve_data_backend(config) == "sqlite"



def load_control_plane_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load control-plane runtime config with env overrides and hygiene checks."""
    config_path = Path(path) if path is not None else _DEFAULT_CONTROL_PLANE_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"Control-plane config file not found: {config_path}")
    payload = _load_yaml_file(config_path)
    control_plane = dict(payload.get("control_plane") or {})
    control_plane = _apply_control_plane_env_overrides(control_plane)
    _validate_control_plane_secret_hygiene(control_plane)

    data_backend = dict(control_plane.get("data_backend") or {})
    backend_type = str(data_backend.get("type") or "bigquery").strip().lower() or "bigquery"
    if backend_type not in {"bigquery", "sqlite"}:
        raise ValueError(
            "control_plane.data_backend.type must be one of: bigquery, sqlite"
        )
    data_backend["type"] = backend_type
    control_plane["data_backend"] = data_backend
    control_plane["providers"] = dict(control_plane.get("providers") or {})
    model_routing = dict(control_plane.get("model_routing") or {})
    model_routing["parts"] = dict(model_routing.get("parts") or {})
    control_plane["model_routing"] = model_routing
    control_plane["observability"] = dict(control_plane.get("observability") or {})
    control_plane["feature_flags"] = dict(control_plane.get("feature_flags") or {})
    return control_plane

def resolve_model_routing_part(
    part_name: str,
    *,
    model_fallback: str = "",
) -> dict[str, str]:
    """Resolve provider/model/base_url for a model-routing part.

    Secrets are intentionally not read from config. API keys remain env-only.
    """
    cp_cfg = load_control_plane_config()
    model_routing = dict(cp_cfg.get("model_routing") or {})
    parts = dict(model_routing.get("parts") or {})
    part_cfg = dict(parts.get(part_name) or {})
    providers = dict(cp_cfg.get("providers") or {})
    provider_name = str(part_cfg.get("provider") or "").strip().lower()
    provider_cfg = dict(providers.get(provider_name) or {})
    model_name = str(part_cfg.get("model") or "").strip() or str(model_fallback or "").strip()
    base_url = str(provider_cfg.get("base_url") or "").strip()
    wire_api = str(provider_cfg.get("wire_api") or "").strip()
    timeout_seconds = str(provider_cfg.get("timeout_seconds") or "").strip()
    return {
        "provider": provider_name,
        "model": model_name,
        "base_url": base_url,
        "wire_api": wire_api,
        "timeout_seconds": timeout_seconds,
    }

def resolve_langgraph_runtime_expectation(
    *,
    part_name: str = "cv_generation_structured_write",
) -> dict[str, str]:
    """Resolve provider/model/base_url/wire_api with env-override then control-plane fallback."""
    routed = resolve_model_routing_part(part_name)
    provider = str(routed.get("provider") or "").strip().lower()
    model = str(routed.get("model") or "").strip()
    base_url = str(routed.get("base_url") or "").strip()
    wire_api = str(routed.get("wire_api") or "").strip()
    source = "control_plane"

    env_provider = str(os.environ.get("FITCV_LANGGRAPH_PROVIDER") or "").strip().lower()
    env_model = str(os.environ.get("FITCV_LANGGRAPH_MODEL") or "").strip()
    env_base_url = str(os.environ.get("FITCV_LANGGRAPH_OPENAI_BASE_URL") or "").strip()
    env_wire_api = str(os.environ.get("FITCV_LANGGRAPH_WIRE_API") or "").strip()
    if any((env_provider, env_model, env_base_url, env_wire_api)):
        if env_provider:
            provider = env_provider
        if env_model:
            model = env_model
        if env_base_url:
            base_url = env_base_url
        if env_wire_api:
            wire_api = env_wire_api
        source = "env_override"

    missing: list[str] = []
    if not provider:
        missing.append("provider")
    if not model:
        missing.append("model")
    if not base_url:
        missing.append("base_url")
    if not wire_api:
        missing.append("wire_api")
    if missing:
        raise ValueError(
            "Missing resolved LangGraph runtime routing fields: "
            + ", ".join(missing)
        )

    return {
        "provider": provider,
        "model": model,
        "base_url": base_url,
        "wire_api": wire_api,
        "source": source,
    }


def _load_policy_file(config_dir: Path, rel_paths: tuple[str, ...]) -> tuple[dict[str, Any], Path]:
    return config_loader.load_policy_file(
        config_dir,
        rel_paths,
        load_yaml_file_fn=_load_yaml_file,
        logger=logger,
    )


def _find_config_dir(base_path: Path) -> Path:
    return config_loader.find_config_dir(base_path)


def _resolve_env_path(path: str | Path | None) -> Path:
    return config_loader.resolve_env_path(path, default_env_candidates=_DEFAULT_ENV_CANDIDATES)


def _is_legacy_env_path(path: Path) -> bool:
    return config_loader.is_legacy_env_path(path)


def _merge_missing_keys(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    return config_loader.merge_missing_keys(base, extra)

def _detect_pipeline_ssot_overlap(
    env_cfg: dict[str, Any],
    pipeline_policy_cfg: dict[str, Any],
) -> list[str]:
    return config_validators.detect_pipeline_ssot_overlap(env_cfg, pipeline_policy_cfg)

def _detect_env_canonical_ownership_overlaps(
    env_cfg: dict[str, Any],
) -> list[str]:
    return config_validators.detect_env_canonical_ownership_overlaps(
        env_cfg,
        canonical_infra_keys=_CANONICAL_INFRA_KEYS,
        canonical_policy_top_level_keys=_CANONICAL_POLICY_TOP_LEVEL_KEYS,
        canonical_pipeline_top_level_keys=_CANONICAL_PIPELINE_TOP_LEVEL_KEYS,
        canonical_taxonomy_top_level_keys=_CANONICAL_TAXONOMY_TOP_LEVEL_KEYS,
        legacy_compatibility_keys=_LEGACY_COMPATIBILITY_KEYS,
    )

def _detect_legacy_compatibility_keys(env_cfg: dict[str, Any]) -> list[str]:
    return sorted(key for key in env_cfg.keys() if key in _LEGACY_COMPATIBILITY_KEYS)

def _resolve_ssot_enforcement_mode(cfg: dict[str, Any] | None = None) -> str:
    env_mode = str(os.environ.get(_SSOT_ENFORCEMENT_ENV, "")).strip().lower()
    mode = env_mode or str((cfg or {}).get("ssot_enforcement_mode", "warn")).strip().lower() or "warn"
    if mode not in {"warn", "strict"}:
        raise ValueError("SSOT enforcement mode must be one of: warn, strict")
    return mode

def _handle_ssot_overlaps(*, mode: str, overlap_label: str, overlaps: list[str]) -> None:
    if not overlaps:
        return
    if mode == "strict":
        raise ValueError(f"{overlap_label}: {', '.join(overlaps)}")
    logger.warning("%s: %s", overlap_label, ", ".join(overlaps))

def _apply_legacy_env_compatibility_projection(cfg: dict[str, Any]) -> dict[str, Any]:
    return config_compat.apply_legacy_env_compatibility_projection(cfg)


def _normalize_skill_synonyms(raw_synonyms: Any) -> dict[str, str]:
    if not isinstance(raw_synonyms, dict):
        return {}
    return {
        str(alias).strip().lower(): str(canonical).strip().lower()
        for alias, canonical in raw_synonyms.items()
        if str(alias).strip() and str(canonical).strip()
    }

def _normalize_alias_map(raw_map: Any) -> dict[str, str]:
    if not isinstance(raw_map, dict):
        return {}
    normalized: dict[str, str] = {}
    for alias, canonical in raw_map.items():
        alias_normalized = _normalize_role_text(alias)
        canonical_normalized = _normalize_role_text(canonical)
        if not alias_normalized or not canonical_normalized:
            continue
        normalized[alias_normalized] = canonical_normalized
    return normalized

def _normalize_neighbor_map(raw_map: Any) -> dict[str, tuple[str, ...]]:
    if not isinstance(raw_map, dict):
        return {}
    normalized: dict[str, tuple[str, ...]] = {}
    for key, values in raw_map.items():
        normalized_key = _normalize_role_text(key)
        if not normalized_key or not isinstance(values, (list, tuple)):
            continue
        normalized_values = tuple(
            candidate
            for value in values
            if (candidate := _normalize_role_text(value))
        )
        if normalized_values:
            normalized[normalized_key] = normalized_values
    return normalized


def _normalize_role_text(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9_]+", " ", str(value).lower())).strip()


def _normalize_role_taxonomy(raw_taxonomy: Any) -> dict[str, Any]:
    if not isinstance(raw_taxonomy, dict):
        return {}

    canonical_role_by_alias: dict[str, str] = {}
    raw_canonical_roles = raw_taxonomy.get("canonical_roles")
    if isinstance(raw_canonical_roles, dict):
        for canonical_role, canonical_payload in raw_canonical_roles.items():
            normalized_canonical = _normalize_role_text(canonical_role)
            if not normalized_canonical:
                continue
            canonical_role_by_alias[normalized_canonical] = normalized_canonical
            aliases: list[Any] = []
            if isinstance(canonical_payload, dict):
                aliases = canonical_payload.get("aliases") or []
            for alias in aliases:
                normalized_alias = _normalize_role_text(alias)
                if normalized_alias:
                    canonical_role_by_alias[normalized_alias] = normalized_canonical

    role_family_by_role: dict[str, str] = {}
    raw_role_families = raw_taxonomy.get("role_families")
    if isinstance(raw_role_families, dict):
        for family_name, family_payload in raw_role_families.items():
            normalized_family = _normalize_role_text(family_name)
            if not normalized_family or not isinstance(family_payload, dict):
                continue
            for role in family_payload.get("roles") or []:
                normalized_role = _normalize_role_text(role)
                if not normalized_role:
                    continue
                canonical_role = canonical_role_by_alias.get(normalized_role, normalized_role)
                role_family_by_role[canonical_role] = normalized_family

    role_family_neighbors: dict[str, tuple[str, ...]] = {}
    raw_neighbors = raw_taxonomy.get("role_family_neighbors")
    if isinstance(raw_neighbors, dict):
        for family_name, neighbors in raw_neighbors.items():
            normalized_family = _normalize_role_text(family_name)
            if not normalized_family or not isinstance(neighbors, list):
                continue
            normalized_neighbors = tuple(
                normalized_neighbor
                for neighbor in neighbors
                if (normalized_neighbor := _normalize_role_text(neighbor))
            )
            role_family_neighbors[normalized_family] = normalized_neighbors

    return {
        "canonical_role_by_alias": canonical_role_by_alias,
        "role_family_by_role": role_family_by_role,
        "role_family_neighbors": role_family_neighbors,
    }



def _normalize_runtime_synonym_overlay_payload(raw_payload: Any) -> dict[str, Any]:
    if not isinstance(raw_payload, dict):
        return {}
    return {
        "skill_synonyms": _normalize_skill_synonyms(raw_payload.get("skill_synonyms")),
        "domain_alias_map": _normalize_alias_map(raw_payload.get("domain_alias_map")),
        "role_family_alias_map": _normalize_alias_map(raw_payload.get("role_family_alias_map")),
        "domain_neighbors": _normalize_neighbor_map(raw_payload.get("domain_neighbors")),
        "role_family_neighbors": _normalize_neighbor_map(raw_payload.get("role_family_neighbors")),
    }

def parse_runtime_synonym_overlay_yaml(raw_yaml: str) -> dict[str, Any]:
    """Parse and validate a run-scoped multi-field synonym overlay YAML payload."""
    try:
        payload = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise ValueError("Synonym overlay must be valid YAML") from exc
    if payload is None:
        raise ValueError("Synonym overlay must define at least one mapping")
    if not isinstance(payload, dict):
        raise ValueError("Synonym overlay must be a mapping")
    supported_keys = {
        "skill_synonyms",
        "domain_alias_map",
        "role_family_alias_map",
        "domain_neighbors",
        "role_family_neighbors",
    }
    if not any(key in payload for key in supported_keys):
        payload = {"skill_synonyms": payload}
    normalized = _normalize_runtime_synonym_overlay_payload(payload)
    if not any(bool(section) for section in normalized.values()):
        raise ValueError("Synonym overlay must define at least one mapping")
    return normalized

def parse_skill_synonym_overlay_yaml(raw_yaml: str) -> dict[str, str]:
    """Backward-compatible parser for skill-only run overlays."""
    try:
        payload = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise ValueError("Synonym overlay must be valid YAML") from exc
    if isinstance(payload, dict):
        candidate = payload.get("skill_synonyms") if "skill_synonyms" in payload else payload
        if isinstance(candidate, dict):
            for alias, canonical in candidate.items():
                if not str(alias).strip() or not str(canonical).strip():
                    raise ValueError("Synonym overlay aliases and canonicals must be non-empty strings")
    return dict(parse_runtime_synonym_overlay_yaml(raw_yaml).get("skill_synonyms") or {})

def apply_runtime_synonym_overlay(
    cfg: dict[str, Any],
    overlay_payload: dict[str, Any],
    *,
    source: str,
    filename: str,
    uploaded_at: str,
    raw_yaml: str | None = None,
) -> dict[str, Any]:
    """Return cfg with a run-scoped multi-field synonym overlay merged in."""
    updated_cfg = dict(cfg)
    normalized_overlay = _normalize_runtime_synonym_overlay_payload(overlay_payload)
    overlay_skill_synonyms = dict(normalized_overlay.get("skill_synonyms") or {})
    overlay_domain_alias_map = dict(normalized_overlay.get("domain_alias_map") or {})
    overlay_role_family_alias_map = dict(normalized_overlay.get("role_family_alias_map") or {})
    overlay_domain_neighbors = dict(normalized_overlay.get("domain_neighbors") or {})
    overlay_role_family_neighbors = dict(normalized_overlay.get("role_family_neighbors") or {})

    runtime = dict(updated_cfg.get("skill_synonyms_runtime") or {})
    base_effective_synonyms = runtime.get("pre_run_overlay_skill_synonyms")
    if not isinstance(base_effective_synonyms, dict):
        base_effective_synonyms = _normalize_skill_synonyms(updated_cfg.get("skill_synonyms"))
    base_effective_synonyms = _normalize_skill_synonyms(base_effective_synonyms)
    merged_synonyms = dict(base_effective_synonyms)
    merged_synonyms.update(overlay_skill_synonyms)

    base_domain_alias_map = _normalize_alias_map(updated_cfg.get("domain_alias_map"))
    merged_domain_alias_map = dict(base_domain_alias_map)
    merged_domain_alias_map.update(overlay_domain_alias_map)
    base_role_family_alias_map = _normalize_alias_map(updated_cfg.get("role_family_alias_map"))
    merged_role_family_alias_map = dict(base_role_family_alias_map)
    merged_role_family_alias_map.update(overlay_role_family_alias_map)
    base_domain_neighbors = _normalize_neighbor_map(updated_cfg.get("domain_neighbors"))
    merged_domain_neighbors = dict(base_domain_neighbors)
    merged_domain_neighbors.update(overlay_domain_neighbors)
    base_role_family_neighbors = _normalize_neighbor_map(updated_cfg.get("role_family_neighbors"))
    merged_role_family_neighbors = dict(base_role_family_neighbors)
    merged_role_family_neighbors.update(overlay_role_family_neighbors)
    merged_role_taxonomy = dict(updated_cfg.get("role_taxonomy") or {})
    merged_nested_role_family_neighbors = _normalize_neighbor_map(merged_role_taxonomy.get("role_family_neighbors"))
    merged_nested_role_family_neighbors.update(overlay_role_family_neighbors)
    merged_role_taxonomy["role_family_neighbors"] = dict(merged_nested_role_family_neighbors)

    runtime["pre_run_overlay_skill_synonyms"] = dict(base_effective_synonyms)
    runtime["has_overlay"] = bool(runtime.get("overlay_paths") or overlay_skill_synonyms)
    runtime["entry_count"] = len(merged_synonyms)
    runtime["has_run_overlay"] = bool(
        overlay_skill_synonyms
        or overlay_domain_alias_map
        or overlay_role_family_alias_map
        or overlay_domain_neighbors
        or overlay_role_family_neighbors
    )
    runtime["run_overlay_source"] = source
    runtime["run_overlay_filename"] = filename
    runtime["run_overlay_uploaded_at"] = uploaded_at
    runtime["run_overlay_entry_count"] = len(overlay_skill_synonyms)
    runtime["run_overlay_section_counts"] = {
        "skill_synonyms": len(overlay_skill_synonyms),
        "domain_alias_map": len(overlay_domain_alias_map),
        "role_family_alias_map": len(overlay_role_family_alias_map),
        "domain_neighbors": len(overlay_domain_neighbors),
        "role_family_neighbors": len(overlay_role_family_neighbors),
    }
    if raw_yaml is not None:
        runtime["run_overlay_yaml"] = str(raw_yaml)
    updated_cfg["skill_synonyms"] = merged_synonyms
    updated_cfg["domain_alias_map"] = merged_domain_alias_map
    updated_cfg["role_family_alias_map"] = merged_role_family_alias_map
    updated_cfg["domain_neighbors"] = merged_domain_neighbors
    updated_cfg["role_family_neighbors"] = merged_role_family_neighbors
    updated_cfg["role_taxonomy"] = merged_role_taxonomy
    updated_cfg["skill_synonyms_runtime"] = runtime
    return updated_cfg

def apply_runtime_skill_synonym_overlay(
    cfg: dict[str, Any],
    overlay_synonyms: dict[str, str],
    *,
    source: str,
    filename: str,
    uploaded_at: str,
    raw_yaml: str | None = None,
) -> dict[str, Any]:
    """Backward-compatible wrapper for skill-only run overlays."""
    return apply_runtime_synonym_overlay(
        cfg,
        {"skill_synonyms": _normalize_skill_synonyms(overlay_synonyms)},
        source=source,
        filename=filename,
        uploaded_at=uploaded_at,
        raw_yaml=raw_yaml,
    )
def _apply_prompt_defaults(cfg: dict[str, Any]) -> dict[str, Any]:
    prompts = cfg.get("prompts")
    if not isinstance(prompts, dict):
        prompts = {}
    enrich_prompt_cfg = prompts.get("enrich")
    if not isinstance(enrich_prompt_cfg, dict):
        enrich_prompt_cfg = {}
    extraction_cfg = enrich_prompt_cfg.get("extraction")
    if not isinstance(extraction_cfg, dict):
        extraction_cfg = {}
    prompt_id = str(extraction_cfg.get("prompt_id") or _DEFAULT_ENRICH_PROMPT_ID).strip()
    extraction_cfg["prompt_id"] = prompt_id or _DEFAULT_ENRICH_PROMPT_ID
    enrich_prompt_cfg["extraction"] = extraction_cfg
    prompts["enrich"] = enrich_prompt_cfg

    ranking_prompt_cfg = prompts.get("ranking")
    if not isinstance(ranking_prompt_cfg, dict):
        ranking_prompt_cfg = {}
    ai_score_cfg = ranking_prompt_cfg.get("ai_score")
    if not isinstance(ai_score_cfg, dict):
        ai_score_cfg = {}
    ranking_prompt_id = str(ai_score_cfg.get("prompt_id") or _DEFAULT_RANKING_AI_SCORE_PROMPT_ID).strip()
    ai_score_cfg["prompt_id"] = ranking_prompt_id or _DEFAULT_RANKING_AI_SCORE_PROMPT_ID
    ranking_prompt_cfg["ai_score"] = ai_score_cfg
    prompts["ranking"] = ranking_prompt_cfg

    cv_generation_prompt_cfg = prompts.get("cv_generation")
    if not isinstance(cv_generation_prompt_cfg, dict):
        cv_generation_prompt_cfg = {}
    structured_write_cfg = cv_generation_prompt_cfg.get("structured_write")
    if not isinstance(structured_write_cfg, dict):
        structured_write_cfg = {}
    structured_write_prompt_id = str(
        structured_write_cfg.get("prompt_id") or _DEFAULT_CV_GENERATION_STRUCTURED_WRITE_PROMPT_ID
    ).strip()
    structured_write_cfg["prompt_id"] = (
        structured_write_prompt_id or _DEFAULT_CV_GENERATION_STRUCTURED_WRITE_PROMPT_ID
    )
    cv_generation_prompt_cfg["structured_write"] = structured_write_cfg
    prompts["cv_generation"] = cv_generation_prompt_cfg
    cfg["prompts"] = prompts
    return cfg


def _build_prompts_runtime(cfg: dict[str, Any]) -> dict[str, Any]:
    enrich_prompt_id = str(
        (((cfg.get("prompts") or {}).get("enrich") or {}).get("extraction") or {}).get("prompt_id")
        or _DEFAULT_ENRICH_PROMPT_ID
    )
    enrich_definition = get_prompt_definition(enrich_prompt_id)
    ranking_prompt_id = str(
        (((cfg.get("prompts") or {}).get("ranking") or {}).get("ai_score") or {}).get("prompt_id")
        or _DEFAULT_RANKING_AI_SCORE_PROMPT_ID
    )
    ranking_definition = get_prompt_definition(ranking_prompt_id)
    cv_generation_structured_prompt_id = str(
        (((cfg.get("prompts") or {}).get("cv_generation") or {}).get("structured_write") or {}).get("prompt_id")
        or _DEFAULT_CV_GENERATION_STRUCTURED_WRITE_PROMPT_ID
    )
    cv_generation_structured_definition = get_prompt_definition(cv_generation_structured_prompt_id)
    return {
        "enrich": {
            "extraction": {
                "prompt_id": enrich_definition.prompt_id,
                "version": enrich_definition.version,
                "template_path": str(enrich_definition.template_path),
                "stage_id": enrich_definition.stage_id,
            }
        },
        "ranking": {
            "ai_score": {
                "prompt_id": ranking_definition.prompt_id,
                "version": ranking_definition.version,
                "template_path": str(ranking_definition.template_path),
                "stage_id": ranking_definition.stage_id,
            }
        },
        "cv_generation": {
            "structured_write": {
                "prompt_id": cv_generation_structured_definition.prompt_id,
                "version": cv_generation_structured_definition.version,
                "template_path": str(cv_generation_structured_definition.template_path),
                "stage_id": cv_generation_structured_definition.stage_id,
            },
        }
    }


def _validate_prompt_config(cfg: dict[str, Any]) -> None:
    enrich_prompt_id = str(
        (((cfg.get("prompts") or {}).get("enrich") or {}).get("extraction") or {}).get("prompt_id")
        or ""
    ).strip()
    if not enrich_prompt_id:
        raise ValueError("prompts.enrich.extraction.prompt_id is required")
    try:
        get_prompt_definition(enrich_prompt_id)
    except KeyError as exc:
        raise ValueError(f"Unknown enrich prompt_id: {enrich_prompt_id}") from exc

    ranking_prompt_id = str(
        (((cfg.get("prompts") or {}).get("ranking") or {}).get("ai_score") or {}).get("prompt_id")
        or ""
    ).strip()
    if not ranking_prompt_id:
        raise ValueError("prompts.ranking.ai_score.prompt_id is required")
    try:
        get_prompt_definition(ranking_prompt_id)
    except KeyError as exc:
        raise ValueError(f"Unknown ranking ai_score prompt_id: {ranking_prompt_id}") from exc

    cv_generation_structured_prompt_id = str(
        (((cfg.get("prompts") or {}).get("cv_generation") or {}).get("structured_write") or {}).get("prompt_id")
        or ""
    ).strip()
    if not cv_generation_structured_prompt_id:
        raise ValueError("prompts.cv_generation.structured_write.prompt_id is required")
    try:
        get_prompt_definition(cv_generation_structured_prompt_id)
    except KeyError as exc:
        raise ValueError(
            f"Unknown cv_generation structured_write prompt_id: {cv_generation_structured_prompt_id}"
        ) from exc


def _resolve_config_relative_path(config_dir: Path, raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return config_dir / path


def _load_skill_synonym_overlays(
    *,
    config_dir: Path,
    overlay_paths: list[str],
) -> tuple[dict[str, str], list[str]]:
    merged: dict[str, str] = {}
    resolved_paths: list[str] = []
    for overlay_path in overlay_paths:
        resolved_path = _resolve_config_relative_path(config_dir, overlay_path)
        overlay_cfg = _load_yaml_file(resolved_path)
        raw_overlay = overlay_cfg.get("skill_synonyms") if "skill_synonyms" in overlay_cfg else overlay_cfg
        overlay_synonyms = _normalize_skill_synonyms(raw_overlay)
        if not overlay_synonyms:
            continue
        merged.update(overlay_synonyms)
        resolved_paths.append(str(resolved_path))
    return merged, resolved_paths


def _normalize_config_keys(cfg: dict[str, Any]) -> dict[str, Any]:
    """Normalize legacy config keys into the canonical runtime shape."""
    if "gemini_model" not in cfg and "ai_score_model" in cfg:
        cfg["gemini_model"] = cfg["ai_score_model"]
    if "vertex_location" not in cfg:
        location = str(cfg.get("location", "")).strip()
        if location and location.lower() != "us":
            cfg["vertex_location"] = location
    pipeline_cfg = dict(cfg.get("pipeline") or {})
    if "vector_search_top_n" not in pipeline_cfg and "vector_top_n" in cfg:
        pipeline_cfg["vector_search_top_n"] = cfg["vector_top_n"]
    if "ai_score_top_n" not in pipeline_cfg and "rerank_top_n" in cfg:
        pipeline_cfg["ai_score_top_n"] = cfg["rerank_top_n"]
    if "final_top_n" not in pipeline_cfg and "final_top_n" in cfg:
        pipeline_cfg["final_top_n"] = cfg["final_top_n"]
    if "evidence_top_k" not in pipeline_cfg and "evidence_top_k" in cfg:
        pipeline_cfg["evidence_top_k"] = cfg["evidence_top_k"]
    if pipeline_cfg:
        cfg["pipeline"] = pipeline_cfg
        if "vector_top_n" not in cfg and "vector_search_top_n" in pipeline_cfg:
            cfg["vector_top_n"] = pipeline_cfg["vector_search_top_n"]
        if "rerank_top_n" not in cfg and "ai_score_top_n" in pipeline_cfg:
            cfg["rerank_top_n"] = pipeline_cfg["ai_score_top_n"]
    return cfg


def _normalize_cv_acceptance_policy_config(cfg: dict[str, Any]) -> dict[str, Any]:
    policy_cfg = dict(cfg.get("cv_acceptance_policy") or {})
    default_required_match = dict(_DEFAULT_CV_REQUIRED_MATCH_POLICY["required_match"])
    required_match_cfg = dict(policy_cfg.get("required_match") or {})

    default_min_ratio = dict(default_required_match["min_ratio_by_fit"])
    min_ratio_cfg = dict(required_match_cfg.get("min_ratio_by_fit") or {})
    normalized_min_ratio = {
        "strong": float(min_ratio_cfg.get("strong", default_min_ratio["strong"])),
        "stretch": float(min_ratio_cfg.get("stretch", default_min_ratio["stretch"])),
    }

    default_max_missing = dict(default_required_match["max_missing_by_fit"])
    max_missing_cfg = dict(required_match_cfg.get("max_missing_by_fit") or {})
    normalized_max_missing = {
        "strong": int(max_missing_cfg.get("strong", default_max_missing["strong"])),
        "stretch": int(max_missing_cfg.get("stretch", default_max_missing["stretch"])),
    }

    force_review_raw = policy_cfg.get("force_review_when_any_required_missing_for_fits") or []
    force_review: list[str] = []
    if isinstance(force_review_raw, list):
        for fit in force_review_raw:
            fit_name = str(fit).strip().lower()
            if fit_name in {"strong", "stretch"} and fit_name not in force_review:
                force_review.append(fit_name)

    normalized_policy = {
        "required_match": {
            "min_ratio_by_fit": normalized_min_ratio,
            "max_missing_by_fit": normalized_max_missing,
        },
        "force_review_when_any_required_missing_for_fits": force_review,
    }
    cfg["cv_acceptance_policy"] = normalized_policy
    cfg["cv_acceptance_policy_runtime"] = normalized_policy
    return cfg


def _apply_infra_env_overrides(cfg: dict[str, Any]) -> dict[str, Any]:
    """Prefer standard environment variables for portable runtime configuration."""
    for cfg_key, env_key in _INFRA_ENV_OVERRIDES.items():
        env_value = os.environ.get(env_key, "").strip()
        if env_value:
            cfg[cfg_key] = env_value
    return cfg

def _strip_legacy_bigquery_bridge_keys_for_sqlite(cfg: dict[str, Any]) -> dict[str, Any]:
    return config_compat.strip_legacy_bigquery_bridge_keys_for_sqlite(
        cfg,
        required_bigquery_bridge_keys=_REQUIRED_BIGQUERY_BRIDGE_KEYS,
    )


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load and validate config from .env.yaml, then merge policy YAML files.

    Args:
        path: Path to the .env.yaml config file.

    Returns:
        Merged config dict. Policy file keys are added alongside .env.yaml keys.
        .env.yaml keys always win on collision.

    Raises:
        FileNotFoundError: If .env.yaml does not exist.
        ValueError: If required infrastructure keys are missing from .env.yaml.
    """
    # Precedence for env-source selection:
    # 1) explicit caller path
    # 2) default env candidates in _DEFAULT_ENV_CANDIDATES
    env_path = _resolve_env_path(path)
    if not env_path.exists():
        raise FileNotFoundError(f"Config file not found: {env_path}")
    if _is_legacy_env_path(env_path):
        warnings.warn(
            f"legacy config path in use: {env_path}",
            UserWarning,
            stacklevel=2,
        )

    try:
        with open(env_path, encoding="utf-8") as f:
            cfg: dict[str, Any] = yaml.safe_load(f) or {}
    except PermissionError:
        # On some hosts the repo-root `.env.yaml` may exist but be unreadable due to ACLs.
        # Fall back to legacy `config/env.yaml` when present to keep local tooling/tests usable.
        fallback_path = Path("config") / "env.yaml"
        if env_path.name == ".env.yaml" and fallback_path.exists():
            with open(fallback_path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            env_path = fallback_path
        else:
            raise

    resolved_env_path = env_path.resolve()
    config_dir = _find_config_dir(resolved_env_path)
    for retired_name in _RETIRED_CONFIG_SURFACES:
        retired_path = config_dir / retired_name
        if retired_path.exists():
            logger.warning(
                "Retired config surface detected and ignored: %s. "
                "Use canonical runtime/control-plane config owners instead.",
                retired_path,
            )
    if env_path.name == ".env.yaml":
        legacy_env_path = config_dir / "env.yaml"
        if legacy_env_path.exists():
            cfg = _merge_missing_keys(cfg, _load_yaml_file(legacy_env_path))
    elif _is_legacy_env_path(env_path):
        root_env_path = config_dir.parent / ".env.yaml"
        if root_env_path.exists():
            cfg = _merge_missing_keys(cfg, _load_yaml_file(root_env_path))

    cfg = _normalize_config_keys(cfg)
    cfg = _apply_infra_env_overrides(cfg)
    env_cfg_snapshot = dict(cfg)
    ssot_mode = _resolve_ssot_enforcement_mode(cfg)
    env_ownership_overlaps = _detect_env_canonical_ownership_overlaps(env_cfg_snapshot)
    _handle_ssot_overlaps(
        mode=ssot_mode,
        overlap_label="Config SSOT ownership overlap detected in env config",
        overlaps=env_ownership_overlaps,
    )
    legacy_keys = _detect_legacy_compatibility_keys(env_cfg_snapshot)
    if legacy_keys:
        logger.warning(
            "Legacy compatibility keys detected in env config: %s. "
            "Move ownership to canonical runtime/policy/taxonomy files before deprecation window closes.",
            ", ".join(legacy_keys),
        )
    cfg = _apply_legacy_env_compatibility_projection(cfg)

    backend = resolve_data_backend(cfg)
    if backend == "sqlite":
        cfg = _strip_legacy_bigquery_bridge_keys_for_sqlite(cfg)

    required_keys = _REQUIRED_BIGQUERY_BRIDGE_KEYS if backend == "bigquery" else []
    missing = [k for k in required_keys if k not in cfg]
    if missing:
        raise ValueError(
            f"Missing config keys for {backend} backend: {missing}"
        )

    loaded_policy_paths: dict[str, Path] = {}
    pipeline_policy_snapshot: dict[str, Any] = {}

    # Precedence for merge stage:
    # env cfg remains highest compatibility source; policy/runtime/taxonomy files
    # only backfill missing keys during Option B transition window.
    # Merge policy YAML files — later files add keys; .env.yaml keys take priority
    for policy_name, rel_paths in _POLICY_FILE_CANDIDATES:
        policy, resolved_policy_path = _load_policy_file(config_dir, rel_paths)
        if policy_name == "pipeline":
            pipeline_policy_snapshot = dict(policy)
        loaded_policy_paths[policy_name] = resolved_policy_path
        for key, value in policy.items():
            if key not in cfg:  # never overwrite .env.yaml values
                cfg[key] = value

    overlaps = _detect_pipeline_ssot_overlap(env_cfg_snapshot, pipeline_policy_snapshot)
    _handle_ssot_overlaps(
        mode=ssot_mode,
        overlap_label="Config SSOT overlap detected between env config and runtime/pipeline policy",
        overlaps=overlaps,
    )
    cfg = _apply_prompt_defaults(cfg)

    base_skill_synonyms = _normalize_skill_synonyms(cfg.get("skill_synonyms"))
    cfg["domain_alias_map"] = _normalize_alias_map(cfg.get("domain_alias_map"))
    cfg["role_family_alias_map"] = _normalize_alias_map(cfg.get("role_family_alias_map"))
    cfg["domain_neighbors"] = _normalize_neighbor_map(cfg.get("domain_neighbors"))
    cfg["role_family_neighbors"] = _normalize_neighbor_map(cfg.get("role_family_neighbors"))
    overlay_paths_raw = cfg.get("skill_synonyms_overlay_paths") or []
    overlay_paths = [
        str(item).strip()
        for item in overlay_paths_raw
        if str(item).strip()
    ] if isinstance(overlay_paths_raw, list) else []
    overlay_skill_synonyms, resolved_overlay_paths = _load_skill_synonym_overlays(
        config_dir=config_dir,
        overlay_paths=overlay_paths,
    )
    effective_skill_synonyms = dict(base_skill_synonyms)
    effective_skill_synonyms.update(overlay_skill_synonyms)
    cfg["skill_synonyms"] = effective_skill_synonyms
    cfg["role_taxonomy"] = _normalize_role_taxonomy(cfg.get("role_taxonomy"))
    cfg["skill_synonyms_runtime"] = {
        "base_policy_path": str(
            loaded_policy_paths.get(
                "skill_synonyms",
                config_dir / "taxonomy" / "skill_synonyms.yaml",
            )
        ),
        "overlay_paths": resolved_overlay_paths,
        "has_overlay": bool(resolved_overlay_paths),
        "entry_count": len(effective_skill_synonyms),
    }
    _validate_prompt_config(cfg)
    cfg["prompts_runtime"] = _build_prompts_runtime(cfg)

    cfg = _normalize_cv_acceptance_policy_config(cfg)
    _validate_nested_cv_config(cfg)
    cfg = apply_cv_compatibility_projection(cfg)
    return cfg


def _validate_nested_cv_config(cfg: dict[str, Any]) -> None:
    """Validate the nested cv block after policy files are merged.

    Raises ValueError with a descriptive message for any violation.
    """
    if "cv" not in cfg:
        raise ValueError("Missing top-level 'cv' key in config")

    cv_cfg = cfg["cv"]

    if "preset" not in cv_cfg:
        raise ValueError("cv.preset is required")
    preset = str(cv_cfg["preset"])
    if preset not in SUPPORTED_PRESETS:
        raise ValueError(
            f"cv.preset must be one of {sorted(SUPPORTED_PRESETS)}, got: {preset!r}"
        )

    # generation block
    if "generation" not in cv_cfg:
        raise ValueError("cv.generation is required")
    gen = cv_cfg["generation"]
    if "model" not in gen:
        raise ValueError("cv.generation.model is required")
    if "prompt_version" not in gen:
        raise ValueError("cv.generation.prompt_version is required")

    # composition block
    if "composition" not in cv_cfg:
        raise ValueError("cv.composition is required")
    comp = cv_cfg["composition"]
    if not isinstance(comp, dict):
        raise ValueError("cv.composition must be a dict")
    # Validate composition against preset registry
    from fitcv.cv_presets import validate_composition
    comp_result = validate_composition(preset, comp)
    if not comp_result["valid"]:
        raise ValueError(
            f"cv.composition errors for preset '{preset}': {comp_result['errors']}"
        )

    # content_rules was retired from the active pipeline contract.
    # Older config files may still include the block; it is ignored.
    if "content_rules" in cv_cfg and not isinstance(cv_cfg["content_rules"], dict):
        raise ValueError("cv.content_rules must be a dict when provided")
    cv_cfg.pop("content_rules", None)

    # validation block
    if "validation" not in cv_cfg:
        raise ValueError("cv.validation is required")
    val = cv_cfg["validation"]
    if "max_pages" not in val:
        raise ValueError("cv.validation.max_pages is required")
    try:
        max_pages = int(val["max_pages"])
    except (TypeError, ValueError):
        raise ValueError(f"cv.validation.max_pages must be an integer, got: {val['max_pages']!r}")
    if max_pages <= 0:
        raise ValueError(f"cv.validation.max_pages must be a positive integer, got: {max_pages}")


# ── Compatibility projection (TEMPORARY — remove after preset-based admin settings plan lands) ─

def apply_cv_compatibility_projection(cfg: dict[str, Any]) -> dict[str, Any]:
    """Project nested cv keys back to flat legacy keys for the migration window.

    This lets control-plane code (settings_schema, etc.) that still reads
    flat keys continue to function until the preset-based admin settings plan
    replaces those reads with nested ones.

    TEMPORARY: Must be removed once plan 2 (preset-based admin settings) is complete.
    """
    cv_cfg = cfg.get("cv")
    if cv_cfg is None:
        return cfg

    cfg["cv_generation_model"] = str(cv_cfg.get("generation", {}).get("model", ""))
    cfg["prompt_version"] = str(cv_cfg.get("generation", {}).get("prompt_version", ""))
    cfg["cv_max_pages"] = int(cv_cfg.get("validation", {}).get("max_pages", 2))

    # required_cv_sections: enabled composition sections only.
    required: list[str] = []
    comp = cv_cfg.get("composition") or {}
    for section_name, section_cfg in comp.items():
        if not isinstance(section_cfg, dict):
            continue
        enabled = section_cfg.get("enabled", False)
        if enabled:
            required.append(section_name.title())
    cfg["required_cv_sections"] = required

    return cfg


def get_required_cv_section_names(config: dict[str, Any]) -> list[str]:
    """Return the configured display names for required CV sections."""
    flat_required = [
        str(section_name)
        for section_name in list(config.get("required_cv_sections") or [])
        if section_name
    ]
    if flat_required:
        return flat_required

    cv_cfg = config.get("cv") or {}
    composition = cv_cfg.get("composition") or {}
    derived_required: list[str] = []
    for section_key, section_cfg in composition.items():
        if not isinstance(section_cfg, dict):
            continue
        if not section_cfg.get("enabled", False):
            continue
        display_name = CV_SECTION_KEY_TO_NAME.get(section_key)
        if display_name:
            derived_required.append(display_name)
    return derived_required


def get_required_structured_section_keys(config: dict[str, Any]) -> list[str]:
    """Return structured section keys mapped from the required section names."""
    keys: list[str] = []
    seen: set[str] = set()
    for section_name in get_required_cv_section_names(config):
        key = CV_SECTION_NAME_TO_KEY.get(section_name.strip().lower())
        if key is None or key in seen:
            continue
        seen.add(key)
        keys.append(key)
    return keys


def get_vertex_location(config: dict[str, Any]) -> str:
    """Return the Vertex AI region, separate from BigQuery location."""
    vertex_location = str(config.get("vertex_location", "")).strip()
    if vertex_location:
        return vertex_location
    return "us-central1"


def get_stage_runtime_value(
    config: dict[str, Any],
    *,
    stage: str,
    key: str,
    default: Any,
    compatibility_fallback_key: str | None = None,
) -> Any:
    """Resolve runtime throughput value from canonical stage_runtime with optional legacy fallback."""
    stage_runtime = dict(config.get("stage_runtime") or {})
    stage_runtime_cfg = dict(stage_runtime.get(stage) or {})
    if key in stage_runtime_cfg:
        return stage_runtime_cfg[key]
    if compatibility_fallback_key:
        fallback_value = config.get(compatibility_fallback_key)
        if fallback_value is not None:
            return fallback_value
    return default


def get_stage_runtime_concurrency(
    config: dict[str, Any],
    *,
    stage: str,
    default: int = 1,
    compatibility_fallback_key: str | None = None,
) -> int:
    raw_value = get_stage_runtime_value(
        config,
        stage=stage,
        key="concurrency",
        default=default,
        compatibility_fallback_key=compatibility_fallback_key,
    )
    try:
        return max(1, int(raw_value))
    except (TypeError, ValueError):
        return max(1, int(default))


def get_stage_runtime_sleep_secs(
    config: dict[str, Any],
    *,
    stage: str,
    default: float = 0.5,
    compatibility_fallback_key: str | None = None,
) -> float:
    raw_value = get_stage_runtime_value(
        config,
        stage=stage,
        key="sleep_secs",
        default=default,
        compatibility_fallback_key=compatibility_fallback_key,
    )
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return float(default)


def get_gemini_model(config: dict[str, Any]) -> str:
    return str(config.get("gemini_model") or "gemini-2.5-flash")


def _normalize_cv_acceptance_policy(raw: Any) -> dict[str, Any]:
    policy = dict(_DEFAULT_CV_ACCEPTANCE_POLICY)
    if not isinstance(raw, dict):
        return policy
    policy["enabled"] = bool(raw.get("enabled", policy["enabled"]))
    labels = raw.get("enforce_for_fit_labels")
    if isinstance(labels, list):
        normalized = [str(item).strip().lower() for item in labels if str(item).strip()]
        if normalized:
            policy["enforce_for_fit_labels"] = normalized
    try:
        policy["min_required_match_score"] = float(raw.get("min_required_match_score", policy["min_required_match_score"]))
    except (TypeError, ValueError):
        pass
    try:
        policy["min_required_skill_support_ratio"] = float(
            raw.get("min_required_skill_support_ratio", policy["min_required_skill_support_ratio"])
        )
    except (TypeError, ValueError):
        pass
    downgrade_status = str(raw.get("downgrade_status") or policy["downgrade_status"]).strip().lower()
    if downgrade_status:
        policy["downgrade_status"] = downgrade_status
    reason_code = str(raw.get("review_reason_code") or policy["review_reason_code"]).strip().lower()
    if reason_code:
        policy["review_reason_code"] = reason_code
    return policy


def get_embedding_model(config: dict[str, Any]) -> str:
    return str(config.get("embedding_model") or "text-embedding-005")


def get_ranking_prompt_id(config: dict[str, Any]) -> str:
    prompt_id = str(
        (((config.get("prompts") or {}).get("ranking") or {}).get("ai_score") or {}).get("prompt_id")
        or _DEFAULT_RANKING_AI_SCORE_PROMPT_ID
    ).strip()
    return prompt_id or _DEFAULT_RANKING_AI_SCORE_PROMPT_ID


def get_cv_generation_structured_prompt_id(config: dict[str, Any]) -> str:
    prompt_id = str(
        (((config.get("prompts") or {}).get("cv_generation") or {}).get("structured_write") or {}).get("prompt_id")
        or _DEFAULT_CV_GENERATION_STRUCTURED_WRITE_PROMPT_ID
    ).strip()
    return prompt_id or _DEFAULT_CV_GENERATION_STRUCTURED_WRITE_PROMPT_ID


def get_cv_generation_model(config: dict[str, Any]) -> str:
    return str((((config.get("cv") or {}).get("generation") or {}).get("model")) or config.get("cv_generation_model") or "gemini-2.5-flash")


def get_cv_generation_prompt_version(config: dict[str, Any]) -> str:
    return str((((config.get("cv") or {}).get("generation") or {}).get("prompt_version")) or config.get("prompt_version") or "v1")


def get_cv_acceptance_policy(config: dict[str, Any]) -> dict[str, Any]:
    runtime_policy = dict(
        config.get("cv_acceptance_policy_runtime")
        or config.get("cv_acceptance_policy")
        or {}
    )
    merged_policy = _normalize_cv_acceptance_policy(config.get("cv_acceptance_policy"))
    required_match = dict(runtime_policy.get("required_match") or {})
    min_ratio = dict(required_match.get("min_ratio_by_fit") or {})
    max_missing = dict(required_match.get("max_missing_by_fit") or {})
    default_required_match = _DEFAULT_CV_REQUIRED_MATCH_POLICY["required_match"]
    merged_policy.update({
        "required_match": {
            "min_ratio_by_fit": {
                "strong": float(min_ratio.get("strong", default_required_match["min_ratio_by_fit"]["strong"])),
                "stretch": float(min_ratio.get("stretch", default_required_match["min_ratio_by_fit"]["stretch"])),
            },
            "max_missing_by_fit": {
                "strong": int(max_missing.get("strong", default_required_match["max_missing_by_fit"]["strong"])),
                "stretch": int(max_missing.get("stretch", default_required_match["max_missing_by_fit"]["stretch"])),
            },
        },
        "force_review_when_any_required_missing_for_fits": [
            str(item).strip().lower()
            for item in (runtime_policy.get("force_review_when_any_required_missing_for_fits") or [])
            if str(item).strip()
        ],
    })
    return merged_policy




