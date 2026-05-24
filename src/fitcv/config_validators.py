"""@meta
name: config_validators
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Validation helpers for SSOT overlap detection in config inputs.
inputs:
  - Env config and canonical policy/runtime key maps
outputs:
  - Normalized overlap findings for enforcement/warnings
lifecycle:
  - status: active
"""

from typing import Any


def detect_pipeline_ssot_overlap(
    env_cfg: dict[str, Any],
    pipeline_policy_cfg: dict[str, Any],
) -> list[str]:
    overlaps: list[str] = []
    for key in sorted(pipeline_policy_cfg.keys()):
        if key in env_cfg:
            overlaps.append(key)

    env_pipeline = env_cfg.get("pipeline")
    policy_pipeline = pipeline_policy_cfg.get("pipeline")
    if isinstance(env_pipeline, dict) and isinstance(policy_pipeline, dict):
        for subkey in sorted(policy_pipeline.keys()):
            if subkey in env_pipeline:
                overlaps.append(f"pipeline.{subkey}")
    return overlaps


def detect_env_canonical_ownership_overlaps(
    env_cfg: dict[str, Any],
    *,
    canonical_infra_keys: set[str],
    canonical_policy_top_level_keys: set[str],
    canonical_pipeline_top_level_keys: set[str],
    canonical_taxonomy_top_level_keys: set[str],
    legacy_compatibility_keys: set[str],
) -> list[str]:
    overlaps: list[str] = []
    for key in sorted(env_cfg.keys()):
        if key in canonical_infra_keys:
            continue
        if key in canonical_policy_top_level_keys:
            overlaps.append(f"{key} (policy)")
            continue
        if key in canonical_pipeline_top_level_keys:
            overlaps.append(f"{key} (runtime/pipeline)")
            continue
        if key in canonical_taxonomy_top_level_keys:
            overlaps.append(f"{key} (taxonomy)")
            continue
        if key in legacy_compatibility_keys:
            overlaps.append(f"{key} (legacy-compat)")
    return overlaps
