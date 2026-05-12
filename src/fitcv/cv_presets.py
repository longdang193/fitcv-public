"""@meta
name: cv_presets
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.cv_presets.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from typing import Any


# ── supported presets ─────────────────────────────────────────────────────────

SUPPORTED_PRESETS = frozenset(["europass"])


# ── allowed enum values per preset ───────────────────────────────────────────

_ALLOWED_DETAIL = frozenset(["compact", "standard", "detailed"])
_ALLOWED_THESIS_MODE = frozenset(["off", "title_only", "short_summary"])
_ALLOWED_BULLET_STYLE = frozenset(["standard", "action_project_result"])
_ALLOWED_SUMMARY_STYLE = frozenset(["concise", "achievement_focused", "skills_focused"])
_ALLOWED_SKILLS_DISPLAY = frozenset(["grouped", "flat"])
_ALLOWED_CERTS_DISPLAY = frozenset(["combined_with_skills", "separate"])


def _make_europass_allowed_values() -> dict[str, Any]:
    return {
        "summary": {
            "style": _ALLOWED_SUMMARY_STYLE,
        },
        "education": {
            "detail": _ALLOWED_DETAIL,
            "thesis": {
                "mode": _ALLOWED_THESIS_MODE,
            },
        },
        "experience": {
            "detail": _ALLOWED_DETAIL,
            "bullet_style": _ALLOWED_BULLET_STYLE,
        },
        "skills": {
            "display_mode": _ALLOWED_SKILLS_DISPLAY,
        },
        "certifications": {
            "display_mode": _ALLOWED_CERTS_DISPLAY,
        },
        "projects": {
            "detail": _ALLOWED_DETAIL,
        },
        "publications": {
            "detail": _ALLOWED_DETAIL,
        },
        "languages": {
            "detail": _ALLOWED_DETAIL,
        },
        # Global detail levels
        "detail": _ALLOWED_DETAIL,
    }


# ── preset registry ───────────────────────────────────────────────────────────

PRESET_REGISTRY: dict[str, dict[str, Any]] = {
    "europass": {
        # Canonical template file for this preset.
        "template_path": "templates/cv_template.md",
        # Section definitions: which keys are valid for this preset.
        "sections": {
            "summary": {},
            "education": {},
            "experience": {},
            "skills": {},
            "certifications": {},
            "projects": {},
            "publications": {},
            "languages": {},
        },
        # Canonical section order for rendering and validation.
        "section_order": [
            "summary",
            "experience",
            "education",
            "skills",
            "certifications",
            "projects",
            "publications",
            "languages",
        ],
        # Allowed enum values — used by validate_composition().
        "allowed_values": _make_europass_allowed_values(),
    },
}


# ── query helpers ─────────────────────────────────────────────────────────────


def get_section_order(preset: str) -> list[str]:
    """Return the canonical section order for the given preset."""
    if preset not in PRESET_REGISTRY:
        raise ValueError(f"Unknown preset: {preset!r}")
    return list(PRESET_REGISTRY[preset]["section_order"])


def get_template_path(preset: str) -> str:
    """Return the template file path for the given preset."""
    if preset not in PRESET_REGISTRY:
        raise ValueError(f"Unknown preset: {preset!r}")
    return PRESET_REGISTRY[preset]["template_path"]


# ── composition validation ────────────────────────────────────────────────────

def validate_composition(
    preset: str,
    composition: dict[str, Any],
) -> dict[str, Any]:
    """Validate a composition dict against the preset's schema.

    Returns::

        {"valid": bool, "errors": list[str]}

    Errors are field-specific strings suitable for admin display.
    """
    if preset not in PRESET_REGISTRY:
        return {"valid": False, "errors": [f"Unknown preset: {preset!r}"]}

    registry = PRESET_REGISTRY[preset]
    valid_sections = registry["sections"]
    allowed = registry.get("allowed_values", {})
    errors: list[str] = []

    for section_name, section_cfg in composition.items():
        if section_name not in valid_sections:
            errors.append(
                f"Unknown section '{section_name}' for preset '{preset}'. "
                f"Supported: {', '.join(sorted(valid_sections))}"
            )
            continue

        if not isinstance(section_cfg, dict):
            errors.append(f"Section '{section_name}' must be a dict, got {type(section_cfg).__name__}")
            continue

        # Validate known fields against allowed enums
        section_allowed = allowed.get(section_name, {})

        for field, allowed_vals in section_allowed.items():
            if isinstance(allowed_vals, dict):
                # Nested field (e.g. education.thesis.mode)
                nested_cfg = section_cfg.get(field, {})
                if isinstance(nested_cfg, dict):
                    for subfield, sub_allowed in allowed_vals.items():
                        if subfield in nested_cfg:
                            # YAML 1.1 parses bare `off`/`on`/`yes`/`no` as booleans;
                            # coerce so the enum check still works
                            raw_val = nested_cfg[subfield]
                            val = str(raw_val) if isinstance(raw_val, bool) else raw_val
                            if val not in sub_allowed:
                                errors.append(
                                    f"Section '{section_name}', field '{field}.{subfield}': "
                                    f"got {nested_cfg[subfield]!r}, allowed: {', '.join(sorted(sub_allowed))}"
                                )
            elif field in section_cfg:
                raw_val = section_cfg[field]
                val = str(raw_val) if isinstance(raw_val, bool) else raw_val
                if val not in allowed_vals:
                    errors.append(
                        f"Section '{section_name}', field '{field}': "
                        f"got {section_cfg[field]!r}, allowed: {', '.join(sorted(str(v) for v in allowed_vals))}"
                    )

    return {"valid": len(errors) == 0, "errors": errors}
