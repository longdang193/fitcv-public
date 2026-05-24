"""@meta
name: section_policy
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared Certifications section-policy helpers for generator/validator symmetry.
inputs:
  - CV configuration
  - Candidate profile certifications
  - Evidence-selected certifications
outputs:
  - Policy decision objects and formatted certification evidence lines
lifecycle:
  - status: active
"""

from __future__ import annotations

from typing import Any

from fitcv.config import CV_SECTION_KEY_TO_NAME


def _normalize_placeholder_token(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.replace("–", "-").replace("—", "-")
    return " ".join(normalized.split())


def _is_placeholder_token(value: Any) -> bool:
    return _normalize_placeholder_token(value) in {
        "",
        "n/a",
        "na",
        "none",
        "null",
        "tbd",
        "to be determined",
        "placeholder",
        "sample",
        "example",
        "your certification",
        "certification name",
        "issuer",
        "year",
        "yyyy",
    }


def _section_enabled(config: dict[str, Any], section_key: str) -> bool:
    composition = ((config.get("cv") or {}).get("composition") or {})
    section_cfg = composition.get(section_key)
    if not isinstance(section_cfg, dict):
        return True
    return bool(section_cfg.get("enabled", True))


def certification_rows_from_profile(profile: dict[str, Any] | None) -> list[dict[str, Any]]:
    rows = profile.get("certifications") if isinstance(profile, dict) else []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def is_meaningful_certification_row(row: dict[str, Any]) -> bool:
    name = str(row.get("name") or "").strip()
    issuer = str(row.get("issuer") or "").strip()
    year = row.get("year")
    if name and not _is_placeholder_token(name):
        return True
    if issuer and not _is_placeholder_token(issuer):
        return True
    if year and not _is_placeholder_token(year):
        return True
    return False


def meaningful_certification_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if is_meaningful_certification_row(row)]


def certification_policy_decisions(
    *,
    config: dict[str, Any],
    profile: dict[str, Any] | None,
    evidence_selected_certifications: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    section_key = "certifications"
    section_name = CV_SECTION_KEY_TO_NAME.get(section_key, "Certifications")
    enabled = _section_enabled(config, section_key)

    profile_rows = certification_rows_from_profile(profile)
    meaningful_profile_rows = meaningful_certification_rows(profile_rows)

    evidence_rows = [row for row in (evidence_selected_certifications or []) if isinstance(row, dict)]
    meaningful_evidence_rows = meaningful_certification_rows(evidence_rows)

    # Selected-evidence grounding contract:
    # certifications are admissible only when they are explicitly present in
    # selected evidence. Profile-only certifications are treated as unsupported.
    admissible_rows = meaningful_evidence_rows
    admissible_via = "evidence" if meaningful_evidence_rows else "none"

    required = bool(enabled and admissible_rows)

    return {
        "section_key": section_key,
        "section_name": section_name,
        "enabled": enabled,
        "profile_rows": profile_rows,
        "meaningful_profile_rows": meaningful_profile_rows,
        "evidence_rows": evidence_rows,
        "meaningful_evidence_rows": meaningful_evidence_rows,
        "admissible_rows": admissible_rows,
        "admissible_via": admissible_via,
        "required": required,
    }


def certification_evidence_lines(policy: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for row in list(policy.get("admissible_rows") or []):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        issuer = str(row.get("issuer") or "").strip()
        year = row.get("year")
        if not name:
            continue
        parts = [name]
        if issuer:
            parts.append(issuer)
        line = " — ".join(parts)
        if year:
            line = f"{line} ({year})"
        lines.append(line)
    return lines
