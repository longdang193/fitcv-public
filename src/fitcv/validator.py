"""@meta
name: validator
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.validator.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import re
from collections.abc import Iterable
from typing import Any, TypedDict

from fitcv.candidate import flatten_skills, infer_role_family
from fitcv.config import CV_SECTION_KEY_TO_NAME, get_required_structured_section_keys
from fitcv.rule_filter import _canonicalise_skill

# ── constants ─────────────────────────────────────────────────────────────────

_LINES_PER_PAGE: int = 55  # A4 estimate at standard font size
_SECTION_HEADING_PATTERN = r"^##?\s+{section}\s*$"
_SOFT_CLAIM_TRIGGER_TOKENS = {
    "analytics",
    "automation",
    "banking",
    "business",
    "communication",
    "cross",
    "dashboard",
    "dashboards",
    "delivery",
    "domain",
    "engineer",
    "forecasting",
    "fraud",
    "insight",
    "insights",
    "kpi",
    "leadership",
    "manager",
    "monitoring",
    "pipeline",
    "pipelines",
    "positioning",
    "reporting",
    "responsibility",
    "retail",
    "risk",
    "role",
    "scientist",
    "stakeholder",
    "streaming",
    "workflow",
    "workflows",
}
_SOFT_ALIGNMENT_ALIASES: dict[str, tuple[str, ...]] = {
    "stakeholder_reporting": (
        "business communication",
        "business reporting",
        "cross functional communication",
        "cross functional reporting",
        "executive reporting",
        "stakeholder communication",
        "stakeholder facing reporting",
        "stakeholder reporting",
    ),
    "dashboarding": (
        "dashboard",
        "dashboards",
        "kpi dashboard",
        "reporting dashboard",
        "reporting workflows",
        "visualisation",
        "visualization",
    ),
    "data_pipeline": (
        "data pipeline",
        "data pipelines",
        "etl",
        "streaming pipeline",
        "streaming workflows",
    ),
    "fraud_detection": (
        "fraud analytics",
        "fraud detection",
        "risk monitoring",
        "transaction monitoring",
    ),
}
_SOFT_SIMILARITY_THRESHOLD = 0.30
_UNRESOLVED_PLACEHOLDER_PATTERNS = (
    re.compile(r"\[your name\]", re.IGNORECASE),
    re.compile(r"\[candidate name\]", re.IGNORECASE),
    re.compile(r"\[your email\]", re.IGNORECASE),
    re.compile(r"\[your phone\]", re.IGNORECASE),
    re.compile(r"\[linkedin url\]", re.IGNORECASE),
)
_CANDIDATE_NAME_PLACEHOLDER_VALUES = {
    "candidate name",
    "your name",
}
_MARKDOWN_PLACEHOLDER_PATTERNS = (
    re.compile(r"\btbd\b", re.IGNORECASE),
    re.compile(r"lorem ipsum", re.IGNORECASE),
    re.compile(r"\bto be (filled|provided|updated)\b", re.IGNORECASE),
)
_EDUCATION_PLACEHOLDER_TOKENS = {
    "",
    "none",
    "null",
    "n/a",
    "na",
    "not specified",
    "not provided",
    "unknown",
}


def _role_family_aliases(config: dict[str, Any]) -> dict[str, set[str]]:
    role_taxonomy = config.get("role_taxonomy") or {}
    role_family_by_role = role_taxonomy.get("role_family_by_role") or {}
    canonical_role_by_alias = role_taxonomy.get("canonical_role_by_alias") or {}
    aliases: dict[str, set[str]] = {}

    if isinstance(role_family_by_role, dict):
        for role, family in role_family_by_role.items():
            normalized_family = str(family or "").strip().lower()
            normalized_role = str(role or "").strip().lower()
            if not normalized_family or not normalized_role:
                continue
            aliases.setdefault(normalized_family, set()).add(normalized_role)

    if isinstance(canonical_role_by_alias, dict) and isinstance(role_family_by_role, dict):
        for alias, canonical_role in canonical_role_by_alias.items():
            normalized_alias = str(alias or "").strip().lower()
            normalized_canonical = str(canonical_role or "").strip().lower()
            normalized_family = str(role_family_by_role.get(normalized_canonical) or "").strip().lower()
            if not normalized_alias or not normalized_family:
                continue
            aliases.setdefault(normalized_family, set()).add(normalized_alias)

    return aliases


class AnalysisGroundingPayload(TypedDict, total=False):
    evidence_payload: list[dict[str, Any]]
    evidence_used: list[dict[str, Any]]
    evidence_selection_summary: dict[str, Any]
    analysis_input_summary: dict[str, Any]


class SelectedEvidenceSupport(TypedDict):
    evidence_ids: list[str]
    employers: set[str]
    projects: set[str]
    skills_lower: set[str]
    skills_canonical: set[str]
    responsibility_themes: set[str]
    domain_tags: set[str]
    role_families: set[str]
    support_phrases: list[str]
    soft_support_tokens: set[str]
    has_selected_support: bool


# ── structural checks ─────────────────────────────────────────────────────────

def validate_output(cv_text: str, required_sections: list[str]) -> dict[str, Any]:
    """Check that all required sections are present in the CV markdown.

    Returns the full output schema with only ``missing_sections`` populated;
    grounding and skill checks are left empty (handled separately or via
    ``run_all_validations``).

    ``valid`` is False when any required section is absent.
    """
    missing: list[str] = []
    for section in required_sections:
        section_pattern = re.compile(
            _SECTION_HEADING_PATTERN.format(section=re.escape(section)),
            re.MULTILINE | re.IGNORECASE,
        )
        heading_match = section_pattern.search(cv_text)
        if heading_match is None:
            missing.append(section)
            continue

        next_heading_match = re.search(r"^##?\s+", cv_text[heading_match.end():], re.MULTILINE)
        section_end = (
            heading_match.end() + next_heading_match.start()
            if next_heading_match is not None
            else len(cv_text)
        )
        section_body = cv_text[heading_match.end():section_end].strip()
        if not section_body:
            missing.append(section)
    return {
        "valid": len(missing) == 0,
        "missing_sections": missing,
        "grounding_violations": [],
        "skill_violations": [],
        "warnings": [],
    }

def _section_body(cv_text: str, section_name: str) -> str:
    section_pattern = re.compile(
        _SECTION_HEADING_PATTERN.format(section=re.escape(section_name)),
        re.MULTILINE | re.IGNORECASE,
    )
    heading_match = section_pattern.search(cv_text)
    if heading_match is None:
        return ""
    next_heading_match = re.search(r"^##?\s+", cv_text[heading_match.end():], re.MULTILINE)
    section_end = (
        heading_match.end() + next_heading_match.start()
        if next_heading_match is not None
        else len(cv_text)
    )
    return cv_text[heading_match.end():section_end].strip()

def _markdown_quality_checks(cv_text: str, required_sections: list[str]) -> tuple[list[str], list[str]]:
    blocking_issues: list[str] = []
    review_flags: list[str] = []

    positions: list[tuple[str, int]] = []
    for section in required_sections:
        pattern = re.compile(
            _SECTION_HEADING_PATTERN.format(section=re.escape(section)),
            re.MULTILINE | re.IGNORECASE,
        )
        match = pattern.search(cv_text)
        if match is not None:
            positions.append((section, match.start()))
    if len(positions) >= 2:
        sorted_positions = sorted(positions, key=lambda item: item[1])
        if [name for name, _ in positions] != [name for name, _ in sorted_positions]:
            blocking_issues.append("Required section headings are out of configured order.")

    bad_bullet_lines = [
        line.strip()
        for line in cv_text.splitlines()
        if re.match(r"^\s*[\*\u2022]\s+", line)
    ]
    if bad_bullet_lines:
        blocking_issues.append("Unsupported bullet marker detected; use '- ' only.")

    for pattern in _MARKDOWN_PLACEHOLDER_PATTERNS:
        if pattern.search(cv_text):
            blocking_issues.append("Placeholder text detected in markdown content.")
            break

    for section_name in ("Experience", "Projects"):
        if section_name not in required_sections:
            continue
        body = _section_body(cv_text, section_name)
        if not body:
            continue
        bullet_count = len(re.findall(r"(?m)^\s*-\s+\S+", body))
        if bullet_count < 2:
            review_flags.append(
                f"{section_name} section appears shallow (fewer than 2 bullets)."
            )

    return (list(dict.fromkeys(blocking_issues)), list(dict.fromkeys(review_flags)))


def _normalize_placeholder_name_token(value: str) -> str:
    normalized = re.sub(r"[\[\]()*_`#]+", " ", str(value or ""))
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized


def _is_candidate_name_placeholder(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    normalized = _normalize_placeholder_name_token(value)
    return normalized in _CANDIDATE_NAME_PLACEHOLDER_VALUES


def _check_candidate_name_placeholders(
    cv_text: str,
    structured_cv: dict[str, Any] | None,
) -> list[str]:
    violations: list[str] = []
    seen_values: set[str] = set()

    if isinstance(structured_cv, dict):
        sections = structured_cv.get("sections")
        if isinstance(sections, dict):
            header = sections.get("header")
            if isinstance(header, dict):
                header_name = header.get("name")
                if _is_candidate_name_placeholder(header_name):
                    normalized_name = str(header_name).strip()
                    if normalized_name and normalized_name.lower() not in seen_values:
                        seen_values.add(normalized_name.lower())
                        violations.append(
                            f"Unresolved candidate-name placeholder detected in CV header: {normalized_name}"
                        )

    for line in cv_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not stripped.startswith("#"):
            continue
        heading_text = stripped.lstrip("#").strip()
        if not heading_text:
            continue
        if _is_candidate_name_placeholder(heading_text):
            normalized_heading = str(heading_text).strip()
            if normalized_heading.lower() not in seen_values:
                seen_values.add(normalized_heading.lower())
                violations.append(
                    f"Unresolved candidate-name placeholder detected in CV header: {normalized_heading}"
                )

    return violations


def _structured_section_has_content(section_key: str, section_value: Any) -> bool:
    if section_key == "summary":
        return (
            isinstance(section_value, dict)
            and isinstance(section_value.get("text"), str)
            and bool(section_value.get("text", "").strip())
        )
    if section_key == "skills":
        if not isinstance(section_value, dict):
            return False
        groups = section_value.get("groups")
        if not isinstance(groups, list) or not groups:
            return False
        for group in groups:
            if not isinstance(group, dict):
                continue
            items = group.get("items")
            if isinstance(items, list) and any(isinstance(item, str) and item.strip() for item in items):
                return True
        return False
    if section_key in {
        "experience",
        "projects",
        "education",
        "certifications",
        "publications",
        "languages",
    }:
        return isinstance(section_value, list) and len(section_value) > 0
    return True


def _find_missing_required_structured_sections(
    structured_cv: dict[str, Any] | None,
    config: dict[str, Any],
    profile: dict[str, Any] | None = None,
) -> list[str]:
    if structured_cv is None:
        return []

    required_keys = get_required_structured_section_keys(config)
    if not required_keys:
        return []

    sections = structured_cv.get("sections")
    if not isinstance(sections, dict):
        return [CV_SECTION_KEY_TO_NAME.get(key, key.title()) for key in required_keys]

    missing_sections: list[str] = []
    for section_key in required_keys:
        if section_key == "education":
            has_profile_education = bool(list((profile or {}).get("education") or []))
            if not has_profile_education:
                # Do not hard-fail Education when the candidate profile has no education records.
                continue
        section_value = sections.get(section_key)
        if not _structured_section_has_content(section_key, section_value):
            missing_sections.append(CV_SECTION_KEY_TO_NAME.get(section_key, section_key.title()))
    return missing_sections


def check_length_constraints(cv_text: str, max_pages: int = 2) -> bool:
    """Return True if the CV fits within max_pages (line-count heuristic).

    Uses a conservative estimate of ``_LINES_PER_PAGE`` lines per A4 page.
    """
    line_count = len(cv_text.splitlines())
    return line_count <= max_pages * _LINES_PER_PAGE


# ── chronology check (on source profile, not CV text) ────────────────────────

def check_chronology(experiences: list[dict[str, Any]]) -> list[str]:
    """Verify date ordering in the source profile ``experiences`` list.

    Checks the input data, not the generated CV text.
    Skips entries where start/end cannot be parsed.
    Returns a list of violation strings (empty = clean).

    Violation: an earlier entry in the list has a start date that overlaps
    with a later entry's date range (i.e. entries are not in reverse-chron order).
    """
    violations: list[str] = []
    parsed: list[tuple[int, int, int]] = []  # (start, end, original_index)

    for idx, exp in enumerate(experiences):
        start_raw = str(exp.get("start") or "")
        end_raw = str(exp.get("end") or "")
        try:
            start_year = int(re.search(r"\d{4}", start_raw).group())  # type: ignore[union-attr]
            end_year = int(re.search(r"\d{4}", end_raw).group())  # type: ignore[union-attr]
            parsed.append((start_year, end_year, idx))
        except (AttributeError, ValueError):
            continue  # missing / unparseable dates → skip

    # Expect consecutive entries to be in reverse-chronological order.
    for i in range(len(parsed) - 1):
        curr_start, curr_end, curr_idx = parsed[i]
        next_start, next_end, next_idx = parsed[i + 1]
        if next_end > curr_start:
            violations.append(
                f"Chronology overlap: experience[{next_idx}] ends {next_end} "
                f"but experience[{curr_idx}] starts {curr_start}"
            )

    return violations


# ── grounding checks (on CV text) ────────────────────────────────────────────

def _normalize_lower_set(values: Iterable[str]) -> set[str]:
    return {value.strip().lower() for value in values if value and value.strip()}


def _extract_employer_mentions(cv_text: str) -> list[str]:
    generic_pattern = re.compile(
        r"(?:\bat\b|@)\s+([A-Z][A-Za-z0-9&\s\-'\.]+?)(?:\s*[\(\[\,\n]|$)",
    )
    mentioned: list[str] = [match.strip() for match in generic_pattern.findall(cv_text) if match.strip()]

    in_experience = False
    heading_pattern = re.compile(r"^###\s+.+?\s*[—–]\s+(.+?)(?:\s*\(|\s*$)")
    for line in cv_text.splitlines():
        stripped = line.strip()
        if re.match(r"^##\s+Experience", stripped, re.IGNORECASE):
            in_experience = True
            continue
        if re.match(r"^##\s+", stripped) and in_experience:
            in_experience = False
            continue
        if not in_experience:
            continue
        match = heading_pattern.match(stripped)
        if match:
            mention = match.group(1).strip()
            if mention:
                mentioned.append(mention)

    return list(dict.fromkeys(mentioned))


def _extract_project_mentions(cv_text: str) -> list[str]:
    indicator_re = re.compile(
        r"(?:\b(?:the|built|led|designed|implemented)\s+)"
        r"((?:[A-Z][A-Za-z0-9]+\s+){1,3})"
        r"(?:project|pipeline|system|platform)\b",
    )
    mentioned: list[str] = []
    for match in indicator_re.finditer(cv_text):
        full_phrase = match.group(0).strip()
        if full_phrase:
            mentioned.append(full_phrase)
    return list(dict.fromkeys(mentioned))


def _extract_skill_section_tokens(cv_text: str) -> list[str]:
    skills_section_re = re.compile(
        r"^##\s+Skills\s*\n(.*?)(?=^##\s|\Z)", re.MULTILINE | re.DOTALL | re.IGNORECASE
    )
    match = skills_section_re.search(cv_text)
    if not match:
        return []
    raw_tokens = re.split(r"[,\n]+", match.group(1))
    return [token.strip() for token in raw_tokens if token.strip()]


def _normalize_section_name(raw_heading: str) -> str:
    return re.sub(r"\s+", " ", raw_heading.strip().lower())


def _extract_soft_claim_lines(cv_text: str) -> list[str]:
    soft_lines: list[str] = []
    current_section: str | None = None
    for raw_line in cv_text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("## "):
            current_section = _normalize_section_name(stripped[3:])
            continue
        if stripped.startswith("### "):
            continue
        if current_section == "skills":
            continue
        if current_section == "summary":
            soft_lines.append(stripped)
            continue
        if stripped.startswith("- "):
            soft_lines.append(stripped[2:].strip())
    return list(dict.fromkeys(line for line in soft_lines if line))


def _text_to_tokens(value: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", value.lower()) if len(token) > 1}


def _best_token_overlap_ratio(claim_text: str, support_phrases: list[str]) -> float:
    claim_tokens = _text_to_tokens(claim_text)
    if not claim_tokens or not support_phrases:
        return 0.0

    best_overlap = 0.0
    for phrase in support_phrases:
        phrase_tokens = _text_to_tokens(phrase)
        if not phrase_tokens:
            continue
        overlap = len(claim_tokens & phrase_tokens) / len(claim_tokens)
        if overlap > best_overlap:
            best_overlap = overlap
    return best_overlap


def _expand_soft_alias_tokens(values: Iterable[str]) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        normalized = re.sub(r"\s+", "_", value.strip().lower())
        alias_values = _SOFT_ALIGNMENT_ALIASES.get(normalized, ())
        tokens |= _text_to_tokens(value)
        for alias in alias_values:
            tokens |= _text_to_tokens(alias)
    return tokens


def _normalize_analysis_grounding(
    analysis_grounding: AnalysisGroundingPayload | None,
    config: dict[str, Any],
) -> SelectedEvidenceSupport:
    evidence_payload = list((analysis_grounding or {}).get("evidence_payload") or [])
    evidence_used = list((analysis_grounding or {}).get("evidence_used") or [])
    evidence_items = evidence_payload or evidence_used

    employers: set[str] = set()
    projects: set[str] = set()
    skills_lower: set[str] = set()
    skills_canonical: set[str] = set()
    responsibility_themes: set[str] = set()
    domain_tags: set[str] = set()
    role_families: set[str] = set()
    support_phrases: list[str] = []
    selected_evidence_ids: list[str] = [
        str(evidence_id).strip()
        for evidence_id in list(((analysis_grounding or {}).get("evidence_selection_summary") or {}).get("selected_evidence_ids") or [])
        if str(evidence_id).strip()
    ]

    for item in evidence_items:
        if not isinstance(item, dict):
            continue
        evidence_id = str(item.get("evidence_id") or "").strip()
        if evidence_id and evidence_id not in selected_evidence_ids:
            selected_evidence_ids.append(evidence_id)

        company = str(item.get("company") or "").strip()
        if company:
            employers.add(company.lower())

        evidence_type = str(item.get("evidence_type") or "").strip().lower()
        project_name = str(item.get("name") or "").strip()
        if project_name and evidence_type in {"project", "project_entry"}:
            projects.add(project_name.lower())

        for raw_skill in list(item.get("skills") or []):
            skill_text = str(raw_skill or "").strip()
            if not skill_text:
                continue
            skills_lower.add(skill_text.lower())
            skills_canonical.add(_canonicalise_skill(skill_text, config))

        for raw_theme in list(item.get("responsibility_themes") or []):
            theme_text = str(raw_theme or "").strip()
            if theme_text:
                responsibility_themes.add(theme_text.lower())

        for raw_domain in list(item.get("domain_tags") or []):
            domain_text = str(raw_domain or "").strip()
            if domain_text:
                domain_tags.add(domain_text.lower())

        role_family = (
            str(item.get("role_family") or "").strip().lower()
            or infer_role_family(str(item.get("role") or ""), config=config)
            or None
        )
        if role_family:
            role_families.add(role_family)

        for text_field in ("name", "business_value", "role", "company"):
            text_value = str(item.get(text_field) or "").strip()
            if text_value:
                support_phrases.append(text_value)
        for list_field in ("bullets", "highlights", "tech_stack"):
            for raw_value in list(item.get(list_field) or []):
                text_value = str(raw_value or "").strip()
                if text_value:
                    support_phrases.append(text_value)

    soft_support_tokens: set[str] = set()
    soft_support_tokens |= _expand_soft_alias_tokens(responsibility_themes)
    soft_support_tokens |= _expand_soft_alias_tokens(domain_tags)
    role_family_aliases = _role_family_aliases(config)
    for role_family in role_families:
        soft_support_tokens |= _text_to_tokens(role_family)
        for alias in role_family_aliases.get(role_family, ()):
            soft_support_tokens |= _text_to_tokens(alias)

    return {
        "evidence_ids": selected_evidence_ids,
        "employers": employers,
        "projects": projects,
        "skills_lower": skills_lower,
        "skills_canonical": skills_canonical,
        "responsibility_themes": responsibility_themes,
        "domain_tags": domain_tags,
        "role_families": role_families,
        "support_phrases": list(dict.fromkeys(support_phrases)),
        "soft_support_tokens": soft_support_tokens,
        "has_selected_support": bool(evidence_items),
    }


def _check_selected_employer_grounding(
    cv_text: str,
    selected_employers: set[str],
    known_employers: set[str],
) -> list[str]:
    violations: list[str] = []
    if not selected_employers:
        return violations
    for mention in _extract_employer_mentions(cv_text):
        mention_lower = mention.lower()
        if mention_lower in selected_employers:
            continue
        if mention_lower in known_employers:
            violations.append(
                f"Employer '{mention}' in CV is present in candidate profile but not in selected evidence"
            )
    return violations


def _check_selected_project_grounding(
    cv_text: str,
    selected_projects: set[str],
    known_projects: set[str],
) -> list[str]:
    violations: list[str] = []
    if not selected_projects:
        return violations
    for mention in _extract_project_mentions(cv_text):
        mention_lower = mention.lower()
        if mention_lower in selected_projects:
            continue
        if mention_lower in known_projects:
            violations.append(
                f"Project reference '{mention}' in CV is present in candidate profile but not in selected evidence"
            )
    return violations


def _check_selected_skill_grounding(
    cv_text: str,
    selected_skills_lower: set[str],
    selected_skills_canonical: set[str],
    candidate_skills_lower: set[str],
    candidate_skills_canonical: set[str],
    config: dict[str, Any],
) -> list[str]:
    violations: list[str] = []
    if not selected_skills_lower and not selected_skills_canonical:
        return violations
    for skill in _extract_skill_section_tokens(cv_text):
        skill_lower = skill.lower()
        skill_canonical = _canonicalise_skill(skill, config)
        if skill_lower in selected_skills_lower or skill_canonical in selected_skills_canonical:
            continue
        if skill_lower in candidate_skills_lower or skill_canonical in candidate_skills_canonical:
            violations.append(
                f"Skill '{skill}' in CV Skills section is present in candidate profile but not in selected evidence"
            )
    return violations


def _deterministic_soft_support(
    claim_text: str,
    support_surface: SelectedEvidenceSupport,
    config: dict[str, Any],
) -> bool:
    claim_tokens = _text_to_tokens(claim_text)
    if not claim_tokens:
        return True
    supported_tokens = (
        support_surface["soft_support_tokens"]
        | _expand_soft_alias_tokens(support_surface["domain_tags"])
        | _expand_soft_alias_tokens(support_surface["responsibility_themes"])
    )
    role_family_aliases = _role_family_aliases(config)
    if support_surface["role_families"]:
        for role_family in support_surface["role_families"]:
            supported_tokens |= _text_to_tokens(role_family)
            for alias in role_family_aliases.get(role_family, ()):
                supported_tokens |= _text_to_tokens(alias)
    return bool(claim_tokens & supported_tokens)


def _claim_requires_soft_validation(claim_text: str, support_surface: SelectedEvidenceSupport) -> bool:
    claim_tokens = _text_to_tokens(claim_text)
    if not claim_tokens:
        return False
    supported_triggers = support_surface["soft_support_tokens"] | _SOFT_CLAIM_TRIGGER_TOKENS
    return bool(claim_tokens & supported_triggers)


def _check_soft_claim_grounding(
    cv_text: str,
    support_surface: SelectedEvidenceSupport,
    config: dict[str, Any],
) -> tuple[list[str], dict[str, Any]]:
    violations: list[str] = []
    summary = {
        "deterministic_supported_soft_claims": 0,
        "semantic_supported_soft_claims": 0,
        "evaluated_soft_claims": 0,
    }
    if not support_surface["has_selected_support"]:
        return violations, summary

    for claim_text in _extract_soft_claim_lines(cv_text):
        if not _claim_requires_soft_validation(claim_text, support_surface):
            continue
        summary["evaluated_soft_claims"] += 1
        if _deterministic_soft_support(claim_text, support_surface, config):
            summary["deterministic_supported_soft_claims"] += 1
            continue
        overlap = _best_token_overlap_ratio(claim_text, support_surface["support_phrases"])
        if overlap >= _SOFT_SIMILARITY_THRESHOLD:
            summary["semantic_supported_soft_claims"] += 1
            continue
        violations.append(
            "Soft claim is not supported by selected evidence: "
            f"'{claim_text}'"
        )
    return violations, summary


def _check_unresolved_placeholders(cv_text: str) -> list[str]:
    violations: list[str] = []
    for pattern in _UNRESOLVED_PLACEHOLDER_PATTERNS:
        match = pattern.search(cv_text)
        if match is None:
            continue
        violations.append(
            f"Generated CV contains unresolved placeholder: '{match.group(0)}'"
        )
    return list(dict.fromkeys(violations))


def _normalize_placeholder_token(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    normalized = normalized.replace("–", "-").replace("—", "-")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _is_placeholder_token(value: Any) -> bool:
    return _normalize_placeholder_token(value) in _EDUCATION_PLACEHOLDER_TOKENS


def _check_synthetic_education_entries(structured_cv: dict[str, Any] | None) -> list[str]:
    if not isinstance(structured_cv, dict):
        return []
    sections = structured_cv.get("sections")
    if not isinstance(sections, dict):
        return []
    education_rows = sections.get("education")
    if not isinstance(education_rows, list):
        return []
    violations: list[str] = []
    for index, row in enumerate(education_rows):
        if not isinstance(row, dict):
            continue
        values = (
            row.get("degree"),
            row.get("institution"),
            row.get("field"),
            row.get("start"),
            row.get("end"),
        )
        if all(_is_placeholder_token(value) for value in values):
            violations.append(
                f"Synthetic Education row detected at index {index}: all fields are placeholders."
            )
            continue
        if _is_placeholder_token(row.get("degree")) and _is_placeholder_token(row.get("institution")):
            if _is_placeholder_token(row.get("start")) and _is_placeholder_token(row.get("end")):
                violations.append(
                    "Synthetic Education row detected at index "
                    f"{index}: placeholder degree/institution/date pair."
                )
    return violations

def _check_synthetic_non_education_entries(structured_cv: dict[str, Any] | None) -> list[str]:
    if not isinstance(structured_cv, dict):
        return []
    sections = structured_cv.get("sections")
    if not isinstance(sections, dict):
        return []
    violations: list[str] = []

    def _rows(section_key: str) -> list[dict[str, Any]]:
        values = sections.get(section_key)
        if not isinstance(values, list):
            return []
        return [item for item in values if isinstance(item, dict)]

    for index, row in enumerate(_rows("projects")):
        bullets = [bullet for bullet in (row.get("bullets") or []) if isinstance(bullet, str)]
        if any(not _is_placeholder_token(bullet) for bullet in bullets):
            continue
        if _is_placeholder_token(row.get("name")) and _is_placeholder_token(row.get("context")):
            violations.append(f"Synthetic Projects row detected at index {index}: placeholder name/context.")

    for index, row in enumerate(_rows("certifications")):
        if all(_is_placeholder_token(value) for value in (row.get("name"), row.get("issuer"), row.get("year"))):
            violations.append(f"Synthetic Certifications row detected at index {index}: all fields are placeholders.")

    for index, row in enumerate(_rows("publications")):
        if all(_is_placeholder_token(value) for value in (row.get("title"), row.get("publisher"), row.get("year"))):
            violations.append(f"Synthetic Publications row detected at index {index}: all fields are placeholders.")

    for index, row in enumerate(_rows("languages")):
        if all(_is_placeholder_token(value) for value in (row.get("name"), row.get("level"))):
            violations.append(f"Synthetic Languages row detected at index {index}: all fields are placeholders.")

    return violations

def check_employer_grounding(cv_text: str, known_employers: list[str]) -> list[str]:
    """Return violations for any employer mentioned in the CV text that is not in known_employers.

    Detection strategies (combined):

    1. **Generic patterns** — ``at <Name>``, ``@ <Name>`` anywhere in the text.
       Em-dashes (``—``, ``–``) are intentionally excluded here because they
       appear in project titles (e.g. ``FitCV — AI-Powered CV Generation
       Pipeline``) and cause false positives.
    2. **Experience heading pattern** — ``### Role — Company (dates)`` lines
       within the ``## Experience`` section.  This is the only context where
       an em-dash reliably separates role from employer.

    If ``known_employers`` is empty, no check is possible → returns [].
    """
    if not known_employers:
        return []

    violations: list[str] = []
    known_lower = _normalize_lower_set(known_employers)

    for mention in _extract_employer_mentions(cv_text):
        if mention.lower() not in known_lower:
            violations.append(
                f"Employer '{mention}' in CV is not in the known employers list"
            )

    return violations


def check_project_existence(cv_text: str, known_projects: list[str]) -> list[str]:
    """Return violations for project names in the CV that are not in known_projects.

    Two detection strategies are combined:
    1. ``###`` headings within the CV (standard template output for project names)
    2. Capitalized multi-word phrases adjacent to project-indicator words
       (e.g. "the Phantom Pipeline project", "Built Phantom Pipeline")

    If ``known_projects`` is empty → no check → returns [].
    """
    if not known_projects:
        return []

    violations: list[str] = []
    known_lower = _normalize_lower_set(known_projects)

    for mention in _extract_project_mentions(cv_text):
        if mention.lower() not in known_lower and not any(
            project_name in mention.lower() for project_name in known_lower
        ):
            violations.append(
                f"Project reference '{mention}' in CV is not in the known projects list"
            )

    # Deduplicate
    return list(dict.fromkeys(violations))



# ── skill provenance (Skills section only) ────────────────────────────────────

def check_skill_provenance(
    cv_text: str,
    candidate_skills: list[str],
    config: dict[str, Any] | None = None,
) -> list[str]:
    """Validate the Skills section of the CV against the candidate's knowledge base.

    Checks the Skills section only (the text after '## Skills' up to the next ## heading).
    Does not scan bullet text — this is conservative by design.

    Returns a list of violation strings for skills found in the Skills section that
    are not in ``candidate_skills`` (case-insensitive, comma/newline delimited).
    """
    if not candidate_skills:
        return []

    cv_skills = _extract_skill_section_tokens(cv_text)
    if not cv_skills:
        return []

    candidate_lower = {s.strip().lower() for s in candidate_skills}
    candidate_canonical = {
        _canonicalise_skill(skill, config)
        for skill in candidate_skills
        if skill.strip()
    }
    violations: list[str] = []

    for skill in cv_skills:
        skill_lower = skill.lower()
        skill_canonical = _canonicalise_skill(skill, config)
        if skill_lower not in candidate_lower and skill_canonical not in candidate_canonical:
            violations.append(
                f"Skill '{skill}' in CV Skills section is not in candidate knowledge base"
            )

    return violations


# ── aggregate orchestrator ────────────────────────────────────────────────────

def run_all_validations(
    cv_text: str,
    profile: dict[str, Any],
    config: dict[str, Any],
    structured_cv: dict[str, Any] | None = None,
    analysis_grounding: AnalysisGroundingPayload | None = None,
) -> dict[str, Any]:
    """Aggregate all validation checks and return the full output schema.

    Output schema::

        {
            "valid": bool,
            "missing_sections": list[str],
            "grounding_violations": list[str],
            "skill_violations": list[str],
            "warnings": list[str],
        }

    ``valid`` is False when any grounding_violations or skill_violations exist,
    or when required sections are missing. Length issues add warnings but do not
    block validity.
    """
    required_sections: list[str] = list(config["required_cv_sections"])
    # Read max_pages: prefer nested cv.validation.max_pages, fall back to flat cv_max_pages
    cv_cfg = config.get("cv") or {}
    max_pages: int = int(
        cv_cfg.get("validation", {}).get("max_pages", 0)
        or config.get("cv_max_pages", 2)
    )

    # Structural section check
    section_result = validate_output(cv_text, required_sections)
    missing_sections = list(section_result["missing_sections"])
    missing_sections.extend(_find_missing_required_structured_sections(structured_cv, config, profile))
    missing_sections = list(dict.fromkeys(missing_sections))

    # Grounding checks
    known_employers: list[str] = [
        str(exp.get("company") or "") for exp in (profile.get("experiences") or [])
        if exp.get("company")
    ]
    known_projects: list[str] = [
        str(proj.get("name") or "") for proj in (profile.get("projects") or [])
        if proj.get("name")
    ]
    candidate_skills = flatten_skills(profile)
    if not candidate_skills:
        raw_candidate_skills = list(profile.get("skills") or [])
        candidate_skills = [
            str(skill)
            for skill in raw_candidate_skills
            if skill
        ]

    grounding_violations: list[str] = (
        check_employer_grounding(cv_text, known_employers)
        + check_project_existence(cv_text, known_projects)
    )
    skill_violations: list[str] = check_skill_provenance(cv_text, candidate_skills, config=config)
    deterministic_grounding_violations: list[str] = []
    semantic_grounding_violations: list[str] = []

    support_surface = _normalize_analysis_grounding(analysis_grounding, config)
    support_source_summary = {
        "hard_fact_mode": "profile_fallback",
        "soft_claim_mode": "disabled",
        "selected_evidence_ids": list(support_surface["evidence_ids"]),
        "selected_evidence_count": len(support_surface["evidence_ids"]),
        "deterministic_supported_soft_claims": 0,
        "semantic_supported_soft_claims": 0,
        "evaluated_soft_claims": 0,
    }
    if support_surface["has_selected_support"]:
        candidate_skills_lower = _normalize_lower_set(candidate_skills)
        candidate_skills_canonical = {
            _canonicalise_skill(skill, config)
            for skill in candidate_skills
            if str(skill).strip()
        }
        deterministic_grounding_violations.extend(
            _check_selected_employer_grounding(
                cv_text,
                support_surface["employers"],
                _normalize_lower_set(known_employers),
            )
        )
        deterministic_grounding_violations.extend(
            _check_selected_project_grounding(
                cv_text,
                support_surface["projects"],
                _normalize_lower_set(known_projects),
            )
        )
        deterministic_grounding_violations.extend(
            _check_selected_skill_grounding(
                cv_text,
                support_surface["skills_lower"],
                support_surface["skills_canonical"],
                candidate_skills_lower,
                candidate_skills_canonical,
                config,
            )
        )
        semantic_grounding_violations, soft_support_summary = _check_soft_claim_grounding(
            cv_text,
            support_surface,
            config,
        )
        support_source_summary.update({
            "hard_fact_mode": "selected_evidence",
            "soft_claim_mode": "hybrid_selected_evidence",
            **soft_support_summary,
        })
        grounding_violations = list(dict.fromkeys(
            grounding_violations
            + deterministic_grounding_violations
            + semantic_grounding_violations
        ))

    selected_evidence_skill_relaxation_enabled = bool(
        ((config.get("cv") or {}).get("validation") or {}).get(
            "allow_profile_skill_outside_selected_evidence",
            True,
        )
    )
    if selected_evidence_skill_relaxation_enabled:
        relaxed_skill_violations = [
            msg for msg in list(grounding_violations)
            if "present in candidate profile but not in selected evidence" in str(msg)
            and str(msg).startswith("Skill ")
        ]
        if relaxed_skill_violations:
            grounding_violations = [
                msg for msg in grounding_violations
                if msg not in relaxed_skill_violations
            ]

    markdown_quality_blocking_issues, markdown_quality_review_flags = _markdown_quality_checks(
        cv_text,
        required_sections,
    )

    # Non-blocking warnings
    warnings: list[str] = []
    if selected_evidence_skill_relaxation_enabled:
        for violation in relaxed_skill_violations:
            warnings.append(f"Relaxed selected-evidence skill grounding violation: {violation}")
    if not check_length_constraints(cv_text, max_pages=max_pages):
        warnings.append(f"CV length warning: exceeds estimated {max_pages}-page limit")

    grounding_violations = list(
        dict.fromkeys(
            grounding_violations
            + _check_unresolved_placeholders(cv_text)
            + _check_candidate_name_placeholders(cv_text, structured_cv)
            + _check_synthetic_education_entries(structured_cv)
            + _check_synthetic_non_education_entries(structured_cv)
        )
    )

    is_valid = (
        len(missing_sections) == 0
        and len(markdown_quality_blocking_issues) == 0
        and len(grounding_violations) == 0
        and len(skill_violations) == 0
    )

    return {
        "valid": is_valid,
        "missing_sections": missing_sections,
        "grounding_violations": grounding_violations,
        "deterministic_grounding_violations": deterministic_grounding_violations,
        "semantic_grounding_violations": semantic_grounding_violations,
        "skill_violations": skill_violations,
        "warnings": warnings,
        "support_source_summary": support_source_summary,
        "markdown_quality_blocking_issues": markdown_quality_blocking_issues,
        "markdown_quality_review_flags": markdown_quality_review_flags,
    }
