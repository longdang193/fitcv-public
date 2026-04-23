"""Structured candidate profile loading, validation, and BigQuery preparation.

Public API
----------
load_profile_yaml          : parse YAML profile file
validate_profile           : check required sections are present
flatten_skills             : extract deduplicated skill list from all evidence
infer_effective_preferences : merge explicit preferences with deterministic fallback intent
prepare_profile_rows       : map profile to all 5 BQ table schemas
load_candidate_to_bigquery : insert into all candidate BQ tables (integration)
"""

import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import yaml


# ── required profile sections ─────────────────────────────────────────────────

_REQUIRED_SECTIONS = ["experiences", "skills", "projects", "achievements", "preferences"]
_ROLE_INFERENCE_LIMIT = 4
_MAX_INFERRED_ROLE_FAMILIES = 2
_MAX_INFERRED_DOMAINS = 3
_ROLE_NOISE_TOKENS = frozenset(
    {
        "jr",
        "junior",
        "sr",
        "senior",
        "lead",
        "staff",
        "principal",
        "freelance",
        "contract",
    }
)
_UPPERCASE_ROLE_PARTS = frozenset({"ai", "bi", "dbt", "etl", "llm", "ml", "mlops", "nlp", "sql"})
_FALLBACK_ROLE_FAMILY_BY_ROLE = {
    "analytics engineer": "data_engineering",
    "bi analyst": "analytics",
    "business intelligence analyst": "analytics",
    "data analyst": "analytics",
    "data engineer": "data_engineering",
    "data scientist": "data_science",
    "machine learning engineer": "ml_engineering",
    "ml engineer": "ml_engineering",
}

_PREFERENCE_TEXT_KEYS = ("target_role", "seniority_target")
_PREFERENCE_LIST_KEYS = (
    "location_types",
    "locations",
    "domains",
    "role_families",
    "exclude_contract_types",
    "exclude_experience_levels",
)
_EXPERIENCE_LIST_KEYS = ("domain_tags", "responsibility_themes")
_PROJECT_LIST_KEYS = ("domain_tags", "responsibility_themes")
_ACHIEVEMENT_LIST_KEYS = ("domain_tags",)


def _normalize_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text


def _normalize_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        text = _normalize_optional_text(value)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen_values:
            continue
        seen_values.add(lowered)
        normalized.append(lowered)
    return normalized


def _normalize_profile_alignment_metadata(profile: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(profile)

    preferences = dict(normalized.get("preferences") or {})
    for key in _PREFERENCE_TEXT_KEYS:
        text = _normalize_optional_text(preferences.get(key))
        if text is not None:
            preferences[key] = text
    for key in _PREFERENCE_LIST_KEYS:
        if key in preferences:
            preferences[key] = _normalize_text_list(preferences.get(key))
    normalized["preferences"] = preferences

    experiences: list[dict[str, Any]] = []
    for experience in normalized.get("experiences") or []:
        if not isinstance(experience, dict):
            experiences.append(experience)
            continue
        normalized_experience = dict(experience)
        role_family = _normalize_optional_text(normalized_experience.get("role_family"))
        if role_family is not None:
            normalized_experience["role_family"] = role_family.lower()
        for key in _EXPERIENCE_LIST_KEYS:
            if key in normalized_experience:
                normalized_experience[key] = _normalize_text_list(normalized_experience.get(key))
        experiences.append(normalized_experience)
    normalized["experiences"] = experiences

    projects: list[dict[str, Any]] = []
    for project in normalized.get("projects") or []:
        if not isinstance(project, dict):
            projects.append(project)
            continue
        normalized_project = dict(project)
        for key in _PROJECT_LIST_KEYS:
            if key in normalized_project:
                normalized_project[key] = _normalize_text_list(normalized_project.get(key))
        projects.append(normalized_project)
    normalized["projects"] = projects

    achievements: list[dict[str, Any]] = []
    for achievement in normalized.get("achievements") or []:
        if not isinstance(achievement, dict):
            achievements.append(achievement)
            continue
        normalized_achievement = dict(achievement)
        for key in _ACHIEVEMENT_LIST_KEYS:
            if key in normalized_achievement:
                normalized_achievement[key] = _normalize_text_list(normalized_achievement.get(key))
        achievements.append(normalized_achievement)
    normalized["achievements"] = achievements

    return normalized


# ── loading ───────────────────────────────────────────────────────────────────

def load_profile_yaml(path: str | Path) -> dict[str, Any]:
    """Load and return the candidate profile from a YAML file.

    Raises:
        FileNotFoundError: if the file does not exist.
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Candidate profile not found: {file_path}")
    with open(file_path, encoding="utf-8") as f:
        loaded = yaml.safe_load(f)
    if not isinstance(loaded, dict):
        raise ValueError(f"Candidate profile must be a YAML object, got {type(loaded).__name__}")
    return _normalize_profile_alignment_metadata(cast(dict[str, Any], loaded))


def load_profile_json_text(payload: str) -> dict[str, Any]:
    """Parse and validate a candidate profile from raw JSON text.

    Raises:
        ValueError: if payload is not valid JSON, not a top-level object,
                    or fails existing `validate_profile()` validation.
    """
    import json
    try:
        profile = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in candidate profile: {exc}") from exc

    if not isinstance(profile, dict):
        raise ValueError(
            f"Candidate profile must be a JSON object, got {type(profile).__name__}"
        )

    errors = validate_profile(profile)
    if errors:
        raise ValueError(f"Candidate profile validation failed: {'; '.join(errors)}")

    return _normalize_profile_alignment_metadata(profile)  # type: ignore[return-value]
def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9_]+", " ", value.lower())).strip()


def _display_role_title(value: str) -> str:
    parts = []
    for part in value.split():
        if part in _UPPERCASE_ROLE_PARTS:
            parts.append(part.upper())
        else:
            parts.append(part.capitalize())
    return " ".join(parts)


def _role_taxonomy(config: dict[str, Any] | None) -> dict[str, Any]:
    raw_taxonomy = (config or {}).get("role_taxonomy")
    if not isinstance(raw_taxonomy, dict):
        return {}
    return raw_taxonomy


def _canonical_role_map(config: dict[str, Any] | None) -> dict[str, str]:
    raw_map = _role_taxonomy(config).get("canonical_role_by_alias")
    if not isinstance(raw_map, dict):
        return {}
    return {
        _normalize_text(str(alias)): _normalize_text(str(canonical))
        for alias, canonical in raw_map.items()
        if _normalize_text(str(alias)) and _normalize_text(str(canonical))
    }


def _role_family_map(config: dict[str, Any] | None) -> dict[str, str]:
    raw_map = _role_taxonomy(config).get("role_family_by_role")
    if not isinstance(raw_map, dict):
        return {}
    return {
        _normalize_text(str(role)): _normalize_text(str(family))
        for role, family in raw_map.items()
        if _normalize_text(str(role)) and _normalize_text(str(family))
    }


def _strip_role_noise(normalized_role: str) -> str:
    filtered_tokens = [token for token in normalized_role.split() if token not in _ROLE_NOISE_TOKENS]
    return " ".join(filtered_tokens).strip()


def _first_matching_role_alias(role_text: str, alias_map: dict[str, str]) -> str | None:
    for candidate in (role_text, _strip_role_noise(role_text)):
        if not candidate:
            continue
        direct_match = alias_map.get(candidate)
        if direct_match:
            return direct_match
        for alias in sorted(alias_map.keys(), key=len, reverse=True):
            if alias and alias in candidate:
                return alias_map[alias]
    return None


def canonicalize_role_title(role_text: str | None, config: dict[str, Any] | None = None) -> str | None:
    normalized_role = _normalize_text(role_text)
    if not normalized_role:
        return None
    alias_map = _canonical_role_map(config)
    if not alias_map:
        return None
    return _first_matching_role_alias(normalized_role, alias_map)


def infer_role_family(
    role_text: str | None,
    *,
    explicit_family: str | None = None,
    config: dict[str, Any] | None = None,
) -> str | None:
    normalized_explicit = _normalize_text(explicit_family)
    if normalized_explicit:
        return normalized_explicit

    role_family_by_role = _role_family_map(config)
    normalized_role = _normalize_text(role_text)
    if not normalized_role:
        return None

    if role_family_by_role:
        canonical_role = canonicalize_role_title(role_text, config)
        if canonical_role:
            configured_family = role_family_by_role.get(canonical_role)
            if configured_family:
                return configured_family
        direct_family = role_family_by_role.get(normalized_role)
        if direct_family:
            return direct_family

    return _first_matching_role_alias(normalized_role, _FALLBACK_ROLE_FAMILY_BY_ROLE)


def _is_missing_preference(value: Any) -> bool:
    return value in (None, "", [])


def _rank_weighted_labels(
    weighted_labels: list[tuple[str, int, int]],
    *,
    limit: int,
) -> list[str]:
    totals: dict[str, int] = {}
    first_seen: dict[str, int] = {}
    for label, weight, seen_index in weighted_labels:
        if not label:
            continue
        totals[label] = totals.get(label, 0) + weight
        first_seen.setdefault(label, seen_index)
    ordered = sorted(
        totals,
        key=lambda label: (-totals[label], first_seen[label], label),
    )
    return ordered[:limit]


def _infer_target_role(profile: dict[str, Any], config: dict[str, Any] | None) -> str | None:
    experiences = list(profile.get("experiences") or [])[:_ROLE_INFERENCE_LIMIT]
    weighted_roles: list[tuple[str, int, int]] = []
    total_experiences = len(experiences)
    for index, experience in enumerate(experiences):
        canonical_role = canonicalize_role_title(str(experience.get("role") or ""), config)
        if not canonical_role:
            continue
        weighted_roles.append((canonical_role, total_experiences - index, index))
    ranked_roles = _rank_weighted_labels(weighted_roles, limit=1)
    if not ranked_roles:
        return None
    return _display_role_title(ranked_roles[0])


def _infer_role_families(profile: dict[str, Any], config: dict[str, Any] | None) -> list[str]:
    experiences = list(profile.get("experiences") or [])[:_ROLE_INFERENCE_LIMIT]
    weighted_families: list[tuple[str, int, int]] = []
    total_experiences = len(experiences)
    for index, experience in enumerate(experiences):
        family = infer_role_family(
            str(experience.get("role") or ""),
            explicit_family=str(experience.get("role_family") or "") or None,
            config=config,
        )
        if not family:
            continue
        weighted_families.append((family, total_experiences - index, index))
    return _rank_weighted_labels(weighted_families, limit=_MAX_INFERRED_ROLE_FAMILIES)


def _infer_domains(profile: dict[str, Any]) -> list[str]:
    weighted_domains: list[tuple[str, int, int]] = []
    experiences = list(profile.get("experiences") or [])
    total_experiences = len(experiences)
    for index, experience in enumerate(experiences):
        weight = total_experiences - index
        for domain in experience.get("domain_tags") or []:
            normalized_domain = _normalize_text(str(domain))
            if normalized_domain:
                weighted_domains.append((normalized_domain, weight, index))

    projects = list(profile.get("projects") or [])
    project_offset = len(weighted_domains) + len(experiences)
    for index, project in enumerate(projects):
        for domain in project.get("domain_tags") or []:
            normalized_domain = _normalize_text(str(domain))
            if normalized_domain:
                weighted_domains.append((normalized_domain, 1, project_offset + index))

    return _rank_weighted_labels(weighted_domains, limit=_MAX_INFERRED_DOMAINS)


def infer_effective_preferences(
    profile: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    preferences = dict(profile.get("preferences") or {})
    inferred_preferences: dict[str, Any] = {}
    preference_sources: dict[str, str] = {}

    if _is_missing_preference(preferences.get("target_role")):
        inferred_target_role = _infer_target_role(profile, config)
        if inferred_target_role:
            inferred_preferences["target_role"] = inferred_target_role

    if _is_missing_preference(preferences.get("role_families")):
        inferred_role_families = _infer_role_families(profile, config)
        if inferred_role_families:
            inferred_preferences["role_families"] = inferred_role_families

    if _is_missing_preference(preferences.get("domains")):
        inferred_domains = _infer_domains(profile)
        if inferred_domains:
            inferred_preferences["domains"] = inferred_domains

    effective_preferences = dict(preferences)
    for key, inferred_value in inferred_preferences.items():
        if _is_missing_preference(preferences.get(key)):
            effective_preferences[key] = inferred_value

    for key, value in effective_preferences.items():
        if value in (None, "", []):
            continue
        preference_sources[key] = (
            "explicit_yaml"
            if not _is_missing_preference(preferences.get(key))
            else {
                "target_role": "inferred_recent_experience",
                "role_families": "inferred_role_family_map",
                "domains": "inferred_profile_domain_tags",
            }.get(key, "inferred")
        )

    return {
        "preferences": preferences,
        "inferred_preferences": inferred_preferences,
        "effective_preferences": effective_preferences,
        "preference_sources": preference_sources,
    }


# ── validation ────────────────────────────────────────────────────────────────

def validate_profile(profile: dict[str, Any]) -> list[str]:
    """Return a list of validation error strings; empty list means valid.

    Checks:
    1. Required sections are present
    2. All exp/proj/ach IDs are globally unique
    3. No dangling evidence_refs (every ref must resolve to a known ID)
    """
    errors: list[str] = []

    # ── 1. required sections ──────────────────────────────────────────────────
    for section in _REQUIRED_SECTIONS:
        if section not in profile:
            errors.append(f"Missing required section: '{section}'")

    if errors:
        return errors  # ID checks require sections; bail early

    # ── 2. ID uniqueness ──────────────────────────────────────────────────────
    all_ids: list[str] = (
        [str(e.get("id", "")) for e in profile.get("experiences", [])]
        + [str(p.get("id", "")) for p in profile.get("projects", [])]
        + [str(a.get("id", "")) for a in profile.get("achievements", [])]
        + [str(ed.get("id", "")) for ed in profile.get("education", [])]
    )
    seen_ids: set[str] = set()
    for id_val in all_ids:
        if not id_val:
            errors.append("Found an experience/project/achievement without an 'id' field")
        elif id_val in seen_ids:
            errors.append(f"Duplicate ID '{id_val}' in candidate profile")
        else:
            seen_ids.add(id_val)

    # ── 3. dangling evidence_refs ─────────────────────────────────────────────
    known_ids: set[str] = set(all_ids)
    for skill in profile.get("skills", []):
        for ref in skill.get("evidence_refs", []):
            if ref not in known_ids:
                errors.append(
                    f"Dangling evidence_ref '{ref}' in skill '{skill.get('name', '?')}'"
                )
    for ach in profile.get("achievements", []):
        for ref in ach.get("evidence_refs", []):
            if ref not in known_ids:
                errors.append(
                    f"Dangling evidence_ref '{ref}' in achievement '{ach.get('id', '?')}'"
                )

    return errors



# ── skill extraction ──────────────────────────────────────────────────────────

def flatten_skills(profile: dict[str, Any]) -> list[str]:
    """Return a deduplicated list of all skills mentioned in the profile.

    Collects from:
    - `skills[].name` (explicit skill inventory)
    - `experiences[].bullets[].skills`
    - `projects[].skills`
    """
    seen: set[str] = set()
    result: list[str] = []

    def _add(skill: str) -> None:
        if skill and skill not in seen:
            seen.add(skill)
            result.append(skill)

    # Explicit skill inventory
    for skill in profile.get("skills", []):
        if isinstance(skill, dict):
            _add(str(skill.get("name", "")))
        else:
            _add(str(skill))

    # Experience bullets
    for exp in profile.get("experiences", []):
        for bullet in exp.get("bullets", []):
            for skill in bullet.get("skills", []):
                _add(str(skill))

    # Projects
    for project in profile.get("projects", []):
        for skill in project.get("skills", []):
            _add(str(skill))

    return result


# ── BQ row preparation ────────────────────────────────────────────────────────

def prepare_profile_rows(profile: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Map a candidate profile dict to BQ table row lists.

    Returns a dict with keys: profile, experiences, projects, skills, achievements.
    Each value is a list of row dicts ready for BigQuery insertion.
    """
    now = datetime.now(tz=timezone.utc).isoformat()
    profile_id = str(uuid.uuid4())
    prefs = profile.get("preferences", {})

    # ── candidate_profile (1 row) ─────────────────────────────────────────────
    profile_rows: list[dict[str, Any]] = [{
        "profile_id":                 profile_id,
        "name":                       profile.get("name", ""),
        "headline":                   profile.get("headline", ""),
        "summary":                    profile.get("summary", ""),
        "location_types":             prefs.get("location_types", []),
        "domains":                    prefs.get("domains", []),
        "seniority_target":           prefs.get("seniority_target", ""),
        "exclude_contract_types":     prefs.get("exclude_contract_types", []),
        "exclude_experience_levels":  prefs.get("exclude_experience_levels", []),
        "updated_at":                 now,
    }]

    # ── candidate_experiences (1 row per bullet) ──────────────────────────────
    experience_rows: list[dict[str, Any]] = []
    for exp in profile.get("experiences", []):
        exp_id = str(exp.get("id", ""))
        for idx, bullet in enumerate(exp.get("bullets", [])):
            experience_rows.append({
                "exp_id":           exp_id,
                "role":             exp.get("role", ""),
                "company":          exp.get("company", ""),
                "location":         exp.get("location", ""),
                "start_date":       exp.get("start", ""),
                "end_date":         exp.get("end", ""),
                "bullet_index":     idx,
                "bullet_text":      bullet.get("text", ""),
                "skills":           bullet.get("skills", []),
                "measurable_impact": bullet.get("measurable_impact", ""),
                "updated_at":       now,
            })

    # ── candidate_projects ────────────────────────────────────────────────────
    project_rows: list[dict[str, Any]] = [
        {
            "project_id":     str(proj.get("id", "")),
            "name":           proj.get("name", ""),
            "skills":         proj.get("skills", []),
            "business_value": proj.get("business_value", ""),
            "evidence":       proj.get("evidence", ""),
            "updated_at":     now,
        }
        for proj in profile.get("projects", [])
    ]

    # ── candidate_skills ──────────────────────────────────────────────────────
    skill_rows: list[dict[str, Any]] = [
        {
            "skill_name":    str(skill.get("name", "")),
            "level":         skill.get("level", ""),
            "years":         skill.get("years"),
            "evidence_refs": skill.get("evidence_refs", []),
            "updated_at":    now,
        }
        for skill in profile.get("skills", [])
    ]

    # ── candidate_achievements ────────────────────────────────────────────────
    achievement_rows: list[dict[str, Any]] = [
        {
            "achievement_id": str(ach.get("id", "")),
            "text":           ach.get("text", ""),
            "category":       ach.get("category", ""),
            "evidence_refs":  ach.get("evidence_refs", []),
            "updated_at":     now,
        }
        for ach in profile.get("achievements", [])
    ]

    return {
        "profile":      profile_rows,
        "experiences":  experience_rows,
        "projects":     project_rows,
        "skills":       skill_rows,
        "achievements": achievement_rows,
    }


# ── integration: BigQuery load ────────────────────────────────────────────────

def load_candidate_to_bigquery(
    profile: dict[str, Any],
    config: dict[str, Any],
) -> None:
    """Insert all candidate profile tables into BigQuery.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.
    """
    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)

    rows_by_table = prepare_profile_rows(profile)
    table_map = {
        "profile":      "candidate_profile",
        "experiences":  "candidate_experiences",
        "projects":     "candidate_projects",
        "skills":       "candidate_skills",
        "achievements": "candidate_achievements",
    }

    for key, table_suffix in table_map.items():
        rows = rows_by_table[key]
        if not rows:
            continue
        table_ref = f"{project}.{dataset}.{table_suffix}"
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors for {table_suffix}: {errors}")
