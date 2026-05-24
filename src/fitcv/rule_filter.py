"""@meta
name: rule_filter
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.rule_filter.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""


import logging
import json
from datetime import datetime, timezone
from typing import Any

from fitcv.config import sqlite_mode_enabled

logger = logging.getLogger(__name__)

RULE_FILTER_SIGNALS: tuple[dict[str, Any], ...] = (
    {"code": "seniority_mismatch", "label": "Seniority mismatch", "default_selected": True},
    {"code": "location_type_excluded", "label": "Location type excluded", "default_selected": True},
    {"code": "contract_type_excluded", "label": "Contract type excluded", "default_selected": True},
    {"code": "experience_level_excluded", "label": "Experience level excluded", "default_selected": True},
    {"code": "must_have_skill_missing", "label": "Missing must-have skills", "default_selected": False},
    {"code": "domain_not_preferred", "label": "Domain not preferred", "default_selected": False},
)

RULE_FILTER_SIGNAL_LABELS: dict[str, str] = {
    str(item["code"]): str(item["label"]) for item in RULE_FILTER_SIGNALS
}
DEFAULT_SELECTED_RULE_FILTERS: list[str] = [
    str(item["code"]) for item in RULE_FILTER_SIGNALS if bool(item["default_selected"])
]
KNOWN_RULE_FILTER_SIGNAL_CODES: set[str] = set(RULE_FILTER_SIGNAL_LABELS)


# ── built-in fallbacks (used when config is not passed) ───────────────────────
# Kept here so unit tests that don't inject config still pass.

_FALLBACK_SENIORITY_LADDER: list[str] = [
    "intern", "entry", "associate", "mid", "senior", "lead", "manager", "director",
]

_FALLBACK_SENIORITY_ALIASES: dict[str, str] = {
    "junior": "entry", "jr": "entry", "sr": "senior",
    "staff": "lead", "principal": "lead", "vp": "director", "vice president": "director",
}

_FALLBACK_SKILL_SYNONYMS: dict[str, str] = {
    "gcp": "google cloud", "google cloud platform": "google cloud",
    "ga4": "google analytics", "google analytics (ga4)": "google analytics",
    "bigquery": "google bigquery", "big query": "google bigquery",
    "k8s": "kubernetes", "aws": "amazon web services", "azure": "microsoft azure",
    "ml": "machine learning", "bigquery ml": "machine learning", "nlp": "natural language processing",
    "gemini": "genai", "vertex ai": "genai",
    "llm": "genai", "llms": "genai", "large language models": "genai",
    "rag": "genai", "prompt engineering": "genai", "vector databases": "genai",
    "postgres": "postgresql", "pg": "postgresql",
    "powerbi": "power bi", "github": "git", "git / github": "git",
}


# ── config helpers ────────────────────────────────────────────────────────────


def _normalized_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip().lower() for item in value if str(item).strip()]
    return []


def _safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback

def _get_seniority_ladder(config: dict[str, Any] | None) -> list[str]:
    """Return the ordered seniority ladder from config, or the built-in fallback."""
    if config:
        seniority = config.get("seniority", {})
        if isinstance(seniority, dict) and seniority.get("ladder"):
            return list(seniority["ladder"])
    return _FALLBACK_SENIORITY_LADDER


def _get_seniority_aliases(config: dict[str, Any] | None) -> dict[str, str]:
    """Return the seniority alias map from config, or the built-in fallback."""
    if config:
        seniority = config.get("seniority", {})
        if isinstance(seniority, dict) and seniority.get("aliases"):
            return {str(k).lower(): str(v).lower() for k, v in seniority["aliases"].items()}
    return _FALLBACK_SENIORITY_ALIASES


def get_skill_synonyms(config: dict[str, Any] | None) -> dict[str, str]:
    """Return the skill synonym map from config, or the built-in fallback."""
    if config:
        synonyms = config.get("skill_synonyms")
        if isinstance(synonyms, dict) and synonyms:
            return {str(k).lower(): str(v).lower() for k, v in synonyms.items()}
    return _FALLBACK_SKILL_SYNONYMS


def _get_preferred_domains(prefs: dict[str, Any]) -> list[str]:
    domains = _normalized_string_list(prefs.get("domains"))
    if domains:
        return domains
    return _normalized_string_list(prefs.get("preferred_domains"))


def _get_excluded_contract_types(prefs: dict[str, Any]) -> list[str]:
    return _normalized_string_list(prefs.get("exclude_contract_types"))


def _get_selected_rule_filter_codes(config: dict[str, Any] | None) -> set[str]:
    rule_filter_cfg = (config or {}).get("rule_filter")
    selected_filters = (
        rule_filter_cfg.get("selected_filters")
        if isinstance(rule_filter_cfg, dict)
        else None
    )
    if not isinstance(selected_filters, list) or not selected_filters:
        return set(DEFAULT_SELECTED_RULE_FILTERS)

    normalized = {
        str(item).strip()
        for item in selected_filters
        if str(item).strip() in KNOWN_RULE_FILTER_SIGNAL_CODES
    }
    return normalized or set(DEFAULT_SELECTED_RULE_FILTERS)


# ── seniority normalisation ───────────────────────────────────────────────────

def _normalise_seniority(
    raw: str | None,
    config: dict[str, Any] | None = None,
) -> str | None:
    """Map raw seniority string to a canonical ladder value, or None if unknown."""
    if not raw:
        return None
    ladder = _get_seniority_ladder(config)
    aliases = _get_seniority_aliases(config)
    lowered = raw.strip().lower()
    mapped = aliases.get(lowered, lowered)
    return mapped if mapped in ladder else None


def check_seniority(
    job: dict[str, Any],
    prefs: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> bool:
    """Return True if the job seniority is within ±1 step of the target.

    Rules:
    - target ± 1 step → pass
    - target + 2 or more → reject (too senior)
    - target - 2 or more → reject (too junior)
    - unknown seniority (None / unrecognised) → pass with warning
    """
    ladder = _get_seniority_ladder(config)
    target_raw = prefs.get("seniority_target", "")
    job_raw = job.get("seniority")

    target = _normalise_seniority(target_raw, config)
    job_seniority = _normalise_seniority(job_raw, config)

    if target is None:
        logger.warning("Unknown seniority_target '%s' in preferences — skipping check", target_raw)
        return True
    if job_seniority is None:
        logger.warning("Job '%s' has unknown seniority '%s' — keeping", job.get("job_url"), job_raw)
        return True

    target_idx = ladder.index(target)
    job_idx = ladder.index(job_seniority)
    return -1 <= (job_idx - target_idx) <= 1


# ── skill canonicalisation ────────────────────────────────────────────────────

def canonicalize_skill(skill: str, config: dict[str, Any] | None = None) -> str:
    """Return the canonical form of a skill name (lower-cased, synonym-resolved)."""
    synonyms = get_skill_synonyms(config)
    lower = skill.strip().lower()
    return synonyms.get(lower, lower)


# ── individual checks ─────────────────────────────────────────────────────────

def check_location_type(job: dict[str, Any], prefs: dict[str, Any]) -> bool:
    """Return True if job location_type matches any preferred type.

    Empty preferred_locations = no preference → accept everything.
    """
    allowed = _normalized_string_list(prefs.get("location_types"))
    if not allowed:
        return True
    location_type = str(job.get("location_type") or "").lower()
    if not location_type:
        return True
    return location_type in allowed


def check_contract_type(job: dict[str, Any], prefs: dict[str, Any]) -> bool:
    """Return True if job contract_type is permitted by include/exclude prefs."""
    contract_type = str(job.get("contract_type") or "").lower()
    excluded = _get_excluded_contract_types(prefs)
    if excluded and contract_type in excluded:
        return False

    allowed = _normalized_string_list(prefs.get("contract_types"))
    if not allowed:
        return True
    return contract_type in allowed


def check_experience_level(job: dict[str, Any], prefs: dict[str, Any]) -> bool:
    """Return True if job experience_level is NOT in the exclusion list.

    experience_level (raw LinkedIn label) is used for exclusion only.
    seniority (LLM-normalised) is the primary signal — handled by check_seniority.
    """
    excluded = _normalized_string_list(prefs.get("exclude_experience_levels"))
    experience_level = str(job.get("experience_level") or "").lower()
    if experience_level == "entry level":
        return True
    return experience_level not in excluded


def check_must_have_skills(
    job: dict[str, Any],
    prefs: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> bool:
    """Return True if all must-have skills appear in the job's required_skills.

    Uses the synonym map (from config or built-in fallback) and case-insensitive
    comparison before checking overlap.
    """
    must_haves = prefs.get("must_have_skills", [])
    if not must_haves:
        return True
    canonical_job_skills = job.get("required_skills_canonical")
    if isinstance(canonical_job_skills, list) and canonical_job_skills:
        job_skills = canonical_job_skills
    else:
        job_skills = job.get("required_skills") or []
    job_skills_canonical = {
        _canonicalise_skill(s, config) for s in job_skills
    }
    return all(_canonicalise_skill(skill, config) in job_skills_canonical for skill in must_haves)


def _compute_missing_must_have_skills(
    job: dict[str, Any],
    prefs: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> list[str]:
    must_haves = list(prefs.get("must_have_skills") or [])
    if not must_haves:
        return []
    canonical_job_skills = job.get("required_skills_canonical")
    if isinstance(canonical_job_skills, list) and canonical_job_skills:
        job_skills = canonical_job_skills
    else:
        job_skills = job.get("required_skills") or []
    job_skills_canonical = {
        _canonicalise_skill(str(skill), config)
        for skill in job_skills
    }
    missing: list[str] = []
    for skill in must_haves:
        canonical_skill = _canonicalise_skill(str(skill), config)
        if canonical_skill not in job_skills_canonical:
            missing.append(canonical_skill)
    return missing


def _build_rule_filter_mark(
    reason_code: str,
    job: dict[str, Any],
    prefs: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if reason_code == "must_have_skill_missing":
        missing_skills = _compute_missing_must_have_skills(job, prefs, config)
        return {
            "code": reason_code,
            "message": "Missing must-have skills",
            "details": {
                "missing_skills": missing_skills,
                "missing_count": len(missing_skills),
            },
        }
    if reason_code == "domain_not_preferred":
        return {
            "code": reason_code,
            "message": "Job domain is outside preferred domains",
            "details": {
                "job_domain": str(job.get("domain") or "").lower(),
                "preferred_domains": _get_preferred_domains(prefs),
            },
        }
    return {
        "code": reason_code,
        "message": RULE_FILTER_SIGNAL_LABELS.get(reason_code, reason_code.replace("_", " ")),
    }


def check_freshness(
    job: dict[str, Any],
    global_settings: dict[str, Any] | None = None,
) -> bool:
    """Return True if published_at is within the admin-configured max_age_days window.

    Reads max_age_days from global_settings (admin-managed). Falls back to 30 days
    if not configured. Does NOT read prefs.max_age_days — freshness is a global
    admin-managed filter, not a candidate preference.

    Missing or unparseable published_at → pass (fail open).
    """
    if global_settings is not None:
        max_age = _safe_int(global_settings.get("global_job_filters.max_age_days", 30), 30)
    else:
        max_age = 30
    published_at = job.get("published_at")
    if not published_at:
        return True
    try:
        if isinstance(published_at, str):
            pub_date = datetime.fromisoformat(published_at.split("T")[0]).replace(tzinfo=timezone.utc)
        else:
            pub_date = published_at
        return (datetime.now(tz=timezone.utc) - pub_date).days <= max_age
    except (ValueError, TypeError):
        logger.warning("Could not parse published_at '%s' — keeping job", published_at)
        return True


def check_applicant_count(
    job: dict[str, Any],
    global_settings: dict[str, Any],
) -> bool:
    """Return True if applications_count is within the admin-configured threshold.

    NULL applications_count → pass (fail open).
    No configured threshold → pass (filter disabled).
    """
    max_count = global_settings.get("global_job_filters.applications_count_max")
    if max_count is None:
        return True
    # Prefer the parsed integer from normalize.py; fall back to raw string field
    count = job.get("applications_count_int", job.get("applications_count"))
    if count is None:
        return True  # fail open
    try:
        return int(count) <= int(max_count)
    except (ValueError, TypeError):
        return True  # unparseable — fail open


def check_domain_preference(job: dict[str, Any], prefs: dict[str, Any]) -> bool:
    """Return True if the job domain matches any preferred domain.

    Empty preferred_domains = no preference → accept everything.
    """
    preferred = _get_preferred_domains(prefs)
    if not preferred:
        return True
    job_family = str(job.get("job_family") or "").lower()
    if job_family and job_family in preferred:
        return True
    domain = str(job.get("domain") or "").lower()
    if not domain:
        return True
    return domain in preferred


# ── pre-enrichment global filter orchestrator ─────────────────────────────────

def apply_pre_enrichment_global_filters(
    jobs: list[dict[str, Any]],
    global_settings: dict[str, Any] | None,
) -> dict[str, list]:
    """Apply admin-managed pre-enrichment global filters.

    Uses only ingest/normalization fields (applications_count_int, published_at).
    Runs before enrich_batch so rejected jobs do not consume LLM/API budget.

    Returns the same shape as apply_rule_filters:
        {"passed": [job_url, ...], "rejected": [{"job_url": ..., "reasons": [...]}, ...]}

    When global_settings is None or empty, all jobs pass (filters disabled).
    """
    if not global_settings:
        return {
            "passed": [str(j.get("job_url", "")) for j in jobs],
            "rejected": [],
        }

    checks: list[tuple[str, Any]] = [
        ("job_too_stale",               lambda j: check_freshness(j, global_settings)),
        ("applications_count_exceeded", lambda j: check_applicant_count(j, global_settings)),
    ]

    passed: list[str] = []
    rejected: list[dict[str, Any]] = []
    for job in jobs:
        url = str(job.get("job_url", ""))
        failed = [reason for reason, fn in checks if not fn(job)]
        if failed:
            rejected.append({"job_url": url, "reasons": failed})
        else:
            passed.append(url)

    return {"passed": passed, "rejected": rejected}


# ── orchestrator ──────────────────────────────────────────────────────────────

def apply_rule_filters(
    jobs: list[dict[str, Any]],
    prefs: dict[str, Any],
    config: dict[str, Any] | None = None,
    global_settings: dict[str, Any] | None = None,
) -> dict[str, list]:
    """Apply all policy checks and return {passed, rejected}.

    Return contract:
        {
            "passed": ["url1", "url3", ...],
            "rejected": [
                {"job_url": "url2", "reasons": ["seniority_mismatch", "contract_type_excluded"]}
            ]
        }

    config: merged config dict (from load_config). When None, built-in fallbacks apply.
    global_settings: retained for backward compatibility with direct callers; no longer
        used by the pipeline (global checks now run pre-enrichment via
        apply_pre_enrichment_global_filters). Freshness and applicant-count checks
        are not applied here regardless of this value.

    Note: experience_level is used for exclusion only. seniority is the primary signal.
    Conflicts (e.g. experience_level=Entry + seniority=mid) are logged but not auto-rejected.
    """
    checks: list[tuple[str, Any]] = [
        ("seniority_mismatch",        lambda j, p: check_seniority(j, p, config)),
        ("location_type_excluded",    check_location_type),
        ("contract_type_excluded",    check_contract_type),
        ("experience_level_excluded", check_experience_level),
        ("must_have_skill_missing",   lambda j, p: check_must_have_skills(j, p, config)),
        ("domain_not_preferred",      check_domain_preference),
    ]

    selected_filter_codes = _get_selected_rule_filter_codes(config)
    passed: list[str] = []
    passed_records: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for job in jobs:
        reasons: list[str] = []
        marks: list[dict[str, Any]] = []
        for reason_code, check_fn in checks:
            if not check_fn(job, prefs):
                if reason_code in selected_filter_codes:
                    reasons.append(reason_code)
                else:
                    marks.append(_build_rule_filter_mark(reason_code, job, prefs, config))

        # Log seniority / experience_level conflicts (do not auto-reject)
        exp_level = (job.get("experience_level") or "").lower()
        seniority = _normalise_seniority(job.get("seniority"), config)
        if exp_level in ("entry level", "internship") and seniority not in (None, "entry", "intern"):
            logger.info(
                "Conflict: experience_level='%s', seniority='%s' for job '%s'",
                job.get("experience_level"), job.get("seniority"), job.get("job_url"),
            )

        if reasons:
            rejected.append({
                "job_url": str(job.get("job_url", "")),
                "reasons": reasons,
                "marks": marks,
            })
        else:
            job_url = str(job.get("job_url", ""))
            passed.append(job_url)
            passed_records.append({"job_url": job_url, "marks": marks})

    return {"passed": passed, "passed_records": passed_records, "rejected": rejected}


# ── integration: persist to BigQuery ─────────────────────────────────────────

def store_filter_results(
    result: dict[str, list],
    run_id: str,
    config: dict[str, Any],
) -> None:
    """Insert rule filter results into fitcv.rule_filter_results.

    Each row includes run_id so the admin UI can show reject reasons for a
    specific run (rather than mixing results across all runs for the same job).

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.
    """
    if sqlite_mode_enabled(config):
        return
    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])
    if key_path:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bigquery.Client(project=project, credentials=credentials)
    else:
        client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.rule_filter_results"
    now = datetime.now(tz=timezone.utc).isoformat()

    rows: list[dict[str, Any]] = []
    passed_records_by_url = {
        str(item.get("job_url") or ""): item
        for item in result.get("passed_records", [])
        if str(item.get("job_url") or "")
    }
    for job_url in result.get("passed", []):
        rows.append({
            "job_url": job_url, "passed": True, "reasons": [],
            "marks_json": json.dumps(
                list((passed_records_by_url.get(str(job_url)) or {}).get("marks") or []),
                ensure_ascii=False,
            ),
            "filtered_at": now, "run_id": run_id,
        })
    for item in result.get("rejected", []):
        rows.append({
            "job_url": item["job_url"], "passed": False,
            "reasons": item["reasons"],
            "marks_json": json.dumps(list(item.get("marks") or []), ensure_ascii=False),
            "filtered_at": now,
            "run_id": run_id,
        })

    if rows:
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors for rule_filter_results: {errors}")


def _get_skill_synonyms(config: dict[str, Any] | None) -> dict[str, str]:
    """Backward-compatible private alias for legacy imports."""
    return get_skill_synonyms(config)


def _canonicalise_skill(skill: str, config: dict[str, Any] | None = None) -> str:
    """Backward-compatible private alias for legacy imports."""
    return canonicalize_skill(skill, config)
