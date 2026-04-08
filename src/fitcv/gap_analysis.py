"""Gap analysis — classify skill and experience fit between a candidate and a job.

Public API
----------
normalise_raw_skill    : light normalisation for raw-string comparison
classify_skill_match   : classify one required skill as matched / partial / missing
compute_gap            : classify all required skills + flag years/overclaim risks
classify_fit           : map a gap result to a fit label (strong/stretch/skip)
store_gap_analysis     : persist gap result to BigQuery (integration)

Skill matching rule
-------------------
Two-level matching:

  1. Raw match (→ ``matched``)
     The candidate skill string equals the required skill string after
     light normalisation (lowercase + strip). No synonym map involved.
     SQL ↔ sql → matched.

  2. Canonical synonym match (→ ``partial``)
     The raw strings differ, but both map to the same canonical skill
     through the synonym map.
     GCP ↔ Google Cloud → partial  (both → "google cloud" via synonym map)
     Apache Airflow ↔ Airflow → partial

  3. No match (→ ``missing``)
     Neither raw nor canonical synonym match.

``partial`` entries are dicts, not plain strings:
  {"required": "Google Cloud", "candidate": "GCP", "canonical": "google cloud"}

This preserves provenance for CV generation and overclaim detection.
"""

import re
from datetime import datetime, timezone
from typing import Any

from fitcv.rule_filter import _canonicalise_skill, _get_skill_synonyms


# ── config defaults ───────────────────────────────────────────────────────────

_DEFAULT_STRONG_RATIO: float = 0.80
_DEFAULT_STRETCH_RATIO: float = 0.50

# Keywords that signal a leadership/ownership requirement in the JD.
_LEADERSHIP_KEYWORDS: frozenset[str] = frozenset({
    "lead", "leading", "leadership", "owns", "ownership",
    "manage", "managing", "manager", "director", "head of",
})
_NON_SKILL_REQUIREMENT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(master'?s|phd|bachelor|degree|stud(y|ies)|university)\b"),
    re.compile(r"\b\d+\+?\s+years?\b|\byears? of\b|\bprofessional experience\b|\bhands[- ]on\b"),
    re.compile(r"\bportfolio\b|\bpublications?\b"),
    re.compile(r"\b(english|german|french|language|c1|c2|b2)\b"),
    re.compile(r"\bability to\b|\bready to\b|\blearn new methods\b|\bswitch between tasks\b|\bchallenging, complex guidelines\b"),
    re.compile(r"\brequirements management\b"),
    re.compile(r"\banalytical skills?\b"),
)


# ── skill normalisation and matching ─────────────────────────────────────────

def normalise_raw_skill(skill: str) -> str:
    """Apply light normalisation for raw-string comparison.

    Steps:
    - strip leading/trailing whitespace
    - lowercase
    - collapse runs of internal whitespace to a single space

    No synonym resolution; no semantic transformation.
    """
    return re.sub(r"\s+", " ", skill.strip().lower())


def _normalise_phrase_text(text: str) -> str:
    """Normalise free-form JD requirement text for phrase-level skill lookup."""
    lowered = normalise_raw_skill(text)
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _normalise_compact_skill(skill: str) -> str:
    """Normalise a skill for compact exact comparison across spacing/punctuation variants."""
    return re.sub(r"[^a-z0-9]+", "", normalise_raw_skill(skill))


def _skill_variants(skill: str, config: dict[str, Any] | None = None) -> set[str]:
    """Return raw/canonical alias variants for phrase-level matching."""
    synonyms = _get_skill_synonyms(config)
    canonical = _normalise_phrase_text(_canonicalise_skill(skill, config))
    variants = {
        _normalise_phrase_text(skill),
        canonical,
        _normalise_compact_skill(skill),
        _normalise_compact_skill(_canonicalise_skill(skill, config)),
    }
    for alias, canonical_value in synonyms.items():
        if _normalise_phrase_text(canonical_value) == canonical:
            variants.add(_normalise_phrase_text(alias))
            variants.add(_normalise_compact_skill(alias))
    return {variant for variant in variants if variant}


def _phrase_mentions_skill(required_skill: str, candidate_skill: str, config: dict[str, Any] | None = None) -> bool:
    """Return True when a long JD phrase explicitly mentions a candidate skill variant."""
    required_text = _normalise_phrase_text(required_skill)
    if len(required_text.split()) < 3 and not any(token in required_skill for token in ("(", ")", "/", ",", ":")):
        return False
    haystack = f" {required_text} "
    for variant in _skill_variants(candidate_skill, config):
        if f" {variant} " in haystack:
            return True
    return False


def _candidate_phrase_mentions_required_skill(required_skill: str, candidate_skill: str) -> bool:
    """Return True when a compound candidate skill explicitly contains the required skill."""
    candidate_text = _normalise_phrase_text(candidate_skill)
    if len(candidate_text.split()) < 2 and not any(token in candidate_skill for token in ("/", ",", "(", ")", "-")):
        return False
    required_variant = _normalise_phrase_text(required_skill)
    if not required_variant:
        return False
    haystack = f" {candidate_text} "
    return f" {required_variant} " in haystack


def _is_matchable_skill_requirement(required_skill: str) -> bool:
    """Return True when a requirement should count toward skill-fit ratio."""
    text = normalise_raw_skill(required_skill)
    return not any(pattern.search(text) for pattern in _NON_SKILL_REQUIREMENT_PATTERNS)


def classify_skill_match(
    required_skill: str,
    candidate_skills: list[str],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify a single required skill against the full candidate skill list.

    Returns a dict::

        {
            "result":    "matched" | "partial" | "missing",
            "required":  str,           # original required skill string
            "candidate": str | None,    # matched candidate string (raw), or None
            "canonical": str | None,    # shared canonical form for partial, or None
        }

    Matching rule (strict two levels):
    1. Raw match   — normalise_raw_skill(required) == normalise_raw_skill(candidate)
    2. Partial     — canonicals agree but raw strings differ (synonym map only)
    3. Missing     — no match at either level
    """
    req_norm = normalise_raw_skill(required_skill)
    req_canonical = _canonicalise_skill(required_skill, config)

    for cand in candidate_skills:
        cand_norm = normalise_raw_skill(cand)
        if req_norm == cand_norm:
            return {
                "result": "matched",
                "required": required_skill,
                "candidate": cand,
                "canonical": None,
            }

    for cand in candidate_skills:
        if _normalise_compact_skill(required_skill) == _normalise_compact_skill(cand):
            return {
                "result": "matched",
                "required": required_skill,
                "candidate": cand,
                "canonical": None,
            }

    for cand in candidate_skills:
        if _phrase_mentions_skill(required_skill, cand, config):
            return {
                "result": "matched",
                "required": required_skill,
                "candidate": cand,
                "canonical": None,
            }

    for cand in candidate_skills:
        if _candidate_phrase_mentions_required_skill(required_skill, cand):
            return {
                "result": "matched",
                "required": required_skill,
                "candidate": cand,
                "canonical": None,
            }

    for cand in candidate_skills:
        cand_canonical = _canonicalise_skill(cand, config)
        if cand_canonical == req_canonical:
            return {
                "result": "partial",
                "required": required_skill,
                "candidate": cand,
                "canonical": req_canonical,
            }

    return {
        "result": "missing",
        "required": required_skill,
        "candidate": None,
        "canonical": None,
    }


# ── internal helpers ──────────────────────────────────────────────────────────

def _parse_years_minimum(years_required: Any) -> int | None:
    """Parse years_required to an integer minimum.

    Handles:
    - None / 0   → None  (unknown; no penalty)
    - int        → int
    - "3-5"      → 3     (minimum of range)
    - other str  → int() if parseable, else None
    """
    if years_required is None:
        return None
    if isinstance(years_required, (int, float)):
        val = int(years_required)
        return None if val == 0 else val
    raw = str(years_required).strip()
    range_match = re.match(r"^(\d+)\s*[-–]\s*\d+$", raw)
    if range_match:
        return int(range_match.group(1))
    try:
        val = int(raw)
        return None if val == 0 else val
    except ValueError:
        return None


def _compute_years_risk(
    years_required: Any,
    years_candidate: int | float | None,
) -> bool:
    """Return True only when both sides are known and candidate falls short."""
    min_required = _parse_years_minimum(years_required)
    if min_required is None or years_candidate is None:
        return False
    return int(years_candidate) < min_required


def _has_leadership_claim(candidate_evidence: list[str]) -> bool:
    """Return True when any evidence string contains a leadership keyword."""
    lowered = " ".join(candidate_evidence).lower()
    return any(kw in lowered for kw in _LEADERSHIP_KEYWORDS)


# ── gap computation ───────────────────────────────────────────────────────────

def compute_gap(
    required_skills: list[str],
    candidate_skills: list[str],
    years_required: Any = None,
    years_candidate: int | float | None = None,
    config: dict[str, Any] | None = None,
    candidate_evidence: list[str] | None = None,
    *,
    years_experience_min: Any | None = None,
    years_experience_max: Any | None = None,
) -> dict[str, Any]:
    """Classify required skills into matched / partial / missing and flag risks.

    Skill matching uses classify_skill_match() (two strict levels):
    - matched : raw-normalised strings are equal (case-insensitive, whitespace collapsed)
    - partial : raw strings differ but share a canonical synonym
    - missing : no raw or canonical match

    ``partial`` is a list of dicts::

        {"required": str, "candidate": str, "canonical": str}

    Phase 1 years-risk uses the canonical enriched-job years inputs when
    provided. ``years_required`` remains a backward-compatibility adapter only.

    ``overclaim_risk`` entries are added when:
    - years_candidate < required minimum years

    Returns a dict with keys:
        matched       list[str]                 required skill strings
        partial       list[dict]                {required, candidate, canonical}
        missing       list[str]                 required skill strings
        years_risk    bool
        overclaim_risk list[str]
    """
    del candidate_evidence
    del years_experience_max

    effective_years_required = (
        years_experience_min
        if years_experience_min is not None
        else years_required
    )

    matched: list[str] = []
    partial: list[dict[str, Any]] = []
    missing: list[str] = []
    ignored_for_fit: list[str] = []
    matchable_required_count = 0

    for req in required_skills:
        if _is_matchable_skill_requirement(req):
            matchable_required_count += 1
        else:
            ignored_for_fit.append(req)
        classification = classify_skill_match(req, candidate_skills, config)
        result = classification["result"]
        if result == "matched":
            matched.append(req)
        elif result == "partial":
            partial.append({
                "required": classification["required"],
                "candidate": classification["candidate"],
                "canonical": classification["canonical"],
            })
        else:
            missing.append(req)

    years_risk = _compute_years_risk(effective_years_required, years_candidate)

    overclaim_risk: list[str] = []
    if years_risk:
        years_min = _parse_years_minimum(effective_years_required)
        overclaim_risk.append(
            f"years_gap: candidate has {years_candidate} years, {years_min} required"
        )

    return {
        "matched": matched,
        "partial": partial,
        "missing": missing,
        "matchable_required_count": matchable_required_count,
        "ignored_for_fit": ignored_for_fit,
        "years_risk": years_risk,
        "overclaim_risk": overclaim_risk,
    }


# ── fit classification ────────────────────────────────────────────────────────

def classify_fit(
    gap: dict[str, Any],
    required_count: int,
    config: dict[str, Any] | None = None,
) -> str:
    """Map a gap result to a fit label.

    Reads thresholds from ``config["gap_thresholds"]`` with built-in defaults:
    - strong_min_matched_ratio: 0.80  (≥80% matched → strong)
    - stretch_min_matched_ratio: 0.50 (50–79% → stretch; <50% → skip)

    Only ``matched`` (raw exact) counts toward the ratio.
    ``partial`` (synonym-only) does not count as a strong match.

    Returns "strong", "stretch", or "skip".
    """
    thresholds = (config or {}).get("gap_thresholds", {})
    strong_min: float = float(thresholds.get("strong_min_matched_ratio", _DEFAULT_STRONG_RATIO))
    stretch_min: float = float(thresholds.get("stretch_min_matched_ratio", _DEFAULT_STRETCH_RATIO))

    if required_count == 0:
        return "strong"

    matched_ratio = len(gap.get("matched") or []) / required_count

    if matched_ratio >= strong_min:
        return "strong"
    if matched_ratio >= stretch_min:
        return "stretch"
    return "skip"


# ── integration: store to bigquery ────────────────────────────────────────────

def store_gap_analysis(
    job_id: str,
    gap: dict[str, Any],
    config: dict[str, Any],
) -> None:
    """Insert a gap analysis row into fitcv.gap_analysis.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.
    """
    from google.cloud import bigquery  # type: ignore[import-not-found]
    from google.oauth2 import service_account  # type: ignore[import-not-found]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
    table_ref = f"{project}.{dataset}.gap_analysis"
    now = datetime.now(tz=timezone.utc).isoformat()

    # Serialise partial dicts to JSON strings for BQ ARRAY<STRING> storage.
    import json
    partial_serialised = [json.dumps(p) for p in (gap.get("partial") or [])]

    row = {
        "job_url": str(job_id),
        "matched_skills": list(gap.get("matched") or []),
        "partial_skills": partial_serialised,
        "missing_skills": list(gap.get("missing") or []),
        "years_risk": bool(gap.get("years_risk", False)),
        "overclaim_risk": list(gap.get("overclaim_risk") or []),
        "analysed_at": now,
    }

    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert errors for gap_analysis: {errors}")
