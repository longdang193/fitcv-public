"""Normalize raw LinkedIn job postings.

Normalization focuses on:
- Whitespace cleanup in text fields
- Exact deduplication by job_url
- Near-duplicate detection (same JD posted in multiple cities)
- Parsing applicationsCount string to int
- Parsing salary string to structured dict

Public API
----------
normalize_whitespace        : collapse excessive whitespace/newlines
deduplicate_jobs            : exact dedupe by job_url
deduplicate_near_duplicates : group by company_id + title + SHA-256(description)
normalize_batch_with_exclusions : normalize + dedupe while tracking dropped rows
parse_applications_count    : "61 applicants" → 61
parse_salary                : "€45,000/yr - €55,000/yr" → {min, max, currency, period}
normalize_job               : orchestrate cleaning on one job dict
normalize_batch             : apply normalization + deduplication to a full list
"""

import hashlib
import re
from typing import Any

from fitcv.ingest import snake_case_keys

# ── whitespace ────────────────────────────────────────────────────────────────

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_whitespace(text: str) -> str:
    """Collapse runs of whitespace (including newlines) to single spaces and strip."""
    return _WHITESPACE_RE.sub(" ", text).strip()


# ── exact deduplication ───────────────────────────────────────────────────────

def deduplicate_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove exact duplicates by job_url, preserving insertion order."""
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for job in jobs:
        url: str = job.get("job_url", "")
        if url not in seen:
            seen.add(url)
            result.append(job)
    return result


# ── near-duplicate deduplication ─────────────────────────────────────────────

def _description_hash(description: str) -> str:
    """SHA-256 of the raw description string (before whitespace normalisation)."""
    return hashlib.sha256(description.encode("utf-8")).hexdigest()


def deduplicate_near_duplicates(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove near-duplicates: same company_id + title + description hash.

    Mindrift posts the same JD in Berlin, Cologne, and Hamburg with different
    job_urls. This groups them and keeps only the first occurrence.
    """
    seen: set[tuple[str, str, str]] = set()
    result: list[dict[str, Any]] = []
    for job in jobs:
        key = (
            str(job.get("company_id", "")),
            str(job.get("title", "")),
            _description_hash(str(job.get("description", ""))),
        )
        if key not in seen:
            seen.add(key)
            result.append(job)
    return result


def normalize_batch_with_exclusions(
    jobs: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize and deduplicate jobs while tracking rows removed from the run.

    Returns:
        A tuple of:
        - normalized jobs that continue in the pipeline
        - excluded normalized jobs annotated with ``dedupe_reason`` and ``input_index``
    """
    normalized = [normalize_job(job) for job in jobs]

    exact_kept_indices: set[int] = set()
    excluded: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for input_index, job in enumerate(normalized):
        url = str(job.get("job_url", ""))
        if url in seen_urls:
            excluded.append({
                **job,
                "input_index": input_index,
                "dedupe_reason": "duplicate_job_url",
            })
            continue
        seen_urls.add(url)
        exact_kept_indices.add(input_index)

    final_kept: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for input_index, job in enumerate(normalized):
        if input_index not in exact_kept_indices:
            continue
        key = (
            str(job.get("company_id", "")),
            str(job.get("title", "")),
            _description_hash(str(job.get("description", ""))),
        )
        if key in seen_keys:
            excluded.append({
                **job,
                "input_index": input_index,
                "dedupe_reason": "near_duplicate_job_posting",
            })
            continue
        seen_keys.add(key)
        final_kept.append(job)

    excluded.sort(key=lambda row: int(row.get("input_index", 0)))
    return final_kept, excluded


# ── applicationsCount parsing ─────────────────────────────────────────────────

# Matches the leading number in strings like "61 applicants" or "Over 200 applicants"
_APPS_COUNT_RE = re.compile(r"(?:Over\s+)?(\d+)\s+applicants?", re.IGNORECASE)
# Matches "Be among the first N applicants" pattern → treat as 0 (very early)
_AMONG_FIRST_RE = re.compile(r"Be among the first", re.IGNORECASE)


def parse_applications_count(raw: str) -> int | None:
    """Parse a raw applicants string to an integer.

    Returns:
        - Integer count for "61 applicants", "Over 200 applicants"
        - 0 for "Be among the first 25 applicants" (indicates early posting)
        - None for empty or unrecognisable strings
    """
    if not raw:
        return None
    if _AMONG_FIRST_RE.search(raw):
        return 0
    match = _APPS_COUNT_RE.search(raw)
    if match:
        return int(match.group(1))
    return None


# ── salary parsing ────────────────────────────────────────────────────────────

# Currency symbol → ISO code
_CURRENCY_SYMBOLS: dict[str, str] = {
    "€": "EUR",
    "$": "USD",
    "£": "GBP",
}

# Matches "€45,000.00/yr" / "$100.00/hr" style salary parts
_SALARY_PART_RE = re.compile(
    r"([€$£])([\d,]+(?:\.\d+)?)/(\w+)"
)


def parse_salary(raw: str) -> dict[str, Any] | None:
    """Parse a raw salary string into a structured dict.

    Args:
        raw: e.g. "€45,000.00/yr - €55,000.00/yr" or ""

    Returns:
        {"min": int, "max": int, "currency": str, "period": str}
        or None if the string is empty or unparseable.
    """
    if not raw:
        return None

    matches = _SALARY_PART_RE.findall(raw)
    if not matches:
        return None

    amounts: list[int] = []
    currency = "UNKNOWN"
    period = "yr"

    for symbol, amount_str, period_raw in matches:
        currency = _CURRENCY_SYMBOLS.get(symbol, symbol)
        period = period_raw
        amounts.append(int(amount_str.replace(",", "").split(".")[0]))

    if not amounts:
        return None

    return {
        "min": min(amounts),
        "max": max(amounts),
        "currency": currency,
        "period": period,
    }


# ── per-job normalisation ─────────────────────────────────────────────────────

def normalize_job(job: dict[str, Any]) -> dict[str, Any]:
    """Apply all normalization steps to a single job dict.

    Adds computed fields:
    - `applications_count_int`: parsed integer (or None)
    - `salary_structured`: parsed dict (or None)

    Mutates a copy, does not modify the input.
    """
    result = snake_case_keys(job)

    # Whitespace-clean the description (main text field)
    if "description" in result:
        result["description"] = normalize_whitespace(str(result["description"]))

    # Parse numeric fields
    result["applications_count_int"] = parse_applications_count(
        str(result.get("applications_count", ""))
    )
    result["salary_structured"] = parse_salary(str(result.get("salary", "")))

    return result


# ── batch normalisation ────────────────────────────────────────────────────────

def normalize_batch(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize and deduplicate a list of raw LinkedIn job dicts.

    Pipeline:
    1. Per-job field normalization
    2. Exact dedupe by job_url
    3. Near-duplicate dedupe by company_id + title + description hash
    """
    deduped, _excluded = normalize_batch_with_exclusions(jobs)
    return deduped
