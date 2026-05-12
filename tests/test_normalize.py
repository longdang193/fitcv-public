"""Tests for fitcv.normalize — all pure unit tests, no external dependencies."""

import pytest
from fitcv.normalize import (
    deduplicate_jobs,
    deduplicate_near_duplicates,
    normalize_batch,
    normalize_batch_with_exclusions,
    normalize_job,
    normalize_whitespace,
    parse_applications_count,
    parse_salary,
)


# ── normalize_whitespace ──────────────────────────────────────────────────────

def test_normalize_whitespace_collapses_spaces() -> None:
    assert normalize_whitespace("  hello   world  ") == "hello world"


def test_normalize_whitespace_collapses_newlines() -> None:
    assert normalize_whitespace("hello\n\n\nworld") == "hello world"


def test_normalize_whitespace_strips_mixed() -> None:
    assert normalize_whitespace("  hello   world  \n\n") == "hello world"


def test_normalize_whitespace_empty_string() -> None:
    assert normalize_whitespace("") == ""


# ── deduplicate_jobs ──────────────────────────────────────────────────────────

def test_deduplicate_jobs_removes_exact_url_dupes() -> None:
    jobs = [
        {"job_url": "https://linkedin.com/job/1", "title": "DE"},
        {"job_url": "https://linkedin.com/job/1", "title": "DE"},
    ]
    assert len(deduplicate_jobs(jobs)) == 1


def test_deduplicate_jobs_keeps_distinct_urls() -> None:
    jobs = [
        {"job_url": "https://linkedin.com/job/1", "title": "DE"},
        {"job_url": "https://linkedin.com/job/2", "title": "DA"},
    ]
    assert len(deduplicate_jobs(jobs)) == 2


def test_deduplicate_jobs_keeps_first_occurrence() -> None:
    jobs = [
        {"job_url": "url1", "title": "first"},
        {"job_url": "url1", "title": "second"},
    ]
    result = deduplicate_jobs(jobs)
    assert result[0]["title"] == "first"


# ── deduplicate_near_duplicates ───────────────────────────────────────────────

def test_deduplicate_near_duplicates_same_company_same_jd() -> None:
    jobs = [
        {"job_url": "url1", "company_id": "101", "title": "AI Trainer", "description": "Same JD text..."},
        {"job_url": "url2", "company_id": "101", "title": "AI Trainer", "description": "Same JD text..."},
    ]
    result = deduplicate_near_duplicates(jobs)
    assert len(result) == 1
    assert result[0]["job_url"] == "url1"  # keeps first


def test_deduplicate_near_duplicates_different_company() -> None:
    jobs = [
        {"job_url": "url1", "company_id": "101", "title": "Data Engineer", "description": "Same text"},
        {"job_url": "url2", "company_id": "202", "title": "Data Engineer", "description": "Same text"},
    ]
    result = deduplicate_near_duplicates(jobs)
    assert len(result) == 2  # different companies → keep both


def test_deduplicate_near_duplicates_same_company_different_title() -> None:
    jobs = [
        {"job_url": "url1", "company_id": "101", "title": "Data Engineer", "description": "Some text"},
        {"job_url": "url2", "company_id": "101", "title": "Senior Data Engineer", "description": "Some text"},
    ]
    result = deduplicate_near_duplicates(jobs)
    assert len(result) == 2  # different titles → keep both


def test_deduplicate_near_duplicates_same_company_different_description() -> None:
    jobs = [
        {"job_url": "url1", "company_id": "101", "title": "DE", "description": "Role in Berlin"},
        {"job_url": "url2", "company_id": "101", "title": "DE", "description": "Role in Munich, different"},
    ]
    result = deduplicate_near_duplicates(jobs)
    assert len(result) == 2  # different description hash → keep both


# ── parse_applications_count ──────────────────────────────────────────────────

def test_parse_applications_count_plain_number() -> None:
    assert parse_applications_count("61 applicants") == 61


def test_parse_applications_count_over_prefix() -> None:
    assert parse_applications_count("Over 200 applicants") == 200


def test_parse_applications_count_be_among_first() -> None:
    assert parse_applications_count("Be among the first 25 applicants") == 0


def test_parse_applications_count_empty_string() -> None:
    assert parse_applications_count("") is None


def test_parse_applications_count_no_number() -> None:
    assert parse_applications_count("applicants") is None


# ── parse_salary ──────────────────────────────────────────────────────────────

def test_parse_salary_euro_yearly() -> None:
    result = parse_salary("€45,000.00/yr - €55,000.00/yr")
    assert result is not None
    assert result["min"] == 45000
    assert result["max"] == 55000
    assert result["currency"] == "EUR"
    assert result["period"] == "yr"


def test_parse_salary_empty_string() -> None:
    assert parse_salary("") is None


def test_parse_salary_dollar_hourly() -> None:
    result = parse_salary("$100.00/hr - $100.00/hr")
    assert result is not None
    assert result["min"] == 100
    assert result["max"] == 100
    assert result["currency"] == "USD"
    assert result["period"] == "hr"


# ── normalize_job + normalize_batch ────────────────────────────────────────────

def test_normalize_job_cleans_whitespace() -> None:
    job = {
        "job_url": "url1",
        "description": "  hello   world  \n\n",
        "company_id": "101",
        "title": "DE",
        "applications_count": "42 applicants",
        "salary": "",
    }
    result = normalize_job(job)
    assert result["description"] == "hello world"


def test_normalize_job_parses_applications_count() -> None:
    job = {
        "job_url": "url1",
        "description": "Some JD",
        "company_id": "101",
        "title": "DE",
        "applications_count": "Over 200 applicants",
        "salary": "",
    }
    result = normalize_job(job)
    assert result["applications_count_int"] == 200


def test_normalize_job_converts_scraper_keys_to_snake_case() -> None:
    job = {
        "jobUrl": "url1",
        "companyName": "ACME",
        "companyId": "101",
        "experienceLevel": "Entry level",
        "applicationsCount": "42 applicants",
        "description": "Some JD",
        "salary": "",
        "title": "DE",
    }
    result = normalize_job(job)
    assert result["job_url"] == "url1"
    assert result["company_name"] == "ACME"
    assert result["company_id"] == "101"
    assert result["experience_level"] == "Entry level"
    assert result["applications_count"] == "42 applicants"


def test_normalize_batch_runs_dedup_and_normalization() -> None:
    jobs = [
        {"job_url": "url1", "company_id": "101", "title": "DE", "description": "Same JD", "applications_count": "5 applicants", "salary": ""},
        {"job_url": "url1", "company_id": "101", "title": "DE", "description": "Same JD", "applications_count": "5 applicants", "salary": ""},
        {"job_url": "url2", "company_id": "102", "title": "DA", "description": "Different JD", "applications_count": "", "salary": ""},
    ]
    result = normalize_batch(jobs)
    assert len(result) == 2  # url1 duplicate removed


def test_normalize_batch_handles_raw_scraper_keys_before_deduplication() -> None:
    jobs = [
        {
            "jobUrl": "url1",
            "companyId": "101",
            "title": "DE 1",
            "description": "JD 1",
            "applicationsCount": "5 applicants",
            "salary": "",
        },
        {
            "jobUrl": "url2",
            "companyId": "102",
            "title": "DE 2",
            "description": "JD 2",
            "applicationsCount": "6 applicants",
            "salary": "",
        },
    ]
    result = normalize_batch(jobs)
    assert len(result) == 2
    assert {job["job_url"] for job in result} == {"url1", "url2"}


def test_normalize_batch_with_exclusions_tracks_removed_duplicates() -> None:
    jobs = [
        {"job_url": "url1", "company_id": "101", "title": "DE", "description": "Same JD", "applications_count": "5 applicants", "salary": ""},
        {"job_url": "url1", "company_id": "101", "title": "DE", "description": "Same JD", "applications_count": "5 applicants", "salary": ""},
        {"job_url": "url2", "company_id": "101", "title": "DE", "description": "Same JD", "applications_count": "5 applicants", "salary": ""},
    ]
    kept, excluded = normalize_batch_with_exclusions(jobs)
    assert len(kept) == 1
    assert [row["dedupe_reason"] for row in excluded] == [
        "duplicate_job_url",
        "near_duplicate_job_posting",
    ]
    assert [row["input_index"] for row in excluded] == [1, 2]
"""
@meta
type: test
scope: unit
domain: normalize
covers:
  - normalization behavior
excludes:
  - full pipeline orchestration
tags:
  - fast
  - ci-safe
"""
