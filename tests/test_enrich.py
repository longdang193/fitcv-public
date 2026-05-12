"""
@meta
type: test
scope: unit
domain: enrich
covers:
  - enrich-stage transformation behavior
excludes:
  - live LLM calls
tags:
  - fast
  - ci-safe
"""

from datetime import datetime, timezone
import sys
import types

import pytest
import fitcv.enrich as enrich_module

from fitcv.enrich import (
    EnrichmentOutput,
    _apply_structured_normalization,
    build_extraction_prompt,
    build_enrich_contract_fingerprint,
    build_raw_job_fingerprint,
    enrich_job,
    load_run_structured_jobs,
    load_structured_jobs,
    lookup_reusable_structured_jobs,
    merge_scraped_and_enriched,
    parse_extraction_response,
)
from fitcv.prompts.models import PromptDefinition


# ── build_extraction_prompt ───────────────────────────────────────────────────

def test_build_extraction_prompt_includes_required_fields() -> None:
    prompt = build_extraction_prompt(
        description="Deine Aufgaben\n * Du arbeitest im Bereich Business Intelligence...",
        scraped_metadata={"title": "Data Analyst", "experienceLevel": "Entry level"},
    )
    assert "required_skills" in prompt
    assert "location_type" in prompt
    assert "job_family" in prompt
    assert "seniority" in prompt
    # must_have_vs_nice_to_have must NOT appear — deferred to v2
    assert "must_have_vs_nice_to_have" not in prompt


def test_build_extraction_prompt_includes_domain_and_job_family() -> None:
    prompt = build_extraction_prompt(
        description="Role in the banking sector",
        scraped_metadata={"sector": "Banking"},
    )
    assert "job_family" in prompt
    assert "domain" in prompt


def test_build_extraction_prompt_contains_description_text() -> None:
    description = "We need SQL and Python skills for this unique role."
    prompt = build_extraction_prompt(description=description, scraped_metadata={})
    assert "SQL" in prompt


def test_build_extraction_prompt_requires_all_keys_present() -> None:
    prompt = build_extraction_prompt(description="Remote SQL role", scraped_metadata={})
    assert "Every schema key must be present" in prompt
    assert "Use [] for unknown list fields" in prompt
    assert "Use null for unknown scalar fields" in prompt


def test_build_extraction_prompt_uses_effective_prompt_id_from_config() -> None:
    prompt = build_extraction_prompt(
        description="Remote SQL role",
        scraped_metadata={},
        config={"prompts": {"enrich": {"extraction": {"prompt_id": "enrich.extraction.v1"}}}},
    )
    assert "expert recruiter extracting structured information" in prompt


def test_build_raw_job_fingerprint_is_stable_for_whitespace_and_case_changes() -> None:
    """@proves pipeline_performance.fingerprint-based-enrich-result-reuse-happens-before-llm-enrichment-using-normalized-raw-job-inputs"""
    job_a = {
        "job_url": "https://example.com/jobs/1",
        "title": "Data Analyst",
        "company_name": "Acme GmbH",
        "location": "Berlin, Germany",
        "description": "Build KPI dashboards with SQL and Python.\nOwn reporting.\n",
        "contract_type": "Full-time",
        "experience_level": "Mid-Senior level",
        "source": "LinkedIn",
        "applications_count_int": 25,
    }
    job_b = {
        "job_url": "https://example.com/jobs/1",
        "title": "  data analyst  ",
        "company_name": " ACME GMBH ",
        "location": "berlin,   germany",
        "description": "Build KPI dashboards with SQL and Python. Own reporting.",
        "contract_type": " full-time ",
        "experience_level": " mid-senior level ",
        "source": "linkedin",
        "applications_count_int": 999,
    }

    result_a = build_raw_job_fingerprint(job_a)
    result_b = build_raw_job_fingerprint(job_b)

    assert "applications_count_int" not in result_a["payload"]
    assert result_a["fingerprint"] == result_b["fingerprint"]


def test_build_enrich_contract_fingerprint_changes_when_prompt_contract_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@proves pipeline_performance.enrich-contract-fingerprinting-invalidates-reuse-automatically-when-prompt-model-schema-behavior-changes"""
    config = {
        "gemini_model": "gemini-2.5-flash",
        "prompts": {"enrich": {"extraction": {"prompt_id": "enrich.extraction.v1"}}},
    }
    baseline = build_enrich_contract_fingerprint(config)

    monkeypatch.setattr(
        "fitcv.enrich.get_prompt_definition",
        lambda prompt_id: PromptDefinition(
            prompt_id=prompt_id,
            stage_id="enrich",
            version="v999",
            template_path=__import__("pathlib").Path("fitcv/prompts/templates/enrich_extraction_v999.md"),
            summary="test override",
        ),
    )

    changed = build_enrich_contract_fingerprint(config)

    assert baseline["fingerprint"] != changed["fingerprint"]


def test_lookup_reusable_structured_jobs_returns_exact_fingerprint_and_contract_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@proves pipeline_performance.shared-structured-jobs-reuse-lookup-avoids-redundant-enrich-calls-while-only-fresh-rows-are-upserted-back-into-the-shared-table"""
    captured: dict[str, object] = {}

    class FakeRow:
        def items(self):
            return [
                ("job_url", "https://example.com/jobs/1"),
                ("required_skills", ["SQL", "Python"]),
                ("required_skills_canonical", ["sql", "python"]),
                ("required_skill_entities_json", '[{"raw_text":"SQL","canonical":"sql"}]'),
                ("mapping_suggestions_json", '[{"must_have_skill":"sql","matches":true,"alias":"sql","canonical":"sql","confidence":1.0}]'),
                ("raw_job_fingerprint", "raw-fingerprint-match"),
                ("enrich_contract_fingerprint", "contract-fingerprint-match"),
                ("enrich_reuse_status", "fresh_enrichment"),
                ("enrichment_version", "v1"),
                ("enrichment_model", "gemini-2.5-flash"),
                ("enriched_at", "2026-04-03T00:00:00+00:00"),
            ]

    class FakeResult:
        def result(self):
            return self

        def __iter__(self):
            return iter([FakeRow()])

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured["client_kwargs"] = kwargs

        def query(self, sql: str, job_config: object) -> FakeResult:
            captured["sql"] = sql
            captured["job_config"] = job_config
            return FakeResult()

    fake_bigquery = types.SimpleNamespace(
        Client=FakeClient,
        QueryJobConfig=lambda **kwargs: types.SimpleNamespace(**kwargs),
        ArrayQueryParameter=lambda name, field_type, values: types.SimpleNamespace(name=name, field_type=field_type, values=values),
    )
    fake_service_account = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(
            from_service_account_file=lambda path: "creds"
        )
    )
    monkeypatch.setitem(sys.modules, "google.cloud", types.SimpleNamespace(bigquery=fake_bigquery))
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", types.SimpleNamespace(service_account=fake_service_account))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    jobs = [
        {
            "job_url": "https://example.com/jobs/1",
            "title": "Data Analyst",
            "company_name": "Acme",
            "description": "Build dashboards with SQL and Python.",
        }
    ]

    reusable = lookup_reusable_structured_jobs(
        jobs,
        {
            "gcp_project": "fitcv-491123",
            "bigquery_dataset": "fitcv",
            "service_account_key": "/tmp/key.json",
        },
        raw_job_fingerprints={"https://example.com/jobs/1": "raw-fingerprint-match"},
        enrich_contract_fingerprint="contract-fingerprint-match",
    )

    assert list(reusable) == ["https://example.com/jobs/1"]
    assert reusable["https://example.com/jobs/1"]["required_skill_entities"] == [
        {"raw_text": "SQL", "canonical": "sql"}
    ]
    assert reusable["https://example.com/jobs/1"]["mapping_suggestions"][0]["canonical"] == "sql"

def test_make_genai_client_openai_compatible_requires_env_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        enrich_module,
        "resolve_model_routing_part",
        lambda part, model_fallback=None: {
            "provider": "openai_compatible",
            "model": "kimi-k2-instruct",
            "base_url": "http://localhost:20128/v1",
        },
    )
    monkeypatch.delenv("FITCV_LANGGRAPH_OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_COMPATIBLE_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Config-routed HTTP provider.*requires API key in env"):
        enrich_module._make_genai_client({"gemini_model": "gemini-2.5-flash"})


def test_lookup_reusable_structured_jobs_normalises_datetime_enriched_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRow:
        def items(self):
            return [
                ("job_url", "https://example.com/jobs/1"),
                ("required_skills", ["SQL", "Python"]),
                ("required_skills_canonical", ["sql", "python"]),
                ("required_skill_entities_json", '[{"raw_text":"SQL","canonical":"sql"}]'),
                ("mapping_suggestions_json", "[]"),
                ("raw_job_fingerprint", "raw-fingerprint-match"),
                ("enrich_contract_fingerprint", "contract-fingerprint-match"),
                ("enrich_reuse_status", "fresh_enrichment"),
                ("enrichment_version", "v1"),
                ("enrichment_model", "gemini-2.5-flash"),
                ("enriched_at", datetime(2026, 4, 3, 12, 0, 0, tzinfo=timezone.utc)),
            ]

    class FakeResult:
        def result(self):
            return self

        def __iter__(self):
            return iter([FakeRow()])

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            pass

        def query(self, sql: str, job_config: object) -> FakeResult:
            return FakeResult()

    fake_bigquery = types.SimpleNamespace(
        Client=FakeClient,
        QueryJobConfig=lambda **kwargs: types.SimpleNamespace(**kwargs),
        ArrayQueryParameter=lambda name, field_type, values: types.SimpleNamespace(name=name, field_type=field_type, values=values),
    )
    fake_service_account = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(
            from_service_account_file=lambda path: "creds"
        )
    )
    monkeypatch.setitem(sys.modules, "google.cloud", types.SimpleNamespace(bigquery=fake_bigquery))
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", types.SimpleNamespace(service_account=fake_service_account))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    jobs = [
        {
            "job_url": "https://example.com/jobs/1",
            "title": "Data Analyst",
            "company_name": "Acme",
            "description": "Build dashboards with SQL and Python.",
        }
    ]

    reusable = lookup_reusable_structured_jobs(
        jobs,
        {
            "gcp_project": "fitcv-491123",
            "bigquery_dataset": "fitcv",
            "service_account_key": "/tmp/key.json",
        },
        raw_job_fingerprints={"https://example.com/jobs/1": "raw-fingerprint-match"},
        enrich_contract_fingerprint="contract-fingerprint-match",
    )

    assert reusable["https://example.com/jobs/1"]["enriched_at"] == "2026-04-03T12:00:00+00:00"


def test_sqlite_reuse_lookup_uses_cached_structured_jobs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempPathFactory,
) -> None:
    sqlite_path = tmp_path / "fitcv.sqlite3"
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "sqlite")
    monkeypatch.setenv("FITCV_CP_SQLITE_PATH", str(sqlite_path))

    load_structured_jobs(
        enriched=[
            {
                "job_url": "https://example.com/jobs/1",
                "title": "Data Analyst",
                "company_name": "Acme",
                "description_cleaned": "SQL dashboards",
                "required_skills": ["SQL"],
                "required_skills_canonical": ["sql"],
                "required_skill_entities": [{"raw_text": "SQL", "canonical": "sql"}],
                "mapping_suggestions": [],
                "raw_job_fingerprint": "raw-fingerprint-match",
                "enrich_contract_fingerprint": "contract-fingerprint-match",
                "enrich_reuse_status": "fresh_enrichment",
                "enrichment_version": "v1",
                "enrichment_model": "model-x",
                "enriched_at": "2026-05-04T00:00:00+00:00",
            }
        ],
        config={},
    )

    reusable = lookup_reusable_structured_jobs(
        normalized_jobs=[
            {
                "job_url": "https://example.com/jobs/1",
                "title": "Data Analyst",
                "company_name": "Acme",
                "description": "SQL dashboards",
            }
        ],
        config={},
        raw_job_fingerprints={"https://example.com/jobs/1": "raw-fingerprint-match"},
        enrich_contract_fingerprint="contract-fingerprint-match",
    )

    assert list(reusable.keys()) == ["https://example.com/jobs/1"]
    reused = reusable["https://example.com/jobs/1"]
    assert reused["enrich_reuse_status"] == "reused_cached_enrichment"
    assert reused["required_skills_canonical"] == ["sql"]


def test_sqlite_reuse_lookup_rejects_contract_or_fingerprint_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempPathFactory,
) -> None:
    sqlite_path = tmp_path / "fitcv.sqlite3"
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "sqlite")
    monkeypatch.setenv("FITCV_CP_SQLITE_PATH", str(sqlite_path))

    load_structured_jobs(
        enriched=[
            {
                "job_url": "https://example.com/jobs/1",
                "required_skills": ["SQL"],
                "raw_job_fingerprint": "raw-fingerprint-a",
                "enrich_contract_fingerprint": "contract-a",
            }
        ],
        config={},
    )

    reusable = lookup_reusable_structured_jobs(
        normalized_jobs=[{"job_url": "https://example.com/jobs/1", "description": "SQL"}],
        config={},
        raw_job_fingerprints={"https://example.com/jobs/1": "raw-fingerprint-b"},
        enrich_contract_fingerprint="contract-b",
    )

    assert reusable == {}


# ── parse_extraction_response — valid JSON ────────────────────────────────────

def test_parse_extraction_response_valid_json() -> None:
    """@proves pipeline_performance.gemini-structured-output-with-response-schema-and-pydantic"""
    raw = '{"required_skills": ["SQL", "Python"], "location_type": "hybrid", "job_family": "data_analytics"}'
    result = parse_extraction_response(raw)
    assert result["errors"] == []
    assert "SQL" in result["parsed"]["required_skills"]
    assert result["parsed"]["location_type"] == "hybrid"
    assert result["raw_response"] == raw


def test_parse_extraction_response_malformed_json() -> None:
    """@proves pipeline_performance.fallback-path-for-unparseable-responses"""
    result = parse_extraction_response("not json at all")
    assert len(result["errors"]) > 0
    assert result["parsed"] == {}     # empty fallback, not a crash
    assert result["raw_response"] == "not json at all"


def test_parse_extraction_response_markdown_fenced_json() -> None:
    raw = '```json\n{"required_skills": ["SQL"]}\n```'
    result = parse_extraction_response(raw)
    assert result["errors"] == []
    assert result["parsed"]["required_skills"] == ["SQL"]


def test_parse_extraction_response_repairs_missing_array_commas() -> None:
    raw = '{"required_skills": ["SQL" "Python"], "location_type": "remote"}'
    result = parse_extraction_response(raw)
    assert result["errors"] == []
    assert result["parsed"]["required_skills"] == ["SQL", "Python"]
    assert result["parsed"]["location_type"] == "remote"


def test_parse_extraction_response_missing_fields_get_defaults() -> None:
    raw = '{"required_skills": ["SQL"]}'
    result = parse_extraction_response(raw)
    assert result["parsed"].get("preferred_skills", []) == []
    assert result["parsed"].get("location_type") is None


def test_parse_extraction_response_normalizes_location_type_enum() -> None:
    raw = '{"location_type": "Remote"}'
    result = parse_extraction_response(raw)
    assert result["parsed"]["location_type"] == "remote"  # lowercased


def test_parse_extraction_response_bad_enum_returns_none() -> None:
    raw = '{"location_type": "in-person-hybrid-flexible"}'
    result = parse_extraction_response(raw)
    assert result["parsed"]["location_type"] is None     # unknown enum → null


def test_parse_extraction_response_null_list_coerced_to_empty() -> None:
    raw = '{"required_skills": null, "location_type": "remote"}'
    result = parse_extraction_response(raw)
    assert result["parsed"]["required_skills"] == []     # null list → []
    assert result["errors"] == []


def test_parse_extraction_response_unknown_keys_ignored() -> None:
    raw = '{"required_skills": ["SQL"], "invented_field": "foo"}'
    result = parse_extraction_response(raw)
    assert "invented_field" not in result["parsed"]
    assert result["errors"] == []


def test_parse_extraction_response_seniority_normalized() -> None:
    raw = '{"seniority": "Senior"}'
    result = parse_extraction_response(raw)
    assert result["parsed"]["seniority"] == "senior"


def test_parse_extraction_response_bad_seniority_returns_none() -> None:
    raw = '{"seniority": "very experienced"}'
    result = parse_extraction_response(raw)
    assert result["parsed"]["seniority"] is None


def test_parse_extraction_response_uses_skill_entities_for_canonical_fields() -> None:
    raw = """
    {
      "required_skills": [
        "proficient in Python programming for data science",
        "English advanced (C1) or above"
      ],
      "required_skill_entities": [
        {"raw_text": "proficient in Python programming for data science", "canonical": "python", "confidence": 0.96}
      ],
      "preferred_skills": ["PowerBI"],
      "preferred_skill_entities": [
        {"raw_text": "PowerBI", "canonical": "power bi", "confidence": 0.98}
      ]
    }
    """
    result = parse_extraction_response(raw)
    assert result["parsed"]["required_skills_canonical"] == ["python"]
    assert result["parsed"]["preferred_skills_canonical"] == ["power bi"]
    assert result["parsed"]["required_skill_entities"] == [
        {
            "raw_text": "proficient in Python programming for data science",
            "canonical": "python",
            "confidence": 0.96,
        }
    ]


# ── merge_scraped_and_enriched ────────────────────────────────────────────────

def test_merge_scraped_and_enriched_preserves_scraped_fields() -> None:
    scraped = {"job_url": "url1", "title": "DA", "company_name": "ACME", "contract_type": "Full-time"}
    enriched = {"required_skills": ["SQL"], "job_family": "analytics"}
    merged = merge_scraped_and_enriched(scraped, enriched)
    assert merged["title"] == "DA"
    assert merged["required_skills"] == ["SQL"]


def test_merge_scraped_and_enriched_adds_audit_fields() -> None:
    scraped = {"job_url": "url1", "title": "DE"}
    enriched = {"required_skills": ["SQL"]}
    merged = merge_scraped_and_enriched(scraped, enriched)
    assert "enrichment_model" in merged
    assert "enrichment_version" in merged
    assert "enriched_at" in merged


def test_merge_scraped_and_enriched_normalizes_datetime_enriched_at() -> None:
    scraped = {"job_url": "url1", "title": "DE"}
    enriched = {
        "required_skills": ["SQL"],
        "enriched_at": datetime(2026, 4, 3, 12, 0, 0, tzinfo=timezone.utc),
    }
    merged = merge_scraped_and_enriched(scraped, enriched)
    assert merged["enriched_at"] == "2026-04-03T12:00:00+00:00"


def test_merge_scraped_and_enriched_uses_config_model() -> None:
    scraped = {"job_url": "url1", "title": "DE"}
    enriched = {}
    config = {"ai_score_model": "gemini-2.0-flash", "enrichment_version": "v1"}
    merged = merge_scraped_and_enriched(scraped, enriched, config=config)
    assert merged["enrichment_model"] == "gemini-2.0-flash"
    assert merged["enrichment_version"] == "v1"


def test_enrich_job_renders_prompt_via_prompt_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """@proves pipeline_performance.enrich-extraction-prompt-text-now-comes-from-a-centralized-prompt-registry-with-config-selected-prompt-ids"""
    captured: dict[str, object] = {}

    class FakeResponse:
        parsed = EnrichmentOutput(
            required_skills=["SQL"],
            preferred_skills=[],
            responsibilities=[],
            tech_stack=[],
            keywords=[],
            location_type="remote",
            seniority="mid",
            domain="fintech",
            job_family="analytics",
            years_experience_min=None,
            years_experience_max=None,
            required_skill_entities=[],
            preferred_skill_entities=[],
        )
        text = ""

    class FakeModels:
        def generate_content(self, *, model: str, contents: str, config: object) -> FakeResponse:
            captured["model"] = model
            captured["contents"] = contents
            captured["config"] = config
            return FakeResponse()

    class FakeClient:
        models = FakeModels()

    monkeypatch.setattr("fitcv.enrich._make_genai_client", lambda config: FakeClient())

    result = enrich_job(
        {
            "job_url": "https://example.com/jobs/1",
            "title": "Data Analyst",
            "description": "Need SQL for analytics work.",
        },
        {
            "gemini_model": "gemini-2.5-flash",
            "prompts": {"enrich": {"extraction": {"prompt_id": "enrich.extraction.v1"}}},
        },
    )

    assert "Need SQL for analytics work." in str(captured["contents"])
    assert result["required_skills"] == ["SQL"]


def test_merge_scraped_and_enriched_preserves_raw_and_canonical_enrich_fields() -> None:
    """@proves pipeline_performance.enrich-stage-raw-plus-canonical-semantic-companions-for-repeated-downstream-fields"""
    scraped = {"job_url": "url1", "title": "DE"}
    enriched = {
        "required_skills": ["Python programming for data science"],
        "required_skills_canonical": ["python"],
        "required_skill_entities": [
            {"raw_text": "Python programming for data science", "canonical": "python"}
        ],
        "mapping_suggestions": [
            {
                "must_have_skill": "python",
                "matches": True,
                "alias": "python programming for data science",
                "canonical": "python",
                "confidence": 1.0,
            }
        ],
        "location_type_raw": "Remote",
        "location_type": "remote",
        "domain_raw": "FinTech",
        "domain": "fintech",
    }

    merged = merge_scraped_and_enriched(scraped, enriched)

    assert merged["required_skills"] == ["Python programming for data science"]
    assert merged["required_skills_canonical"] == ["python"]
    assert merged["required_skill_entities"] == [
        {"raw_text": "Python programming for data science", "canonical": "python"}
    ]
    assert merged["mapping_suggestions"][0]["alias"] == "python programming for data science"
    assert merged["location_type_raw"] == "Remote"
    assert merged["location_type"] == "remote"
    assert merged["domain_raw"] == "FinTech"
    assert merged["domain"] == "fintech"


def test_load_structured_jobs_uses_explicit_staging_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeSchemaField:
        def __init__(self, name: str, field_type: str, mode: str = "NULLABLE") -> None:
            self.name = name
            self.field_type = field_type
            self.mode = mode

    class FakeLoadJobConfig:
        def __init__(self, **kwargs: object) -> None:
            captured["job_config"] = kwargs

    class FakeJob:
        def result(self) -> None:
            return None

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured["client_kwargs"] = kwargs

        def load_table_from_json(self, rows: list[dict[str, object]], table: str, job_config: object) -> FakeJob:
            captured["rows"] = rows
            captured["table"] = table
            captured["load_job_config_obj"] = job_config
            return FakeJob()

        def query(self, sql: str) -> FakeJob:
            captured["merge_sql"] = sql
            return FakeJob()

    fake_bigquery = types.SimpleNamespace(
        Client=FakeClient,
        LoadJobConfig=FakeLoadJobConfig,
        SchemaField=FakeSchemaField,
        SourceFormat=types.SimpleNamespace(NEWLINE_DELIMITED_JSON="NEWLINE_DELIMITED_JSON"),
    )
    fake_service_account = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(
            from_service_account_file=lambda path: "creds"
        )
    )
    fake_google_cloud = types.SimpleNamespace(bigquery=fake_bigquery)
    fake_google_oauth2 = types.SimpleNamespace(service_account=fake_service_account)

    monkeypatch.setitem(sys.modules, "google.cloud", fake_google_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", fake_google_oauth2)
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    load_structured_jobs(
        enriched=[{"job_url": "url1", "salary_min": None, "salary_max": None}],
        config={
            "gcp_project": "fitcv-491123",
            "bigquery_dataset": "fitcv",
            "service_account_key": "/tmp/key.json",
        },
    )

    job_config_kwargs = captured["job_config"]
    assert isinstance(job_config_kwargs, dict)
    schema = job_config_kwargs["schema"]
    assert isinstance(schema, list)
    salary_min = next(field for field in schema if field.name == "salary_min")
    salary_max = next(field for field in schema if field.name == "salary_max")
    applications_count = next(field for field in schema if field.name == "applications_count")
    assert salary_min.field_type == "FLOAT64"
    assert salary_max.field_type == "FLOAT64"
    assert applications_count.field_type == "INT64"


def test_enrich_batch_retries_resource_exhausted_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.enrich import enrich_batch

    attempts = {"count": 0}
    sleeps: list[float] = []

    class FakeResourceExhausted(Exception):
        pass

    def fake_enrich_job(job: dict[str, object], config: dict[str, object]) -> dict[str, object]:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise FakeResourceExhausted("quota")
        return {"job_url": "url1"}

    fake_exceptions = types.SimpleNamespace(ResourceExhausted=FakeResourceExhausted)

    monkeypatch.setattr("fitcv.enrich.enrich_job", fake_enrich_job)
    monkeypatch.setitem(sys.modules, "google.api_core.exceptions", fake_exceptions)
    monkeypatch.setattr("time.sleep", lambda secs: sleeps.append(secs))

    result = enrich_batch(
        normalized_jobs=[{"job_url": "url1"}],
        config={"enrichment_sleep_secs": 1.5, "enrichment_max_retries": 1},
    )

    assert result == [{"job_url": "url1"}]
    assert attempts["count"] == 2
    assert sleeps == [1.5, 1.5]  # backoff sleep + success-path global rate-limit sleep


def test_enrich_chunk_isolates_single_job_retry_from_following_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@proves bounded_parallel_enrichment.per-job-failure-isolation"""
    from fitcv.enrich import enrich_batch

    attempts_by_url = {"url1": 0, "url2": 0}

    class FakeResourceExhausted(Exception):
        pass

    def fake_enrich_job(job: dict[str, object], config: dict[str, object]) -> dict[str, object]:
        job_url = str(job["job_url"])
        attempts_by_url[job_url] += 1
        if job_url == "url1" and attempts_by_url[job_url] == 1:
            raise FakeResourceExhausted("quota")
        return {"job_url": job_url, "enriched": True}

    fake_exceptions = types.SimpleNamespace(ResourceExhausted=FakeResourceExhausted)

    monkeypatch.setattr("fitcv.enrich.enrich_job", fake_enrich_job)
    monkeypatch.setitem(sys.modules, "google.api_core.exceptions", fake_exceptions)
    monkeypatch.setattr("time.sleep", lambda secs: None)

    result = enrich_batch(
        normalized_jobs=[{"job_url": "url1"}, {"job_url": "url2"}],
        config={
            "enrichment_batch_size": 2,
            "enrichment_concurrency": 1,
            "enrichment_sleep_secs": 0.0,
            "enrichment_max_retries": 1,
        },
    )

    assert [row["job_url"] for row in result] == ["url1", "url2"]
    assert attempts_by_url == {"url1": 2, "url2": 1}


def test_enrich_batch_retries_genai_client_error_429_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.enrich import enrich_batch

    attempts = {"count": 0}
    sleeps: list[float] = []

    class FakeResourceExhausted(Exception):
        pass

    class FakeClientError(Exception):
        def __init__(self, status_code: int) -> None:
            super().__init__(f"status={status_code}")
            self.status_code = status_code

    def fake_enrich_job(job: dict[str, object], config: dict[str, object]) -> dict[str, object]:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise FakeClientError(429)
        return {"job_url": "url1"}

    fake_exceptions = types.SimpleNamespace(ResourceExhausted=FakeResourceExhausted)
    fake_errors = types.SimpleNamespace(ClientError=FakeClientError)
    fake_genai = types.SimpleNamespace(errors=fake_errors)
    fake_google = types.SimpleNamespace(genai=fake_genai)

    monkeypatch.setattr("fitcv.enrich.enrich_job", fake_enrich_job)
    monkeypatch.setitem(sys.modules, "google.api_core.exceptions", fake_exceptions)
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)
    monkeypatch.setitem(sys.modules, "google.genai.errors", fake_errors)
    monkeypatch.setattr("time.sleep", lambda secs: sleeps.append(secs))

    result = enrich_batch(
        normalized_jobs=[{"job_url": "url1"}],
        config={"enrichment_sleep_secs": 2.0, "enrichment_max_retries": 1},
    )

    assert result == [{"job_url": "url1"}]
    assert attempts["count"] == 2
    assert sleeps == [2.0, 2.0]  # backoff sleep + success-path global rate-limit sleep


def test_enrich_batch_uses_exponential_backoff_for_repeated_429s(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fitcv.enrich import enrich_batch

    attempts = {"count": 0}
    sleeps: list[float] = []

    class FakeResourceExhausted(Exception):
        pass

    class FakeClientError(Exception):
        def __init__(self, status_code: int) -> None:
            super().__init__(f"status={status_code}")
            self.status_code = status_code

    def fake_enrich_job(job: dict[str, object], config: dict[str, object]) -> dict[str, object]:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise FakeClientError(429)
        return {"job_url": "url1"}

    fake_exceptions = types.SimpleNamespace(ResourceExhausted=FakeResourceExhausted)
    fake_errors = types.SimpleNamespace(ClientError=FakeClientError)
    fake_genai = types.SimpleNamespace(errors=fake_errors)
    fake_google = types.SimpleNamespace(genai=fake_genai)

    monkeypatch.setattr("fitcv.enrich.enrich_job", fake_enrich_job)
    monkeypatch.setitem(sys.modules, "google.api_core.exceptions", fake_exceptions)
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)
    monkeypatch.setitem(sys.modules, "google.genai.errors", fake_errors)
    monkeypatch.setattr("time.sleep", lambda secs: sleeps.append(secs))

    result = enrich_batch(
        normalized_jobs=[{"job_url": "url1"}],
        config={"enrichment_sleep_secs": 1.5, "enrichment_max_retries": 2},
    )

    assert result == [{"job_url": "url1"}]
    assert attempts["count"] == 3
    assert sleeps == [1.5, 3.0, 1.5]  # two backoff sleeps + success-path global rate-limit sleep


def test_enrich_job_uses_google_genai_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeGenerateContentConfig:
        def __init__(self, **kwargs: object) -> None:
            captured["gen_config_kwargs"] = kwargs

    class FakeResponse:
        text = '{"required_skills": ["SQL"], "location_type": "remote"}'
        parsed = None  # trigger fallback path

    class FakeModels:
        def generate_content(
            self, *, model: str, contents: str, config: object
        ) -> FakeResponse:
            captured["model"] = model
            captured["contents"] = contents
            captured["config"] = config
            return FakeResponse()

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured["client_kwargs"] = kwargs
            self.models = FakeModels()

    fake_genai_types = types.SimpleNamespace(GenerateContentConfig=FakeGenerateContentConfig)
    fake_genai = types.SimpleNamespace(Client=FakeClient, types=fake_genai_types)
    fake_google = types.SimpleNamespace(
        auth=types.SimpleNamespace(default=lambda scopes=None: ("creds", "project"))
    )

    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.auth", fake_google.auth)
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)
    monkeypatch.setitem(sys.modules, "google.genai.types", fake_genai_types)
    setattr(fake_google, "genai", fake_genai)
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")

    result = enrich_job(
        job={
            "job_url": "https://example.com/jobs/1",
            "title": "Data Engineer",
            "description": "Build pipelines with SQL.",
            "experience_level": "Mid-Senior level",
            "contract_type": "Full-time",
            "sector": "Software",
            "location": "Remote",
        },
        config={
            "gcp_project": "fitcv-491123",
            "vertex_location": "us-central1",
            "gemini_model": "gemini-2.5-flash",
            "ai_score_model": "gemini-2.5-flash",
        },
    )

    assert result["required_skills"] == ["SQL"]
    assert result["location_type"] == "remote"
    assert captured["model"] == "gemini-2.5-flash"
    # GenerateContentConfig should have been constructed with response_schema=EnrichmentOutput
    from fitcv.enrich import EnrichmentOutput as _EO
    assert captured["gen_config_kwargs"].get("response_schema") is _EO
    client_kwargs = captured["client_kwargs"]
    assert isinstance(client_kwargs, dict)
    assert (client_kwargs.get("vertexai") is True) or bool(client_kwargs.get("api_key"))
    if "location" in client_kwargs:
        assert client_kwargs["location"] == "us-central1"


def test_enrich_job_prefers_gemini_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeGenerateContentConfig:
        def __init__(self, **kwargs: object) -> None:
            captured["gen_config_kwargs"] = kwargs

    class FakeResponse:
        text = '{"required_skills": ["SQL"]}'
        parsed = None  # trigger fallback path

    class FakeModels:
        def generate_content(
            self, *, model: str, contents: str, config: object
        ) -> FakeResponse:
            captured["model"] = model
            captured["contents"] = contents
            captured["config"] = config
            return FakeResponse()

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured["client_kwargs"] = kwargs
            self.models = FakeModels()

    fake_genai_types = types.SimpleNamespace(GenerateContentConfig=FakeGenerateContentConfig)
    fake_genai = types.SimpleNamespace(Client=FakeClient, types=fake_genai_types)
    fake_google = types.SimpleNamespace(
        auth=types.SimpleNamespace(default=lambda scopes=None: (_ for _ in ()).throw(AssertionError("google.auth.default should not be called")))
    )

    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.auth", fake_google.auth)
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)
    monkeypatch.setitem(sys.modules, "google.genai.types", fake_genai_types)
    setattr(fake_google, "genai", fake_genai)

    result = enrich_job(
        job={"job_url": "https://example.com/jobs/1", "description": "SQL"},
        config={"gcp_project": "fitcv-491123", "gemini_model": "gemini-2.5-flash"},
    )

    assert result["required_skills"] == ["SQL"]
    client_kwargs = captured["client_kwargs"]
    assert isinstance(client_kwargs, dict)
    assert client_kwargs["api_key"] == "test-key"
    assert "vertexai" not in client_kwargs
    # GenerateContentConfig should have been called with response_schema
    assert "response_schema" in captured.get("gen_config_kwargs", {})


# ── load_run_structured_jobs ──────────────────────────────────────────────────

def test_load_run_structured_jobs_inserts_to_correct_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeSchemaField:
        def __init__(self, name: str, field_type: str, mode: str = "NULLABLE") -> None:
            self.name = name
            self.field_type = field_type
            self.mode = mode

    class FakeLoadJobConfig:
        def __init__(self, **kwargs: object) -> None:
            captured["job_config"] = kwargs

    class FakeJob:
        def result(self) -> None:
            return None

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured["client_kwargs"] = kwargs

        def load_table_from_json(
            self, rows: list[dict[str, object]], table: str, job_config: object
        ) -> FakeJob:
            captured["rows"] = rows
            captured["table"] = table
            captured["load_job_config_obj"] = job_config
            return FakeJob()

    fake_bigquery = types.SimpleNamespace(
        Client=FakeClient,
        LoadJobConfig=FakeLoadJobConfig,
        SchemaField=FakeSchemaField,
        SourceFormat=types.SimpleNamespace(NEWLINE_DELIMITED_JSON="NEWLINE_DELIMITED_JSON"),
        WriteDisposition=types.SimpleNamespace(WRITE_APPEND="WRITE_APPEND"),
    )
    fake_service_account = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(
            from_service_account_file=lambda path: "creds"
        )
    )
    fake_google_cloud = types.SimpleNamespace(bigquery=fake_bigquery)
    fake_google_oauth2 = types.SimpleNamespace(service_account=fake_service_account)

    monkeypatch.setitem(sys.modules, "google.cloud", fake_google_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", fake_google_oauth2)
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)

    enriched = [
        {
            "job_url": "https://example.com/1",
            "title": "Data Engineer",
            "company_name": "ACME",
            "location": "Remote",
            "contract_type": "Full-time",
            "experience_level": "mid",
            "published_at": "2026-01-01",
            "location_type": "remote",
            "seniority": "senior",
            "required_skills": ["SQL", "Python"],
            "preferred_skills": [],
            "responsibilities": [],
            "domain": "fintech",
            "tech_stack": [],
            "years_experience_min": 3,
            "years_experience_max": None,
            "keywords": [],
            "job_family": "data_engineering",
            "description_cleaned": "Build pipelines.",
            "enrichment_version": "v1",
            "enrichment_model": "gemini-2.0-flash",
            "enriched_at": "2026-01-01T00:00:00+00:00",
            # extra fields NOT in run_structured_jobs schema:
            "company_id": "123",
            "sector": "Software",
            "salary_min": 50000.0,
            "salary_max": 80000.0,
            "salary_currency": "EUR",
            "applications_count": 10,
        }
    ]
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    load_run_structured_jobs(
        enriched=enriched,
        run_id="run-abc",
        config={
            "gcp_project": "fitcv-491123",
            "bigquery_dataset": "fitcv",
            "service_account_key": "/tmp/key.json",
        },
    )

    table = captured["table"]
    assert "run_structured_jobs" in table, f"Expected run_structured_jobs table, got: {table}"
    assert "structured_jobs" not in table.replace("run_structured_jobs", ""), (
        "Table must be run_structured_jobs, not structured_jobs"
    )


def test_load_run_structured_jobs_injects_run_id_into_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeJob:
        def result(self) -> None:
            return None

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            pass

        def load_table_from_json(
            self, rows: list[dict[str, object]], table: str, job_config: object
        ) -> FakeJob:
            captured["rows"] = rows
            return FakeJob()

    fake_bigquery = types.SimpleNamespace(
        Client=FakeClient,
        LoadJobConfig=lambda **kw: None,
        SchemaField=lambda name, ft, mode="NULLABLE": None,
        SourceFormat=types.SimpleNamespace(NEWLINE_DELIMITED_JSON="NEWLINE_DELIMITED_JSON"),
        WriteDisposition=types.SimpleNamespace(WRITE_APPEND="WRITE_APPEND"),
    )
    fake_service_account = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(
            from_service_account_file=lambda path: "creds"
        )
    )
    monkeypatch.setitem(sys.modules, "google.cloud", types.SimpleNamespace(bigquery=fake_bigquery))
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", types.SimpleNamespace(service_account=fake_service_account))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    enriched = [{"job_url": "https://example.com/1", "title": "DE"}]
    load_run_structured_jobs(
        enriched=enriched,
        run_id="run-xyz",
        config={
            "gcp_project": "fitcv-491123",
            "bigquery_dataset": "fitcv",
            "service_account_key": "/tmp/key.json",
        },
    )

    rows = captured["rows"]
    assert isinstance(rows, list)
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-xyz"
    assert rows[0]["job_url"] == "https://example.com/1"


def test_load_run_structured_jobs_uses_write_append(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeJob:
        def result(self) -> None:
            return None

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            pass

        def load_table_from_json(
            self, rows: list[dict[str, object]], table: str, job_config: object
        ) -> FakeJob:
            captured["job_config_kwargs"] = getattr(job_config, "_kwargs", {})
            return FakeJob()

    class FakeLoadJobConfig:
        def __init__(self, **kwargs: object) -> None:
            self._kwargs = kwargs

    fake_bigquery = types.SimpleNamespace(
        Client=FakeClient,
        LoadJobConfig=FakeLoadJobConfig,
        SchemaField=lambda name, ft, mode="NULLABLE": None,
        SourceFormat=types.SimpleNamespace(NEWLINE_DELIMITED_JSON="NEWLINE_DELIMITED_JSON"),
        WriteDisposition=types.SimpleNamespace(WRITE_APPEND="WRITE_APPEND"),
    )
    fake_service_account = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(
            from_service_account_file=lambda path: "creds"
        )
    )
    monkeypatch.setitem(sys.modules, "google.cloud", types.SimpleNamespace(bigquery=fake_bigquery))
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", types.SimpleNamespace(service_account=fake_service_account))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    enriched = [{"job_url": "https://example.com/1"}]
    load_run_structured_jobs(
        enriched=enriched,
        run_id="run-xyz",
        config={
            "gcp_project": "fitcv-491123",
            "bigquery_dataset": "fitcv",
            "service_account_key": "/tmp/key.json",
        },
    )

    kwargs = captured["job_config_kwargs"]
    assert kwargs.get("write_disposition") == "WRITE_APPEND", (
        f"Expected WRITE_APPEND, got: {kwargs.get('write_disposition')}"
    )


def test_load_run_structured_jobs_excludes_schema_extra_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extra fields from structured_jobs (company_id, sector, salary_*) must not appear in rows."""
    captured: dict[str, object] = {}

    class FakeJob:
        def result(self) -> None:
            return None

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            pass

        def load_table_from_json(
            self, rows: list[dict[str, object]], table: str, job_config: object
        ) -> FakeJob:
            captured["rows"] = rows
            return FakeJob()

    fake_bigquery = types.SimpleNamespace(
        Client=FakeClient,
        LoadJobConfig=lambda **kw: None,
        SchemaField=lambda name, ft, mode="NULLABLE": None,
        SourceFormat=types.SimpleNamespace(NEWLINE_DELIMITED_JSON="NEWLINE_DELIMITED_JSON"),
        WriteDisposition=types.SimpleNamespace(WRITE_APPEND="WRITE_APPEND"),
    )
    fake_service_account = types.SimpleNamespace(
        Credentials=types.SimpleNamespace(
            from_service_account_file=lambda path: "creds"
        )
    )
    monkeypatch.setitem(sys.modules, "google.cloud", types.SimpleNamespace(bigquery=fake_bigquery))
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bigquery)
    monkeypatch.setitem(sys.modules, "google.oauth2", types.SimpleNamespace(service_account=fake_service_account))
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_service_account)
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "bigquery")

    enriched = [
        {
            "job_url": "https://example.com/1",
            "company_id": "SHOULD_NOT_APPEAR",
            "sector": "SHOULD_NOT_APPEAR",
            "salary_min": 99999.0,
            "salary_max": 99999.0,
            "salary_currency": "USD",
            "applications_count": 42,
        }
    ]
    load_run_structured_jobs(
        enriched=enriched,
        run_id="run-abc",
        config={
            "gcp_project": "fitcv-491123",
            "bigquery_dataset": "fitcv",
            "service_account_key": "/tmp/key.json",
        },
    )

    row = captured["rows"][0]  # type: ignore[index]
    for excluded in ("company_id", "sector", "salary_min", "salary_max", "salary_currency", "applications_count"):
        assert excluded not in row, f"Field {excluded!r} should not be in run_structured_jobs row"


def test_load_run_structured_jobs_writes_sqlite_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempPathFactory,
) -> None:
    import sqlite3

    sqlite_path = tmp_path / "fitcv_cp.sqlite3"
    monkeypatch.setenv("FITCV_CP_DATA_BACKEND", "sqlite")
    monkeypatch.setenv("FITCV_CP_SQLITE_PATH", str(sqlite_path))

    enriched = [
        {
            "job_url": "https://example.com/jobs/1",
            "title": "Data Engineer",
            "company_name": "Acme",
            "required_skills": ["SQL", "Python"],
            "required_skills_canonical": ["sql", "python"],
            "preferred_skills": ["dbt"],
            "preferred_skills_canonical": ["dbt"],
            "responsibilities": ["Build pipelines"],
            "responsibilities_canonical": ["build pipelines"],
            "tech_stack": ["Python", "BigQuery"],
            "tech_stack_canonical": ["python", "bigquery"],
            "keywords": ["etl"],
            "keywords_canonical": ["etl"],
            "required_skill_entities": [{"raw_text": "SQL", "canonical": "sql"}],
            "preferred_skill_entities": [{"raw_text": "dbt", "canonical": "dbt"}],
            "mapping_suggestions": [{"canonical": "sql", "matches": True}],
            "enriched_at": "2026-05-10T00:00:00+00:00",
        }
    ]

    inserted = load_run_structured_jobs(enriched=enriched, run_id="run-123", config={})

    assert inserted == 1
    with sqlite3.connect(sqlite_path) as conn:
        row = conn.execute(
            "SELECT run_id, job_url, payload_json FROM run_structured_jobs WHERE run_id = ? AND job_url = ?",
            ("run-123", "https://example.com/jobs/1"),
        ).fetchone()

    assert row is not None
    assert row[0] == "run-123"
    assert row[1] == "https://example.com/jobs/1"
    payload = __import__("json").loads(str(row[2]))
    assert payload["run_id"] == "run-123"
    assert payload["required_skills"] == ["SQL", "Python"]
    assert payload["required_skill_entities_json"] == '[{"raw_text": "SQL", "canonical": "sql"}]'
    assert payload["mapping_suggestions_json"] == '[{"canonical": "sql", "matches": true}]'


# ── EnrichmentOutput + _apply_structured_normalization ───────────────────────

def test_apply_structured_normalization_lowercases_domain() -> None:
    output = EnrichmentOutput(domain="FinTech", job_family="Data_Engineering")
    result = _apply_structured_normalization(output, config=None)
    assert result["domain"] == "fintech"
    assert result["job_family"] == "data_engineering"


def test_apply_structured_normalization_rejects_invalid_location_type() -> None:
    output = EnrichmentOutput(location_type="office")  # not in valid set
    result = _apply_structured_normalization(output, config=None)
    assert result["location_type"] is None


def test_apply_structured_normalization_accepts_valid_location_type() -> None:
    for valid in ("remote", "hybrid", "onsite"):
        output = EnrichmentOutput(location_type=valid)
        result = _apply_structured_normalization(output, config=None)
        assert result["location_type"] == valid


def test_apply_structured_normalization_rejects_invalid_seniority() -> None:
    output = EnrichmentOutput(seniority="executive")  # not in valid set
    result = _apply_structured_normalization(output, config=None)
    assert result["seniority"] is None


def test_apply_structured_normalization_empty_lists_preserved() -> None:
    """Pydantic enforces list[str] so None items cannot be constructed.
    This tests that empty lists are preserved correctly."""
    output = EnrichmentOutput(required_skills=[])
    result = _apply_structured_normalization(output, config=None)
    assert result["required_skills"] == []


def test_apply_structured_normalization_none_domain_stays_none() -> None:
    output = EnrichmentOutput(domain=None)
    result = _apply_structured_normalization(output, config=None)
    assert result["domain"] is None


def test_apply_structured_normalization_strips_whitespace() -> None:
    output = EnrichmentOutput(domain="  FinTech  ", job_family="  ML Engineering  ")
    result = _apply_structured_normalization(output, config=None)
    assert result["domain"] == "fintech"
    assert result["job_family"] == "ml engineering"


def test_apply_structured_normalization_emits_canonical_skill_companions() -> None:
    """@proves pipeline_performance.canonical-skill-companion-lists-and-entity-payloads-for-required-preferred-skills
    @proves pipeline_performance.enrich-stage-mapping-suggestion-capture-for-review-debug-surfaces
    """
    output = EnrichmentOutput(
        required_skills=["GCP", "Python programming for data science"],
        preferred_skills=["PowerBI"],
        required_skill_entities=[
            {"raw_text": "GCP", "canonical": "google cloud", "confidence": 0.99},
            {"raw_text": "Python programming for data science", "canonical": "python", "confidence": 0.96},
        ],
        preferred_skill_entities=[
            {"raw_text": "PowerBI", "canonical": "power bi", "confidence": 0.98},
        ],
        tech_stack=["BigQuery"],
        keywords=["LLM"],
    )

    result = _apply_structured_normalization(
        output,
        config={
            "skill_synonyms": {
                "gcp": "google cloud",
                "powerbi": "power bi",
                "llm": "genai",
            }
        },
    )

    assert result["required_skills"] == ["GCP", "Python programming for data science"]
    assert result["required_skills_canonical"] == [
        "google cloud",
        "python",
    ]
    assert result["preferred_skills_canonical"] == ["power bi"]
    assert "tech_stack_canonical" not in result
    assert "keywords_canonical" not in result
    assert result["required_skill_entities"] == [
        {"raw_text": "GCP", "canonical": "google cloud", "confidence": 0.99},
        {
            "raw_text": "Python programming for data science",
            "canonical": "python",
            "confidence": 0.96,
        },
    ]
    assert result["preferred_skill_entities"] == [
        {"raw_text": "PowerBI", "canonical": "power bi", "confidence": 0.98}
    ]
    assert result["mapping_suggestions"] == [
        {
            "must_have_skill": "google cloud",
            "matches": True,
            "alias": "gcp",
            "canonical": "google cloud",
            "confidence": 0.99,
        },
        {
            "must_have_skill": "power bi",
            "matches": True,
            "alias": "powerbi",
            "canonical": "power bi",
            "confidence": 0.98,
        },
    ]


def test_apply_structured_normalization_excludes_non_skill_requirement_content() -> None:
    output = EnrichmentOutput(
        required_skills=[
            "Master's or PhD Degree in Data Science, Statistics, Mathematics, Computer Science, or related quantitative field",
            "at least 5 years of hands-on data science experience with proven business impact",
            "proficient in Python programming for data science (pandas, numpy, scipy, scikit-learn, statsmodels)",
            "proficient in SQL and database operations for data manipulation and analysis",
            "English advanced (C1) or above",
        ],
        required_skill_entities=[
            {
                "raw_text": "proficient in Python programming for data science (pandas, numpy, scipy, scikit-learn, statsmodels)",
                "canonical": "python",
                "confidence": 0.96,
            },
            {
                "raw_text": "proficient in Python programming for data science (pandas, numpy, scipy, scikit-learn, statsmodels)",
                "canonical": "pandas",
                "confidence": 0.94,
            },
            {
                "raw_text": "proficient in SQL and database operations for data manipulation and analysis",
                "canonical": "sql",
                "confidence": 0.95,
            },
        ],
    )

    result = _apply_structured_normalization(output, config={})

    assert result["required_skills_canonical"] == ["python", "pandas", "sql"]
    assert all("degree" not in skill for skill in result["required_skills_canonical"])
    assert all("english" not in skill for skill in result["required_skills_canonical"])
    assert len(result["required_skill_entities"]) == 3


def test_apply_structured_normalization_filters_soft_skills_languages_and_domain_knowledge() -> None:
    output = EnrichmentOutput(
        required_skills=[
            "Analytical Focus",
            "Attention to detail",
            "German communication",
            "Telecom domain experience",
            "Prompt Engineering",
            "Vector Databases",
            "SQL",
        ],
        required_skill_entities=[
            {"raw_text": "Analytical Focus", "canonical": "analytical thinking", "confidence": 1.0},
            {"raw_text": "Attention to detail", "canonical": "attention to detail", "confidence": 1.0},
            {"raw_text": "German communication", "canonical": "german", "confidence": 1.0},
            {"raw_text": "Telecom domain experience", "canonical": "telecommunications domain knowledge", "confidence": 1.0},
            {"raw_text": "Prompt Engineering", "canonical": "genai", "confidence": 0.95},
            {"raw_text": "Vector Databases", "canonical": "genai", "confidence": 0.95},
            {"raw_text": "SQL", "canonical": "sql", "confidence": 1.0},
        ],
    )

    result = _apply_structured_normalization(output, config={})

    assert result["required_skills_canonical"] == ["sql"]
    assert result["required_skill_entities"] == [
        {"raw_text": "SQL", "canonical": "sql", "confidence": 1.0}
    ]
    assert result["mapping_suggestions"] == []


def test_apply_structured_normalization_uses_conservative_alias_fallback_when_entities_absent() -> None:
    output = EnrichmentOutput(
        required_skills=["GCP", "Python programming for data science"],
        preferred_skills=["PowerBI"],
    )

    result = _apply_structured_normalization(
        output,
        config={
            "skill_synonyms": {
                "gcp": "google cloud",
                "powerbi": "power bi",
            }
        },
    )

    assert result["required_skills_canonical"] == ["google cloud"]
    assert result["preferred_skills_canonical"] == ["power bi"]
    assert result["required_skill_entities"] == [
        {"raw_text": "GCP", "canonical": "google cloud", "confidence": 1.0}
    ]
    assert result["mapping_suggestions"] == [
        {
            "must_have_skill": "google cloud",
            "matches": True,
            "alias": "gcp",
            "canonical": "google cloud",
            "confidence": 1.0,
        },
        {
            "must_have_skill": "power bi",
            "matches": True,
            "alias": "powerbi",
            "canonical": "power bi",
            "confidence": 1.0,
        },
    ]


def test_apply_structured_normalization_preserves_raw_scalar_companions() -> None:
    """@proves pipeline_performance.enrich-stage-raw-plus-canonical-semantic-companions-for-repeated-downstream-fields"""
    output = EnrichmentOutput(
        location_type="Remote",
        seniority="Senior",
        domain=" FinTech ",
        job_family=" Data_Engineering ",
    )

    result = _apply_structured_normalization(output, config=None)

    assert result["location_type_raw"] == "Remote"
    assert result["location_type"] == "remote"
    assert result["seniority_raw"] == "Senior"
    assert result["seniority"] == "senior"
    assert result["domain_raw"] == " FinTech "
    assert result["domain"] == "fintech"
    assert result["job_family_raw"] == " Data_Engineering "
    assert result["job_family"] == "data_engineering"

def test_apply_structured_normalization_emits_domain_and_role_family_mapping_suggestions() -> None:
    output = EnrichmentOutput(
        domain=" Telco ",
        job_family=" BI Analyst ",
    )
    result = _apply_structured_normalization(
        output,
        config={
            "domain_alias_map": {"telco": "telecommunications"},
            "role_family_alias_map": {"bi analyst": "analytics"},
        },
    )
    assert result["domain_mapping_suggestions"] == [
        {
            "field": "domain",
            "alias": "telco",
            "canonical": "telecommunications",
            "confidence": 1.0,
            "matches": True,
        }
    ]
    assert result["role_family_mapping_suggestions"] == [
        {
            "field": "role_family",
            "alias": "bi analyst",
            "canonical": "analytics",
            "confidence": 1.0,
            "matches": True,
        }
    ]

def test_apply_structured_normalization_maps_role_family_alias_to_taxonomy_family() -> None:
    output = EnrichmentOutput(job_family="Data Science")
    result = _apply_structured_normalization(
        output,
        config={
            "role_taxonomy": {
                "role_family_neighbors": {
                    "data_science": ["analytics"],
                    "analytics": ["data_science"],
                }
            }
        },
    )
    assert result["role_family_mapping_suggestions"] == [
        {
            "field": "role_family",
            "alias": "data science",
            "canonical": "data_science",
            "confidence": 1.0,
            "matches": True,
        }
    ]

def test_merge_scraped_and_enriched_seeds_domain_suggestion_from_sector() -> None:
    scraped = {
        "job_url": "url1",
        "title": "DA",
        "sector": "Retail Banking",
    }
    enriched = {
        "domain": "banking",
        "domain_mapping_suggestions": [],
    }
    merged = merge_scraped_and_enriched(scraped, enriched)
    assert merged["domain_mapping_suggestions"] == [
        {
            "field": "domain",
            "alias": "retail banking",
            "canonical": "banking",
            "confidence": 1.0,
            "matches": True,
        }
    ]

def test_merge_scraped_and_enriched_seeds_role_family_from_title_taxonomy() -> None:
    scraped = {
        "job_url": "url2",
        "title": "Senior Data Scientist",
    }
    enriched = {
        "job_family_raw": "data science",
        "job_family": "data science",
        "role_family_mapping_suggestions": [],
    }
    merged = merge_scraped_and_enriched(
        scraped,
        enriched,
        config={
            "role_taxonomy": {
                "canonical_role_by_alias": {
                    "senior data scientist": "data scientist",
                    "data scientist": "data scientist",
                },
                "role_family_by_role": {
                    "data scientist": "data_science",
                },
            }
        },
    )
    assert merged["role_family_mapping_suggestions"] == [
        {
            "field": "role_family",
            "alias": "data science",
            "canonical": "data_science",
            "confidence": 1.0,
            "matches": True,
        }
    ]

def test_merge_scraped_and_enriched_seeds_role_family_when_job_family_missing() -> None:
    scraped = {
        "job_url": "url3",
        "title": "Business Intelligence Analyst",
    }
    enriched = {
        "job_family_raw": None,
        "job_family": None,
        "role_family_mapping_suggestions": [],
    }
    merged = merge_scraped_and_enriched(
        scraped,
        enriched,
        config={
            "role_taxonomy": {
                "canonical_role_by_alias": {
                    "business intelligence analyst": "data analyst",
                    "data analyst": "data analyst",
                },
                "role_family_by_role": {
                    "data analyst": "data_science",
                },
            }
        },
    )
    assert merged["role_family_mapping_suggestions"] == [
        {
            "field": "role_family",
            "alias": "data science",
            "canonical": "data_science",
            "confidence": 1.0,
            "matches": True,
        }
    ]

def test_apply_structured_normalization_emits_role_family_when_alias_is_underscore_canonical() -> None:
    output = EnrichmentOutput(job_family="data_science")
    result = _apply_structured_normalization(
        output,
        config={
            "role_taxonomy": {
                "role_family_neighbors": {
                    "data_science": ["analytics"],
                }
            }
        },
    )
    assert result["role_family_mapping_suggestions"] == [
        {
            "field": "role_family",
            "alias": "data science",
            "canonical": "data_science",
            "confidence": 1.0,
            "matches": True,
        }
    ]


# ── enrich_job primary and fallback paths ─────────────────────────────────────

def _job_fixture() -> dict:
    return {
        "job_url": "https://example.com/job/1",
        "title": "Data Engineer",
        "description": "Build pipelines with Python and Spark.",
        "location": "Berlin",
        "experience_level": "",
        "contract_type": "",
        "sector": "",
    }


def _config_fixture() -> dict:
    return {
        "gcp_project": "test-proj",
        "gemini_model": "gemini-2.5-flash",
        "ai_score_model": "gemini-2.5-flash",
        "location": "us-central1",
        "enrichment_version": "v1",
    }


def test_enrich_job_uses_response_parsed() -> None:
    """Primary path: response.parsed is used and normalization applied."""
    from unittest.mock import MagicMock, patch

    mock_response = MagicMock()
    mock_response.parsed = EnrichmentOutput(
        required_skills=["Python", "Spark"],
        location_type="remote",
        seniority="mid",
        domain="FinTech",    # should be lowercased by normalization
        job_family="data_engineering",
    )

    with patch("fitcv.enrich._make_genai_client") as mk:
        mk.return_value.models.generate_content.return_value = mock_response
        result = enrich_job(_job_fixture(), _config_fixture())

    assert result["required_skills"] == ["Python", "Spark"]
    assert result["location_type"] == "remote"
    assert result["seniority"] == "mid"
    assert result["domain"] == "fintech"
    assert result["job_family"] == "data_engineering"


def test_enrich_job_fallback_when_parsed_is_none(caplog: pytest.LogCaptureFixture) -> None:
    """@proves pipeline_performance.fallback-path-for-unparseable-responses"""
    import logging
    from unittest.mock import MagicMock, patch

    mock_response = MagicMock()
    mock_response.parsed = None
    # malformed JSON — missing comma (what gemini-2.5-flash sometimes produces)
    mock_response.text = '{"required_skills": ["SQL" "Python"], "location_type": "remote"}'

    with patch("fitcv.enrich._make_genai_client") as mk, \
         caplog.at_level(logging.WARNING, logger="fitcv.enrich"):
        mk.return_value.models.generate_content.return_value = mock_response
        result = enrich_job(_job_fixture(), _config_fixture())

    assert "falling back" in caplog.text.lower()
    assert isinstance(result["required_skills"], list)
    assert result["location_type"] == "remote"


def test_enrich_job_fallback_empty_on_bad_text(caplog: pytest.LogCaptureFixture) -> None:
    """Returns empty enrichment (no crash) when parsed=None and text is not JSON."""
    import logging
    from unittest.mock import MagicMock, patch

    mock_response = MagicMock()
    mock_response.parsed = None
    mock_response.text = "I cannot extract structured data from this input."

    with patch("fitcv.enrich._make_genai_client") as mk, \
         caplog.at_level(logging.WARNING, logger="fitcv.enrich"):
        mk.return_value.models.generate_content.return_value = mock_response
        result = enrich_job(_job_fixture(), _config_fixture())

    assert result["required_skills"] == []
    assert result["location_type"] is None


# ── bounded parallel enrichment (Tasks 3 + 4) ─────────────────────────────────

def _fake_enrich_job(job: dict, config: dict) -> dict:
    """Simple fake: echoes job with enriched=True marker."""
    return {**job, "enriched": True}


def test_enrich_batch_preserves_input_order_under_parallel_batches() -> None:
    """@proves bounded_parallel_enrichment.deterministic-output-order"""
    from unittest.mock import patch
    from fitcv.enrich import enrich_batch

    jobs = [{"job_url": f"u{i}"} for i in range(6)]
    with patch("fitcv.enrich.enrich_job", side_effect=_fake_enrich_job), \
         patch("time.sleep"):
        result = enrich_batch(
            jobs,
            config={"enrichment_batch_size": 2, "enrichment_concurrency": 3},
        )

    assert [r["job_url"] for r in result] == [f"u{i}" for i in range(6)]


def test_enrich_batch_respects_batch_size() -> None:
    """All jobs are enriched even when batch_size < len(jobs)."""
    from unittest.mock import patch
    from fitcv.enrich import enrich_batch

    jobs = [{"job_url": f"j{i}"} for i in range(7)]
    with patch("fitcv.enrich.enrich_job", side_effect=_fake_enrich_job), \
         patch("time.sleep"):
        result = enrich_batch(
            jobs,
            config={"enrichment_batch_size": 3, "enrichment_concurrency": 2},
        )

    assert len(result) == 7
    assert all(r["enriched"] is True for r in result)


def test_enrich_batch_concurrency_one_behaves_like_sequential() -> None:
    """concurrency=1 produces the same result set as sequential execution."""
    from unittest.mock import patch
    from fitcv.enrich import enrich_batch

    jobs = [{"job_url": f"s{i}"} for i in range(5)]
    with patch("fitcv.enrich.enrich_job", side_effect=_fake_enrich_job), \
         patch("time.sleep"):
        result = enrich_batch(
            jobs,
            config={"enrichment_batch_size": 2, "enrichment_concurrency": 1},
        )

    assert len(result) == 5
    assert [r["job_url"] for r in result] == [f"s{i}" for i in range(5)]


def test_enrich_batch_no_jobs_dropped() -> None:
    """Zero jobs are lost when batches complete—one count-per-job is exact."""
    from unittest.mock import patch
    from fitcv.enrich import enrich_batch

    N = 11
    jobs = [{"job_url": f"n{i}"} for i in range(N)]
    with patch("fitcv.enrich.enrich_job", side_effect=_fake_enrich_job), \
         patch("time.sleep"):
        result = enrich_batch(
            jobs,
            config={"enrichment_batch_size": 4, "enrichment_concurrency": 3},
        )

    assert len(result) == N


def test_enrich_batch_non_recoverable_error_propagates() -> None:
    """A non-recoverable exception in a chunk must propagate, not be swallowed."""
    from unittest.mock import patch
    from fitcv.enrich import enrich_batch

    class FakeResourceExhausted(Exception):
        pass

    call_count = {"n": 0}

    def boom(job, config):
        call_count["n"] += 1
        raise RuntimeError("catastrophic failure")

    with pytest.raises(RuntimeError, match="catastrophic"):
        with patch("fitcv.enrich.enrich_job", side_effect=boom), \
             patch("time.sleep"), \
             patch("fitcv.enrich._GOOGLE_EXCEPTIONS", (FakeResourceExhausted,), create=True):
            # Inject minimal module stubs needed by enrich_batch imports
            import types as _types, sys as _sys
            _fe = _types.SimpleNamespace(ResourceExhausted=FakeResourceExhausted)
            _fce = _types.SimpleNamespace(ClientError=Exception)
            _sys.modules.setdefault("google.api_core.exceptions", _fe)
            _sys.modules.setdefault("google.genai.errors", _fce)
            enrich_batch(
                [{"job_url": "x"}],
                config={"enrichment_batch_size": 1, "enrichment_concurrency": 1},
            )


def test_enrich_batch_uses_configured_batch_size_and_concurrency() -> None:
    """Config values are respected: batch_size=1 means one job per chunk."""
    from unittest.mock import patch
    from fitcv.enrich import enrich_batch

    call_sizes: list[int] = []

    def fake_enrich(job, config):
        return {**job, "enriched": True}

    original_chunk = None

    def capture_chunk(chunk, config):
        call_sizes.append(len(chunk))
        return [fake_enrich(j, config) for j in chunk]

    with patch("fitcv.enrich.enrich_job", side_effect=fake_enrich), \
         patch("time.sleep"):
        # Monkey-patch _enrich_chunk to capture chunk sizes
        import fitcv.enrich as _enrich_mod
        orig = getattr(_enrich_mod, "_enrich_chunk", None)
        _enrich_mod._enrich_chunk = capture_chunk
        try:
            result = enrich_batch(
                [{"job_url": f"c{i}"} for i in range(4)],
                config={"enrichment_batch_size": 2, "enrichment_concurrency": 2},
            )
        finally:
            if orig is not None:
                _enrich_mod._enrich_chunk = orig

    assert len(result) == 4
    # Each chunk had at most batch_size=2 jobs
    assert all(s <= 2 for s in call_sizes), f"Chunk sizes: {call_sizes}"
