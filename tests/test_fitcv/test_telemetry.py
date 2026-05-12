"""
@meta
type: test
scope: unit
domain: observability
covers:
  - OTel runtime degradation and config behavior
  - bounded Langfuse JSON serialization helpers
excludes:
  - live collector connectivity
tags:
  - fast
  - ci-safe
"""

import json

from fitcv import telemetry


def test_telemetry_disabled_by_default_reports_disabled() -> None:
    telemetry.reset_telemetry_runtime_for_tests()
    status = telemetry.telemetry_export_status()
    assert status["status"] == "disabled"
    assert status["degradation_reason"] == "otel_disabled"


def test_telemetry_enabled_without_endpoint_reports_endpoint_missing(monkeypatch) -> None:
    telemetry.reset_telemetry_runtime_for_tests()
    monkeypatch.setenv("FITCV_OTEL_ENABLED", "true")
    monkeypatch.delenv("FITCV_OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    status = telemetry.telemetry_export_status()
    assert status["status"] == "degraded"
    assert status["degradation_reason"] in {"otel_dependency_missing", "otel_exporter_endpoint_missing"}



def test_telemetry_does_not_report_enabled_after_failed_init(monkeypatch) -> None:
    telemetry.reset_telemetry_runtime_for_tests()
    monkeypatch.setenv("FITCV_OTEL_ENABLED", "true")
    monkeypatch.setenv("FITCV_OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:3000/api/public/otel/v1/traces")
    monkeypatch.setattr(telemetry, "_parse_otlp_headers", lambda _value: "bad")
    first = telemetry.telemetry_export_status()
    second = telemetry.telemetry_export_status()
    assert first["status"] == "degraded"
    assert second["status"] == "degraded"


def test_trace_context_always_has_otel_compatible_ids() -> None:
    telemetry.reset_telemetry_runtime_for_tests()
    trace_context = telemetry.build_trace_context("seed-value")
    assert len(str(trace_context["trace_id"])) == 32
    assert len(str(trace_context["span_id"])) == 16
    assert len(str(trace_context["parent_span_id"])) == 16


def test_current_trace_context_is_none_when_telemetry_disabled() -> None:
    telemetry.reset_telemetry_runtime_for_tests()
    assert telemetry.current_trace_context() is None


def test_observe_span_yields_none_when_telemetry_disabled() -> None:
    telemetry.reset_telemetry_runtime_for_tests()
    with telemetry.observe_span("pipeline.test", attributes={"run_id": "r1"}) as trace_context:
        assert trace_context is None
        assert telemetry.current_trace_context() is None


def test_langfuse_link_status_disabled_by_default() -> None:
    status = telemetry.langfuse_link_status("abc123")
    assert status["status"] == "disabled"
    assert status["degradation_reason"] == "langfuse_disabled"
    assert status["trace_url"] is None


def test_langfuse_link_status_degraded_when_enabled_without_base_url(monkeypatch) -> None:
    monkeypatch.setenv("FITCV_LANGFUSE_ENABLED", "true")
    monkeypatch.delenv("FITCV_LANGFUSE_BASE_URL", raising=False)
    status = telemetry.langfuse_link_status("abc123")
    assert status["status"] == "degraded"
    assert status["degradation_reason"] == "langfuse_base_url_missing"
    assert status["trace_url"] is None


def test_langfuse_link_status_returns_trace_url_when_configured(monkeypatch) -> None:
    monkeypatch.setenv("FITCV_LANGFUSE_ENABLED", "true")
    monkeypatch.setenv("FITCV_LANGFUSE_BASE_URL", "http://localhost:3000")
    status = telemetry.langfuse_link_status("trace-123")
    assert status["status"] == "unverified"
    assert status["degradation_reason"] == "langfuse_ingestion_unverified"
    assert status["trace_url"] == "http://localhost:3000/trace/trace-123"


def test_langfuse_link_status_returns_verified_when_ingestion_confirmed(monkeypatch) -> None:
    monkeypatch.setenv("FITCV_LANGFUSE_ENABLED", "true")
    monkeypatch.setenv("FITCV_LANGFUSE_BASE_URL", "http://localhost:3000")
    status = telemetry.langfuse_link_status("trace-123", verified=True)
    assert status["status"] == "verified"
    assert status["degradation_reason"] is None
    assert status["trace_url"] == "http://localhost:3000/trace/trace-123"


def test_serialize_langfuse_json_returns_none_for_none() -> None:
    assert telemetry.serialize_langfuse_json(None) is None


def test_serialize_langfuse_json_bounds_collections_and_mappings() -> None:
    payload = {"items": list(range(40))}
    payload.update(
        {
            f"k{idx}": idx
            for idx in range(60)
        }
    )

    serialized = telemetry.serialize_langfuse_json(payload)

    assert serialized is not None
    parsed = json.loads(serialized)
    assert len(parsed) == telemetry._LANGFUSE_MAPPING_MAX_ITEMS
    assert len(parsed["items"]) == telemetry._LANGFUSE_COLLECTION_MAX_ITEMS
    assert parsed["items"][-1] == telemetry._LANGFUSE_COLLECTION_MAX_ITEMS - 1


def test_serialize_langfuse_json_truncates_long_strings() -> None:
    serialized = telemetry.serialize_langfuse_json({"text": "x" * 5000}, max_chars=200)

    assert serialized is not None
    assert len(serialized) == 200
    assert serialized.endswith("... [truncated]")


class _UnserializableValue:
    def __str__(self) -> str:
        return "custom-value"


def test_serialize_langfuse_json_falls_back_for_unserializable_values() -> None:
    serialized = telemetry.serialize_langfuse_json({"value": _UnserializableValue()})

    assert serialized is not None
    parsed = json.loads(serialized)
    assert parsed == {"value": {"value": "custom-value"}} or parsed == {"value": "custom-value"}


def test_bound_langfuse_excerpt_truncates_with_marker() -> None:
    excerpt = telemetry.bound_langfuse_excerpt("A" * 40, max_chars=20)

    assert excerpt is not None
    assert len(excerpt) == 20
    assert excerpt.endswith("... [truncated]")


def test_bound_langfuse_markdown_preserves_short_text() -> None:
    markdown = telemetry.bound_langfuse_markdown("## Title\nShort body", max_chars=100)

    assert markdown == "## Title\nShort body"


def test_bound_langfuse_list_limits_items_and_marks_overflow() -> None:
    values = [f"value-{idx}" for idx in range(5)]

    bounded = telemetry.bound_langfuse_list(
        values,
        max_items=3,
        max_item_chars=8,
    )

    assert bounded[:3] == ["value-0", "value-1", "value-2"]
    assert bounded[3] == "[truncated] 2 more item(s)"


def test_bound_langfuse_issue_list_uses_issue_specific_marker() -> None:
    values = [f"issue-{idx}" for idx in range(25)]

    bounded = telemetry.bound_langfuse_issue_list(values)

    assert len(bounded) == 21
    assert bounded[-1] == "[truncated issues] 5 more item(s)"


def test_render_langfuse_markdown_sections_skips_empty_sections() -> None:
    markdown = telemetry.render_langfuse_markdown_sections(
        [
            ("## Job", ["Title: Data Analyst"]),
            ("## Empty", []),
            ("## Candidate", ["Headline: Analytics Engineer"]),
        ]
    )

    assert markdown == "## Job\nTitle: Data Analyst\n\n## Candidate\nHeadline: Analytics Engineer"


def test_render_langfuse_labeled_list_section_formats_bullets() -> None:
    section_lines = telemetry.render_langfuse_labeled_list_section(
        "### Skills",
        ["Python", "SQL", "Dashboards"],
        max_items=2,
        max_item_chars=20,
    )

    assert section_lines == [
        "### Skills",
        "- Python",
        "- SQL",
        "- [truncated] 1 more item(s)",
        "",
    ]


def test_render_langfuse_labeled_text_section_bounds_value() -> None:
    section_lines = telemetry.render_langfuse_labeled_text_section(
        "### Job Excerpt",
        "A" * 40,
        max_chars=20,
    )

    assert section_lines == [
        "### Job Excerpt",
        "AAAAA... [truncated]",
        "",
    ]


def test_build_langfuse_trace_attributes_omits_none_values() -> None:
    attributes = telemetry.build_langfuse_trace_attributes(
        trace_name="fitcv.run_pipeline",
        session_id="run-123",
        user_id=None,
        input_payload={"jobs_path": "jobs.json"},
        output_payload=None,
        metadata={"stage": "pipeline"},
        extra_attributes={"run_id": "run-123", "unused": None},
    )

    assert attributes["langfuse.trace.name"] == "fitcv.run_pipeline"
    assert attributes["langfuse.session.id"] == "run-123"
    assert attributes["run_id"] == "run-123"
    assert "langfuse.user.id" not in attributes
    assert "langfuse.trace.output" not in attributes
    assert "unused" not in attributes
    assert json.loads(attributes["langfuse.trace.input"]) == {"jobs_path": "jobs.json"}
    assert json.loads(attributes["langfuse.trace.metadata"]) == {"stage": "pipeline"}


def test_build_langfuse_observation_attributes_serializes_optional_payloads() -> None:
    attributes = telemetry.build_langfuse_observation_attributes(
        observation_type="generation",
        input_payload={"prompt": "hello"},
        output_payload={"answer": "world"},
        metadata={"stage_id": "cv_generation"},
        model="gpt-test",
        model_parameters={"temperature": 0.2},
        usage_details={"input_tokens": 10, "output_tokens": 20},
        cost_details={"total_cost": 0.01},
        prompt_name="fitcv_structured_generation_prompt",
        extra_attributes={"job_url": "https://example.test/job"},
    )

    assert attributes["langfuse.observation.type"] == "generation"
    assert attributes["langfuse.observation.model"] == "gpt-test"
    assert attributes["langfuse.observation.prompt_name"] == "fitcv_structured_generation_prompt"
    assert attributes["job_url"] == "https://example.test/job"
    assert json.loads(attributes["langfuse.observation.input"]) == {"prompt": "hello"}
    assert json.loads(attributes["langfuse.observation.output"]) == {"answer": "world"}
    assert json.loads(attributes["langfuse.observation.metadata"]) == {"stage_id": "cv_generation"}
    assert json.loads(attributes["langfuse.observation.model_parameters"]) == {"temperature": 0.2}
    assert json.loads(attributes["langfuse.observation.usage_details"]) == {"input_tokens": 10, "output_tokens": 20}
    assert json.loads(attributes["langfuse.observation.cost_details"]) == {"total_cost": 0.01}


def test_build_langfuse_item_observation_envelope_normalizes_fields() -> None:
    envelope = telemetry.build_langfuse_item_observation_envelope(
        observation_type="cv_generation_item",
        run_id="run-123",
        candidate_id="cand-1",
        job_id="job-1",
        attempt_id="attempt-2",
        attempt_index="0",
        selected="yes",
        parent_observation_id="analysis-attempt-1",
        provider="openai-compatible",
        model="cx/gpt-5.2",
        fallback_used="off",
        fallback_reason="timeout",
        rendered_input="input markdown",
        rendered_output="output markdown",
        metadata={"prompt_version": "v3"},
        input_structured={"job_title": "Data Analyst"},
        output_structured={"final_disposition": "accepted"},
    )

    assert envelope["observation_type"] == "cv_generation_item"
    assert envelope["schema_version"] == "v1"
    assert envelope["redaction_version"] == "v1"
    assert envelope["attempt_index"] == 1
    assert envelope["selected"] is True
    assert envelope["fallback_used"] is False
    assert envelope["input"] == "input markdown"
    assert envelope["output"] == "output markdown"
    assert envelope["metadata"]["prompt_version"] == "v3"
    assert envelope["metadata"]["input_structured"] == {"job_title": "Data Analyst"}
    assert envelope["metadata"]["output_structured"] == {"final_disposition": "accepted"}


def test_build_langfuse_item_observation_envelope_omits_none_fields() -> None:
    envelope = telemetry.build_langfuse_item_observation_envelope(
        observation_type="cv_analysis_item",
        rendered_input="input",
        rendered_output="output",
        input_structured=None,
        output_structured=None,
        metadata=None,
    )

    assert envelope["observation_type"] == "cv_analysis_item"
    assert envelope["schema_version"] == "v1"
    assert envelope["redaction_version"] == "v1"
    assert envelope["metadata"]["input_structured"] is None
    assert envelope["metadata"]["output_structured"] is None
    assert "run_id" not in envelope
    assert "attempt_id" not in envelope
    assert "selected" not in envelope


def test_build_langfuse_item_observation_attributes_renders_reviewer_markdown() -> None:
    attributes = telemetry.build_langfuse_item_observation_attributes(
        observation_name="cv_analysis_item",
        observation_type="generation",
        rendered_input="# Candidate\n- Name: Ada\n\n# Job\n- Title: Staff Data Engineer",
        rendered_output="# Decision\n- Fit: strong\n\n# Evidence\n- Built ETL pipelines",
        input_structured={"candidate": {"name": "Ada"}, "job": {"title": "Staff Data Engineer"}},
        output_structured={"fit_decision": "strong", "evidence": ["Built ETL pipelines"]},
        metadata={"stage_id": "cv_analysis", "selected": True},
        usage_details={"input_tokens": 12, "output_tokens": 34},
        cost_details={"total_cost": 0.12},
        model="gemini-2.5-flash",
        prompt_name="cv_analysis_prompt",
        extra_attributes={"fitcv.run_id": "run-123", "fitcv.job_id": "job-456"},
    )

    assert attributes["langfuse.observation.name"] == "cv_analysis_item"
    assert attributes["langfuse.observation.input"] == "# Candidate\n- Name: Ada\n\n# Job\n- Title: Staff Data Engineer"
    assert attributes["langfuse.observation.output"] == "# Decision\n- Fit: strong\n\n# Evidence\n- Built ETL pipelines"
    metadata = json.loads(attributes["langfuse.observation.metadata"])
    assert metadata["stage_id"] == "cv_analysis"
    assert metadata["input_structured"] == {"candidate": {"name": "Ada"}, "job": {"title": "Staff Data Engineer"}}
    assert metadata["output_structured"] == {"fit_decision": "strong", "evidence": ["Built ETL pipelines"]}
    assert metadata["selected"] is True
    assert metadata["observation_type"] == "cv_analysis_item"
    assert metadata["model"] == "gemini-2.5-flash"
    assert attributes["fitcv.run_id"] == "run-123"
    assert attributes["fitcv.job_id"] == "job-456"


def test_build_langfuse_item_observation_attributes_bounds_rendered_and_structured_payloads() -> None:
    long_line = "A" * 5000
    attributes = telemetry.build_langfuse_item_observation_attributes(
        observation_name="cv_generation_item",
        observation_type="generation",
        rendered_input=long_line,
        rendered_output=long_line,
        input_structured={"items": [f"value-{idx}" for idx in range(100)]},
        output_structured={"markdown": long_line},
        metadata={"stage_id": "cv_generation"},
    )

    assert attributes["langfuse.observation.input"].endswith("... [truncated]")
    assert attributes["langfuse.observation.output"].endswith("... [truncated]")
    metadata = json.loads(attributes["langfuse.observation.metadata"])
    assert len(metadata["input_structured"]["items"]) == telemetry._LANGFUSE_COLLECTION_MAX_ITEMS
    assert str(metadata["output_structured"]["markdown"]).endswith("... [truncated]")


def test_build_langfuse_item_observation_attributes_omits_missing_optional_fields() -> None:
    attributes = telemetry.build_langfuse_item_observation_attributes(
        observation_name="cv_generation_item",
        observation_type="generation",
        rendered_input="input",
        rendered_output="output",
        input_structured={"candidate_id": "cand-1"},
        output_structured={"status": "accepted"},
        metadata=None,
        model=None,
        prompt_name=None,
        usage_details=None,
        cost_details=None,
        extra_attributes={"unused": None},
    )

    metadata = json.loads(attributes["langfuse.observation.metadata"])
    assert metadata["input_structured"] == {"candidate_id": "cand-1"}
    assert metadata["output_structured"] == {"status": "accepted"}
    assert metadata["observation_type"] == "cv_generation_item"
    assert "langfuse.observation.model" not in attributes
    assert "langfuse.observation.prompt_name" not in attributes
    assert "langfuse.observation.usage_details" not in attributes
    assert "langfuse.observation.cost_details" not in attributes
    assert "unused" not in attributes
