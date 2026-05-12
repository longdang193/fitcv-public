---
doc_id: observability
doc_type: operator-guide
explains:
  features:
    - inspection_debugging
    - trigger_run_management
  components:
    - src/fitcv
    - src/fitcv_cp
---

# Observability

FitCV’s observability model is centered on **run truth**, **stage-owned
artifacts**, and **persisted event timelines**. There is no separate agent
console; the agentic parts are observed through the same run-detail, export,
and event surfaces that operators already use to inspect the rest of the
pipeline.

This page is the front-door guide for operators and developers who want to
understand what the system did, why it did it, and where the agentic behavior
is visible.

## Core Observation Surfaces

### Runs list

`/admin/runs`

Use this page to find the run, its current status, trigger mode, and whether it
is worth drilling into immediately.

The runs list includes an **Outbox Replay Health (Visible Runs)** card with:

- dead-letter totals and impacted runs
- replay success ratio
- direct `Download JSON` link to `/admin/outbox-replay-health.json?view=...`

### Run detail

`/admin/runs/{run_id}`

This is the main observability surface. It combines:

- timeline events
- stage progress
- run health summaries
- stage-quality metrics
- run exports
- stage-artifact downloads
- synonym overlay and review-adjacent surfaces
- telemetry export health (degraded vs healthy)

For most debugging, start here before opening raw JSON exports.

## Two-Layer Langfuse Observability Model

Observability uses two complementary layers:

- **Layer 1: run-scoped summary surfaces**
  - run/root trace context remains operator entrypoint
  - aggregate summaries such as `pipeline_complete` remain run-level
  - run-summary surfaces avoid duplicating full item-level raw IO
- **Layer 2: item-level evaluable observations**
  - `cv_analysis_item` captures one item observation per candidate-job analysis attempt
  - `cv_generation_item` captures one item observation per candidate-job generation attempt
  - item observations are nested under same run trace for lineage continuity

Item observations support both audiences:

- **reviewers/operators**: readable rendered `input`/`output`
- **automation/filtering**: structured metadata payloads for filters and joins

Bounded payload policy:

- rendered `input` and `output` are capped/redacted through telemetry helpers
- item observations preserve disposition-aware summaries across success and failure paths
- raw chain-of-thought, unbounded provider payloads, and oversized blobs are not stored in item observations
- telemetry degradation must not block primary pipeline execution

Wave 1 verification status:

- focused telemetry and pipeline regression coverage verifies schema, truncation, retry/disposition semantics, and lineage expectations
- one local Langfuse validation pass has verified that the run trace still shows root/run-summary context while nested `cv_analysis_item` and `cv_generation_item` observations expose reviewer-readable rendered IO under the same trace

## OpenTelemetry Export Runtime

FitCV now supports OpenTelemetry export wiring with safe fallback behavior.

Runtime toggles:

- `FITCV_OTEL_ENABLED` (`true`/`false`)
- `FITCV_OTEL_EXPORTER_OTLP_ENDPOINT` (OTLP HTTP endpoint)
- `FITCV_OTEL_SERVICE_NAME` (optional, defaults to `fitcv-control-plane`)
- `FITCV_LANGFUSE_RICH_IO_ENABLED` (`true`/`false`; local startup scripts default to `true` when unset)
- `control_plane.observability.emit_model_routing_diagnostics` (runtime routing event toggle)
- `control_plane.observability.emit_backend_capability_diagnostics` (backend diagnostics event toggle)

Collector example:

- local collector endpoint: `http://localhost:4318/v1/traces`
- for remote collectors, set the full OTLP HTTP trace endpoint URL

Fallback behavior:

- if OTel dependencies are unavailable, export is marked degraded
- if exporter endpoint is missing, export is marked degraded
- telemetry degradation does not block stage execution or artifact persistence
- stage artifacts remain the evidence source of truth

Operator signal:

- run detail now shows a **Telemetry Export Health** card
- degraded telemetry events are counted from persisted run events payloads
- Langfuse trace-link health uses truth-preserving statuses:
  - `disabled`: Langfuse integration is disabled
  - `degraded`: required link inputs are missing
  - `unverified`: trace URL is constructible, but ingestion is not confirmed by this signal alone

Quick troubleshooting:

- `status=degraded`, `degradation_reason=otel_disabled`
  - set `FITCV_OTEL_ENABLED=true`
- `status=degraded`, `degradation_reason=otel_exporter_endpoint_missing`
  - set `FITCV_OTEL_EXPORTER_OTLP_ENDPOINT`
- `status=degraded`, `degradation_reason=otel_dependency_missing`
  - install OpenTelemetry SDK/exporter dependencies in runtime image/venv
- `status=degraded`, `degradation_reason=otel_exporter_init_failed`
  - verify endpoint reachability, protocol path, and collector health

Environment precedence note:

- run-detail health cards reflect the process environment used by the active web/worker processes
- shell/system env overrides startup-script defaults
- startup scripts now default `FITCV_LANGFUSE_RICH_IO_ENABLED=true` for local/dev observability unless explicitly overridden
- if Langfuse/OTel status looks unexpected, print effective env values first:

```powershell
Write-Host "FITCV_LANGFUSE_ENABLED=$env:FITCV_LANGFUSE_ENABLED"
Write-Host "FITCV_LANGFUSE_BASE_URL=$env:FITCV_LANGFUSE_BASE_URL"
Write-Host "FITCV_LANGFUSE_RICH_IO_ENABLED=$env:FITCV_LANGFUSE_RICH_IO_ENABLED"
Write-Host "FITCV_OTEL_ENABLED=$env:FITCV_OTEL_ENABLED"
Write-Host "FITCV_OTEL_EXPORTER_OTLP_ENDPOINT=$env:FITCV_OTEL_EXPORTER_OTLP_ENDPOINT"
Write-Host "FITCV_OTEL_SERVICE_NAME=$env:FITCV_OTEL_SERVICE_NAME"
```

Langfuse export consumption lanes:

- raw export: keep full fidelity for forensics
- analysis-ready export: filter to rows with meaningful `input` or `output`, plus `:rich_io` rows
  (including exports where `input`/`output` are stringified JSON objects)
- reviewer-first Wave 1 item observations should surface sectioned markdown/text in Langfuse `input`/`output`, while metadata keeps structured backing payloads for filters and joins

Example:

```powershell
python scripts/filter_langfuse_export.py `
  --input logs/exports_*.jsonl `
  --output logs/exports_analysis_ready.jsonl
```

Pipeline summary quality block:

- `pipeline_complete` rich output now includes `quality_summary` with:
  - acceptance/review/failure distribution and rates
  - analysis-to-generation conversion
  - retry counts

Langfuse latency semantics:

- UI `Latency` is derived from observation/span timing (`start_time` -> `end_time`).
- `:rich_io` payload field `output.latency_ms` is a custom diagnostic metric and is not
  automatically used by Langfuse UI latency unless a timed observation exists.
- Control-plane rich IO ingestion now emits an `observation-create` span for
  high-value events when `latency_ms > 0`, so UI latency and payload latency can
  agree for those rows.

### Control-Plane Structured Diagnostics

Control-plane enqueue and backend-binding paths now emit structured diagnostics:

- `control_plane.backend_execution`
- `control_plane.model_routing`
- `control_plane.backend_fallback_binding`

Current required fields include:

- request identity: `run_id`, `trace_id`, `stage`, `task_part`
- backend/routing facts: backend identifiers, queue/backend run ids, provider/model labels

### Raw run events

`GET /runs/{run_id}/events`

Use this when you want the machine-facing event stream rather than the rendered
HTML timeline. This is especially helpful for tooling, incident review, or
cross-run comparisons.

## Outbox Replay Alert Check

For active control (not just passive observation), FitCV provides:

- `GET /admin/outbox-replay-health.json`
- `POST /admin/outbox-replay-health/check`

The check route evaluates:

- whether outbox/dead-letter status is degraded
- whether replay success ratio is below threshold

When enabled (`emit_event=true`), the check emits an auditable event:

- `stage=outbox_replay_health_alert`
- `level=warning` on alert, `info` on healthy

### Scheduler-Friendly Checker Script

Use:

```powershell
python scripts/check_outbox_replay_health.py --base-url http://localhost:8010 --view active --min-replay-success-ratio 0.95
```

Exit code contract:

- `0`: health check decision is `ok`
- `2`: health check decision is `alert`
- `3`: request/runtime failure while executing the check

This makes the script safe to plug into cron, Windows Task Scheduler, CI, or
external monitors.

Webhook routing option:

- `scripts/route_outbox_replay_health_alert.py` wraps the checker and sends
  alert/error outcomes to a webhook endpoint while preserving non-zero exits.

## Agentic Observation By Area

### CV analysis and generation

The main surfaces are:

- `/admin/runs/{run_id}/cv-analysis-trace.json`
- `/admin/runs/{run_id}/agentic-live-trace.json`
- `/admin/runs/{run_id}/cv-debug.json`
- `/admin/runs/{run_id}/hitl-review-audit.json`
- `/admin/runs/{run_id}/stage-artifacts.json`
- `/admin/runs/{run_id}/stage-artifacts/cv_analysis.json`
- `/admin/runs/{run_id}/stage-artifacts/cv_generation.json`

Use these to inspect:

- analysis-time agentic decisions before generation starts
- live-provider request and response attempts
- provider, model, template, and schema provenance for the agentic path
- validation retry and repair behavior
- evidence retrieval
- bounded evidence summaries
- gap analysis outcomes
- readiness and skip decisions
- generation validation
- repair behavior
- final generation acceptance or failure
- review-required outcomes, operator actions, and pending review queue state
- markdown-quality review and blocking outcomes

### CV analysis trace

`/admin/runs/{run_id}/cv-analysis-trace.json`

Use this when the question starts in `cv_analysis` rather than in the live
generation provider path.

This artifact follows the same shared agentic trace standard as
`agentic-live-trace.json`, but its `step_id` is `cv_analysis` and its records
capture the pre-generation analysis step for each ranked job.

This artifact is the persisted run-scoped trace surface for:

- reranker-blocked versus analysis-attempted rows
- analysis runtime provenance
- evidence-selection outcomes and fallback usage
- bounded analysis failures without depending on transient worker logs

Shared contract families you should expect in this trace:

- top-level run-scoped trace state such as `trace_family`, `step_id`,
  `trace_status`, `trace_summary`, `records`, and `degradation`
- per-record stage facts such as `runtime_provenance`, `attempts`,
  `input_summary`, `output_summary`, `validation_summary`, `repair_summary`,
  and `error_summary`

Current `cv_analysis`-specific details still visible inside that shared
contract:

- the step id is `cv_analysis`
- records are job-scoped
- attempts correspond to bounded analysis attempts
- output summaries carry evidence-selection counts rather than generation or
  repair facts

### Agentic live trace

`/admin/runs/{run_id}/agentic-live-trace.json`

Use this when the question is specifically about the live agentic provider path
rather than the broader CV-generation ledger.

This artifact is the first persisted trace surface that follows the repo's
shared agentic trace standard. Today it is specific to `cv_generation`, but the
top-level vocabulary is intended to be reused by future agentic traces too.

This artifact is the persisted run-scoped trace surface for:

- actual runtime path used
- provider attempt timing and status
- bounded provider error payloads
- repair retries triggered by missing sections
- final validation-cycle summary

Shared contract families you should expect in this trace:

- top-level run-scoped trace state such as `trace_family`, `step_id`,
  `trace_status`, `trace_summary`, `records`, and `degradation`
- per-record agentic facts such as `runtime_provenance`, `attempts`,
  `input_summary`, `output_summary`, `validation_summary`, `repair_summary`,
  and `error_summary`

Current `cv_generation`-specific details still visible inside that shared
contract:

- the step id is `cv_generation`
- records are job-scoped
- runtime provenance includes the CV template and structured CV schema contract
- attempts correspond to generation and repair-retry calls

The trace is intentionally bounded. It does not store raw chain-of-thought,
full prompt bodies, or full raw provider response bodies.

### Timeline and event reasoning

The timeline in run detail and the raw event stream are the best way to inspect:

- stage starts and completions
- checkpoint pauses and continues
- snapshot persistence failures
- agentic fallback or review-related transitions
- HITL review actions (`cv_review_action`)

If something “felt weird” during a run, the timeline is often the fastest way
to locate the exact stage boundary where the behavior changed.

### Markdown quality outcomes

When CV generation is agentic and markdown quality checks are enabled, markdown
consistency outcomes are observable through:

- run detail "Markdown Quality" card
- `cv_generation.json` quality metrics and output counts
- `cv-debug.json` per-record validation snapshots
- `hitl-review-audit.json` review-required reason/action payloads

Outcome semantics:

- blocking markdown issues (for example unsupported bullet markers) route to
  validation failure
- shallow markdown structure routes to `review_required`
- accepted markdown passes both structural and grounding checks

### Mapping suggestions and synonym proposals

The current agentic synonym surfaces are:

- `/admin/runs/{run_id}/mapping-suggestions.json`
- `/admin/runs/{run_id}/synonym-proposals-trace.json`
- `/admin/runs/{run_id}/synonym-proposals.json`
- `/admin/runs/{run_id}/approved-synonym-proposals.yaml`
- `/admin/synonyms/global.yaml`
- `/admin/mapping-suggestions.json`
- `/admin/synonym-proposals.json`

These let you inspect:

- which aliases were detected from run-scoped evidence
- per-alias proposal-generation trace status and degradation
- how those suggestions were grouped into review-ready synonym proposals
- which proposals are still unreviewed versus already actioned
- which approved run-scoped mappings can be exported as overlay YAML

Run detail now also exposes synonym-review operational summaries:

- batch submit summary (`applied`, `skipped`, `failed`)
- apply-approved-to-run summary (`applied`, `skipped`, `failed`)
- promote-to-global summary (`applied`, `skipped`, `failed`)
- promote-to-global classification counts (`new`, `unchanged`, `overridden`)
- triage refresh summary (`triaged`, `reused`, `skipped`, `failed`)
- triage status badge (`fresh`, `partial`, `stale`, `not_generated`)
- advisory recommendation metadata shown per pending proposal
  (`recommended_action`, recommendation confidence, rationale, risk flags)

Recommendation display is advisory-only. Final review and promotion actions
remain explicit HITL submits by the operator.

Run-local application semantics:

- review status changes alone do not imply cross-run canonical mutation
- `Apply Approved to This Run` materializes approved pairs into the run snapshot
  for downstream stage execution
- promote-to-global remains the explicit canonical update path

Promotion semantics are merge/overlay-based:

- exported `approved-synonym-proposals.yaml` is run-approved delta only
- exported `global.yaml` is the full canonical synonym map snapshot
- promote-to-global applies selected delta rows onto the global canonical map
- alias collisions are surfaced as overrides rather than silent replacement

Triage refresh emits run events for timeline/debug usage:

- `synonym_proposal_triage_completed`
  - includes counts (`triaged`, `reused`, `skipped`, `failed`)
  - includes runtime metadata (`provider`, `model`, `wire_api`, `base_url`)

### Synonym proposals trace

`/admin/runs/{run_id}/synonym-proposals-trace.json`

Use this when debugging proposal-generation flow quality rather than reviewing
proposal payload content itself.

This artifact follows the same shared agentic trace contract as:

- `cv-analysis-trace.json`
- `agentic-live-trace.json`

Its `step_id` is `synonym_proposals`, and it captures:

- proposal-generation attempt status
- alias-scoped records
- persistence degradation status such as `bundle_only_degraded`

### Settings and runtime context

The main runtime-context surface is:

- `/admin/runs/{run_id}/settings-used.json`

Use it to answer:

- which effective settings this run actually used
- whether a per-run override changed behavior
- whether synonym overlay or prompt/runtime configuration influenced the result

## Recommended Observation Workflow

When debugging a surprising run:

1. open `/admin/runs/{run_id}`
2. scan run status, run health, and stage progress
3. read the timeline around the first suspicious transition
4. open `stage-artifacts.json` for stage-owned truth
5. open `cv-analysis-trace.json` if the issue starts before generation
6. open `agentic-live-trace.json` if the issue is specifically in the live
   agentic generation path
7. open `synonym-proposals-trace.json` when proposal persistence or proposal
   generation status looks degraded
8. open `cv-debug.json` for the broader CV-generation ledger
9. open `hitl-review-audit.json` for review queue status and action history
10. inspect the run detail "Markdown Quality" card when quality drift or shallow
    outputs are suspected
11. open `settings-used.json` if behavior may be config-driven
12. open mapping-suggestion or synonym-proposal exports if the issue is
   taxonomy-related

## What Each Surface Is Good At

- run detail HTML:
  - fast human triage
- raw events:
  - sequence reconstruction and tooling
- CV analysis trace export:
  - analysis-stage attempt, evidence-selection, and pre-generation debugging
- agentic live trace export:
  - live-provider attempt, retry, bounded failure debugging, and shared
    agentic trace-contract inspection
- synonym proposals trace export:
  - proposal-generation attempt and persistence-degradation debugging
- stage artifacts:
  - stage-owned truth
- CV debug export:
  - compact CV-generation ledger and per-job debug records
- HITL review audit export:
  - run-scoped `review_required` queue, pending/resolved status, and operator
    action history (`approve`, `regenerate_once`, `reject`)
- Markdown Quality card:
  - compact view of markdown-quality review-required and blocking outcomes
  - sample reasons to accelerate triage before drilling into raw artifacts
- settings-used export:
  - runtime context and override visibility
- mapping-suggestions and synonym-proposals exports:
  - agentic taxonomy and review surfaces

## Important Boundaries

- run-detail labels are derived views, not the source of semantic truth
- stage artifacts are the primary source for stage-owned decisions
- generated docs in `docs/generated/` explain repo structure, not live run state
- deeper behavioral ownership still lives in `docs/features/` and `docs/stages/`

## Related Docs

- [api.md](api.md)
- [usage.md](usage.md)
- [pipeline.md](pipeline.md)
- [architecture.md](architecture.md)
