---
doc_id: api
doc_type: reference
explains:
  features:
    - inspection_debugging
    - settings_system
    - trigger_run_management
  components:
    - src/fitcv_cp
---

# API

This page summarizes the main HTTP surfaces exposed by the FitCV control plane.
It is intentionally concise: enough to make the routes discoverable and usable,
without duplicating lower-level implementation details from the code or managed
feature docs.

## API Shape

The repo exposes two broad classes of HTTP surface:

- JSON-oriented API routes for runs, events, and settings
- HTML admin routes for operators

Most agentic observation and operator actions happen through the admin routes,
while automation and programmatic inspection usually start with the JSON routes.

## Health

### `GET /healthz`

Purpose:

- lightweight service health check

Typical response:

```json
{
  "ok": true
}
```

## Control-Plane Runtime Backend Mode

The control-plane runtime backend is resolved by the backend runtime resolver and can be overridden at process start:

- `FITCV_CP_DATA_BACKEND` (`bigquery` or `sqlite`)

Behavior:

- `bigquery` mode initializes BigQuery-backed storage dependencies.
- `sqlite` mode skips BigQuery client initialization so local startup does not require GCP ADC.

## Run Trigger And Inspection API

### `POST /runs`

Purpose:

- create a run through the JSON API

Behavior:

- captures input source
- snapshots effective settings
- inserts the run row before enqueue
- enqueues background execution

Typical payload shape:

```json
{
  "jobs_path": "data/sample_jobs.json",
  "config_path": "config/env.yaml",
  "triggered_by": "admin",
  "config_overrides": {},
  "run_mode": "run_all"
}
```

`run_mode` supports:

- `run_all`
- `manual_staged`

Typical response:

- `201 Created`
- JSON object containing run identifier:

```json
{
  "run_id": "<uuid>"
}
```

### `GET /runs`

Purpose:

- list pipeline runs in JSON form

Typical response:

- array of run objects with status, lifecycle, and summary fields

### `GET /runs/{run_id}`

Purpose:

- fetch one run in JSON form

Typical response:

- one run object, or `404` if missing

### `GET /runs/{run_id}/events`

Purpose:

- fetch the raw persisted event stream for one run

Typical response:

- ordered array of event objects

Event shape includes:

```json
{
  "event_id": "example-event-id",
  "stage": "pipeline_complete",
  "level": "info",
  "message": "Pipeline complete",
  "created_at": "2026-04-28T19:00:11+00:00",
  "payload_json": "{...}"
}
```

## Settings API

### `GET /settings`

Purpose:

- fetch the current active settings view

Typical response:

- JSON object of active settings values

### `POST /settings/{key}`

Purpose:

- update one settings key through the JSON API

Behavior:

- coerces value to schema type
- validates before persistence

Typical response:

- `200 OK`

## Operator HTML Surfaces

### `GET /admin/runs`

Purpose:

- runs list UI

### `GET /admin/outbox-replay-health.json`

Purpose:

- machine-readable aggregate outbox/dead-letter replay health for a runs view

Query params:

- `view` = `active` | `all` | `archived` (default `active`)

Typical response shape:

```json
{
  "view": "active",
  "run_count": 12,
  "generated_at": "2026-05-02T23:01:00+00:00",
  "outbox_replay_health": {
    "dead_letter_total": 0,
    "impacted_runs": 0,
    "replay_event_count": 5,
    "replay_candidates": 10,
    "replayed": 10,
    "failed": 0,
    "replay_success_ratio": 1.0,
    "status": "healthy"
  }
}
```

### `POST /admin/outbox-replay-health/check`

Purpose:

- evaluate outbox replay health against threshold policy and emit auditable control-plane event

Query params:

- `view` = `active` | `all` | `archived` (default `active`)
- `min_replay_success_ratio` (default `0.95`)
- `emit_event` (default `true`)
- `event_run_id` (default `system-outbox-replay-health`)

Typical response shape:

```json
{
  "view": "active",
  "run_count": 12,
  "min_replay_success_ratio": 0.95,
  "decision": "ok",
  "reason_code": "healthy",
  "outbox_replay_health": {
    "dead_letter_total": 0,
    "impacted_runs": 0,
    "replay_success_ratio": 1.0,
    "status": "healthy"
  }
}
```

### `GET /admin/runs/{run_id}`

Purpose:

- run detail UI

This is the main human-facing inspection surface for:

- timeline
- run health
- stage metrics
- exports
- checkpoint/continue flow
- synonym review activity

## Reuse Contract (Run Detail + Artifacts)

The pipeline now exposes one symmetric reuse decision envelope and per-stage reuse metrics across:

- `enrich`
- `ranking`
- `cv_analysis`
- `cv_generation`

### Reuse Decision Envelope

Where present in stage output rows, `reuse_decision` uses:

```json
{
  "decision": "reused_exact_match | fresh_compute | reuse_disabled",
  "reason_code": "exact_fingerprint_match | no_reusable_snapshot_match | stage_reuse_disabled | ...",
  "fingerprint": "stage input fingerprint or null",
  "source_run_id": "optional source run id",
  "source_artifact_type": "enrich | ranking_ai_score | cv_analysis | cv_generation | ..."
}
```

### Stage Reuse Metrics

Run detail surfaces reuse metrics from stage transition artifacts under:

- `stages.<stage_id>.decision_summary.reuse_metrics`

Current metric buckets:

- `enrich`: `reused_rows`, `fresh_rows`, `total_rows`, `reuse_rate`
- `ranking`: `reused_ai_scores`, `fresh_ai_scores`, `total_ai_scores`, `reuse_rate`
- `cv_analysis`: `analysis_rows_executed`, `reused_analysis_rows`, `fresh_analysis_rows`, `analysis_reuse_rate`
- `cv_generation`: `reused_rows`, `fresh_rows`, `total_rows`, `reuse_rate`

### Reuse Anomaly Event

When overlap exists but reuse rate drops below floor, pipeline emits diagnostic event:

- stage: `reuse_anomaly`
- payload includes:
  - `reuse_rate_floor`
  - `min_overlap`
  - breached stages with `total`, `reused`, `fresh`, `reuse_rate`, and reason histogram

Default guard (configurable under `reuse.anomaly_guard`):

- `min_overlap = 5`
- `reuse_rate_floor = 0.05`

### `GET /admin/settings`

Purpose:

- settings UI

## Run Lifecycle Action Routes

These are primarily operator-facing action endpoints:

- `POST /admin/runs/{run_id}/stop`
- `POST /admin/runs/bulk/cancel`
- `POST /admin/runs/bulk/archive`
- `POST /admin/runs/bulk/unarchive`
- `POST /admin/runs/{run_id}/archive`
- `POST /admin/runs/{run_id}/unarchive`
- `POST /admin/runs/{run_id}/repair-cancellation`
- `POST /admin/runs/{run_id}/continue`
- `POST /admin/runs/{run_id}/synonym-overlay`
- `POST /admin/runs/{run_id}/cv-review-action`
- `POST /admin/runs/{run_id}/synonym-proposals/{proposal_id}/action`
- `POST /admin/runs/{run_id}/synonym-proposals/batch-action`
- `POST /admin/runs/{run_id}/synonym-proposals/apply-approved-to-run`
- `POST /admin/runs/{run_id}/synonym-proposals/promote-preview`
- `POST /admin/runs/{run_id}/synonym-proposals/promote-commit`
- `POST /admin/runs/{run_id}/synonym-proposals/triage-refresh`

These generally redirect in the HTML workflow, and they may return conflict or
not-found errors when the run is not in a valid state for the action.

## Export And Debug Routes

### Run-scoped exports

- `GET /admin/runs/{run_id}/export.json`
- `GET /admin/runs/{run_id}/hitl-review-audit.json`
- `GET /admin/runs/{run_id}/cv-debug.json`
- `GET /admin/runs/{run_id}/cv-analysis-trace.json`
- `GET /admin/runs/{run_id}/agentic-live-trace.json`
- `GET /admin/runs/{run_id}/stage-artifacts.json`
- `GET /admin/runs/{run_id}/stage-artifacts/{stage_id}.json`
- `GET /admin/runs/{run_id}/settings-used.json`
- `GET /admin/runs/{run_id}/mapping-suggestions.json`
- `GET /admin/runs/{run_id}/synonym-proposals.json`
- `GET /admin/runs/{run_id}/synonym-proposals-trace.json`
- `GET /admin/runs/{run_id}/approved-synonym-proposals.yaml`
- `GET /admin/synonyms/global.yaml`
- `GET /admin/synonyms/global-domain.yaml`
- `GET /admin/synonyms/global-role-family.yaml`
- `GET /admin/runs/{run_id}/artifacts.zip`

These routes expose the main observation payloads for completed or sufficiently
advanced runs.

Common behavior:

- `404` when the artifact does not exist yet
- `409` when the route is only valid for succeeded runs and the run is not yet
  terminal

Review audit note:

- `hitl-review-audit.json` contains run-scoped `review_required` queue items and
  action history (`approve`, `regenerate_once`, `reject`)
- `export.json` rows may include HITL fields such as:
  - `hitl_review_required`
  - `hitl_review_reason`
  - `hitl_review_pending`
  - `hitl_review_action`
  - `hitl_review_actor`
  - `hitl_review_action_at`
  - `hitl_review_note`
  - `hitl_review_category`

Trace-specific note:

- shared-standard trace downloads currently include:
  - `cv-analysis-trace.json` with `step_id=cv_analysis`
  - `agentic-live-trace.json` with `step_id=cv_generation`
  - `synonym-proposals-trace.json` with `step_id=synonym_proposals`

### Filtered enriched export for rerun input

- `GET /admin/runs/{run_id}/enriched/export-filtered.zip`

Purpose:

- export server-side filtered enriched rows as rerun-ready bundle.

Query params:

- `filter_name` (`all | passed | rejected | unknown`)
- `q` (search query)
- repeated `pipeline_outcome` values (for example `not_shortlisted`, `scored_not_ranked`)

Response:

- `application/zip`
- includes:
  - `jobs.filtered.jsonl`
  - `jobs.filtered.manifest.json`

JSONL row shape (`rerun_input.v1`):

- `schema_version`
- `job_url`
- `source_run_id`
- `pipeline_outcome`
- `filter_status`
- `shortlist_status`
- `scoring_status`
- `final_top_n_status`
- `raw_job`

Manifest highlights:

- `schema_version`
- `generated_at`
- `source_run_id`
- `export_id`
- `filters`
- `row_count`
- `ordering`
- `checksum_sha256`
- `warnings`

Compatibility note:

- bundle is designed for direct re-upload into trigger flow using JSONL mode described below.

### Upload trigger JSONL compatibility

`POST /admin/upload-trigger` with `jobs_input_mode=upload` now accepts:

- JSON array files (existing behavior)
- rerun JSONL files (`.jsonl`) where each line contains an object with `raw_job`

JSONL validation:

- each non-empty line must be valid JSON object
- each row must include object field `raw_job`
- uploaded rows are converted into canonical merged jobs JSON array snapshot

### Aggregate exports

- `GET /admin/mapping-suggestions.json`
- `GET /admin/synonym-proposals.json`
- `GET /admin/outbox-replay-health.json`

These are useful for cross-run review workflows and higher-level operator
inspection.

## Synonym Proposal Review Routes

### Run-scoped review and promotion routes

- `POST /admin/runs/{run_id}/synonym-proposals/{proposal_id}/action`
- `POST /admin/runs/{run_id}/synonym-proposals/batch-action`
- `POST /admin/runs/{run_id}/synonym-proposals/promote-preview`
- `POST /admin/runs/{run_id}/synonym-proposals/promote-commit`
- `POST /admin/runs/{run_id}/synonym-proposals/triage-refresh`

Purpose:

- execute HITL review actions in the context of one run
- support repeated batch review submissions
- explicitly apply approved pairs into the current run snapshot for downstream stages
- preview and confirm explicit promotion of approved run proposals into global
  synonym policy

Typical form fields:

- `action` (`approve` / `defer` / `reject`) for single-action route
- `proposal_action__{proposal_id}` entries for batch route
- `promote_proposal_id` entries for promotion preview
- `acted_by`, `note` for apply-approved-to-run
- `selected_ids_csv`, `acted_by`, `note` for promotion commit
- `acted_by`, `note` for triage refresh

Typical behavior:

- review routes redirect back to run detail
- batch route returns per-submit summary via query params:
  - `synonym_batch_applied`
  - `synonym_batch_skipped`
  - `synonym_batch_failed`
- apply-approved-to-run returns summary via query params:
  - `synonym_apply_to_run_applied`
  - `synonym_apply_to_run_skipped`
  - `synonym_apply_to_run_failed`
- promote commit returns summary via query params:
  - `synonym_promote_applied`
  - `synonym_promote_skipped`
  - `synonym_promote_failed`
  - `synonym_promote_new_aliases`
  - `synonym_promote_unchanged_aliases`
  - `synonym_promote_overridden_aliases`
- triage refresh returns summary via query params:
  - `synonym_triage_triaged`
  - `synonym_triage_reused`
  - `synonym_triage_skipped`
  - `synonym_triage_failed`
- promotion requires run-scoped status `approved_for_run_overlay`
- approved overlay export is delta-only (run-approved pairs), not a full global map export
- global export endpoints return the full canonical policy maps:
  - `GET /admin/synonyms/global.yaml` (skills)
  - `GET /admin/synonyms/global-domain.yaml` (domain)
  - `GET /admin/synonyms/global-role-family.yaml` (role family)
- promote-to-global is a merge/overlay into SSOT:
  - `config/taxonomy/skill_synonyms.yaml`
  - `config/taxonomy/domain_synonyms.yaml`
  - `config/taxonomy/role_family_synonyms.yaml`
  where alias collisions are explicit overrides within each field map

Triage refresh behavior:

- updates recommendation fields only (`recommended_action`, confidence,
  rationale, risk flags, runtime metadata)
- does not mutate `proposal_status`
- supports in-run reuse using proposal/runtime fingerprint matching

### Legacy aggregate review routes

- `POST /admin/synonym-proposals/{proposal_id}/start-review`
- `POST /admin/synonym-proposals/{proposal_id}/approve-for-run-overlay`
- `POST /admin/synonym-proposals/{proposal_id}/reject`
- `POST /admin/synonym-proposals/{proposal_id}/defer`

Purpose:

- move a persisted proposal through the review workflow

Typical form fields:

- `acted_by`
- `note`

Typical response shape:

```json
{
  "proposal_id": "synprop-example",
  "run_id": "run-example",
  "proposal_status": "approved_for_run_overlay"
}
```

## File Download Routes

### `GET /admin/cvs/{version_id}/download`

Purpose:

- download one generated CV as Markdown

## Notes On Payload Expectations

- Run objects expose lifecycle and summary data, not every stage detail inline
- Event payloads may include JSON-encoded machine detail inside `payload_json`
- Export routes are the preferred source for detailed agentic and stage-owned
  debugging payloads
- Control-plane orchestration observability now emits structured diagnostics for:
  - backend execution (`control_plane.backend_execution`)
  - routing diagnostics (`control_plane.model_routing`)
  - fallback binding (`control_plane.backend_fallback_binding`)
- The API shape is closely tied to the control plane and background worker, not
  a separate public product API boundary

## Related Docs

- [observability.md](observability.md)
- [usage.md](usage.md)
- [setup.md](setup.md)
- [architecture.md](architecture.md)
