---
doc_id: configuration
doc_type: operator-guide
explains:
  features:
    - settings_system
    - trigger_run_management
  configs:
    - .env
    - config/runtime/control_plane.yaml
    - config/runtime/pipeline.yaml
---

# Configuration

FitCV uses layered configuration with clear ownership boundaries.

## Primary Runtime Inputs

- trigger base config file (`config_path` in `/runs` request; default `config/env.yaml`)
- persisted control-plane settings (`/admin/settings` and `/settings` surfaces)
- per-run trigger overrides (`config_overrides` in `/runs`)
- process environment variables for backend/provider credentials and runtime toggles

## Canonical Ownership Matrix (Option B Baseline)

This matrix defines current SSOT ownership for migration execution.

| Config surface | Canonical ownership | Notes |
| --- | --- | --- |
| `config/runtime/control_plane.yaml` | control-plane backend/provider/model routing defaults | Includes `control_plane.data_backend.*`, `control_plane.providers.*`, `control_plane.model_routing.*`, feature flags, observability toggles. |
| `config/runtime/pipeline.yaml` | pipeline execution knobs | Owns enrichment/rerank timing, top-N controls, lifecycle limits, replay health thresholds, model defaults used by runtime pipeline stages. |
| `config/policy/cv.yaml` | CV generation and validation policy | Owns nested `cv.*` contract (`preset`, composition, validation, generation defaults). |
| `config/taxonomy/taxonomy.yaml` | shared business taxonomy and enum families | Owns seniority taxonomy, location/contract/experience enums, role taxonomy maps. |
| `config/env.yaml` | legacy compatibility base input + infra bridge | Current default `config_path`; still carries infra keys and some legacy policy/runtime-adjacent keys during migration window. |
| `config/env.private.yaml` | no active canonical owner in this worktree | File not present in tracked worktree; treat as deprecated/removed unless explicitly reintroduced as local-only untracked override. |

### Legacy-Duplicate Classification (Current Baseline)

- compatibility-only (to be drained from `config/env.yaml`): `seniority_ladder`, `application_statuses`, `cv_analysis_min_score`, overlap with runtime knobs that already live in `config/runtime/pipeline.yaml`
- canonical-infra bridge (kept until loader migration complete): `gcp_project`, `bigquery_dataset`, `service_account_key`, `location`
- removable private surface: `config/env.private.yaml` (no active tracked consumer in this worktree baseline)
- removable smoke surface: `config/live_smoke.yaml` (duplicates infra/model ownership outside canonical runtime files)

### Task 1 Ownership And Disposition Decisions

| Candidate | Canonical owner | Decision | Consumer evidence |
| --- | --- | --- | --- |
| `config/env.yaml:cv_acceptance_policy` duplicate block | `config/env.yaml` (single block only) | remove duplicate declaration, keep one canonical block | `src/fitcv/pipeline.py` + `src/fitcv/config.py:get_cv_acceptance_policy` |
| `max_cv_jobs`, `cv_analysis_min_score`, `required_skill_overlap_min`, `preferred_skill_overlap_min`, `language_match_min`, `summary_quality_min` in `config/env.yaml` | `config/runtime/pipeline.yaml` | drain from `env.yaml`; keep compatibility mapping only where active runtime still needs bridge | overlap warnings and compatibility projection in `src/fitcv/config.py` |
| `seniority_ladder` in `config/env.yaml` | `config/taxonomy/taxonomy.yaml` | keep temporary compatibility-only bridge; block expansion | consumer in `src/fitcv/rule_filter.py` |
| `application_statuses` in `config/env.yaml` | `config/taxonomy/taxonomy.yaml` | keep temporary compatibility-only bridge; block expansion | consumer in `src/fitcv/tracker.py` |
| `control_plane.model_routing.parts.cv_analysis_semantic_alignment` | none (no active runtime consumer) | remove key from `config/runtime/control_plane.yaml` | no source consumer found; only config declaration present |
| `config/live_smoke.yaml` | none (retired in Razor+SSOT lane) | delete file; no runtime dependency allowed | current file duplicates infra/model keys and increases ownership ambiguity |
| `config/env.private.yaml` | none (not tracked) | classify as deprecated local-only override; do not add tracked runtime dependency | file missing in tracked worktree baseline |

### CV Acceptance Strictness Policy (Option B)

Canonical policy owner in this lane: `config/env.yaml` key `cv_acceptance_policy`.

Policy meaning:

- `required_match.min_ratio_by_fit.<fit>`: minimum `matched_required / matchable_required_count` ratio to remain eligible for auto-accept.
- `required_match.max_missing_by_fit.<fit>`: maximum missing required items allowed for auto-accept.
- `force_review_when_any_required_missing_for_fits`: fit classes that must route to `review_required` when any required item is missing.

`review_required` semantics in this contract:

- non-fatal HITL branch
- generation/validation output can exist
- auto-accept is blocked by policy and requires operator decision (accept/reject/edit)
- hard-failure statuses remain reserved for `validation_failed`, `generation_failed`, `persistence_failed`
### Planned Deprecation Boundaries

1. `config/env.yaml` remains accepted as trigger default during transition, but ownership must shift to canonical runtime/policy/taxonomy files.
2. `config/env.private.yaml` is treated as deprecated in tracked repo state; no new runtime dependency may be introduced.
3. Legacy `.env.yaml` references in scripts/tests/control-plane UI remain compatibility targets until explicit removal gates pass.

### Legacy Removal Gates

Remove legacy compatibility behavior only when all gates pass:

1. reference gate:
   - repo scan shows no required runtime entrypoint depends on removed legacy key/path shape
   - command: `rg -n "\\.env\\.yaml|config/env\\.yaml|seniority_ladder|application_statuses" src scripts tests docs -S`
2. parity gate:
   - legacy and canonical config inputs produce equivalent agreed projections in tests
   - command: `pytest tests/test_config.py -q`
3. contract gate:
   - repo contract checks pass after any deprecation-path removal patch
   - command: `python scripts/hooks/run_validator.py --fast`

## Config Invariants

- secrets are env-only
- no secret values in YAML
- no secret key-name indirection in YAML
- `settings-used.json` is the run-time evidence snapshot

## Effective Settings Resolution

At trigger time, the control plane composes effective run settings in this order:

1. load base config from `config_path` (`TriggerRequest.config_path`)
2. load persisted active settings (`load_active_settings(...)`)
3. apply persisted settings into base config (`apply_settings_to_config`)
4. apply run-scoped trigger overrides (`config_overrides`) after validation/coercion
5. recompute derived compatibility fields
6. persist the run-scoped snapshot as `settings-used.json`/effective settings payload

Interpretation rules:

- `/admin/settings` edits persisted defaults for future runs only.
- trigger-time per-run overrides do not mutate saved defaults.
- completed-run truth belongs to the run-scoped effective settings snapshot.
- process environment variables control backend/provider credentials and runtime wiring; they are not persisted through settings-save routes.

## Settings Surface Ownership

The settings page intentionally mixes editable controls with metadata-only rows.

- editable: schema-backed controls with persistence keys and save handlers
- metadata-only: fixed/runtime-owned values shown for operator context and provenance
- hidden-deprecated: compatibility keys intentionally removed from operator UI and rejected by settings-save routes (`422`) to prevent false runtime authority signals
- compatibility-readonly: legacy alias keys shown for migration visibility but rendered non-editable; canonical runtime throughput keys remain single editable authority

Examples:

- editable: retrieval funnel sizes, ranking weights, timing, run lifecycle guard, CV composition toggles
- metadata-only: fixed runtime-contract fields such as single-option model metadata
- hidden-deprecated: legacy AI-authority controls (for example `cv_generation_model`) retained only for compatibility projection, not operator control
- compatibility-readonly: runtime throughput legacy aliases mapped to canonical `stage_runtime.*` keys
- canonical-save-path: settings-save routes persist canonical `stage_runtime.*` throughput keys and ignore compatibility alias inputs in timing-section writes
- enrich-pacing-contract: enrich concurrency controls worker parallelism; shared request-start pacing still limits aggregate request rate
- ranking-pacing-contract: ranking uses `stage_runtime.ranking.concurrency` workers, but submit loop still sleeps `stage_runtime.ranking.sleep_secs` between submissions, so provider timestamps can look sequential when sleep is positive

### Settings Mode Strip SSOT Contract

- mode-strip fields (`Agentic Mode`, `Live Provider`, `Live Model`, `Authority State`) are resolved by one control-plane authority resolver
- `Agentic Mode` source: persisted setting `cv.agentic_late_stage.enabled`
- `Live Provider` and `Live Model` source: runtime env `FITCV_LANGGRAPH_PROVIDER` / `FITCV_LANGGRAPH_MODEL`
- `Authority State`:
  - `aligned`: toggle and runtime env expectations are consistent
  - `drifted`: toggle/env expectations conflict; UI shows explicit drift reason

## Backend and Provider Routing

- backend routing: process env `FITCV_CP_DATA_BACKEND` selects backend mode (`sqlite` / `bigquery`) at runtime
- local sqlite persistence authority:
  - pipeline run snapshots persist in sqlite (`local_pipeline_runs`)
  - pipeline run events persist in sqlite (`local_pipeline_run_events`)
  - local in-process caches are non-authoritative and restart survival must come from sqlite tables
- model/provider routing defaults: canonical owner is `config/runtime/control_plane.yaml`
  - `control_plane.providers.*.base_url`
  - `control_plane.providers.*.wire_api`
  - `control_plane.model_routing.parts.*.provider`
  - `control_plane.model_routing.parts.*.model`
- env overrides for LangGraph runtime expectation are optional and override-only:
  - `FITCV_LANGGRAPH_PROVIDER`
  - `FITCV_LANGGRAPH_MODEL`
  - `FITCV_LANGGRAPH_OPENAI_BASE_URL`
  - `FITCV_LANGGRAPH_WIRE_API`
- precedence for routing expectation:
  1. non-empty `FITCV_LANGGRAPH_*` env values (override-only path)
  2. control-plane defaults from `config/runtime/control_plane.yaml`
  3. fail fast if required fields remain unresolved
- provider credentials: read from process env

## AI-Plane Contract (Migration Freeze)

This migration lane defines a strict split between storage backend selection and AI runtime behavior.

- `data_plane` ownership:
  - runtime backend selection (`sqlite` / `bigquery`)
  - persistence/query adapter behavior only
- `ai_plane` ownership:
  - provider + model routing via `control_plane.model_routing.parts.*`
  - AI credential contract via process env keys

Prohibited coupling:

- backend mode must not alter AI provider selection
- backend mode must not alter AI model selection
- backend mode must not alter AI auth key resolution order

Canonical AI auth contract:

- primary key: `FITCV_LLM_API_KEY`
- temporary compatibility aliases (deprecation window only): `OPENAI_API_KEY`, `OPENAI_COMPATIBLE_API_KEY`
- `service_account_key` is not an AI credential and is reserved for BigQuery data-plane auth only

Fail-fast runtime contract:

- if routed AI provider/model cannot be resolved, runtime must fail with explicit configuration error
- if AI auth key is missing for routed HTTP provider, runtime must fail with explicit credential error
- no implicit Gemini/default model fallback is allowed as runtime authority in unified mode

## Related Docs

- [setup.md](setup.md)
- [usage.md](usage.md)
- [architecture.md](architecture.md)

