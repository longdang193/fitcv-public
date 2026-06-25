---
doc_id: usage
doc_type: operator-guide
explains:
  features:
    - inspection_debugging
    - settings_system
    - trigger_run_management
  stages:
    - cv_analysis
    - cv_generation
    - ranking
---

# Usage

FitCV usage splits into operator usage (UI/API) and engineering usage (runtime/tests/docs sync).

## Operator Flow

Entry point: `/admin/runs`

1. trigger a run (path/upload/paste)
2. choose run mode (`Run All` or `Stage by Stage`)
3. monitor events, stage progress, and lifecycle state
4. inspect run detail tabs and stage artifacts
5. export evidence (`export.json`, `cv-debug.json`, `settings-used.json`, stage artifacts, artifacts zip)

### Run Detail Overview Navigation

Run detail is now decision-first. Default view emphasizes status, outcome, warnings, next actions, stage snapshot, and effective-settings delta.

Workflow entry routes:

- synonym review workspace: `GET /admin/runs/{run_id}/synonym-review`
- artifacts workspace: run detail exports section (`GET /admin/runs/{run_id}` + `#run-exports-workspace`)

Diagnostics access:

- diagnostics section entry: `#diag-synonym-fingerprints`
- advanced diagnostics container: `#advanced-diagnostics`
- exports workspace anchor: `#run-exports-workspace`

Tooltip glossary semantics:

- `confidence`: model certainty for suggested mapping
- `triage mode`: freshness/reuse mode for recommendation decisions
- `suppressed`: proposal hidden by suppression policy or duplicate resolution
- `alias conflict`: alias already mapped to a different canonical value
- `run-scoped overlay`: override applies only to this run, not global defaults

Artifact truth note:

- run-scoped persisted artifacts/endpoints are source of truth
- local `artifacts/live_run_<run_id>/` is deterministic evidence mirror for portability/debug handoff
- backfill missing historical mirrors with:
  - `python scripts/backfill_live_run_artifacts.py --run-id <run_id> --dry-run`
  - `python scripts/backfill_live_run_artifacts.py --run-id <run_id>`

## Lifecycle Actions

Operator lifecycle actions are exposed through run-scoped admin routes:

- stop active run: `POST /admin/runs/{run_id}/stop`
- continue checkpointed run: `POST /admin/runs/{run_id}/continue`
- archive/unarchive run: `POST /admin/runs/{run_id}/archive`, `POST /admin/runs/{run_id}/unarchive`
- bulk archive/unarchive/cancel: `POST /admin/runs/bulk/archive`, `POST /admin/runs/bulk/unarchive`, `POST /admin/runs/bulk/cancel`
- reconciliation/repair when needed: `POST /admin/runs/{run_id}/repair-cancellation`
- bulk delete archived runs: `POST /admin/runs/bulk/delete-archived`

Archive and delete stay separate on purpose:

- `Archive` hides run from active view but keeps run detail, events, and exports.
- `Delete archived runs` is available only from `/admin/runs?view=archived`.
- delete uses `archived_at` age, defaults to `Older than 30 days`, and sends the currently matched archived run ids from the UI preview so confirmation and execution stay aligned.
- delete does not clear shared caches, embeddings, bookmarks, or settings.

## Settings Workflow

Use `/admin/settings` to tune future-run defaults.

Operator truth model:

1. adjust shared defaults on `/admin/settings`
2. optionally apply trigger-time per-run overrides when starting a run
3. verify historical run truth from run-level `settings-used.json`

Important:

- editing settings does not rewrite past runs
- per-run overrides do not change shared saved defaults
- metadata-only rows in Settings are informational (runtime-owned), not editable controls

## Engineering Workflow

1. run app/worker in sqlite or bigquery mode
2. reproduce/verify via live run
3. run focused tests
4. run contract/validator checks before merge

## Key Surfaces

- `GET /healthz`
- `POST /runs`
- `GET /runs/{run_id}`
- `GET /runs/{run_id}/events`
- `GET /admin/runs`
- `GET /admin/runs/{run_id}`
- `GET /admin/settings`

## Related Docs

- [setup.md](setup.md)
- [pipeline.md](pipeline.md)
- [architecture.md](architecture.md)
