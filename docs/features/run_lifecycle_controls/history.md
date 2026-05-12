# Run Lifecycle Controls — History

<!-- GENERATED HISTORY START -->

## 2026-04-22

### Option B Phase 2 Rollout Implementation Plan


Verification:
- `See plan body closeout verification notes.`

Outcome:
Completed the Option B phase 2 rollout.

### Option B Phase 3 Cleanup Implementation Plan


Verification:
- `See plan body closeout verification notes.`

Outcome:
Completed the Option B phase 3 cleanup.

### Phase 4 Required Metadata Correction Implementation Plan


Verification:
- `See plan body closeout verification notes.`

Outcome:
Completed the phase 4 required metadata correction.

### Phase 5 Evidence-Oriented Lineage Alignment Implementation Plan


Verification:
- `See plan body closeout verification notes.`

Outcome:
Completed the phase 5 evidence-oriented lineage alignment.

### Phase 6 Lineage Evidence Hydration Implementation Plan


Verification:
- `See plan body closeout verification notes.`

Outcome:
Completed the phase 6 lineage evidence hydration.

<!-- GENERATED HISTORY END -->

## Human Notes

## Changelog

### 1.4.0 — active

- Timeout wording now distinguishes queue wait, active runtime, and Stage by Stage manual-wait time
- This keeps the existing server-owned timeout guard behavior while making the operator-facing contract clearer across the two execution modes

### 1.3.0 — active

- The control plane now enforces a server-owned max-runtime guard for unfinished runs
- Timed-out queued and paused `awaiting_continue` runs now close as `cancelled`, while timed-out `running` and `cancelling` runs close as timeout failures
- Timeout transitions append dedicated lifecycle events so operators can distinguish them from admin cancels and ordinary execution failures

### 1.2.0 — active

- `awaiting_continue` runs are now cancellable and transition directly to terminal `cancelled`
- Single-run and bulk cancel now share the same broadened non-terminal eligibility rule
- Paused-run cancellation appends a dedicated lifecycle event so it stays distinguishable from queue and cooperative cancellation

### 1.1.0 — active

- Added bulk `cancel`, `archive`, and `unarchive` lifecycle endpoints for selected runs
- Runs-list UI now supports visible-row multi-selection with a conditional bulk action bar
- Batch lifecycle responses report requested, processed, and skipped runs explicitly while preserving per-run audit events

### 1.0.0 — active

- Cancel queued runs via RQ (`cancel_queued_run` in `queue.py`)
- Cooperative cancellation: `_cancellation_check` callback in `worker_job.py`, 3 checkpoints in `pipeline.py` (before enrichment, AI scoring, CV generation)
- `PipelineCancelled` exception for clean mid-flight abort
- Stale cancellation repair endpoint (`/admin/runs/{run_id}/repair-cancellation`)
- Archive and unarchive terminal runs with audit trail in `pipeline_run_events`
- Full test coverage: 12+ tests in `test_app.py` covering cancel, archive, unarchive, repair, and filter scenarios

### 0.4.0 — building

- Archive and unarchive terminal runs
- Stale cancellation repair endpoint

## Post-Execution Review

- All capabilities from the contract are implemented and tested
- Cooperative cancellation pattern checks BigQuery for `cancel_requested_at` at each checkpoint
- Three-tier stop logic: queue cancel → pre-claim cancel → cooperative cancelling
