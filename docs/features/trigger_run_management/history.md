# Trigger & Run Management — History

## Changelog

### 2.17.0 — active

- Run-owned artifact bundles now export `run_mode` / `run_mode_label`, so downloaded diagnostics can distinguish `Run All` from `Stage by Stage` without a control-plane lookup

### 2.16.0 — active

- Run detail now offers `Download All Artifacts (.zip)` as a convenience bundle for currently available run-owned artifacts
- The bundle follows the same stage-gating rules as the individual exports, so partial runs export only what they have actually reached
- Individual artifact downloads remain unchanged and continue to be the authoritative surfaces

### 2.15.0 — active

- `Run All` now persists shared stage progress and stage-owned artifacts as each stage boundary is reached instead of waiting until final success
- `Run All` and `Stage by Stage` now share canonical labels across trigger, runs list, and run detail surfaces
- Staged-only continuation and the staged-only post-enrich synonym override remain the only intentional mid-run control differences

### 2.14.0 — active

- Run-results exports now stay job-centric and no longer carry row-level shortlist-debug baggage that is already owned by shortlist-stage diagnostics
- Large runs now avoid some row-scaled Layer 4 skip and validation-event noise, leaving aggregate stage summaries and stage-owned artifacts as the primary operator surface
- This keeps run-detail behavior stable while reducing dead-weight export and timeline volume

### 2.13.0 — active

- Run detail now opens as a summary-first shell and lazy-loads heavy inspection panes instead of blocking the first render on enriched-job queries
- The `Enriched Jobs` pane now uses server-owned search, filter, and pagination so large runs remain operable without shipping every row to the browser
- Timeline rendering is now bounded on first paint, with older events loaded intentionally instead of by default

### 2.12.0 — active

- Mapping-suggestions exports now stay unavailable until `enrich` has actually been reached, instead of appearing from an empty pre-enrich snapshot
- Normalize now always emits an aggregate timeline row so paused-after-normalize runs can download the normalize artifact through the timeline even when no duplicates were removed
- Run detail health indicators now distinguish `Pending` unreached-stage metrics from reached-but-empty `N/A` metrics

### 2.11.0 — active

- Run detail now gates stage-owned exports by stage reachability so files like `Mapping Suggestions JSON` are not offered prematurely
- The timeline now keeps stage JSON download links on aggregate stage rows instead of repeating them on per-job subevents
- The synonym-overlay card now exposes the actual YAML snapshot when available, making run-scoped synonym inspection more actionable

### 2.10.0 — active

- Runs page now lets operators attach a run-scoped synonym-overlay YAML before trigger for both `Run All` and `Stage by Stage`
- Manual staged runs still support a later enrich-checkpoint replacement upload before continuing into `rule_filter`
- Run detail now shows synonym-overlay state in a dedicated card instead of mixing the upload control into the top action row

### 2.9.0 — active

- Runs list is now selection-first: row-level lifecycle actions were removed from the table, and bulk actions remain the list-level control surface
- The visible runs-table columns were reduced to the core operational fields so the list no longer overlaps around `Triggered By`, `Created`, and `Duration`
- Single-run lifecycle controls remain on run detail, so one-off continue/stop/archive actions are still available without crowding the list

### 2.8.0 — active

- Runs list now uses a compact per-row `⋯` action trigger instead of wide inline action buttons
- Long `jobs_path` values are truncated in-row with the full path preserved on hover
- This rollout keeps both bulk actions and per-run lifecycle controls visible without pushing the action column off-screen

### 2.7.0 — active

- Paused manual runs in `awaiting_continue` now expose `Stop Run` alongside `Run Next Stage`
- The runs list and run detail both surface the broadened cancel rule so operators do not need to resume a paused run before ending it
- This rollout keeps existing continue/resume behavior intact while making paused-run termination explicit

### 2.6.0 — active

- Runs list now supports visible-row selection with a conditional bulk action bar
- Operators can apply bulk cancel, archive, and unarchive actions without losing the existing per-run controls
- This rollout keeps lifecycle eligibility server-owned and limits phase 1 selection semantics to the currently visible list rows

### 2.5.0 — active

- Stage-slice downloads for `cv_analysis` can now expose channel-level evidence retrieval counts and final selected-evidence rationale before `cv_generation`
- Manual staged runs keep the same `ranking -> cv_analysis -> cv_generation` checkpoint order while exporting richer `cv_analysis` provenance

### 2.4.0 — active

- Manual staged runs now pause after `ranking` with `next_stage = cv_analysis`, and can pause again after `cv_analysis` before `cv_generation`
- Resuming into `cv_generation` now uses persisted `cv_analysis` outputs instead of recomputing evidence and fit-gate work by default
- This rollout keeps the existing staged/manual control-plane flow while splitting the final stage into two resumable checkpoints

### 2.3.0 — active

- Run-results shortlist debug now reports raw unique-job counts separately from raw row counts and carries explicit raw-hit flags for passed jobs
- Shortlist retrieval now uses the latest active persistent embedding row per canonical `job_url`, reducing duplicate-row noise in run-level shortlist inspection
- This rollout keeps persistent embeddings and shortlist backfill behavior intact

### 2.2.0 — active

- Admin settings now expose `rule_filter.selected_filters` so deterministic post-enrichment checks can be configured as blocking versus mark-only
- The default blocking set remains seniority, location type, contract type, and experience level; `must_have_skill_missing` and `domain_not_preferred` now default to mark-only
- This rollout keeps rule ownership in `rule_filter` and does not move those checks into ranking

### 2.1.0 — active

- Manual staged runs paused after `enrich` can now upload a run-scoped synonym-overlay YAML before continuing into `rule_filter`
- Uploaded overlays are persisted on the run's effective settings snapshot and apply only to that run's downstream stages
- This rollout keeps the trusted base `config/skill_synonyms.yaml` unchanged and does not add a full synonym editor

### 2.0.0 — active

- Added a staged/manual run mode that pauses after each major stage and persists checkpoint metadata plus a serialized checkpoint payload for continuation
- Manual runs can now resume from `next_stage` via explicit `Run Next Stage` actions instead of restarting the full pipeline by default
- Run detail and runs-list surfaces now distinguish automatic vs manual execution while preserving the existing one-click `run_all` flow

### 1.9.0 — active

- Run detail stage-artifact downloads now carry richer per-stage input, output, and changed-state samples instead of summary-only stage blocks
- Timeline-linked stage-slice JSON downloads remain download-first while becoming more useful for debugging stage transitions
- This rollout preserves the existing artifact surfaces and does not add a unified run-bundle export

### 1.8.0 — active

- Run detail can now download a dedicated `settings-used.json` snapshot for succeeded runs when available
- Timeline rows for recognized stage-boundary events now expose per-stage JSON downloads derived from the persisted run-scoped stage-artifacts snapshot
- This rollout keeps the run detail strictly download-first and does not add an artifact viewer

### 1.7.0 — active

- Run detail can now download a run-scoped `stage-artifacts.json` snapshot for succeeded runs when available
- Stage-transition artifacts now expose bounded per-stage handoff summaries alongside the existing results export and CV debug download surfaces
- This rollout keeps stage-transition artifacts as an inspection/debug surface, not a replacement for the results export

### 1.6.1 — active

- Adopted the stage-aware doc system by mapping `trigger_run_management` to `primary_stage: normalize`
- Declared bounded stage participation across `normalize`, `enrich`, `rule_filter`, and `shortlist`
- This was a documentation-structure adoption only; no run-trigger runtime behavior changed by itself

### 1.6.0 — active

- Run-results export now includes an explicit decision chain per job so shortlist path, primary fit authority, CV attempt/skip, and validation outcome are visible without inference
- Ranked jobs now use reranker fit as the sole post-filter authority for CV eligibility instead of reconciling against a competing gap-fit decision
- Run detail pipeline outcomes now surface decision-chain detail alongside the existing status badge

### 1.5.3 — active
- Top-level run-results `shortlist_debug` now distinguishes raw vector-search hits from the later scoring shortlist, including explicit backfill counts and backfilled job URLs when retrieval misses passed jobs
- Layer 3 shortlist event messaging now reports raw vector hits separately from scoring-shortlist size when backfill occurs
- Legacy note: this release predated the later decision-consistency cleanup that made reranker fit the sole post-filter authority

### 1.5.2 — active

- Run-results export now includes shortlist debug context, including a per-row `shortlist_debug` block for passed jobs and a top-level shortlist summary with the candidate query text and shortlist counts
- `not_shortlisted` rows now explain that the job URL was not returned by vector search instead of only showing null scores
- Layer 4 gap matching now handles long requirement phrases more usefully and excludes obviously non-skill requirements from the fit-ratio denominator

### 1.6.0 — active

- Run detail now groups run-scoped JSON downloads into a compact `Run Exports` surface instead of a long top-row button pile
- Run detail now merges stage-quality and late-stage-reuse diagnostics into one compact `Run Health` card
- Enriched-jobs inspection now collapses fit metadata into a stacked `Fit Context` cell so `Pipeline Outcome` stays visible

### 1.5.1 — active

- Ranked jobs skipped by the Layer 4 fit gate now surface as `ranked_skipped_fit_gate` instead of being folded into the generic `ranked_no_cv` bucket
- Layer 4 fit-gated ranked jobs now emit CV-generation debug records, so run-scoped CV debug snapshots stay complete instead of dropping those jobs entirely
- Run detail pipeline outcome labels now show `Ranked, skipped by fit gate` for that status

### 1.5.0 — active

- Run-results export now distinguishes passed non-CV jobs as `not_shortlisted`, `shortlisted_not_scored`, or `scored_not_ranked` instead of collapsing them into one vague status
- Run detail now shows a compact `Pipeline Outcome` label for enriched jobs so “passed filter” is no longer confused with “CV should have been generated”
- Layer 4 CV generation now rebuilds ranked jobs from enriched context before gap analysis, so debug snapshots and gap summaries retain JD fields like title and required skills
### 1.5.0 — active

- `Stage by Stage` checkpoints now retain reranker-blocked CV debug rows through the final `cv_generation` resume so succeeded staged runs finish with truthful run-scoped artifacts


### 1.4.0 — active

- Run detail now exposes a separate `Download CV Debug JSON` action for succeeded runs when a run-scoped CV-generation debug snapshot is available

### 1.3.0 — active

- Run-results export now carries structured CV data and CV generation metadata when present
- Control-plane CV read paths can fetch structured CV fields from `cv_versions` while keeping markdown downloads unchanged

### 1.2.0 — active

- Run detail page exposes `Download Results JSON` for succeeded runs with an export snapshot
- Run-complete worker persists an immutable run-results export snapshot on `pipeline_runs`
- Export payload includes ordered jobs, enrichments, statuses, scores, and inline CV markdown when present

### 1.1.0 — active

- CV results banner on run detail page
- CV downloads for generated CVs
- Specs/plans: see `refs` in the feature contract

### 1.0.0 — active

- Initial feature: runs list page with status badges and three trigger modes
- Specs/plans: see `refs` in the feature contract

## Post-Execution Review

> Fill after status transitions to `active`.
