# Inspection & Debugging — History

## Changelog

### 2.28.0 — active

- Run-scoped artifact headers now version the reranker-blocked truth family explicitly, export `run_mode` / `run_mode_label`, and make bundle manifests self-describing without control-plane lookup
- `cv_analysis` reuse diagnostics now describe executed analysis rows separately from reranker-blocked rows, so run-health and stage-local reuse counters no longer mix "analyzed" with "blocked before analysis"

### 2.27.0 — active

- Compact `results.json` rows now keep reranker-blocked ranked jobs internally consistent by propagating `blocked_by_reranker_fit` into the compact `decision_chain` instead of falling back to a generic `not_run`
- `cv-debug.json` ranked-job coverage accounting now counts reranker-blocked rows as explicit non-attempted ranked jobs with omission reasons, so coverage summaries no longer silently drop them

### 2.26.0 — active

- Final-stage artifacts and operator surfaces now distinguish reranker-blocked ranked jobs from true analyzed-and-skipped jobs with the explicit `blocked_by_reranker_fit` outcome
- This keeps `results.json`, `cv_analysis` diagnostics, and run-detail messaging aligned with the real stage boundary instead of treating both outcomes as one generic skip path

### 2.25.0 — active

- Skipped-fit-gate rows in `results.json` no longer contradict themselves by pairing a row-level `cv_analysis.status = skipped_fit_gate` with a `decision_chain` placeholder of `not_run`
- The compact ledger now treats skipped-fit-gate as a completed `cv_analysis` outcome while keeping `cv_generation` explicitly unattempted

### 2.24.0 — active

- `results.json` rows now stay compact and job-ledger-only by dropping full `original_job`, full `enriched_job`, bulky score-explanation substructures, and full CV bodies
- `stage-artifacts.json` remains the heavy diagnostics bundle, so stage-owned samples, decision summaries, and provenance stay available without being duplicated into every job row
- Skipped-fit-gate CV-generation debug records now preserve their fit-gate outcome reason consistently instead of losing it at the final-stage debug handoff

### 2.23.0 — active

- `cv_analysis` stage summaries no longer report misleading fresh/reused embedding totals that were produced by summing cumulative per-record cache snapshots
- Per-record evidence-selection summaries still retain their local semantic embedding-count diagnostics for deep debugging

### 2.22.0 — active

- Run detail now offers `Download All Artifacts (.zip)` as a convenience bundle over the existing artifact contract
- Bundle contents are stage-gated and availability-aware, so partial runs export only the artifacts they have actually reached
- Each bundle now includes a lightweight `manifest.json` describing included and missing files

### 2.21.0 — active

- Run detail now shows shared stage progress for both `Run All` and `Stage by Stage` instead of treating progress as a staged-only concept
- `Run All` now persists reached-stage diagnostics during execution, so stage-owned artifact availability no longer lags behind continuous execution mode
- Checkpoint-only controls remain staged-only, keeping shared progress separate from resumability

### 2.20.0 — active

- The pipeline summary no longer computes retired top-level `stage_quality_metrics`, `late_stage_reuse_metrics`, or `shortlist_debug` blocks now that stage-owned diagnostics own those concerns
- `results.json` stays slimmer and more job-centric by dropping row-level `shortlist_debug` baggage while keeping decision-chain and outcome facts
- Layer 4 timeline noise is reduced by dropping per-job skip and validation-failed event emission in favor of aggregate stage summaries plus stage-owned artifacts

### 2.19.0 — active

- Run detail now renders as a lightweight summary shell while the `Enriched Jobs`, `Original Job Input`, and `Candidate Profile` tabs lazy-load their content as HTML fragments
- Enriched-job inspection moved from client-side row filtering to server-owned search, filter, and pagination so large runs no longer ship all rows in the initial HTML
- The event timeline now defaults to a bounded latest-event slice with a progressive older-events expansion path instead of rendering the full history on first paint

### 2.18.0 — active

- Mapping-suggestions snapshots are now persisted only after `enrich`, so pre-enrich paused runs no longer surface an empty but valid export
- Normalize now always emits an aggregate timeline row, which means paused-after-normalize runs can reach `Download Normalize JSON` through the normal stage-row ownership model even when zero duplicates were removed
- `Run Health` now distinguishes unreached-stage `Pending` metrics from reached-but-empty `N/A` metrics instead of treating all `0/0` rows like a completed result

### 2.17.0 — active

- `results.json` now stays job-centric as the run ledger, while `stage-artifacts.json` remains the bundled diagnostics export for stage-owned metrics, reuse, and samples
- Run detail now derives `Run Health` from stage-transition artifacts instead of relying on overlapping run-results diagnostic blocks
- Late-stage reuse snapshots remain available for internal reuse lookup, but they now live under a bounded diagnostic-support block instead of the main operator-facing results ledger

### 2.16.0 — active

- Run-detail exports are now stage-gated, so stage-owned downloads such as `Mapping Suggestions JSON` only appear after the owning stage has actually been reached
- The synonym-overlay card now shows a collapsible YAML snapshot instead of relying on `Effective Synonyms: N` as the primary inspection signal
- Event timeline rows now use humanized stage labels, compact aggregate stage summaries, aggregate-only stage download links, and severity-based `Run Health` tiles for faster diagnosis

### 2.15.0 — active

- `settings-used.json` now keeps canonical operator-facing settings in `effective_settings` and moves compatibility-era flat keys into an explicit `compatibility_projection` block when present
- Ranking-stage artifacts now expose the active AI-score prompt ID, template path, and model, while `cv_generation` artifacts and debug snapshots expose the active structured writer prompt ID and template path
- `cv-debug.json` now reports attempted-vs-non-attempted ranked-job coverage plus omission reasons so `snapshot_complete` is easier to interpret without cross-referencing other exports

### 2.13.0 — active

- Run detail now exposes a compact `Late-Stage Reuse` section for `ranking` AI-score reuse and `cv_analysis` reuse
- Ranking and `cv_analysis` artifacts plus run results now carry exact-match reuse statuses, stage input fingerprints, and fresh-vs-reused counts
- Run results now persist bounded late-stage reuse snapshots so reuse behavior can be audited from exports without extra storage lookups

### 2.12.0 — active

- Run detail now exposes a compact `Stage Quality Metrics` section so bottlenecks are visible without opening every stage JSON
- Stage artifacts and run results now carry authoritative stage-owned metric blocks for shortlist backfill rate, ranking label distribution, `cv_analysis` skip/ready/failure rates, and `cv_generation` accepted/validation-fail/runtime-failure rates
- Metric blocks stay bounded and omit later stages on partial manual runs instead of rendering misleading placeholders

### 2.11.0 — active

- `cv_analysis` artifacts can now show lexical-versus-semantic channel subscores, effective hybrid weights, semantic methods, and semantic embedding reuse state
- Shortlist debug and stage artifacts now expose whether the single candidate query embedding was reused or freshly generated, including its signature and contract fingerprint
- Shortlist inspection wording now stays explicit that the live path uses reusable job embeddings plus the deterministic candidate query vector, not stored candidate chunk embeddings

### 2.10.0 — active

- `cv_generation` validation snapshots now expose deterministic selected-evidence grounding violations separately from softer selected-evidence support failures
- Final-stage debug records and artifacts now carry compact support-source summaries so reviewers can see how hybrid grounding decided whether a CV stayed inside the selected evidence bundle

### 2.9.0 — active

- Shortlist debug now exposes bounded candidate-query components alongside the rendered `candidate_query_text`
- Reviewers can now see flattened-skill samples plus inferred role-family and domain hints that fed retrieval
- This makes shortlist misses easier to interpret without opening the candidate YAML directly

### 2.8.0 — active

- `cv_analysis` artifacts can now show separate retrieval-channel counts for required-skill, role, domain, and responsibility evidence lookup
- Analysis records and stage downloads now expose merged-pool sizing, selected-evidence IDs, matched channels, and selection reasons before CV writing

### 2.7.0 — active

- Stage-transition artifacts now expose separate `cv_analysis` and `cv_generation` blocks instead of folding all final-stage behavior into one stage
- `cv_analysis` surfaces generation-ready, skipped-fit-gate, and analysis-failed outcomes separately from generation/validation/persistence outcomes
- This rollout keeps the existing run-scoped CV debug snapshot surface while making final-stage artifact ownership more accurate

### 2.6.0 — active

- Ranking-stage samples now expose per-feature weighted score contributions alongside raw six-feature values and `final_score`
- Ranking-stage inspection now includes weighted preference-fit components plus configured preference-fit weights and fit-label thresholds in the decision summary
- This rollout improves ranking explainability and calibration visibility without adding a new in-browser artifact viewer

### 2.5.0 — active

- Shortlist stage artifacts and shortlist debug payloads now report fresh-vs-reused embedding counts for passed jobs
- Row-level shortlist inspection can expose `embedding_reuse_status`, `embedding_input_signature`, and `embedding_contract_fingerprint` when available
- Retrieval facts remain separate from embedding reuse facts so backfill and raw-hit debugging stay readable

### 2.4.0 — active

- Run-scoped enriched rows and enrich-stage artifact samples can now expose `raw_job_fingerprint`, `enrich_contract_fingerprint`, and `enrich_reuse_status`
- Enrich-stage decision summaries now report fresh-vs-reused enrich counts so repeated-run reuse behavior is visible without opening shared tables directly

### 2.3.0 — active

- Shortlist artifacts now report both raw row counts and raw unique-job counts while keeping the older unique-hit summary available for compatibility
- Shortlist output and changed-state samples now carry explicit retrieval facts such as `raw_hit_present`, `retrieval_anomaly_present`, and clearer shortlist outcomes
- This rollout improves shortlist inspection clarity without adding a new in-browser artifact viewer

### 2.2.0 — active

- Run detail now shows non-blocking rule-filter marks on passed rows instead of making selectable-screening passes look completely clean
- Rule-filter stage artifacts now report `selected_filters`, reject-reason counts, and mark-code counts for easier checkpoint debugging
- This rollout keeps marks additive to the existing `reasons` contract and does not add a new in-browser artifact viewer

### 2.1.0 — active

- Run detail now shows whether a run-scoped synonym overlay is active while a manual run is paused after `enrich`
- The enrich checkpoint exposes an upload action for reviewed synonym overlays alongside the existing mapping-suggestion downloads
- This rollout keeps the inspection surface checkpoint-oriented and does not add an in-browser synonym editor

### 2.0.0 — active

- Inspection surfaces now expose manual staged-run checkpoint state, including execution mode, checkpoint status, completed stages, and next stage
- Paused manual runs can inspect stage artifacts before continuation, and stage-artifact downloads remain available while a run is `awaiting_continue`
- Run detail and runs list now expose `Run Next Stage` actions for paused manual runs without introducing a separate debug viewer
- Settings-used snapshots and enrich-stage artifacts can now report effective enrich prompt provenance, including prompt ID, version, template path, and model

### 1.9.2 — active

- Run-scoped enriched-job reads now deserialize canonical skill entity JSON companions and mapping suggestions for inspection/debug surfaces
- Enriched exports can carry raw-plus-canonical enrich context without forcing downstream readers to reinterpret those JSON blobs manually

### 1.9.1 — active

- Newly generated stage-transition artifacts now report `schema_version: "stage_transition_artifacts_v3"` so the six-feature ranking artifact shape is version-detectable
- Shortlist stage artifacts now separate raw vector row counts from unique-job raw-hit counts
- Shortlist debug payloads now describe observed raw-hit status using `not_returned_in_raw_hits` wording instead of implying stronger retrieval causes
- Raw retrieval anomalies that fail to rejoin `passed_jobs` are exposed diagnostically instead of silently entering the scoring shortlist

### 1.9.0 — active

- Shortlist stage artifacts now separate raw vector row counts from unique-job raw-hit counts
- Shortlist debug payloads now describe observed raw-hit status using `not_returned_in_raw_hits` wording instead of implying stronger retrieval causes
- Raw retrieval anomalies that fail to rejoin `passed_jobs` are exposed diagnostically instead of silently entering the scoring shortlist

### 1.8.1 — active

- Newly generated stage-transition artifacts now report `schema_version: "stage_transition_artifacts_v3"` so the six-feature ranking artifact shape is version-detectable
- Specs/plans: see `refs` in the feature contract

### 1.8.0 — active

- Ranking-stage artifacts now expose the full six-feature ranking contract used by a run, including configured weights, missing-value defaults, zero-weight features, and contributing features
- Ranking `inputs_sample`, `outputs_sample`, and scored-not-ranked samples now carry all six ranking feature values plus `final_score`

### 1.7.0 — active

- Stage-transition artifacts now carry bounded input, output, and changed-state samples for each stage instead of summary-only handoff counts
- The richer artifact contract makes stage downloads more useful for debugging failed retrieval, ranking, filtering, and CV-generation transitions
- This rollout keeps `settings-used.json` separate and does not introduce `run-bundle.json`

### 1.6.0 — active

- Added a dedicated run-scoped `settings-used.json` download so effective run settings can be inspected without opening stage artifacts or internal snapshots
- Event timeline rows for recognized stage-boundary events can now download the corresponding stage-slice JSON directly
- This rollout stays download-only and does not add an in-page artifact viewer

### 1.6.0 — active

- Succeeded `Stage by Stage` runs now preserve reranker-blocked CV debug rows across the pause after `cv_analysis`, so final `results.json` and `cv-debug.json` match the already-correct `Run All` truth

### 1.5.0 — active

- Added a run-scoped stage-transition artifact download so major pipeline handoffs can be inspected without reconstructing them from later exports
- Inspection surfaces now explicitly distinguish stage-transition artifacts from the heavier CV-generation debug snapshot
- This rollout keeps the stage-transition artifact bounded and summary-first rather than duplicating full downstream payloads

### 1.4.1 — active

- Adopted the stage-aware doc system by mapping `inspection_debugging` to `primary_stage: cv_generation`
- Declared bounded stage participation across `enrich`, `rule_filter`, `shortlist`, `ranking`, and `cv_generation`
- This was a documentation-structure adoption only; no inspection runtime behavior changed by itself

### 1.4.0 — active

- Run detail can now show explicit decision-chain detail from run-results export instead of only a generic outcome badge
- CV-generation debug snapshots now separate authoritative ranking fit from secondary gap explanation so the decision path is easier to inspect

### 1.3.0 — active

- Run detail now exposes an admin-only `Download CV Debug JSON` action when a run-scoped CV-generation debug snapshot exists
- Completed runs can persist a bounded run-scoped CV-generation debug snapshot with live Layer 4 artifacts and failure-path details

### 1.2.0 — active

- Run detail results tab and large table usability improvements
- Specs/plans: see `refs` in the feature contract

### 1.1.0 — active

- Run input snapshot consistency
- Specs/plans: see `refs` in the feature contract

### 1.0.0 — active

- Initial feature: 3-tab inspection interface on run detail page
- Specs/plans: see `refs` in the feature contract

## Post-Execution Review

> Fill after status transitions to `active`.
