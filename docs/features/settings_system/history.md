# Settings System — History

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

### Phase 8 Settings And Performance Evidence Backfill Implementation Plan


Verification:
- `See plan body closeout verification notes.`

Outcome:
Completed the phase 8 settings and performance evidence backfill.

### Phase 6 Lineage Evidence Hydration Implementation Plan


Verification:
- `See plan body closeout verification notes.`

Outcome:
Completed the phase 6 lineage evidence hydration.

### Phase 10 Settings And Performance Residual Evidence Audit Implementation Plan


Verification:
- `See plan body closeout verification notes.`

Outcome:
Completed the phase 10 settings and performance residual evidence audit.

<!-- GENERATED HISTORY END -->

## Human Notes

## Changelog

### 2.13.0 — active

- `/admin/settings` now includes a dedicated `Agentic` task section for bounded future-run agentic defaults instead of leaving semantic and late-stage controls implicit or scattered across unrelated cards
- The agentic surface keeps advanced semantic-alignment tuning behind disclosure, renders fixed runtime metadata as non-editable explanation, and points operators back to run detail plus `settings-used.json` for historical truth
- Metadata-only settings can no longer be persisted through the legacy single-key save routes

### 2.12.0 — active

- The settings page now aligns card-level save boundaries with the actual retrieval and advanced-retrieval validation paths instead of sharing one broader retrieval submission seam
- Metadata-only controls no longer have to be posted back as hidden required inputs, and the page now explains future-run defaults, per-run overrides, baseline-default fallback, and `settings-used.json` historical truth more explicitly
- Advanced retrieval tuning now exposes the remaining supported required-skill and role semantic-alignment weights while reopening advanced disclosure blocks automatically on validation errors

### 2.11.0 — active

- The admin settings page is now organized around operator tasks (`Selection`, `Ranking`, `CV Output`, `Run Safety`, and `Advanced`) instead of presenting the schema as a flat registry dump
- Single-option pseudo-choice controls such as `cv_preset` and `cv_analysis.semantic_alignment.model` are now rendered as fixed runtime metadata instead of editable dropdowns
- The page now makes current-versus-draft state clearer, highlights unsaved edits, and renders CV section visibility in a denser matrix layout

### 2.10.0 — active

- Operator-facing `settings-used.json` exports now keep canonical nested settings in the primary surface and move compatibility-era flat keys into an explicit compatibility block when needed
- This keeps runtime compatibility intact while making the exported settings contract match the active admin UI more closely

### 2.9.0 — active

- The admin settings UI now exposes `run_lifecycle.max_runtime_minutes` as the server-owned timeout guard for unfinished runs
- This lifecycle setting lives outside the CV surface and keeps timeout ownership with the control plane instead of ad-hoc UI behavior

### 2.8.0 — active

- The admin settings UI no longer exposes metadata-only `cv_prompt_version` or legacy `cv_template_path` as if they were live runtime controls
- Active runtime consumers now prefer canonical nested retrieval settings (`pipeline.vector_search_top_n` and `pipeline.ai_score_top_n`) while retaining compatibility fallbacks where needed

### 2.7.0 — active

- The admin settings UI no longer exposes the three dormant CV content-rule toggles as if they were meaningful runtime switches
- `CV Maximum Pages` remains the only user-facing CV validation setting and continues to map to warning-only runtime behavior

### 2.6.0 — active

- Retrieval settings now expose `cv_analysis.semantic_alignment.*` controls, including enable/model settings and lexical-versus-semantic weights for responsibility and domain alignment
- Settings validation now rejects hybrid-weight pairs that do not sum to `1.0`
- Specs/plans: see `refs` in the feature contract

### 2.5.0 — active

- Ranking settings now include explicit `preference_fit_weights` for domain, role-family, and location-type calibration
- Ranking settings copy now reflects semantic role alignment for `title_relevance` and AI-reranker label calibration for fit thresholds
- Specs/plans: see `refs` in the feature contract

### 2.4.1 — active

- Ranking settings copy now matches runtime semantics for title-to-target-role similarity and domain/location preference alignment
- Specs/plans: see `refs` in the feature contract

### 2.4.0 — active

- Ranking settings now map to a real six-feature runtime contract instead of a hidden two-feature subset
- Supported ranking features can be made non-contributing explicitly with weight `0.0` while still remaining visible in runtime and artifacted config
- Specs/plans: see `refs` in the feature contract

### 2.3.0 — active

- Admin-editable CV generation and composition settings
- Specs/plans: see `refs` in the feature contract

### 2.0.0 — active

- Preset-based CV config migration
- Specs/plans: see `refs` in the feature contract

### 1.0.0 — active

- Initial feature: BigQuery-backed settings store and schema registry
- Specs/plans: see `refs` in the feature contract

## Post-Execution Review

> Fill after status transitions to `active`.
