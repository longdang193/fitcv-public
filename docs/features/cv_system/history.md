# CV System — History

## Changelog

### 1.33.0 — active

- `cv_generation` now tells the writer to use the real profile name in the header and performs one deterministic header-only repair when validation fails solely because of a candidate-name placeholder
- The repair path rewrites only the candidate identity fields, rerenders markdown, reruns validation once, and keeps debug artifacts explicit about initial failure versus repaired acceptance

### 1.32.0 — active

- Run-scoped CV artifacts now export explicit contract-era versions and `run_mode` metadata, so final-stage bundles can be audited without relying on control-plane context
- `cv_analysis` reuse diagnostics now separate executed analysis rows from reranker-blocked rows, which keeps late-stage reuse math aligned with the pre-analysis short-circuit path
- `cv_generation` validation now rejects plain `Candidate Name` placeholders in structured headers and rendered markdown, instead of only catching bracketed forms such as `[Candidate Name]`

### 1.31.0 — active

- Reranker-blocked ranked jobs now stay explicit in compact exports and debug coverage summaries, so the final-stage artifact layer no longer understates the pre-analysis short-circuit as a generic not-run outcome

### 1.30.0 — active

- Ranked jobs with authoritative reranker `fit_label = skip` now stop before evidence retrieval and gap computation inside `cv_analysis`, instead of paying full late-stage analysis cost
- The final-stage contract now distinguishes `blocked_by_reranker_fit` from `skipped_fit_gate`, so operator views and artifacts can tell apart "blocked before analysis" versus "analyzed and then skipped"

### 1.29.0 — active

- Compact `results.json` rows now keep skipped-fit-gate semantics aligned with the final-stage split: `cv_analysis` is treated as completed with a gate decision, while `cv_generation` remains explicitly unattempted

### 1.28.0 — active

- `cv_analysis` semantic alignment now covers `required_skill_support` and `role_alignment` in addition to domain and responsibility channels
- Required-skill and role semantic lift stay bounded through dedicated lexical/semantic weight pairs, and stage artifacts now report the expanded channel-level semantic method coverage explicitly

### 1.27.0 — active

- `cv_analysis` stage artifacts no longer expose stage-level fresh/reused embedding totals derived from summed cumulative per-record cache snapshots
- Record-local evidence-selection embedding counts remain available for deep investigation without pretending to be run-wide totals

### 1.26.0 — active

- Final run exports now keep `results.json` focused on the per-job outcome ledger, while stage-owned diagnostics remain in `stage-artifacts.json` and per-stage artifacts
- Late-stage reuse snapshots remain available for exact-match reuse lookup, but they now live under an internal diagnostic-support block instead of the main operator-facing run-results surface

### 1.25.0 — active

- `cv_generation` validation now rejects unresolved candidate-name placeholders such as `[Candidate Name]` instead of accepting them as valid output
- CV-generation diagnostics now report the active structured prompt identity directly, rather than relying on `cv_prompt_version` as the only provenance breadcrumb

### 1.24.0 — active

- `cv_prompt_version` is no longer presented as an operator-facing CV setting; active prompt selection remains owned by `config/runtime/prompts.yaml` and the prompt registry
- Legacy `cv_template_path` remains internal fallback-only instead of being treated as an active admin-editable CV control

### 1.23.0 — active

- The dormant CV content-rule toggles (`Emphasize Required Skills`, `Align JD Terminology`, and `Evidence Grounded Only`) were removed from the active pipeline contract and admin settings surface
- `CV Maximum Pages` remains as the sole user-facing CV validation control, and page overflow continues to be treated as a warning instead of a hard failure

### 1.22.0 — active

- `cv_generation.structured_write.v1` is now the sole active `cv_generation` runtime prompt contract in config and runtime provenance
- The dormant markdown writer prompt is no longer part of the active pipeline contract, which makes prompt config and debugging output match the real structured-first execution path

### 1.21.0 — active

- `ranking` can now reuse exact-match AI-score rows per job when the stage-owned AI-score fingerprint and reranker contract still match
- `cv_analysis` can now reuse exact-match analysis records per ranked job when the stage-owned analysis fingerprint and evidence-selection contract still match
- Run results now persist late-stage reuse snapshots and late-stage reuse-rate metrics so repeated late-stage runs can skip unchanged expensive work safely

### 1.20.0 — active

- Run summaries and stage-transition artifacts now expose stage-level quality metrics so shortlist, ranking, `cv_analysis`, and `cv_generation` bottlenecks are visible without job-by-job inspection
- Shortlist, ranking, `cv_analysis`, and `cv_generation` each now own one explicit quality-metric block instead of relying on ad hoc downstream interpretation

### 1.19.0 — active

- `cv_analysis` now scores domain and responsibility alignment with hybrid lexical-plus-semantic matching instead of relying only on lexical overlap
- Semantic embedding generation and reuse are now stage-owned by `cv_analysis`, while final evidence selection stays bounded and coverage-aware
- Shortlist now reuses the single candidate-query embedding used for vector retrieval when the bounded query signature and candidate-query embedding contract still match, and the active shortlist runtime no longer generates unused candidate chunk embeddings

### 1.18.0 — active

- Ranking can now infer fallback `target_role`, `role_families`, and `domains` from recent candidate evidence when explicit YAML preferences are sparse, while keeping explicit preferences authoritative
- Shared role normalization moved into central taxonomy config so ranking and inference no longer rely on private alias tables
- `cv_generation` validation now treats the selected `cv_analysis` evidence bundle as the primary grounding surface for hard facts such as employers, projects, and skills when selected evidence is available
- Softer role, domain, and responsibility claims now use a bounded hybrid support path that combines selected-theme/tag checks with compact selected-evidence text matching
- Prompt guidance now explicitly tells CV generation to stay inside the selected evidence bundle for job-specific responsibility, domain, and role-positioning claims

### 1.17.0 — active

- Shortlist candidate-query construction now uses `flatten_skills(profile)` instead of relying only on explicit root skills
- Shortlist retrieval can now see bounded inferred role-family and domain hints from the broader candidate evidence surface
- This improves shortlist recall while keeping the single-query retrieval model deterministic and bounded

### 1.16.0 — active

- `cv_analysis` evidence retrieval now retrieves separate candidate pools for required-skill support, role alignment, domain alignment, and responsibility alignment before merging and deduping them by stable `evidence_id`
- Final evidence selection is now one bounded per-job bundle with explicit selected-evidence IDs, matched-channel metadata, and selection reasons that feed `cv_generation`
- Candidate YAML can now include additive role-family, domain-tag, and responsibility-theme metadata to improve analysis quality without breaking existing profiles

### 1.15.0 — active

- The final Layer 4 flow is now split into `cv_analysis` and `cv_generation`
- `cv_analysis` owns ranked-job context merge, evidence retrieval, gap analysis, and fit-gate preparation before CV writing
- `cv_generation` now consumes persisted analysis outputs for writing, validation, repair, and persistence instead of recomputing that analysis by default

### 1.14.0 — active

- Ranking now uses a stricter reranker rubric that makes required-skill evidence, readiness, and role alignment primary while keeping preference signals secondary
- `title_relevance` now reflects bounded semantic role alignment instead of pure token overlap, and `preference_fit` now combines weighted domain, role-family, and location-type alignment
- This rollout improves ranking quality before CV generation without changing `must_have_match` semantics or CV-generation authority

### 1.13.0 — active

- Shortlist can now skip repeated `job_summary` embedding work for unchanged passed jobs when the structured signature and embedding contract still match
- Latest-only retrieval semantics remain unchanged, so reused and fresh vectors still compete as one active row per `job_url`
- This rollout optimizes shortlist embedding cost only; it does not change reranking or CV-generation authority

### 1.12.0 — active

- Shortlist retrieval now ranks only the latest active persistent `job_summary` embedding row per canonical `job_url`, reducing duplicate-row competition from historical embeddings
- Shortlist artifact and export surfaces now expose clearer retrieval facts such as raw unique-job counts, explicit backfill outcomes, and shortlist-row retrieval flags
- This rollout keeps persistent embeddings and backfill in place; it does not move shortlist to a fully run-scoped embedding store

### 1.11.1 — active

- Ranking can now prefer enrich-stage canonical required-skill companions when computing must-have overlap
- This rollout keeps CV-generation authority unchanged while reducing repeated raw-skill reinterpretation upstream of CV composition

### 1.11.0 — active

- Stage-transition artifacts now preserve bounded input, output, and changed-state context for `normalize`, `enrich`, `rule_filter`, `shortlist`, `ranking`, and `cv_generation`
- `cv_generation` remains the richest stage block, but this rollout keeps the separate `cv-debug.json` surface for compatibility
- The settings snapshot remains separate and is still not duplicated wholesale into every stage block

### 1.10.0 — active

- Effective run settings can now be exported once as a dedicated `settings-used.json` artifact instead of being duplicated into every stage block
- Stage-boundary artifact inspection can now be downloaded as per-stage JSON slices without changing the runtime stage contracts
- This rollout adds inspection surfaces only; it does not change ranking, CV generation, or validation authority

### 1.9.0 — active

- Run summaries now emit bounded stage-transition artifact blocks for `normalize`, `enrich`, `rule_filter`, `shortlist`, `ranking`, and `cv_generation`
- The new stage-transition artifact reuses existing runtime seams and keeps `cv_generation` summarized rather than duplicating the full CV debug payload
- This rollout adds stage-boundary inspection only; it does not change the authoritative CV generation or validation decisions

### 1.8.1 — active

- Adopted the stage-aware doc system by mapping `cv_system` to `primary_stage: cv_generation`
- Declared bounded stage participation across `enrich`, `ranking`, and `cv_generation`
- This was a documentation-structure adoption only; no runtime CV behavior changed by itself

### 1.8.0 — active

- Phase 1 ranking now uses only the runtime-computed `ai_score` and `vector_similarity` contract instead of implying inactive weighted features
- Reranker fit is now the sole post-filter authority for ranking-time fit and CV-generation eligibility; gap analysis remains explanatory support only
- Structured normalization and markdown validation now share one config-driven required-section contract, with bounded completeness checks for enabled required sections

### 1.7.0 — active

- Ranked jobs can now capture a bounded live CV-generation debug record containing initial structured output, initial validation state, repair metadata, and final accepted artifact when available
- Persistence failures during CV version storage can now be inspected through a run-scoped debug snapshot instead of relying only on logs

### 1.6.0 — active

- Experience composition now uses JD-sensitive role-level bullet selection instead of reusing the same narrow slice across jobs
- Prompt construction can attach bounded secondary supporting evidence to grouped role blocks, with achievements preferred and support kept explicitly secondary
- Experience prompt semantics now ask for grounded re-emphasis and synthesis instead of mere bullet restatement

### 1.5.0 — active

- Project evidence is now preserved as grouped `project_entry` blocks instead of only thin `name + skills` snippets
- Project prompt construction now carries richer grounded context such as duration, business value, selected stack lines, and selected highlights when present
- Sparse projects still degrade gracefully without invented impact language
- Thin project evidence remains fallback/supporting only rather than the primary Projects construction path

### 1.5.0 — active

- Reranker-blocked ranked jobs now keep their non-attempted CV-generation truth through succeeded `Stage by Stage` finalization instead of degrading to generic `ranked_no_cv` semantics

### 1.4.0 — active

- CV evidence retrieval is now section-aware rather than one flat mixed top-k pool
- Experience evidence is preserved as grouped role/company/date entries with bounded bullets
- Prompt construction now passes grouped work-history blocks into CV generation instead of flattening experience back into loose snippets

### 1.3.0 — active

- CV generation now creates a schema-versioned structured CV document before rendering markdown
- `cv_versions` now persists structured CV JSON plus generation metadata alongside markdown
- Run-scoped exports can include structured CV content and CV generation metadata for new rows

### 1.2.0 — active

- Admin-editable CV generation settings via settings UI
- Specs/plans: see `refs` in the feature contract

### 1.1.0 — active

- Preset-based CV composition configuration
- Specs/plans: see `refs` in the feature contract

### 1.0.0 — active

- Initial feature: preset registry and CV composition model
- Specs/plans: see `refs` in the feature contract

## Post-Execution Review

> Fill after status transitions to `active`.
