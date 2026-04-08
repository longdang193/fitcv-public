# Pipeline Performance — History

## Changelog

### 1.11.0 — active

- `cv_analysis` reuse metrics now report executed-analysis rows, reused executed rows, fresh executed rows, and reranker-blocked rows separately so performance debugging does not confuse blocked-ranked rows with analyzed work

### 1.10.0 — active

- The reranker short-circuit path now stays visible in operator-facing coverage accounting, so ranked jobs blocked before analysis are no longer dropped from CV-debug non-attempted totals

### 1.9.0 — active

- Reranker `skip` jobs now short-circuit before evidence retrieval, gap computation, and semantic alignment inside `cv_analysis`
- This reduces late-stage work on ranked jobs that were already ineligible for CV generation according to the authoritative reranker fit label

### 1.8.0 — active

- `results.json` now behaves more like a true compact job ledger by dropping full job snapshots, bulky score-explanation internals, and full CV bodies that were already duplicated elsewhere
- This keeps stage-owned diagnostics in `stage-artifacts.json` while reducing operator-facing export weight for each job row

### 1.7.0 — active

- `cv_analysis` now gives bounded semantic support to required-skill and role channels instead of limiting semantic lift to domain and responsibility alignment
- The new channel-level weight pairs keep runtime behavior inspectable while improving semantic help on the highest-value fit signals

### 1.6.0 — active

- Operator-facing enriched-job exports now keep canonical semantic fields and reuse/fingerprint provenance while dropping retired raw duplicate classification baggage
- Large runs now avoid some row-scaled Layer 4 skip and validation-event noise by leaning on aggregate stage summaries and stage-owned artifacts instead
- This keeps the runtime behavior unchanged while trimming dead-weight export and timeline volume

### 1.5.0 — active

- Shortlist now computes a stable structured embedding-input signature before generating `job_summary` vectors
- Shortlist reuses the latest stored embedding row for a `job_url` only when both the structured signature and embedding contract fingerprint match
- Shortlist diagnostics now report fresh-vs-reused embedding counts alongside retrieval facts

### 1.4.0 — active

- Enrich now computes a stable raw-job fingerprint from normalized pre-enrichment inputs and can reuse shared `structured_jobs` rows when the fingerprint matches
- Reuse is additionally gated by an enrich-contract fingerprint so prompt, model, schema, or post-processing drift invalidates cached enrich output automatically
- Enrich-stage artifacts and run-scoped enriched exports now expose fresh-vs-reused provenance plus raw-job and enrich-contract fingerprint fields for debugging

### 1.3.0 — active

- Enrich extraction prompt text is now loaded from a centralized prompt registry instead of a large inline builder string
- Runtime config validates the effective enrich prompt ID and exposes prompt runtime metadata for downstream inspection
- Enrich-stage inspection can report prompt provenance without changing stage business logic or response-schema ownership

### 1.2.0 — active

- Enrich now emits raw-plus-canonical companions for repeatedly interpreted semantic fields
- Required/preferred skills now include canonical companion lists plus entity payloads
- Run-scoped enriched rows can carry reviewable mapping suggestions without mutating the trusted synonym map

### 1.1.0 — active

- Gemini structured output with Pydantic fallback
- Specs/plans: see `refs` in the feature contract

### 1.0.0 — active

- Initial feature: pre-enrichment global job filters
- Specs/plans: see `refs` in the feature contract

## Post-Execution Review

> Fill after status transitions to `active`.
