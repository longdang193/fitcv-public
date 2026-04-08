# Stage Overview

> Generated — do not edit manually. Source: `docs/stages/*.yaml`

| Stage | Depends On | Primary Features | Summary |
|---|---|---|---|
| `cv_analysis` | `ranking` | `cv_system` | Prepare ranked jobs for writing by merging enriched job context, retrieving candidate evidence from the normalized profile through separate required-skill, role, domain, and responsibility channels, scoring domain and responsibility with hybrid lexical-plus-semantic alignment, selecting one bounded coverage-aware evidence bundle, computing grounded gap summaries, reusing exact-match analysis records when the stage-owned fingerprint and contract still match, resolving the final fit gate that decides whether CV generation should run and what evidence bundle downstream validation is allowed to rely on, and emitting stage-owned quality and reuse metrics. |
| `cv_generation` | `cv_analysis` | `cv_system` | Convert generation-ready CV-analysis records into accepted or rejected CV artifacts through structured prompt-driven writing, hybrid analysis-grounded validation, repair, and persistence without recomputing analysis or reselecting evidence by default, while emitting stage-owned accepted and validation-failure quality metrics for bottleneck diagnosis. |
| `enrich` | `normalize` | `cv_system` | Filter normalized jobs through pre-enrichment global checks, then enrich surviving jobs into a shared downstream contract where required/preferred skills keep raw phrases, canonical skill entities, and reuse provenance when unchanged jobs can safely skip a fresh LLM call. |
| `normalize` | — | `trigger_run_management` | Normalize raw run inputs into a stable job list, apply deduplication, and hand the surviving normalized rows to downstream filtering and enrichment. |
| `ranking` | `shortlist` | `cv_system` | Score shortlist candidates with the six-feature ranking contract, stricter reranker rubric, semantic role alignment, weighted preference alignment, and deterministic fallback candidate-intent inference; reuse exact-match AI-score rows when the stage-owned fingerprint and contract still match; assign the authoritative post-filter ranking fit; and select the ranked jobs eligible to proceed toward CV generation with versioned stage-transition artifacts plus stage-owned quality and reuse metrics. |
| `rule_filter` | `enrich` | `trigger_run_management` | Apply deterministic eligibility checks to enriched jobs and split them into passed and rejected sets before retrieval and ranking begin. |
| `shortlist` | `rule_filter` | `inspection_debugging` | Turn passed jobs into a retrieval-aware scoring shortlist by building a richer bounded candidate query from the full profile evidence surface, reusing the single shortlist candidate-query embedding and unchanged job-summary embeddings when safe, searching only the latest active embedding row per canonical job URL with the deterministic candidate query vector actually used for retrieval, then capturing raw hits, shortlist transitions, any backfill needed before scoring, and the stage-owned shortlist backfill-rate quality metric. |

## Stage Contracts

Each stage has a contract at `docs/stages/<stage_id>.yaml`:

```text
docs/stages/<stage_id>.yaml
```

For the machine-friendly index, see `docs/generated/stages_index.yaml`.
