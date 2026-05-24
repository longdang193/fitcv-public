---
doc_id: pipeline
doc_type: architecture-guide
explains:
  stages:
    - cv_analysis
    - cv_generation
    - enrich
    - normalize
    - ranking
    - rule_filter
    - shortlist
---

# Pipeline

Stage order:

`normalize -> enrich -> rule_filter -> shortlist -> ranking -> cv_analysis -> cv_generation`

This page is a cross-cutting summary of runtime behavior and ownership.

Input jobs contract and normalization: [job-data-input.md](job-data-input.md).

## Stage Responsibilities

- `normalize`: canonicalize incoming jobs
- `enrich`: derive structured job fields and reuse-aware metadata
- `rule_filter`: deterministic gating before expensive steps
- `shortlist`: vector/retrieval candidate narrowing
- `ranking`: authoritative fit scoring and decision labels
- `cv_analysis`: evidence selection and generation readiness
- `cv_generation`: structured generation, validation, repair, persistence

## Execution Modes

- full run (`Run All`)
- checkpointed run (`Stage by Stage`)

Mode changes pacing, not stage truth semantics.

## Contracts and Evidence

- stage outcomes are stage-owned truth
- operator summaries are derived views
- run artifacts/events must remain consistent with stage-owned outcomes
- `StageResult`/trace fields and run exports are the audit surface

## Two-Layer Observability Ownership

Observability separates run-level and item-level surfaces:

- **run-summary layer**
  - run-level events and summaries remain operator entrypoint surfaces
  - aggregate completion/debug surfaces describe run-wide behavior
- **item-observation layer**
  - item-level analysis/generation traces capture one candidate-job attempt at a time
  - item observations carry reviewer-facing input/output plus structured metadata for filtering

Ownership rule:

- run-summary surfaces answer **how run behaved overall**
- item observations answer **what happened for one candidate-job attempt**
- avoid duplicating full item raw IO into aggregate run-summary payloads

## Portability Expectations

- sqlite and bigquery backends must preserve the same operator-visible contracts
- provider/model routing must be config/env controlled, not hardcoded

## Symmetry and Invariance Rules

- AI-stage decisions are backend-invariant: the same input must resolve the same routed AI provider/model regardless of `sqlite` vs `bigquery`.
- Backend differences are persistence-only: storage schema/adapter metadata may differ, but AI decision logic and provenance semantics must remain equivalent.
- The runtime must treat `control_plane.model_routing.parts.*` as authoritative for AI stage provider/model selection.
- Non-agentic legacy mode fields must not override unified AI routing authority.

## AI Credential and Error Contract

- Canonical AI credential key: `FITCV_LLM_API_KEY`.
- Temporary aliases may be accepted only during bounded deprecation windows: `OPENAI_API_KEY`, `OPENAI_COMPATIBLE_API_KEY`.
- `service_account_key` remains data-plane-only for BigQuery auth, not AI invocation auth.

Fail-fast guarantees:

- missing routed AI model/provider -> explicit runtime configuration failure
- missing AI API key for routed provider -> explicit runtime credential failure
- no hidden fallback to legacy Gemini defaults in unified runtime path

## Related Docs

- [architecture.md](architecture.md)
- [usage.md](usage.md)
- [FitCV-pipeline.md](FitCV-pipeline.md)
