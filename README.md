# FitCV

> A job-matching and CV-generation system with an operator-facing control plane for running, inspecting, and tuning the pipeline.

## Who Uses It

FitCV is built for a person or team that wants to turn a large set of raw job postings into a smaller set of high-confidence applications, with grounded CV output and strong operational visibility.

Typical users are:

- an operator running and reviewing application batches
- an engineer building or maintaining the pipeline
- a workflow owner who needs inspection, traceability, and repeatable CV generation

## Problem

The underlying workflow is messy if handled manually:

- raw jobs arrive in inconsistent formats
- job relevance is hard to judge at scale
- generic CV rewriting is difficult to trust
- pipeline runs are hard to operate without good inspection and control surfaces

FitCV addresses that by combining structured enrichment, deterministic filtering, retrieval and ranking, grounded CV generation, and an admin control plane that makes the whole system observable and tunable.

## Solution

FitCV processes jobs in a staged pipeline:

1. normalize incoming job inputs into a stable schema
2. enrich jobs into structured records with skills, seniority, domain, and role context
3. apply deterministic rule filtering before expensive retrieval and ranking
4. shortlist plausible jobs through retrieval
5. rank shortlisted jobs with stricter fit logic
6. analyze only the best candidates for grounded CV evidence
7. generate validated CV outputs with repair safeguards

The admin control plane then lets operators trigger runs, inspect stage outputs, download artifacts, adjust settings, and manage run lifecycle actions without terminal-only workflows.

## Key Pipeline Stages

- `normalize`
  - cleans and deduplicates raw job inputs into a stable run-scoped job list
- `enrich`
  - extracts structured job fields and supports safe reuse when unchanged jobs already have valid enrich output
- `rule_filter`
  - removes deterministic mismatches before expensive ranking work begins
- `shortlist`
  - retrieves the most plausible jobs with bounded, reuse-aware retrieval inputs
- `ranking`
  - applies the authoritative post-filter fit decision and selects which jobs can move toward CV generation
- `cv_analysis`
  - retrieves candidate evidence, computes grounded gap summaries, and decides whether a ranked job is generation-ready
- `cv_generation`
  - writes, validates, repairs when safe, and persists final CV outputs

## Major Control-Plane Features

- Trigger runs from uploaded files, pasted JSON, or path-based inputs
- Run in either `Run All` or `Stage by Stage` mode
- Inspect run progress, stage artifacts, and per-job outcomes
- Download bundled run artifacts and stage-owned diagnostics
- Manage editable pipeline settings through the UI
- Pause, continue, archive, and cancel runs through lifecycle controls

## Engineering Highlights

The most important system work in this repo is not just “generate CVs.” It is the surrounding reliability, diagnostics, and performance design:

- **Stage-aware architecture**
  - the pipeline is split into explicit stages with clear boundaries and stage-local artifacts
- **Operator-facing inspection**
  - runs expose compact ledgers, stage diagnostics, timeline events, and downloadable artifact bundles
- **Reranker short-circuiting**
  - weak ranked jobs are blocked before expensive CV-analysis evidence retrieval
- **Artifact truth alignment**
  - reranker-blocked, skipped-fit-gate, and generated outcomes are kept explicit in exported artifacts
- **Performance and reuse work**
  - enrichment, shortlist retrieval inputs, ranking rows, and CV-analysis outputs all support bounded reuse where contracts still match
- **Generation safety**
  - generated CVs are validated against grounded evidence, and specific low-risk failures such as placeholder candidate names can be repaired deterministically

## Architecture

```text
Jobs JSON / upload / path input
  |
  v
FastAPI admin control plane
  |  trigger runs, snapshot settings, persist run state
  v
RQ worker + Redis
  |  execute pipeline stages
  v
Core FitCV pipeline
  |  normalize -> enrich -> rule_filter -> shortlist -> ranking -> cv_analysis -> cv_generation
  v
BigQuery
  |  run state, events, structured jobs, rule-filter results, CV versions
  v
Admin inspection surfaces and downloadable artifacts
```

Operational invariants:

- run records are inserted before queue enqueue
- effective settings are snapshotted at trigger time
- run inputs are treated as immutable once captured
- run-scoped artifacts remain tied to the run that produced them

## Main Components

### Control plane

- `src/fitcv_cp/`
  - FastAPI app
  - run lifecycle routes
  - settings management
  - BigQuery-backed inspection surfaces
  - worker integration

### Core pipeline

- `src/fitcv/`
  - normalization and enrichment
  - deterministic filtering
  - shortlist retrieval
  - ranking
  - CV analysis and generation
  - validation and repair safeguards

## Docs

- [FitCV-pipeline.md](/c:/Users/HOANG%20PHI%20LONG%20DANG/repos/JOB-PROJECT/docs/FitCV-pipeline.md)
  Current-state pipeline architecture, execution flow, and major engineering safeguards.
- [fitcv-control-plane-setup.md](/c:/Users/HOANG%20PHI%20LONG%20DANG/repos/JOB-PROJECT/docs/fitcv-control-plane-setup.md)
  Local setup, Docker usage, credentials, and troubleshooting.
- [stage_overview.md](/c:/Users/HOANG%20PHI%20LONG%20DANG/repos/JOB-PROJECT/docs/generated/stage_overview.md)
  Summary of active pipeline stage contracts.

## Source Layout

```text
src/fitcv_cp/     admin control plane
src/fitcv/        core pipeline
docs/features/    feature contracts and history
docs/stages/      stage contracts
docs/generated/   generated discovery docs
tests/            automated coverage
config/           runtime and policy configuration
assets/           SQL and supporting assets
```

## Getting Started

For setup and local execution, start with:

- [fitcv-control-plane-setup.md](/c:/Users/HOANG%20PHI%20LONG%20DANG/repos/JOB-PROJECT/docs/fitcv-control-plane-setup.md)

Typical Docker startup:

```powershell
docker compose up -d --build redis web worker
```

The admin UI is available at:

```text
http://localhost:8000/admin/runs
```
