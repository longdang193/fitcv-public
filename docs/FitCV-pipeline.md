# FitCV Pipeline

## Purpose

FitCV turns noisy raw job postings into a smaller set of high-confidence opportunities and grounded CV outputs.

The pipeline is designed to do three things well:

- understand jobs through structured enrichment
- narrow the candidate set before expensive work
- generate CV outputs that are inspectable, evidence-grounded, and operationally traceable

This document describes the current pipeline architecture, not the historical design notebook that led to it.

## End-to-End Flow

```text
Input jobs
  -> normalize
  -> enrich
  -> rule_filter
  -> shortlist
  -> ranking
  -> cv_analysis
  -> cv_generation
  -> persisted outputs and run artifacts
```

The control plane wraps that flow with:

- run creation and queueing
- immutable settings snapshots
- run-scoped artifact persistence
- run inspection and lifecycle actions

## Who The Pipeline Serves

The pipeline is meant for an operator who needs to:

- process many jobs in one run
- inspect why jobs passed or failed
- trust the difference between weak and strong matches
- generate CVs only when the evidence is good enough
- debug and tune the workflow without guesswork

## Stage Responsibilities

### 1. `normalize`

Purpose:

- clean incoming job inputs
- standardize fields
- deduplicate repeated postings
- produce a stable run-scoped job list

Why it matters:

- downstream stages should never have to reason about raw input noise directly

### 2. `enrich`

Purpose:

- extract structured job fields such as title, seniority, domain, location type, skills, and responsibilities
- convert raw postings into a stable downstream contract
- reuse prior enrich output safely when unchanged jobs and unchanged enrich contracts still match

Why it matters:

- later filtering, retrieval, ranking, and CV generation depend on structured job understanding, not raw text alone

### 3. `rule_filter`

Purpose:

- apply deterministic eligibility checks before expensive retrieval and ranking work
- separate clearly rejected jobs from jobs that may proceed

Current role:

- this is the first strong cost-control boundary in the pipeline
- it keeps obvious mismatches out of later LLM- and retrieval-backed stages

### 4. `shortlist`

Purpose:

- retrieve the most plausible jobs from the passed set
- build a bounded candidate query from the candidate profile
- reuse shortlist inputs when the deterministic contract still matches

Why it matters:

- retrieval is used for recall, not final judgment
- the goal is to reduce the passed set to a smaller, plausible ranking set

### 5. `ranking`

Purpose:

- apply the authoritative post-filter fit judgment
- combine stricter fit logic with shortlist outputs
- select the ranked jobs that are still eligible to continue

Important current behavior:

- ranking is the stage that decides the authoritative reranker fit label
- jobs marked `skip` here are no longer allowed to consume expensive downstream CV-analysis work

### 6. `cv_analysis`

Purpose:

- retrieve candidate evidence for the strongest ranked jobs
- evaluate required-skill, role, domain, and responsibility support
- compute grounded gap summaries
- decide whether a ranked job is generation-ready

Important current behavior:

- reranker `skip` jobs are short-circuited before expensive evidence retrieval
- only jobs that survive ranking fit are analyzed deeply
- analyzed jobs are explicitly distinguished from jobs blocked before analysis

### 7. `cv_generation`

Purpose:

- generate structured and markdown CV outputs
- validate them against selected evidence
- repair narrowly safe failures when deterministic correction is possible
- persist accepted outputs and debug artifacts

Important current behavior:

- generation is constrained by analysis-selected evidence
- validation is part of the pipeline contract, not an optional afterthought

## Pipeline Safeguards

### Deterministic narrowing before expensive work

The system narrows jobs in multiple layers:

- normalization and deduplication
- deterministic rule filtering
- shortlist retrieval
- ranking fit authority
- CV-analysis fit readiness

That prevents the most expensive stages from becoming a cleanup layer for obviously weak candidates.

### Reranker short-circuit before expensive CV analysis

One of the biggest delivered improvements is that reranker `skip` jobs are now stopped before expensive CV-analysis evidence retrieval.

That means:

- ranking remains the authoritative fit decision after filtering
- weak ranked jobs do not consume unnecessary analysis work
- artifacts still record those jobs truthfully as reranker-blocked rather than collapsing them into vague not-run states

### Grounded generation and validation

CV generation is not treated as free-form writing. It is bounded by:

- selected evidence bundles
- fit analysis outputs
- validation rules
- deterministic repair only for a narrow class of low-risk failures

This keeps the output more trustworthy and easier to debug.

## Control-Plane Integration

The control plane is not separate from the pipeline story. It is how the system becomes operable.

The control plane provides:

- trigger surfaces for job inputs
- settings snapshots at run start
- persisted run records and event timelines
- stage-local artifact downloads
- compact results ledgers and deeper stage diagnostics
- lifecycle controls for active and completed runs

Without that layer, the pipeline would still exist, but it would be much harder to operate, inspect, and tune.

## Execution Modes

The same stage order supports two execution policies:

- `Run All`
- `Stage by Stage`

### `Run All`

- executes continuously through the stage order
- best for normal batch operation

### `Stage by Stage`

- pauses after each major stage
- persists checkpoint state between stages
- allows stage-local review before continuing

Why this matters:

- it turns the pipeline into a debuggable operator workflow, not just a background batch job

## Artifact Model

The pipeline now exposes a clearer artifact contract than earlier versions.

### Compact ledger

`results.json` is the compact per-job ledger:

- high-level job outcome
- fit path
- CV status
- concise decision-chain facts

### Stage diagnostics

Heavy stage detail lives in:

- `stage-artifacts.json`
- per-stage artifact files
- `cv-debug.json`

This separation matters because it keeps the main results export usable while still preserving deep debugging detail.

### Truthful outcome distinctions

A major debugging cleanup in the project was making artifact truth explicit about late-stage outcomes, especially the difference between:

- reranker-blocked rows
- analyzed-and-skipped rows
- generation-attempted rows
- accepted vs rejected generation outputs

That work makes inspection much more trustworthy.

## Biggest Engineering Improvements Delivered

### 1. Better cost control before late stages

- deterministic rule filtering ahead of expensive work
- reranker short-circuiting before CV-analysis evidence retrieval

### 2. Better observability

- stage-local artifacts
- run health and stage diagnostics
- exportable artifact bundles
- compact results ledgers plus deeper debug artifacts

### 3. Better artifact truth

- clearer status semantics across ranking, analysis, and generation
- run-mode-aware exported artifacts
- explicit handling for reranker-blocked rows and analyzed outcomes

### 4. Better reuse and performance

- enrich reuse
- shortlist input reuse
- ranking reuse
- CV-analysis reuse
- execution-aware reuse diagnostics

### 5. Better generation safety

- validation against selected evidence
- deterministic repair for specific safe failures
- reduced risk of low-quality or misleading accepted outputs

## Mental Model

The cleanest way to think about FitCV is:

### Layer 1: understand the job

- normalize
- enrich

### Layer 2: narrow the candidate set

- rule_filter
- shortlist
- ranking

### Layer 3: personalize safely

- cv_analysis
- cv_generation
- validation and repair

### Layer 4: operate and inspect

- trigger runs
- inspect stage outputs
- download artifacts
- tune settings
- manage lifecycle actions

That is the main value of the system: not just generating a CV, but doing it through a staged, inspectable, and increasingly reliable pipeline.

