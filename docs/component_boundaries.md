---
doc_id: component_boundaries
doc_type: architecture-contract
explains:
  features:
    - trigger_run_management
    - inspection_debugging
    - run_lifecycle_controls
---

# Component Boundaries

This document defines component ownership and dependency direction for current FitCV runtime architecture.

## Source Of Truth Per Concern

- flow = orchestrator
- traces = OTel-compatible IDs and telemetry transport
- decisions = policy layer
- evidence = stage artifacts
- operations = control plane UI/API

## Components

### 1) Orchestration

Owns:

- run lifecycle state transitions
- stage sequencing and retry/cancel/checkpoint flow
- continue/resume semantics

Primary surfaces:

- `src/fitcv_cp/worker_job.py`
- future orchestrator adapter (`default runtime`, `Prefect`)

### 2) Evidence Contract

Owns:

- canonical stage result envelope:
  - `StageResult = { output, evidence, validation, decision, policy_version, trace_context }`
- stage-owned artifact truth rules

Primary surfaces:

- `src/fitcv/pipeline.py`
- stage artifact payloads and contract tests

### 3) Telemetry

Owns:

- trace context continuity (`trace_id`, `span_id`, `parent_span_id`)
- event emission reliability and replayability
- exporter/collector integration behavior (when enabled)

Primary surfaces:

- `src/fitcv_cp/reporter.py`
- `src/fitcv_cp/bq_store.py` event persistence path
- run-detail telemetry degradation surfaces

### 4) Policy Engine

Owns:

- deterministic acceptance/rejection decision rules
- policy version linkage for decisions and replay
- replay modes (`strict`, `policy_replay`)

Primary surfaces:

- `src/fitcv/pipeline.py`
- validator/gate modules and replay metadata

### 5) Data Plane

Owns:

- run metadata/state store
- artifact store
- event store (including retry/dead-letter durability)

Primary surfaces:

- `src/fitcv_cp/bq_store.py`
- adapter boundaries for BigQuery-now and Postgres/object-storage-later

### 6) AI Runtime

Owns:

- model/tool invocation for agentic seams
- agentic reasoning outputs passed as stage evidence

Primary surfaces:

- `src/fitcv/` agentic stage code paths (`cv_analysis`, `cv_generation`, synonym-assist seams)

### 7) Control Plane API/UI

Owns:

- operator actions, run inspection, and exports
- operational diagnostics and safety controls
- no authority to redefine stage semantics or policy decisions

Primary surfaces:

- `src/fitcv_cp/app.py`
- `src/fitcv_cp/templates/`

## Dependency Direction

Allowed (high-level):

1. `control_plane` -> `orchestration` + `data_plane` + read-only `evidence`/`telemetry` surfaces
2. `orchestration` -> `pipeline stages` + `policy` + `telemetry` + `data_plane`
3. `pipeline stages` -> `evidence contract` + `policy` + `ai runtime` + `telemetry context`
4. `telemetry` -> `data_plane` only for persistence transport (not policy authority)

Forbidden:

1. `control_plane` directly deciding acceptance outcomes.
2. `telemetry` redefining decisions or evidence truth.
3. `ai_runtime` bypassing policy/validation gates.
4. `data_plane` owning flow semantics.

## Current Module Mapping

- `src/fitcv_cp/app.py` -> control_plane
- `src/fitcv_cp/worker_job.py` -> orchestration
- `src/fitcv_cp/reporter.py` -> telemetry adapter
- `src/fitcv_cp/bq_store.py` -> data_plane adapter
- `src/fitcv/pipeline.py` -> evidence contract + policy + ai runtime integration
