# FitCV

> Job-matching and CV-generation pipeline with operator-facing control plane.

## Who Uses It

- **Operators** running application batches and reviewing outcomes in admin UI.
- **Engineers** maintaining pipeline logic, settings, and run infrastructure.
- **Workflow owners** needing repeatable, inspectable CV production with run-level evidence.

## Problem

Manual job-to-CV workflow breaks at scale:

- inconsistent raw job inputs
- hard-to-trust relevance decisions
- expensive downstream steps on low-quality candidates
- weak operational visibility without run/stage inspection surfaces

## Solution

FitCV uses staged pipeline + control plane:

- staged processing: normalize → enrich → rule_filter → shortlist → ranking → cv_analysis → cv_generation
- deterministic gates before expensive stages
- run-scoped artifacts and lifecycle controls
- admin surfaces for trigger, inspect, download, and settings updates

## Key Pipeline Stages

1. **normalize** — canonicalize and deduplicate input jobs.
2. **enrich** — derive structured fields (skills, role, level, domain signals).
3. **rule_filter** — apply deterministic exclusion rules.
4. **shortlist** — retrieve plausible candidates for deeper scoring.
5. **ranking** — compute fit decisions and promote best jobs.
6. **cv_analysis** — gather grounded evidence and readiness signals.
7. **cv_generation** — generate CV output with validation/repair safeguards.

See deep stage behavior in [docs/FitCV-pipeline.md](docs/FitCV-pipeline.md) and [docs/pipeline.md](docs/pipeline.md).

## Major Features and Engineering Highlights

- **Control-plane run operations**: trigger runs, inspect stages/items, stop/archive lifecycle actions.
- **Settings-driven execution**: persistent settings applied through control-plane settings store.
- **Artifact-backed observability**: run/item diagnostics and downloadable outputs.
- **Reuse/performance safeguards**: bounded reuse in selected stages to reduce redundant work.
- **Generation safety**: validation and deterministic repair path for low-risk output defects.

Related docs:

- [docs/api.md](docs/api.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/component_boundaries.md](docs/component_boundaries.md)
- [docs/configuration.md](docs/configuration.md)
- [docs/observability.md](docs/observability.md)

## Architecture

```text
Inputs (file/path/json)
  -> FastAPI control plane (src/fitcv_cp)
  -> Redis + RQ worker execution
  -> Core pipeline stages (src/fitcv)
  -> Persistent run state + artifacts
  -> Admin inspection/download surfaces
```

Primary architecture references:

- [docs/architecture.md](docs/architecture.md)
- [docs/fitcv-control-plane-setup.md](docs/fitcv-control-plane-setup.md)
- [docs/pipeline.md](docs/pipeline.md)

## Getting Started

### Pre-requisites

- Python environment (`.venv` expected in repo workflows)
- Docker + Docker Compose
- Redis (via compose service)
- Runtime config file (default: `.env.yaml`)
- Credentials required by configured backends (see setup doc)

### Setup

- Read setup guide: [docs/fitcv-control-plane-setup.md](docs/fitcv-control-plane-setup.md)
- Start local services:

```powershell
docker compose up -d --build redis web worker
```

- Open admin UI:

```text
http://localhost:8000/admin/runs
```

## Pending and Further Improvement

### Pending

- Add early warning alerts so operators know quickly when a run is going off track.
- Improve run error summaries so users can find what failed and what to do next faster.

### Further Improvement

- Add a side-by-side run comparison so teams can see which settings lead to better results.
- Add stronger final CV checks to increase trust before people submit applications.
