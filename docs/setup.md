---
doc_id: setup
doc_type: setup-guide
explains:
  features:
    - admin_control_plane_core
    - trigger_run_management
  components:
    - src/fitcv
    - src/fitcv_cp
---

# Setup

FitCV is a `managed_architecture_metadata` repo with two runtime surfaces:

- control plane app (`src/fitcv_cp/`)
- staged pipeline runtime (`src/fitcv/`)

This page is the short setup map. Use [fitcv-control-plane-setup.md](fitcv-control-plane-setup.md) for deeper runbook detail.

## Prerequisites

- Python environment with repo dependencies
- Redis when using queue mode
- Optional Docker Desktop for containerized startup
- Runtime credentials/secrets via process env or local `.env` (untracked)

## Supported Backend Modes

### SQLite mode (recommended for local)

- set `FITCV_CP_DATA_BACKEND=sqlite`
- set sqlite path via `FITCV_CP_SQLITE_PATH` if needed
- start app + worker (or inline mode)

### BigQuery mode

- set `FITCV_CP_DATA_BACKEND=bigquery`
- provide `GCP_PROJECT`, `BIGQUERY_DATASET`
- provide `GOOGLE_APPLICATION_CREDENTIALS`

## Startup Shapes

### Local app + worker

1. start Redis (if queue mode)
2. start web app
3. start worker
4. open `/admin/runs`

### Docker mode

- run `docker compose up -d --build redis web worker`
- open `/admin/runs`

## Quick Validation

- `GET /healthz`
- trigger one run from `/admin/runs`
- verify run detail artifacts and events are visible

## Related Docs

- [configuration.md](configuration.md)
- [usage.md](usage.md)
- [pipeline.md](pipeline.md)
- [architecture.md](architecture.md)
