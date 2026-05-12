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

This guide targets first-run success: clone repo, start web + worker, open
`/admin/runs`, trigger one run.

Use [fitcv-control-plane-setup.md](fitcv-control-plane-setup.md) for deeper
operator runbook detail.

## 1) Prerequisites

- Python 3.11+
- Git
- Optional Docker Desktop (recommended easiest path)
- Redis (needed for local queue mode)

Quick checks:

```powershell
python --version
git --version
docker --version
```

## 2) Clone And Install

```powershell
git clone https://github.com/longdang193/fitcv-public.git
cd fitcv-public
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 3) Runtime Config Contract (Single Source Of Truth)

Canonical runtime config file:

- `config/env.yaml`

Secrets and runtime env vars file:

- `.env` (untracked)

Notes:

- Do not depend on `.env.yaml.example`.
- `.env.yaml` may exist as local override in some setups, but canonical default
  for control-plane runs is `config/env.yaml`.

Minimum backend env for local SQLite mode:

```powershell
$env:FITCV_CP_DATA_BACKEND = "sqlite"
```

Optional SQLite path override:

```powershell
$env:FITCV_CP_SQLITE_PATH = ".\data\fitcv_local.db"
```

## 4) Start Application (Choose One)

### Track A: Docker (recommended)

```powershell
docker compose up -d --build redis web worker
```

Open:

- `http://localhost:8000/admin/runs`

### Track B: Local web + worker

Terminal 1:

```powershell
.\start_web.ps1
```

Terminal 2:

```powershell
.\start_worker.ps1
```

Open:

- `http://localhost:8000/admin/runs`

## 5) Validate Health + First Run

- `GET /healthz` should return HTTP 200
- trigger one run from `/admin/runs`
- confirm run transitions queued -> running -> succeeded/failed
- confirm run detail shows artifacts/events

## 6) Backend Modes

### SQLite (recommended local default)

- `FITCV_CP_DATA_BACKEND=sqlite`
- optional `FITCV_CP_SQLITE_PATH`

### BigQuery (advanced)

Use after SQLite path works.

Required env:

- `FITCV_CP_DATA_BACKEND=bigquery`
- `GCP_PROJECT`
- `BIGQUERY_DATASET`
- `GOOGLE_APPLICATION_CREDENTIALS`

## 7) Troubleshooting

- `/admin/runs` not opening:
  - check web process/container running
  - check port 8000 conflict
- run stuck queued:
  - check worker running
  - check Redis reachable
- `/healthz` fails:
  - inspect web logs
  - re-check env/backend values

## Related Docs

- [configuration.md](configuration.md)
- [usage.md](usage.md)
- [pipeline.md](pipeline.md)
- [architecture.md](architecture.md)
