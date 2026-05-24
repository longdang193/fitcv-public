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

Canonical config ownership:

- `config/env.yaml`
  - base runtime entry config for control-plane runs
  - infra/environment keys and local candidate-profile path
- `config/runtime/pipeline.yaml`
  - canonical owner for pipeline/ranking/retrieval model+limit knobs
  - stage behavior defaults used by `src/fitcv/*` runtime modules

Secrets and runtime env vars file:

- `.env` (untracked)

Provider routing ownership:

- canonical default owner: `config/runtime/control_plane.yaml`
- `.env` `FITCV_LANGGRAPH_*` values are override-only (optional)
- do not treat `.env` as default owner for provider/model/base_url/wire_api

Notes:

- Do not depend on `.env.yaml.example`.
- `.env.yaml` may exist as local override in some setups, but canonical default
  for control-plane runs is `config/env.yaml`.
- Do not duplicate runtime/pipeline knobs in `config/env.yaml` when they are
  already owned by `config/runtime/pipeline.yaml`.
- For provider routing expectation, precedence is:
  1. non-empty `FITCV_LANGGRAPH_*` env overrides
  2. control-plane defaults in `config/runtime/control_plane.yaml`
  3. fail fast on unresolved required fields

Minimum backend env for local SQLite mode:

```powershell
$env:FITCV_CP_DATA_BACKEND = "sqlite"
```

Optional SQLite path override:

```powershell
$env:FITCV_CP_SQLITE_PATH = ".\data\fitcv_local.db"
```

## 4) Candidate Profile Contract (Canonical + Optional Private Source)

Current default runtime path in `config/env.yaml`:

- `paths.candidate_profile: data/candidate_profile.yaml`

Use these files with strict boundaries:

- `data/candidate_profile.template.yaml`
  - public-safe scaffold only
  - no private values or PII
  - edit only when profile contract/schema changes
- `data/candidate_profile.yaml`
  - canonical runtime candidate profile source for current defaults
  - keep this synchronized with your local private profile workflow
- `data/candidate_profile.private.yaml` (optional local workflow)
  - local private candidate values
  - ignored/untracked helper surface when teams choose private-source workflow

Recommended workflow:

1. Review required keys in `data/candidate_profile.template.yaml`.
2. Fill real values in `data/candidate_profile.yaml` (or generate/sync it from a private local source).
3. Run checks:

```powershell
git check-ignore data/candidate_profile.private.yaml
pytest -q tests/test_candidate_profile_template_contract.py
```

## 5) Start Application (Choose One)

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
