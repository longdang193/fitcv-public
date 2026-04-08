# FitCV Admin Control Plane Setup

Use this guide to run the FitCV admin UI and background worker on Windows.

The control plane has three moving parts:

- `web`: FastAPI admin UI and API
- `worker`: RQ background worker that runs pipeline jobs
- `redis`: queue backend

Both `web` and `worker` must be running for pipeline runs to execute.

## Choose One Mode

Use one mode at a time. Do not mix a local web or worker process with the Docker worker.

- Local mode: run `.\start_web.ps1` and `.\start_worker.ps1`
- Docker mode: from the checkout or worktree you want to run, use `docker compose up -d --build redis web worker`

If the Docker worker is already running and you want local mode:

```powershell
docker compose stop worker
```

## Credentials

Recommended: keep your Google service-account JSON outside the repo and pass its absolute path explicitly.

Example:

```powershell
.\start_web.ps1 -CredentialPath "C:\secure\your-service-account.json"
.\start_worker.ps1 -CredentialPath "C:\secure\your-service-account.json"
```

Optional local-only convenience: keep an untracked `sa_key.json` in the repo root.

```powershell
Copy-Item -LiteralPath "C:\secure\your-service-account.json" -Destination ".\sa_key.json" -Force
```

If you use `.\sa_key.json`, the helper scripts can be started without `-CredentialPath`.

Do not commit real service-account keys into the repository.

## Local Mode

### 1. Start Redis

```powershell
docker compose up -d redis
```

### 2. Start the Web Server

If you are using an external credential file:

```powershell
.\start_web.ps1 -CredentialPath "C:\secure\your-service-account.json"
```

If you are using `.\sa_key.json`:

```powershell
.\start_web.ps1
```

The admin UI is available at `http://localhost:8000/admin/runs`.

### 3. Start the Worker

Open a second PowerShell window.

If you are using an external credential file:

```powershell
.\start_worker.ps1 -CredentialPath "C:\secure\your-service-account.json"
```

If you are using `.\sa_key.json`:

```powershell
.\start_worker.ps1
```

The worker is healthy when you see:

```text
*** Listening on fitcv...
```

### 4. Stop Local Services

```powershell
.\stop_fitcv.ps1
```

## Docker Mode

Use Docker mode when you want `redis`, `web`, and `worker` all inside containers.

Important: run Docker commands from the repo checkout or git worktree whose files you want Docker to use.

Examples:

- main checkout: `C:\Users\HOANG PHI LONG DANG\repos\JOB-PROJECT`
- feature worktree: `C:\Users\HOANG PHI LONG DANG\repos\JOB-PROJECT\.worktrees\<feature-branch>`

Docker uses the current build context directory. If you run `docker compose up -d --build redis web worker` from a feature worktree, the containers are built from that worktree's files, not from another branch or checkout.

If your service-account key is outside the repo, point Docker at it first:

```powershell
$env:GCP_SA_KEY_PATH="C:\secure\your-service-account.json"
```

Then change into the checkout or worktree you want to run and start everything:

```powershell
cd "C:\Users\HOANG PHI LONG DANG\repos\JOB-PROJECT\.worktrees\<feature-branch>"
docker compose up -d --build redis web worker
```

If you want to run the main checkout instead, use:

```powershell
cd "C:\Users\HOANG PHI LONG DANG\repos\JOB-PROJECT"
docker compose up -d --build redis web worker
```

The admin UI is available at `http://localhost:8000/admin/runs`.

Notes:

- The containers use `/app/.env.yaml` and `/app/config/env.yaml`
- The service-account key is mounted inside the container as `/app/sa_key.json`
- Do not pass Windows paths like `C:\...json` into Docker-triggered runs

## Verify the Setup

### Check the UI

Open:

- `http://localhost:8000/admin/runs`
- `http://localhost:8000/healthz`

### Trigger a Test Run

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8000/runs" `
  -ContentType "application/json" `
  -Body '{"jobs_path":"data/sample_jobs.json","config_path":".env.yaml","triggered_by":"admin"}'
```

Expected run status progression:

1. `queued`
2. `running`
3. `succeeded` or `failed`

## Troubleshooting

### `localhost:8000` does not open

The web server is not running. Check the active mode:

- local mode: rerun `.\start_web.ps1`
- Docker mode: check `docker compose logs web --tail 50`

### Run stays `queued`

The worker is not running.

- local mode: rerun `.\start_worker.ps1`
- Docker mode: check `docker compose logs worker --tail 50`

### Worker fails on Windows with `fork` errors

Do not start the worker with raw `rq worker ...`.

Use:

```powershell
.\start_worker.ps1
```

This repo uses a Windows-safe `SimpleWorker`.

### Docker run fails with missing credential file

Docker containers cannot use Windows host paths like `C:\...\service-account.json` inside the container.

Use one of these:

- set `$env:GCP_SA_KEY_PATH` before `docker compose up`
- or place an untracked `.\sa_key.json` in the repo root

### Docker run fails with missing config file

Rebuild and restart the containers:

```powershell
cd "C:\Users\HOANG PHI LONG DANG\repos\JOB-PROJECT\.worktrees\ranking-six-feature-reactivation"
docker compose down
docker compose up -d --build redis web worker
```

## Key Files

| File | Purpose |
|---|---|
| `start_web.ps1` | Starts the local FastAPI web server |
| `start_worker.ps1` | Starts the local Windows-safe RQ worker |
| `stop_fitcv.ps1` | Stops local FitCV web and worker processes |
| `src/fitcv_cp/app.py` | FastAPI routes, templates, and queue integration |
| `src/fitcv_cp/worker_job.py` | Background job entrypoint |
| `src/fitcv_cp/queue.py` | Redis and worker setup |
| `.env.yaml` | Runtime config |
| `docker-compose.yml` | Docker services for `redis`, `web`, and `worker` |
| `Dockerfile` | Shared image for the web and worker containers |
