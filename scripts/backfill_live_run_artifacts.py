"""@meta
name: backfill_live_run_artifacts
type: script
domain: run_orchestration
ownership: infrastructure
responsibility:
  - Backfill deterministic live-run artifact mirror folders for terminal runs.
inputs:
  - fitcv_cp runtime backend state and run records
outputs:
  - artifacts/live_run_<run_id> filesystem mirrors
lifecycle:
  - status: active
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fitcv_cp.backend_runtime import resolve_backend_runtime
from fitcv_cp.bq_store import get_run, list_runs
from fitcv_cp.models import RunStatus
from fitcv_cp.run_artifact_mirror import persist_terminal_run_artifact_mirror


def _get_bq():
    from google.cloud import bigquery

    return bigquery.Client()


def _has_artifact_payload(run: object) -> bool:
    return any(
        bool(str(getattr(run, field, "") or "").strip())
        for field in (
            "results_export_json",
            "cv_generation_debug_json",
            "stage_transition_artifacts_json",
            "settings_used_json",
            "mapping_suggestions_json",
            "synonym_proposals_json",
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill artifacts/live_run_<run_id> mirrors for terminal runs.",
    )
    parser.add_argument("--run-id", help="Only backfill one run id.")
    parser.add_argument("--dry-run", action="store_true", help="Report actions without writing files.")
    parser.add_argument("--limit", type=int, default=5000, help="Max runs to inspect when --run-id is not provided.")
    args = parser.parse_args()

    runtime = resolve_backend_runtime()
    bq = _get_bq() if runtime.backend_type == "bigquery" else None
    project = runtime.project
    dataset = runtime.dataset

    target_runs = []
    if args.run_id:
        run = get_run(args.run_id, bq, project=project, dataset=dataset)
        if run is None:
            print(f"run_not_found run_id={args.run_id}")
            return 2
        target_runs = [run]
    else:
        target_runs = list_runs(
            bq,
            project=project,
            dataset=dataset,
            limit=max(1, int(args.limit)),
            include_archived=True,
        )

    created = 0
    skipped_existing = 0
    missing_payload = 0
    errors = 0

    terminal = {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}
    for run in target_runs:
        if getattr(run, "status", None) not in terminal:
            continue
        run_id = str(getattr(run, "run_id", "") or "").strip()
        if not run_id:
            continue
        mirror_dir = Path("artifacts") / f"live_run_{run_id}"
        if mirror_dir.exists():
            skipped_existing += 1
            print(f"skip_existing run_id={run_id}")
            continue
        if not _has_artifact_payload(run):
            missing_payload += 1
            print(f"skip_missing_payload run_id={run_id}")
            continue
        if args.dry_run:
            created += 1
            print(f"dry_run_create run_id={run_id}")
            continue
        try:
            persist_terminal_run_artifact_mirror(
                run_id=run_id,
                bq=bq,
                project=project,
                dataset=dataset,
            )
            if mirror_dir.exists():
                created += 1
                print(f"created run_id={run_id}")
            else:
                errors += 1
                print(f"error_not_created run_id={run_id}")
        except Exception as exc:  # pragma: no cover - safety path
            errors += 1
            print(f"error run_id={run_id} detail={exc}")

    print(
        "summary "
        f"created={created} "
        f"skipped_existing={skipped_existing} "
        f"missing_payload={missing_payload} "
        f"errors={errors}"
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
