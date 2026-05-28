"""@meta
name: reconciler_service
type: module
domain: run_orchestration
ownership: infrastructure
responsibility:
  - Provide dedicated reconciler process for abandoned attempt recovery.
inputs:
  - control-plane config (retry.reconciler_interval_seconds)
  - backend runtime (sqlite or BigQuery)
outputs:
  - periodic reconcile_abandoned_attempts() side effects
lifecycle:
  - status: active
"""

from __future__ import annotations

import datetime
import logging
import os
import time

from fitcv_cp.backend_runtime import resolve_backend_runtime
from fitcv_cp.reconciler import reconcile_abandoned_attempts
from fitcv_cp.retry_settings import load_retry_settings
from fitcv_cp.store import ControlPlaneStore

logger = logging.getLogger(__name__)


def _build_store() -> ControlPlaneStore:
    runtime = resolve_backend_runtime()
    if runtime.backend_type == "sqlite":
        os.environ.setdefault("FITCV_CP_SQLITE_PATH", runtime.sqlite_path)
        return ControlPlaneStore(bq=None, project=str(runtime.project or "local"), dataset=str(runtime.dataset or "local"))

    from fitcv_cp.main import _build_bigquery_client

    bq = _build_bigquery_client()
    return ControlPlaneStore(bq=bq, project=str(runtime.project), dataset=str(runtime.dataset))


def run_reconciler_forever() -> None:
    settings = load_retry_settings()
    interval = int(settings.reconciler_interval_seconds)
    if interval <= 0:
        logger.info("reconciler disabled (reconciler_interval_seconds=%s)", interval)
        return

    store = _build_store()
    logger.info("reconciler started (interval_seconds=%s)", interval)

    while True:
        started_at = datetime.datetime.now(datetime.timezone.utc)
        try:
            summary = reconcile_abandoned_attempts(store, now=started_at)
            logger.info(
                "reconcile summary scanned_runs=%s abandoned_attempts=%s requeued_attempts=%s terminal_failed_runs=%s",
                summary.scanned_runs,
                summary.abandoned_attempts,
                summary.requeued_attempts,
                summary.terminal_failed_runs,
            )
        except Exception as exc:
            logger.exception("reconcile failed: %s", exc)
        time.sleep(max(1, interval))


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    run_reconciler_forever()


if __name__ == "__main__":
    main()
