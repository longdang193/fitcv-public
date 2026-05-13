"""
@meta
name: check_outbox_replay_health
type: utility
domain: observability
responsibility:
  - Evaluate outbox replay health via control-plane endpoint.
  - Exit non-zero when alert thresholds are breached.
inputs:
  - control-plane base url
  - view and replay success ratio threshold
outputs:
  - stdout JSON payload
  - process exit code suitable for schedulers/alerts
capabilities:
  - trigger_run_management.run-health-surface
tags:
  - reliability
  - alerting
lifecycle:
  status: active
"""

from __future__ import annotations

import argparse
import json

import httpx
from fitcv.config import load_config


def _resolve_min_replay_success_ratio(*, config_path: str, cli_override: float | None) -> float:
    if cli_override is not None:
        return float(cli_override)
    try:
        cfg = load_config(config_path)
    except Exception:
        return 0.95
    replay_cfg = dict(cfg.get("outbox_replay_health") or {})
    raw = replay_cfg.get("min_replay_success_ratio")
    if isinstance(raw, (int, float, str)):
        try:
            return float(raw)
        except ValueError:
            return 0.95
    return 0.95


def main() -> int:
    parser = argparse.ArgumentParser(description="Check outbox replay health and trigger alert decision.")
    parser.add_argument("--base-url", default="http://localhost:8010", help="Control-plane base URL.")
    parser.add_argument("--config-path", default=".env.yaml", help="Path to FitCV runtime env YAML.")
    parser.add_argument("--view", default="active", choices=["active", "all", "archived"])
    parser.add_argument("--min-replay-success-ratio", type=float, default=None)
    parser.add_argument("--event-run-id", default="system-outbox-replay-health")
    parser.add_argument(
        "--emit-event",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Emit control-plane event for each check.",
    )
    args = parser.parse_args()

    min_replay_success_ratio = _resolve_min_replay_success_ratio(
        config_path=str(args.config_path),
        cli_override=args.min_replay_success_ratio,
    )

    base_url = str(args.base_url).rstrip("/")
    endpoint = f"{base_url}/admin/outbox-replay-health/check"
    params = {
        "view": args.view,
        "min_replay_success_ratio": min_replay_success_ratio,
        "emit_event": args.emit_event,
        "event_run_id": args.event_run_id,
    }
    try:
        with httpx.Client(timeout=15.0) as client:
            response = client.post(endpoint, params=params)
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        print(json.dumps({"status": "error", "message": str(exc)}, ensure_ascii=False))
        return 3

    print(json.dumps(payload, ensure_ascii=False))
    decision = str(payload.get("decision") or "")
    return 2 if decision == "alert" else 0


if __name__ == "__main__":
    raise SystemExit(main())
