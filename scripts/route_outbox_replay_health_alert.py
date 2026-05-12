"""
@meta
name: route_outbox_replay_health_alert
type: utility
domain: observability
responsibility:
  - Execute the outbox replay health checker.
  - Route alert/error outcomes to a webhook endpoint.
inputs:
  - checker runtime flags
  - webhook url and timeout
outputs:
  - checker JSON payload on stdout
  - optional webhook delivery for alert/error paths
  - process exit code compatible with schedulers
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
import subprocess
import sys
from pathlib import Path

import httpx


def _safe_json_load(raw: str) -> dict:
    try:
        parsed = json.loads(raw)
    except Exception:
        return {"raw_output": raw}
    if isinstance(parsed, dict):
        return parsed
    return {"raw_output": raw}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run outbox replay health checker and route alert/error outcomes to webhook."
    )
    parser.add_argument("--base-url", default="http://localhost:8010")
    parser.add_argument("--config-path", default=".env.yaml")
    parser.add_argument("--view", default="active", choices=["active", "all", "archived"])
    parser.add_argument("--min-replay-success-ratio", type=float, default=None)
    parser.add_argument("--event-run-id", default="system-outbox-replay-health")
    parser.add_argument(
        "--emit-event",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Emit control-plane event in the checker path.",
    )
    parser.add_argument(
        "--webhook-url",
        default="",
        help="Webhook URL for alert/error notifications (optional).",
    )
    parser.add_argument("--webhook-timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--notify-on-ok",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Also notify webhook on healthy outcomes.",
    )
    args = parser.parse_args()

    checker_script = Path(__file__).with_name("check_outbox_replay_health.py")
    cmd = [
        sys.executable,
        str(checker_script),
        "--base-url",
        str(args.base_url),
        "--config-path",
        str(args.config_path),
        "--view",
        str(args.view),
        "--event-run-id",
        str(args.event_run_id),
    ]
    if args.min_replay_success_ratio is not None:
        cmd.extend(["--min-replay-success-ratio", str(args.min_replay_success_ratio)])
    cmd.append("--emit-event" if args.emit_event else "--no-emit-event")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    stdout_text = str(result.stdout or "").strip()
    stderr_text = str(result.stderr or "").strip()
    payload = _safe_json_load(stdout_text)
    if stderr_text:
        payload["checker_stderr"] = stderr_text

    exit_code = int(result.returncode)
    should_notify = args.notify_on_ok or exit_code in {2, 3}
    webhook_status = "skipped"
    webhook_error = ""
    if should_notify and str(args.webhook_url or "").strip():
        body = {
            "source": "fitcv.outbox_replay_health",
            "checker_exit_code": exit_code,
            "checker_payload": payload,
        }
        try:
            with httpx.Client(timeout=float(args.webhook_timeout_seconds)) as client:
                resp = client.post(str(args.webhook_url), json=body)
                resp.raise_for_status()
            webhook_status = "sent"
        except Exception as exc:
            webhook_status = "failed"
            webhook_error = str(exc)
            if exit_code == 0:
                exit_code = 4
    elif should_notify:
        webhook_status = "not_configured"

    routed = {
        "checker_exit_code": int(result.returncode),
        "final_exit_code": exit_code,
        "webhook_status": webhook_status,
        "webhook_error": webhook_error,
        "payload": payload,
    }
    print(json.dumps(routed, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
