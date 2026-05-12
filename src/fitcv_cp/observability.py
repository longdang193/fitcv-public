"""@meta
name: observability
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.observability.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def emit_observability_event(event: str, payload: dict[str, Any]) -> None:
    body = {"event": event, **payload}
    logger.info(json.dumps(body, ensure_ascii=False, sort_keys=True))

