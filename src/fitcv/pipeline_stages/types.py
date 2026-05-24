"""@meta
name: pipeline_stages.types
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared stage boundary types for progressive extraction out of src.fitcv.pipeline.
inputs:
  - StageContext passed into stage modules.
outputs:
  - StageResult envelope returned by stage modules.
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class StageContext:
    run_id: str
    config: Mapping[str, Any]


@dataclass(frozen=True)
class StageResult:
    stage: str
    status: str
    payload: Mapping[str, Any]
