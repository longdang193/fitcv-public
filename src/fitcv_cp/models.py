"""@meta
name: models
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.models.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import dataclasses
import datetime
from enum import Enum
from typing import Optional


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    AWAITING_CONTINUE = "awaiting_continue"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class EventLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


# Standardised stage names — use these constants in reporter and pipeline.py
STAGE_PIPELINE_START = "pipeline_start"
STAGE_LAYER1_JOBS = "layer1_jobs"
STAGE_LAYER2_CANDIDATE = "layer2_candidate"
STAGE_LAYER3_FILTER = "layer3_filter"
STAGE_LAYER3_RANKING = "layer3_ranking"
STAGE_LAYER4_CV_SKIP = "layer4_cv_skip"
STAGE_LAYER4_CV_VALIDATION_FAILED = "layer4_cv_validation_failed"
STAGE_PIPELINE_COMPLETE = "pipeline_complete"
STAGE_PIPELINE_FAILED = "pipeline_failed"


@dataclasses.dataclass
class PipelineRun:
    run_id: str
    status: RunStatus
    triggered_by: str
    trigger_source: str
    jobs_path: str
    config_path: str
    created_at: datetime.datetime
    started_at: Optional[datetime.datetime] = None
    finished_at: Optional[datetime.datetime] = None
    total_jobs: Optional[int] = None
    passed_filter: Optional[int] = None
    ranked: Optional[int] = None
    cvs_generated: Optional[int] = None
    error_message: Optional[str] = None
    error_stage: Optional[str] = None   # which stage the run failed at
    effective_settings_json: Optional[str] = None  # merged config snapshot at trigger time
    results_export_json: Optional[str] = None      # immutable run-results export snapshot for completed runs
    cv_generation_debug_json: Optional[str] = None  # immutable run-scoped CV-generation debug snapshot
    stage_transition_artifacts_json: Optional[str] = None  # immutable run-scoped stage transition artifact snapshot
    settings_used_json: Optional[str] = None  # immutable run-scoped effective-settings snapshot
    mapping_suggestions_json: Optional[str] = None  # immutable run-scoped mapping suggestions snapshot
    synonym_proposals_json: Optional[str] = None  # mutable run-scoped synonym proposal review snapshot
    run_mode: str = "run_all"
    checkpoint_status: Optional[str] = None
    next_stage: Optional[str] = None
    last_completed_stage: Optional[str] = None
    completed_stages: Optional[list[str]] = None
    checkpoint_payload_json: Optional[str] = None
    # run-scoped input metadata
    jobs_input_source: Optional[str] = None           # "path" | "upload" | "paste"
    jobs_input_json: Optional[str] = None             # canonical resolved jobs-input snapshot for supported trigger modes in new runs
    jobs_input_manifest_json: Optional[str] = None    # trigger-time jobs-input provenance metadata (e.g. upload source filenames)
    candidate_profile_source: Optional[str] = None    # "default_config" | "upload" | "paste"
    candidate_profile_json: Optional[str] = None      # canonical resolved candidate-profile snapshot for supported trigger modes in new runs
    # lifecycle controls
    queue_job_id: Optional[str] = None               # RQ job id for queued-run cancellation
    orchestration_backend: Optional[str] = None      # orchestration backend used for this run
    orchestration_run_id: Optional[str] = None       # backend-native execution id for this run
    cancel_requested_at: Optional[datetime.datetime] = None
    cancel_requested_by: Optional[str] = None
    archived_at: Optional[datetime.datetime] = None
    archived_by: Optional[str] = None
    raw_status: Optional[str] = None


@dataclasses.dataclass
class RunEvent:
    run_id: str
    event_id: str
    stage: str
    level: str   # one of EventLevel values
    message: str
    created_at: datetime.datetime
    payload_json: Optional[str] = None
