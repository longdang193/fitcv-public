"""@meta
name: pipeline_stage_context
type: module
domain: runtime
ownership: feature
capabilities:
  - inspection_debugging.stage-transition-diagnostics
responsibility:
  - Provide typed pipeline state helpers for checkpoint restore/serialize.
inputs:
  - run_id and checkpoint payload dictionaries
outputs:
  - canonical pipeline state dictionaries and stage inference
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar


@dataclass
class PipelineState:
    CHECKPOINT_SCHEMA_VERSION: ClassVar[int] = 1
    run_id: str
    raw_jobs: list[dict[str, Any]] = field(default_factory=list)
    normalized: list[dict[str, Any]] = field(default_factory=list)
    deduplicated_jobs: list[dict[str, Any]] = field(default_factory=list)
    pre_filter_rejected_jobs: list[dict[str, Any]] = field(default_factory=list)
    enriched: list[dict[str, Any]] = field(default_factory=list)
    passed_jobs: list[dict[str, Any]] = field(default_factory=list)
    candidate_filter_rejected_jobs: list[dict[str, Any]] = field(default_factory=list)
    raw_shortlist: list[dict[str, Any]] = field(default_factory=list)
    shortlist: list[dict[str, Any]] = field(default_factory=list)
    backfilled_job_urls: list[str] = field(default_factory=list)
    candidate_query_debug: dict[str, Any] = field(default_factory=dict)
    ai_scores: list[dict[str, Any]] = field(default_factory=list)
    ranking_inputs: list[dict[str, Any]] = field(default_factory=list)
    ranked: list[dict[str, Any]] = field(default_factory=list)
    cv_analysis_results: list[dict[str, Any]] = field(default_factory=list)
    cv_results: list[dict[str, Any]] = field(default_factory=list)
    cv_generation_debug_records: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_checkpoint_payload(
        cls,
        *,
        run_id: str,
        checkpoint_payload: dict[str, Any] | None,
    ) -> "PipelineState":
        payload = checkpoint_payload or {}
        if isinstance(payload.get("checkpoint_payload"), dict):
            payload = dict(payload["checkpoint_payload"])
        state = cls(run_id=run_id)
        for key in cls.payload_keys():
            value = payload.get(key)
            if key == "candidate_query_debug":
                if isinstance(value, dict):
                    setattr(state, key, dict(value))
                continue
            if isinstance(value, list):
                setattr(state, key, list(value))
        return state

    @classmethod
    def payload_keys(cls) -> tuple[str, ...]:
        return (
            "raw_jobs",
            "normalized",
            "deduplicated_jobs",
            "pre_filter_rejected_jobs",
            "enriched",
            "passed_jobs",
            "candidate_filter_rejected_jobs",
            "raw_shortlist",
            "shortlist",
            "backfilled_job_urls",
            "candidate_query_debug",
            "ai_scores",
            "ranking_inputs",
            "ranked",
            "cv_analysis_results",
            "cv_results",
            "cv_generation_debug_records",
        )

    def as_state_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "raw_jobs": list(self.raw_jobs),
            "normalized": list(self.normalized),
            "deduplicated_jobs": list(self.deduplicated_jobs),
            "pre_filter_rejected_jobs": list(self.pre_filter_rejected_jobs),
            "enriched": list(self.enriched),
            "passed_jobs": list(self.passed_jobs),
            "candidate_filter_rejected_jobs": list(self.candidate_filter_rejected_jobs),
            "raw_shortlist": list(self.raw_shortlist),
            "shortlist": list(self.shortlist),
            "backfilled_job_urls": list(self.backfilled_job_urls),
            "candidate_query_debug": dict(self.candidate_query_debug),
            "ai_scores": list(self.ai_scores),
            "ranking_inputs": list(self.ranking_inputs),
            "ranked": list(self.ranked),
            "cv_analysis_results": list(self.cv_analysis_results),
            "cv_results": list(self.cv_results),
            "cv_generation_debug_records": list(self.cv_generation_debug_records),
        }


def infer_last_completed_stage_from_state(state: dict[str, Any]) -> str | None:
    stage_state_keys = (
        ("cv_analysis", ("cv_analysis_results",)),
        ("ranking", ("ranked", "ranking_inputs", "ai_scores")),
        ("shortlist", ("shortlist", "raw_shortlist", "backfilled_job_urls", "candidate_query_debug")),
        ("rule_filter", ("passed_jobs", "candidate_filter_rejected_jobs")),
        ("enrich", ("enriched", "pre_filter_rejected_jobs")),
        ("normalize", ("normalized", "deduplicated_jobs", "raw_jobs")),
    )
    for stage_name, keys in stage_state_keys:
        for key in keys:
            value = state.get(key)
            if isinstance(value, list) and value:
                return stage_name
            if isinstance(value, dict) and value:
                return stage_name
    return None
