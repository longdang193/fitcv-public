"""@meta
name: pipeline_store
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.pipeline_store.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class PipelineStore:
    load_raw_jobs_fn: Callable[..., None]
    load_candidate_profile_fn: Callable[..., None]
    lookup_reusable_structured_jobs_fn: Callable[..., dict[str, dict[str, Any]]]
    load_structured_jobs_fn: Callable[..., None]
    load_run_structured_jobs_fn: Callable[..., None]
    store_filter_results_fn: Callable[..., None]
    embed_and_store_jobs_fn: Callable[..., None]
    store_shortlist_fn: Callable[..., None]
    store_final_ranking_fn: Callable[..., None]
    store_cv_version_fn: Callable[..., None]

    def load_raw_jobs(self, raw_rows: list[dict[str, Any]], config: dict[str, Any]) -> None:
        self.load_raw_jobs_fn(raw_rows, config)

    def load_candidate_profile(self, profile: dict[str, Any], config: dict[str, Any]) -> None:
        self.load_candidate_profile_fn(profile, config)

    def lookup_reusable_structured_jobs(
        self,
        normalized_jobs: list[dict[str, Any]],
        config: dict[str, Any],
        *,
        raw_job_fingerprints: dict[str, str],
        enrich_contract_fingerprint: str,
    ) -> dict[str, dict[str, Any]]:
        return self.lookup_reusable_structured_jobs_fn(
            normalized_jobs,
            config,
            raw_job_fingerprints=raw_job_fingerprints,
            enrich_contract_fingerprint=enrich_contract_fingerprint,
        )

    def load_structured_jobs(
        self,
        rows: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> None:
        self.load_structured_jobs_fn(rows, config)

    def load_run_structured_jobs(
        self,
        rows: list[dict[str, Any]],
        run_id: str,
        config: dict[str, Any],
    ) -> None:
        self.load_run_structured_jobs_fn(rows, run_id, config)

    def store_filter_results(
        self,
        result: dict[str, list[Any]],
        run_id: str,
        config: dict[str, Any],
    ) -> None:
        self.store_filter_results_fn(result, run_id, config)

    def embed_and_store_jobs(
        self,
        jobs: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> None:
        self.embed_and_store_jobs_fn(jobs, config)

    def store_shortlist(
        self,
        shortlist: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> None:
        self.store_shortlist_fn(shortlist, config)

    def store_final_ranking(
        self,
        ranked_jobs: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> None:
        self.store_final_ranking_fn(ranked_jobs, config)

    def store_cv_version(self, version: dict[str, Any], config: dict[str, Any]) -> None:
        self.store_cv_version_fn(version, config)
