"""@meta
name: store
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.store.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from fitcv_cp import bq_store
from fitcv_cp.models import PipelineRun, RunEvent


class RunStore(Protocol):
    def insert_run(self, run: PipelineRun) -> None: ...
    def update_run_queue_job_id(self, run_id: str, queue_job_id: str) -> None: ...
    def update_run_orchestration_binding(
        self,
        run_id: str,
        *,
        queue_job_id: str | None,
        orchestration_backend: str | None,
        orchestration_run_id: str | None,
    ) -> None: ...
    def get_run(self, run_id: str) -> PipelineRun | None: ...
    def list_runs(
        self,
        *,
        limit: int = 50,
        include_archived: bool = False,
        archived_only: bool = False,
    ) -> list[PipelineRun]: ...
    def get_events(self, run_id: str) -> list[RunEvent]: ...
    def update_run_status(self, run_id: str, status: Any, **kwargs: Any) -> None: ...
    def update_run_checkpoint(self, run_id: str, **kwargs: Any) -> None: ...
    def request_run_cancel(
        self,
        run_id: str,
        requested_by: str,
        target_status: str,
    ) -> bool: ...
    def archive_run(self, run_id: str, archived_by: str) -> None: ...
    def unarchive_run(self, run_id: str) -> None: ...
    def list_cvs_for_run(self, run_id: str) -> list[dict[str, Any]]: ...
    def get_cv_markdown(self, version_id: str) -> str | None: ...
    def list_run_structured_jobs(self, run_id: str) -> list[dict[str, Any]]: ...
    def list_filter_results_for_run(self, run_id: str) -> list[dict[str, Any]]: ...
    def get_pipeline_runs_schema_status(self) -> dict[str, Any]: ...
    def append_event(self, event: RunEvent) -> dict[str, str]: ...
    def update_run_effective_settings(self, run_id: str, effective_settings_json: str) -> None: ...
    def update_run_synonym_proposals(
        self, run_id: str, synonym_proposals_json: str
    ) -> dict[str, str]: ...
    def update_run_cv_generation_debug(self, run_id: str, cv_generation_debug_json: str) -> None: ...
    def insert_cv_version_row(self, row: dict[str, Any]) -> list[Any]: ...


@dataclass
class ControlPlaneStore:
    bq: Any
    project: str
    dataset: str
    insert_run_fn: Any | None = None
    update_run_queue_job_id_fn: Any | None = None
    update_run_orchestration_binding_fn: Any | None = None
    get_run_fn: Any | None = None
    list_runs_fn: Any | None = None
    get_events_fn: Any | None = None
    update_run_status_fn: Any | None = None
    update_run_checkpoint_fn: Any | None = None
    request_run_cancel_fn: Any | None = None
    archive_run_fn: Any | None = None
    unarchive_run_fn: Any | None = None
    list_cvs_for_run_fn: Any | None = None
    get_cv_markdown_fn: Any | None = None
    list_run_structured_jobs_fn: Any | None = None
    list_filter_results_for_run_fn: Any | None = None
    get_pipeline_runs_schema_status_fn: Any | None = None
    append_event_fn: Any | None = None
    update_run_effective_settings_fn: Any | None = None
    update_run_synonym_proposals_fn: Any | None = None
    update_run_cv_generation_debug_fn: Any | None = None
    insert_cv_version_row_fn: Any | None = None

    def insert_run(self, run: PipelineRun) -> None:
        fn = self.insert_run_fn or bq_store.insert_run
        fn(run, self.bq, project=self.project, dataset=self.dataset)

    def update_run_queue_job_id(self, run_id: str, queue_job_id: str) -> None:
        fn = self.update_run_queue_job_id_fn or bq_store.update_run_queue_job_id
        fn(
            run_id,
            queue_job_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def update_run_orchestration_binding(
        self,
        run_id: str,
        *,
        queue_job_id: str | None,
        orchestration_backend: str | None,
        orchestration_run_id: str | None,
    ) -> None:
        fn = self.update_run_orchestration_binding_fn or bq_store.update_run_orchestration_binding
        fn(
            run_id,
            queue_job_id=queue_job_id,
            orchestration_backend=orchestration_backend,
            orchestration_run_id=orchestration_run_id,
            bq=self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def get_run(self, run_id: str) -> PipelineRun | None:
        fn = self.get_run_fn or bq_store.get_run
        return fn(run_id, self.bq, project=self.project, dataset=self.dataset)

    def list_runs(
        self,
        *,
        limit: int = 50,
        include_archived: bool = False,
        archived_only: bool = False,
    ) -> list[PipelineRun]:
        fn = self.list_runs_fn or bq_store.list_runs
        return fn(
            self.bq,
            project=self.project,
            dataset=self.dataset,
            limit=limit,
            include_archived=include_archived,
            archived_only=archived_only,
        )

    def get_events(self, run_id: str) -> list[RunEvent]:
        fn = self.get_events_fn or bq_store.get_events
        return fn(run_id, self.bq, project=self.project, dataset=self.dataset)

    def update_run_status(self, run_id: str, status: Any, **kwargs: Any) -> None:
        fn = self.update_run_status_fn or bq_store.update_run_status
        fn(run_id, status, self.bq, project=self.project, dataset=self.dataset, **kwargs)

    def update_run_checkpoint(self, run_id: str, **kwargs: Any) -> None:
        fn = self.update_run_checkpoint_fn or bq_store.update_run_checkpoint
        fn(run_id, self.bq, project=self.project, dataset=self.dataset, **kwargs)

    def request_run_cancel(
        self,
        run_id: str,
        requested_by: str,
        target_status: str,
    ) -> bool:
        fn = self.request_run_cancel_fn or bq_store.request_run_cancel
        return bool(
            fn(
                run_id,
                requested_by,
                target_status,
                self.bq,
                project=self.project,
                dataset=self.dataset,
            )
        )

    def archive_run(self, run_id: str, archived_by: str) -> None:
        fn = self.archive_run_fn or bq_store.archive_run
        fn(run_id, archived_by, self.bq, project=self.project, dataset=self.dataset)

    def unarchive_run(self, run_id: str) -> None:
        fn = self.unarchive_run_fn or bq_store.unarchive_run
        fn(run_id, self.bq, project=self.project, dataset=self.dataset)

    def list_cvs_for_run(self, run_id: str) -> list[dict[str, Any]]:
        fn = self.list_cvs_for_run_fn or bq_store.list_cvs_for_run
        return list(fn(run_id, self.bq, project=self.project, dataset=self.dataset))

    def get_cv_markdown(self, version_id: str) -> str | None:
        fn = self.get_cv_markdown_fn or bq_store.get_cv_markdown
        return fn(version_id, self.bq, project=self.project, dataset=self.dataset)

    def list_run_structured_jobs(self, run_id: str) -> list[dict[str, Any]]:
        fn = self.list_run_structured_jobs_fn or bq_store.list_run_structured_jobs
        return list(fn(run_id, self.bq, project=self.project, dataset=self.dataset))

    def list_filter_results_for_run(self, run_id: str) -> list[dict[str, Any]]:
        fn = self.list_filter_results_for_run_fn or bq_store.list_filter_results_for_run
        return list(fn(run_id, self.bq, project=self.project, dataset=self.dataset))

    def get_pipeline_runs_schema_status(self) -> dict[str, Any]:
        fn = self.get_pipeline_runs_schema_status_fn or bq_store.get_pipeline_runs_schema_status
        return dict(fn(self.bq, project=self.project, dataset=self.dataset))

    def append_event(self, event: RunEvent) -> dict[str, str]:
        fn = self.append_event_fn or bq_store.append_event
        return dict(fn(event, self.bq, project=self.project, dataset=self.dataset))

    def update_run_effective_settings(self, run_id: str, effective_settings_json: str) -> None:
        fn = self.update_run_effective_settings_fn or bq_store.update_run_effective_settings
        fn(run_id, effective_settings_json, self.bq, project=self.project, dataset=self.dataset)

    def update_run_synonym_proposals(
        self, run_id: str, synonym_proposals_json: str
    ) -> dict[str, str]:
        fn = self.update_run_synonym_proposals_fn or bq_store.update_run_synonym_proposals
        return dict(fn(run_id, synonym_proposals_json, self.bq, project=self.project, dataset=self.dataset))

    def update_run_cv_generation_debug(self, run_id: str, cv_generation_debug_json: str) -> None:
        fn = self.update_run_cv_generation_debug_fn or bq_store.update_run_cv_generation_debug
        fn(run_id, cv_generation_debug_json, self.bq, project=self.project, dataset=self.dataset)

    def insert_cv_version_row(self, row: dict[str, Any]) -> list[Any]:
        fn = self.insert_cv_version_row_fn or bq_store.insert_cv_version_row
        return list(fn(row, self.bq, project=self.project, dataset=self.dataset))
