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

from fitcv_cp.backend_runtime import BackendRuntime, set_backend_runtime
from fitcv_cp import bq_store
from fitcv_cp.models import PipelineRun, RunEvent
from fitcv_cp.run_artifact_contracts import decode_run_attempt_payload_or_none


class RunStore(Protocol):
    def insert_run(self, run: PipelineRun) -> None: ...
    def update_run_queue_job_id(self, run_id: str, queue_job_id: str) -> dict[str, str]: ...
    def update_run_orchestration_binding(
        self,
        run_id: str,
        *,
        queue_job_id: str | None,
        orchestration_backend: str | None,
        orchestration_run_id: str | None,
    ) -> dict[str, str]: ...
    def get_run(self, run_id: str) -> PipelineRun | None: ...
    def list_runs(
        self,
        *,
        limit: int = 50,
        include_archived: bool = False,
        archived_only: bool = False,
    ) -> list[PipelineRun]: ...
    def get_events(self, run_id: str) -> list[RunEvent]: ...
    def update_run_status(self, run_id: str, status: Any, **kwargs: Any) -> dict[str, str]: ...
    def update_run_checkpoint(self, run_id: str, **kwargs: Any) -> dict[str, str]: ...
    def request_run_cancel(
        self,
        run_id: str,
        requested_by: str,
        target_status: str,
    ) -> bool: ...
    def archive_run(self, run_id: str, archived_by: str) -> None: ...
    def unarchive_run(self, run_id: str) -> None: ...
    def delete_archived_runs(self, older_than_days: int | str, run_ids: list[str] | None = None) -> dict[str, Any]: ...
    def list_cvs_for_run(self, run_id: str) -> list[dict[str, Any]]: ...
    def get_cv_markdown(self, version_id: str) -> str | None: ...
    def list_run_structured_jobs(self, run_id: str) -> list[dict[str, Any]]: ...
    def list_filter_results_for_run(self, run_id: str) -> list[dict[str, Any]]: ...
    def get_pipeline_runs_schema_status(self) -> dict[str, Any]: ...
    def list_run_attempt_payloads(self, run_id: str) -> list[dict[str, Any]]: ...
    def append_event(self, event: RunEvent) -> dict[str, str]: ...
    def update_run_effective_settings(self, run_id: str, effective_settings_json: str) -> dict[str, str]: ...
    def update_run_synonym_proposals(
        self, run_id: str, synonym_proposals_json: str
    ) -> dict[str, str]: ...
    def update_run_cv_generation_debug(self, run_id: str, cv_generation_debug_json: str) -> dict[str, str]: ...
    def update_run_stage_transition_artifacts(self, run_id: str, stage_transition_artifacts_json: str) -> dict[str, str]: ...
    def insert_cv_version_row(self, row: dict[str, Any]) -> list[Any]: ...


@dataclass
class ControlPlaneStore:
    bq: Any
    project: str
    dataset: str
    backend_runtime: BackendRuntime | None = None
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
    delete_archived_runs_fn: Any | None = None
    list_cvs_for_run_fn: Any | None = None
    get_cv_markdown_fn: Any | None = None
    list_run_structured_jobs_fn: Any | None = None
    list_filter_results_for_run_fn: Any | None = None
    get_pipeline_runs_schema_status_fn: Any | None = None
    append_event_fn: Any | None = None
    update_run_effective_settings_fn: Any | None = None
    update_run_synonym_proposals_fn: Any | None = None
    update_run_cv_generation_debug_fn: Any | None = None
    update_run_stage_transition_artifacts_fn: Any | None = None
    insert_cv_version_row_fn: Any | None = None

    def __post_init__(self) -> None:
        if self.backend_runtime is not None:
            set_backend_runtime(self.backend_runtime)

    def _resolve_fn(self, override_fn: Any | None, default_fn: Any) -> Any:
        return override_fn or default_fn

    def _call(self, override_fn: Any | None, default_fn: Any, *args: Any, **kwargs: Any) -> Any:
        fn = self._resolve_fn(override_fn, default_fn)
        return fn(*args, **kwargs)

    def _call_list(self, override_fn: Any | None, default_fn: Any, *args: Any, **kwargs: Any) -> list[Any]:
        value = self._call(override_fn, default_fn, *args, **kwargs)
        if value is None:
            return []
        return list(value)

    def _call_dict(self, override_fn: Any | None, default_fn: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        value = self._call(override_fn, default_fn, *args, **kwargs)
        if value is None:
            return {}
        return dict(value)

    def insert_run(self, run: PipelineRun) -> None:
        self._call(
            self.insert_run_fn,
            bq_store.insert_run,
            run,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def update_run_queue_job_id(self, run_id: str, queue_job_id: str) -> dict[str, str]:
        return self._call_dict(
            self.update_run_queue_job_id_fn,
            bq_store.update_run_queue_job_id,
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
    ) -> dict[str, str]:
        return self._call_dict(
            self.update_run_orchestration_binding_fn,
            bq_store.update_run_orchestration_binding,
            run_id,
            queue_job_id=queue_job_id,
            orchestration_backend=orchestration_backend,
            orchestration_run_id=orchestration_run_id,
            bq=self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def get_run(self, run_id: str) -> PipelineRun | None:
        return self._call(
            self.get_run_fn,
            bq_store.get_run,
            run_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def list_runs(
        self,
        *,
        limit: int = 50,
        include_archived: bool = False,
        archived_only: bool = False,
    ) -> list[PipelineRun]:
        return self._call_list(
            self.list_runs_fn,
            bq_store.list_runs,
            self.bq,
            project=self.project,
            dataset=self.dataset,
            limit=limit,
            include_archived=include_archived,
            archived_only=archived_only,
        )

    def get_events(self, run_id: str) -> list[RunEvent]:
        return self._call_list(
            self.get_events_fn,
            bq_store.get_events,
            run_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def update_run_status(self, run_id: str, status: Any, **kwargs: Any) -> dict[str, str]:
        return self._call_dict(
            self.update_run_status_fn,
            bq_store.update_run_status,
            run_id,
            status,
            self.bq,
            project=self.project,
            dataset=self.dataset,
            **kwargs,
        )

    def update_run_checkpoint(self, run_id: str, **kwargs: Any) -> dict[str, str]:
        return self._call_dict(
            self.update_run_checkpoint_fn,
            bq_store.update_run_checkpoint,
            run_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
            **kwargs,
        )

    def request_run_cancel(
        self,
        run_id: str,
        requested_by: str,
        target_status: str,
    ) -> bool:
        return bool(
            self._call(
                self.request_run_cancel_fn,
                bq_store.request_run_cancel,
                run_id,
                requested_by,
                target_status,
                self.bq,
                project=self.project,
                dataset=self.dataset,
            )
        )

    def archive_run(self, run_id: str, archived_by: str) -> None:
        self._call(
            self.archive_run_fn,
            bq_store.archive_run,
            run_id,
            archived_by,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def unarchive_run(self, run_id: str) -> None:
        self._call(
            self.unarchive_run_fn,
            bq_store.unarchive_run,
            run_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )


    def delete_archived_runs(self, older_than_days: int | str, run_ids: list[str] | None = None) -> dict[str, Any]:
        return self._call_dict(
            self.delete_archived_runs_fn,
            bq_store.delete_archived_runs,
            older_than_days,
            self.bq,
            project=self.project,
            dataset=self.dataset,
            run_ids=run_ids,
        )
    def list_cvs_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return self._call_list(
            self.list_cvs_for_run_fn,
            bq_store.list_cvs_for_run,
            run_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def get_cv_markdown(self, version_id: str) -> str | None:
        return self._call(
            self.get_cv_markdown_fn,
            bq_store.get_cv_markdown,
            version_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def list_run_structured_jobs(self, run_id: str) -> list[dict[str, Any]]:
        return self._call_list(
            self.list_run_structured_jobs_fn,
            bq_store.list_run_structured_jobs,
            run_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def list_filter_results_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return self._call_list(
            self.list_filter_results_for_run_fn,
            bq_store.list_filter_results_for_run,
            run_id,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def get_pipeline_runs_schema_status(self) -> dict[str, Any]:
        return self._call_dict(
            self.get_pipeline_runs_schema_status_fn,
            bq_store.get_pipeline_runs_schema_status,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def list_run_attempt_payloads(self, run_id: str) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for event in self.get_events(run_id):
            payload = decode_run_attempt_payload_or_none(event.payload_json)
            if payload is None:
                continue
            payloads.append(payload)
        return payloads

    def append_event(self, event: RunEvent) -> dict[str, str]:
        return dict(
            self._call(
                self.append_event_fn,
                bq_store.append_event,
                event,
                self.bq,
                project=self.project,
                dataset=self.dataset,
            )
        )

    def update_run_effective_settings(self, run_id: str, effective_settings_json: str) -> dict[str, str]:
        return self._call_dict(
            self.update_run_effective_settings_fn,
            bq_store.update_run_effective_settings,
            run_id,
            effective_settings_json,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def update_run_synonym_proposals(
        self, run_id: str, synonym_proposals_json: str
    ) -> dict[str, str]:
        return dict(
            self._call(
                self.update_run_synonym_proposals_fn,
                bq_store.update_run_synonym_proposals,
                run_id,
                synonym_proposals_json,
                self.bq,
                project=self.project,
                dataset=self.dataset,
            )
        )

    def update_run_cv_generation_debug(self, run_id: str, cv_generation_debug_json: str) -> dict[str, str]:
        return self._call_dict(
            self.update_run_cv_generation_debug_fn,
            bq_store.update_run_cv_generation_debug,
            run_id,
            cv_generation_debug_json,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def update_run_stage_transition_artifacts(
        self,
        run_id: str,
        stage_transition_artifacts_json: str,
    ) -> dict[str, str]:
        return self._call_dict(
            self.update_run_stage_transition_artifacts_fn,
            bq_store.update_run_stage_transition_artifacts,
            run_id,
            stage_transition_artifacts_json,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )

    def insert_cv_version_row(self, row: dict[str, Any]) -> list[Any]:
        return self._call_list(
            self.insert_cv_version_row_fn,
            bq_store.insert_cv_version_row,
            row,
            self.bq,
            project=self.project,
            dataset=self.dataset,
        )




