"""Lightweight event reporter injected into run_pipeline() by the worker."""
import datetime
import json
import logging
import uuid
from typing import Any, Optional

from fitcv_cp.bq_store import append_event
from fitcv_cp.models import RunEvent

logger = logging.getLogger(__name__)


class PipelineReporter:
    def __init__(self, run_id: str, bq: Any, *, project: str, dataset: str) -> None:
        self._run_id = run_id
        self._bq = bq
        self._project = project
        self._dataset = dataset

    def emit(
        self,
        stage: str,
        level: str,
        message: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        if self._bq is None:
            return
        event = RunEvent(
            run_id=self._run_id,
            event_id=str(uuid.uuid4()),
            stage=stage,
            level=level,
            message=message,
            created_at=datetime.datetime.now(datetime.timezone.utc),
            payload_json=json.dumps(payload) if payload else None,
        )
        try:
            append_event(event, self._bq, project=self._project, dataset=self._dataset)
        except Exception as exc:
            logger.warning("Reporter failed to write event: %s", exc)
