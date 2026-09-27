"""Run mapped CDS extraction with worker-free waits between provider calls.

TaskFlow Python functions cannot defer in Airflow 3, so this operator resumes
the same checkpoint-backed monthly partition when PostgreSQL denies a slot.
"""

from __future__ import annotations

from datetime import timezone
from typing import Any

from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.sdk import BaseOperator, Context

from orchestration.tasks.progress import extract_with_progress
from pipeline.extraction.cds import CdsSubmissionDeferred


class CdsPacedExtractOperator(BaseOperator):
    """Defer without consuming an extraction worker or provider pool slot."""

    def __init__(
        self,
        *,
        request_spec: dict[str, object],
        progress_id: int,
        run_mode: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.request_spec = request_spec
        self.progress_id = progress_id
        self.run_mode = run_mode

    def execute(self, context: Context) -> dict[str, object]:
        try:
            return extract_with_progress(
                self.request_spec, self.progress_id, self.run_mode, paced=True
            )
        except CdsSubmissionDeferred as deferred:
            self.defer(
                trigger=DateTimeTrigger(
                    moment=deferred.next_allowed_at.astimezone(timezone.utc)
                ),
                method_name="execute_complete",
            )
            raise AssertionError("Airflow defer unexpectedly returned.")

    def execute_complete(
        self, context: Context, event: Any = None
    ) -> dict[str, object]:
        """Recheck the database clock and resume saved chunks after wakeup."""
        return self.execute(context)
