"""Run mapped CDS extraction with worker-free waits between provider calls.

TaskFlow Python functions cannot defer in Airflow 3, so this operator resumes
the same checkpoint-backed monthly partition when PostgreSQL denies a slot.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.sdk import BaseOperator, Context

from orchestration.tasks.extract import extract_with_progress
from pipeline.extraction.cds import CdsSubmissionDeferred


class CdsPacedExtractOperator(BaseOperator):
    """Defer without consuming an extraction worker or provider pool slot."""

    def __init__(
        self,
        *,
        request_spec: dict[str, object],
        progress_id: int,
        run_mode: str,
        earliest_start_at: str | None = None,
        first_result: dict[str, object] | None = None,
        first_pass: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.request_spec = request_spec
        start_date = request_spec.get("start_date")
        if not isinstance(start_date, str) or len(start_date) < 7:
            raise ValueError(
                "Mapped CDS extraction requires request_spec.start_date "
                "in YYYY-MM-DD format."
            )
        self.month_label = start_date[:7]
        self.progress_id = progress_id
        self.run_mode = run_mode
        self.earliest_start_at = earliest_start_at
        self.first_result = first_result
        self.first_pass = first_pass

    def execute(self, context: Context) -> dict[str, object]:
        if self.first_result is not None and not self.first_result.get("first_attempt_failed"):
            return self.first_result
        if self.earliest_start_at is not None:
            earliest = datetime.fromisoformat(self.earliest_start_at)
            if earliest.tzinfo is None:
                raise ValueError("Scheduled earliest start must be timezone-aware.")
            if datetime.now(timezone.utc) < earliest:
                self.defer(
                    trigger=DateTimeTrigger(moment=earliest.astimezone(timezone.utc)),
                    method_name="execute_complete",
                )
                raise AssertionError("Airflow defer unexpectedly returned.")
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
        except Exception as error:
            if not self.first_pass:
                raise
            self.log.warning(
                "First extraction attempt failed; month=%s; error=%s",
                self.request_spec.get("start_date"), type(error).__name__,
            )
            return {
                "first_attempt_failed": True,
                "request_spec": self.request_spec,
                "progress_id": self.progress_id,
                "run_mode": self.run_mode,
                "earliest_start_at": self.earliest_start_at,
            }

    def execute_complete(
        self, context: Context, event: Any = None
    ) -> dict[str, object]:
        """Recheck the database clock and resume saved chunks after wakeup."""
        return self.execute(context)

