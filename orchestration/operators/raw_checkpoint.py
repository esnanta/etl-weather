"""Ensure mapped RAW checkpoints with worker-free waits between provider calls.

TaskFlow Python functions cannot defer in Airflow 3, so this operator resumes
the same checkpoint-backed monthly partition when PostgreSQL denies a slot.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.sdk import BaseOperator, Context

from orchestration.repositories.database import runtime_database_engine
from orchestration.repositories.pipeline_progress import are_extract_attempts_terminal
from orchestration.tasks.raw_checkpoint import ensure_raw_checkpoint_with_progress
from orchestration.scheduling.submission_gate import SubmissionDeferred


# Failed first-pass resolvers poll the durable barrier without occupying a worker.
FIRST_PASS_BARRIER_POLL_SECONDS = 30


def first_pass_barrier_complete(progress_id: int) -> bool:
    """Check whether every expected partition reached a terminal first-pass state."""
    with runtime_database_engine() as engine:
        return are_extract_attempts_terminal(engine, progress_id)


class RawCheckpointOperator(BaseOperator):
    """Ensure a RAW checkpoint while deferring provider-pacing waits."""

    template_fields = (
        "request_spec",
        "progress_id",
        "run_mode",
        "earliest_start_at",
        "first_result",
    )

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
        # Mapped TaskGroup inputs are references at DAG-parse time. Airflow
        # resolves these template fields for the concrete mapped task instance
        # before execute(), so request validation belongs at runtime.
        self.request_spec = request_spec
        self.progress_id = progress_id
        self.run_mode = run_mode
        self.earliest_start_at = earliest_start_at
        self.first_result = first_result
        self.first_pass = first_pass
        self.month_label: str | None = None

    def _resolve_month_label(self) -> str:
        if not isinstance(self.request_spec, dict):
            raise ValueError("Mapped RAW checkpoint requires resolved request_spec metadata.")
        start_date = self.request_spec.get("start_date")
        if not isinstance(start_date, str) or len(start_date) < 7:
            raise ValueError(
                "Mapped RAW checkpoint requires request_spec.start_date "
                "in YYYY-MM-DD format."
            )
        self.month_label = start_date[:7]
        return self.month_label

    def execute(self, context: Context) -> dict[str, object]:
        month_label = self._resolve_month_label()

        if self.first_result is not None:
            if not isinstance(self.first_result, dict):
                raise ValueError(
                    "RAW resolver requires resolved first-attempt control metadata."
                )
            if not self.first_result.get("first_attempt_failed"):
                return self.first_result
            if not first_pass_barrier_complete(self.progress_id):
                next_check = datetime.now(timezone.utc) + timedelta(
                    seconds=FIRST_PASS_BARRIER_POLL_SECONDS
                )
                self.log.info(
                    "RAW retry waiting for first-pass barrier; month=%s; "
                    "next_check_at=%s",
                    month_label,
                    next_check.isoformat(),
                )
                self.defer(
                    trigger=DateTimeTrigger(moment=next_check),
                    method_name="execute_complete",
                )
                raise AssertionError("Airflow defer unexpectedly returned.")

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
            return ensure_raw_checkpoint_with_progress(
                self.request_spec, self.progress_id, self.run_mode, paced=True
            )
        except SubmissionDeferred as deferred:
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
                "RAW checkpoint attempt failed; month=%s; error=%s",
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
