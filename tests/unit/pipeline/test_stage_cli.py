"""Verify independent daily ETL CLIs and runtime stage dispatch.

The production CLI main functions and runtime batch wrappers are under test.
Environment, CDS request factory, and per-partition downstream operations are
mocked because these tests verify routing, not external CDS or PostgreSQL I/O.
Checkpoint validation and idempotent persistence are covered by their existing
storage, runtime, and isolated PostgreSQL integration suites.
"""

from __future__ import annotations

import importlib

import pytest

from pipeline.models.weather import BoundingBox, DataSource, WeatherRequest
from pipeline import runtime


def _request(start: str = "2024-01-15", end: str = "2024-03-10") -> WeatherRequest:
    return WeatherRequest(
        source=DataSource.CDS,
        dataset="reanalysis-era5-land",
        start_date=start,
        end_date=end,
        variables=["tp"],
        bbox=BoundingBox(
            north_lat=5.0,
            west_lon=95.0,
            south_lat=5.0,
            east_lon=95.0,
        ),
    )


@pytest.mark.parametrize(
    ("module_name", "function_name", "requires_api_key"),
    [
        ("scripts.run_extract", "extract_monthly_batch", True),
        ("scripts.run_transform", "transform_monthly_batch", False),
        ("scripts.run_load", "load_monthly_batch", False),
    ],
)
def test_independent_cli_calls_only_its_own_batch(
    monkeypatch, module_name, function_name, requires_api_key
) -> None:
    module = importlib.import_module(module_name)
    request = _request()
    calls = []
    monkeypatch.setattr(module, "validate_and_log_app_environment", lambda logger: None)

    def build_request(*, require_api_key: bool):
        assert require_api_key is requires_api_key
        return request

    monkeypatch.setattr(module, "build_cds_request", build_request)
    monkeypatch.setattr(module, function_name, lambda value: calls.append(value))

    module.main()

    assert calls == [request]


@pytest.mark.parametrize(
    ("module_name", "function_name"),
    [
        ("scripts.run_extract", "extract_monthly_batch"),
        ("scripts.run_transform", "transform_monthly_batch"),
        ("scripts.run_load", "load_monthly_batch"),
    ],
)
def test_independent_cli_stops_after_environment_error(
    monkeypatch, module_name, function_name
) -> None:
    module = importlib.import_module(module_name)
    monkeypatch.setattr(
        module, "validate_and_log_app_environment",
        lambda logger: (_ for _ in ()).throw(ValueError("invalid environment")),
    )
    monkeypatch.setattr(
        module, "build_cds_request",
        lambda **kwargs: pytest.fail("request created after invalid environment"),
    )
    monkeypatch.setattr(
        module, function_name,
        lambda request: pytest.fail("stage ran after invalid environment"),
    )
    with pytest.raises(SystemExit) as exc:
        module.main()
    assert exc.value.code == 2


@pytest.mark.parametrize(
    ("batch_name", "boundary_name"),
    [
        ("extract_monthly_batch", "ensure_raw_checkpoint_partition"),
        ("transform_monthly_batch", "ensure_transformed_checkpoint_partition"),
        ("load_monthly_batch", "load_request_partition"),
    ],
)
def test_stage_batch_uses_only_requested_boundary_and_monthly_partitions(
    monkeypatch, batch_name, boundary_name
) -> None:
    seen = []
    request = _request()
    def execute(partition):
        seen.append((partition.start_date, partition.end_date))
        return {
            "request_key": "fixture",
            "artifact_path": "fixture",
            "artifact_status": "resumed",
            "load_status": "skipped",
        }
    monkeypatch.setattr(runtime, boundary_name, execute)
    getattr(runtime, batch_name)(request)

    assert seen == [
        ("2024-01-15", "2024-01-31"),
        ("2024-02-01", "2024-02-29"),
        ("2024-03-01", "2024-03-10"),
    ]


@pytest.mark.parametrize(
    ("batch_name", "boundary_name"),
    [
        ("extract_monthly_batch", "ensure_raw_checkpoint_partition"),
        ("transform_monthly_batch", "ensure_transformed_checkpoint_partition"),
        ("load_monthly_batch", "load_request_partition"),
    ],
)
def test_stage_batch_propagates_failure_without_running_later_partitions(
    monkeypatch, batch_name, boundary_name
) -> None:
    seen = []
    def execute(partition):
        seen.append(partition.start_date)
        if len(seen) == 2:
            raise FileNotFoundError("checkpoint missing")
        return {
            "request_key": "fixture",
            "artifact_path": "fixture",
            "artifact_status": "resumed",
        }
    monkeypatch.setattr(runtime, boundary_name, execute)

    with pytest.raises(FileNotFoundError, match="checkpoint missing"):
        getattr(runtime, batch_name)(_request())

    assert seen == ["2024-01-15", "2024-02-01"]
