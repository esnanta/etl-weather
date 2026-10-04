from datetime import date

import pytest

from orchestration.progress import build_pipeline_progress_definition
from orchestration.requests import build_historical_request_specs


def _specs(start_date: str, end_date: str) -> list[dict[str, object]]:
    return build_historical_request_specs(
        start_date,
        end_date,
        current_local_date=date(2026, 9, 25),
    )


def test_progress_definition_is_cadence_neutral() -> None:
    one_month = build_pipeline_progress_definition(_specs("2024-01-01", "2024-01-31"))
    two_months = build_pipeline_progress_definition(_specs("2024-01-01", "2024-02-29"))
    quarter = build_pipeline_progress_definition(_specs("2024-01-01", "2024-03-31"))

    assert one_month.expected_partition_count == 1
    assert two_months.expected_partition_count == 2
    assert quarter.expected_partition_count == 3
    assert one_month.start_date == "2024-01-01"
    assert quarter.end_date == "2024-03-31"
    assert len({
        one_month.configuration_identity,
        two_months.configuration_identity,
        quarter.configuration_identity,
    }) == 3


def test_progress_identity_is_stable_and_request_sensitive() -> None:
    specs = _specs("2024-01-01", "2024-03-31")
    first = build_pipeline_progress_definition(specs)
    second = build_pipeline_progress_definition(list(reversed(specs)))

    changed = [
        {**spec, "bbox": {**spec["bbox"], "north_lat": 6.0}}
        for spec in specs
    ]

    assert first.configuration_identity == second.configuration_identity
    assert first.configuration_identity != build_pipeline_progress_definition(
        changed
    ).configuration_identity
    assert [partition.start_date for partition in first.partitions] == [
        "2024-01-01",
        "2024-02-01",
        "2024-03-01",
    ]


def test_progress_identity_changes_with_context() -> None:
    specs = _specs("2024-01-01", "2024-01-31")

    first = build_pipeline_progress_definition(
        specs,
        identity_context={"campaign_key": "campaign-a"},
    )
    second = build_pipeline_progress_definition(
        specs,
        identity_context={"campaign_key": "campaign-b"},
    )

    assert first.configuration_identity != second.configuration_identity


def test_progress_definition_rejects_empty_or_mixed_configuration() -> None:
    with pytest.raises(ValueError, match="at least one"):
        build_pipeline_progress_definition([])

    specs = _specs("2024-01-01", "2024-02-29")
    mixed = [dict(spec) for spec in specs]
    mixed[1] = {
        **mixed[1],
        "bbox": {**mixed[1]["bbox"], "north_lat": 6.0},
    }
    with pytest.raises(ValueError, match="same bounding box"):
        build_pipeline_progress_definition(mixed)
