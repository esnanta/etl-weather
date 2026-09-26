from datetime import date
from types import SimpleNamespace

import pytest

from orchestration.campaign import (
    CAMPAIGN_END_YEAR,
    CAMPAIGN_START_YEAR,
    CampaignQuarter,
    CampaignSelection,
    build_quarter_request_specs,
    initialize_campaign_progress,
    iter_campaign_quarters,
    matching_campaign_progress_definition,
    select_earliest_incomplete_quarter,
)


def test_campaign_contains_exactly_forty_ordered_quarters() -> None:
    quarters = iter_campaign_quarters()

    assert len(quarters) == 40
    assert quarters[0] == CampaignQuarter(CAMPAIGN_START_YEAR, 1)
    assert quarters[-1] == CampaignQuarter(CAMPAIGN_END_YEAR, 4)
    assert quarters == tuple(sorted(quarters))


@pytest.mark.parametrize(
    ("quarter", "expected_start", "expected_end"),
    [
        (CampaignQuarter(2016, 1), date(2016, 1, 1), date(2016, 3, 31)),
        (CampaignQuarter(2016, 2), date(2016, 4, 1), date(2016, 6, 30)),
        (CampaignQuarter(2025, 4), date(2025, 10, 1), date(2025, 12, 31)),
    ],
)
def test_campaign_quarter_calendar_boundaries(
    quarter: CampaignQuarter,
    expected_start: date,
    expected_end: date,
) -> None:
    assert quarter.start_date == expected_start
    assert quarter.end_date == expected_end


def test_campaign_quarter_rejects_out_of_scope_values() -> None:
    with pytest.raises(ValueError, match="Campaign year"):
        CampaignQuarter(2015, 4)
    with pytest.raises(ValueError, match="quarter"):
        CampaignQuarter(2016, 5)


def test_quarter_request_specs_map_exactly_three_whole_months() -> None:
    specs = build_quarter_request_specs(
        CampaignQuarter(2024, 1),
        current_local_date=date(2026, 9, 25),
    )

    assert [(spec["start_date"], spec["end_date"]) for spec in specs] == [
        ("2024-01-01", "2024-01-31"),
        ("2024-02-01", "2024-02-29"),
        ("2024-03-01", "2024-03-31"),
    ]


def test_campaign_initialization_seeds_every_quarter(monkeypatch) -> None:
    definitions = []

    def record_progress(engine, definition):
        definitions.append(definition)
        return len(definitions)

    monkeypatch.setattr(
        "orchestration.repositories.pipeline_progress.initialize_pipeline_progress",
        record_progress,
    )
    ids = initialize_campaign_progress(
        object(), current_local_date=date(2026, 9, 26)
    )

    assert ids == tuple(range(1, 41))
    assert len(definitions) == 40
    assert all(item.expected_partition_count == 3 for item in definitions)
    assert (definitions[0].start_date, definitions[0].end_date) == (
        "2016-01-01", "2016-03-31"
    )
    assert (definitions[-1].start_date, definitions[-1].end_date) == (
        "2025-10-01", "2025-12-31"
    )


@pytest.fixture
def progress_selection(monkeypatch):
    reconciled = []
    statuses = {}
    monkeypatch.setattr(
        "orchestration.campaign.initialize_campaign_progress",
        lambda engine, **kwargs: tuple(range(1, 41)),
    )
    monkeypatch.setattr(
        "orchestration.repositories.pipeline_progress.reconcile_pipeline_progress",
        lambda engine, progress_id: reconciled.append(progress_id),
    )
    monkeypatch.setattr(
        "orchestration.repositories.pipeline_progress.get_pipeline_progress_by_id",
        lambda engine, progress_id: SimpleNamespace(
            execution_status=statuses.get(progress_id, "pending"),
            validation_status="valid" if statuses.get(progress_id) == "complete"
            else "pending",
        ),
    )
    return statuses, reconciled


def test_selection_starts_at_first_incomplete_quarter(progress_selection) -> None:
    _, reconciled = progress_selection
    selected = select_earliest_incomplete_quarter(
        object(), current_local_date=date(2026, 9, 26)
    )

    assert isinstance(selected, CampaignSelection)
    assert selected.quarter == CampaignQuarter(2016, 1)
    assert selected.progress_id == 1
    assert len(selected.request_specs) == 3
    assert reconciled == [1]


def test_selection_revisits_earliest_gap_after_missed_ticks(progress_selection) -> None:
    statuses, reconciled = progress_selection
    statuses.update({1: "complete", 2: "complete", 4: "complete"})

    first = select_earliest_incomplete_quarter(
        object(), current_local_date=date(2026, 9, 26)
    )
    second = select_earliest_incomplete_quarter(
        object(), current_local_date=date(2026, 9, 26)
    )

    assert first is not None and first.quarter == CampaignQuarter(2016, 3)
    assert second is not None and second.quarter == first.quarter
    assert reconciled == [1, 2, 3, 1, 2, 3]


def test_selection_is_no_op_when_all_quarters_are_complete(progress_selection) -> None:
    statuses, reconciled = progress_selection
    statuses.update({number: "complete" for number in range(1, 41)})

    assert select_earliest_incomplete_quarter(
        object(), current_local_date=date(2026, 9, 26)
    ) is None
    assert reconciled == list(range(1, 41))


def test_selection_waits_for_whole_quarter_eligibility(progress_selection) -> None:
    _, reconciled = progress_selection

    assert select_earliest_incomplete_quarter(
        object(), current_local_date=date(2016, 4, 5)
    ) is None
    assert reconciled == []


def test_manual_whole_month_matches_exact_campaign_partition() -> None:
    from orchestration.tasks.era5_land import build_historical_request_specs
    from pipeline.request import build_request_key, weather_request_from_spec

    spec = build_historical_request_specs(
        "2024-02-01", "2024-02-29", current_local_date=date(2026, 9, 26)
    )[0]
    definition = matching_campaign_progress_definition(spec)

    assert definition is not None
    assert (definition.start_date, definition.end_date) == (
        "2024-01-01", "2024-03-31"
    )
    assert build_request_key(weather_request_from_spec(spec)) in {
        partition.request_key for partition in definition.partitions
    }


def test_partial_month_and_changed_configuration_do_not_repair_campaign() -> None:
    from orchestration.tasks.era5_land import build_historical_request_specs

    partial = build_historical_request_specs(
        "2024-02-10", "2024-02-29", current_local_date=date(2026, 9, 26)
    )[0]
    assert matching_campaign_progress_definition(partial) is None

    changed = build_historical_request_specs(
        "2024-02-01", "2024-02-29", current_local_date=date(2026, 9, 26)
    )[0]
    changed["bbox"]["north_lat"] = 5.2
    assert matching_campaign_progress_definition(changed) is None
