from datetime import date
from types import SimpleNamespace

import pytest

from orchestration.campaign import (
    CAMPAIGN_END_YEAR,
    CAMPAIGN_START_YEAR,
    CampaignQuarter,
    CampaignBlocked,
    CampaignSelection,
    build_quarter_request_specs,
    initialize_campaign_progress,
    iter_campaign_quarters,
    matching_campaign_progress_definition,
    select_earliest_incomplete_quarter,
)


def test_campaign_contains_all_configured_ordered_quarters() -> None:
    quarters = iter_campaign_quarters()
    expected_count = (CAMPAIGN_END_YEAR - CAMPAIGN_START_YEAR + 1) * 4

    assert len(quarters) == expected_count
    assert quarters[0] == CampaignQuarter(CAMPAIGN_START_YEAR, 1)
    assert quarters[-1] == CampaignQuarter(CAMPAIGN_END_YEAR, 4)
    assert quarters == tuple(sorted(quarters))


@pytest.mark.parametrize(
    ("quarter", "expected_start", "expected_end"),
    [
        (
            CampaignQuarter(CAMPAIGN_START_YEAR, 1),
            date(CAMPAIGN_START_YEAR, 1, 1),
            date(CAMPAIGN_START_YEAR, 3, 31),
        ),
        (
            CampaignQuarter(CAMPAIGN_START_YEAR, 2),
            date(CAMPAIGN_START_YEAR, 4, 1),
            date(CAMPAIGN_START_YEAR, 6, 30),
        ),
        (
            CampaignQuarter(CAMPAIGN_END_YEAR, 4),
            date(CAMPAIGN_END_YEAR, 10, 1),
            date(CAMPAIGN_END_YEAR, 12, 31),
        ),
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
        CampaignQuarter(CAMPAIGN_START_YEAR - 1, 4)
    with pytest.raises(ValueError, match="quarter"):
        CampaignQuarter(CAMPAIGN_START_YEAR, 5)


def test_quarter_request_specs_map_exactly_three_whole_months() -> None:
    specs = build_quarter_request_specs(
        CampaignQuarter(CAMPAIGN_START_YEAR, 1),
        current_local_date=date.max,
    )

    assert [(spec["start_date"], spec["end_date"]) for spec in specs] == [
        (
            f"{CAMPAIGN_START_YEAR}-01-01",
            f"{CAMPAIGN_START_YEAR}-01-31",
        ),
        (
            f"{CAMPAIGN_START_YEAR}-02-01",
            date(
                CAMPAIGN_START_YEAR,
                3,
                1,
            ).replace(day=1).fromordinal(
                date(CAMPAIGN_START_YEAR, 3, 1).toordinal() - 1
            ).isoformat(),
        ),
        (
            f"{CAMPAIGN_START_YEAR}-03-01",
            f"{CAMPAIGN_START_YEAR}-03-31",
        ),
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

    expected_count = (CAMPAIGN_END_YEAR - CAMPAIGN_START_YEAR + 1) * 4
    assert ids == tuple(range(1, expected_count + 1))
    assert len(definitions) == expected_count
    assert all(item.expected_partition_count == 3 for item in definitions)
    assert (definitions[0].start_date, definitions[0].end_date) == (
        f"{CAMPAIGN_START_YEAR}-01-01", f"{CAMPAIGN_START_YEAR}-03-31"
    )
    assert (definitions[-1].start_date, definitions[-1].end_date) == (
        f"{CAMPAIGN_END_YEAR}-10-01", f"{CAMPAIGN_END_YEAR}-12-31"
    )


@pytest.fixture
def progress_selection(monkeypatch):
    reconciled = []
    statuses = {}
    monkeypatch.setattr(
        "orchestration.campaign.initialize_campaign_progress",
        lambda engine, **kwargs: tuple(
            range(1, (CAMPAIGN_END_YEAR - CAMPAIGN_START_YEAR + 1) * 4 + 1)
        ),
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
            failed_partition_count=1 if statuses.get(progress_id) == "failed" else 0,
        ),
    )
    monkeypatch.setattr(
        "orchestration.repositories.pipeline_progress.try_claim_pipeline_progress",
        lambda engine, progress_id, owner: True,
    )
    monkeypatch.setattr(
        "orchestration.repositories.pipeline_progress.get_failed_progress_partitions",
        lambda engine, progress_id: (
            (
                date(CAMPAIGN_START_YEAR, 2, 1),
                date(CAMPAIGN_START_YEAR, 3, 1) - __import__("datetime").timedelta(days=1),
                "ValidationError",
            ),
        ),
    )
    return statuses, reconciled


def test_selection_starts_at_first_incomplete_quarter(progress_selection) -> None:
    _, reconciled = progress_selection
    selected = select_earliest_incomplete_quarter(
        object(), claim_owner="scheduled__one",
        current_local_date=date(2026, 9, 26)
    )

    assert isinstance(selected, CampaignSelection)
    assert selected.quarter == CampaignQuarter(CAMPAIGN_START_YEAR, 1)
    assert selected.progress_id == 1
    assert len(selected.request_specs) == 3
    assert reconciled == [1]


def test_selection_revisits_earliest_gap_after_missed_ticks(progress_selection) -> None:
    statuses, reconciled = progress_selection
    statuses.update({1: "complete", 2: "complete", 4: "complete"})

    first = select_earliest_incomplete_quarter(
        object(), claim_owner="scheduled__one",
        current_local_date=date(2026, 9, 26)
    )
    second = select_earliest_incomplete_quarter(
        object(), claim_owner="scheduled__one",
        current_local_date=date(2026, 9, 26)
    )

    assert first is not None and first.quarter == CampaignQuarter(CAMPAIGN_START_YEAR, 3)
    assert second is not None and second.quarter == first.quarter
    assert reconciled == [1, 2, 3, 1, 2, 3]


def test_selection_is_no_op_when_all_quarters_are_complete(progress_selection) -> None:
    statuses, reconciled = progress_selection
    quarter_count = (CAMPAIGN_END_YEAR - CAMPAIGN_START_YEAR + 1) * 4
    statuses.update({number: "complete" for number in range(1, quarter_count + 1)})

    assert select_earliest_incomplete_quarter(
        object(), claim_owner="scheduled__one",
        current_local_date=date(2026, 9, 26)
    ) is None
    assert reconciled == list(range(1, quarter_count + 1))


def test_selection_waits_for_whole_quarter_eligibility(progress_selection) -> None:
    _, reconciled = progress_selection

    assert select_earliest_incomplete_quarter(
        object(), claim_owner="scheduled__one",
        current_local_date=date(CAMPAIGN_START_YEAR, 4, 5)
    ) is None
    assert reconciled == []


def test_failed_quarter_blocks_automatic_resubmission(progress_selection) -> None:
    statuses, reconciled = progress_selection
    statuses[1] = "failed"

    selected = select_earliest_incomplete_quarter(
        object(), claim_owner="scheduled__next",
        current_local_date=date(2026, 9, 26)
    )
    assert isinstance(selected, CampaignBlocked)
    assert selected.quarter == CampaignQuarter(CAMPAIGN_START_YEAR, 1)
    assert f"{CAMPAIGN_START_YEAR}-02-01" in selected.reason
    assert "ValidationError" in selected.reason
    assert reconciled == [1]


def test_claimed_quarter_blocks_competing_selector(
    progress_selection, monkeypatch
) -> None:
    monkeypatch.setattr(
        "orchestration.repositories.pipeline_progress.try_claim_pipeline_progress",
        lambda engine, progress_id, owner: False,
    )
    selected = select_earliest_incomplete_quarter(
        object(), claim_owner="scheduled__competing",
        current_local_date=date(2026, 9, 26)
    )
    assert isinstance(selected, CampaignBlocked)
    assert selected.progress_id == 1
    assert "claimed" in selected.reason


def test_manual_whole_month_matches_exact_campaign_partition() -> None:
    from orchestration.requests import build_historical_request_specs
    from pipeline.request import build_request_key, weather_request_from_spec

    spec = build_historical_request_specs(
        f"{CAMPAIGN_START_YEAR}-02-01",
        (date(CAMPAIGN_START_YEAR, 3, 1) - __import__("datetime").timedelta(days=1)).isoformat(),
        current_local_date=date.max
    )[0]
    definition = matching_campaign_progress_definition(spec)

    assert definition is not None
    assert (definition.start_date, definition.end_date) == (
        f"{CAMPAIGN_START_YEAR}-01-01", f"{CAMPAIGN_START_YEAR}-03-31"
    )
    assert build_request_key(weather_request_from_spec(spec)) in {
        partition.request_key for partition in definition.partitions
    }


def test_partial_month_and_changed_configuration_do_not_repair_campaign() -> None:
    from orchestration.requests import build_historical_request_specs

    partial = build_historical_request_specs(
        f"{CAMPAIGN_START_YEAR}-02-10",
        (date(CAMPAIGN_START_YEAR, 3, 1) - __import__("datetime").timedelta(days=1)).isoformat(),
        current_local_date=date.max
    )[0]
    assert matching_campaign_progress_definition(partial) is None

    changed = build_historical_request_specs(
        "2024-02-01", "2024-02-29", current_local_date=date(2026, 9, 26)
    )[0]
    changed["bbox"]["north_lat"] = 5.2
    assert matching_campaign_progress_definition(changed) is None
