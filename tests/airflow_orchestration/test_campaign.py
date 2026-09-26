from datetime import date

import pytest

from orchestration.campaign import (
    CAMPAIGN_END_YEAR,
    CAMPAIGN_START_YEAR,
    CampaignQuarter,
    build_quarter_request_specs,
    iter_campaign_quarters,
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
