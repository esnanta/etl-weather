"""Protect Plan C campaign identity and checkpoint separation across orchestration.

Tests exercise real request construction, progress definitions and campaign matching.
Only database side effects are isolated; PostgreSQL reconciliation/locking has its
own integration coverage, and Airflow DAG structure is tested separately.
"""

from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from config import CDS_VARIABLES, MONTHLY_SLOT_SPACING_MINUTES
from orchestration.campaign import repair as campaign_repair
from orchestration.campaign.policy import (
    CampaignQuarter,
    CampaignSelection,
    build_campaign_progress_definition,
    matching_campaign_progress_definition,
)
from orchestration.progress.definition import build_pipeline_progress_definition
from orchestration.scheduling.requests import build_historical_request_specs
from orchestration.tasks import build_monthly_requests as monthly_jobs
from pipeline.requests.weather import (
    build_request_key,
    build_variable_signature,
    weather_request_from_spec,
    weather_request_to_spec,
)
from pipeline.storage.artifacts import raw_artifact_for_request, transformed_artifact_for_request

OLD_VARIABLES = ("tp", "t2m", "d2m", "swvl1", "ssrd")
NINE_SIGNATURE = "d2m_ssrd_swvl1_swvl2_swvl3_t2m_tp_u10_v10"


def _specs(start: str = "2016-01-01", end: str = "2016-03-31"):
    return build_historical_request_specs(start, end, current_local_date=date.max)


def _old_spec(spec):
    request = weather_request_from_spec(spec)
    return weather_request_to_spec(replace(request, variables=list(OLD_VARIABLES)))


def test_monthly_request_factory_uses_exact_plan_c_variables() -> None:
    specs = _specs()
    assert len(specs) == 3
    assert len(CDS_VARIABLES) == 9
    for spec in specs:
        request = weather_request_from_spec(spec)
        assert request.variables == list(CDS_VARIABLES)
        assert build_variable_signature(request.variables) == NINE_SIGNATURE


def test_old_and_new_progress_and_partition_keys_are_disjoint() -> None:
    new_specs = _specs()
    old_specs = [_old_spec(spec) for spec in new_specs]
    new = build_campaign_progress_definition(new_specs)
    old = build_campaign_progress_definition(old_specs)
    reordered = [
        weather_request_to_spec(replace(
            weather_request_from_spec(spec),
            variables=list(reversed(CDS_VARIABLES)),
        ))
        for spec in new_specs
    ]

    assert new.configuration_identity != old.configuration_identity
    assert new.configuration_identity == build_campaign_progress_definition(
        list(reversed(reordered))
    ).configuration_identity
    assert new.expected_partition_count == old.expected_partition_count == 3
    assert not (
        {item.request_key for item in new.partitions}
        & {item.request_key for item in old.partitions}
    )


def test_mixed_five_and_nine_variable_progress_is_rejected() -> None:
    specs = _specs()
    mixed = [specs[0], _old_spec(specs[1]), specs[2]]
    with pytest.raises(ValueError, match="same variables"):
        build_pipeline_progress_definition(mixed)


def test_manual_repair_matches_only_active_nine_variable_campaign() -> None:
    current = _specs()[0]
    old = _old_spec(current)
    active_definition = matching_campaign_progress_definition(current)

    assert active_definition is not None
    active_key = build_request_key(weather_request_from_spec(current))
    old_key = build_request_key(weather_request_from_spec(old))
    assert active_key in {p.request_key for p in active_definition.partitions}
    assert old_key not in {p.request_key for p in active_definition.partitions}
    assert matching_campaign_progress_definition(old) is None


def test_legacy_manual_load_does_not_modify_active_campaign(monkeypatch) -> None:
    old = _old_spec(_specs()[0])

    def reject_database_access():
        raise AssertionError("Legacy manual repair must not open campaign DB")

    monkeypatch.setattr(
        campaign_repair, "runtime_database_engine", reject_database_access
    )
    campaign_repair.reconcile_campaign_repair(
        progress_id=99,
        request_spec=old,
        request_key=build_request_key(weather_request_from_spec(old)),
        transformed_path="legacy.parquet",
        transformed_sha256="legacy-sha",
        transformed_size=42,
        row_count=1,
        manual_repair=True,
    )


def test_old_artifact_cannot_be_resumed_as_new_month(tmp_path) -> None:
    new = weather_request_from_spec(_specs()[0])
    old = weather_request_from_spec(_old_spec(_specs()[0]))
    assert raw_artifact_for_request(new, data_directory=tmp_path).path != (
        raw_artifact_for_request(old, data_directory=tmp_path).path
    )
    assert transformed_artifact_for_request(new, data_directory=tmp_path).path != (
        transformed_artifact_for_request(old, data_directory=tmp_path).path
    )
    assert NINE_SIGNATURE in raw_artifact_for_request(new, data_directory=tmp_path).path.name


def test_scheduled_and_manual_jobs_share_active_variables_and_one_job_shape(
    monkeypatch,
) -> None:
    @contextmanager
    def fake_engine():
        yield object()

    monkeypatch.setattr(monthly_jobs, "runtime_database_engine", fake_engine)
    monkeypatch.setattr(monthly_jobs, "initialize_progress_storage", lambda engine: None)
    manual_definitions = []
    monkeypatch.setattr(
        monthly_jobs, "initialize_pipeline_progress",
        lambda engine, definition: manual_definitions.append(definition) or 17,
    )
    scheduled_specs = _specs()
    selected = CampaignSelection(
        quarter=CampaignQuarter(2016, 1),
        progress_id=23,
        request_specs=tuple(scheduled_specs),
    )
    monkeypatch.setattr(
        monthly_jobs, "select_earliest_incomplete_quarter",
        lambda engine, claim_owner: selected,
    )
    anchor = datetime(2026, 11, 4, 0, 0, tzinfo=UTC)
    manual = monthly_jobs.prepare_monthly_jobs(
        "manual",
        {"start_date": "2016-01-01", "end_date": "2016-03-31"},
        slot_anchor=anchor,
    )
    scheduled = monthly_jobs.prepare_monthly_jobs(
        "scheduled", {}, run_id="scheduled__plan_c", slot_anchor=anchor,
    )

    assert len(manual) == len(scheduled) == 3
    assert manual_definitions[0].expected_partition_count == 3
    assert {job["run_mode"] for job in manual} == {"manual"}
    assert {job["run_mode"] for job in scheduled} == {"scheduled"}
    assert all(job["earliest_start_at"] is None for job in manual)
    for index, (manual_job, scheduled_job) in enumerate(zip(manual, scheduled, strict=True)):
        assert set(manual_job) == set(scheduled_job)
        assert manual_job["month_label"] == scheduled_job["month_label"]
        assert manual_job["request_spec"] == scheduled_job["request_spec"]
        assert manual_job["progress_id"] == 17
        assert scheduled_job["progress_id"] == 23
        assert weather_request_from_spec(manual_job["request_spec"]).variables == list(CDS_VARIABLES)
        assert datetime.fromisoformat(scheduled_job["earliest_start_at"]) == (
            anchor + timedelta(minutes=index * MONTHLY_SLOT_SPACING_MINUTES)
        )


def test_boundary_rejects_legacy_result_key_for_nine_variable_request() -> None:
    from orchestration.progress.tracking import require_request_key

    current = weather_request_from_spec(_specs()[0])
    legacy = weather_request_from_spec(_old_spec(_specs()[0]))
    with pytest.raises(ValueError, match="does not match"):
        require_request_key(
            {"request_key": build_request_key(legacy)},
            build_request_key(current),
        )

