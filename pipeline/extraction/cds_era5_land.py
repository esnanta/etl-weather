"""Retrieve ERA5-Land CDS chunks with local-day context and resumable cache."""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import xarray as xr

from pipeline.contracts.era5_land import (
    ERA5_LAND_START_DATE,
    LOCAL_DAY_CONTEXT_DAYS,
)
from pipeline.interfaces.etl import WeatherExtractor
from pipeline.observability.logging import log_metric
from pipeline.models.weather import RawWeatherDataset, WeatherRequest
from pipeline.providers.catalog import get_source_dataset_profile
from pipeline.providers.cds_client import retrieve_cds_dataset
from pipeline.providers.cds_era5_land import (
    VARIABLE_DESCRIPTIONS,
    build_cds_request_payload,
    normalize_cds_dataset,
)
from pipeline.requests.weather import (
    build_request,
    build_request_key,
    split_request_by_month,
    validate_request,
)

logger = logging.getLogger("pipeline.extraction.cds")


def _build_era5_land_retrieval_request(request: WeatherRequest) -> WeatherRequest:
    start = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    dataset_start = datetime.strptime(ERA5_LAND_START_DATE, "%Y-%m-%d").date()
    buffered_start = max(
        start - timedelta(days=LOCAL_DAY_CONTEXT_DAYS),
        dataset_start,
    )
    return replace(request, start_date=buffered_start.isoformat())


def _retrieval_chunk_purpose(
    chunk: WeatherRequest,
    target_request: WeatherRequest,
) -> str:
    """Describe whether a retrieval chunk provides context, target data, or both."""
    chunk_start = datetime.strptime(chunk.start_date, "%Y-%m-%d").date()
    chunk_end = datetime.strptime(chunk.end_date, "%Y-%m-%d").date()
    target_start = datetime.strptime(target_request.start_date, "%Y-%m-%d").date()

    if chunk_end < target_start:
        return "local-day context"
    if chunk_start < target_start:
        return "local-day context + target period"
    return "target period"


def _format_context_period(
    retrieval_request: WeatherRequest,
    target_request: WeatherRequest,
) -> str:
    """Return the portion of retrieval used only as local-day context."""
    retrieval_start = datetime.strptime(
        retrieval_request.start_date,
        "%Y-%m-%d",
    ).date()
    target_start = datetime.strptime(target_request.start_date, "%Y-%m-%d").date()
    if retrieval_start >= target_start:
        return "none"
    context_end = target_start - timedelta(days=1)
    return f"{retrieval_start.isoformat()} -> {context_end.isoformat()}"


def _download_cds(
    request: WeatherRequest,
    *,
    purpose: str,
    before_submission: Callable[[WeatherRequest], None] | None = None,
) -> xr.Dataset:
    """Retrieve and normalize one ERA5-Land CDS calendar-month chunk."""
    profile = get_source_dataset_profile(request.source, request.dataset)
    dataset = retrieve_cds_dataset(
        request,
        api_dataset=str(profile["api_dataset"]),
        payload=build_cds_request_payload(request),
        purpose=purpose,
        before_submission=before_submission,
    )
    return normalize_cds_dataset(dataset)


# Keep the previously proven five-variable CDS payload together; fetch the
# four Plan C additions separately to avoid the nine-variable monthly cost.
_BASE_RETRIEVAL_VARIABLES = frozenset({"tp", "t2m", "d2m", "swvl1", "ssrd"})
_MAX_VARIABLES_PER_CDS_REQUEST = 5


def _variable_retrieval_chunks(request: WeatherRequest) -> list[WeatherRequest]:
    """Split provider submissions, not the logical monthly weather partition."""
    if len(request.variables) <= _MAX_VARIABLES_PER_CDS_REQUEST:
        return [request]
    base = [name for name in request.variables if name in _BASE_RETRIEVAL_VARIABLES]
    additions = [name for name in request.variables if name not in _BASE_RETRIEVAL_VARIABLES]
    if len(base) > _MAX_VARIABLES_PER_CDS_REQUEST:
        raise ValueError("Base ERA5-Land retrieval group exceeds the provider limit.")
    groups = [base] if base else []
    groups.extend(
        additions[index:index + _MAX_VARIABLES_PER_CDS_REQUEST]
        for index in range(0, len(additions), _MAX_VARIABLES_PER_CDS_REQUEST)
    )
    return [replace(request, variables=group) for group in groups]


def _download_cds_period(
    request: WeatherRequest,
    *,
    target_request: WeatherRequest,
    before_submission: Callable[[WeatherRequest], None] | None = None,
    chunk_cache_directory: Path | None = None,
) -> xr.Dataset:
    """Retrieve resumable calendar/variable chunks and reconstruct one RAW dataset."""
    months = split_request_by_month(request)
    datasets = []
    for month in months:
        variable_datasets = []
        for chunk in _variable_retrieval_chunks(month):
            chunk_key = build_request_key(chunk)
            cache_path = (
                chunk_cache_directory / f"{chunk_key}.nc"
                if chunk_cache_directory is not None else None
            )
            if cache_path is not None and cache_path.exists():
                with xr.open_dataset(cache_path) as opened:
                    if opened.attrs.get("retrieval_request_key") != chunk_key:
                        raise ValueError("CDS chunk checkpoint identity does not match.")
                    dataset = opened.load()
                logger.info("Resumed CDS retrieval chunk from %s", cache_path)
            else:
                dataset = _download_cds(
                    chunk,
                    purpose=_retrieval_chunk_purpose(chunk, target_request),
                    before_submission=before_submission,
                )
                if cache_path is not None:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    with tempfile.NamedTemporaryFile(
                        dir=cache_path.parent,
                        prefix=".chunk-",
                        suffix=".nc",
                        delete=False,
                    ) as temporary:
                        temporary_path = Path(temporary.name)
                    try:
                        dataset.assign_attrs(
                            retrieval_request_key=chunk_key
                        ).to_netcdf(temporary_path)
                        os.replace(temporary_path, cache_path)
                    finally:
                        temporary_path.unlink(missing_ok=True)
            missing = set(chunk.variables) - set(dataset.data_vars)
            if missing:
                raise ValueError(
                    "CDS retrieval chunk lacks requested variables: "
                    + ", ".join(sorted(missing))
                )
            variable_datasets.append(dataset[chunk.variables])

        # Exact coordinate alignment prevents silently merging distinct grids
        # or hourly sequences from independently submitted variable groups.
        datasets.append(
            xr.merge(variable_datasets, join="exact", compat="no_conflicts")
            if len(variable_datasets) > 1 else variable_datasets[0]
        )

    if len(datasets) == 1:
        return datasets[0]

    combined = xr.concat(
        datasets,
        dim="valid_time",
        data_vars="minimal",
        coords="minimal",
        compat="override",
    ).sortby("valid_time")
    valid_time_index = combined.get_index("valid_time")
    if valid_time_index.has_duplicates:
        combined = combined.isel(
            valid_time=~valid_time_index.duplicated(keep="first")
        )
    return combined


class CdsEra5LandExtractor(WeatherExtractor):
    """Extract validated ERA5-Land provider variables through CDS."""

    def __init__(
        self,
        *,
        before_submission: Callable[[WeatherRequest], None] | None = None,
        chunk_cache_directory: Path | None = None,
    ) -> None:
        self.before_submission = before_submission
        self.chunk_cache_directory = chunk_cache_directory

    def extract(self, request: WeatherRequest) -> RawWeatherDataset:
        validate_request(request)
        normalized_request = build_request(request)
        retrieval_request = _build_era5_land_retrieval_request(normalized_request)

        retrieval_chunks = split_request_by_month(retrieval_request)
        log_metric(
            "CDS retrieval plan",
            {
                "target_period": (
                    f"{normalized_request.start_date} -> "
                    f"{normalized_request.end_date}"
                ),
                "retrieval_period": (
                    f"{retrieval_request.start_date} -> "
                    f"{retrieval_request.end_date}"
                ),
                "context_period": _format_context_period(
                    retrieval_request,
                    normalized_request,
                ),
                "download_chunks": sum(
                    len(_variable_retrieval_chunks(chunk)) for chunk in retrieval_chunks
                ),
                "calendar_chunks": len(retrieval_chunks),
            },
        )

        dataset = _download_cds_period(
            retrieval_request,
            target_request=normalized_request,
            before_submission=self.before_submission,
            chunk_cache_directory=self.chunk_cache_directory,
        )

        missing_variables = [
            variable
            for variable in normalized_request.variables
            if variable not in dataset.data_vars
        ]
        if missing_variables:
            available_variables = ", ".join(sorted(str(v) for v in dataset.data_vars))
            raise ValueError(
                "Requested CDS variables are missing from the downloaded dataset. "
                f"Missing: {', '.join(missing_variables)}. "
                f"Available: {available_variables}"
            )

        profile = get_source_dataset_profile(
            normalized_request.source,
            normalized_request.dataset,
        )
        log_metric(
            "CDS source profile",
            {
                "dataset_name": profile["dataset_name"],
                "data_reference": profile["data_reference"],
                "grid": profile["grid"],
                "horizontal_resolution": profile["horizontal_resolution"],
                "time_steps_utc": profile["time_steps_utc"],
                "levels": profile["levels"],
                "target_period": (
                    f"{normalized_request.start_date} -> "
                    f"{normalized_request.end_date}"
                ),
                "retrieved_period": (
                    f"{retrieval_request.start_date} -> "
                    f"{retrieval_request.end_date}"
                ),
                "requested_variables": ", ".join(normalized_request.variables),
                "available_variables": len(dataset.data_vars),
                "variable_descriptions": {
                    variable: VARIABLE_DESCRIPTIONS.get(variable, variable)
                    for variable in normalized_request.variables
                },
            },
        )

        dataset = dataset[normalized_request.variables]
        return RawWeatherDataset(
            request=normalized_request,
            downloaded_at=datetime.now(timezone.utc),
            raw_data=dataset,
        )
