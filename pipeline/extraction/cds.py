"""Retrieve ERA5-Land hourly chunks while preserving local-day context.

Orchestrated downloads need resumable chunks so provider pacing can release a
worker between submissions without repeating an already completed request.
"""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import xarray as xr
from dotenv import load_dotenv

from pipeline.contracts.era5_land import (
    ERA5_LAND_START_DATE,
    LOCAL_DAY_CONTEXT_DAYS,
)
from pipeline.interfaces import WeatherExtractor
from pipeline.logging import log_metric
from pipeline.model import RawWeatherDataset, WeatherRequest
from pipeline.providers.cds import (
    CDS_API_TO_PROVIDER_VARIABLES,
    CDS_API_VARIABLE_NAMES,
    VARIABLE_DESCRIPTIONS,
    get_source_dataset_profile,
)
from pipeline.request import (
    build_request, build_request_key, split_request_by_month, validate_request,
)

logger = logging.getLogger(__name__)

CDSAPI_URL_ENV_KEY = "CDSAPI_URL"
CDSAPI_KEY_ENV_KEY = "CDSAPI_KEY"
DEFAULT_CDSAPI_URL = "https://cds.climate.copernicus.eu/api"


class _PacedSubmissionSession(requests.Session):
    """Reserve a durable slot for each provider POST, including library retries."""

    def __init__(
        self,
        request: WeatherRequest,
        before_submission: Callable[[WeatherRequest], None],
    ) -> None:
        super().__init__()
        self._chunk_request = request
        self._before_submission = before_submission

    def request(self, method: str, url: str, *args, **kwargs):
        if method.upper() == "POST":
            self._before_submission(self._chunk_request)
        return super().request(method, url, *args, **kwargs)


class CdsSubmissionDeferred(Exception):
    """Signal that a provider call should resume after a durable pacing gate."""

    def __init__(self, next_allowed_at: datetime):
        self.next_allowed_at = next_allowed_at
        super().__init__(f"CDS submission deferred until {next_allowed_at.isoformat()}")

def _normalize_cds_dataset(dataset: xr.Dataset) -> xr.Dataset:
    """Normalize schema mechanics while preserving provider variable names."""
    if "time" in dataset.coords and "valid_time" not in dataset.coords:
        dataset = dataset.rename({"time": "valid_time"})

    rename_map = {
        api_name: provider_name
        for api_name, provider_name in CDS_API_TO_PROVIDER_VARIABLES.items()
        if api_name in dataset.data_vars and provider_name not in dataset.data_vars
    }
    if rename_map:
        dataset = dataset.rename(rename_map)
    return dataset


def _resolve_cds_credentials() -> tuple[str, str]:
    load_dotenv()
    cdsapi_url = os.getenv(CDSAPI_URL_ENV_KEY, DEFAULT_CDSAPI_URL).strip()
    cdsapi_key = os.getenv(CDSAPI_KEY_ENV_KEY, "").strip()
    if not cdsapi_key:
        raise ValueError(
            f"{CDSAPI_KEY_ENV_KEY} is missing. Set it in .env as "
            "CDSAPI_KEY=YOUR_UID:YOUR_API_KEY"
        )
    return cdsapi_url, cdsapi_key


def _build_cds_request_payload(request: WeatherRequest) -> dict[str, object]:
    start = datetime.strptime(request.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(request.end_date, "%Y-%m-%d").date()
    if start.month != end.month or start.year != end.year:
        raise ValueError(
            "CDS request interval must stay in one month. "
            "Use split_request_by_month before extraction."
        )

    request_days: list[str] = []
    current_day = start
    while current_day <= end:
        request_days.append(f"{current_day.day:02d}")
        current_day += timedelta(days=1)

    profile = get_source_dataset_profile(request.source, request.dataset)
    api_variables = [
        CDS_API_VARIABLE_NAMES.get(variable, variable)
        for variable in request.variables
    ]

    payload: dict[str, object] = {
        "variable": api_variables,
        "year": f"{start.year}",
        "month": f"{start.month:02d}",
        "day": request_days,
        "time": [f"{hour:02d}:00" for hour in range(24)],
        "area": [
            request.bbox.north_lat,
            request.bbox.west_lon,
            request.bbox.south_lat,
            request.bbox.east_lon,
        ],
        "data_format": "netcdf",
        "download_format": "unarchived",
    }
    if profile.get("requires_product_type", False):
        payload["product_type"] = "reanalysis"
    return payload


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
    try:
        import cdsapi
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Missing dependency 'cdsapi'. Install it to use source='cds'."
        ) from error

    cdsapi_url, cdsapi_key = _resolve_cds_credentials()
    profile = get_source_dataset_profile(request.source, request.dataset)
    payload = _build_cds_request_payload(request)
    client_options = {"url": cdsapi_url, "key": cdsapi_key, "quiet": True}
    if before_submission is not None:
        client_options["session"] = _PacedSubmissionSession(request, before_submission)
    client = cdsapi.Client(**client_options)

    temp_path = Path(tempfile.gettempdir()) / (
        f"cds_{request.start_date}_{request.end_date}_"
        f"{'_'.join(request.variables)}.nc"
    )
    if temp_path.exists():
        temp_path.unlink()

    logger.info(
        "Downloading CDS dataset from Copernicus API | "
        "chunk_period=%s -> %s | purpose=%s.",
        request.start_date,
        request.end_date,
        purpose,
    )
    try:
        client.retrieve(profile["api_dataset"], payload, str(temp_path))
        with xr.open_dataset(temp_path) as opened_dataset:
            dataset = opened_dataset.load()
    finally:
        if temp_path.exists():
            temp_path.unlink()

    return _normalize_cds_dataset(dataset)


def _download_cds_period(
    request: WeatherRequest,
    *,
    target_request: WeatherRequest,
    before_submission: Callable[[WeatherRequest], None] | None = None,
    chunk_cache_directory: Path | None = None,
) -> xr.Dataset:
    chunks = split_request_by_month(request)
    datasets = []
    for chunk in chunks:
        cache_path = (
            chunk_cache_directory / f"{build_request_key(chunk)}.nc"
            if chunk_cache_directory is not None else None
        )
        if cache_path is not None and cache_path.exists():
            with xr.open_dataset(cache_path) as opened:
                if opened.attrs.get("retrieval_request_key") != build_request_key(chunk):
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
                    dir=cache_path.parent, prefix=".chunk-", suffix=".nc",
                    delete=False,
                ) as temporary:
                    temporary_path = Path(temporary.name)
                try:
                    dataset.assign_attrs(
                        retrieval_request_key=build_request_key(chunk)
                    ).to_netcdf(temporary_path)
                    os.replace(temporary_path, cache_path)
                finally:
                    temporary_path.unlink(missing_ok=True)
        datasets.append(dataset)
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


class CdsExtractor(WeatherExtractor):
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
        retrieval_request = _build_era5_land_retrieval_request(
            normalized_request
        )

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
                "download_chunks": len(retrieval_chunks),
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
