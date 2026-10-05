"""Handle generic CDS credentials, paced HTTP submission, and file retrieval."""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Callable
from pathlib import Path

import requests
import xarray as xr
from dotenv import load_dotenv

from pipeline.models.weather import WeatherRequest

logger = logging.getLogger("pipeline.extraction.cds")

# CDS API endpoint environment key.
CDSAPI_URL_ENV_KEY = "CDSAPI_URL"

# CDS API credential environment key.
CDSAPI_KEY_ENV_KEY = "CDSAPI_KEY"

# Default Copernicus Climate Data Store API endpoint.
DEFAULT_CDSAPI_URL = "https://cds.climate.copernicus.eu/api"


class _PacedSubmissionSession(requests.Session):
    """Reserve a submission slot before each provider POST, including retries."""

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


def _resolve_cds_credentials() -> tuple[str, str]:
    """Load the configured CDS endpoint and required API credential."""
    load_dotenv()
    cdsapi_url = os.getenv(CDSAPI_URL_ENV_KEY, DEFAULT_CDSAPI_URL).strip()
    cdsapi_key = os.getenv(CDSAPI_KEY_ENV_KEY, "").strip()
    if not cdsapi_key:
        raise ValueError(
            f"{CDSAPI_KEY_ENV_KEY} is missing. Set it in .env as "
            "CDSAPI_KEY=YOUR_UID:YOUR_API_KEY"
        )
    return cdsapi_url, cdsapi_key


def retrieve_cds_dataset(
    request: WeatherRequest,
    *,
    api_dataset: str,
    payload: dict[str, object],
    purpose: str,
    before_submission: Callable[[WeatherRequest], None] | None = None,
) -> xr.Dataset:
    """Retrieve one CDS chunk into memory and remove its temporary NetCDF file."""
    try:
        import cdsapi
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Missing dependency 'cdsapi'. Install it to use source='cds'."
        ) from error

    cdsapi_url, cdsapi_key = _resolve_cds_credentials()
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
        client.retrieve(api_dataset, payload, str(temp_path))
        with xr.open_dataset(temp_path) as opened_dataset:
            dataset = opened_dataset.load()
    finally:
        if temp_path.exists():
            temp_path.unlink()

    return dataset
