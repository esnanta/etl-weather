"""Define deterministic durable progress identities for orchestration batches."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

PIPELINE_NAME = "cds_era5_land_etl"


@dataclass(frozen=True)
class ProgressPartition:
    request_key: str
    start_date: str
    end_date: str


@dataclass(frozen=True)
class PipelineProgressDefinition:
    pipeline_name: str
    source: str
    dataset_name: str
    start_date: str
    end_date: str
    configuration_identity: str
    partitions: tuple[ProgressPartition, ...]

    @property
    def expected_partition_count(self) -> int:
        return len(self.partitions)


def build_pipeline_progress_definition(
    request_specs: Iterable[Mapping[str, Any]],
    *,
    pipeline_name: str = PIPELINE_NAME,
    identity_context: Mapping[str, Any] | None = None,
) -> PipelineProgressDefinition:
    """Build cadence-neutral durable progress identity from request partitions."""
    from pipeline.request import build_request_key, weather_request_from_spec

    if not pipeline_name.strip():
        raise ValueError("pipeline_name must not be empty.")

    requests = [weather_request_from_spec(spec) for spec in request_specs]
    if not requests:
        raise ValueError("Pipeline progress requires at least one request partition.")

    ordered = sorted(requests, key=lambda item: (item.start_date, item.end_date))
    first = ordered[0]
    source = first.source.value
    dataset_name = first.dataset
    variables = sorted(first.variables)
    bbox = {
        "north_lat": first.bbox.north_lat,
        "south_lat": first.bbox.south_lat,
        "east_lon": first.bbox.east_lon,
        "west_lon": first.bbox.west_lon,
    }

    for request in ordered[1:]:
        if request.source.value != source:
            raise ValueError("All progress partitions must use the same source.")
        if request.dataset != dataset_name:
            raise ValueError("All progress partitions must use the same dataset.")
        if sorted(request.variables) != variables:
            raise ValueError("All progress partitions must use the same variables.")
        request_bbox = {
            "north_lat": request.bbox.north_lat,
            "south_lat": request.bbox.south_lat,
            "east_lon": request.bbox.east_lon,
            "west_lon": request.bbox.west_lon,
        }
        if request_bbox != bbox:
            raise ValueError("All progress partitions must use the same bounding box.")

    partitions = tuple(
        ProgressPartition(
            request_key=build_request_key(request),
            start_date=request.start_date,
            end_date=request.end_date,
        )
        for request in ordered
    )
    payload = {
        "pipeline_name": pipeline_name.strip(),
        "source": source,
        "dataset_name": dataset_name,
        "start_date": partitions[0].start_date,
        "end_date": partitions[-1].end_date,
        "variables": variables,
        "bbox": bbox,
        "request_keys": [partition.request_key for partition in partitions],
    }
    if identity_context is not None:
        payload["identity_context"] = dict(identity_context)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    configuration_identity = hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    return PipelineProgressDefinition(
        pipeline_name=pipeline_name.strip(),
        source=source,
        dataset_name=dataset_name,
        start_date=partitions[0].start_date,
        end_date=partitions[-1].end_date,
        configuration_identity=configuration_identity,
        partitions=partitions,
    )
