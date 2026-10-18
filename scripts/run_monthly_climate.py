"""Separate, opt-in monthly aggregation from persisted UTC+7 daily observations.

This experimental command is independent of daily ETL and Airflow. It never
retrieves CDS data. Run against an isolated database until integration-tested.
"""
from __future__ import annotations

import argparse
import calendar
import hashlib
import json
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from database.config import (
    APPLICATION_SCHEMA, MONTHLY_CLIMATE_TABLE, MONTHLY_CLIMATE_DETAIL_TABLE,
    resolve_database_config,
)
from pipeline.models.weather import BoundingBox, DataSource
from pipeline.repositories.daily_climate_reader import (
    DailyClimateSelection, load_daily_climate_for_modelling,
)

_FIELDS = {
    "tp_daily_total_mm": "tp_monthly_total_mm",
    "t2m_daily_mean_c": "t2m_monthly_mean_c",
    "d2m_daily_mean_c": "d2m_monthly_mean_c",
    "swvl1_daily_mean_m3_m3": "swvl1_monthly_mean_m3_m3",
    "ssrd_daily_total_j_m2": "ssrd_monthly_total_j_m2",
}


def aggregate_month(data: pd.DataFrame, start: date) -> pd.DataFrame:
    """Require all local days per grid point before aggregating non-null values."""
    days = calendar.monthrange(start.year, start.month)[1]
    expected = set(pd.date_range(start, periods=days).date)
    frame = data.copy()
    frame["local_day"] = pd.to_datetime(frame["valid_time_local"]).dt.date
    if frame.duplicated(["latitude", "longitude", "local_day"]).any():
        raise ValueError("Duplicate local day/grid observations.")
    rows = []
    for (lat, lon), group in frame.groupby(["latitude", "longitude"], sort=True):
        if set(group["local_day"]) != expected:
            raise ValueError(f"Incomplete local-day coverage for grid ({lat}, {lon}).")
        result = {"latitude": float(lat), "longitude": float(lon)}
        for source, target in _FIELDS.items():
            values = pd.to_numeric(group[source], errors="raise")
            if values.isna().any():
                result[target] = None
            elif source in {"tp_daily_total_mm", "ssrd_daily_total_j_m2"}:
                result[target] = float(values.sum())
            else:
                result[target] = float(values.mean())
        rows.append(result)
    if not rows:
        raise ValueError("No grid points available for monthly aggregation.")
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--dataset", default="reanalysis-era5-land")
    parser.add_argument("--north", type=float, default=6.1)
    parser.add_argument("--south", type=float, default=1.9)
    parser.add_argument("--west", type=float, default=94.9)
    parser.add_argument("--east", type=float, default=98.3)
    parser.add_argument("--execute", action="store_true", help="Write monthly rows; default is read-only preview")
    args = parser.parse_args()
    start = date.fromisoformat(args.month + "-01")
    end = date(start.year, start.month, calendar.monthrange(start.year, start.month)[1])
    bbox = BoundingBox(north_lat=args.north, south_lat=args.south,
                       east_lon=args.east, west_lon=args.west)
    config = resolve_database_config()
    selection = DailyClimateSelection(
        source=DataSource.CDS, dataset_name=args.dataset,
        start_date=start, end_date=end, bbox=bbox,
    )
    engine = create_engine(config.sqlalchemy_url, future=True)
    try:
        daily = load_daily_climate_for_modelling(selection, config, engine=engine)
        monthly = aggregate_month(daily, start)
        identity = dict(source=selection.source.value, dataset_name=args.dataset,
                        month_start=start.isoformat(), month_end=end.isoformat(),
                        north_lat=args.north, south_lat=args.south,
                        west_lon=args.west, east_lon=args.east,
                        variable_signature="d2m,ssrd,swvl1,t2m,tp")
        request_key = "monthly:" + hashlib.sha256(
            json.dumps(identity, sort_keys=True).encode()
        ).hexdigest()
        print(f"Monthly preview: {args.month}, grids={len(monthly)}, request_key={request_key}")
        if not args.execute:
            print("No database changes (preview only).")
            return
        parent = f"{APPLICATION_SCHEMA}.{MONTHLY_CLIMATE_TABLE}"
        detail = f"{APPLICATION_SCHEMA}.{MONTHLY_CLIMATE_DETAIL_TABLE}"
        with engine.begin() as conn:
            # Abort rather than overwrite previous monthly observations: the
            # first accepted write remains stable until an explicit refresh design.
            exists = conn.execute(text(f"SELECT id FROM {parent} WHERE request_key=:key"),
                                  {"key": request_key}).scalar_one_or_none()
            if exists is not None:
                raise ValueError("Monthly request already persisted; refusing overwrite.")
            inserted_id = conn.execute(text(f"""
                INSERT INTO {parent}
                (request_key, dataset_name, source, month_start, month_end,
                 variable_signature, north_lat, south_lat, east_lon, west_lon)
                VALUES (:request_key, :dataset_name,
                        CAST(:source AS {APPLICATION_SCHEMA}.data_source_enum),
                        :month_start, :month_end, :variable_signature,
                        :north_lat, :south_lat, :east_lon, :west_lon)
                RETURNING id
            """), dict(identity, request_key=request_key)).scalar_one()
            statement = text(f"""
                INSERT INTO {detail}
                (monthly_climate_id, month_start, latitude, longitude,
                 tp_monthly_total_mm, t2m_monthly_mean_c, d2m_monthly_mean_c,
                 swvl1_monthly_mean_m3_m3, ssrd_monthly_total_j_m2)
                VALUES (:monthly_climate_id, :month_start, :latitude, :longitude,
                        :tp_monthly_total_mm, :t2m_monthly_mean_c,
                        :d2m_monthly_mean_c, :swvl1_monthly_mean_m3_m3,
                        :ssrd_monthly_total_j_m2)
            """)
            records = monthly.where(pd.notnull(monthly), None).to_dict("records")
            conn.execute(statement, [dict(row, monthly_climate_id=inserted_id,
                                          month_start=start) for row in records])
        print(f"Committed {len(monthly)} monthly grid observations.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
