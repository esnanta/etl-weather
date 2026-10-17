"""Measure campaign host resources before enabling scheduled CDS submissions.

The historical campaign needs a repeatable, read-only snapshot of local storage,
memory, triggerer processes, and PostgreSQL size without printing credentials.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from shutil import disk_usage
import sys

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config import validate_and_log_app_environment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


def _disk(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"path": str(path), "error": "path does not exist"}
    usage = disk_usage(path)
    return {
        "path": str(path),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
    }


def _memory() -> dict[str, int]:
    fields = {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
    result = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, _, value = line.partition(":")
        if name in fields:
            result[name.lower() + "_bytes"] = int(value.split()[0]) * 1024
    return result


def _triggerers() -> dict[str, object]:
    processes = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").lower()
            if b"airflow" not in command or b"triggerer" not in command:
                continue
            status = (entry / "status").read_text()
            sizes = {}
            for line in status.splitlines():
                if line.startswith(("VmRSS:", "VmHWM:")):
                    name, value = line.split(":", 1)
                    sizes[name.lower() + "_bytes"] = int(value.split()[0]) * 1024
            processes.append({"pid": int(entry.name), **sizes})
        except (OSError, ValueError):
            continue
    return {"process_count": len(processes), "processes": processes,
            "note": "Process presence and RSS do not prove heartbeat or throughput."}


def _deferred_tasks(airflow_home: Path) -> dict[str, object]:
    url = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
    if url and not url.startswith("sqlite:"):
        return {"status": "not measured: Airflow metadata is not local SQLite"}
    path = Path(url.removeprefix("sqlite:///")) if url.startswith("sqlite:///") else airflow_home / "airflow.db"
    if not path.is_file():
        return {"status": "not measured: local Airflow metadata file is absent"}
    try:
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3)) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM task_instance WHERE state = 'deferred'"
            ).fetchone()[0]
        return {"deferred_task_instances": count}
    except sqlite3.Error as error:
        return {"status": f"not measured: {type(error).__name__}"}


def _postgres() -> dict[str, object]:
    try:
        from sqlalchemy import create_engine, text
        from database.config import (
            PACING_SCHEMA,
            PACING_TABLE,
            PIPELINE_PROGRESS_DETAIL_TABLE,
            PIPELINE_PROGRESS_SCHEMA,
            PIPELINE_PROGRESS_TABLE,
        )
        from database.config import (
            DAILY_CLIMATE_DETAIL_TABLE,
            DAILY_CLIMATE_TABLE,
            DatabaseConfig,
            SCHEMA_NAME,
        )

        engine = create_engine(DatabaseConfig.from_env().url, future=True,
                               connect_args={"connect_timeout": 5})
        try:
            with engine.connect() as connection:
                database_bytes = connection.execute(
                    text("SELECT pg_database_size(current_database())")
                ).scalar_one()
                tables = {}
                for table in (
                    f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_TABLE}",
                    f"{PIPELINE_PROGRESS_SCHEMA}.{PIPELINE_PROGRESS_DETAIL_TABLE}",
                    f"{PACING_SCHEMA}.{PACING_TABLE}",
                    f"{SCHEMA_NAME}.{DAILY_CLIMATE_TABLE}",
                    f"{SCHEMA_NAME}.{DAILY_CLIMATE_DETAIL_TABLE}",
                ):
                    size = connection.execute(
                        text("SELECT pg_total_relation_size(to_regclass(:name))"),
                        {"name": table},
                    ).scalar_one()
                    tables[table] = size
            return {"database_bytes": database_bytes, "table_bytes": tables,
                    "server_disk_free_bytes": None,
                    "note": "Measure PostgreSQL data and WAL filesystem free space on its host."}
        finally:
            engine.dispose()
    except Exception as error:
        return {"status": f"not measured: {type(error).__name__}"}


def main() -> None:
    try:
        validate_and_log_app_environment(logger)
    except ValueError:
        raise SystemExit(2) from None
    root = Path(__file__).resolve().parents[1]
    airflow_home = Path(os.getenv("AIRFLOW_HOME", root / "orchestration"))
    result = {
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "host": os.uname().nodename,
        "disks": {
            "artifacts": _disk(root / "data"),
            "temporary": _disk(Path(tempfile.gettempdir())),
            "airflow_logs": _disk(airflow_home / "logs"),
            "airflow_metadata": _disk(airflow_home),
        },
        "memory": _memory(),
        "cpu_count": os.cpu_count(),
        "load_average": os.getloadavg(),
        "triggerer": _triggerers(),
        "deferred_tasks": _deferred_tasks(airflow_home),
        "postgresql": _postgres(),
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
