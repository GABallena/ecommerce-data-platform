"""
monitoring — Pipeline monitoring, metrics persistence, and CLI dashboard.

Provides:
  6.1  Enhanced pipeline run history (per-task detail rows)
  6.2  Runtime metrics persistence (duration, attempts per task)
  6.3  Failure tracking (error messages, failure counts per task over time)
  6.4  Ingestion volume tracking (rows per source per entity per run)
  6.5  CLI dashboard (unified view of all monitoring data)
"""

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("monitoring")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "pipeline" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

PIPELINE_HISTORY = LOG_DIR / "pipeline_history.csv"
TASK_HISTORY = LOG_DIR / "task_history.csv"
INGESTION_LOG = LOG_DIR / "ingestion_log.csv"
DQ_HISTORY = LOG_DIR / "dq_reports" / "dq_history.csv"
FAILURE_LOG = LOG_DIR / "failure_log.csv"


class PipelineMonitor:
    """Collects metrics from a DAG run and persists them."""

    def __init__(self, pipeline_run_id: str | None = None):
        self.pipeline_run_id = pipeline_run_id or datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S"
        )

    def log_task_results(self, dag_id: str, results: dict):
        """Persist one row per task with full timing + status detail."""
        header = [
            "pipeline_run_id",
            "run_ts",
            "dag_id",
            "task_id",
            "status",
            "start_time",
            "end_time",
            "duration_seconds",
            "attempts",
            "error",
        ]
        write_header = not TASK_HISTORY.exists()

        with open(TASK_HISTORY, "a", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(header)
            for tid, r in results.items():
                writer.writerow(
                    [
                        self.pipeline_run_id,
                        datetime.now(timezone.utc).isoformat(),
                        dag_id,
                        tid,
                        r.status.value,
                        r.start_time.isoformat() if r.start_time else "",
                        r.end_time.isoformat() if r.end_time else "",
                        f"{r.duration_seconds:.3f}",
                        r.attempts,
                        r.error[:500] if r.error else "",
                    ]
                )

        logger.info(
            "Task history logged: %d tasks for run %s",
            len(results),
            self.pipeline_run_id,
        )

    def log_failures(self, dag_id: str, results: dict):
        """Log failed tasks to a dedicated failure log."""
        failed = {tid: r for tid, r in results.items() if not r.succeeded}
        if not failed:
            return

        header = [
            "pipeline_run_id",
            "failure_ts",
            "dag_id",
            "task_id",
            "status",
            "attempts",
            "error",
        ]
        write_header = not FAILURE_LOG.exists()

        with open(FAILURE_LOG, "a", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(header)
            for tid, r in failed.items():
                writer.writerow(
                    [
                        self.pipeline_run_id,
                        datetime.now(timezone.utc).isoformat(),
                        dag_id,
                        tid,
                        r.status.value,
                        r.attempts,
                        r.error[:500] if r.error else "",
                    ]
                )

        logger.warning(
            "Failures logged: %d task(s) failed in run %s",
            len(failed),
            self.pipeline_run_id,
        )

    def get_ingestion_volumes(self) -> list[dict]:
        """Read ingestion_log.csv and return volume summary per source/entity."""
        if not INGESTION_LOG.exists():
            return []

        volumes = []
        with open(INGESTION_LOG) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("status") == "success":
                    volumes.append(
                        {
                            "run_id": row.get("run_id", ""),
                            "source": row.get("source", ""),
                            "entity": row.get("entity", ""),
                            "rows": int(row.get("rows_ingested", 0)),
                            "duration": float(row.get("duration_seconds", 0)),
                            "timestamp": row.get("completed_at", ""),
                        }
                    )
        return volumes


def _read_csv(path: Path) -> list[dict]:
    """Read a CSV into a list of dicts (empty list if file missing)."""
    if not path.exists():
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def get_dashboard_data() -> dict[str, Any]:
    """Assemble all monitoring data for the dashboard."""
    data: dict[str, Any] = {}

    data["pipeline_runs"] = _read_csv(PIPELINE_HISTORY)

    data["task_history"] = _read_csv(TASK_HISTORY)

    data["failures"] = _read_csv(FAILURE_LOG)

    data["ingestion_volumes"] = _read_csv(INGESTION_LOG)

    data["dq_history"] = _read_csv(DQ_HISTORY)

    dq_dir = LOG_DIR / "dq_reports"
    if dq_dir.exists():
        jsons = sorted(dq_dir.glob("dq_report_*.json"))
        if jsons:
            with open(jsons[-1]) as f:
                data["latest_dq_report"] = json.load(f)
        else:
            data["latest_dq_report"] = None
    else:
        data["latest_dq_report"] = None

    return data
