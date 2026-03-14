#!/usr/bin/env python3
"""
run_pipeline.py — Full pipeline orchestration entry point.

Builds the DAG:  ingest → build_warehouse → data_quality_checks
Executes tasks in topological order with retries + exponential back-off.
Sends failure alerts via configured channels.

Usage:
    python pipeline/run_pipeline.py                  # single run (default)
    python pipeline/run_pipeline.py --schedule       # daily scheduler loop
    python pipeline/run_pipeline.py --schedule --hour 3 --minute 30
"""

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.orchestration.dag import DAG, Task  # noqa: E402
from pipeline.orchestration.alerts import send_alert, format_dag_failure_alert  # noqa: E402
from pipeline.orchestration.scheduler import DailyScheduler  # noqa: E402
from pipeline.monitoring import PipelineMonitor  # noqa: E402

LOG_DIR = PROJECT_ROOT / "pipeline" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "pipeline_run.log"),
    ],
)
logger = logging.getLogger("orchestration")

CONFIG_PATH = PROJECT_ROOT / "pipeline" / "configs" / "ingestion_config.json"
PIPELINE_HISTORY_LOG = LOG_DIR / "pipeline_history.csv"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)


def task_ingest(**kwargs):
    """Run the ingestion layer via run_ingestion.py as a subprocess."""
    _run_module("pipeline.run_ingestion", "ingestion")


def task_build_warehouse(**kwargs):
    """Run the warehouse build (Bronze → Silver → Gold) via run_warehouse.py."""
    _run_module("pipeline.run_warehouse", "warehouse")


def task_data_quality(**kwargs):
    """Run the full data quality system (workstream 5)."""
    from pipeline.quality import DataQualityEngine, write_report

    engine = DataQualityEngine()
    try:
        report = engine.run_all()
    finally:
        engine.close()
    write_report(report)
    logger.info("DQ: %d/%d checks passed", report.passed, report.total)
    if not report.all_passed:
        raise RuntimeError(
            f"Data quality: {report.failed} check(s) failed. "
            "See pipeline/logs/dq_reports/ for details."
        )


def _run_module(module: str, label: str):
    """Execute a Python module as subprocess so it gets a clean process + exit code."""
    cmd = [sys.executable, "-m", module]
    logger.info("Spawning: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            logger.info("[%s] %s", label, line)
    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            logger.error("[%s] %s", label, line)

    if result.returncode != 0:
        raise RuntimeError(
            f"{label} failed with exit code {result.returncode}. "
            f"See pipeline/logs/{label}_run.log for details."
        )


def _log_pipeline_run(dag_id: str, results: dict, elapsed: float):
    """Append one row per run to pipeline_history.csv."""
    import csv

    header = [
        "run_ts",
        "dag_id",
        "total_tasks",
        "succeeded",
        "failed",
        "elapsed_seconds",
    ]
    write_header = not PIPELINE_HISTORY_LOG.exists()

    succeeded = sum(1 for r in results.values() if r.succeeded)
    failed = len(results) - succeeded

    with open(PIPELINE_HISTORY_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerow(
            [
                datetime.now(timezone.utc).isoformat(),
                dag_id,
                len(results),
                succeeded,
                failed,
                f"{elapsed:.3f}",
            ]
        )


def build_dag() -> DAG:
    """
    Define the pipeline DAG:

        ingest  →  build_warehouse  →  data_quality
    """
    config = load_config()
    retry_cfg = config.get("retry", {})

    dag = DAG(dag_id="ecommerce_daily")

    dag.add_task(
        Task(
            task_id="ingest",
            callable=task_ingest,
            dependencies=[],
            max_retries=retry_cfg.get("max_retries", 3),
            retry_delay=retry_cfg.get("retry_delay", 2.0),
            retry_backoff=retry_cfg.get("retry_backoff", 2.0),
        )
    )

    dag.add_task(
        Task(
            task_id="build_warehouse",
            callable=task_build_warehouse,
            dependencies=["ingest"],
            max_retries=2,
            retry_delay=3.0,
            retry_backoff=2.0,
        )
    )

    dag.add_task(
        Task(
            task_id="data_quality",
            callable=task_data_quality,
            dependencies=["build_warehouse"],
            max_retries=1,
            retry_delay=1.0,
            retry_backoff=1.0,
        )
    )

    return dag


def run_once():
    """Execute the full pipeline DAG once."""
    monitor = PipelineMonitor()
    dag = build_dag()
    start = time.time()
    results = dag.run()
    elapsed = time.time() - start

    _log_pipeline_run(dag.dag_id, results, elapsed)
    monitor.log_task_results(dag.dag_id, results)
    monitor.log_failures(dag.dag_id, results)

    failed = {tid: r for tid, r in results.items() if not r.succeeded}
    if failed:
        subject, body = format_dag_failure_alert(dag.dag_id, results)
        send_alert(subject, body)
        logger.error("Pipeline finished with failures — alert dispatched")
        sys.exit(1)
    else:
        logger.info("Pipeline completed successfully in %.3fs", elapsed)


def main():
    parser = argparse.ArgumentParser(description="E-Commerce Pipeline Orchestrator")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Run in scheduler mode (daily at --hour:--minute UTC)",
    )
    parser.add_argument(
        "--hour", type=int, default=2, help="UTC hour for scheduled runs (default: 2)"
    )
    parser.add_argument(
        "--minute",
        type=int,
        default=0,
        help="UTC minute for scheduled runs (default: 0)",
    )
    args = parser.parse_args()

    if args.schedule:
        scheduler = DailyScheduler(
            run_fn=run_once,
            run_hour_utc=args.hour,
            run_minute_utc=args.minute,
        )
        scheduler.start()
    else:
        run_once()


if __name__ == "__main__":
    main()
