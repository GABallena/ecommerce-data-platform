#!/usr/bin/env python3

\
\
\
\
\
\
\

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.ingestion import (
    PostgresIngestor,
    PaymentAPIIngestor,
    CSVMarketingIngestor,
    InventoryIngestor,
)
from pipeline.ingestion.base_ingestor import IngestionError

LOG_DIR = PROJECT_ROOT / "pipeline" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "ingestion_run.log"),
    ],
)
logger = logging.getLogger("ingestion")

INGESTOR_MAP = {
    "postgres": PostgresIngestor,
    "payments": PaymentAPIIngestor,
    "marketing": CSVMarketingIngestor,
    "inventory": InventoryIngestor,
}

def validate_config(config: dict):
    \
    if "partition_date" not in config:
        raise IngestionError("Config missing required key: 'partition_date'")
    try:
        datetime.strptime(config["partition_date"], "%Y-%m-%d")
    except (ValueError, TypeError):
        raise IngestionError(
            f"Invalid partition_date format: '{config.get('partition_date')}' — expected YYYY-MM-DD"
        )
    if "sources" not in config or not config["sources"]:
        raise IngestionError("Config missing or empty 'sources'")

    errors = []
    for source_name, source_cfg in config["sources"].items():
        if source_name not in INGESTOR_MAP:
            errors.append(f"Unknown source '{source_name}' — no registered ingestor")
            continue
        entities = source_cfg.get("entities", {})
        if not entities:
            errors.append(f"Source '{source_name}' has no entities defined")
        for entity, filename in entities.items():
            fpath = PROJECT_ROOT / filename
            if not fpath.exists():
                errors.append(f"{source_name}/{entity}: file not found → {fpath}")
    if errors:
        for e in errors:
            logger.error("CONFIG VALIDATION: %s", e)
        raise IngestionError(
            f"{len(errors)} config validation error(s) — fix before running. "
            f"See log above for details."
        )
    logger.info(
        "Config validation passed (%d sources, %d entities)",
        len(config["sources"]),
        sum(len(s.get("entities", {})) for s in config["sources"].values()),
    )

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run ingestion pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to ingestion config JSON (default: pipeline/configs/ingestion_config.json)",
    )
    args = parser.parse_args()

    if args.config:
        config_path = Path(args.config)
    else:
        config_path = PROJECT_ROOT / "pipeline" / "configs" / "ingestion_config.json"

    if not config_path.exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)

    with open(config_path) as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON in config file: %s", exc)
            sys.exit(1)

    try:
        validate_config(config)
    except IngestionError as exc:
        logger.error("ABORTING: %s", exc)
        sys.exit(1)

    partition_date = config["partition_date"]
    retry_cfg = config.get("retry", {})
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    results: list[dict] = []
    success_count = 0
    fail_count = 0

    logger.info("=" * 60)
    logger.info("INGESTION RUN  run_id=%s  partition_date=%s", run_id, partition_date)
    logger.info("=" * 60)

    for source_name, source_cfg in config["sources"].items():
        ingestor_cls = INGESTOR_MAP.get(source_name)
        if not ingestor_cls:
            logger.error("No ingestor registered for source '%s'", source_name)
            continue

        ingestor = ingestor_cls(
            max_retries=retry_cfg.get("max_retries", 3),
            retry_delay=retry_cfg.get("retry_delay", 2.0),
            retry_backoff=retry_cfg.get("retry_backoff", 2.0),
        )

        for entity, filename in source_cfg["entities"].items():
            source_file = PROJECT_ROOT / filename
            result = ingestor.ingest(
                source_file=source_file,
                entity=entity,
                partition_date=partition_date,
                run_id=run_id,
            )
            results.append(result)
            if result["status"] == "success":
                success_count += 1
            else:
                fail_count += 1

    logger.info("=" * 60)
    logger.info(
        "INGESTION COMPLETE  success=%d  failed=%d  total=%d",
        success_count,
        fail_count,
        success_count + fail_count,
    )
    logger.info("=" * 60)

    for r in results:
        status_icon = "✓" if r["status"] == "success" else "✗"
        dur = r.get("duration_seconds", "")
        dur_str = f"  {dur}s" if dur else ""
        logger.info(
            "  %s  %s/%-25s  rows=%-5s  %s%s",
            status_icon,
            r["source"],
            r["entity"],
            r["rows_ingested"],
            r["raw_path"] or r.get("error_message", ""),
            dur_str,
        )

    if fail_count:
        logger.error(
            "PIPELINE FAILED — %d of %d entities failed. "
            "Check pipeline/logs/ingestion_log.csv for details.",
            fail_count,
            success_count + fail_count,
        )
        sys.exit(1)
    else:
        logger.info("ALL ENTITIES INGESTED SUCCESSFULLY")

if __name__ == "__main__":
    main()
