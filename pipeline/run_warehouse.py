#!/usr/bin/env python3

\
\
\
\
\
\

import json
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.warehouse.engine import WarehouseEngine

LOG_DIR = PROJECT_ROOT / "pipeline" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "warehouse_run.log"),
    ],
)
logger = logging.getLogger("warehouse")

def main():
    config_path = PROJECT_ROOT / "pipeline" / "configs" / "ingestion_config.json"
    with open(config_path) as f:
        config = json.load(f)
    partition_date = config["partition_date"]

    logger.info("=" * 60)
    logger.info("WAREHOUSE BUILD  partition_date=%s", partition_date)
    logger.info("=" * 60)

    start = time.time()
    engine = WarehouseEngine()

    try:
        for layer in ("bronze", "silver", "gold"):
            engine.run_layer(layer, partition_date)

        logger.info("")
        logger.info("═" * 50)
        logger.info("TABLE STATISTICS")
        logger.info("═" * 50)

        total_tables = 0
        total_rows = 0
        for schema in ("bronze", "silver", "gold"):
            stats = engine.table_stats(schema)
            for s in stats:
                logger.info(
                    "  %-8s %-35s %d rows", s["schema"], s["table"], s["row_count"]
                )
                total_tables += 1
                total_rows += s["row_count"]

        elapsed = time.time() - start
        logger.info("")
        logger.info("═" * 50)
        logger.info(
            "WAREHOUSE BUILD COMPLETE  tables=%d  total_rows=%d  elapsed=%.3fs",
            total_tables,
            total_rows,
            elapsed,
        )
        logger.info("═" * 50)

        gold_tables = {s["table"] for s in engine.table_stats("gold")}
        expected = {
            "dim_date",
            "dim_customers",
            "dim_products",
            "fact_orders",
            "fact_order_items",
            "fact_payments",
            "fact_inventory_daily",
            "fact_campaign_performance",
            "fact_email_engagement",
            "fact_purchase_orders",
            "metric_customer_lifetime_value",
            "metric_order_conversion_rate",
            "metric_revenue_by_campaign",
            "metric_inventory_turnover",
        }
        missing = expected - gold_tables
        if missing:
            logger.error("MISSING GOLD TABLES: %s", missing)
            sys.exit(1)
        else:
            logger.info("ALL GOLD TABLES PRESENT ✓")

    finally:
        engine.close()

if __name__ == "__main__":
    main()
