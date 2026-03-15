\
\
\
\
\
\
\
\
\
\
\
\
\

import csv
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger("data_quality")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = PROJECT_ROOT / "pipeline" / "warehouse" / "warehouse.duckdb"
REPORT_DIR = PROJECT_ROOT / "pipeline" / "logs" / "dq_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

@dataclass
class CheckResult:
    check_id: str
    category: str
    table: str
    description: str
    passed: bool
    failing_rows: int = 0
    details: str = ""

@dataclass
class DQReport:
    run_id: str
    run_ts: str
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.checks)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.passed)

    @property
    def failed(self) -> int:
        return self.total - self.passed

    @property
    def all_passed(self) -> bool:
        return self.failed == 0

    def summary_text(self) -> str:
        lines = [
            f"DATA QUALITY REPORT  run_id={self.run_id}  ts={self.run_ts}",
            f"  Total checks: {self.total}   Passed: {self.passed}   Failed: {self.failed}",
            "",
        ]
        for c in self.checks:
            icon = "✓" if c.passed else "✗"
            fail_info = f"  ({c.failing_rows} rows)" if not c.passed else ""
            lines.append(
                f"  {icon}  [{c.category:12s}] {c.check_id:45s} {c.description}{fail_info}"
            )
            if c.details and not c.passed:
                lines.append(f"     → {c.details}")
        return "\n".join(lines)

class DataQualityEngine:
\

    def __init__(self, db_path: Path = DB_PATH):
        self.con = duckdb.connect(str(db_path), read_only=True)

    def close(self):
        self.con.close()

    def run_all(self, run_id: str | None = None) -> DQReport:
        \
        if not run_id:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

        report = DQReport(
            run_id=run_id,
            run_ts=datetime.now(timezone.utc).isoformat(),
        )

        start = time.time()
        logger.info("=" * 60)
        logger.info("DATA QUALITY RUN  run_id=%s", run_id)
        logger.info("=" * 60)

        self._check_nulls(report)
        self._check_schema(report)
        self._check_duplicates(report)
        self._check_referential_integrity(report)
        self._check_currency_consistency(report)
        self._check_arithmetic(report)

        elapsed = time.time() - start
        logger.info("")
        logger.info(
            "DQ COMPLETE  passed=%d  failed=%d  total=%d  elapsed=%.3fs",
            report.passed,
            report.failed,
            report.total,
            elapsed,
        )
        return report

    def _check_nulls(self, report: DQReport):
        logger.info("─ 5.1  Null checks ─")
        required_columns = {
            "gold.dim_customers": [
                "customer_sk",
                "customer_id",
                "canonical_customer_id",
                "email",
            ],
            "gold.dim_products": ["product_sk", "product_id", "sku", "product_name"],
            "gold.dim_date": ["date_key", "full_date"],
            "gold.fact_orders": [
                "order_sk",
                "order_id",
                "customer_sk",
                "order_date_key",
                "order_status",
                "currency",
                "total_amount",
            ],
            "gold.fact_order_items": [
                "order_item_sk",
                "order_item_id",
                "order_sk",
                "product_sk",
                "quantity",
                "unit_price",
            ],
            "gold.fact_payments": [
                "payment_sk",
                "transaction_id",
                "order_sk",
                "payment_method",
                "status",
                "currency",
                "gross_amount",
            ],
            "gold.fact_inventory_daily": [
                "inventory_sk",
                "snapshot_date_key",
                "product_sk",
                "warehouse_id",
                "on_hand_qty",
            ],
            "gold.fact_campaign_performance": [
                "campaign_perf_sk",
                "campaign_date_key",
                "platform",
                "campaign_id",
                "impressions",
            ],
            "gold.fact_email_engagement": [
                "email_sk",
                "send_date_key",
                "customer_sk",
                "delivery_status",
            ],
            "gold.fact_purchase_orders": [
                "po_sk",
                "po_id",
                "product_sk",
                "ordered_qty",
            ],
            "silver.stg_orders": ["order_id", "customer_id", "order_date", "currency"],
            "silver.stg_order_items": ["order_item_id", "order_id", "product_id"],
            "silver.stg_customers": ["customer_id", "email"],
            "silver.stg_products": ["product_id", "sku"],
            "silver.stg_payment_transactions": [
                "transaction_id",
                "order_id",
                "currency",
            ],
        }

        for table, columns in required_columns.items():
            for col in columns:
                check_id = f"null_{table}.{col}"
                cnt = self._query_scalar(
                    f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NULL'
                )
                report.checks.append(
                    CheckResult(
                        check_id=check_id,
                        category="null",
                        table=table,
                        description=f"{col} NOT NULL",
                        passed=cnt == 0,
                        failing_rows=cnt,
                    )
                )

    def _check_schema(self, report: DQReport):
        logger.info("─ 5.2  Schema checks ─")

        expected_schemas: dict[str, list[tuple[str, str]]] = {
            "gold.dim_customers": [
                ("customer_sk", "INTEGER"),
                ("customer_id", "INTEGER"),
                ("canonical_customer_id", "INTEGER"),
                ("email", "VARCHAR"),
            ],
            "gold.dim_products": [
                ("product_sk", "INTEGER"),
                ("product_id", "INTEGER"),
                ("sku", "VARCHAR"),
                ("product_name", "VARCHAR"),
            ],
            "gold.fact_orders": [
                ("order_sk", "INTEGER"),
                ("order_id", "INTEGER"),
                ("customer_sk", "INTEGER"),
                ("order_date_key", "INTEGER"),
                ("currency", "VARCHAR"),
                ("total_amount", "DECIMAL(18,2)"),
                ("total_amount_usd", "DECIMAL(18,2)"),
            ],
            "gold.fact_order_items": [
                ("order_item_sk", "INTEGER"),
                ("order_sk", "INTEGER"),
                ("product_sk", "INTEGER"),
                ("quantity", "INTEGER"),
                ("unit_price", "DECIMAL(18,2)"),
                ("line_total", "DECIMAL(18,2)"),
            ],
            "gold.fact_payments": [
                ("payment_sk", "INTEGER"),
                ("transaction_id", "VARCHAR"),
                ("order_sk", "INTEGER"),
                ("currency", "VARCHAR"),
                ("gross_amount", "DECIMAL(18,2)"),
            ],
            "gold.fact_inventory_daily": [
                ("inventory_sk", "INTEGER"),
                ("snapshot_date_key", "INTEGER"),
                ("product_sk", "INTEGER"),
                ("on_hand_qty", "INTEGER"),
            ],
            "gold.fact_campaign_performance": [
                ("campaign_perf_sk", "INTEGER"),
                ("campaign_date_key", "INTEGER"),
                ("platform", "VARCHAR"),
                ("impressions", "INTEGER"),
            ],
            "gold.fact_email_engagement": [
                ("email_sk", "INTEGER"),
                ("send_date_key", "INTEGER"),
                ("customer_sk", "INTEGER"),
                ("delivery_status", "VARCHAR"),
            ],
            "gold.fact_purchase_orders": [
                ("po_sk", "INTEGER"),
                ("po_id", "INTEGER"),
                ("product_sk", "INTEGER"),
                ("ordered_qty", "INTEGER"),
            ],
        }

        for table, expected_cols in expected_schemas.items():
            schema, tbl_name = table.split(".")
            actual = self.con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                [schema, tbl_name],
            ).fetchall()
            actual_map = {name: dtype for name, dtype in actual}

            missing = []
            type_mismatches = []
            for col_name, col_type in expected_cols:
                if col_name not in actual_map:
                    missing.append(col_name)
                elif actual_map[col_name] != col_type:
                    type_mismatches.append(
                        f"{col_name}: expected {col_type}, got {actual_map[col_name]}"
                    )

            ok = not missing and not type_mismatches
            details_parts = []
            if missing:
                details_parts.append(f"missing columns: {missing}")
            if type_mismatches:
                details_parts.append(f"type mismatches: {type_mismatches}")

            report.checks.append(
                CheckResult(
                    check_id=f"schema_{table}",
                    category="schema",
                    table=table,
                    description="Schema matches expected definition",
                    passed=ok,
                    details="; ".join(details_parts) if details_parts else "",
                )
            )

    def _check_duplicates(self, report: DQReport):
        logger.info("─ 5.3  Duplicate checks ─")

        unique_keys = [
            ("gold.fact_orders", "order_id"),
            ("gold.fact_orders", "order_sk"),
            ("gold.fact_order_items", "order_item_id"),
            ("gold.fact_order_items", "order_item_sk"),
            ("gold.fact_payments", "transaction_id"),
            ("gold.fact_payments", "payment_sk"),
            ("gold.fact_campaign_performance", "campaign_perf_sk"),
            ("gold.fact_email_engagement", "email_sk"),
            ("gold.fact_purchase_orders", "po_id"),
            ("gold.fact_purchase_orders", "po_sk"),
            ("gold.fact_inventory_daily", "inventory_sk"),
            ("gold.dim_customers", "customer_sk"),
            ("gold.dim_products", "product_sk"),
            ("silver.stg_orders", "order_id"),
            ("silver.stg_order_items", "order_item_id"),
            ("silver.stg_payment_transactions", "transaction_id"),
        ]

        for table, col in unique_keys:
            total = self._query_scalar(f"SELECT COUNT(*) FROM {table}")
            distinct = self._query_scalar(
                f'SELECT COUNT(DISTINCT "{col}") FROM {table}'
            )
            dups = total - distinct
            report.checks.append(
                CheckResult(
                    check_id=f"dup_{table}.{col}",
                    category="duplicate",
                    table=table,
                    description=f"{col} is unique",
                    passed=dups == 0,
                    failing_rows=dups,
                    details=f"{total} total, {distinct} distinct" if dups > 0 else "",
                )
            )

    def _check_referential_integrity(self, report: DQReport):
        logger.info("─ 5.4  Referential integrity checks ─")

        fk_checks = [
            ("gold.fact_orders", "customer_sk", "gold.dim_customers", "customer_sk"),
            ("gold.fact_orders", "order_date_key", "gold.dim_date", "date_key"),
            ("gold.fact_order_items", "order_sk", "gold.fact_orders", "order_sk"),
            ("gold.fact_order_items", "product_sk", "gold.dim_products", "product_sk"),
            ("gold.fact_payments", "order_sk", "gold.fact_orders", "order_sk"),
            ("gold.fact_payments", "payment_date_key", "gold.dim_date", "date_key"),
            (
                "gold.fact_inventory_daily",
                "product_sk",
                "gold.dim_products",
                "product_sk",
            ),
            (
                "gold.fact_inventory_daily",
                "snapshot_date_key",
                "gold.dim_date",
                "date_key",
            ),
            (
                "gold.fact_campaign_performance",
                "campaign_date_key",
                "gold.dim_date",
                "date_key",
            ),
            (
                "gold.fact_email_engagement",
                "customer_sk",
                "gold.dim_customers",
                "customer_sk",
            ),
            (
                "gold.fact_email_engagement",
                "send_date_key",
                "gold.dim_date",
                "date_key",
            ),
            (
                "gold.fact_purchase_orders",
                "product_sk",
                "gold.dim_products",
                "product_sk",
            ),
            (
                "gold.fact_purchase_orders",
                "created_date_key",
                "gold.dim_date",
                "date_key",
            ),
        ]

        for child_tbl, child_col, parent_tbl, parent_col in fk_checks:
            orphans = self._query_scalar(f"""
                SELECT COUNT(*) FROM {child_tbl} c
                WHERE c.{child_col} IS NOT NULL
                  AND c.{child_col} NOT IN (SELECT {parent_col} FROM {parent_tbl})
            """)
            report.checks.append(
                CheckResult(
                    check_id=f"fk_{child_tbl}.{child_col}__{parent_tbl}.{parent_col}",
                    category="referential",
                    table=child_tbl,
                    description=f"{child_col} → {parent_tbl}.{parent_col}",
                    passed=orphans == 0,
                    failing_rows=orphans,
                )
            )

    def _check_currency_consistency(self, report: DQReport):
        logger.info("─ 5.5  Currency consistency checks ─")

        mismatched = self._query_scalar("""
            SELECT COUNT(*)
            FROM gold.fact_orders o
            JOIN gold.fact_payments p ON o.order_sk = p.order_sk
            WHERE o.currency != p.currency
        """)
        report.checks.append(
            CheckResult(
                check_id="currency_order_payment_match",
                category="currency",
                table="gold.fact_orders ↔ gold.fact_payments",
                description="Order currency matches payment currency",
                passed=mismatched == 0,
                failing_rows=mismatched,
            )
        )

        unknown = self._query_scalar("""
            SELECT COUNT(DISTINCT o.currency)
            FROM gold.fact_orders o
            WHERE o.currency NOT IN (SELECT currency FROM silver.seed_exchange_rates)
        """)
        report.checks.append(
            CheckResult(
                check_id="currency_known_rate",
                category="currency",
                table="gold.fact_orders",
                description="All order currencies have exchange rates",
                passed=unknown == 0,
                failing_rows=unknown,
            )
        )

        bad_usd = self._query_scalar("""
            SELECT COUNT(*) FROM gold.fact_orders
            WHERE total_amount > 0 AND total_amount_usd <= 0
        """)
        report.checks.append(
            CheckResult(
                check_id="currency_usd_conversion_positive",
                category="currency",
                table="gold.fact_orders",
                description="USD conversion preserves positive amounts",
                passed=bad_usd == 0,
                failing_rows=bad_usd,
            )
        )

    def _check_arithmetic(self, report: DQReport):
        logger.info("─ 5.6  Arithmetic checks ─")

        bad_totals = self._query_scalar("""
            SELECT COUNT(*) FROM gold.fact_orders
            WHERE ABS(
                total_amount - (subtotal_amount - discount_amount + shipping_amount + tax_amount)
            ) > 0.01
        """)
        report.checks.append(
            CheckResult(
                check_id="arith_order_total",
                category="arithmetic",
                table="gold.fact_orders",
                description="total = subtotal - discount + shipping + tax (±0.01)",
                passed=bad_totals == 0,
                failing_rows=bad_totals,
            )
        )

        bad_lines = self._query_scalar("""
            SELECT COUNT(*) FROM gold.fact_order_items
            WHERE ABS(
                line_total - (unit_price * quantity - discount_amount)
            ) > 0.01
        """)
        report.checks.append(
            CheckResult(
                check_id="arith_line_total",
                category="arithmetic",
                table="gold.fact_order_items",
                description="line_total = unit_price × quantity - discount (±0.01)",
                passed=bad_lines == 0,
                failing_rows=bad_lines,
            )
        )

        bad_nets = self._query_scalar("""
            SELECT COUNT(*) FROM gold.fact_payments
            WHERE transaction_type NOT IN ('void', 'authorization')
              AND ABS(net_amount - (gross_amount - fee_amount)) > 0.01
        """)
        report.checks.append(
            CheckResult(
                check_id="arith_payment_net",
                category="arithmetic",
                table="gold.fact_payments",
                description="net_amount = gross_amount - fee_amount (±0.01)",
                passed=bad_nets == 0,
                failing_rows=bad_nets,
            )
        )

        bad_gp = self._query_scalar("""
            SELECT COUNT(*) FROM gold.fact_order_items
            WHERE ABS(gross_profit - (line_total - unit_cost * quantity)) > 0.01
        """)
        report.checks.append(
            CheckResult(
                check_id="arith_gross_profit",
                category="arithmetic",
                table="gold.fact_order_items",
                description="gross_profit = line_total - unit_cost × quantity (±0.01)",
                passed=bad_gp == 0,
                failing_rows=bad_gp,
            )
        )

        bad_po = self._query_scalar("""
            SELECT COUNT(*) FROM gold.fact_purchase_orders
            WHERE ABS(po_total - ordered_qty * unit_cost) > 0.01
        """)
        report.checks.append(
            CheckResult(
                check_id="arith_po_total",
                category="arithmetic",
                table="gold.fact_purchase_orders",
                description="po_total = ordered_qty × unit_cost (±0.01)",
                passed=bad_po == 0,
                failing_rows=bad_po,
            )
        )

        for table, col in [
            ("gold.fact_orders", "total_amount"),
            ("gold.fact_orders", "total_amount_usd"),
            ("gold.fact_order_items", "unit_price"),
            ("gold.fact_payments", "gross_amount"),
            ("gold.fact_inventory_daily", "inventory_value"),
        ]:
            neg = self._query_scalar(f"SELECT COUNT(*) FROM {table} WHERE {col} < 0")
            report.checks.append(
                CheckResult(
                    check_id=f"arith_nonneg_{table}.{col}",
                    category="arithmetic",
                    table=table,
                    description=f"{col} >= 0",
                    passed=neg == 0,
                    failing_rows=neg,
                )
            )

    def _query_scalar(self, sql: str) -> int:
        return self.con.execute(sql).fetchone()[0]

def write_report(report: DQReport) -> Path:
    \
    json_path = REPORT_DIR / f"dq_report_{report.run_id}.json"
    data = {
        "run_id": report.run_id,
        "run_ts": report.run_ts,
        "total_checks": report.total,
        "passed": report.passed,
        "failed": report.failed,
        "checks": [
            {
                "check_id": c.check_id,
                "category": c.category,
                "table": c.table,
                "description": c.description,
                "passed": c.passed,
                "failing_rows": c.failing_rows,
                "details": c.details,
            }
            for c in report.checks
        ],
    }
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2)

    csv_path = REPORT_DIR / "dq_history.csv"
    write_header = not csv_path.exists()
    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["run_id", "run_ts", "total", "passed", "failed"])
        writer.writerow(
            [report.run_id, report.run_ts, report.total, report.passed, report.failed]
        )

    logger.info("Report written to %s", json_path)
    return json_path
