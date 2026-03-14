"""
Warehouse engine — runs SQL models against DuckDB, reading from the raw Parquet zone.
Supports Bronze (1:1 raw load), Silver (cleaned/staged), and Gold (star schema) layers.
"""

import logging
import time
from pathlib import Path

import duckdb

logger = logging.getLogger("warehouse")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_ROOT = PROJECT_ROOT / "pipeline" / "raw"
DB_PATH = PROJECT_ROOT / "pipeline" / "warehouse" / "warehouse.duckdb"
SQL_DIR = PROJECT_ROOT / "pipeline" / "warehouse"

class WarehouseEngine:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._create_schemas()

    def _create_schemas(self):
        for schema in ("bronze", "silver", "gold"):
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def run_sql_file(self, sql_path: Path, description: str = "") -> None:
        """Execute a SQL file against the warehouse."""
        label = description or sql_path.stem
        logger.info("Running model: %s", label)
        start = time.time()

        sql = sql_path.read_text()
        sql = sql.replace("{{RAW_ROOT}}", str(RAW_ROOT))
        sql = sql.replace("{{PARTITION_DATE}}", self._partition_date or "")

        for statement in self._split_statements(sql):
            statement = statement.strip()
            if statement:
                self.conn.execute(statement)

        elapsed = time.time() - start
        logger.info("  ✓ %s completed (%.3fs)", label, elapsed)

    def run_sql(self, sql: str, description: str = "") -> None:
        """Execute raw SQL string."""
        label = description or "inline-sql"
        sql = sql.replace("{{RAW_ROOT}}", str(RAW_ROOT))
        sql = sql.replace("{{PARTITION_DATE}}", self._partition_date or "")
        for statement in self._split_statements(sql):
            statement = statement.strip()
            if statement:
                self.conn.execute(statement)

    def query(self, sql: str):
        """Run a query and return the result as a list of tuples."""
        return self.conn.execute(sql).fetchall()

    def query_df(self, sql: str):
        """Run a query and return a pandas DataFrame."""
        return self.conn.execute(sql).fetchdf()

    def set_partition_date(self, dt: str):
        self._partition_date = dt

    _partition_date: str | None = None

    def run_layer(self, layer: str, partition_date: str):
        """Run all .sql files in a layer directory, sorted by filename."""
        self.set_partition_date(partition_date)
        layer_dir = SQL_DIR / layer
        if not layer_dir.exists():
            logger.warning("Layer directory not found: %s", layer_dir)
            return
        sql_files = sorted(layer_dir.glob("*.sql"))
        if not sql_files:
            logger.warning("No SQL files found in %s", layer_dir)
            return
        logger.info("═" * 50)
        logger.info("LAYER: %s  (%d models)", layer.upper(), len(sql_files))
        logger.info("═" * 50)
        for sql_file in sql_files:
            self.run_sql_file(sql_file, description=f"{layer}/{sql_file.stem}")

    def table_stats(self, schema: str) -> list[dict]:
        """Return row counts for all tables in a schema."""
        tables = self.conn.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
        ).fetchall()
        stats = []
        for (table_name,) in tables:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
            stats.append({"schema": schema, "table": table_name, "row_count": count})
        return stats

    def close(self):
        self.conn.close()

    @staticmethod
    def _split_statements(sql: str) -> list[str]:
        """Split SQL text on semicolons, respecting basic quoting."""
        statements = []
        current = []
        in_quote = False
        quote_char = None
        for char in sql:
            if char in ("'", '"') and not in_quote:
                in_quote = True
                quote_char = char
                current.append(char)
            elif char == quote_char and in_quote:
                in_quote = False
                quote_char = None
                current.append(char)
            elif char == ';' and not in_quote:
                statements.append(''.join(current))
                current = []
            else:
                current.append(char)
        if current:
            remainder = ''.join(current).strip()
            if remainder:
                statements.append(remainder)
        return statements
