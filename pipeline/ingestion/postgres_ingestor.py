"""
Ingestor for PostgreSQL-sourced tables: orders, order_items, customers, products.
Simulates incremental reads from CSV exports of the production database.
"""

from pathlib import Path

import pandas as pd

from .base_ingestor import BaseIngestor


class PostgresIngestor(BaseIngestor):
    SOURCE_NAME = "postgres"

    ENTITIES = ["orders", "order_items", "customers", "products"]

    _DATE_COLS: dict[str, list[str]] = {
        "orders": ["order_date", "updated_at"],
        "order_items": [],
        "customers": ["created_at"],
        "products": ["created_at"],
    }

    def _read_source(self, source_file: str | Path, **kwargs) -> pd.DataFrame:
        entity = kwargs.get("entity", "")
        parse_dates = self._DATE_COLS.get(entity, [])
        df = pd.read_csv(source_file, parse_dates=parse_dates if parse_dates else False)
        return df
