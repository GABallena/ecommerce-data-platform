"""
Ingestor for warehouse / inventory system: inventory_snapshot, purchase_orders.
"""

from pathlib import Path

import pandas as pd

from .base_ingestor import BaseIngestor


class InventoryIngestor(BaseIngestor):
    SOURCE_NAME = "inventory"

    ENTITIES = ["inventory_snapshot", "purchase_orders"]

    _DATE_COLS: dict[str, list[str]] = {
        "inventory_snapshot": ["snapshot_date"],
        "purchase_orders": ["created_at", "expected_delivery_date"],
    }

    def _read_source(self, source_file: str | Path, **kwargs) -> pd.DataFrame:
        entity = kwargs.get("entity", "")
        parse_dates = self._DATE_COLS.get(entity, [])
        df = pd.read_csv(source_file, parse_dates=parse_dates if parse_dates else False)
        return df
