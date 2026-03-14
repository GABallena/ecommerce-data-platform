"""
Ingestor for marketing CSV exports: campaign_performance, email_sends.
"""

from pathlib import Path

import pandas as pd

from .base_ingestor import BaseIngestor


class CSVMarketingIngestor(BaseIngestor):
    SOURCE_NAME = "marketing"

    ENTITIES = ["campaign_performance", "email_sends"]

    _DATE_COLS: dict[str, list[str]] = {
        "campaign_performance": ["campaign_date", "loaded_at"],
        "email_sends": ["send_date", "loaded_at"],
    }

    def _read_source(self, source_file: str | Path, **kwargs) -> pd.DataFrame:
        entity = kwargs.get("entity", "")
        parse_dates = self._DATE_COLS.get(entity, [])
        df = pd.read_csv(source_file, parse_dates=parse_dates if parse_dates else False)
        return df
