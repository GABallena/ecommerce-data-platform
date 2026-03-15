\
\
\
\

import json
from pathlib import Path

import pandas as pd

from .base_ingestor import BaseIngestor

class PaymentAPIIngestor(BaseIngestor):
    SOURCE_NAME = "payments"

    ENTITIES = ["transactions"]

    def _read_source(self, source_file: str | Path, **kwargs) -> pd.DataFrame:
        with open(source_file) as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        if "provider_created_at" in df.columns:
            df["provider_created_at"] = pd.to_datetime(df["provider_created_at"])
        return df
