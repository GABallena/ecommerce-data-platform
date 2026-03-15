\
\
\
\

import csv
import hashlib
import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

class IngestionError(Exception):
\

    pass

logger = logging.getLogger("ingestion")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_ROOT = PROJECT_ROOT / "pipeline" / "raw"
LOG_DIR = PROJECT_ROOT / "pipeline" / "logs"
INGESTION_LOG_FILE = LOG_DIR / "ingestion_log.csv"

RAW_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

class IngestionLogEntry:
\

    def __init__(
        self,
        run_id: str,
        source: str,
        entity: str,
        source_file: str,
        status: str = "started",
        rows_ingested: int = 0,
        raw_path: str = "",
        schema_hash: str = "",
        schema_columns: str = "",
        error_message: str = "",
    ):
        self.run_id = run_id
        self.source = source
        self.entity = entity
        self.source_file = source_file
        self.status = status
        self.rows_ingested = rows_ingested
        self.raw_path = raw_path
        self.schema_hash = schema_hash
        self.schema_columns = schema_columns
        self.error_message = error_message
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.completed_at: str | None = None

    def complete(
        self,
        status: str,
        rows: int,
        raw_path: str,
        schema_hash: str,
        schema_columns: str,
    ):
        self.status = status
        self.rows_ingested = rows
        self.raw_path = raw_path
        self.schema_hash = schema_hash
        self.schema_columns = schema_columns
        self.completed_at = datetime.now(timezone.utc).isoformat()

    def fail(self, error: str):
        self.status = "failed"
        self.error_message = error
        self.completed_at = datetime.now(timezone.utc).isoformat()

    def as_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "source": self.source,
            "entity": self.entity,
            "source_file": self.source_file,
            "status": self.status,
            "rows_ingested": self.rows_ingested,
            "raw_path": self.raw_path,
            "schema_hash": self.schema_hash,
            "schema_columns": self.schema_columns,
            "error_message": self.error_message,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
        }

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            s = datetime.fromisoformat(self.started_at)
            e = datetime.fromisoformat(self.completed_at)
            return round((e - s).total_seconds(), 3)
        return None

class BaseIngestor:
\
\
\
\
\
\

    SOURCE_NAME: str = "unknown"

    def __init__(
        self,
        raw_root: Path = RAW_ROOT,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        retry_backoff: float = 2.0,
    ):
        self.raw_root = raw_root
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff

    _NON_RETRYABLE = (
        FileNotFoundError,
        PermissionError,
        IsADirectoryError,
        IngestionError,
        ValueError,
        KeyError,
    )

    def ingest(
        self,
        source_file: str | Path,
        entity: str,
        partition_date: str,
        run_id: str | None = None,
        **kwargs: Any,
    ) -> dict:
        \
\
\
\
\
        run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        log_entry = IngestionLogEntry(
            run_id=run_id,
            source=self.SOURCE_NAME,
            entity=entity,
            source_file=str(source_file),
        )

        try:
            self._preflight_checks(source_file, entity, partition_date)
        except IngestionError as exc:
            error_msg = f"PRE-FLIGHT FAILED: {exc}"
            logger.error("✗ %s/%s — %s", self.SOURCE_NAME, entity, error_msg)
            log_entry.fail(error_msg)
            self._write_log(log_entry)
            return log_entry.as_dict()

        attempt = 0
        delay = self.retry_delay
        last_error = ""

        while attempt <= self.max_retries:
            try:
                if attempt > 0:
                    logger.info(
                        "Retry %d/%d for %s/%s after %.1fs",
                        attempt,
                        self.max_retries,
                        self.SOURCE_NAME,
                        entity,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= self.retry_backoff

                logger.info(
                    "Reading %s/%s from %s", self.SOURCE_NAME, entity, source_file
                )
                df = self._read_source(source_file, entity=entity, **kwargs)

                if df.empty:
                    raise IngestionError(
                        f"Source returned 0 rows for {self.SOURCE_NAME}/{entity} "
                        f"from {source_file} — refusing to write empty dataset"
                    )

                schema_cols = ",".join(df.columns.tolist())
                schema_hash = self._compute_schema_hash(df)
                self._detect_schema_drift(entity, schema_hash, schema_cols)

                raw_path = self._write_raw(df, entity, partition_date)

                log_entry.complete(
                    status="success",
                    rows=len(df),
                    raw_path=str(raw_path),
                    schema_hash=schema_hash,
                    schema_columns=schema_cols,
                )
                self._write_log(log_entry)
                logger.info(
                    "✓ %s/%s — %d rows → %s  (%.3fs)",
                    self.SOURCE_NAME,
                    entity,
                    len(df),
                    raw_path,
                    log_entry.duration_seconds or 0,
                )
                return log_entry.as_dict()

            except self._NON_RETRYABLE as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "✗ %s/%s — non-retryable error (fail-fast): %s",
                    self.SOURCE_NAME,
                    entity,
                    last_error,
                )
                logger.debug("Traceback:\n%s", traceback.format_exc())
                break

            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "Attempt %d failed for %s/%s: %s",
                    attempt + 1,
                    self.SOURCE_NAME,
                    entity,
                    last_error,
                )
                logger.debug("Traceback:\n%s", traceback.format_exc())
                attempt += 1

        log_entry.fail(last_error)
        self._write_log(log_entry)
        logger.error("✗ %s/%s — FAILED: %s", self.SOURCE_NAME, entity, last_error)
        return log_entry.as_dict()

    def _read_source(self, source_file: str | Path, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    def _preflight_checks(
        self, source_file: str | Path, entity: str, partition_date: str
    ):
        \
        source_path = Path(source_file)

        if not source_path.exists():
            raise IngestionError(f"Source file does not exist: {source_path}")

        if source_path.is_dir():
            raise IngestionError(
                f"Source path is a directory, not a file: {source_path}"
            )

        if not os.access(source_path, os.R_OK):
            raise IngestionError(f"Source file is not readable: {source_path}")

        if source_path.stat().st_size == 0:
            raise IngestionError(f"Source file is empty (0 bytes): {source_path}")

        try:
            datetime.strptime(partition_date, "%Y-%m-%d")
        except ValueError:
            raise IngestionError(
                f"Invalid partition_date '{partition_date}' — expected YYYY-MM-DD"
            )

        if not entity or not entity.strip():
            raise IngestionError("Entity name is empty")

    @staticmethod
    def _compute_schema_hash(df: pd.DataFrame) -> str:
        \
        sig = "|".join(f"{c}:{df[c].dtype}" for c in df.columns)
        return hashlib.sha256(sig.encode()).hexdigest()[:16]

    def _detect_schema_drift(self, entity: str, current_hash: str, current_cols: str):
        \
        schema_registry = LOG_DIR / "schema_registry.json"
        registry: dict = {}
        if schema_registry.exists():
            with open(schema_registry) as f:
                registry = json.load(f)

        key = f"{self.SOURCE_NAME}/{entity}"
        previous = registry.get(key)

        if previous and previous["hash"] != current_hash:
            logger.warning(
                "⚠ SCHEMA DRIFT detected for %s\n  previous: %s\n  current:  %s",
                key,
                previous["columns"],
                current_cols,
            )

        registry[key] = {
            "hash": current_hash,
            "columns": current_cols,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(schema_registry, "w") as f:
            json.dump(registry, f, indent=2)

    def _write_raw(self, df: pd.DataFrame, entity: str, partition_date: str) -> Path:
        \
        dest_dir = self.raw_root / self.SOURCE_NAME / entity / f"dt={partition_date}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / "data.parquet"
        df.to_parquet(dest_file, index=False, engine="pyarrow")
        return dest_file

    @staticmethod
    def _write_log(entry: IngestionLogEntry):
        \
        row = entry.as_dict()
        file_exists = INGESTION_LOG_FILE.exists()

        with open(INGESTION_LOG_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
