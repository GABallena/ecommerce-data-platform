# E-Commerce Data Pipeline

End-to-end data platform that ingests raw e-commerce data from multiple source systems, transforms it through a medallion architecture (Bronze → Silver → Gold), and produces analytics-ready fact and dimension tables in a local DuckDB warehouse.

## What It Does

1. **Ingests** data from 4 source systems (PostgreSQL, payment API, marketing CSVs, inventory database) into a partitioned raw landing zone with schema detection, retry logic, and ingestion logging.

2. **Transforms** raw data through three warehouse layers:
   - **Bronze** — raw data loaded as-is into the warehouse
   - **Silver** — cleaned, deduplicated, schema-enforced staging tables with seed/reference data
   - **Gold** — star schema with 3 dimension tables, 7 fact tables, and 4 metric/summary tables

3. **Validates** data quality with 113 automated checks across 6 categories: null/completeness, uniqueness, referential integrity, value ranges, cross-table reconciliation, and freshness.

4. **Orchestrates** the full pipeline as a DAG with dependency management, scheduled daily runs, failure alerts, and automatic retries.

5. **Monitors** pipeline health through a CLI dashboard showing run history, task status, data quality trends, and ingestion volumes.

## Project Structure

```
├── data/                        Source CSV/JSON files
├── pipeline/
│   ├── ingestion/               Ingestors (base + 4 source-specific)
│   ├── warehouse/               SQL transformations (bronze/silver/gold) + DuckDB engine
│   ├── quality/                 Data quality engine (113 checks)
│   ├── orchestration/           DAG, scheduler, alerts
│   ├── monitoring/              Pipeline monitor + CLI dashboard
│   ├── configs/                 Ingestion and alert configs
│   ├── raw/                     Partitioned raw landing zone
│   └── logs/                    Run logs, DQ reports, schema registry
├── infra/
│   ├── terraform/               AWS ECS Fargate deployment
│   ├── configs/                 Environment-specific configs (dev/prod)
│   └── scripts/                 Deploy and local run scripts
├── docs/                        Technical docs, data dictionary, client requirements
├── .github/workflows/           CI/CD pipeline
├── Dockerfile                   Multi-stage container build
├── docker-compose.yml           Local multi-service setup
└── requirements.txt             Python dependencies
```

## Tools Required

- **Python 3.10+**
- **pip** (Python package manager)

Python packages (installed automatically):
- `pandas` — data manipulation
- `pyarrow` — Parquet I/O
- `duckdb` — embedded analytical database

## How to Run

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the Full Pipeline

```bash
python -m pipeline.run_pipeline
```

This executes the complete DAG: ingest → build warehouse → run data quality checks.

### Run Individual Steps

```bash
python -m pipeline.run_ingestion --config pipeline/configs/ingestion_config.json
python -m pipeline.run_warehouse
python -m pipeline.quality.run_data_quality
```

### View the Dashboard

```bash
python -m pipeline.monitoring.dashboard
```

### Run with Docker

```bash
docker compose up pipeline
```

### Run with Scheduler (daily at 02:00)

```bash
python -m pipeline.run_pipeline --schedule --hour 2 --minute 0
```
