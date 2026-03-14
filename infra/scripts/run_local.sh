#!/usr/bin/env bash
set -euo pipefail

COMMAND="${1:-pipeline}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

case "$COMMAND" in
    pipeline)
        echo "Running full pipeline..."
        docker compose up --build pipeline
        ;;
    ingest)
        echo "Running ingestion..."
        docker compose run --rm pipeline pipeline/run_ingestion.py
        ;;
    warehouse)
        echo "Running warehouse build..."
        docker compose run --rm pipeline pipeline/run_warehouse.py
        ;;
    quality)
        echo "Running data quality checks..."
        docker compose run --rm --build quality
        ;;
    dashboard)
        echo "Opening dashboard..."
        docker compose run --rm --build dashboard
        ;;
    *)
        echo "Usage: $0 {pipeline|ingest|warehouse|quality|dashboard}"
        exit 1
        ;;
esac
