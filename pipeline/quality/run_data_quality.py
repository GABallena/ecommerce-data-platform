"""
run_data_quality.py — Entry point for the Data Quality system.

Usage:
    python -m pipeline.quality.run_data_quality
    python pipeline/quality/run_data_quality.py
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.quality import DataQualityEngine, DQReport, write_report


def main() -> DQReport:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)-18s  %(levelname)-7s  %(message)s",
    )

    engine = DataQualityEngine()
    try:
        report = engine.run_all()
    finally:
        engine.close()

    write_report(report)

    print()
    print(report.summary_text())
    print()

    if not report.all_passed:
        print(f"⚠  {report.failed} check(s) FAILED — review the report above.")
        sys.exit(1)
    else:
        print(f"All {report.total} checks passed.")

    return report


if __name__ == "__main__":
    main()
