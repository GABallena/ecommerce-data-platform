"""
dashboard.py — CLI monitoring dashboard.

Reads all log files and prints a unified, formatted view of:
  - Pipeline run history
  - Per-task runtime metrics
  - Failure log
  - Ingestion volumes
  - Data quality trends

Usage:
    python -m pipeline.monitoring.dashboard
    python -m pipeline.monitoring.dashboard --section runs
    python -m pipeline.monitoring.dashboard --section tasks
    python -m pipeline.monitoring.dashboard --section failures
    python -m pipeline.monitoring.dashboard --section ingestion
    python -m pipeline.monitoring.dashboard --section dq
    python -m pipeline.monitoring.dashboard --json
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.monitoring import get_dashboard_data


def _table(rows: list[dict], columns: list[tuple[str, str, int]]) -> str:
    """
    Format rows into an ASCII table.
    columns: list of (dict_key, display_header, width)
    """
    if not rows:
        return "  (no data)\n"

    lines = []
    hdr = "  ".join(h.ljust(w) for _, h, w in columns)
    lines.append(hdr)
    lines.append("  ".join("─" * w for _, _, w in columns))

    for row in rows:
        cells = []
        for key, _, w in columns:
            val = str(row.get(key, ""))
            if len(val) > w:
                val = val[: w - 1] + "…"
            cells.append(val.ljust(w))
        lines.append("  ".join(cells))

    return "\n".join(lines) + "\n"


def _shorten_ts(ts: str) -> str:
    """Shorten ISO timestamp to readable form."""
    if not ts:
        return ""
    return ts[:19].replace("T", " ")


def render_runs(data: dict) -> str:
    runs = data.get("pipeline_runs", [])
    lines = ["\n═══ PIPELINE RUN HISTORY ═══\n"]

    if not runs:
        lines.append("  (no runs recorded)\n")
        return "\n".join(lines)

    for r in runs:
        r["run_ts_short"] = _shorten_ts(r.get("run_ts", ""))
    lines.append(
        _table(
            runs,
            [
                ("run_ts_short", "Timestamp", 20),
                ("dag_id", "DAG", 18),
                ("total_tasks", "Tasks", 6),
                ("succeeded", "OK", 4),
                ("failed", "Fail", 5),
                ("elapsed_seconds", "Duration(s)", 12),
            ],
        )
    )

    total = len(runs)
    failed_runs = sum(1 for r in runs if int(r.get("failed", 0)) > 0)
    lines.append(
        f"  Total runs: {total}   Failed runs: {failed_runs}   "
        f"Success rate: {(total - failed_runs) / total * 100:.0f}%\n"
    )
    return "\n".join(lines)


def render_tasks(data: dict) -> str:
    tasks = data.get("task_history", [])
    lines = ["\n═══ TASK RUNTIME METRICS ═══\n"]

    if not tasks:
        lines.append("  (no task data)\n")
        return "\n".join(lines)

    for t in tasks:
        t["start_short"] = _shorten_ts(t.get("start_time", ""))

    lines.append(
        _table(
            tasks,
            [
                ("pipeline_run_id", "Run ID", 16),
                ("task_id", "Task", 20),
                ("status", "Status", 16),
                ("duration_seconds", "Duration(s)", 12),
                ("attempts", "Attempts", 9),
                ("start_short", "Started", 20),
            ],
        )
    )

    from collections import defaultdict

    durations = defaultdict(list)
    for t in tasks:
        try:
            durations[t["task_id"]].append(float(t.get("duration_seconds", 0)))
        except (ValueError, KeyError):
            pass

    if durations:
        lines.append("  Average durations:")
        for tid, durs in sorted(durations.items()):
            avg = sum(durs) / len(durs)
            lines.append(f"    {tid:25s}  avg={avg:.3f}s  runs={len(durs)}")
        lines.append("")

    return "\n".join(lines)


def render_failures(data: dict) -> str:
    failures = data.get("failures", [])
    lines = ["\n═══ FAILURE LOG ═══\n"]

    if not failures:
        lines.append("  No failures recorded ✓\n")
        return "\n".join(lines)

    for f in failures:
        f["ts_short"] = _shorten_ts(f.get("failure_ts", ""))
        err = f.get("error", "")
        f["error_short"] = err[:60] + "…" if len(err) > 60 else err

    lines.append(
        _table(
            failures,
            [
                ("ts_short", "Timestamp", 20),
                ("pipeline_run_id", "Run ID", 16),
                ("task_id", "Task", 20),
                ("status", "Status", 16),
                ("attempts", "Attempts", 9),
                ("error_short", "Error", 60),
            ],
        )
    )
    return "\n".join(lines)


def render_ingestion(data: dict) -> str:
    vols = data.get("ingestion_volumes", [])
    lines = ["\n═══ INGESTION VOLUME TRACKING ═══\n"]

    if not vols:
        lines.append("  (no ingestion data)\n")
        return "\n".join(lines)

    success = [v for v in vols if v.get("status") == "success"]
    if not success:
        success = vols

    for v in success:
        v["ts_short"] = _shorten_ts(v.get("completed_at", v.get("started_at", "")))

    lines.append(
        _table(
            success,
            [
                ("run_id", "Run ID", 16),
                ("source", "Source", 12),
                ("entity", "Entity", 22),
                ("rows_ingested", "Rows", 8),
                ("duration_seconds", "Duration(s)", 12),
                ("ts_short", "Completed", 20),
            ],
        )
    )

    from collections import defaultdict

    src_totals = defaultdict(int)
    for v in success:
        try:
            src_totals[v.get("source", "?")] += int(v.get("rows_ingested", 0))
        except (ValueError, KeyError):
            pass

    if src_totals:
        lines.append("  Volume by source:")
        for src, total in sorted(src_totals.items()):
            lines.append(f"    {src:15s}  {total:>6d} rows")
        lines.append(f"    {'TOTAL':15s}  {sum(src_totals.values()):>6d} rows")
        lines.append("")

    return "\n".join(lines)


def render_dq(data: dict) -> str:
    history = data.get("dq_history", [])
    latest = data.get("latest_dq_report")
    lines = ["\n═══ DATA QUALITY TRENDS ═══\n"]

    if not history:
        lines.append("  (no DQ runs)\n")
        return "\n".join(lines)

    for h in history:
        h["ts_short"] = _shorten_ts(h.get("run_ts", ""))
        try:
            total = int(h.get("total", 0))
            passed = int(h.get("passed", 0))
            h["pass_rate"] = f"{passed / total * 100:.0f}%" if total else "N/A"
        except (ValueError, ZeroDivisionError):
            h["pass_rate"] = "N/A"

    lines.append(
        _table(
            history,
            [
                ("run_id", "Run ID", 16),
                ("ts_short", "Timestamp", 20),
                ("total", "Total", 6),
                ("passed", "Passed", 7),
                ("failed", "Failed", 7),
                ("pass_rate", "Rate", 6),
            ],
        )
    )

    if latest and latest.get("failed", 0) > 0:
        lines.append("  Latest failures:")
        for c in latest.get("checks", []):
            if not c.get("passed"):
                lines.append(f"    ✗  {c['check_id']:50s}  {c['description']}")
                if c.get("details"):
                    lines.append(f"       → {c['details']}")
        lines.append("")
    elif latest:
        lines.append(
            f"  Latest run: all {latest.get('total_checks', '?')} checks passed ✓\n"
        )

    return "\n".join(lines)


SECTIONS = {
    "runs": render_runs,
    "tasks": render_tasks,
    "failures": render_failures,
    "ingestion": render_ingestion,
    "dq": render_dq,
}


def main():
    parser = argparse.ArgumentParser(description="Pipeline Monitoring Dashboard")
    parser.add_argument(
        "--section", choices=list(SECTIONS.keys()), help="Show only a specific section"
    )
    parser.add_argument("--json", action="store_true", help="Output raw data as JSON")
    args = parser.parse_args()

    data = get_dashboard_data()

    if args.json:
        print(json.dumps(data, indent=2, default=str))
        return

    print("\n" + "═" * 60)
    print("   PIPELINE MONITORING DASHBOARD")
    print("═" * 60)

    if args.section:
        print(SECTIONS[args.section](data))
    else:
        for render_fn in SECTIONS.values():
            print(render_fn(data))

    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
