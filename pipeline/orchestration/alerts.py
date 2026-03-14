"""
alerts.py — Failure alerting for pipeline runs.

Supports multiple alert channels (log-file, email, Slack webhook).
Channels are configured via pipeline/configs/alerts_config.json.
Falls back to log-only if no config exists.
"""

import json
import logging
import smtplib
import ssl
import urllib.request
import urllib.error
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

logger = logging.getLogger("orchestration.alerts")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ALERTS_CONFIG_PATH = PROJECT_ROOT / "pipeline" / "configs" / "alerts_config.json"


def load_alert_config() -> dict[str, Any]:
    """Load alert config; return empty dict if missing."""
    if ALERTS_CONFIG_PATH.exists():
        with open(ALERTS_CONFIG_PATH) as f:
            return json.load(f)
    return {}


def send_alert(subject: str, body: str) -> None:
    """Dispatch an alert via all configured channels."""
    config = load_alert_config()

    logger.warning("🔔 ALERT: %s\n%s", subject, body)

    email_cfg = config.get("email")
    if email_cfg and email_cfg.get("enabled"):
        _send_email(email_cfg, subject, body)

    slack_cfg = config.get("slack")
    if slack_cfg and slack_cfg.get("enabled"):
        _send_slack(slack_cfg, subject, body)


def _send_email(cfg: dict, subject: str, body: str) -> None:
    """Send alert via SMTP. Config keys: smtp_host, smtp_port, from_addr, to_addrs, username, password."""
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = cfg["from_addr"]
        msg["To"] = ", ".join(cfg["to_addrs"])

        context = ssl.create_default_context()
        with smtplib.SMTP(cfg["smtp_host"], cfg.get("smtp_port", 587)) as server:
            server.starttls(context=context)
            if cfg.get("username"):
                server.login(cfg["username"], cfg["password"])
            server.sendmail(cfg["from_addr"], cfg["to_addrs"], msg.as_string())
        logger.info("Email alert sent to %s", cfg["to_addrs"])
    except Exception as exc:
        logger.error("Failed to send email alert: %s", exc)


def _send_slack(cfg: dict, subject: str, body: str) -> None:
    """Post alert to Slack webhook. Config keys: webhook_url."""
    try:
        payload = json.dumps({"text": f"*{subject}*\n```{body}```"}).encode("utf-8")
        req = urllib.request.Request(
            cfg["webhook_url"],
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                logger.info("Slack alert sent")
            else:
                logger.warning("Slack returned status %d", resp.status)
    except Exception as exc:
        logger.error("Failed to send Slack alert: %s", exc)


def format_dag_failure_alert(dag_id: str, results: dict) -> tuple[str, str]:
    """Build subject + body from DAG run results."""
    failed = {tid: r for tid, r in results.items() if not r.succeeded}
    subject = f"[PIPELINE ALERT] DAG '{dag_id}' — {len(failed)} task(s) failed"

    lines = [
        f"DAG: {dag_id}",
        f"Failed tasks: {len(failed)} / {len(results)} total",
        "",
    ]
    for tid, r in failed.items():
        lines.append(f"  Task: {tid}")
        lines.append(f"    Status:   {r.status.value}")
        lines.append(f"    Attempts: {r.attempts}")
        lines.append(f"    Error:    {r.error}")
        lines.append("")

    body = "\n".join(lines)
    return subject, body
