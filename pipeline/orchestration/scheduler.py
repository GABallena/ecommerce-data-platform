\
\
\
\
\
\
\

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Callable

logger = logging.getLogger("orchestration.scheduler")

class DailyScheduler:
\
\
\
\
\
\
\

    def __init__(
        self,
        run_fn: Callable[[], None],
        run_hour_utc: int = 2,
        run_minute_utc: int = 0,
    ):
        self.run_fn = run_fn
        self.run_hour = run_hour_utc
        self.run_minute = run_minute_utc

    def next_run_time(self) -> datetime:
        \
        now = datetime.now(timezone.utc)
        candidate = now.replace(
            hour=self.run_hour, minute=self.run_minute, second=0, microsecond=0
        )
        if candidate <= now:
            candidate += timedelta(days=1)
        return candidate

    def start(self) -> None:
        \
        logger.info(
            "Scheduler started — daily at %02d:%02d UTC",
            self.run_hour,
            self.run_minute,
        )
        try:
            while True:
                next_run = self.next_run_time()
                wait_seconds = (next_run - datetime.now(timezone.utc)).total_seconds()
                logger.info(
                    "Next run: %s (in %.0f seconds)",
                    next_run.isoformat(),
                    wait_seconds,
                )
                time.sleep(max(wait_seconds, 0))

                logger.info("⏰ Scheduled trigger — starting pipeline run")
                try:
                    self.run_fn()
                except Exception as exc:
                    logger.error("Scheduled run failed: %s", exc)
        except KeyboardInterrupt:
            logger.info("Scheduler stopped (Ctrl-C)")

    def run_once(self) -> None:
        \
        logger.info("Manual trigger — starting pipeline run")
        self.run_fn()
