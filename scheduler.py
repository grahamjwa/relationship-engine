"""
Scheduler for Relationship Engine
Runs scheduled tasks using APScheduler.
"""

import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from nightly_recompute import run_nightly

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("scheduler")


def start_scheduler():
    """Start the blocking scheduler with all configured jobs."""
    if not HAS_APSCHEDULER:
        logger.error("APScheduler not installed. Run: pip install apscheduler")
        return

    scheduler = BlockingScheduler(timezone="US/Eastern")

    # 2:00 AM ET — Nightly graph recomputation
    scheduler.add_job(
        run_nightly,
        trigger=CronTrigger(hour=2, minute=0, timezone="US/Eastern"),
        id="nightly_recompute",
        name="Nightly Graph Recompute",
        misfire_grace_time=3600,
        replace_existing=True
    )
    logger.info("Scheduled: nightly_recompute at 2:00 AM ET")

    logger.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler shutting down.")
        scheduler.shutdown()


if __name__ == "__main__":
    start_scheduler()
