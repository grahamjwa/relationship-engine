"""
Scheduler for Relationship Engine
Runs scheduled tasks using APScheduler.
"""

import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrapers"))

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from nightly_recompute import run_nightly

# Import signal pipeline (may not exist yet)
try:
    from signal_pipeline import run_signal_scan
    HAS_SIGNAL_PIPELINE = True
except ImportError:
    HAS_SIGNAL_PIPELINE = False

# Import weekly digest
try:
    from weekly_digest import post_weekly_digest
    HAS_WEEKLY_DIGEST = True
except ImportError:
    HAS_WEEKLY_DIGEST = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("scheduler")


def run_weekly():
    """Run weekly digest on Sundays."""
    if HAS_WEEKLY_DIGEST:
        logger.info("Starting weekly digest...")
        try:
            post_weekly_digest()
            logger.info("Weekly digest complete.")
        except Exception as e:
            logger.error(f"Weekly digest failed: {e}")
    else:
        logger.warning("Weekly digest not available.")


def run_morning_scan():
    """Run morning signal scan - funding and hiring."""
    if HAS_SIGNAL_PIPELINE:
        logger.info("Starting morning signal scan...")
        try:
            run_signal_scan(scan_types=["funding", "hiring"], max_companies=10, verbose=True)
            logger.info("Morning signal scan complete.")
        except Exception as e:
            logger.error(f"Morning scan failed: {e}")
    else:
        logger.warning("Signal pipeline not available.")


def run_afternoon_scan():
    """Run afternoon signal scan - funding only (lighter)."""
    if HAS_SIGNAL_PIPELINE:
        logger.info("Starting afternoon signal scan...")
        try:
            run_signal_scan(scan_types=["funding"], max_companies=5, verbose=True)
            logger.info("Afternoon signal scan complete.")
        except Exception as e:
            logger.error(f"Afternoon scan failed: {e}")
    else:
        logger.warning("Signal pipeline not available.")


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

    # 6:00 AM ET — Morning signal scan (funding + hiring)
    if HAS_SIGNAL_PIPELINE:
        scheduler.add_job(
            run_morning_scan,
            trigger=CronTrigger(hour=6, minute=0, timezone="US/Eastern"),
            id="morning_signal_scan",
            name="Morning Signal Scan",
            misfire_grace_time=3600,
            replace_existing=True
        )
        logger.info("Scheduled: morning_signal_scan at 6:00 AM ET")

        # 5:00 PM ET — Afternoon signal scan (funding only)
        scheduler.add_job(
            run_afternoon_scan,
            trigger=CronTrigger(hour=17, minute=0, timezone="US/Eastern"),
            id="afternoon_signal_scan",
            name="Afternoon Signal Scan",
            misfire_grace_time=3600,
            replace_existing=True
        )
        logger.info("Scheduled: afternoon_signal_scan at 5:00 PM ET")
    else:
        logger.warning("Signal pipeline not available - skipping signal scan jobs")

    # 8:00 AM ET Sunday — Weekly digest
    if HAS_WEEKLY_DIGEST:
        scheduler.add_job(
            run_weekly,
            trigger=CronTrigger(day_of_week='sun', hour=8, minute=0, timezone="US/Eastern"),
            id="weekly_digest",
            name="Weekly Digest",
            misfire_grace_time=3600,
            replace_existing=True
        )
        logger.info("Scheduled: weekly_digest at 8:00 AM ET on Sundays")

    logger.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler shutting down.")
        scheduler.shutdown()


if __name__ == "__main__":
    start_scheduler()
