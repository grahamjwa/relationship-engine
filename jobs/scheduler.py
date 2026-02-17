"""
Scheduler for Relationship Engine
Runs scheduled tasks using APScheduler.

Schedule:
  2:00 AM ET — Nightly graph recompute + opportunity scoring
  6:00 AM ET — Morning signal scan (funding + hiring)
  7:00 AM ET — Morning briefing to Discord
  5:00 PM ET — Afternoon signal scan (funding + hiring)
  Sunday 3:00 AM ET — Weekly executive movement scan
"""

import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_APSCHEDULER = True
except ImportError:
    HAS_APSCHEDULER = False

from jobs.nightly_recompute import run_nightly

try:
    from integrations.morning_briefing import run_morning_briefing
    HAS_MORNING_BRIEFING = True
except ImportError:
    HAS_MORNING_BRIEFING = False

try:
    from scrapers.signal_pipeline import run_signal_scan
    HAS_SIGNAL_PIPELINE = True
except ImportError:
    HAS_SIGNAL_PIPELINE = False

try:
    from scrapers.executive_tracker import run_movement_scan
    HAS_EXECUTIVE_TRACKER = True
except ImportError:
    HAS_EXECUTIVE_TRACKER = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("scheduler")


def _run_signal_scan_wrapper():
    """Wrapper for signal scan with error handling."""
    try:
        logger.info("Starting signal scan...")
        result = run_signal_scan(scan_types=["funding", "hiring"], max_companies=30, verbose=True)
        logger.info(f"Signal scan complete: {result.get('total_inserted', 0)} new signals")
    except Exception as e:
        logger.error(f"Signal scan failed: {e}")


def _run_executive_tracker_wrapper():
    """Wrapper for executive tracker with error handling."""
    try:
        logger.info("Starting executive movement scan...")
        movements = run_movement_scan()
        logger.info(f"Executive scan complete: {len(movements)} movements found")
    except Exception as e:
        logger.error(f"Executive tracker failed: {e}")


def _run_morning_briefing_wrapper():
    """Wrapper for morning briefing with error handling."""
    try:
        logger.info("Generating morning briefing...")
        run_morning_briefing()
        logger.info("Morning briefing sent.")
    except Exception as e:
        logger.error(f"Morning briefing failed: {e}")


def start_scheduler():
    """Start the blocking scheduler with all configured jobs."""
    if not HAS_APSCHEDULER:
        logger.error("APScheduler not installed. Run: pip install apscheduler")
        return

    scheduler = BlockingScheduler(timezone="US/Eastern")

    # 2:00 AM ET — Nightly graph recomputation + opportunity scoring
    scheduler.add_job(
        run_nightly,
        trigger=CronTrigger(hour=2, minute=0, timezone="US/Eastern"),
        id="nightly_recompute",
        name="Nightly Graph Recompute",
        misfire_grace_time=3600,
        replace_existing=True
    )
    logger.info("Scheduled: nightly_recompute at 2:00 AM ET")

    # 6:00 AM ET — Morning signal scan
    if HAS_SIGNAL_PIPELINE:
        scheduler.add_job(
            _run_signal_scan_wrapper,
            trigger=CronTrigger(hour=6, minute=0, timezone="US/Eastern"),
            id="morning_signal_scan",
            name="Morning Signal Scan",
            misfire_grace_time=3600,
            replace_existing=True
        )
        logger.info("Scheduled: morning_signal_scan at 6:00 AM ET")

    # 7:00 AM ET — Morning briefing
    if HAS_MORNING_BRIEFING:
        scheduler.add_job(
            _run_morning_briefing_wrapper,
            trigger=CronTrigger(hour=7, minute=0, timezone="US/Eastern"),
            id="morning_briefing",
            name="Morning Briefing",
            misfire_grace_time=3600,
            replace_existing=True
        )
        logger.info("Scheduled: morning_briefing at 7:00 AM ET")

    # 5:00 PM ET — Afternoon signal scan
    if HAS_SIGNAL_PIPELINE:
        scheduler.add_job(
            _run_signal_scan_wrapper,
            trigger=CronTrigger(hour=17, minute=0, timezone="US/Eastern"),
            id="afternoon_signal_scan",
            name="Afternoon Signal Scan",
            misfire_grace_time=3600,
            replace_existing=True
        )
        logger.info("Scheduled: afternoon_signal_scan at 5:00 PM ET")

    # Sunday 3:00 AM ET — Weekly executive movement scan
    if HAS_EXECUTIVE_TRACKER:
        scheduler.add_job(
            _run_executive_tracker_wrapper,
            trigger=CronTrigger(day_of_week="sun", hour=3, minute=0, timezone="US/Eastern"),
            id="weekly_executive_scan",
            name="Weekly Executive Movement Scan",
            misfire_grace_time=7200,
            replace_existing=True
        )
        logger.info("Scheduled: weekly_executive_scan at Sunday 3:00 AM ET")

    logger.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler shutting down.")
        scheduler.shutdown()


if __name__ == "__main__":
    start_scheduler()
