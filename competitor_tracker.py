import sqlite3
import sys
import logging
from datetime import datetime
from typing import Optional, List, Dict

sys.path.insert(0, '/sessions/sharp-admiring-curie/relationship_engine')
from core.graph_engine import get_db_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def migrate_competitor_schema(db_path: str) -> None:
    """Migrate database schema to add competitor tracking columns."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE deals ADD COLUMN competitor_broker TEXT")
        logger.info("Added competitor_broker column to deals table")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            logger.debug("competitor_broker column already exists")
        else:
            raise

    try:
        cursor.execute("ALTER TABLE deals ADD COLUMN lost_reason TEXT")
        logger.info("Added lost_reason column to deals table")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            logger.debug("lost_reason column already exists")
        else:
            raise

    conn.commit()
    conn.close()


def log_competitor(deal_id: int, broker_name: str, lost_reason: str, db_path: Optional[str] = None) -> None:
    """Record a competitor broker on a deal.

    Args:
        deal_id: ID of the deal
        broker_name: Name of competing broker
        lost_reason: Reason we lost the deal (e.g., "better commission", "existing relationship")
        db_path: Path to database (uses default if None)
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE deals
            SET competitor_broker = ?, lost_reason = ?
            WHERE id = ?
        """, (broker_name, lost_reason, deal_id))

        conn.commit()
        logger.info(f"Logged competitor {broker_name} for deal {deal_id}: {lost_reason}")
    except Exception as e:
        logger.error(f"Error logging competitor: {e}")
        raise
    finally:
        conn.close()


def get_competitor_history(broker_name: Optional[str] = None, db_path: Optional[str] = None) -> List[Dict]:
    """Get all competitor encounters, optionally filtered by broker.

    Args:
        broker_name: Filter by specific broker (returns all if None)
        db_path: Path to database (uses default if None)

    Returns:
        List of dicts with deal_id, competitor_broker, lost_reason, deal_created_at
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        if broker_name:
            cursor.execute("""
                SELECT id, competitor_broker, lost_reason, created_at
                FROM deals
                WHERE competitor_broker = ?
                ORDER BY created_at DESC
            """, (broker_name,))
        else:
            cursor.execute("""
                SELECT id, competitor_broker, lost_reason, created_at
                FROM deals
                WHERE competitor_broker IS NOT NULL
                ORDER BY created_at DESC
            """)

        results = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Retrieved {len(results)} competitor records")
        return results
    finally:
        conn.close()


def detect_repeat_losses(threshold: int = 2, db_path: Optional[str] = None) -> Dict[str, int]:
    """Find brokers who have beaten us threshold+ times.

    Args:
        threshold: Minimum number of losses to trigger alert
        db_path: Path to database (uses default if None)

    Returns:
        Dict mapping broker_name to loss count
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT competitor_broker, COUNT(*) as loss_count
            FROM deals
            WHERE competitor_broker IS NOT NULL
            GROUP BY competitor_broker
            HAVING loss_count >= ?
            ORDER BY loss_count DESC
        """, (threshold,))

        results = {row[0]: row[1] for row in cursor.fetchall()}
        logger.info(f"Found {len(results)} brokers with {threshold}+ losses")
        return results
    finally:
        conn.close()


def generate_competitor_report(db_path: Optional[str] = None) -> str:
    """Generate markdown summary of competitive landscape.

    Args:
        db_path: Path to database (uses default if None)

    Returns:
        Markdown formatted report
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Get total competitor encounters
        cursor.execute("""
            SELECT COUNT(*) FROM deals WHERE competitor_broker IS NOT NULL
        """)
        total_encounters = cursor.fetchone()[0]

        # Get top competitors
        cursor.execute("""
            SELECT competitor_broker, COUNT(*) as count
            FROM deals
            WHERE competitor_broker IS NOT NULL
            GROUP BY competitor_broker
            ORDER BY count DESC
            LIMIT 10
        """)
        top_competitors = cursor.fetchall()

        # Get loss reasons
        cursor.execute("""
            SELECT lost_reason, COUNT(*) as count
            FROM deals
            WHERE lost_reason IS NOT NULL
            GROUP BY lost_reason
            ORDER BY count DESC
        """)
        loss_reasons = cursor.fetchall()

        # Build report
        report = f"# Competitor Tracker Report\n\n"
        report += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        report += f"## Summary\n"
        report += f"- Total competitive encounters: {total_encounters}\n\n"

        report += f"## Top Competitors\n"
        if top_competitors:
            for broker, count in top_competitors:
                report += f"- {broker}: {count} losses\n"
        else:
            report += "- No competitor data available\n"

        report += f"\n## Loss Reasons\n"
        if loss_reasons:
            for reason, count in loss_reasons:
                report += f"- {reason}: {count} times\n"
        else:
            report += "- No loss reason data available\n"

        logger.info("Generated competitor report")
        return report
    finally:
        conn.close()


def alert_repeat_competitor(broker_name: str, count: int) -> None:
    """Print/log alert when broker threshold exceeded.

    Args:
        broker_name: Name of the competing broker
        count: Number of losses to this broker
    """
    alert_msg = f"⚠️  ALERT: {broker_name} has beaten us {count} times!"
    logger.warning(alert_msg)
    print(alert_msg)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Track competing brokers on deals")
    parser.add_argument('--report', action='store_true', help='Generate competitor report')
    parser.add_argument('--log-competitor', nargs=3, metavar=('DEAL_ID', 'BROKER', 'REASON'),
                       help='Log a competitor')
    parser.add_argument('--detect-losses', type=int, default=2,
                       help='Threshold for detecting repeat losses (default: 2)')
    parser.add_argument('--db', help='Database path (uses default if not specified)')

    args = parser.parse_args()
    db_path = args.db or get_db_path()

    # Initialize schema
    migrate_competitor_schema(db_path)

    if args.log_competitor:
        deal_id, broker, reason = args.log_competitor
        log_competitor(int(deal_id), broker, reason, db_path)
        print(f"Logged competitor: {broker} on deal {deal_id}")

    if args.report:
        report = generate_competitor_report(db_path)
        print(report)

        # Check for repeat losses
        repeat_losses = detect_repeat_losses(args.detect_losses, db_path)
        if repeat_losses:
            print("\n## Repeat Loss Alerts\n")
            for broker, count in repeat_losses.items():
                alert_repeat_competitor(broker, count)

    if not args.log_competitor and not args.report:
        parser.print_help()


if __name__ == '__main__':
    main()
