import sqlite3
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Dict

sys.path.insert(0, '/sessions/sharp-admiring-curie/relationship_engine')
from graph_engine import get_db_path

# Try to import search client
try:
    from scrapers.search_client import search_general
    HAS_SEARCH = True
except ImportError:
    HAS_SEARCH = False
    search_general = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Valid signal types
VALID_SIGNAL_TYPES = [
    'vacancy_rate',
    'sublease_availability',
    'lease_comp',
    'construction_start',
    'absorption'
]


def migrate_market_schema(db_path: str) -> None:
    """Create market_signals table if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_type TEXT NOT NULL,
                submarket TEXT,
                metric_name TEXT,
                metric_value REAL,
                source TEXT,
                source_url TEXT,
                signal_date DATE,
                notes TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        logger.info("Market signals table ready")
    except Exception as e:
        logger.error(f"Error creating market_signals table: {e}")
        raise
    finally:
        conn.close()


def log_signal(
    signal_type: str,
    submarket: str,
    metric_name: str,
    metric_value: float,
    source: str,
    source_url: Optional[str] = None,
    signal_date: Optional[str] = None,
    notes: Optional[str] = None,
    db_path: Optional[str] = None
) -> int:
    """Insert a market signal.

    Args:
        signal_type: Type of signal (vacancy_rate, sublease_availability, lease_comp, construction_start, absorption)
        submarket: Geographic submarket (e.g., "Midtown", "Financial District")
        metric_name: Name of the metric (e.g., "overall", "office", "retail")
        metric_value: Numeric value of the metric
        source: Source of data (e.g., "CoStar", "CBRE", "internal")
        source_url: URL to source if available
        signal_date: Date of signal (defaults to today)
        notes: Additional notes
        db_path: Path to database (uses default if None)

    Returns:
        ID of inserted signal
    """
    if db_path is None:
        db_path = get_db_path()

    if signal_type not in VALID_SIGNAL_TYPES:
        logger.warning(f"Invalid signal type: {signal_type}")

    if signal_date is None:
        signal_date = datetime.now().strftime('%Y-%m-%d')

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO market_signals
            (signal_type, submarket, metric_name, metric_value, source, source_url, signal_date, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (signal_type, submarket, metric_name, metric_value, source, source_url, signal_date, notes))

        signal_id = cursor.lastrowid
        conn.commit()
        logger.info(f"Logged market signal {signal_id}: {signal_type} in {submarket}")
        return signal_id
    except Exception as e:
        logger.error(f"Error logging signal: {e}")
        raise
    finally:
        conn.close()


def get_signals(
    signal_type: Optional[str] = None,
    submarket: Optional[str] = None,
    days: int = 90,
    db_path: Optional[str] = None
) -> List[Dict]:
    """Query recent market signals.

    Args:
        signal_type: Filter by signal type (optional)
        submarket: Filter by submarket (optional)
        days: Number of days in the past to retrieve (default: 90)
        db_path: Path to database (uses default if None)

    Returns:
        List of signal dicts ordered by date descending
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        query = "SELECT * FROM market_signals WHERE signal_date >= ?"
        params = [cutoff_date]

        if signal_type:
            query += " AND signal_type = ?"
            params.append(signal_type)

        if submarket:
            query += " AND submarket = ?"
            params.append(submarket)

        query += " ORDER BY signal_date DESC, created_at DESC"

        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Retrieved {len(results)} signals (type={signal_type}, submarket={submarket}, days={days})")
        return results
    finally:
        conn.close()


def generate_market_report(submarket: Optional[str] = None, db_path: Optional[str] = None) -> str:
    """Generate markdown summary of recent market signals.

    Args:
        submarket: Filter report to specific submarket (optional)
        db_path: Path to database (uses default if None)

    Returns:
        Markdown formatted report
    """
    if db_path is None:
        db_path = get_db_path()

    signals = get_signals(submarket=submarket, db_path=db_path)

    # Build report
    report = f"# Market Intelligence Report\n\n"
    report += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    if submarket:
        report += f"**Submarket:** {submarket}\n"
    report += "\n"

    if not signals:
        report += "No market signals available for the specified criteria.\n"
        return report

    # Group signals by type
    by_type = {}
    for signal in signals:
        sig_type = signal['signal_type']
        if sig_type not in by_type:
            by_type[sig_type] = []
        by_type[sig_type].append(signal)

    # Group signals by submarket
    by_submarket = {}
    for signal in signals:
        sm = signal['submarket'] or 'Unknown'
        if sm not in by_submarket:
            by_submarket[sm] = []
        by_submarket[sm].append(signal)

    # Write by signal type
    report += "## Signals by Type\n\n"
    for sig_type in VALID_SIGNAL_TYPES:
        if sig_type in by_type:
            report += f"### {sig_type.replace('_', ' ').title()}\n"
            for signal in by_type[sig_type]:
                submarket = signal['submarket'] or 'Unknown'
                metric = signal['metric_name'] or 'N/A'
                value = signal['metric_value']
                source = signal['source'] or 'Unknown'
                date = signal['signal_date']
                report += f"- **{submarket}** ({metric}): {value} - {source} ({date})\n"
            report += "\n"

    # Write by submarket if not already filtered
    if not submarket:
        report += "## Signals by Submarket\n\n"
        for sm in sorted(by_submarket.keys()):
            report += f"### {sm}\n"
            report += f"- Signals: {len(by_submarket[sm])}\n"

    logger.info("Generated market report")
    return report


def search_market_signals(submarket: str) -> Optional[List[Dict]]:
    """Search for current market data for a submarket.

    Args:
        submarket: Geographic submarket to search (e.g., "Midtown")

    Returns:
        List of newly logged signals or None if search unavailable
    """
    if not HAS_SEARCH:
        logger.warning("search_general not available - cannot search market signals")
        return None

    try:
        query = f"{submarket} real estate market vacancy rate lease comp 2026"
        results = search_general(query)
        logger.info(f"Searched market signals for {submarket}")
        return results
    except Exception as e:
        logger.error(f"Error searching market signals: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Track market-level signals")
    parser.add_argument('--report', action='store_true', help='Generate market report')
    parser.add_argument('--submarket', help='Filter report/query to specific submarket')
    parser.add_argument('--log', nargs=5, metavar=('TYPE', 'SUBMARKET', 'METRIC', 'VALUE', 'SOURCE'),
                       help='Log market signal: TYPE SUBMARKET METRIC VALUE SOURCE')
    parser.add_argument('--search', help='Search for market signals in submarket')
    parser.add_argument('--query-days', type=int, default=90, help='Days back for query (default: 90)')
    parser.add_argument('--db', help='Database path (uses default if not specified)')

    args = parser.parse_args()
    db_path = args.db or get_db_path()

    # Initialize schema
    migrate_market_schema(db_path)

    if args.log:
        signal_type, submarket, metric, value, source = args.log
        try:
            metric_value = float(value)
            signal_id = log_signal(
                signal_type=signal_type,
                submarket=submarket,
                metric_name=metric,
                metric_value=metric_value,
                source=source,
                db_path=db_path
            )
            print(f"Logged signal {signal_id}: {signal_type} in {submarket} = {metric_value}")
        except ValueError:
            print(f"Error: value must be numeric, got '{value}'")
            sys.exit(1)

    if args.report:
        report = generate_market_report(submarket=args.submarket, db_path=db_path)
        print(report)

    if args.search:
        results = search_market_signals(args.search)
        if results:
            print(f"Found market data for {args.search}:")
            print(results)
        else:
            print(f"No market data found for {args.search}")

    if not args.log and not args.report and not args.search:
        parser.print_help()


if __name__ == '__main__':
    main()
