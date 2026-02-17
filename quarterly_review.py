"""
Quarterly Business Review Module for Relationship Engine
Generates comprehensive QBR metrics and reports.
"""

import os
import sys
import sqlite3
import logging
import argparse
from typing import Optional, Dict, Tuple
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core.graph_engine import get_db_path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _get_conn(db_path: str = None) -> sqlite3.Connection:
    """Get database connection."""
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_quarter_dates(quarter: Optional[str] = None,
                     year: Optional[int] = None) -> Tuple[date, date]:
    """
    Get start and end dates for a quarter.

    Args:
        quarter: 'Q1', 'Q2', 'Q3', 'Q4' (None = current quarter)
        year: 4-digit year (None = current year)

    Returns:
        Tuple of (start_date, end_date)
    """
    today = date.today()
    if year is None:
        year = today.year
    if quarter is None:
        month = today.month
        if month <= 3:
            quarter = 'Q1'
        elif month <= 6:
            quarter = 'Q2'
        elif month <= 9:
            quarter = 'Q3'
        else:
            quarter = 'Q4'

    quarter = quarter.upper()
    quarter_map = {
        'Q1': (1, 3),
        'Q2': (4, 6),
        'Q3': (7, 9),
        'Q4': (10, 12),
    }

    if quarter not in quarter_map:
        raise ValueError(f"Invalid quarter: {quarter}")

    start_month, end_month = quarter_map[quarter]
    start_date = date(year, start_month, 1)
    # Last day of end month
    if end_month == 12:
        end_date = date(year + 1, 1, 1) + relativedelta(days=-1)
    else:
        end_date = date(year, end_month + 1, 1) + relativedelta(days=-1)

    return start_date, end_date


def get_quarterly_metrics(start: date, end: date,
                         db_path: str = None) -> Dict:
    """
    Pull key metrics for a quarter.

    Returns dict with:
        - deals_closed_count: number of won/closed deals
        - total_sf_transacted: total square feet in closed deals
        - total_revenue: sum of (deal_value * commission_rate) for closed deals
        - win_rate: closed / (closed + lost) as percentage
        - new_relationships_added: count of new relationships formed
        - signals_captured: total signals received
        - outreach_count: number of outreach activities
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    start_str = start.strftime('%Y-%m-%d')
    end_str = end.strftime('%Y-%m-%d')

    metrics = {}

    # Closed deals
    cur.execute("""
        SELECT
            COUNT(*) as count,
            COALESCE(SUM(square_feet), 0) as total_sf,
            COALESCE(SUM(deal_value * COALESCE(commission_rate, 0.06)), 0) as revenue
        FROM deals
        WHERE status IN ('closed', 'won')
        AND (started_date BETWEEN ? AND ? OR updated_at BETWEEN ? AND ?)
    """, (start_str, end_str, start_str, end_str))
    row = cur.fetchone()
    metrics['deals_closed_count'] = row['count'] or 0
    metrics['total_sf_transacted'] = row['total_sf'] or 0
    metrics['total_revenue'] = row['revenue'] or 0

    # Win rate (closed + lost)
    cur.execute("""
        SELECT
            COUNT(CASE WHEN status IN ('closed', 'won') THEN 1 END) as won,
            COUNT(CASE WHEN status = 'lost' THEN 1 END) as lost
        FROM deals
        WHERE (started_date BETWEEN ? AND ? OR updated_at BETWEEN ? AND ?)
    """, (start_str, end_str, start_str, end_str))
    row = cur.fetchone()
    won = row['won'] or 0
    lost = row['lost'] or 0
    total = won + lost
    metrics['win_rate'] = (won / total * 100) if total > 0 else 0

    # New relationships (contacts created in period)
    cur.execute("""
        SELECT COUNT(*) as count FROM contacts WHERE created_at BETWEEN ? AND ?
    """, (start_str, end_str))
    metrics['new_relationships_added'] = cur.fetchone()['count'] or 0

    # Signals captured
    cur.execute("""
        SELECT COUNT(*) as count FROM hiring_signals WHERE signal_date BETWEEN ? AND ?
        UNION ALL
        SELECT COUNT(*) FROM funding_events WHERE event_date BETWEEN ? AND ?
    """, (start_str, end_str, start_str, end_str))
    signal_rows = cur.fetchall()
    metrics['signals_captured'] = sum(r['count'] or 0 for r in signal_rows)

    # Outreach count
    cur.execute("""
        SELECT COUNT(*) as count FROM outreach_log WHERE outreach_date BETWEEN ? AND ?
    """, (start_str, end_str))
    metrics['outreach_count'] = cur.fetchone()['count'] or 0

    conn.close()
    return metrics


def get_client_retention(start: date, end: date,
                        db_path: str = None) -> Dict:
    """
    Calculate client retention metrics.

    Returns dict with:
        - active_at_start: clients with status='active_client' at period start
        - active_at_end: clients with status='active_client' at period end
        - new_clients: added during period
        - churn_count: lost during period
        - churn_rate: (lost / active_at_start) as percentage
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    start_str = start.strftime('%Y-%m-%d')
    end_str = end.strftime('%Y-%m-%d')

    metrics = {}

    # Active at start
    cur.execute("""
        SELECT COUNT(*) as count FROM companies
        WHERE status = 'active_client'
        AND (created_at <= ? OR created_at IS NULL)
    """, (start_str,))
    metrics['active_at_start'] = cur.fetchone()['count'] or 0

    # Active at end
    cur.execute("""
        SELECT COUNT(*) as count FROM companies WHERE status = 'active_client'
    """)
    metrics['active_at_end'] = cur.fetchone()['count'] or 0

    # New clients (added in period)
    cur.execute("""
        SELECT COUNT(*) as count FROM companies
        WHERE status = 'active_client' AND created_at BETWEEN ? AND ?
    """, (start_str, end_str))
    metrics['new_clients'] = cur.fetchone()['count'] or 0

    # Churn (changed from active to former)
    cur.execute("""
        SELECT COUNT(*) as count FROM companies
        WHERE status = 'former_client'
        AND updated_at BETWEEN ? AND ?
    """, (start_str, end_str))
    metrics['churn_count'] = cur.fetchone()['count'] or 0

    churn_rate = 0
    if metrics['active_at_start'] > 0:
        churn_rate = (metrics['churn_count'] / metrics['active_at_start']) * 100
    metrics['churn_rate'] = churn_rate

    conn.close()
    return metrics


def get_pipeline_health(db_path: str = None) -> Dict:
    """
    Get current pipeline health snapshot.

    Returns dict with:
        - current_weighted_value: sum of (deal_value * probability) for active deals
        - stage_distribution: dict of stage -> count
        - deal_count: total active deals
        - largest_deal: dict of largest active deal info
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    # Active deals
    cur.execute("""
        SELECT
            d.id, d.company_id, c.name as company_name,
            d.status, d.deal_value,
            COALESCE(d.probability_pct, 10) as prob
        FROM deals d
        JOIN companies c ON d.company_id = c.id
        WHERE d.status NOT IN ('lost', 'dead', 'closed', 'won')
    """)

    deals = [dict(row) for row in cur.fetchall()]

    metrics = {
        'current_weighted_value': 0,
        'stage_distribution': {},
        'deal_count': len(deals),
        'largest_deal': None
    }

    weighted_val = 0
    largest = None
    largest_val = 0

    for deal in deals:
        val = deal.get('deal_value') or 0
        prob = deal.get('prob') or 10
        weighted_val += (val * prob / 100)

        stage = deal.get('status', 'unknown')
        metrics['stage_distribution'][stage] = metrics['stage_distribution'].get(stage, 0) + 1

        if val > largest_val:
            largest_val = val
            largest = deal

    metrics['current_weighted_value'] = weighted_val
    if largest:
        metrics['largest_deal'] = {
            'company': largest['company_name'],
            'value': largest['deal_value'],
            'status': largest['status'],
            'probability': largest['prob']
        }

    conn.close()
    return metrics


def generate_quarterly_review(quarter: Optional[str] = None,
                             year: Optional[int] = None,
                             db_path: str = None) -> str:
    """
    Generate complete markdown QBR report.

    Args:
        quarter: 'Q1', 'Q2', 'Q3', 'Q4' (None = current)
        year: 4-digit year (None = current)
        db_path: Database path

    Returns:
        Markdown formatted report
    """
    if db_path is None:
        db_path = get_db_path()

    start, end = get_quarter_dates(quarter, year)
    quarter = quarter or get_quarter_dates()[0].strftime('%m')
    if quarter and quarter not in ['Q1', 'Q2', 'Q3', 'Q4']:
        month = int(quarter)
        if month <= 3:
            quarter = 'Q1'
        elif month <= 6:
            quarter = 'Q2'
        elif month <= 9:
            quarter = 'Q3'
        else:
            quarter = 'Q4'
    year = year or date.today().year

    metrics = get_quarterly_metrics(start, end, db_path)
    retention = get_client_retention(start, end, db_path)
    pipeline = get_pipeline_health(db_path)

    lines = [
        f"# Quarterly Business Review — {quarter} {year}",
        f"*Period: {start.strftime('%B %d, %Y')} – {end.strftime('%B %d, %Y')}*",
        f"*Generated: {datetime.now().strftime('%B %d, %Y at %H:%M:%S')}*",
        "",
        "## Executive Summary",
        "",
        "### Revenue & Deals",
        f"- **Deals Closed**: {metrics['deals_closed_count']}",
        f"- **Total SF Transacted**: {metrics['total_sf_transacted']:,.0f}",
        f"- **Revenue Generated**: ${metrics['total_revenue']:,.0f}",
        f"- **Win Rate**: {metrics['win_rate']:.1f}%",
        "",
        "### Client Management",
        f"- **Active Clients Start**: {retention['active_at_start']}",
        f"- **Active Clients End**: {retention['active_at_end']}",
        f"- **New Clients Added**: {retention['new_clients']}",
        f"- **Client Churn**: {retention['churn_count']} ({retention['churn_rate']:.1f}%)",
        "",
        "### Business Development",
        f"- **New Relationships**: {metrics['new_relationships_added']}",
        f"- **Signals Captured**: {metrics['signals_captured']}",
        f"- **Outreach Activities**: {metrics['outreach_count']}",
        "",
        "## Pipeline Health",
        f"- **Active Deals**: {pipeline['deal_count']}",
        f"- **Weighted Pipeline Value**: ${pipeline['current_weighted_value']:,.0f}",
    ]

    if pipeline['largest_deal']:
        deal = pipeline['largest_deal']
        lines.extend([
            f"- **Largest Deal**: {deal['company']} (${deal['value']:,.0f}, "
            f"{deal['status']}, {deal['probability']}% prob.)",
        ])

    lines.extend([
        "",
        "### Stage Distribution",
    ])

    for stage in ['prospect', 'pitch', 'tour', 'proposal', 'negotiation', 'signed']:
        count = pipeline['stage_distribution'].get(stage, 0)
        if count > 0:
            lines.append(f"- **{stage.title()}**: {count}")

    lines.extend([
        "",
        "## Performance Metrics",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Deals Closed | {metrics['deals_closed_count']} |",
        f"| Total Revenue | ${metrics['total_revenue']:,.0f} |",
        f"| Average Deal Size | ${metrics['total_revenue']/max(metrics['deals_closed_count'], 1):,.0f} |",
        f"| Win Rate | {metrics['win_rate']:.1f}% |",
        f"| Client Retention | {100 - retention['churn_rate']:.1f}% |",
        f"| New Relationships | {metrics['new_relationships_added']} |",
        f"| Outreach Rate | {metrics['outreach_count']} activities |",
        "",
        "## Key Highlights",
        "",
    ])

    # Add insights
    if metrics['deals_closed_count'] > 0:
        lines.append(f"✓ Closed {metrics['deals_closed_count']} deal(s) generating "
                     f"${metrics['total_revenue']:,.0f} in revenue")
    if metrics['win_rate'] > 70:
        lines.append(f"✓ Strong win rate of {metrics['win_rate']:.1f}%")
    if retention['churn_rate'] < 10:
        lines.append(f"✓ Low client churn of {retention['churn_rate']:.1f}%")
    if metrics['new_relationships_added'] > 20:
        lines.append(f"✓ Added {metrics['new_relationships_added']} new relationships")
    if pipeline['deal_count'] > 10:
        lines.append(f"✓ Pipeline contains {pipeline['deal_count']} active deals")

    if not any([metrics['deals_closed_count'], metrics['win_rate'], retention['churn_rate']]):
        lines.append("• Monitor pipeline progression and conversion rates")
        lines.append("• Increase outreach activities to top prospects")

    lines.extend([
        "",
        "## Recommendations",
        "",
        f"1. **Pipeline Focus**: {pipeline['deal_count']} active deals in pipeline",
        f"   - Prioritize {max(1, pipeline['deal_count']//3)} deals in negotiation stage",
        f"   - Expected close value: ${pipeline['current_weighted_value']:,.0f}",
        "",
        f"2. **Client Retention**: {retention['churn_rate']:.1f}% churn rate",
        f"   - Continue engagement with {retention['active_at_end']} active clients",
        f"   - Added {retention['new_clients']} new clients this quarter",
        "",
        f"3. **Business Development**: {metrics['outreach_count']} outreach activities",
        f"   - Generated {metrics['signals_captured']} market signals",
        f"   - Established {metrics['new_relationships_added']} new relationships",
        "",
    ])

    return "\n".join(lines)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Generate quarterly business review")
    parser.add_argument("--quarter", type=str, default=None,
                       help="Quarter: Q1, Q2, Q3, Q4 (default: current)")
    parser.add_argument("--year", type=int, default=None,
                       help="Year (default: current)")
    parser.add_argument("--db", type=str, default=None,
                       help="Database path (optional)")

    args = parser.parse_args()

    try:
        report = generate_quarterly_review(args.quarter, args.year, args.db)
        print(report)
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
