"""
Deal Pipeline Module for Relationship Engine
Tracks active deals through stages with probability weighting and revenue projection.
"""

import os
import sys
import sqlite3
import logging
import argparse
from typing import Optional, Dict, List
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core.graph_engine import get_db_path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Deal stage probability mapping
STAGES = {
    'prospect': 10,      # Early stage awareness
    'pitch': 20,         # Pitch delivered
    'tour': 30,          # Property tour completed
    'proposal': 50,      # Formal proposal submitted
    'negotiation': 70,   # Terms being negotiated
    'signed': 95,        # Deal signed, pending closing
}


def _get_conn(db_path: str = None) -> sqlite3.Connection:
    """Get database connection."""
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_deals_table(db_path: str = None) -> bool:
    """Ensure deals table exists with required columns."""
    if db_path is None:
        db_path = get_db_path()

    try:
        conn = _get_conn(db_path)
        cur = conn.cursor()

        # Create deals table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                deal_type TEXT,
                status TEXT DEFAULT 'prospect',
                square_feet REAL,
                annual_rent REAL,
                deal_value REAL,
                commission_rate REAL DEFAULT 0.06,
                probability_pct INTEGER,
                started_date TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                FOREIGN KEY (company_id) REFERENCES companies(id)
            )
        """)

        # Add missing columns if they don't exist
        for col, col_type in [
            ('probability_pct', 'INTEGER'),
            ('commission_rate', 'REAL'),
            ('started_date', 'TEXT'),
        ]:
            try:
                cur.execute(f"ALTER TABLE deals ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to ensure deals table: {e}")
        return False


def get_pipeline(status_filter: Optional[List[str]] = None,
                db_path: str = None) -> List[Dict]:
    """
    Get all active deals with stage, probability, and expected value.

    Args:
        status_filter: List of statuses to include (e.g., ['prospect', 'pitch', 'proposal'])
                      None = all active deals (exclude 'lost', 'dead', 'closed')
        db_path: Database path

    Returns:
        List of deal dicts with calculated expected value
    """
    if db_path is None:
        db_path = get_db_path()

    _ensure_deals_table(db_path)

    conn = _get_conn(db_path)
    cur = conn.cursor()

    if status_filter is None:
        # Active deals
        status_clause = "status NOT IN ('lost', 'dead', 'closed', 'won')"
    else:
        placeholders = ','.join(['?' for _ in status_filter])
        status_clause = f"status IN ({placeholders})"

    query = f"""
        SELECT
            d.id, d.company_id, c.name as company_name,
            d.deal_type, d.status,
            d.square_feet, d.annual_rent, d.deal_value,
            d.commission_rate, d.probability_pct,
            d.started_date, d.created_at, d.updated_at, d.notes
        FROM deals d
        JOIN companies c ON d.company_id = c.id
        WHERE {status_clause}
        ORDER BY d.probability_pct DESC, d.deal_value DESC
    """

    if status_filter is None:
        cur.execute(query)
    else:
        cur.execute(query, status_filter)

    deals = []
    for row in cur.fetchall():
        deal = dict(row)

        # Calculate expected value
        deal_val = deal.get('deal_value') or 0
        prob = deal.get('probability_pct') or STAGES.get(deal.get('status'), 10)
        commission_rate = deal.get('commission_rate') or 0.06

        deal['probability_pct'] = prob
        deal['expected_value'] = (deal_val * prob / 100) * commission_rate
        deal['commission_amount'] = deal_val * commission_rate if deal_val else 0

        deals.append(deal)

    conn.close()
    return deals


def advance_deal(deal_id: int, new_status: str, notes: str = None,
                db_path: str = None) -> bool:
    """
    Update deal status to next stage.

    Args:
        deal_id: Deal ID
        new_status: New status (prospect, pitch, tour, proposal, negotiation, signed, lost, won)
        notes: Optional notes about the advancement
        db_path: Database path

    Returns:
        True if successful, False otherwise
    """
    if db_path is None:
        db_path = get_db_path()

    if new_status not in list(STAGES.keys()) + ['lost', 'won', 'dead', 'closed']:
        logger.error(f"Invalid status: {new_status}")
        return False

    try:
        conn = _get_conn(db_path)
        cur = conn.cursor()

        # Get current deal
        cur.execute("SELECT * FROM deals WHERE id = ?", (deal_id,))
        deal = cur.fetchone()

        if not deal:
            logger.error(f"Deal {deal_id} not found")
            conn.close()
            return False

        # Update status and probability
        prob = STAGES.get(new_status, 10)
        context_note = notes or ""
        if deal['status'] != new_status:
            context_note = f"Status: {deal['status']} → {new_status}. {context_note}".strip()

        cur.execute("""
            UPDATE deals
            SET status = ?, probability_pct = ?, updated_at = CURRENT_TIMESTAMP, notes = ?
            WHERE id = ?
        """, (new_status, prob, context_note, deal_id))

        conn.commit()
        conn.close()

        logger.info(f"Deal {deal_id} advanced to {new_status} (probability: {prob}%)")
        return True
    except Exception as e:
        logger.error(f"Failed to advance deal: {e}")
        return False


def pipeline_summary(db_path: str = None) -> Dict:
    """
    Get aggregate pipeline metrics.

    Returns dict with:
        - deals_by_stage: dict of stage -> count
        - total_pipeline_value: sum of all deal values
        - weighted_pipeline_value: sum of (deal_value * probability)
        - average_deal_size: mean deal value
        - total_expected_commission: sum of expected commissions
        - deal_count: total number of deals
    """
    if db_path is None:
        db_path = get_db_path()

    deals = get_pipeline(db_path=db_path)

    summary = {
        'deals_by_stage': {},
        'total_pipeline_value': 0,
        'weighted_pipeline_value': 0,
        'total_expected_commission': 0,
        'average_deal_size': 0,
        'deal_count': len(deals)
    }

    if not deals:
        return summary

    # Count by stage and aggregate values
    stage_counts = {}
    for deal in deals:
        status = deal.get('status', 'unknown')
        stage_counts[status] = stage_counts.get(status, 0) + 1

        deal_val = deal.get('deal_value') or 0
        summary['total_pipeline_value'] += deal_val
        summary['weighted_pipeline_value'] += deal.get('expected_value', 0) / (
            deal.get('commission_rate') or 0.06
        )
        summary['total_expected_commission'] += deal.get('expected_value', 0)

    summary['deals_by_stage'] = stage_counts
    summary['average_deal_size'] = summary['total_pipeline_value'] / len(deals) if deals else 0

    return summary


def generate_pipeline_report(db_path: str = None) -> str:
    """
    Generate a markdown-formatted pipeline report.

    Returns:
        Markdown string with pipeline overview and details
    """
    if db_path is None:
        db_path = get_db_path()

    deals = get_pipeline(db_path=db_path)
    summary = pipeline_summary(db_path=db_path)

    lines = [
        "# Deal Pipeline Report",
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
        "",
        "## Summary",
        f"- **Total Deals**: {summary['deal_count']}",
        f"- **Total Pipeline Value**: ${summary['total_pipeline_value']:,.0f}",
        f"- **Weighted Pipeline Value**: ${summary['weighted_pipeline_value']:,.0f}",
        f"- **Average Deal Size**: ${summary['average_deal_size']:,.0f}",
        f"- **Expected Commission**: ${summary['total_expected_commission']:,.0f}",
        "",
        "## By Stage",
    ]

    # Add stage breakdown
    for stage in ['prospect', 'pitch', 'tour', 'proposal', 'negotiation', 'signed']:
        count = summary['deals_by_stage'].get(stage, 0)
        stage_deals = [d for d in deals if d.get('status') == stage]
        stage_value = sum(d.get('deal_value', 0) for d in stage_deals)
        if count > 0:
            lines.append(f"- **{stage.title()}**: {count} deals, ${stage_value:,.0f}")

    lines.extend([
        "",
        "## Active Deals",
        ""
    ])

    # Group deals by stage for detail view
    for stage in ['prospect', 'pitch', 'tour', 'proposal', 'negotiation', 'signed']:
        stage_deals = [d for d in deals if d.get('status') == stage]
        if not stage_deals:
            continue

        lines.append(f"### {stage.title()} ({len(stage_deals)} deals)")
        lines.append("")

        for deal in sorted(stage_deals, key=lambda x: x.get('deal_value', 0), reverse=True):
            company = deal.get('company_name', 'Unknown')
            deal_type = deal.get('deal_type', 'General')
            value = deal.get('deal_value', 0)
            prob = deal.get('probability_pct', 10)
            expected = deal.get('expected_value', 0)
            sf = deal.get('square_feet', 0)

            sf_str = f" | {sf:,.0f} SF" if sf else ""
            lines.append(f"- **{company}** ({deal_type}){sf_str}")
            lines.append(f"  Value: ${value:,.0f} | Probability: {prob}% | Expected: ${expected:,.0f}")

        lines.append("")

    return "\n".join(lines)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Deal pipeline management")
    parser.add_argument("--summary", action="store_true",
                       help="Print pipeline summary")
    parser.add_argument("--report", action="store_true",
                       help="Generate full markdown report")
    parser.add_argument("--db", type=str, default=None,
                       help="Database path (optional)")
    parser.add_argument("--advance", type=int, default=None,
                       help="Deal ID to advance")
    parser.add_argument("--status", type=str, default=None,
                       help="New status for deal (when using --advance)")
    parser.add_argument("--notes", type=str, default=None,
                       help="Notes for deal advancement")

    args = parser.parse_args()

    if args.advance:
        if not args.status:
            print("Error: --status required when using --advance")
            return 1
        success = advance_deal(args.advance, args.status, args.notes, args.db)
        if success:
            print(f"✓ Deal {args.advance} advanced to {args.status}")
        else:
            print(f"✗ Failed to advance deal")
            return 1
    elif args.report:
        report = generate_pipeline_report(args.db)
        print(report)
    else:
        # Default: show summary
        summary = pipeline_summary(args.db)
        deals = get_pipeline(db_path=args.db)

        print("\n" + "="*70)
        print("DEAL PIPELINE SUMMARY")
        print("="*70)
        print(f"\nTotal Deals: {summary['deal_count']}")
        print(f"Total Pipeline Value: ${summary['total_pipeline_value']:,.0f}")
        print(f"Weighted Pipeline Value: ${summary['weighted_pipeline_value']:,.0f}")
        print(f"Expected Commission: ${summary['total_expected_commission']:,.0f}")
        print(f"Average Deal Size: ${summary['average_deal_size']:,.0f}")

        print("\nBy Stage:")
        for stage in ['prospect', 'pitch', 'tour', 'proposal', 'negotiation', 'signed']:
            count = summary['deals_by_stage'].get(stage, 0)
            if count > 0:
                stage_deals = [d for d in deals if d.get('status') == stage]
                value = sum(d.get('deal_value', 0) for d in stage_deals)
                print(f"  {stage.title():15s}: {count:2d} deals | ${value:>12,.0f}")

        print("\nTop 5 Deals by Value:")
        for i, deal in enumerate(sorted(deals, key=lambda x: x.get('deal_value', 0), reverse=True)[:5], 1):
            print(f"  {i}. {deal['company_name']:30s} | "
                  f"${deal.get('deal_value', 0):>10,.0f} | "
                  f"{deal.get('status'):15s} ({deal.get('probability_pct', 10)}%)")

    return 0


if __name__ == "__main__":
    exit(main())
