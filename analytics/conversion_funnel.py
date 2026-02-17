import sys
sys.path.insert(0, '/sessions/sharp-admiring-curie/relationship_engine')

import sqlite3
import logging
import argparse
from datetime import datetime, timedelta
from core.graph_engine import get_db_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_funnel_metrics(start_date=None, end_date=None, db_path=None):
    """
    Query outreach_log and deals to compute conversion metrics.

    Returns:
        dict: Contains total_outreach, meetings_set, proposals, closed, and conversion rates
    """
    if db_path is None:
        db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Build date filters
        date_filter = ""
        params = []
        if start_date:
            date_filter += " AND o.created_date >= ?"
            params.append(start_date)
        if end_date:
            date_filter += " AND o.created_date <= ?"
            params.append(end_date)

        # Get total outreach
        query_outreach = f"""
            SELECT COUNT(*) as total FROM outreach_log o
            WHERE 1=1 {date_filter}
        """
        cursor.execute(query_outreach, params)
        total_outreach = cursor.fetchone()['total']

        # Get meetings set (meeting_set outcome)
        query_meetings = f"""
            SELECT COUNT(*) as total FROM outreach_log o
            WHERE o.outcome = 'meeting_set' {date_filter}
        """
        cursor.execute(query_meetings, params)
        meetings_set = cursor.fetchone()['total']

        # Get proposals (deals in stage 'proposal' or higher)
        query_proposals = f"""
            SELECT COUNT(*) as total FROM deals d
            JOIN outreach_log o ON d.id = o.deal_id
            WHERE d.stage IN ('proposal', 'negotiation', 'closed_won', 'closed_lost') {date_filter}
        """
        cursor.execute(query_proposals, params)
        proposals = cursor.fetchone()['total']

        # Get closed deals (closed_won)
        query_closed = f"""
            SELECT COUNT(*) as total FROM deals d
            JOIN outreach_log o ON d.id = o.deal_id
            WHERE d.stage = 'closed_won' {date_filter}
        """
        cursor.execute(query_closed, params)
        closed = cursor.fetchone()['total']

        conn.close()

        # Calculate conversion rates
        metrics = {
            'total_outreach': total_outreach,
            'meetings_set': meetings_set,
            'proposals': proposals,
            'closed': closed,
            'outreach_to_meeting_rate': (meetings_set / total_outreach * 100) if total_outreach > 0 else 0,
            'meeting_to_proposal_rate': (proposals / meetings_set * 100) if meetings_set > 0 else 0,
            'proposal_to_close_rate': (closed / proposals * 100) if proposals > 0 else 0,
            'overall_conversion_rate': (closed / total_outreach * 100) if total_outreach > 0 else 0,
        }

        logger.info(f"Funnel metrics computed: {total_outreach} outreach, {meetings_set} meetings, {proposals} proposals, {closed} closed")
        return metrics

    except Exception as e:
        logger.error(f"Error computing funnel metrics: {e}")
        return {}


def generate_funnel_report(start_date=None, end_date=None, db_path=None):
    """
    Generate a markdown funnel report with percentages.

    Returns:
        str: Markdown formatted report
    """
    metrics = get_funnel_metrics(start_date, end_date, db_path)

    if not metrics:
        return "# Conversion Funnel Report\n\nNo data available."

    date_range = "All Time"
    if start_date and end_date:
        date_range = f"{start_date} to {end_date}"
    elif start_date:
        date_range = f"From {start_date}"
    elif end_date:
        date_range = f"Until {end_date}"

    report = f"""# Conversion Funnel Report

**Date Range:** {date_range}

## Funnel Stages

| Stage | Count | Conversion Rate |
|-------|-------|-----------------|
| Total Outreach | {metrics['total_outreach']} | 100.0% |
| Meetings Set | {metrics['meetings_set']} | {metrics['outreach_to_meeting_rate']:.1f}% |
| Proposals | {metrics['proposals']} | {metrics['meeting_to_proposal_rate']:.1f}% |
| Closed Won | {metrics['closed']} | {metrics['proposal_to_close_rate']:.1f}% |

## Overall Metrics

- **Overall Conversion Rate:** {metrics['overall_conversion_rate']:.2f}%
- **Outreach to Meeting:** {metrics['outreach_to_meeting_rate']:.2f}%
- **Meeting to Proposal:** {metrics['meeting_to_proposal_rate']:.2f}%
- **Proposal to Close:** {metrics['proposal_to_close_rate']:.2f}%

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

    logger.info("Funnel report generated successfully")
    return report


def main():
    parser = argparse.ArgumentParser(description='Analyze conversion funnel metrics')
    parser.add_argument('--days', type=int, help='Analyze last N days')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--db-path', type=str, help='Database path')
    parser.add_argument('--report', action='store_true', help='Generate markdown report')

    args = parser.parse_args()

    start_date = None
    end_date = None

    if args.days:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=args.days)
        start_date = start_date.isoformat()
        end_date = end_date.isoformat()

    if args.start_date:
        start_date = args.start_date
    if args.end_date:
        end_date = args.end_date

    if args.report or args.days:
        report = generate_funnel_report(start_date, end_date, args.db_path)
        print(report)
    else:
        metrics = get_funnel_metrics(start_date, end_date, args.db_path)
        print("Funnel Metrics:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")


if __name__ == '__main__':
    main()
