import sys
sys.path.insert(0, '/sessions/sharp-admiring-curie/relationship_engine')

import sqlite3
import logging
import argparse
from collections import defaultdict
from datetime import datetime
from core.graph_engine import get_db_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_response_rates(db_path=None):
    """
    Group outreach_log by outreach_type and channel, compute response rate.

    Response = outcome != 'no_answer' and outcome != 'bounced'

    Returns:
        dict: Response rates by outreach_type and channel
    """
    if db_path is None:
        db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get all outreach by type and channel
        query = """
            SELECT
                outreach_type,
                channel,
                COUNT(*) as total,
                SUM(CASE WHEN outcome NOT IN ('no_answer', 'bounced') THEN 1 ELSE 0 END) as responses
            FROM outreach_log
            GROUP BY outreach_type, channel
            ORDER BY outreach_type, channel
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        response_rates = {}
        for row in rows:
            outreach_type = row['outreach_type']
            channel = row['channel']
            total = row['total']
            responses = row['responses']
            response_rate = (responses / total * 100) if total > 0 else 0

            key = f"{outreach_type}:{channel}"
            response_rates[key] = {
                'outreach_type': outreach_type,
                'channel': channel,
                'total': total,
                'responses': responses,
                'response_rate': response_rate,
            }

        conn.close()
        logger.info(f"Response rates computed for {len(response_rates)} type-channel combinations")
        return response_rates

    except Exception as e:
        logger.error(f"Error computing response rates: {e}")
        return {}


def get_best_performing(metric='response_rate', db_path=None):
    """
    Return ranked outreach types by metric.

    Args:
        metric: 'response_rate' or 'total' or 'responses'

    Returns:
        list: Sorted list of dicts with outreach info, best first
    """
    response_rates = get_response_rates(db_path)

    if not response_rates:
        return []

    sorted_list = sorted(
        response_rates.values(),
        key=lambda x: x.get(metric, 0),
        reverse=True
    )

    logger.info(f"Ranked {len(sorted_list)} outreach types by {metric}")
    return sorted_list


def generate_response_report(db_path=None):
    """
    Generate a markdown report of response rates by outreach type and channel.

    Returns:
        str: Markdown formatted report
    """
    best_performing = get_best_performing('response_rate', db_path)

    if not best_performing:
        return "# Response Analysis Report\n\nNo outreach data available."

    report = """# Response Analysis Report

## Best Performing Outreach Types

| Outreach Type | Channel | Total | Responses | Response Rate |
|---|---|---|---|---|
"""

    for item in best_performing:
        report += f"| {item['outreach_type']} | {item['channel']} | {item['total']} | {item['responses']} | {item['response_rate']:.1f}% |\n"

    # Summary stats
    total_outreach = sum(item['total'] for item in best_performing)
    total_responses = sum(item['responses'] for item in best_performing)
    overall_rate = (total_responses / total_outreach * 100) if total_outreach > 0 else 0

    report += f"""
## Summary

- **Total Outreach:** {total_outreach}
- **Total Responses:** {total_responses}
- **Overall Response Rate:** {overall_rate:.2f}%

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

    logger.info("Response report generated successfully")
    return report


def main():
    parser = argparse.ArgumentParser(description='Analyze outreach response rates')
    parser.add_argument('--metric', type=str, default='response_rate',
                        choices=['response_rate', 'total', 'responses'],
                        help='Metric to rank by')
    parser.add_argument('--db-path', type=str, help='Database path')
    parser.add_argument('--report', action='store_true', help='Generate markdown report')

    args = parser.parse_args()

    if args.report:
        report = generate_response_report(args.db_path)
        print(report)
    else:
        best = get_best_performing(args.metric, args.db_path)
        print(f"Best Performing Outreach Types (by {args.metric}):")
        for i, item in enumerate(best, 1):
            print(f"{i}. {item['outreach_type']} ({item['channel']}): {item['response_rate']:.1f}% ({item['responses']}/{item['total']})")


if __name__ == '__main__':
    main()
