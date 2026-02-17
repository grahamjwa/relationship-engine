import sys
sys.path.insert(0, '/sessions/sharp-admiring-curie/relationship_engine')

import sqlite3
import logging
import argparse
from datetime import datetime
from core.graph_engine import get_db_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_timing_stats(db_path=None):
    """
    Group outreach_log by day of week and hour, compute success rate.

    Success = outcome in ('connected', 'meeting_set', 'replied')

    Returns:
        dict: Statistics by day_of_week and hour with success rates
    """
    if db_path is None:
        db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get stats by day of week
        query_days = """
            SELECT
                strftime('%w', created_date) as day_of_week,
                COUNT(*) as total,
                SUM(CASE WHEN outcome IN ('connected', 'meeting_set', 'replied') THEN 1 ELSE 0 END) as successes
            FROM outreach_log
            GROUP BY day_of_week
            ORDER BY day_of_week
        """

        cursor.execute(query_days)
        day_rows = cursor.fetchall()

        day_names = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

        timing_stats = {
            'by_day': {},
            'by_hour': {}
        }

        for row in day_rows:
            day_num = int(row['day_of_week'])
            day_name = day_names[day_num]
            total = row['total']
            successes = row['successes']
            success_rate = (successes / total * 100) if total > 0 else 0

            timing_stats['by_day'][day_name] = {
                'day': day_name,
                'total': total,
                'successes': successes,
                'success_rate': success_rate,
            }

        # Get stats by hour
        query_hours = """
            SELECT
                strftime('%H', created_date) as hour,
                COUNT(*) as total,
                SUM(CASE WHEN outcome IN ('connected', 'meeting_set', 'replied') THEN 1 ELSE 0 END) as successes
            FROM outreach_log
            GROUP BY hour
            ORDER BY hour
        """

        cursor.execute(query_hours)
        hour_rows = cursor.fetchall()

        for row in hour_rows:
            hour = int(row['hour'])
            total = row['total']
            successes = row['successes']
            success_rate = (successes / total * 100) if total > 0 else 0

            timing_stats['by_hour'][hour] = {
                'hour': hour,
                'total': total,
                'successes': successes,
                'success_rate': success_rate,
            }

        conn.close()
        logger.info(f"Timing stats computed for {len(timing_stats['by_day'])} days and {len(timing_stats['by_hour'])} hours")
        return timing_stats

    except Exception as e:
        logger.error(f"Error computing timing stats: {e}")
        return {'by_day': {}, 'by_hour': {}}


def best_days(db_path=None):
    """
    Return days ranked by success rate.

    Returns:
        list: Sorted list of day dicts, best first
    """
    timing_stats = get_timing_stats(db_path)

    if not timing_stats.get('by_day'):
        return []

    sorted_list = sorted(
        timing_stats['by_day'].values(),
        key=lambda x: x.get('success_rate', 0),
        reverse=True
    )

    logger.info(f"Ranked {len(sorted_list)} days by success rate")
    return sorted_list


def best_hours(db_path=None):
    """
    Return hours ranked by success rate.

    Returns:
        list: Sorted list of hour dicts, best first
    """
    timing_stats = get_timing_stats(db_path)

    if not timing_stats.get('by_hour'):
        return []

    sorted_list = sorted(
        timing_stats['by_hour'].values(),
        key=lambda x: x.get('success_rate', 0),
        reverse=True
    )

    logger.info(f"Ranked {len(sorted_list)} hours by success rate")
    return sorted_list


def generate_timing_report(db_path=None):
    """
    Generate a markdown report of success rates by day and hour.

    Returns:
        str: Markdown formatted report
    """
    best_day_list = best_days(db_path)
    best_hour_list = best_hours(db_path)

    if not best_day_list and not best_hour_list:
        return "# Timing Analysis Report\n\nNo outreach data available."

    report = """# Timing Analysis Report

## Best Days for Outreach

| Day | Total | Successes | Success Rate |
|---|---|---|---|
"""

    for item in best_day_list:
        report += f"| {item['day']} | {item['total']} | {item['successes']} | {item['success_rate']:.1f}% |\n"

    report += """
## Best Hours for Outreach

| Hour | Total | Successes | Success Rate |
|---|---|---|---|
"""

    for item in best_hour_list:
        hour_str = f"{item['hour']:02d}:00"
        report += f"| {hour_str} | {item['total']} | {item['successes']} | {item['success_rate']:.1f}% |\n"

    # Find top performers
    if best_day_list:
        top_day = best_day_list[0]
        report += f"\n**Best Day:** {top_day['day']} ({top_day['success_rate']:.1f}%)\n"

    if best_hour_list:
        top_hour = best_hour_list[0]
        report += f"**Best Hour:** {top_hour['hour']:02d}:00 ({top_hour['success_rate']:.1f}%)\n"

    report += f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

    logger.info("Timing report generated successfully")
    return report


def main():
    parser = argparse.ArgumentParser(description='Analyze outreach timing for best days/hours')
    parser.add_argument('--db-path', type=str, help='Database path')
    parser.add_argument('--report', action='store_true', help='Generate markdown report')

    args = parser.parse_args()

    if args.report:
        report = generate_timing_report(args.db_path)
        print(report)
    else:
        best_day_list = best_days(args.db_path)
        best_hour_list = best_hours(args.db_path)

        print("Best Days for Outreach:")
        for i, item in enumerate(best_day_list, 1):
            print(f"{i}. {item['day']}: {item['success_rate']:.1f}% ({item['successes']}/{item['total']})")

        print("\nBest Hours for Outreach:")
        for i, item in enumerate(best_hour_list, 1):
            print(f"{i}. {item['hour']:02d}:00: {item['success_rate']:.1f}% ({item['successes']}/{item['total']})")


if __name__ == '__main__':
    main()
