import sys
sys.path.insert(0, '/sessions/sharp-admiring-curie/relationship_engine')

import sqlite3
import logging
import argparse
from datetime import datetime
from core.graph_engine import get_db_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_sector_metrics(db_path=None):
    """
    Join companies with deals and outreach, compute per-sector metrics.

    Returns:
        dict: Metrics by sector including deal count, win rate, total value, etc.
    """
    if db_path is None:
        db_path = get_db_path()

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get sector metrics
        query = """
            SELECT
                c.sector,
                COUNT(DISTINCT d.id) as deal_count,
                SUM(CASE WHEN d.stage = 'closed_won' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN d.stage = 'closed_won' THEN d.value ELSE 0 END) as total_value,
                COUNT(DISTINCT o.id) as outreach_count
            FROM companies c
            LEFT JOIN deals d ON c.id = d.company_id
            LEFT JOIN outreach_log o ON c.id = o.company_id
            WHERE c.sector IS NOT NULL AND c.sector != ''
            GROUP BY c.sector
            ORDER BY c.sector
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        sector_metrics = {}

        for row in rows:
            sector = row['sector']
            deal_count = row['deal_count'] or 0
            wins = row['wins'] or 0
            total_value = row['total_value'] or 0
            outreach_count = row['outreach_count'] or 0

            win_rate = (wins / deal_count * 100) if deal_count > 0 else 0
            avg_deal_size = (total_value / wins) if wins > 0 else 0
            conversion_rate = (deal_count / outreach_count * 100) if outreach_count > 0 else 0

            sector_metrics[sector] = {
                'sector': sector,
                'deal_count': deal_count,
                'wins': wins,
                'win_rate': win_rate,
                'total_value': total_value,
                'avg_deal_size': avg_deal_size,
                'outreach_count': outreach_count,
                'conversion_rate': conversion_rate,
            }

        conn.close()
        logger.info(f"Sector metrics computed for {len(sector_metrics)} sectors")
        return sector_metrics

    except Exception as e:
        logger.error(f"Error computing sector metrics: {e}")
        return {}


def rank_sectors(metric='win_rate', db_path=None):
    """
    Return ranked list of sectors by specified metric.

    Args:
        metric: 'win_rate', 'total_value', 'avg_deal_size', 'conversion_rate', 'deal_count'

    Returns:
        list: Sorted list of sector dicts, best first
    """
    sector_metrics = get_sector_metrics(db_path)

    if not sector_metrics:
        return []

    sorted_list = sorted(
        sector_metrics.values(),
        key=lambda x: x.get(metric, 0),
        reverse=True
    )

    logger.info(f"Ranked {len(sorted_list)} sectors by {metric}")
    return sorted_list


def generate_sector_report(db_path=None):
    """
    Generate a markdown report of sector performance metrics.

    Returns:
        str: Markdown formatted report
    """
    ranked_by_win_rate = rank_sectors('win_rate', db_path)
    ranked_by_value = rank_sectors('total_value', db_path)

    if not ranked_by_win_rate:
        return "# Sector Performance Report\n\nNo sector data available."

    report = """# Sector Performance Report

## Sectors by Win Rate

| Sector | Deals | Wins | Win Rate | Total Value | Avg Deal Size |
|---|---|---|---|---|---|
"""

    for item in ranked_by_win_rate:
        if item['deal_count'] > 0:  # Only show sectors with deals
            report += f"| {item['sector']} | {item['deal_count']} | {item['wins']} | {item['win_rate']:.1f}% | ${item['total_value']:,.0f} | ${item['avg_deal_size']:,.0f} |\n"

    report += """
## Sectors by Total Value

| Sector | Total Value | Avg Deal Size | Deals | Win Rate |
|---|---|---|---|---|
"""

    for item in ranked_by_value:
        if item['deal_count'] > 0:
            report += f"| {item['sector']} | ${item['total_value']:,.0f} | ${item['avg_deal_size']:,.0f} | {item['deal_count']} | {item['win_rate']:.1f}% |\n"

    report += """
## Sectors by Conversion Rate

| Sector | Outreach | Deals | Conversion Rate |
|---|---|---|---|
"""

    ranked_by_conversion = rank_sectors('conversion_rate', db_path)
    for item in ranked_by_conversion:
        if item['outreach_count'] > 0:
            report += f"| {item['sector']} | {item['outreach_count']} | {item['deal_count']} | {item['conversion_rate']:.1f}% |\n"

    # Summary
    total_sectors = len(ranked_by_win_rate)
    total_deals = sum(item['deal_count'] for item in ranked_by_win_rate)
    total_wins = sum(item['wins'] for item in ranked_by_win_rate)
    total_value = sum(item['total_value'] for item in ranked_by_win_rate)
    overall_win_rate = (total_wins / total_deals * 100) if total_deals > 0 else 0

    report += f"""
## Summary

- **Total Sectors:** {total_sectors}
- **Total Deals:** {total_deals}
- **Total Wins:** {total_wins}
- **Overall Win Rate:** {overall_win_rate:.2f}%
- **Total Pipeline Value:** ${total_value:,.0f}

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

    logger.info("Sector report generated successfully")
    return report


def main():
    parser = argparse.ArgumentParser(description='Analyze sector performance metrics')
    parser.add_argument('--metric', type=str, default='win_rate',
                        choices=['win_rate', 'total_value', 'avg_deal_size', 'conversion_rate', 'deal_count'],
                        help='Metric to rank by')
    parser.add_argument('--db-path', type=str, help='Database path')
    parser.add_argument('--report', action='store_true', help='Generate markdown report')

    args = parser.parse_args()

    if args.report:
        report = generate_sector_report(args.db_path)
        print(report)
    else:
        ranked = rank_sectors(args.metric, args.db_path)
        print(f"Sectors Ranked by {args.metric.replace('_', ' ').title()}:")
        for i, item in enumerate(ranked, 1):
            if args.metric == 'win_rate':
                print(f"{i}. {item['sector']}: {item['win_rate']:.1f}% win rate ({item['wins']}/{item['deal_count']} deals)")
            elif args.metric == 'total_value':
                print(f"{i}. {item['sector']}: ${item['total_value']:,.0f} total value")
            elif args.metric == 'avg_deal_size':
                print(f"{i}. {item['sector']}: ${item['avg_deal_size']:,.0f} avg deal size")
            elif args.metric == 'conversion_rate':
                print(f"{i}. {item['sector']}: {item['conversion_rate']:.1f}% conversion rate")
            elif args.metric == 'deal_count':
                print(f"{i}. {item['sector']}: {item['deal_count']} deals")


if __name__ == '__main__':
    main()
