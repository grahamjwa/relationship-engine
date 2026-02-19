"""
Weekly Report Generator — Auto-generate weekly markdown reports.

Sections:
  - Meetings This Week
  - Deals Moved Forward
  - New Opportunities
  - Follow-ups Completed
  - Signals Detected
  - Next Week Priorities
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path


def _get_conn(db_path=None):
    conn = sqlite3.connect(db_path or get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def generate_weekly_report(week_ending=None, db_path=None):
    """Generate weekly report data. Returns dict with all sections."""
    if week_ending is None:
        week_ending = datetime.now().strftime('%Y-%m-%d')

    # Calculate week range
    end_date = datetime.strptime(week_ending, '%Y-%m-%d')
    start_date = end_date - timedelta(days=7)
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    next_week_end = (end_date + timedelta(days=7)).strftime('%Y-%m-%d')

    conn = _get_conn(db_path)
    cur = conn.cursor()

    report = {
        'week_ending': week_ending,
        'start_date': start_str,
        'end_date': end_str,
    }

    # 1. Meetings This Week
    cur.execute("""
        SELECT o.outreach_date, c.name as company, o.outreach_type, o.outcome, o.notes
        FROM outreach_log o
        LEFT JOIN companies c ON o.target_company_id = c.id
        WHERE o.outreach_date BETWEEN ? AND ?
        AND o.outreach_type IN ('in_person', 'meeting', 'call')
        ORDER BY o.outreach_date ASC
    """, (start_str, end_str))
    report['meetings'] = [dict(r) for r in cur.fetchall()]

    # 2. Deals Moved Forward (deal_stages changes)
    try:
        cur.execute("""
            SELECT company_name, stage, building_address, square_feet,
                   estimated_value, stage_entered_at
            FROM deal_stages
            WHERE stage_entered_at BETWEEN ? AND ?
            ORDER BY stage_entered_at ASC
        """, (start_str, end_str))
        report['deals_moved'] = [dict(r) for r in cur.fetchall()]
    except Exception:
        report['deals_moved'] = []

    # 3. New Opportunities (companies with recent score increases or new additions)
    cur.execute("""
        SELECT name, status, opportunity_score, sector
        FROM companies
        WHERE created_at BETWEEN ? AND ?
        OR opportunity_score >= 70
        ORDER BY opportunity_score DESC
        LIMIT 10
    """, (start_str, end_str))
    report['new_opportunities'] = [dict(r) for r in cur.fetchall()]

    # 4. Follow-ups Completed
    cur.execute("""
        SELECT o.outreach_date, c.name as company, o.outreach_type, o.outcome
        FROM outreach_log o
        LEFT JOIN companies c ON o.target_company_id = c.id
        WHERE o.outreach_date BETWEEN ? AND ?
        AND o.follow_up_done = 1
        ORDER BY o.outreach_date ASC
    """, (start_str, end_str))
    report['followups_completed'] = [dict(r) for r in cur.fetchall()]

    # 5. Signals Detected
    # Funding
    cur.execute("""
        SELECT c.name, f.round_type, f.amount, f.event_date
        FROM funding_events f
        JOIN companies c ON f.company_id = c.id
        WHERE f.event_date BETWEEN ? AND ?
        ORDER BY f.amount DESC
    """, (start_str, end_str))
    report['funding_signals'] = [dict(r) for r in cur.fetchall()]

    # Hiring
    cur.execute("""
        SELECT c.name, h.signal_type, h.signal_date, h.description as detail
        FROM hiring_signals h
        JOIN companies c ON h.company_id = c.id
        WHERE h.signal_date BETWEEN ? AND ?
        ORDER BY h.signal_date DESC
    """, (start_str, end_str))
    report['hiring_signals'] = [dict(r) for r in cur.fetchall()]

    # Executive changes
    try:
        cur.execute("""
            SELECT person_name, new_title, new_company, old_company, change_type, effective_date
            FROM executive_changes
            WHERE effective_date BETWEEN ? AND ?
            ORDER BY
                CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END
        """, (start_str, end_str))
        report['exec_changes'] = [dict(r) for r in cur.fetchall()]
    except Exception:
        report['exec_changes'] = []

    # 6. Next Week Priorities (upcoming follow-ups)
    cur.execute("""
        SELECT o.follow_up_date, c.name as company, o.outreach_type, o.notes
        FROM outreach_log o
        LEFT JOIN companies c ON o.target_company_id = c.id
        WHERE o.follow_up_done = 0
        AND o.follow_up_date BETWEEN ? AND ?
        ORDER BY o.follow_up_date ASC
    """, (end_str, next_week_end))
    report['next_week_followups'] = [dict(r) for r in cur.fetchall()]

    # Overdue
    cur.execute("""
        SELECT o.follow_up_date, c.name as company, o.outreach_type
        FROM outreach_log o
        LEFT JOIN companies c ON o.target_company_id = c.id
        WHERE o.follow_up_done = 0 AND o.follow_up_date < ?
        ORDER BY o.follow_up_date ASC
    """, (end_str,))
    report['overdue'] = [dict(r) for r in cur.fetchall()]

    conn.close()
    return report


def format_report_markdown(report):
    """Format report data as markdown."""
    lines = []
    lines.append(f"# Weekly Report — Week Ending {report['week_ending']}")
    lines.append(f"*{report['start_date']} to {report['end_date']}*\n")

    # Meetings
    lines.append("## Meetings This Week")
    if report['meetings']:
        for m in report['meetings']:
            outcome = f" → {m['outcome']}" if m.get('outcome') else ""
            lines.append(f"- **{m.get('company', '?')}** ({m['outreach_date']}) "
                        f"— {m['outreach_type']}{outcome}")
    else:
        lines.append("*No meetings logged.*\n")

    # Deals Moved
    lines.append("\n## Deals Moved Forward")
    if report['deals_moved']:
        for d in report['deals_moved']:
            sf = f"{d['square_feet']:,} SF" if d.get('square_feet') else ""
            val = f"${d['estimated_value']:,.0f}" if d.get('estimated_value') else ""
            lines.append(f"- **{d['company_name']}** → {d['stage']} "
                        f"({d.get('building_address', '?')}) {sf} {val}")
    else:
        lines.append("*No stage changes.*\n")

    # New Opportunities
    lines.append("\n## New Opportunities")
    if report['new_opportunities']:
        for o in report['new_opportunities']:
            lines.append(f"- **{o['name']}** — {o.get('sector', '?')} "
                        f"(Score: {o.get('opportunity_score', '?')})")
    else:
        lines.append("*None.*\n")

    # Follow-ups
    lines.append("\n## Follow-ups Completed")
    if report['followups_completed']:
        for f in report['followups_completed']:
            lines.append(f"- {f.get('company', '?')} — {f['outreach_type']} ({f['outreach_date']})")
    else:
        lines.append("*None completed.*\n")

    # Signals
    lines.append("\n## Signals Detected")

    if report['funding_signals']:
        lines.append("### Funding")
        for f in report['funding_signals']:
            amt = f"${f['amount']:,.0f}" if f.get('amount') else "undisclosed"
            lines.append(f"- **{f['name']}** — {f.get('round_type', '?')} ({amt})")

    if report['hiring_signals']:
        lines.append("### Hiring")
        for h in report['hiring_signals']:
            lines.append(f"- **{h['name']}** — {h.get('signal_type', '?')}: "
                        f"{h.get('detail', '')[:80]}")

    if report['exec_changes']:
        lines.append("### Executive Changes")
        for e in report['exec_changes']:
            lines.append(f"- **{e['person_name']}** — {e.get('change_type', '?')}: "
                        f"{e.get('new_title', '?')} at {e.get('new_company', '?')}")

    if not (report['funding_signals'] or report['hiring_signals'] or report['exec_changes']):
        lines.append("*No signals this week.*\n")

    # Next Week
    lines.append("\n## Next Week Priorities")
    if report['overdue']:
        lines.append("### Overdue")
        for o in report['overdue']:
            lines.append(f"- 🔴 **{o.get('company', '?')}** — {o['outreach_type']} "
                        f"(due {o['follow_up_date']})")

    if report['next_week_followups']:
        lines.append("### Scheduled")
        for f in report['next_week_followups']:
            lines.append(f"- {f.get('company', '?')} — {f['outreach_type']} "
                        f"(due {f['follow_up_date']})")

    if not (report['overdue'] or report['next_week_followups']):
        lines.append("*No follow-ups scheduled.*\n")

    lines.append(f"\n---\n*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    return "\n".join(lines)


def export_report_markdown(report_data, filepath):
    """Export report as markdown file."""
    md = format_report_markdown(report_data)
    with open(filepath, 'w') as f:
        f.write(md)
    return filepath


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Weekly Report Generator')
    parser.add_argument('--week-ending', default=None, help='Week ending date (YYYY-MM-DD)')
    parser.add_argument('--output', default=None, help='Output filepath')
    args = parser.parse_args()

    report = generate_weekly_report(args.week_ending)
    md = format_report_markdown(report)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(md)
        print(f"Report saved to {args.output}")
    else:
        print(md)
