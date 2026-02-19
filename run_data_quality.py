"""
Data Quality Report — outputs markdown report of DB health.

Run: python3 run_data_quality.py
     python3 run_data_quality.py --save  (writes to reports/data_quality.md)
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: F401

from core.graph_engine import get_db_path


def run_quality_check(db_path=None):
    """Run all data quality checks. Returns markdown string."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    lines = [
        f"# Data Quality Report",
        f"*Generated {datetime.now().strftime('%B %d, %Y %H:%M')}*",
        "",
    ]

    # ── Table counts ─────────────────────────────────────────────────────
    lines.append("## Table Row Counts")
    lines.append("")
    tables = ['companies', 'contacts', 'relationships', 'funding_events',
              'hiring_signals', 'outreach_log', 'buildings', 'leases',
              'deals', 'market_notes']
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            lines.append(f"- **{t}**: {cur.fetchone()[0]}")
        except Exception:
            lines.append(f"- **{t}**: table not found")
    lines.append("")

    # ── Missing fields ───────────────────────────────────────────────────
    lines.append("## Missing Fields")
    lines.append("")

    checks = [
        ("Companies missing sector",
         "SELECT COUNT(*) FROM companies WHERE sector IS NULL OR sector = '' OR sector = 'unknown'"),
        ("Companies missing HQ city",
         "SELECT COUNT(*) FROM companies WHERE hq_city IS NULL OR hq_city = ''"),
        ("Companies missing status",
         "SELECT COUNT(*) FROM companies WHERE status IS NULL OR status = ''"),
        ("Companies missing type",
         "SELECT COUNT(*) FROM companies WHERE type IS NULL OR type = ''"),
        ("Contacts missing email",
         "SELECT COUNT(*) FROM contacts WHERE email IS NULL OR email = ''"),
        ("Contacts missing title",
         "SELECT COUNT(*) FROM contacts WHERE title IS NULL OR title = ''"),
        ("Contacts missing previous_companies",
         "SELECT COUNT(*) FROM contacts WHERE previous_companies IS NULL OR previous_companies = ''"),
        ("Contacts missing phone",
         "SELECT COUNT(*) FROM contacts WHERE phone IS NULL OR phone = ''"),
    ]

    issues = 0
    for label, query in checks:
        try:
            cur.execute(query)
            count = cur.fetchone()[0]
            flag = " ⚠️" if count > 0 else ""
            lines.append(f"- {label}: **{count}**{flag}")
            if count > 0:
                issues += 1
        except Exception as e:
            lines.append(f"- {label}: error ({e})")
    lines.append("")

    # ── Duplicate company names ──────────────────────────────────────────
    lines.append("## Duplicate Company Names")
    lines.append("")

    cur.execute("""
        SELECT LOWER(name) as lname, COUNT(*) as cnt, GROUP_CONCAT(id) as ids
        FROM companies
        GROUP BY LOWER(name)
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)
    dupes = cur.fetchall()
    if dupes:
        for d in dupes:
            dd = dict(d)
            lines.append(f"- \"{dd['lname']}\" — {dd['cnt']} entries (IDs: {dd['ids']})")
            issues += 1
    else:
        lines.append("No duplicates found.")
    lines.append("")

    # ── Orphaned records ─────────────────────────────────────────────────
    lines.append("## Orphaned Records")
    lines.append("")

    orphan_checks = [
        ("Contacts with invalid company_id",
         """SELECT COUNT(*) FROM contacts
            WHERE company_id IS NOT NULL
            AND company_id NOT IN (SELECT id FROM companies)"""),
        ("Funding events with invalid company_id",
         """SELECT COUNT(*) FROM funding_events
            WHERE company_id NOT IN (SELECT id FROM companies)"""),
        ("Hiring signals with invalid company_id",
         """SELECT COUNT(*) FROM hiring_signals
            WHERE company_id NOT IN (SELECT id FROM companies)"""),
        ("Outreach with invalid target_company_id",
         """SELECT COUNT(*) FROM outreach_log
            WHERE target_company_id IS NOT NULL
            AND target_company_id NOT IN (SELECT id FROM companies)"""),
        ("Leases with invalid company_id",
         """SELECT COUNT(*) FROM leases
            WHERE company_id NOT IN (SELECT id FROM companies)"""),
        ("Leases with invalid building_id",
         """SELECT COUNT(*) FROM leases
            WHERE building_id NOT IN (SELECT id FROM buildings)"""),
    ]

    for label, query in orphan_checks:
        try:
            cur.execute(query)
            count = cur.fetchone()[0]
            flag = " ⚠️" if count > 0 else ""
            lines.append(f"- {label}: **{count}**{flag}")
            if count > 0:
                issues += 1
        except Exception as e:
            lines.append(f"- {label}: error ({e})")
    lines.append("")

    # ── Stale data ───────────────────────────────────────────────────────
    lines.append("## Stale Data")
    lines.append("")

    try:
        cur.execute("SELECT MAX(event_date) FROM funding_events")
        latest_funding = cur.fetchone()[0] or "none"
        lines.append(f"- Latest funding event: **{latest_funding}**")
    except Exception:
        pass

    try:
        cur.execute("SELECT MAX(signal_date) FROM hiring_signals")
        latest_hiring = cur.fetchone()[0] or "none"
        lines.append(f"- Latest hiring signal: **{latest_hiring}**")
    except Exception:
        pass

    try:
        cur.execute("SELECT MAX(outreach_date) FROM outreach_log")
        latest_outreach = cur.fetchone()[0] or "none"
        lines.append(f"- Latest outreach: **{latest_outreach}**")
    except Exception:
        pass

    lines.append("")

    # ── Summary ──────────────────────────────────────────────────────────
    lines.append("## Summary")
    lines.append("")
    if issues == 0:
        lines.append("No data quality issues found.")
    else:
        lines.append(f"**{issues} issue(s) found.** Review above for details.")

    conn.close()
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Data Quality Report")
    parser.add_argument("--save", action="store_true", help="Save to reports/data_quality.md")
    args = parser.parse_args()

    report = run_quality_check()
    print(report)

    if args.save:
        out_path = os.path.join(os.path.dirname(__file__), 'reports', 'data_quality.md')
        with open(out_path, 'w') as f:
            f.write(report)
        print(f"\nSaved to {out_path}")
