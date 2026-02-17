import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from graph_engine import get_db_path

import logging
import argparse
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def signals_no_outreach(db_path: str = None) -> List[Dict]:
    """
    Find companies with funding_events or hiring_signals in last 90 days
    but zero outreach_log entries.

    Returns:
        List of dicts with: company_name, signal_count, latest_signal_date, opportunity_score
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cutoff_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

    cur.execute("""
        SELECT
            c.id,
            c.name,
            COUNT(DISTINCT s.id) as signal_count,
            MAX(s.signal_date) as latest_signal_date,
            COALESCE(c.opportunity_score, 0) as opportunity_score
        FROM companies c
        JOIN signals s ON c.id = s.company_id
        WHERE s.signal_date >= ?
        AND s.signal_type IN ('funding_event', 'hiring_signal')
        AND c.id NOT IN (
            SELECT DISTINCT company_id FROM outreach_log
        )
        GROUP BY c.id, c.name
        ORDER BY opportunity_score DESC, signal_count DESC
    """, (cutoff_date,))

    results = []
    for row in cur.fetchall():
        results.append({
            'company_name': row['name'],
            'company_id': row['id'],
            'signal_count': row['signal_count'],
            'latest_signal_date': row['latest_signal_date'],
            'opportunity_score': row['opportunity_score']
        })

    conn.close()
    return results


def high_score_never_contacted(threshold: int = 30, db_path: str = None) -> List[Dict]:
    """
    Find companies with opportunity_score > threshold and zero outreach_log entries ever.

    Returns:
        Sorted by score descending
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            name,
            opportunity_score
        FROM companies
        WHERE opportunity_score > ?
        AND id NOT IN (
            SELECT DISTINCT company_id FROM outreach_log
        )
        ORDER BY opportunity_score DESC
    """, (threshold,))

    results = []
    for row in cur.fetchall():
        results.append({
            'company_id': row['id'],
            'company_name': row['name'],
            'opportunity_score': row['opportunity_score']
        })

    conn.close()
    return results


def clients_gone_cold(days: int = 60, db_path: str = None) -> List[Dict]:
    """
    Find active_client companies where last outreach_log entry is older than N days,
    or no outreach at all.

    Returns:
        List of dicts with: company_name, last_contact_date, days_since, contact_count
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    cur.execute("""
        SELECT
            c.id,
            c.name,
            MAX(ol.outreach_date) as last_contact_date,
            COUNT(ol.id) as contact_count,
            CAST((julianday('now') - julianday(MAX(ol.outreach_date))) AS INTEGER) as days_since
        FROM companies c
        LEFT JOIN outreach_log ol ON c.id = ol.company_id
        WHERE c.status = 'active_client'
        GROUP BY c.id, c.name
        HAVING MAX(ol.outreach_date) IS NULL OR MAX(ol.outreach_date) < ?
        ORDER BY days_since DESC NULLS FIRST
    """, (cutoff_date,))

    results = []
    for row in cur.fetchall():
        results.append({
            'company_id': row['id'],
            'company_name': row['name'],
            'last_contact_date': row['last_contact_date'],
            'days_since': row['days_since'],
            'contact_count': row['contact_count']
        })

    conn.close()
    return results


def contacts_no_email(db_path: str = None) -> List[Dict]:
    """
    Find contacts at target companies (status in high_growth_target, prospect)
    with no email address.

    Returns:
        List of dicts with: contact_name, company_name, company_id, title
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
            ct.id,
            ct.first_name || ' ' || ct.last_name as contact_name,
            c.id as company_id,
            c.name as company_name,
            ct.title
        FROM contacts ct
        JOIN companies c ON ct.company_id = c.id
        WHERE c.status IN ('high_growth_target', 'prospect')
        AND (ct.email IS NULL OR ct.email = '')
        ORDER BY c.name, ct.first_name
    """)

    results = []
    for row in cur.fetchall():
        results.append({
            'contact_id': row['id'],
            'contact_name': row['contact_name'],
            'company_id': row['company_id'],
            'company_name': row['company_name'],
            'title': row['title']
        })

    conn.close()
    return results


def uncovered_companies(db_path: str = None) -> List[Dict]:
    """
    Find companies with opportunity_score > 20 but zero contacts in the contacts table.

    Returns:
        List of dicts with: company_name, opportunity_score, contact_count
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT
            c.id,
            c.name,
            c.opportunity_score,
            COUNT(ct.id) as contact_count
        FROM companies c
        LEFT JOIN contacts ct ON c.id = ct.company_id
        WHERE c.opportunity_score > 20
        GROUP BY c.id, c.name
        HAVING COUNT(ct.id) = 0
        ORDER BY c.opportunity_score DESC
    """)

    results = []
    for row in cur.fetchall():
        results.append({
            'company_id': row['id'],
            'company_name': row['name'],
            'opportunity_score': row['opportunity_score'],
            'contact_count': row['contact_count']
        })

    conn.close()
    return results


def generate_action_list(db_path: str = None) -> str:
    """
    Combine all gap analyses into a prioritized markdown action list.

    Returns:
        Markdown formatted action list with sections: "Immediate Action", "This Week", "Data Gaps"
    """
    if db_path is None:
        db_path = get_db_path()

    lines = []
    lines.append("# Outreach Gaps - Action List")
    lines.append("")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # Immediate Action: signals_no_outreach (top 5)
    immediate = signals_no_outreach(db_path)[:5]
    if immediate:
        lines.append("## Immediate Action")
        lines.append("*Companies with recent signals (90d) but no outreach*")
        lines.append("")
        for item in immediate:
            lines.append(f"- **{item['company_name']}** (Score: {item['opportunity_score']})")
            lines.append(f"  - Signals: {item['signal_count']} (latest: {item['latest_signal_date']})")
        lines.append("")

    # This Week: high_score_never_contacted + clients_gone_cold
    never_contacted = high_score_never_contacted(threshold=30, db_path=db_path)[:10]
    cold_clients = clients_gone_cold(days=60, db_path=db_path)

    if never_contacted or cold_clients:
        lines.append("## This Week")
        lines.append("")

    if never_contacted:
        lines.append("### High-Potential Never Contacted")
        lines.append("*Companies with high opportunity scores but zero outreach history*")
        lines.append("")
        for item in never_contacted:
            lines.append(f"- **{item['company_name']}** (Score: {item['opportunity_score']})")
        lines.append("")

    if cold_clients:
        lines.append("### Clients Gone Cold")
        lines.append("*Active clients with no outreach in 60+ days*")
        lines.append("")
        for item in cold_clients:
            days_str = str(item['days_since']) if item['days_since'] is not None else 'Never'
            lines.append(f"- **{item['company_name']}**")
            lines.append(f"  - Days since contact: {days_str}")
            lines.append(f"  - Total contacts: {item['contact_count']}")
        lines.append("")

    # Data Gaps
    no_email = contacts_no_email(db_path)
    uncovered = uncovered_companies(db_path)

    if no_email or uncovered:
        lines.append("## Data Gaps")
        lines.append("")

    if no_email:
        lines.append("### Contacts Missing Email Addresses")
        lines.append(f"*{len(no_email)} contacts at target companies need email addresses*")
        lines.append("")
        for item in no_email[:20]:
            lines.append(f"- {item['contact_name']} ({item['title']}) @ {item['company_name']}")
        if len(no_email) > 20:
            lines.append(f"- ... and {len(no_email) - 20} more")
        lines.append("")

    if uncovered:
        lines.append("### Uncovered High-Potential Companies")
        lines.append("*Companies with opportunity_score > 20 but zero contacts on file*")
        lines.append("")
        for item in uncovered:
            lines.append(f"- **{item['company_name']}** (Score: {item['opportunity_score']})")
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Identify outreach gaps and generate action list"
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate full markdown action list report"
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=30,
        help="Opportunity score threshold for high_score_never_contacted (default: 30)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=60,
        help="Days threshold for clients_gone_cold (default: 60)"
    )

    args = parser.parse_args()

    if args.report:
        print(generate_action_list())
    else:
        db_path = get_db_path()

        print("\n=== SIGNALS WITH NO OUTREACH (90 days) ===")
        for item in signals_no_outreach(db_path)[:5]:
            print(f"{item['company_name']}: {item['signal_count']} signals, score {item['opportunity_score']}")

        print("\n=== HIGH SCORE NEVER CONTACTED (threshold={}) ===".format(args.threshold))
        for item in high_score_never_contacted(threshold=args.threshold, db_path=db_path)[:5]:
            print(f"{item['company_name']}: score {item['opportunity_score']}")

        print("\n=== CLIENTS GONE COLD ({}+ days) ===".format(args.days))
        for item in clients_gone_cold(days=args.days, db_path=db_path)[:5]:
            days_str = str(item['days_since']) if item['days_since'] is not None else 'Never'
            print(f"{item['company_name']}: {days_str} days, {item['contact_count']} contacts")

        print("\n=== CONTACTS WITHOUT EMAIL ===")
        no_email = contacts_no_email(db_path)
        print(f"Total: {len(no_email)}")
        for item in no_email[:5]:
            print(f"{item['contact_name']} ({item['title']}) @ {item['company_name']}")

        print("\n=== UNCOVERED COMPANIES (score > 20) ===")
        uncovered = uncovered_companies(db_path)
        print(f"Total: {len(uncovered)}")
        for item in uncovered[:5]:
            print(f"{item['company_name']}: score {item['opportunity_score']}")
