"""
Outreach Manager
================
Log outreach, track follow-ups, surface reminders.

Works with the existing outreach_log table.
Default follow-up window: 7 days from outreach date.
"""

import sqlite3
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path


def log_outreach(company_id: int,
                 contact_id: Optional[int] = None,
                 outreach_type: str = "email",
                 outcome: str = "sent",
                 notes: str = "",
                 angle: str = "",
                 intro_path_used: str = "",
                 follow_up_days: int = 7,
                 db_path: Optional[str] = None) -> int:
    """
    Log an outreach touch and auto-schedule follow-up.

    Args:
        company_id: Target company ID
        contact_id: Optional target contact ID
        outreach_type: email | call | linkedin | meeting | text
        outcome: sent | connected | voicemail | no_answer | bounced | meeting_set | replied
        notes: Freeform notes
        angle: What angle/topic was used
        intro_path_used: If warm intro, who was the connector
        follow_up_days: Days until follow-up reminder (0 = no follow-up)

    Returns:
        ID of the new outreach_log row
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    follow_up_date = None
    if follow_up_days > 0:
        follow_up_date = (datetime.now() + timedelta(days=follow_up_days)).strftime("%Y-%m-%d")

    cur.execute("""
        INSERT INTO outreach_log
            (target_company_id, target_contact_id, outreach_type, outcome,
             notes, angle, intro_path_used, outreach_date, follow_up_date,
             follow_up_done)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    """, (company_id, contact_id, outreach_type, outcome,
          notes, angle, intro_path_used, now, follow_up_date))

    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return row_id


def get_due_followups(db_path: Optional[str] = None,
                      include_overdue: bool = True) -> List[Dict]:
    """
    Return outreach entries where follow-up is due today or overdue.

    Returns list of dicts sorted by follow_up_date ascending (most overdue first).
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")

    if include_overdue:
        cur.execute("""
            SELECT o.id, o.target_company_id, o.target_contact_id,
                   o.outreach_type, o.outcome, o.notes, o.angle,
                   o.outreach_date, o.follow_up_date,
                   c.name as company_name,
                   ct.first_name || ' ' || ct.last_name as contact_name
            FROM outreach_log o
            LEFT JOIN companies c ON o.target_company_id = c.id
            LEFT JOIN contacts ct ON o.target_contact_id = ct.id
            WHERE o.follow_up_date IS NOT NULL
              AND o.follow_up_date <= ?
              AND (o.follow_up_done IS NULL OR o.follow_up_done = 0)
            ORDER BY o.follow_up_date ASC
        """, (today,))
    else:
        cur.execute("""
            SELECT o.id, o.target_company_id, o.target_contact_id,
                   o.outreach_type, o.outcome, o.notes, o.angle,
                   o.outreach_date, o.follow_up_date,
                   c.name as company_name,
                   ct.first_name || ' ' || ct.last_name as contact_name
            FROM outreach_log o
            LEFT JOIN companies c ON o.target_company_id = c.id
            LEFT JOIN contacts ct ON o.target_contact_id = ct.id
            WHERE o.follow_up_date = ?
              AND (o.follow_up_done IS NULL OR o.follow_up_done = 0)
            ORDER BY o.follow_up_date ASC
        """, (today,))

    results = []
    for row in cur.fetchall():
        row_d = dict(row)
        days_overdue = 0
        if row_d['follow_up_date']:
            fu_dt = datetime.strptime(row_d['follow_up_date'], "%Y-%m-%d")
            days_overdue = (datetime.now() - fu_dt).days
        row_d['days_overdue'] = max(days_overdue, 0)
        results.append(row_d)

    conn.close()
    return results


def mark_followup_done(outreach_id: int,
                       reschedule_days: int = 0,
                       db_path: Optional[str] = None) -> None:
    """
    Mark a follow-up as done. Optionally reschedule a new follow-up.

    Args:
        outreach_id: ID of the outreach_log row
        reschedule_days: If > 0, set a new follow_up_date N days from now
        db_path: Optional DB path override
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if reschedule_days > 0:
        new_date = (datetime.now() + timedelta(days=reschedule_days)).strftime("%Y-%m-%d")
        cur.execute("""
            UPDATE outreach_log
            SET follow_up_done = 0, follow_up_date = ?
            WHERE id = ?
        """, (new_date, outreach_id))
    else:
        cur.execute("""
            UPDATE outreach_log
            SET follow_up_done = 1
            WHERE id = ?
        """, (outreach_id,))

    conn.commit()
    conn.close()


def get_outreach_history(company_id: int,
                         limit: int = 20,
                         db_path: Optional[str] = None) -> List[Dict]:
    """Get recent outreach history for a company."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT o.id, o.outreach_type, o.outcome, o.notes, o.angle,
               o.outreach_date, o.follow_up_date, o.follow_up_done,
               ct.first_name || ' ' || ct.last_name as contact_name
        FROM outreach_log o
        LEFT JOIN contacts ct ON o.target_contact_id = ct.id
        WHERE o.target_company_id = ?
        ORDER BY o.outreach_date DESC
        LIMIT ?
    """, (company_id, limit))

    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results


def get_followup_summary(db_path: Optional[str] = None) -> Dict:
    """Quick summary of follow-up status."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE follow_up_date <= ? AND (follow_up_done IS NULL OR follow_up_done = 0)) as due,
            COUNT(*) FILTER (WHERE follow_up_date > ? AND (follow_up_done IS NULL OR follow_up_done = 0)) as upcoming,
            COUNT(*) FILTER (WHERE follow_up_done = 1) as completed
        FROM outreach_log
        WHERE follow_up_date IS NOT NULL
    """, (today, today))

    row = cur.fetchone()
    conn.close()

    if row:
        return {'due': row[0], 'upcoming': row[1], 'completed': row[2]}
    return {'due': 0, 'upcoming': 0, 'completed': 0}


if __name__ == "__main__":
    print("Follow-up Summary:")
    summary = get_followup_summary()
    print(f"  Due/Overdue: {summary['due']}")
    print(f"  Upcoming: {summary['upcoming']}")
    print(f"  Completed: {summary['completed']}")

    print("\nDue Follow-ups:")
    due = get_due_followups()
    if due:
        for d in due:
            print(f"  {d['company_name']} — {d['outreach_type']} on {d['outreach_date']} "
                  f"(overdue {d['days_overdue']}d)")
    else:
        print("  None due.")
