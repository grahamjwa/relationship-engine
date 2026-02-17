"""
LinkedIn Activity Tracker for Relationship Engine
Tracks LinkedIn activity and detects job changes for contacts.
"""

import os
import sys
import sqlite3
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _get_conn(db_path: str = None) -> sqlite3.Connection:
    """Get database connection."""
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_linkedin_table(conn: sqlite3.Connection):
    """Create linkedin_activity table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS linkedin_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER NOT NULL,
            activity_type TEXT NOT NULL,
            details TEXT,
            detected_date DATE,
            source TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        )
    """)
    conn.commit()


# Valid activity types
VALID_ACTIVITY_TYPES = {
    "connection_sent",
    "connection_accepted",
    "job_change",
    "profile_update",
    "post_engagement"
}


def log_activity(contact_id: int, activity_type: str, details: str = "", db_path: str = None) -> Dict:
    """
    Log LinkedIn activity for a contact.

    Args:
        contact_id: Contact ID
        activity_type: Type of activity (connection_sent, connection_accepted, job_change, profile_update, post_engagement)
        details: Optional details about the activity
        db_path: Database path

    Returns:
        Dict with activity_id and status
    """
    if db_path is None:
        db_path = get_db_path()

    if activity_type not in VALID_ACTIVITY_TYPES:
        return {
            "status": "failed",
            "message": f"Invalid activity_type: {activity_type}. Must be one of: {', '.join(VALID_ACTIVITY_TYPES)}"
        }

    conn = _get_conn(db_path)
    _ensure_linkedin_table(conn)
    cur = conn.cursor()

    result = {
        "activity_id": None,
        "status": "failed",
        "message": ""
    }

    try:
        # Check if contact exists
        cur.execute("SELECT id FROM contacts WHERE id = ?", (contact_id,))
        if not cur.fetchone():
            result["message"] = f"Contact {contact_id} not found"
            logger.warning(result["message"])
            return result

        # Insert activity
        detected_date = date.today().isoformat()
        cur.execute("""
            INSERT INTO linkedin_activity
            (contact_id, activity_type, details, detected_date, source)
            VALUES (?, ?, ?, ?, 'manual')
        """, (contact_id, activity_type, details, detected_date))

        activity_id = cur.lastrowid
        result["activity_id"] = activity_id
        result["status"] = "success"
        result["message"] = f"Logged {activity_type} for contact {contact_id}"

        conn.commit()
        logger.info(result["message"])

    except Exception as e:
        result["message"] = f"Error logging activity: {str(e)}"
        logger.error(result["message"])
    finally:
        conn.close()

    return result


def detect_job_changes(db_path: str = None) -> Dict:
    """
    Query contacts and search for potential job changes.
    (Placeholder for web search integration)

    Returns:
        Dict with detected job changes
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    result = {
        "status": "success",
        "changes_detected": [],
        "message": "Job change detection requires web search integration"
    }

    try:
        # Get recent contacts
        thirty_days_ago = (date.today() - timedelta(days=30)).isoformat()
        cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.company_id, co.name as company_name
            FROM contacts c
            LEFT JOIN companies co ON c.company_id = co.id
            WHERE c.updated_at >= ?
            ORDER BY c.updated_at DESC
            LIMIT 50
        """, (thirty_days_ago,))

        contacts = cur.fetchall()
        logger.info(f"Checking {len(contacts)} recent contacts for job changes")

        # In a real implementation, would search web for each contact:
        # search_client.search(f"{first_name} {last_name} new role OR joined OR appointed")
        # For now, log that detection would happen
        result["contacts_checked"] = len(contacts)

    except Exception as e:
        result["status"] = "failed"
        result["message"] = f"Error detecting job changes: {str(e)}"
        logger.error(result["message"])
    finally:
        conn.close()

    return result


def get_pending_connections(db_path: str = None) -> List[Dict]:
    """
    Get connections that were sent but not yet accepted.

    Returns:
        List of dicts with contact details and connection sent date
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    pending = []

    try:
        cur.execute("""
            SELECT
                c.id,
                c.first_name,
                c.last_name,
                c.email,
                c.company_id,
                co.name as company_name,
                la.detected_date as connection_sent_date
            FROM linkedin_activity la
            JOIN contacts c ON la.contact_id = c.id
            LEFT JOIN companies co ON c.company_id = co.id
            WHERE la.activity_type = 'connection_sent'
                AND la.contact_id NOT IN (
                    SELECT contact_id FROM linkedin_activity
                    WHERE activity_type = 'connection_accepted'
                )
            ORDER BY la.detected_date DESC
        """)

        for row in cur.fetchall():
            pending.append({
                "contact_id": row[0],
                "first_name": row[1],
                "last_name": row[2],
                "email": row[3],
                "company_id": row[4],
                "company_name": row[5],
                "connection_sent_date": row[6]
            })

        logger.info(f"Found {len(pending)} pending connections")

    except Exception as e:
        logger.error(f"Error getting pending connections: {str(e)}")
    finally:
        conn.close()

    return pending


def get_activity_summary(days: int = 30, db_path: str = None) -> Dict:
    """
    Get summary of LinkedIn activity over N days.

    Args:
        days: Number of days to look back
        db_path: Database path

    Returns:
        Dict with activity summary
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    summary = {
        "period_days": days,
        "total_activities": 0,
        "by_type": {},
        "activities": []
    }

    try:
        cutoff_date = (date.today() - timedelta(days=days)).isoformat()

        # Get activity counts by type
        cur.execute("""
            SELECT activity_type, COUNT(*) as count
            FROM linkedin_activity
            WHERE detected_date >= ?
            GROUP BY activity_type
        """, (cutoff_date,))

        for row in cur.fetchall():
            summary["by_type"][row[0]] = row[1]
            summary["total_activities"] += row[1]

        # Get recent activities
        cur.execute("""
            SELECT
                la.id,
                la.contact_id,
                c.first_name,
                c.last_name,
                la.activity_type,
                la.details,
                la.detected_date
            FROM linkedin_activity la
            JOIN contacts c ON la.contact_id = c.id
            WHERE la.detected_date >= ?
            ORDER BY la.detected_date DESC
            LIMIT 100
        """, (cutoff_date,))

        for row in cur.fetchall():
            summary["activities"].append({
                "activity_id": row[0],
                "contact_id": row[1],
                "contact_name": f"{row[2]} {row[3]}",
                "activity_type": row[4],
                "details": row[5],
                "detected_date": row[6]
            })

        logger.info(f"Activity summary for {days} days: {summary['total_activities']} total activities")

    except Exception as e:
        logger.error(f"Error getting activity summary: {str(e)}")
    finally:
        conn.close()

    return summary


def generate_linkedin_report(db_path: str = None) -> str:
    """
    Generate a markdown report of LinkedIn activity.

    Returns:
        Markdown-formatted report
    """
    if db_path is None:
        db_path = get_db_path()

    # Get 30-day summary
    summary = get_activity_summary(days=30, db_path=db_path)
    pending = get_pending_connections(db_path=db_path)

    report_lines = [
        "# LinkedIn Activity Report",
        "",
        f"**Period:** Last {summary['period_days']} days",
        f"**Total Activities:** {summary['total_activities']}",
        "",
        "## Activity by Type",
        ""
    ]

    # Activity breakdown
    for activity_type in sorted(VALID_ACTIVITY_TYPES):
        count = summary["by_type"].get(activity_type, 0)
        report_lines.append(f"- {activity_type}: {count}")

    report_lines.extend([
        "",
        "## Pending Connections",
        ""
    ])

    if pending:
        report_lines.append(f"Found {len(pending)} pending connections:")
        report_lines.append("")
        for conn in pending[:20]:  # Limit to 20
            report_lines.append(f"- {conn['first_name']} {conn['last_name']} ({conn['company_name'] or 'Unknown'})")
            if conn['connection_sent_date']:
                report_lines.append(f"  - Sent: {conn['connection_sent_date']}")
    else:
        report_lines.append("No pending connections.")

    report_lines.extend([
        "",
        "## Recent Activities",
        ""
    ])

    if summary["activities"]:
        for activity in summary["activities"][:10]:  # Show top 10
            report_lines.append(f"- **{activity['contact_name']}** - {activity['activity_type']}")
            report_lines.append(f"  - Date: {activity['detected_date']}")
            if activity['details']:
                report_lines.append(f"  - Details: {activity['details']}")
    else:
        report_lines.append("No recent activities.")

    return "\n".join(report_lines)


def main():
    """CLI interface."""
    parser = argparse.ArgumentParser(description="LinkedIn tracker for relationship engine")
    parser.add_argument("--detect-changes", action="store_true", help="Detect potential job changes")
    parser.add_argument("--report", action="store_true", help="Generate LinkedIn activity report")
    parser.add_argument("--log", type=int, metavar="CONTACT_ID", help="Log activity for contact ID")
    parser.add_argument("--activity-type", type=str, help="Activity type (for --log)")
    parser.add_argument("--details", type=str, help="Activity details (for --log)")
    parser.add_argument("--db", type=str, help="Database path (optional)")

    args = parser.parse_args()

    if args.detect_changes:
        result = detect_job_changes(args.db)
        print(f"Job change detection: {result}")

    elif args.report:
        report = generate_linkedin_report(args.db)
        print(report)

    elif args.log:
        if not args.activity_type:
            print("Error: --activity-type required for --log")
            parser.print_help()
            return

        result = log_activity(
            contact_id=args.log,
            activity_type=args.activity_type,
            details=args.details or "",
            db_path=args.db
        )
        print(f"Activity logged: {result}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
