"""
Calendar Sync for Relationship Engine
Parses .ics (iCalendar) files to log calendar events and meetings.
Extracts attendee information and matches to contacts in database.
"""

import os
import sys
import sqlite3
import logging
import argparse
from datetime import datetime
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


def _ensure_calendar_table(conn: sqlite3.Connection):
    """Create calendar_events table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS calendar_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT NOT NULL,
            start_time DATETIME,
            end_time DATETIME,
            attendees TEXT,
            location TEXT,
            company_id INTEGER,
            contact_id INTEGER,
            logged_to_outreach INTEGER DEFAULT 0,
            ics_uid TEXT UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_id) REFERENCES companies(id),
            FOREIGN KEY (contact_id) REFERENCES contacts(id)
        )
    """)
    conn.commit()


def parse_ics(file_path: str) -> Dict:
    """
    Parse a .ics (iCalendar) file and extract event details.

    Returns dict with keys:
    - subject: Event title
    - start_time: Start datetime
    - end_time: End datetime
    - attendees: List of attendee emails/names
    - location: Event location
    - ics_uid: Unique identifier from ICS file
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return {}

    event_data = {
        "subject": "",
        "start_time": None,
        "end_time": None,
        "attendees": [],
        "location": "",
        "ics_uid": ""
    }

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Basic ICS parsing (no external lib)
        lines = content.split('\n')

        for i, line in enumerate(lines):
            line = line.strip()

            # Extract UID
            if line.startswith('UID:'):
                event_data['ics_uid'] = line[4:].strip()

            # Extract subject
            elif line.startswith('SUMMARY:'):
                event_data['subject'] = line[8:].strip()

            # Extract location
            elif line.startswith('LOCATION:'):
                event_data['location'] = line[9:].strip()

            # Extract start time (handle both DTSTART and DTSTART;VALUE=DATE)
            elif line.startswith('DTSTART'):
                dt_str = line.split(':', 1)[1].strip() if ':' in line else ""
                if dt_str:
                    try:
                        # Try parsing as datetime
                        if 'T' in dt_str:
                            event_data['start_time'] = datetime.strptime(dt_str[:15], "%Y%m%dT%H%M%S").isoformat()
                        else:
                            event_data['start_time'] = datetime.strptime(dt_str[:8], "%Y%m%d").isoformat()
                    except ValueError:
                        pass

            # Extract end time
            elif line.startswith('DTEND'):
                dt_str = line.split(':', 1)[1].strip() if ':' in line else ""
                if dt_str:
                    try:
                        # Try parsing as datetime
                        if 'T' in dt_str:
                            event_data['end_time'] = datetime.strptime(dt_str[:15], "%Y%m%dT%H%M%S").isoformat()
                        else:
                            event_data['end_time'] = datetime.strptime(dt_str[:8], "%Y%m%d").isoformat()
                    except ValueError:
                        pass

            # Extract attendees (ATTENDEE lines can have mailto: prefix)
            elif line.startswith('ATTENDEE'):
                # Extract email from ATTENDEE;CN=Name:mailto:email@domain.com
                # or just ATTENDEE:mailto:email@domain.com
                parts = line.split(':')
                if len(parts) >= 2:
                    attendee_email = parts[-1].strip()
                    if attendee_email:
                        event_data['attendees'].append(attendee_email)

        # Ensure attendees is a comma-separated string if not empty
        if event_data['attendees']:
            event_data['attendees'] = ', '.join(event_data['attendees'])
        else:
            event_data['attendees'] = ""

        logger.info(f"Parsed calendar event: {event_data['subject']}")
        return event_data

    except Exception as e:
        logger.error(f"Error parsing ICS file {file_path}: {str(e)}")
        return {}


def match_attendees(attendees: str, db_path: str = None) -> List[int]:
    """
    Match attendee emails/names to contact IDs in database.

    Args:
        attendees: Comma-separated string of emails or names
        db_path: Database path

    Returns:
        List of contact IDs that match
    """
    if not attendees or not attendees.strip():
        return []

    conn = _get_conn(db_path)
    cur = conn.cursor()
    contact_ids = []

    attendee_list = [a.strip() for a in attendees.split(',')]

    for attendee in attendee_list:
        # Try to match by email
        cur.execute("SELECT id FROM contacts WHERE LOWER(email) = LOWER(?)", (attendee,))
        row = cur.fetchone()

        if row:
            contact_ids.append(row[0])
            logger.info(f"Matched attendee by email: {attendee} -> contact {row[0]}")
            continue

        # Try to match by name (basic split)
        parts = attendee.split()
        if len(parts) >= 2:
            first_name = parts[0]
            last_name = ' '.join(parts[1:])

            cur.execute("""
                SELECT id FROM contacts
                WHERE LOWER(first_name) = LOWER(?) AND LOWER(last_name) = LOWER(?)
            """, (first_name, last_name))
            row = cur.fetchone()

            if row:
                contact_ids.append(row[0])
                logger.info(f"Matched attendee by name: {attendee} -> contact {row[0]}")

    conn.close()
    return contact_ids


def log_meeting(event_data: Dict, db_path: str = None) -> Dict:
    """
    Log a meeting to calendar_events table.
    Optionally log to outreach_log with channel='in_person'.

    Args:
        event_data: Dict with keys: subject, start_time, end_time, attendees, location, company_id, contact_id
        db_path: Database path

    Returns:
        Dict with event_id and status
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    _ensure_calendar_table(conn)
    cur = conn.cursor()

    result = {
        "event_id": None,
        "status": "failed",
        "message": ""
    }

    try:
        # Insert into calendar_events
        cur.execute("""
            INSERT INTO calendar_events
            (subject, start_time, end_time, attendees, location, company_id, contact_id, ics_uid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_data.get('subject', ''),
            event_data.get('start_time'),
            event_data.get('end_time'),
            event_data.get('attendees', ''),
            event_data.get('location', ''),
            event_data.get('company_id'),
            event_data.get('contact_id'),
            event_data.get('ics_uid', '')
        ))

        event_id = cur.lastrowid
        result["event_id"] = event_id

        # Optionally log to outreach_log if company_id is set
        if event_data.get('company_id'):
            cur.execute("""
                INSERT INTO outreach_log
                (target_company_id, target_contact_id, outreach_date, outreach_type, outcome, notes)
                VALUES (?, ?, date('now'), 'in_person', 'meeting', ?)
            """, (
                event_data.get('company_id'),
                event_data.get('contact_id'),
                f"Calendar meeting: {event_data.get('subject', 'Meeting')}"
            ))

            # Update calendar_events to mark as logged
            cur.execute("UPDATE calendar_events SET logged_to_outreach = 1 WHERE id = ?", (event_id,))

        conn.commit()
        result["status"] = "success"
        result["message"] = f"Logged meeting: {event_data.get('subject', 'Meeting')}"
        logger.info(result["message"])

    except sqlite3.IntegrityError as e:
        # Likely duplicate UID
        result["message"] = f"Event already logged or duplicate: {str(e)}"
        logger.warning(result["message"])
    except Exception as e:
        result["message"] = f"Error logging meeting: {str(e)}"
        logger.error(result["message"])
    finally:
        conn.close()

    return result


def sync_calendar_dir(directory: str, db_path: str = None) -> Dict:
    """
    Process all .ics files in a directory.

    Args:
        directory: Path to directory containing .ics files
        db_path: Database path

    Returns:
        Dict with summary of processed files
    """
    if not os.path.isdir(directory):
        logger.error(f"Directory not found: {directory}")
        return {"status": "failed", "message": "Directory not found"}

    summary = {
        "status": "success",
        "files_processed": 0,
        "events_logged": 0,
        "errors": []
    }

    # Find all .ics files
    ics_files = [f for f in os.listdir(directory) if f.endswith('.ics')]
    logger.info(f"Found {len(ics_files)} .ics files in {directory}")

    for filename in ics_files:
        file_path = os.path.join(directory, filename)

        try:
            # Parse ICS file
            event_data = parse_ics(file_path)

            if not event_data.get('subject'):
                summary["errors"].append(f"{filename}: No subject found")
                continue

            # Try to match attendees
            attendee_contact_ids = []
            if event_data.get('attendees'):
                attendee_contact_ids = match_attendees(event_data['attendees'], db_path)

            # Set first matched contact if any
            if attendee_contact_ids:
                event_data['contact_id'] = attendee_contact_ids[0]

            # Log to database
            result = log_meeting(event_data, db_path)

            if result["status"] == "success":
                summary["events_logged"] += 1
            else:
                summary["errors"].append(f"{filename}: {result['message']}")

            summary["files_processed"] += 1

        except Exception as e:
            summary["errors"].append(f"{filename}: {str(e)}")

    logger.info(f"Calendar sync complete: {summary['files_processed']} files, {summary['events_logged']} events logged")
    return summary


def main():
    """CLI interface."""
    parser = argparse.ArgumentParser(description="Calendar sync for relationship engine")
    parser.add_argument("--file", type=str, help="Parse single .ics file")
    parser.add_argument("--dir", type=str, help="Sync all .ics files in directory")
    parser.add_argument("--db", type=str, help="Database path (optional)")

    args = parser.parse_args()

    if args.file:
        # Parse single file
        event_data = parse_ics(args.file)
        if event_data.get('subject'):
            # Try to match attendees
            if event_data.get('attendees'):
                attendee_ids = match_attendees(event_data['attendees'], args.db)
                if attendee_ids:
                    event_data['contact_id'] = attendee_ids[0]

            result = log_meeting(event_data, args.db)
            print(f"Result: {result}")
        else:
            print("Error: Could not parse file")

    elif args.dir:
        # Sync directory
        result = sync_calendar_dir(args.dir, args.db)
        print(f"Sync result: {result}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
