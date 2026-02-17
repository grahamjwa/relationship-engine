"""
Email Parser for Relationship Engine
Parses forwarded emails to extract contacts, meetings, and action items.
"""

import os
import sys
import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

try:
    from dotenv import load_dotenv
    load_dotenv()  # config.py already loaded .env
except ImportError:
    pass


def parse_email(email_text: str) -> Dict:
    """
    Parse an email to extract:
    - Sender info
    - Company mentioned
    - Action items
    - Meeting details
    - Contacts mentioned
    """
    if not HAS_ANTHROPIC:
        return {"error": "Anthropic not available"}
    
    client = anthropic.Anthropic()
    
    prompt = f"""Parse this email and extract structured information.

EMAIL:
{email_text}

Return JSON with:
{{
    "sender": {{
        "name": "Full Name",
        "email": "email@domain.com",
        "company": "Company Name",
        "title": "Title if mentioned"
    }},
    "companies_mentioned": ["Company 1", "Company 2"],
    "contacts_mentioned": [
        {{"name": "Full Name", "company": "Company", "title": "Title"}}
    ],
    "meeting": {{
        "scheduled": true/false,
        "date": "YYYY-MM-DD or null",
        "time": "HH:MM or null",
        "location": "Location or null",
        "subject": "Meeting topic"
    }},
    "action_items": [
        "Action item 1",
        "Action item 2"
    ],
    "follow_up_needed": true/false,
    "follow_up_date": "YYYY-MM-DD or null",
    "sentiment": "positive/neutral/negative",
    "summary": "One sentence summary"
}}

Return ONLY valid JSON."""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        import json
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        
        return json.loads(text)
    except Exception as e:
        return {"error": str(e)}


def log_email_to_db(parsed: Dict, db_path: str = None) -> Dict:
    """Log parsed email data to database."""
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    result = {
        "company_id": None,
        "contact_id": None,
        "outreach_id": None,
        "actions": []
    }
    
    # Find or create company
    sender = parsed.get("sender", {})
    company_name = sender.get("company")
    
    if company_name:
        cur.execute("SELECT id FROM companies WHERE name LIKE ?", (f"%{company_name}%",))
        row = cur.fetchone()
        if row:
            result["company_id"] = row[0]
            result["actions"].append(f"Matched company: {company_name}")
        else:
            result["actions"].append(f"Company not found: {company_name} (add manually)")
    
    # Find or create contact
    sender_name = sender.get("name")
    sender_email = sender.get("email")
    
    if sender_name and result["company_id"]:
        parts = sender_name.strip().split()
        first_name = parts[0] if parts else ""
        last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
        
        cur.execute("""
            SELECT id FROM contacts 
            WHERE (first_name LIKE ? AND last_name LIKE ?) 
            OR email = ?
        """, (f"%{first_name}%", f"%{last_name}%", sender_email))
        
        row = cur.fetchone()
        if row:
            result["contact_id"] = row[0]
            result["actions"].append(f"Matched contact: {sender_name}")
        else:
            # Create contact
            cur.execute("""
                INSERT INTO contacts (first_name, last_name, company_id, email, title, role_level)
                VALUES (?, ?, ?, ?, ?, 'decision_maker')
            """, (first_name, last_name, result["company_id"], sender_email, sender.get("title", "")))
            result["contact_id"] = cur.lastrowid
            result["actions"].append(f"Created contact: {sender_name}")
    
    # Log outreach
    if result["company_id"]:
        outcome = "responded_positive" if parsed.get("sentiment") == "positive" else "pending"
        follow_up = parsed.get("follow_up_date")
        
        cur.execute("""
            INSERT INTO outreach_log 
            (target_company_id, target_contact_id, outreach_date, outreach_type, outcome, notes, follow_up_date)
            VALUES (?, ?, date('now'), 'email', ?, ?, ?)
        """, (
            result["company_id"], 
            result["contact_id"],
            outcome,
            parsed.get("summary", ""),
            follow_up
        ))
        result["outreach_id"] = cur.lastrowid
        result["actions"].append(f"Logged email outreach")
        
        if follow_up:
            result["actions"].append(f"Follow-up set for {follow_up}")
    
    # Log meeting if scheduled
    meeting = parsed.get("meeting", {})
    if meeting.get("scheduled") and meeting.get("date"):
        result["actions"].append(f"Meeting scheduled: {meeting.get('date')} - {meeting.get('subject', 'TBD')}")
    
    conn.commit()
    conn.close()
    
    return result


def process_forwarded_email(email_text: str, db_path: str = None) -> str:
    """Process a forwarded email and return summary."""
    parsed = parse_email(email_text)
    
    if "error" in parsed:
        return f"Error parsing email: {parsed['error']}"
    
    result = log_email_to_db(parsed, db_path)
    
    summary_lines = [
        f"**Email Processed**",
        f"From: {parsed.get('sender', {}).get('name', 'Unknown')} ({parsed.get('sender', {}).get('company', 'Unknown')})",
        f"Summary: {parsed.get('summary', 'No summary')}",
        "",
        "**Actions:**"
    ]
    
    for action in result.get("actions", []):
        summary_lines.append(f"  • {action}")
    
    if parsed.get("action_items"):
        summary_lines.append("")
        summary_lines.append("**Action Items from Email:**")
        for item in parsed["action_items"]:
            summary_lines.append(f"  • {item}")
    
    return "\n".join(summary_lines)


if __name__ == "__main__":
    # Test with sample email
    test_email = """
    From: John Smith <john.smith@citadel.com>
    To: Graham Walter
    Subject: RE: Office Space Discussion
    
    Hi Graham,
    
    Thanks for reaching out. We're definitely interested in exploring options 
    for our expansion. Can we set up a call next Tuesday at 2pm to discuss 
    the 50,000 SF requirement?
    
    Also, please loop in Sarah Chen from our facilities team.
    
    Best,
    John Smith
    Head of Real Estate
    Citadel
    """
    
    result = process_forwarded_email(test_email)
    print(result)
