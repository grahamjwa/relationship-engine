"""
Executive Movement Tracker
Searches for executives who moved from client companies to target companies.
"""

import os
import sys
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from search_client import search_web
from graph_engine import get_db_path

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass


def search_executive_moves(from_company: str, to_company: str) -> List[Dict]:
    """Search for executives who moved between two companies."""
    query = f'"{from_company}" "{to_company}" executive OR director OR head joined OR hired OR appointed'
    return search_web(query)


def search_company_departures(company: str) -> List[Dict]:
    """Search for recent executive departures from a company."""
    query = f'"{company}" executive OR director OR managing director departed OR left OR resigned OR joined 2024 OR 2025'
    return search_web(query)


def classify_movement(company_from: str, company_to: str, search_results: List[Dict]) -> List[Dict]:
    """Use Claude to extract executive movements from search results."""
    if not HAS_ANTHROPIC or not search_results:
        return []
    
    client = anthropic.Anthropic()
    
    snippets = "\n\n".join([
        f"Title: {r.get('title', '')}\nSnippet: {r.get('snippet', '')}\nURL: {r.get('link', '')}"
        for r in search_results[:8]
    ])
    
    prompt = f"""Analyze these search results for executive movements from {company_from} to {company_to}.

{snippets}

Extract any executives who:
1. Left {company_from} and joined {company_to}
2. Were hired by {company_to} from {company_from}

For each person found, return JSON:
{{
    "movements": [
        {{
            "name": "Full Name",
            "previous_title": "Title at {company_from}",
            "new_title": "Title at {company_to}",
            "date": "YYYY-MM or 'recent' if unclear",
            "source_url": "URL",
            "confidence": 0.0-1.0
        }}
    ]
}}

If no clear movements found, return {{"movements": []}}
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
        
        data = json.loads(text)
        return data.get("movements", [])
    except Exception as e:
        print(f"Classification error: {e}")
        return []


def insert_executive_move(
    db_path: str,
    contact_name: str,
    from_company_id: int,
    to_company_id: int,
    previous_title: str,
    new_title: str,
    move_date: str,
    source_url: str
) -> int:
    """Insert executive move and create/update contact."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Parse name
    parts = contact_name.strip().split()
    first_name = parts[0] if parts else "Unknown"
    last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    
    # Check if contact exists at target company
    cur.execute("""
        SELECT id FROM contacts 
        WHERE first_name = ? AND last_name = ? AND company_id = ?
    """, (first_name, last_name, to_company_id))
    
    existing = cur.fetchone()
    
    if existing:
        # Update previous_companies
        cur.execute("""
            SELECT c.name FROM companies c WHERE c.id = ?
        """, (from_company_id,))
        from_name = cur.fetchone()[0]
        
        cur.execute("""
            UPDATE contacts 
            SET previous_companies = COALESCE(previous_companies || ', ', '') || ?,
                title = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (from_name, new_title, existing[0]))
        contact_id = existing[0]
    else:
        # Get from company name
        cur.execute("SELECT name FROM companies WHERE id = ?", (from_company_id,))
        from_name = cur.fetchone()[0]
        
        # Insert new contact
        cur.execute("""
            INSERT INTO contacts (first_name, last_name, company_id, title, role_level, previous_companies, notes)
            VALUES (?, ?, ?, ?, 'decision_maker', ?, ?)
        """, (first_name, last_name, to_company_id, new_title, from_name, f"Moved from {from_name} ({move_date})"))
        contact_id = cur.lastrowid
    
    conn.commit()
    conn.close()
    return contact_id


def send_move_alert(name: str, from_company: str, to_company: str, title: str):
    """Send Discord alert for executive movement."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not webhook_url or not HAS_REQUESTS:
        return
    
    message = f"🔄 **Executive Move Alert**\n{name} ({title})\n{from_company} → {to_company}\n*Warm intro opportunity*"
    
    try:
        requests.post(webhook_url, json={"content": message}, timeout=5)
    except:
        pass


def scan_movements_from_client(client_id: int, client_name: str, db_path: str = None) -> List[Dict]:
    """Scan for movements from a client to any target company."""
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Get target companies
    cur.execute("""
        SELECT id, name FROM companies 
        WHERE status IN ('high_growth_target', 'prospect')
        LIMIT 20
    """)
    targets = [dict(row) for row in cur.fetchall()]
    conn.close()
    
    all_movements = []
    
    for target in targets:
        print(f"  Checking {client_name} → {target['name']}...")
        
        results = search_executive_moves(client_name, target['name'])
        if results:
            movements = classify_movement(client_name, target['name'], results)
            
            for move in movements:
                if move.get('confidence', 0) >= 0.7:
                    contact_id = insert_executive_move(
                        db_path=db_path,
                        contact_name=move['name'],
                        from_company_id=client_id,
                        to_company_id=target['id'],
                        previous_title=move.get('previous_title', ''),
                        new_title=move.get('new_title', ''),
                        move_date=move.get('date', 'recent'),
                        source_url=move.get('source_url', '')
                    )
                    
                    if contact_id:
                        move['contact_id'] = contact_id
                        move['from_company'] = client_name
                        move['to_company'] = target['name']
                        all_movements.append(move)
                        
                        send_move_alert(move['name'], client_name, target['name'], move.get('new_title', ''))
                        print(f"    Found: {move['name']} → {target['name']}")
    
    return all_movements


def run_movement_scan(db_path: str = None):
    """Scan all clients for executive movements to targets."""
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT id, name FROM companies 
        WHERE status = 'active_client'
    """)
    clients = [dict(row) for row in cur.fetchall()]
    conn.close()
    
    print(f"Scanning executive movements from {len(clients)} clients...")
    
    all_movements = []
    for client in clients:
        print(f"\nScanning departures from {client['name']}...")
        movements = scan_movements_from_client(client['id'], client['name'], db_path)
        all_movements.extend(movements)
    
    print(f"\nTotal movements found: {len(all_movements)}")
    return all_movements


if __name__ == "__main__":
    # Test with one client
    movements = scan_movements_from_client(
        client_id=7,  # Millennium Management
        client_name="Millennium Management"
    )
    print(f"\nFound {len(movements)} movements")
    for m in movements:
        print(f"  {m['name']}: {m['from_company']} → {m['to_company']}")
