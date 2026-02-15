#!/usr/bin/env python3
"""
Relationship Engine — Alerts
Checks database for actionable intelligence and posts to Discord webhook.
Usage: python3 alerts.py
"""

import sqlite3
import os
import json
import requests
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/relationship_engine/.env"))

DB_PATH = os.path.expanduser("~/relationship_engine/data/relationship_engine.db")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def post_to_discord(message):
    """Post a message to Discord via webhook."""
    if not DISCORD_WEBHOOK_URL:
        print("  ⚠ DISCORD_WEBHOOK_URL not set in .env")
        print(message)
        return False
    
    # Discord has 2000 char limit per message
    chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
    for chunk in chunks:
        try:
            resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            print(f"  ⚠ Discord post error: {e}")
            return False
    return True

def check_new_funding(conn):
    """Check for funding events added in last 24 hours."""
    rows = conn.execute("""
        SELECT c.name, f.round_type, f.amount, f.source_url
        FROM funding_events f
        JOIN companies c ON f.company_id = c.id
        WHERE f.created_at >= datetime('now', '-24 hours')
        ORDER BY f.created_at DESC
    """).fetchall()
    return rows

def check_high_hiring(conn):
    """Check for high-relevance hiring signals in last 24 hours."""
    rows = conn.execute("""
        SELECT c.name, h.signal_type, h.role_title, h.details, h.source_url
        FROM hiring_signals h
        JOIN companies c ON h.company_id = c.id
        WHERE h.relevance = 'high'
          AND h.created_at >= datetime('now', '-24 hours')
        ORDER BY h.created_at DESC
    """).fetchall()
    return rows

def check_lease_expirations(conn):
    """Check for leases expiring within 6 months."""
    rows = conn.execute("""
        SELECT c.name, b.name as building, l.square_feet, l.lease_expiry,
               CAST((julianday(l.lease_expiry) - julianday('now')) / 30.44 AS INTEGER) as months_left
        FROM leases l
        JOIN companies c ON l.company_id = c.id
        JOIN buildings b ON l.building_id = b.id
        WHERE l.lease_expiry BETWEEN date('now') AND date('now', '+6 months')
        ORDER BY l.lease_expiry ASC
    """).fetchall()
    return rows

def check_overdue_followups(conn):
    """Check for overdue follow-ups."""
    rows = conn.execute("""
        SELECT c.name, ct.first_name || ' ' || ct.last_name as contact,
               o.outreach_type, o.follow_up_date,
               CAST((julianday('now') - julianday(o.follow_up_date)) AS INTEGER) as days_overdue
        FROM outreach_log o
        JOIN companies c ON o.target_company_id = c.id
        LEFT JOIN contacts ct ON o.target_contact_id = ct.id
        WHERE o.follow_up_done = 0
          AND o.follow_up_date <= date('now')
        ORDER BY o.follow_up_date ASC
    """).fetchall()
    return rows

def run_alerts(verbose=True):
    """Run all alert checks and post to Discord."""
    conn = get_db()
    alerts = []
    
    # New funding
    funding = check_new_funding(conn)
    if funding:
        section = "💰 **NEW FUNDING EVENTS** (last 24h)\n"
        for f in funding:
            amt = f"${f['amount']:,.0f}" if f['amount'] else "amount unknown"
            section += f"• **{f['name']}** — {f['round_type']} ({amt})\n"
        alerts.append(section)
    
    # High-relevance hiring
    hiring = check_high_hiring(conn)
    if hiring:
        section = "🔴 **HIGH-PRIORITY HIRING SIGNALS** (last 24h)\n"
        for h in hiring:
            role = h['role_title'] or h['signal_type']
            section += f"• **{h['name']}** — {role}: {h['details'][:100]}\n"
        alerts.append(section)
    
    # Lease expirations
    leases = check_lease_expirations(conn)
    if leases:
        section = "📅 **LEASE EXPIRATIONS** (next 6 months)\n"
        for l in leases:
            sf = f"{l['square_feet']:,} SF" if l['square_feet'] else "SF unknown"
            section += f"• **{l['name']}** @ {l['building']} — {sf} — expires {l['lease_expiry']} ({l['months_left']}mo)\n"
        alerts.append(section)
    
    # Overdue follow-ups
    overdue = check_overdue_followups(conn)
    if overdue:
        section = "⏰ **OVERDUE FOLLOW-UPS**\n"
        for o in overdue:
            contact = o['contact'] if o['contact'] != ' ' else 'unknown'
            section += f"• **{o['name']}** / {contact} — {o['outreach_type']} — {o['days_overdue']} days overdue\n"
        alerts.append(section)
    
    conn.close()
    
    if not alerts:
        if verbose:
            print("  No alerts to send.")
        return 0
    
    # Build message
    header = f"🏢 **RELATIONSHIP ENGINE ALERTS** — {date.today().strftime('%A, %B %d, %Y')}\n"
    header += "━" * 40 + "\n\n"
    message = header + "\n".join(alerts)
    
    if verbose:
        print(message)
    
    post_to_discord(message)
    
    alert_count = len(funding) + len(hiring) + len(leases) + len(overdue)
    if verbose:
        print(f"\n  Posted {alert_count} alerts to Discord.")
    return alert_count

if __name__ == "__main__":
    run_alerts()
