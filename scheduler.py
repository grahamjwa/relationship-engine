#!/usr/bin/env python3
"""
Relationship Engine — Scheduler
Runs scrapers, generates reports, and posts alerts on a daily schedule.
Usage: python3 scheduler.py
       python3 scheduler.py --test  (run all jobs once immediately)

Schedule (ET):
  6:00 AM  — Run funding + hiring scrapers
  6:30 AM  — Generate daily intelligence report → Discord
  5:00 PM  — Run scrapers again (afternoon check)
"""

import sqlite3
import os
import sys
import time
import schedule
import requests
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/relationship_engine/.env"))

# Add project root to path for imports
sys.path.insert(0, os.path.expanduser("~/relationship_engine"))

DB_PATH = os.path.expanduser("~/relationship_engine/data/relationship_engine.db")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def post_to_discord(message):
    """Post to Discord via webhook."""
    if not DISCORD_WEBHOOK_URL:
        print(f"  [{datetime.now().strftime('%H:%M')}] ⚠ No DISCORD_WEBHOOK_URL set")
        print(message)
        return
    chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
    for chunk in chunks:
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
        except Exception as e:
            print(f"  ⚠ Discord error: {e}")

def job_morning_scrape():
    """6:00 AM — Run all scrapers."""
    print(f"\n  [{datetime.now().strftime('%H:%M')}] Running morning scrape...")
    try:
        from scrapers.funding_scraper import run_scraper as run_funding
        funding_count = run_funding(verbose=True)
    except Exception as e:
        print(f"  ⚠ Funding scraper error: {e}")
        funding_count = 0
    
    try:
        from scrapers.hiring_scraper import run_scraper as run_hiring
        hiring_count = run_hiring(verbose=True)
    except Exception as e:
        print(f"  ⚠ Hiring scraper error: {e}")
        hiring_count = 0
    
    print(f"  [{datetime.now().strftime('%H:%M')}] Scrape complete: {funding_count} funding, {hiring_count} hiring signals")

def job_daily_report():
    """6:30 AM — Generate and post daily intelligence report."""
    print(f"\n  [{datetime.now().strftime('%H:%M')}] Generating daily report...")
    conn = get_db()
    
    report = f"🏢 **DAILY INTELLIGENCE REPORT** — {date.today().strftime('%A, %B %d, %Y')}\n"
    report += "━" * 40 + "\n\n"
    
    # Stats
    tables = ['companies', 'contacts', 'relationships', 'outreach_log', 'funding_events', 'hiring_signals']
    stats = []
    for t in tables:
        count = conn.execute(f"SELECT COUNT(*) as c FROM {t}").fetchone()['c']
        stats.append(f"{t.replace('_', ' ').title()}: {count}")
    report += "📊 **Database:** " + " | ".join(stats) + "\n\n"
    
    # Untouched targets
    untouched = conn.execute("SELECT * FROM v_untouched_targets").fetchall()
    if untouched:
        report += f"🎯 **Untouched Targets** ({len(untouched)})\n"
        for u in untouched:
            report += f"• {u['name']} — {u['status']} ({u['sector'] or 'no sector'})\n"
        report += "\n"
    
    # Intro paths
    paths = conn.execute("SELECT * FROM v_intro_paths LIMIT 10").fetchall()
    if paths:
        report += f"🔗 **Top Intro Paths**\n"
        for p in paths:
            report += f"• {p['team_member']} → {p['connector']} → {p['target_person']} ({p['target_title']}) @ {p['target_company']}\n"
        report += "\n"
    
    # Recent funding
    funding = conn.execute("SELECT * FROM v_recent_funding LIMIT 5").fetchall()
    if funding:
        report += "💰 **Recent Funding**\n"
        for f in funding:
            amt = f"${f['amount']:,.0f}" if f['amount'] else "amount unknown"
            report += f"• {f['company_name']} — {f['round_type']} ({amt}) — {f['days_since_funding']}d ago\n"
        report += "\n"
    
    # High-value hiring
    hiring = conn.execute("SELECT * FROM v_high_value_hiring LIMIT 5").fetchall()
    if hiring:
        report += "👥 **High-Value Hiring Signals**\n"
        for h in hiring:
            report += f"• {h['company_name']} — {h['signal_type']}: {h['role_title'] or 'N/A'}\n"
        report += "\n"
    
    # Upcoming expirations
    expirations = conn.execute("SELECT * FROM v_upcoming_expirations LIMIT 5").fetchall()
    if expirations:
        report += "📅 **Upcoming Lease Expirations**\n"
        for e in expirations:
            sf = f"{e['square_feet']:,} SF" if e['square_feet'] else "SF unknown"
            report += f"• {e['company_name']} @ {e['building_name']} — {sf} — {e['months_until_expiry']}mo\n"
        report += "\n"
    
    # Overdue follow-ups
    overdue = conn.execute("SELECT * FROM v_overdue_followups").fetchall()
    if overdue:
        report += f"⏰ **Overdue Follow-ups** ({len(overdue)})\n"
        for o in overdue:
            report += f"• {o['company_name']} / {o['contact_name'] or 'unknown'} — {o['days_overdue']}d overdue\n"
        report += "\n"
    
    # Outreach effectiveness
    effectiveness = conn.execute("SELECT * FROM v_outreach_effectiveness").fetchall()
    if effectiveness:
        report += "📞 **Outreach Effectiveness**\n"
        for e in effectiveness:
            report += f"• {e['outreach_type']}: {e['success_rate_pct']}% ({e['positive_outcomes']}/{e['total_attempts']})\n"
        report += "\n"
    
    conn.close()
    
    print(report)
    post_to_discord(report)
    print(f"  [{datetime.now().strftime('%H:%M')}] Report posted.")

def job_afternoon_scrape():
    """5:00 PM — Run scrapers again."""
    print(f"\n  [{datetime.now().strftime('%H:%M')}] Running afternoon scrape...")
    job_morning_scrape()

def job_afternoon_alerts():
    """5:15 PM — Check and post alerts."""
    print(f"\n  [{datetime.now().strftime('%H:%M')}] Running alerts check...")
    try:
        from alerts import run_alerts
        run_alerts(verbose=True)
    except Exception as e:
        print(f"  ⚠ Alerts error: {e}")

def run_test():
    """Run all jobs once immediately for testing."""
    print("\n  ═══ TEST MODE — Running all jobs ═══\n")
    job_morning_scrape()
    print("\n  --- \n")
    job_daily_report()
    print("\n  --- \n")
    job_afternoon_alerts()
    print("\n  ═══ TEST COMPLETE ═══")

def main():
    if "--test" in sys.argv:
        run_test()
        return
    
    print("\n  ═══ Relationship Engine Scheduler ═══")
    print(f"  Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}")
    print("  Schedule:")
    print("    6:00 AM  — Morning scrape")
    print("    6:30 AM  — Daily intelligence report")
    print("    5:00 PM  — Afternoon scrape")
    print("    5:15 PM  — Alerts check")
    print("  Press Ctrl+C to stop.\n")
    
    # Schedule jobs (ET times)
    schedule.every().day.at("06:00").do(job_morning_scrape)
    schedule.every().day.at("06:30").do(job_daily_report)
    schedule.every().day.at("17:00").do(job_afternoon_scrape)
    schedule.every().day.at("17:15").do(job_afternoon_alerts)
    
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
