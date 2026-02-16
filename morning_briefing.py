"""
Morning Briefing for Relationship Engine
Sends a proactive daily briefing to Graham via Discord DM.
Runs at 7:00 AM ET via scheduler.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_engine import get_db_path

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass


def get_briefing_data(db_path=None):
    """Gather all data for morning briefing."""
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    data = {}
    
    # Follow-ups due today/overdue
    cur.execute("""
        SELECT c.name, o.notes, o.follow_up_date
        FROM outreach_log o
        JOIN companies c ON o.target_company_id = c.id
        WHERE o.follow_up_date <= date('now')
        AND o.follow_up_done = 0
        ORDER BY o.follow_up_date ASC
        LIMIT 5
    """)
    data['follow_ups'] = [dict(row) for row in cur.fetchall()]
    
    # Hot targets: funded in last 7 days, not contacted
    cur.execute("""
        SELECT c.name, f.round_type, f.amount, f.lead_investor
        FROM companies c
        JOIN funding_events f ON c.id = f.company_id
        WHERE f.event_date >= date('now', '-7 days')
        AND c.spoc_covered = 0
        AND NOT EXISTS (
            SELECT 1 FROM outreach_log o 
            WHERE o.target_company_id = c.id 
            AND o.outreach_date >= f.event_date
        )
        ORDER BY f.amount DESC
        LIMIT 3
    """)
    data['hot_funded'] = [dict(row) for row in cur.fetchall()]
    
    # High hiring activity
    cur.execute("""
        SELECT c.name, COUNT(h.id) as signal_count
        FROM companies c
        JOIN hiring_signals h ON c.id = h.company_id
        WHERE h.signal_date >= date('now', '-7 days')
        AND h.relevance IN ('high', 'medium')
        AND c.spoc_covered = 0
        GROUP BY c.id
        HAVING COUNT(h.id) >= 3
        ORDER BY COUNT(h.id) DESC
        LIMIT 3
    """)
    data['hiring_spikes'] = [dict(row) for row in cur.fetchall()]
    
    # Top 3 by opportunity score (not contacted in 14 days)
    cur.execute("""
        SELECT c.name, ROUND(c.opportunity_score) as score
        FROM companies c
        WHERE c.opportunity_score > 30
        AND c.spoc_covered = 0
        AND c.status IN ('high_growth_target', 'prospect')
        AND NOT EXISTS (
            SELECT 1 FROM outreach_log o 
            WHERE o.target_company_id = c.id 
            AND o.outreach_date >= date('now', '-14 days')
        )
        ORDER BY c.opportunity_score DESC
        LIMIT 3
    """)
    data['top_opportunities'] = [dict(row) for row in cur.fetchall()]
    
    # Outreach stats this week
    cur.execute("""
        SELECT COUNT(*) as count
        FROM outreach_log
        WHERE outreach_date >= date('now', '-7 days')
    """)
    data['outreach_this_week'] = cur.fetchone()['count']
    
    # Meetings booked this week
    cur.execute("""
        SELECT COUNT(*) as count
        FROM outreach_log
        WHERE outreach_date >= date('now', '-7 days')
        AND outcome = 'meeting_booked'
    """)
    data['meetings_booked'] = cur.fetchone()['count']
    
    conn.close()
    return data


def format_briefing(data):
    """Format briefing data into Discord message."""
    today = datetime.now().strftime('%A, %B %d')
    
    lines = [
        f"**☀️ Morning Briefing — {today}**",
        ""
    ]
    
    # Follow-ups
    if data['follow_ups']:
        lines.append("**📅 Follow-ups Due:**")
        for f in data['follow_ups']:
            lines.append(f"  • {f['name']}")
        lines.append("")
    
    # Hot funded
    if data['hot_funded']:
        lines.append("**💰 Just Funded (not contacted):**")
        for h in data['hot_funded']:
            amount = f"${h['amount']:,.0f}" if h['amount'] else "undisclosed"
            investor = f" ({h['lead_investor']})" if h['lead_investor'] else ""
            lines.append(f"  • {h['name']}: {h['round_type'] or 'Funding'} {amount}{investor}")
        lines.append("")
    
    # Hiring spikes
    if data['hiring_spikes']:
        lines.append("**📈 Hiring Spikes:**")
        for h in data['hiring_spikes']:
            lines.append(f"  • {h['name']}: {h['signal_count']} signals this week")
        lines.append("")
    
    # Top opportunities
    if data['top_opportunities']:
        lines.append("**🎯 Top Untouched Opportunities:**")
        for t in data['top_opportunities']:
            lines.append(f"  • {t['name']} (score: {t['score']})")
        lines.append("")
    
    # Weekly stats
    lines.append(f"**📊 This Week:** {data['outreach_this_week']} touches, {data['meetings_booked']} meetings booked")
    
    # Call to action
    if data['hot_funded'] or data['top_opportunities']:
        lines.append("")
        lines.append("*Reply with a company name to get the path.*")
    
    return "\n".join(lines)


def send_discord_dm(message):
    """Send DM via Discord webhook or bot."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    
    if not webhook_url or not HAS_REQUESTS:
        print("Discord webhook not configured")
        return False
    
    try:
        # Add a ping to make it feel like a DM
        payload = {"content": message}
        requests.post(webhook_url, json=payload, timeout=10)
        print("Briefing sent to Discord")
        return True
    except Exception as e:
        print(f"Failed to send briefing: {e}")
        return False


def run_morning_briefing(db_path=None):
    """Generate and send morning briefing."""
    print(f"Generating morning briefing at {datetime.now().strftime('%H:%M:%S')}")
    
    data = get_briefing_data(db_path)
    message = format_briefing(data)
    
    print("\n" + message + "\n")
    
    send_discord_dm(message)
    
    return data


if __name__ == "__main__":
    run_morning_briefing()
