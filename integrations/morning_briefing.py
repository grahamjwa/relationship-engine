"""
Morning Briefing for Relationship Engine
Sends a proactive daily briefing to Graham via Discord DM.
Runs at 7:00 AM ET via scheduler.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path
from core.agency_engine import get_agency_briefing_data
from scrapers.executive_scanner import get_executive_briefing_data
from import_all import list_pending_imports, IMPORT_DIR

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from dotenv import load_dotenv
    load_dotenv()  # config.py already loaded .env
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
        AND COALESCE(c.spoc_covered, 0) = 0
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
        AND COALESCE(c.spoc_covered, 0) = 0
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
        AND COALESCE(c.spoc_covered, 0) = 0
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
    
    # New opportunities from overnight scans (hiring_signals inserted in last 24h)
    cur.execute("""
        SELECT c.name, h.signal_type, h.details, h.relevance
        FROM hiring_signals h
        JOIN companies c ON h.company_id = c.id
        WHERE h.signal_date >= date('now', '-1 day')
        AND h.relevance = 'high'
        ORDER BY h.signal_date DESC
        LIMIT 5
    """)
    data['new_opportunities'] = [dict(row) for row in cur.fetchall()]

    # Warm intros available (contacts at targets who previously worked at our clients)
    cur.execute("""
        SELECT ct.first_name || ' ' || ct.last_name AS contact_name,
               ct.title,
               comp.name AS target_company,
               ct.previous_companies
        FROM contacts ct
        JOIN companies comp ON ct.company_id = comp.id
        WHERE comp.status IN ('high_growth_target', 'prospect')
        AND ct.previous_companies IS NOT NULL
        AND ct.previous_companies != ''
        AND EXISTS (
            SELECT 1 FROM companies cl
            WHERE cl.status = 'active_client'
            AND ct.previous_companies LIKE '%' || cl.name || '%'
        )
        LIMIT 5
    """)
    data['warm_intros'] = [dict(row) for row in cur.fetchall()]

    # Competitor activity (deals lost in last 30 days)
    cur.execute("""
        SELECT c.name AS company, d.notes
        FROM deals d
        JOIN companies c ON d.company_id = c.id
        WHERE d.status = 'lost'
        AND d.closed_date >= date('now', '-30 days')
        ORDER BY d.closed_date DESC
        LIMIT 3
    """)
    data['competitor_activity'] = [dict(row) for row in cur.fetchall()]

    # Predictive chain: companies with high lease probability
    cur.execute("""
        SELECT name, ROUND(chain_lease_prob) AS prob
        FROM companies
        WHERE chain_lease_prob > 50
        ORDER BY chain_lease_prob DESC
        LIMIT 3
    """)
    data['lease_predictions'] = [dict(row) for row in cur.fetchall()]

    conn.close()

    # Pending imports
    data['pending_imports'] = list_pending_imports()

    # Agency briefing data
    try:
        data['agency'] = get_agency_briefing_data(db_path)
    except Exception:
        data['agency'] = {}

    # Executive changes
    try:
        data['executives'] = get_executive_briefing_data(db_path)
    except Exception:
        data['executives'] = {}

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
    
    # New opportunities from overnight scans
    if data.get('new_opportunities'):
        lines.append("**🆕 New Opportunities Found (overnight):**")
        for opp in data['new_opportunities']:
            details = opp.get('details', '')[:80]
            lines.append(f"  • {opp['name']}: {opp['signal_type']} — {details}")
        lines.append("")

    # Warm intros available
    if data.get('warm_intros'):
        lines.append("**🤝 Warm Intros Available:**")
        for wi in data['warm_intros']:
            lines.append(f"  • {wi['contact_name']} ({wi['title']}) at {wi['target_company']} — ex {wi['previous_companies']}")
        lines.append("")

    # Competitor activity
    if data.get('competitor_activity'):
        lines.append("**⚠️ Competitor Activity (last 30d):**")
        for ca in data['competitor_activity']:
            notes = ca.get('notes', 'no details')[:60]
            lines.append(f"  • {ca['company']}: {notes}")
        lines.append("")

    # Lease predictions
    if data.get('lease_predictions'):
        lines.append("**🔮 Lease Probability (>50%):**")
        for lp in data['lease_predictions']:
            lines.append(f"  • {lp['name']}: {lp['prob']}% likely to lease")
        lines.append("")

    # Pending imports
    if data.get('pending_imports'):
        lines.append("**📥 Data Ready to Import:**")
        for f in data['pending_imports']:
            lines.append(f"  • {f}")
        lines.append(f"  Run `python import_all.py` to process.")
        lines.append("")

    # Executive changes (24h)
    execs = data.get('executives', {})
    if execs.get('high_priority_changes'):
        lines.append("**🔔 Executive Changes (24h):**")
        for ec in execs['high_priority_changes']:
            from_part = f" (from {ec['old_company']})" if ec.get('old_company') else ""
            lines.append(f"  • 🔴 {ec['person_name']} → {ec.get('new_title', '?')} "
                         f"at {ec['company_name']}{from_part}")
        if execs.get('medium_count', 0) > 0:
            lines.append(f"  • + {execs['medium_count']} medium-priority changes")
        lines.append("")
    elif execs.get('total_24h', 0) > 0:
        lines.append(f"**🔔 Executive Changes:** {execs['total_24h']} changes in last 24h (no high-priority)")
        lines.append("")

    # Agency: Tasks due today
    agency = data.get('agency', {})
    if agency.get('tasks_due'):
        lines.append("**🏢 Agency Tasks Due:**")
        for t in agency['tasks_due'][:5]:
            bldg = t.get('building_name') or ''
            tenant = t.get('tenant_or_company') or ''
            prefix = f"[{bldg}] " if bldg else ""
            label = f"**{tenant}**: " if tenant else ""
            priority_badge = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(
                t.get('priority', ''), "")
            lines.append(f"  • {priority_badge} {prefix}{label}{t['task_text']}")
        lines.append("")

    # Agency: Expiring tenants
    if agency.get('expiring_soon'):
        lines.append("**⏰ Leases Expiring Soon:**")
        for t in agency['expiring_soon']:
            bldg = t.get('building_name') or ''
            lines.append(f"  • {t['tenant_name']} at {bldg} — expires {t['lease_expiry']}")
        lines.append("")

    # Agency: New market matches
    if agency.get('new_market_matches', 0) > 0:
        lines.append(f"**🎯 Market Matches: {agency['new_market_matches']} requirement(s) match your buildings**")
        for bname, info in agency.get('match_details', {}).items():
            for m in info['matches'][:3]:
                floors = ", ".join(f"Fl {f['floor']}" for f in m['matched_floors'][:3])
                lines.append(f"  • {m['company']} ({m['sf_min']:,}-{m['sf_max']:,} SF) → {bname}: {floors}")
        lines.append("")

    # Agency: Recent activity
    if agency.get('recent_activity'):
        lines.append("**📊 Agency Activity (7d):**")
        for a in agency['recent_activity'][:5]:
            bldg = a.get('building_name') or ''
            lines.append(f"  • {a['activity_date']} | {a['activity_type']} | {a.get('company_name', '')} at {bldg}")
        lines.append("")

    # Weekly stats
    lines.append(f"**📊 This Week:** {data['outreach_this_week']} touches, {data['meetings_booked']} meetings booked")

    # Call to action
    if data['hot_funded'] or data['top_opportunities'] or data.get('warm_intros'):
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
