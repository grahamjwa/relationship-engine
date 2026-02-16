"""
Weekly Digest for Relationship Engine
Sends a comprehensive weekly summary to Discord every Sunday.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

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


def _get_discord_webhook():
    return os.environ.get("DISCORD_WEBHOOK_URL", "")


def get_weekly_stats(db_path: str) -> dict:
    """Get statistics for the past week."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    stats = {}
    
    # New funding events this week
    cur.execute("""
        SELECT COUNT(DISTINCT company_id), SUM(amount)
        FROM funding_events 
        WHERE event_date >= date('now', '-7 days')
    """)
    row = cur.fetchone()
    stats['funding_companies'] = row[0] or 0
    stats['funding_total'] = row[1] or 0
    
    # New hiring signals this week
    cur.execute("""
        SELECT COUNT(*), COUNT(DISTINCT company_id)
        FROM hiring_signals 
        WHERE signal_date >= date('now', '-7 days')
    """)
    row = cur.fetchone()
    stats['hiring_signals'] = row[0] or 0
    stats['hiring_companies'] = row[1] or 0
    
    # High relevance hiring signals
    cur.execute("""
        SELECT COUNT(*)
        FROM hiring_signals 
        WHERE signal_date >= date('now', '-7 days')
        AND relevance = 'high'
    """)
    stats['high_relevance_hiring'] = cur.fetchone()[0] or 0
    
    # Outreach activity this week
    cur.execute("""
        SELECT COUNT(*), COUNT(DISTINCT target_company_id)
        FROM outreach_log 
        WHERE outreach_date >= date('now', '-7 days')
    """)
    row = cur.fetchone()
    stats['outreach_count'] = row[0] or 0
    stats['outreach_companies'] = row[1] or 0
    
    # Deals updated this week
    cur.execute("""
        SELECT COUNT(*)
        FROM deals 
        WHERE updated_at >= date('now', '-7 days')
    """)
    stats['deals_updated'] = cur.fetchone()[0] or 0
    
    # New relationships added
    cur.execute("""
        SELECT COUNT(*)
        FROM relationships 
        WHERE created_at >= date('now', '-7 days')
    """)
    stats['new_relationships'] = cur.fetchone()[0] or 0
    
    conn.close()
    return stats


def get_top_movers(db_path: str) -> dict:
    """Get companies with biggest opportunity score changes."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Top opportunities
    cur.execute("""
        SELECT name, status, opportunity_score
        FROM companies
        WHERE opportunity_score IS NOT NULL
        ORDER BY opportunity_score DESC
        LIMIT 5
    """)
    top_opportunities = [dict(row) for row in cur.fetchall()]
    
    # Recently funded
    cur.execute("""
        SELECT c.name, f.round_type, f.amount, f.lead_investor
        FROM funding_events f
        JOIN companies c ON f.company_id = c.id
        WHERE f.event_date >= date('now', '-7 days')
        GROUP BY c.id
        ORDER BY f.amount DESC
        LIMIT 5
    """)
    recently_funded = [dict(row) for row in cur.fetchall()]
    
    # Untouched high-value targets
    cur.execute("""
        SELECT c.name, c.opportunity_score
        FROM companies c
        WHERE c.status IN ('active_client', 'high_growth_target', 'prospect')
        AND c.opportunity_score > 20
        AND NOT EXISTS (
            SELECT 1 FROM outreach_log o 
            WHERE o.target_company_id = c.id 
            AND o.outreach_date >= date('now', '-30 days')
        )
        ORDER BY c.opportunity_score DESC
        LIMIT 5
    """)
    untouched = [dict(row) for row in cur.fetchall()]
    
    conn.close()
    return {
        'top_opportunities': top_opportunities,
        'recently_funded': recently_funded,
        'untouched': untouched
    }


def get_network_health(db_path: str) -> dict:
    """Get network health metrics."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Total entities
    cur.execute("SELECT COUNT(*) FROM companies")
    total_companies = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM contacts")
    total_contacts = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM relationships")
    total_relationships = cur.fetchone()[0]
    
    # Active clients
    cur.execute("SELECT COUNT(*) FROM companies WHERE status = 'active_client'")
    active_clients = cur.fetchone()[0]
    
    # Monitored clients
    cur.execute("SELECT COUNT(*) FROM monitored_clients")
    monitored = cur.fetchone()[0]
    
    # At risk (active clients with no outreach in 60 days)
    cur.execute("""
        SELECT COUNT(*)
        FROM companies c
        WHERE c.status = 'active_client'
        AND NOT EXISTS (
            SELECT 1 FROM outreach_log o 
            WHERE o.target_company_id = c.id 
            AND o.outreach_date >= date('now', '-60 days')
        )
    """)
    at_risk = cur.fetchone()[0]
    
    conn.close()
    return {
        'total_companies': total_companies,
        'total_contacts': total_contacts,
        'total_relationships': total_relationships,
        'active_clients': active_clients,
        'monitored': monitored,
        'at_risk': at_risk
    }


def generate_weekly_digest(db_path: str = None) -> str:
    """Generate the weekly digest message."""
    if db_path is None:
        db_path = get_db_path()
    
    stats = get_weekly_stats(db_path)
    movers = get_top_movers(db_path)
    health = get_network_health(db_path)
    
    # Format date range
    today = datetime.now()
    week_ago = today - timedelta(days=7)
    date_range = f"{week_ago.strftime('%b %d')} - {today.strftime('%b %d, %Y')}"
    
    # Top opportunities
    if movers['top_opportunities']:
        opp_lines = "\n".join(
            f"  {i+1}. {o['name']} ({o['status']}) - Score: {o['opportunity_score']:.0f}"
            for i, o in enumerate(movers['top_opportunities'])
        )
    else:
        opp_lines = "  None scored yet"
    
    # Recently funded
    if movers['recently_funded']:
        funded_lines = "\n".join(
            f"  - {f['name']}: {f['round_type'] or 'Funding'}" + 
            (f" - ${f['amount']:,.0f}" if f['amount'] else "") +
            (f" ({f['lead_investor']})" if f['lead_investor'] else "")
            for f in movers['recently_funded']
        )
    else:
        funded_lines = "  None this week"
    
    # Untouched
    if movers['untouched']:
        untouched_lines = "\n".join(
            f"  - {u['name']} (Score: {u['opportunity_score']:.0f})"
            for u in movers['untouched']
        )
    else:
        untouched_lines = "  All high-value targets contacted"
    
    message = f"""**WEEKLY INTELLIGENCE DIGEST**
{date_range}

**THIS WEEK'S ACTIVITY**
Funding Events: {stats['funding_companies']} companies raised ${stats['funding_total']:,.0f}
Hiring Signals: {stats['hiring_signals']} signals from {stats['hiring_companies']} companies ({stats['high_relevance_hiring']} high-relevance)
Outreach: {stats['outreach_count']} touches to {stats['outreach_companies']} companies
Deals Updated: {stats['deals_updated']}
New Relationships: {stats['new_relationships']}

**TOP OPPORTUNITIES**
{opp_lines}

**RECENTLY FUNDED**
{funded_lines}

**NEED ATTENTION (High-value, no recent outreach)**
{untouched_lines}

**NETWORK HEALTH**
Companies: {health['total_companies']} | Contacts: {health['total_contacts']} | Relationships: {health['total_relationships']}
Active Clients: {health['active_clients']} | Monitored: {health['monitored']} | At Risk: {health['at_risk']}

---
*Weekly digest generated {today.strftime('%A, %B %d at %I:%M %p')}*"""

    return message


def post_weekly_digest(db_path: str = None):
    """Post weekly digest to Discord."""
    webhook_url = _get_discord_webhook()
    if not webhook_url or not HAS_REQUESTS:
        print("Discord webhook not configured")
        return
    
    message = generate_weekly_digest(db_path)
    
    try:
        requests.post(webhook_url, json={"content": message}, timeout=10)
        print("Weekly digest posted to Discord")
    except Exception as e:
        print(f"Discord webhook failed: {e}")


if __name__ == "__main__":
    print(generate_weekly_digest())
    print("\n--- Posting to Discord ---")
    post_weekly_digest()
