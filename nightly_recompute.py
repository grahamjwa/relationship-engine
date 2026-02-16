"""
Nightly Recompute Script for Relationship Engine
Runs graph computations, opportunity scoring, and posts daily intelligence to Discord.
"""

import os
import sys
import time
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_engine import compute_all, get_db_path, get_top_centrality, get_top_leverage

try:
    from opportunity_scoring import (
        save_opportunity_scores, get_top_opportunities, generate_daily_insights
    )
    HAS_SCORING = True
except ImportError:
    HAS_SCORING = False

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


def _get_recent_funding(db_path):
    """Get recent funding events from last 7 days, deduplicated by company."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT c.name as company_name, 
               f.round_type, 
               MAX(f.amount) as amount,
               f.lead_investor,
               f.event_date
        FROM funding_events f
        JOIN companies c ON f.company_id = c.id
        WHERE f.event_date >= date('now', '-7 days')
        GROUP BY c.id
        ORDER BY f.event_date DESC
        LIMIT 5
    """)
    
    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results


def _log_recompute(db_path, nodes, edges, clusters, top_cent_name, top_lev_name, duration, status, error_message=None):
    """Log recomputation results to recompute_log table."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO recompute_log
            (nodes_computed, edges_computed, clusters_found,
             top_centrality_node, top_leverage_node,
             duration_seconds, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (nodes, edges, clusters, top_cent_name, top_lev_name, round(duration, 2), status, error_message))
    
    conn.commit()
    conn.close()


def _post_to_discord(webhook_url, nodes, edges, clusters, top_centrality, top_leverage, 
                      top_opportunities, insights, recent_funding, duration, db_path):
    """Post daily intelligence report to Discord webhook."""
    if not webhook_url or not HAS_REQUESTS:
        return

    date_str = datetime.now().strftime('%A, %B %d, %Y')
    
    # Top Opportunities
    if top_opportunities:
        opp_lines = "\n".join(
            f"  {i+1}. {o['name']} ({o['status']}) - Score: {o['opportunity_score']:.0f}"
            for i, o in enumerate(top_opportunities[:5])
        )
    else:
        opp_lines = "  None identified"
    
    # Recent Funding (deduplicated)
    if recent_funding:
        funding_lines = "\n".join(
            f"  - {f['company_name']}: {f['round_type'] or 'Funding'}" + 
            (f" - ${f['amount']:,.0f}" if f['amount'] else "") +
            (f" (Lead: {f['lead_investor']})" if f['lead_investor'] else "")
            for f in recent_funding
        )
    else:
        funding_lines = "  None in last 7 days"
    
    # High Priority - with names
    high_priority = insights.get('high_priority', []) if insights else []
    if high_priority:
        hp_lines = "\n".join(
            f"  - {item['company_name']}: {item['type'].replace('_', ' ').title()}"
            for item in high_priority[:5]
        )
    else:
        hp_lines = "  None"
    
    # Undercovered - with names
    undercovered = insights.get('undercovered', []) if insights else []
    if undercovered:
        uc_lines = "\n".join(
            f"  - {item['company_name']} ({item['status']})"
            for item in undercovered[:5]
        )
    else:
        uc_lines = "  None - all targets have recent outreach"
    
    # At Risk - with names
    at_risk = insights.get('at_risk', []) if insights else []
    if at_risk:
        ar_lines = "\n".join(
            f"  - {item['company_name']} (last contact: {item['last_outreach'] or 'never'})"
            for item in at_risk[:5]
        )
    else:
        ar_lines = "  None - all client relationships healthy"
    
    # Top Centrality
    cent_lines = "\n".join(
        f"  {i+1}. {e['name']} - {e.get('centrality_score', 0):.2f}"
        for i, e in enumerate(top_centrality[:5])
    ) or "  No data"
    
    # Top Leverage
    lev_lines = "\n".join(
        f"  {i+1}. {e['name']} - {e.get('leverage_score', 0):.2f}"
        for i, e in enumerate(top_leverage[:5])
    ) or "  No data"

    message = f"""**DAILY INTELLIGENCE REPORT**
{date_str}

**NETWORK OVERVIEW**
Entities: {nodes} | Relationships: {edges} | Clusters: {clusters}

**TOP OPPORTUNITIES**
{opp_lines}

**RECENT FUNDING (Last 7 Days)**
{funding_lines}

**ACTION REQUIRED**

High Priority ({len(high_priority)}):
{hp_lines}

Undercovered Targets ({len(undercovered)}):
{uc_lines}

At Risk Relationships ({len(at_risk)}):
{ar_lines}

**NETWORK LEADERS**

By Centrality:
{cent_lines}

By Leverage:
{lev_lines}

---
**DEFINITIONS**
- Opportunity Score: Weighted combination of funding recency, hiring signals, lease expiry, and relationship proximity
- High Priority: Recent funding, upcoming lease expiry, or high-value hiring signal
- Undercovered: Target companies with no outreach in 90+ days
- At Risk: Active clients with no engagement in 60+ days
- Centrality: Sum of outgoing relationship weights (who has the most direct connections)
- Leverage: 2-hop reach (who can connect you to the most people through their network)

*Report generated in {duration:.1f}s*"""

    try:
        requests.post(webhook_url, json={"content": message}, timeout=10)
    except Exception as e:
        print(f"Discord webhook failed: {e}")


def run_nightly(db_path=None):
    """Execute full nightly recomputation pipeline."""
    if db_path is None:
        db_path = get_db_path()
    
    start = time.time()
    status = "success"
    error_msg = None
    nodes = edges = clusters = 0
    top_centrality = []
    top_leverage = []
    top_opportunities = []
    insights = {}
    recent_funding = []

    try:
        # 1. Run graph computation
        print("Running graph computation...")
        results = compute_all(db_path, verbose=True)
        
        nodes = results.get('nodes', 0)
        edges = results.get('edges', 0)
        clusters = results.get('clusters', 0)
        
        # 2. Run opportunity scoring
        if HAS_SCORING:
            print("\nComputing opportunity scores...")
            save_opportunity_scores(db_path)
            top_opportunities = get_top_opportunities(10, db_path)
            insights = generate_daily_insights(db_path)
            print(f"  High Priority: {len(insights.get('high_priority', []))}")
            print(f"  Undercovered: {len(insights.get('undercovered', []))}")
            print(f"  At Risk: {len(insights.get('at_risk', []))}")
        
        # 3. Get recent funding
        recent_funding = _get_recent_funding(db_path)
        
        # 4. Get top entities from database
        top_centrality = get_top_centrality(5, db_path)
        top_leverage = get_top_leverage(5, db_path)
        
        duration = time.time() - start
        print(f"\nNightly recompute complete in {duration:.1f}s")
        
    except Exception as e:
        duration = time.time() - start
        status = "failed"
        error_msg = str(e)
        print(f"Recomputation FAILED after {duration:.1f}s: {e}")
        import traceback
        traceback.print_exc()

    # Get top names for logging
    top_cent_name = top_centrality[0]['name'] if top_centrality else "N/A"
    top_lev_name = top_leverage[0]['name'] if top_leverage else "N/A"

    # Log to database
    try:
        _log_recompute(db_path, nodes, edges, clusters, top_cent_name, top_lev_name, duration, status, error_msg)
    except Exception as e:
        print(f"Failed to log recompute: {e}")

    # Post to Discord
    webhook_url = _get_discord_webhook()
    if webhook_url:
        _post_to_discord(webhook_url, nodes, edges, clusters, top_centrality, top_leverage,
                         top_opportunities, insights, recent_funding, duration, db_path)

    return {
        'nodes': nodes,
        'edges': edges,
        'clusters': clusters,
        'top_centrality': top_centrality,
        'top_leverage': top_leverage,
        'top_opportunities': top_opportunities,
        'insights': insights,
        'duration': duration,
        'status': status
    }


if __name__ == "__main__":
    run_nightly()
