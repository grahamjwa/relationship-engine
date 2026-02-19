"""
Overview Dashboard — Non-technical review for Graham.

Sections:
  1. Needs Attention — overdue follow-ups, high-priority suggestions, at-risk items
  2. Today's Agenda — follow-ups due today, meetings, tasks
  3. Recent Wins — completed deals, successful outreach, approved suggestions
  4. Quick Stats — companies, contacts, outreach, deals
  5. OpenClaw Status — memory count, subagent health, cost, pending suggestions
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="Overview", page_icon="📊", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# DATA
# =============================================================================

conn = get_conn()
cur = conn.cursor()
today_str = datetime.now().strftime('%Y-%m-%d')

# ── NEEDS ATTENTION ──

# Overdue follow-ups
cur.execute("""
    SELECT o.id, o.follow_up_date, c.name as company, o.outreach_type, o.notes
    FROM outreach_log o
    LEFT JOIN companies c ON o.target_company_id = c.id
    WHERE o.follow_up_done = 0 AND o.follow_up_date IS NOT NULL
    AND o.follow_up_date < ?
    ORDER BY o.follow_up_date ASC
    LIMIT 10
""", (today_str,))
overdue_followups = [dict(r) for r in cur.fetchall()]

# High-priority pending suggestions
try:
    cur.execute("""
        SELECT title, description, priority, created_at
        FROM openclaw_suggestions
        WHERE status = 'pending' AND priority = 'high'
        ORDER BY created_at DESC LIMIT 5
    """)
    high_pri_suggestions = [dict(r) for r in cur.fetchall()]
except Exception:
    high_pri_suggestions = []

# At-risk clients (active clients with no outreach in 45+ days)
cur.execute("""
    SELECT c.name, c.id,
           MAX(o.outreach_date) as last_outreach,
           julianday('now') - julianday(MAX(o.outreach_date)) as days_since
    FROM companies c
    LEFT JOIN outreach_log o ON o.target_company_id = c.id
    WHERE c.status = 'active_client'
    GROUP BY c.id
    HAVING days_since > 45 OR last_outreach IS NULL
    ORDER BY days_since DESC
    LIMIT 10
""")
at_risk = [dict(r) for r in cur.fetchall()]

# ── TODAY'S AGENDA ──

cur.execute("""
    SELECT o.id, o.follow_up_date, c.name as company, o.outreach_type, o.notes
    FROM outreach_log o
    LEFT JOIN companies c ON o.target_company_id = c.id
    WHERE o.follow_up_done = 0
    AND o.follow_up_date = ?
    ORDER BY o.follow_up_date ASC
""", (today_str,))
today_followups = [dict(r) for r in cur.fetchall()]

# SPOC check-ins due today
try:
    cur.execute("""
        SELECT name, spoc_broker, spoc_status
        FROM companies
        WHERE spoc_follow_up_date = ?
        AND spoc_status IS NOT NULL
    """, (today_str,))
    today_spoc = [dict(r) for r in cur.fetchall()]
except Exception:
    today_spoc = []

# Agency tasks due today
try:
    cur.execute("""
        SELECT t.task_text, t.tenant_or_company, b.name as building
        FROM agency_tasks t
        LEFT JOIN agency_buildings b ON t.building_id = b.id
        WHERE t.status != 'done' AND t.due_date = ?
    """, (today_str,))
    today_tasks = [dict(r) for r in cur.fetchall()]
except Exception:
    today_tasks = []

# ── RECENT WINS ──

# Closed deals (last 30 days)
cur.execute("""
    SELECT c.name, d.deal_type, d.square_feet, d.deal_value, d.closed_date
    FROM deals d
    JOIN companies c ON d.company_id = c.id
    WHERE d.status IN ('closed', 'won')
    AND d.closed_date >= date('now', '-30 days')
    ORDER BY d.closed_date DESC
    LIMIT 5
""")
recent_wins_deals = [dict(r) for r in cur.fetchall()]

# Successful outreach (last 14 days)
cur.execute("""
    SELECT c.name, o.outreach_type, o.outcome, o.outreach_date
    FROM outreach_log o
    LEFT JOIN companies c ON o.target_company_id = c.id
    WHERE o.outcome IN ('meeting_booked', 'meeting_held', 'responded_positive', 'meeting_set', 'replied')
    AND o.outreach_date >= date('now', '-14 days')
    ORDER BY o.outreach_date DESC
    LIMIT 5
""")
recent_wins_outreach = [dict(r) for r in cur.fetchall()]

# Completed suggestions (last 14 days)
try:
    cur.execute("""
        SELECT title, responded_at
        FROM openclaw_suggestions
        WHERE status = 'completed'
        AND responded_at >= datetime('now', '-14 days')
        ORDER BY responded_at DESC LIMIT 5
    """)
    recent_completed = [dict(r) for r in cur.fetchall()]
except Exception:
    recent_completed = []

# ── QUICK STATS ──

cur.execute("SELECT COUNT(*) FROM companies")
total_companies = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM contacts")
total_contacts = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM outreach_log WHERE outreach_date >= date('now', '-30 days')")
outreach_30d = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM deals WHERE status IN ('active', 'in_progress')")
active_deals = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM companies WHERE status = 'active_client'")
active_clients = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM funding_events WHERE event_date >= date('now', '-30 days')")
funding_30d = cur.fetchone()[0]

# ── OPENCLAW STATUS ──

try:
    cur.execute("SELECT COUNT(*) FROM openclaw_memory")
    memory_count = cur.fetchone()[0]
except Exception:
    memory_count = 0

try:
    cur.execute("SELECT COUNT(*) FROM subagents WHERE enabled = 1")
    active_agents = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM subagent_runs WHERE status = 'failed' AND started_at >= datetime('now', '-7 days')")
    agent_failures = cur.fetchone()[0]
except Exception:
    active_agents = 0
    agent_failures = 0

try:
    cur.execute("SELECT COALESCE(SUM(cost_usd), 0) FROM api_usage WHERE created_at >= datetime('now', '-30 days')")
    monthly_cost = cur.fetchone()[0]
except Exception:
    monthly_cost = 0

try:
    cur.execute("SELECT COUNT(*) FROM openclaw_suggestions WHERE status = 'pending'")
    pending_suggestions = cur.fetchone()[0]
except Exception:
    pending_suggestions = 0

conn.close()


# =============================================================================
# UI
# =============================================================================

st.title("Overview")
st.caption(f"{datetime.now().strftime('%A, %B %d, %Y')}")

# ─────────────────────────────────────────────────────────────────────────────
# NEEDS ATTENTION
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Needs Attention")

attention_count = len(overdue_followups) + len(high_pri_suggestions) + len(at_risk)

if attention_count == 0:
    st.success("Nothing urgent. You're on top of it.")
else:
    if overdue_followups:
        st.markdown("**Overdue Follow-ups:**")
        for fu in overdue_followups:
            days = (datetime.now().date() - datetime.strptime(fu['follow_up_date'], '%Y-%m-%d').date()).days
            st.markdown(f"🔴 **{fu.get('company', '?')}** — {fu['outreach_type']} "
                       f"({days}d overdue)" +
                       (f" — {fu['notes'][:60]}" if fu.get('notes') else ""))

    if at_risk:
        st.markdown("**At-Risk Clients** (no contact 45+ days):")
        for ar in at_risk:
            days = int(ar['days_since']) if ar.get('days_since') else 999
            last = ar.get('last_outreach') or 'never'
            st.markdown(f"🟠 **{ar['name']}** — last contact: {last} ({days}d ago)")

    if high_pri_suggestions:
        st.markdown("**High-Priority Suggestions:**")
        for s in high_pri_suggestions:
            st.markdown(f"🔴 {s['title']}")

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# TODAY'S AGENDA
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Today's Agenda")

agenda_items = len(today_followups) + len(today_spoc) + len(today_tasks)

if agenda_items == 0:
    st.info("Clear schedule today.")
else:
    if today_followups:
        for tf in today_followups:
            st.markdown(f"📋 **{tf.get('company', '?')}** — {tf['outreach_type']} follow-up"
                       + (f" — {tf['notes'][:60]}" if tf.get('notes') else ""))

    if today_spoc:
        for sp in today_spoc:
            st.markdown(f"🔒 **{sp['name']}** — SPOC check-in"
                       + (f" (Broker: {sp['spoc_broker']})" if sp.get('spoc_broker') else ""))

    if today_tasks:
        for tt in today_tasks:
            bldg = tt.get('building') or ''
            st.markdown(f"🏢 [{bldg}] **{tt.get('tenant_or_company', '?')}** — {tt['task_text']}")

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# RECENT WINS
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Recent Wins")

has_wins = recent_wins_deals or recent_wins_outreach or recent_completed

if not has_wins:
    st.caption("No wins logged recently. Keep grinding.")
else:
    if recent_wins_deals:
        for d in recent_wins_deals:
            sf = f"{d['square_feet']:,.0f} SF" if d.get('square_feet') else ""
            val = f"${d['deal_value']:,.0f}" if d.get('deal_value') else ""
            st.markdown(f"✅ **{d['name']}** — {d['deal_type']} closed {sf} {val}")

    if recent_wins_outreach:
        for o in recent_wins_outreach:
            st.markdown(f"🟢 **{o.get('name', '?')}** — {o['outcome'].replace('_', ' ')} "
                       f"via {o['outreach_type']} ({o['outreach_date']})")

    if recent_completed:
        for c in recent_completed:
            st.markdown(f"💡 Completed: {c['title']}")

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# QUICK STATS
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("Quick Stats")

s1, s2, s3, s4, s5, s6 = st.columns(6)
s1.metric("Companies", total_companies)
s2.metric("Contacts", total_contacts)
s3.metric("Active Clients", active_clients)
s4.metric("Active Deals", active_deals)
s5.metric("Outreach (30d)", outreach_30d)
s6.metric("Funding (30d)", funding_30d)

st.markdown("---")

# ─────────────────────────────────────────────────────────────────────────────
# OPENCLAW STATUS
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("OpenClaw Status")

oc1, oc2, oc3, oc4 = st.columns(4)
oc1.metric("Memories Stored", memory_count)
oc2.metric("Active Subagents", active_agents)
oc3.metric("Monthly API Cost", f"${monthly_cost:.4f}")
oc4.metric("Pending Suggestions", pending_suggestions)

if agent_failures > 0:
    st.warning(f"{agent_failures} subagent failures in the last 7 days.")

st.markdown("---")
st.caption("Relationship Engine — Overview Dashboard")
