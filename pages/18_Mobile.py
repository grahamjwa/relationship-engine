"""
Mobile View — Simplified single-column layout for phone access.

Sections:
  1. Today's Follow-ups (with Done button)
  2. High Alerts (last 3)
  3. Quick Log buttons
  4. Recent Activity (last 5)
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="Mobile", page_icon="📱", layout="centered")

DB_PATH = get_db_path()
today_str = datetime.now().strftime('%Y-%m-%d')


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# 1. TODAY'S FOLLOW-UPS
# =============================================================================

st.markdown("### 📋 Follow-ups")

conn = get_conn()
cur = conn.cursor()

# Overdue + today
cur.execute("""
    SELECT o.id, o.follow_up_date, c.name as company, o.outreach_type, o.notes
    FROM outreach_log o
    LEFT JOIN companies c ON o.target_company_id = c.id
    WHERE o.follow_up_done = 0
    AND o.follow_up_date IS NOT NULL
    AND o.follow_up_date <= ?
    ORDER BY o.follow_up_date ASC
    LIMIT 10
""", (today_str,))
followups = [dict(r) for r in cur.fetchall()]
conn.close()

if followups:
    for fu in followups:
        is_overdue = fu['follow_up_date'] < today_str
        prefix = "🔴" if is_overdue else "🔵"
        company = fu.get('company', '?')
        otype = fu.get('outreach_type', '?')

        cols = st.columns([4, 1])
        with cols[0]:
            st.markdown(f"{prefix} **{company}** — {otype}")
            if fu.get('notes'):
                st.caption(fu['notes'][:60])
        with cols[1]:
            if st.button("✅", key=f"mob_done_{fu['id']}"):
                conn = get_conn()
                conn.execute("UPDATE outreach_log SET follow_up_done = 1 WHERE id = ?",
                           (fu['id'],))
                conn.commit()
                conn.close()
                st.rerun()
else:
    st.caption("No follow-ups due.")

st.markdown("---")

# =============================================================================
# 2. HIGH ALERTS
# =============================================================================

st.markdown("### 🔴 Alerts")

conn = get_conn()
cur = conn.cursor()
alerts = []

# Exec changes
try:
    cur.execute("""
        SELECT person_name, new_title, new_company, change_type, effective_date
        FROM executive_changes
        WHERE priority = 'high'
        AND effective_date >= date('now', '-7 days')
        ORDER BY effective_date DESC LIMIT 2
    """)
    for r in cur.fetchall():
        r = dict(r)
        alerts.append(f"👔 {r['person_name']} → {r.get('new_title', '?')} at {r.get('new_company', '?')}")
except Exception:
    pass

# Large funding
try:
    cur.execute("""
        SELECT c.name, f.amount, f.round_type, f.event_date, f.lead_investor
        FROM funding_events f
        JOIN companies c ON f.company_id = c.id
        WHERE f.amount >= 50000000
        AND f.event_date >= date('now', '-7 days')
        ORDER BY f.amount DESC LIMIT 2
    """)
    for r in cur.fetchall():
        r = dict(r)
        date_str = r['event_date'][:10] if r.get('event_date') else '?'
        amt = f"${r['amount']:,.0f}" if r.get('amount') else "?"
        val_str = ""
        round_type = r.get('round_type', '?')

        # Check if lead investor is in our DB
        investor_str = ""
        if r.get('lead_investor'):
            investor_str = r['lead_investor']

        alerts.append(f"💰 ({date_str}) **{r['name']}** received {amt} ({round_type})"
                     + (f" from {investor_str}" if investor_str else ""))
except Exception:
    pass

# Agency tenant expiring
try:
    cur.execute("""
        SELECT t.tenant_name, t.occupied_sf, b.name as building
        FROM agency_tenants t
        JOIN agency_buildings b ON t.building_id = b.id
        WHERE t.lease_expiry_date BETWEEN date('now') AND date('now', '+6 months')
        AND t.occupied_sf >= 10000
        LIMIT 1
    """)
    for r in cur.fetchall():
        r = dict(r)
        alerts.append(f"🏢 {r['tenant_name']} ({r['occupied_sf']:,} SF) at {r['building']} — expiring soon")
except Exception:
    pass

conn.close()

if alerts:
    for a in alerts[:3]:
        st.markdown(a)
else:
    st.caption("No alerts.")

st.markdown("---")

# =============================================================================
# 3. QUICK LOG
# =============================================================================

st.markdown("### ⚡ Quick Log")

log_type = st.radio("Type", ["Call", "Meeting", "Rumor", "Funding"], horizontal=True, key="mob_log_type")

log_company = st.text_input("Company", key="mob_log_company")
log_notes = st.text_input("Notes", key="mob_log_notes")

if st.button("Log It", key="mob_log_btn") and log_company:
    conn = get_conn()
    cur = conn.cursor()

    # Find company
    cur.execute("SELECT id FROM companies WHERE name LIKE ?", (f"%{log_company}%",))
    company_row = cur.fetchone()
    company_id = company_row['id'] if company_row else None

    if log_type in ("Call", "Meeting"):
        outreach_type = 'call' if log_type == 'Call' else 'in_person'
        cur.execute("""
            INSERT INTO outreach_log
            (target_company_id, outreach_date, outreach_type, outcome, notes,
             follow_up_date, follow_up_done)
            VALUES (?, date('now'), ?, 'pending', ?, date('now', '+7 days'), 0)
        """, (company_id, outreach_type, log_notes or f"{log_type} with {log_company}"))
        conn.commit()
        st.success(f"Logged {log_type.lower()} with {log_company}")

    elif log_type == "Rumor":
        # Parse SF from notes if possible
        sf = 0
        import re
        sf_match = re.search(r'(\d[\d,]*)\s*(?:sf|SF|rsf|RSF)', log_notes or '')
        if sf_match:
            sf = int(sf_match.group(1).replace(',', ''))

        cur.execute("""
            INSERT INTO market_requirements (company_id, sf_min, notes, source, status)
            VALUES (?, ?, ?, 'discord', 'active')
        """, (company_id, sf, log_notes or f"{log_company} in market"))
        conn.commit()
        st.success(f"Logged rumor: {log_company}")

    elif log_type == "Funding":
        # Parse amount from notes
        amount = 0
        amt_match = re.search(r'\$?([\d,.]+)\s*([MBmb])', log_notes or '') if log_notes else None
        if amt_match:
            num = float(amt_match.group(1).replace(',', ''))
            mult = amt_match.group(2).upper()
            amount = int(num * 1000000 if mult == 'M' else num * 1000000000)

        cur.execute("""
            INSERT INTO funding_events (company_id, event_date, amount, round_type, notes)
            VALUES (?, date('now'), ?, 'unknown', ?)
        """, (company_id, amount, log_notes or f"Funding for {log_company}"))
        conn.commit()
        st.success(f"Logged funding: {log_company}")

    conn.close()

st.markdown("---")

# =============================================================================
# 4. RECENT ACTIVITY
# =============================================================================

st.markdown("### 📜 Recent")

conn = get_conn()
cur = conn.cursor()

cur.execute("""
    SELECT o.outreach_date, c.name as company, o.outreach_type, o.outcome, o.notes
    FROM outreach_log o
    LEFT JOIN companies c ON o.target_company_id = c.id
    ORDER BY o.outreach_date DESC, o.id DESC
    LIMIT 5
""")
recent = [dict(r) for r in cur.fetchall()]
conn.close()

if recent:
    for r in recent:
        company = r.get('company', '?')
        otype = r.get('outreach_type', '?')
        date = r.get('outreach_date', '?')[:10] if r.get('outreach_date') else '?'
        outcome = r.get('outcome', '')
        st.caption(f"({date}) **{company}** — {otype}"
                  + (f" → {outcome}" if outcome else ""))
else:
    st.caption("No recent activity.")

st.markdown("---")
st.caption(f"Mobile View — {datetime.now().strftime('%H:%M')}")
