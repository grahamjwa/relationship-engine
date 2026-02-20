"""
Mobile View — Simplified single-column layout for phone access.

Single column, large buttons, no charts.
Sections:
  1. ACTION NEEDED (overdue follow-ups)
  2. TOP 5 (Company - Score - One Signal)
  3. TODAY (last 24h signals, max 5)
  4. QUICK LOG [Call] [Meeting] [Note] [Rumor]
"""

import os
import sys
import re
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
# 1. ACTION NEEDED
# =============================================================================

conn = get_conn()
cur = conn.cursor()

cur.execute("""
    SELECT o.id, o.follow_up_date, c.name as company, o.outreach_type, o.notes
    FROM outreach_log o
    LEFT JOIN companies c ON o.target_company_id = c.id
    WHERE o.follow_up_done = 0 AND o.follow_up_date IS NOT NULL
    AND o.follow_up_date <= ?
    ORDER BY o.follow_up_date ASC LIMIT 10
""", (today_str,))
followups = [dict(r) for r in cur.fetchall()]
conn.close()

st.markdown(f"### 🔴 Action Needed ({len(followups)})")

if followups:
    for fu in followups:
        is_overdue = fu['follow_up_date'] < today_str
        prefix = "🔴" if is_overdue else "🔵"
        company = fu.get('company', '?')

        cols = st.columns([5, 1])
        with cols[0]:
            st.markdown(f"{prefix} **{company}** — {fu.get('outreach_type', '?')}")
        with cols[1]:
            if st.button("Done", key=f"mob_{fu['id']}"):
                c = get_conn()
                c.execute("UPDATE outreach_log SET follow_up_done = 1 WHERE id = ?",
                         (fu['id'],))
                c.commit()
                c.close()
                st.rerun()
else:
    st.caption("Clear.")

st.markdown("---")

# =============================================================================
# 2. TOP 5
# =============================================================================

st.markdown("### 📊 Top 5")

conn = get_conn()
cur = conn.cursor()

cur.execute("""
    SELECT c.id, c.name, c.opportunity_score,
           (SELECT h.description FROM hiring_signals h
            WHERE h.company_id = c.id ORDER BY h.signal_date DESC LIMIT 1) as last_hiring,
           (SELECT f.round_type || ' $' || CAST(f.amount AS TEXT)
            FROM funding_events f WHERE f.company_id = c.id
            ORDER BY f.event_date DESC LIMIT 1) as last_funding,
           (SELECT l.lease_expiry FROM leases l
            WHERE l.company_id = c.id AND l.lease_expiry >= date('now')
            ORDER BY l.lease_expiry ASC LIMIT 1) as next_expiry
    FROM companies c
    WHERE c.opportunity_score IS NOT NULL
    AND c.status NOT IN ('team_affiliated')
    ORDER BY c.opportunity_score DESC
    LIMIT 5
""")
top5 = [dict(r) for r in cur.fetchall()]
conn.close()

for co in top5:
    score = co.get('opportunity_score', 0) or 0
    # Pick best signal to show
    signal = ""
    if co.get('last_funding'):
        signal = co['last_funding'][:40]
    elif co.get('last_hiring'):
        signal = co['last_hiring'][:40]
    elif co.get('next_expiry'):
        signal = f"Lease exp {co['next_expiry']}"

    st.markdown(f"**{co['name']}** ({score:.0f}) — {signal}")

st.markdown("---")

# =============================================================================
# 3. TODAY (last 24h signals)
# =============================================================================

st.markdown("### 📰 Today")

conn = get_conn()
cur = conn.cursor()
signals_24h = []

# Funding
try:
    cur.execute("""
        SELECT c.name, f.amount, f.round_type, f.event_date, f.lead_investor
        FROM funding_events f
        JOIN companies c ON f.company_id = c.id
        WHERE f.event_date >= date('now', '-1 day')
        ORDER BY f.amount DESC LIMIT 3
    """)
    for r in cur.fetchall():
        r = dict(r)
        amt = f"${r['amount']:,.0f}" if r.get('amount') else "?"

        # Check if lead investor is in our DB
        investors = ""
        if r.get('lead_investor'):
            cur2 = conn.cursor()
            cur2.execute("SELECT name FROM companies WHERE name LIKE ?",
                        (f"%{r['lead_investor'].split(',')[0].strip()}%",))
            known = cur2.fetchone()
            if known:
                investors = f"[{known['name']}]"
                if ',' in (r.get('lead_investor') or ''):
                    others = r['lead_investor'].split(',')[1:]
                    investors += ', ' + ', '.join(o.strip() for o in others[:2])
            else:
                investors = r.get('lead_investor', '')[:40]

        date_short = r['event_date'][5:10] if r.get('event_date') else '?'
        signals_24h.append(
            f"💰 ({date_short}) {r['name']} received {amt} "
            f"({r.get('round_type', '?')})"
            + (f" from {investors}" if investors else ""))
except Exception:
    pass

# Exec changes
try:
    cur.execute("""
        SELECT person_name, new_title, new_company, effective_date
        FROM executive_changes
        WHERE effective_date >= date('now', '-1 day')
        AND priority IN ('high', 'medium')
        ORDER BY effective_date DESC LIMIT 2
    """)
    for r in cur.fetchall():
        r = dict(r)
        signals_24h.append(f"👔 {r['person_name']} → {r.get('new_title', '?')} at {r.get('new_company', '?')}")
except Exception:
    pass

conn.close()

if signals_24h:
    for s in signals_24h[:5]:
        st.markdown(s)
else:
    st.caption("No signals today.")

st.markdown("---")

# =============================================================================
# 4. QUICK LOG
# =============================================================================

st.markdown("### ⚡ Quick Log")

log_type = st.radio("", ["Call", "Meeting", "Note", "Rumor"], horizontal=True, key="mob_type")

log_company = st.text_input("Company", key="mob_co")
log_notes = st.text_input("Details", key="mob_notes")

if st.button("Log It", key="mob_log", use_container_width=True) and log_company:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM companies WHERE name LIKE ?", (f"%{log_company}%",))
    company_row = cur.fetchone()
    cid = company_row['id'] if company_row else None

    if log_type in ("Call", "Meeting"):
        otype = 'call' if log_type == 'Call' else 'in_person'
        cur.execute("""
            INSERT INTO outreach_log
            (target_company_id, outreach_date, outreach_type, outcome, notes,
             follow_up_date, follow_up_done)
            VALUES (?, date('now'), ?, 'pending', ?, date('now', '+7 days'), 0)
        """, (cid, otype, log_notes or f"{log_type} with {log_company}"))
        conn.commit()
        st.success("OK")

    elif log_type == "Note":
        cur.execute("""
            INSERT INTO outreach_log
            (target_company_id, outreach_date, outreach_type, outcome, notes,
             follow_up_done)
            VALUES (?, date('now'), 'note', 'logged', ?, 1)
        """, (cid, log_notes or f"Note: {log_company}"))
        conn.commit()
        st.success("OK")

    elif log_type == "Rumor":
        sf = 0
        sf_match = re.search(r'(\d[\d,]*)\s*(?:sf|SF|rsf|RSF)', log_notes or '')
        if sf_match:
            sf = int(sf_match.group(1).replace(',', ''))
        cur.execute("""
            INSERT INTO market_requirements (company_id, sf_min, notes, source, status)
            VALUES (?, ?, ?, 'mobile', 'active')
        """, (cid, sf, log_notes or f"{log_company} in market"))
        conn.commit()
        st.success("OK")

    conn.close()

st.markdown("---")
st.caption(f"{datetime.now().strftime('%H:%M')}")
