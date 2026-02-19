"""
In-Market Tracker — Consolidated view of all groups rumored to be in market.

Uses market_requirements table. Shows matches to agency availabilities.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="In-Market Tracker", page_icon="🏷️", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def get_requirements(sf_min=None, sf_max=None, submarket=None, status=None):
    conn = get_conn()
    cur = conn.cursor()

    conditions = ["1=1"]
    params = []

    if sf_min:
        conditions.append("sf_min >= ?")
        params.append(sf_min)
    if sf_max:
        conditions.append("COALESCE(sf_max, sf_min) <= ?")
        params.append(sf_max)
    if submarket and submarket != "All":
        conditions.append("submarket LIKE ?")
        params.append(f"%{submarket}%")
    if status and status != "All":
        conditions.append("status = ?")
        params.append(status)

    where = " AND ".join(conditions)
    cur.execute(f"""
        SELECT mr.*, c.name as company_name, c.status as company_status
        FROM market_requirements mr
        LEFT JOIN companies c ON mr.company_id = c.id
        WHERE {where}
        ORDER BY mr.created_at DESC
    """, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_agency_matches(sf_need, submarket=None, tolerance=0.2):
    """Find agency availabilities matching a requirement."""
    conn = get_conn()
    cur = conn.cursor()

    min_sf = int(sf_need * (1 - tolerance))
    max_sf = int(sf_need * (1 + tolerance))

    conditions = ["a.available_sf BETWEEN ? AND ?", "a.status = 'available'"]
    params = [min_sf, max_sf]

    if submarket:
        conditions.append("b.submarket LIKE ?")
        params.append(f"%{submarket}%")

    where = " AND ".join(conditions)
    try:
        cur.execute(f"""
            SELECT a.*, b.name as building_name, b.address, b.submarket
            FROM agency_availabilities a
            JOIN agency_buildings b ON a.building_id = b.id
            WHERE {where}
            ORDER BY a.available_sf ASC
            LIMIT 10
        """, params)
        rows = [dict(r) for r in cur.fetchall()]
    except Exception:
        rows = []
    conn.close()
    return rows


def add_requirement(company_name, sf_min, sf_max=None, submarket=None,
                    broker=None, source=None, confidence='medium', notes=None):
    conn = get_conn()
    cur = conn.cursor()

    # Check if company exists
    cur.execute("SELECT id FROM companies WHERE name LIKE ?", (f"%{company_name}%",))
    row = cur.fetchone()
    company_id = row['id'] if row else None

    cur.execute("""
        INSERT INTO market_requirements
        (company_id, sf_min, sf_max, submarket, broker, source, confidence, notes, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')
    """, (company_id, sf_min, sf_max, submarket, broker, source, confidence, notes))
    conn.commit()
    conn.close()


# =============================================================================
# UI
# =============================================================================

st.title("In-Market Tracker")
st.caption("Groups rumored or confirmed to be in the market for space")

conn = get_conn()
cur = conn.cursor()

# Get filter options
try:
    cur.execute("SELECT DISTINCT submarket FROM market_requirements WHERE submarket IS NOT NULL ORDER BY submarket")
    submarkets = ["All"] + [r['submarket'] for r in cur.fetchall()]
except Exception:
    submarkets = ["All"]

try:
    cur.execute("SELECT DISTINCT status FROM market_requirements WHERE status IS NOT NULL ORDER BY status")
    statuses = ["All"] + [r['status'] for r in cur.fetchall()]
except Exception:
    statuses = ["All", "active", "signed", "withdrawn", "lost"]

conn.close()

# Quick Add Form
with st.expander("➕ Add New Requirement", expanded=False):
    qa_cols = st.columns([2, 1, 1, 1, 1])
    with qa_cols[0]:
        qa_company = st.text_input("Company", key="qa_co")
    with qa_cols[1]:
        qa_sf_min = st.number_input("Min SF", min_value=0, step=1000, key="qa_sfmin")
    with qa_cols[2]:
        qa_sf_max = st.number_input("Max SF", min_value=0, step=1000, key="qa_sfmax")
    with qa_cols[3]:
        qa_submarket = st.text_input("Submarket", key="qa_sub")
    with qa_cols[4]:
        qa_broker = st.text_input("Broker", key="qa_broker")

    qa_cols2 = st.columns([1, 1, 2])
    with qa_cols2[0]:
        qa_source = st.text_input("Source", key="qa_src", placeholder="broker call, CoStar, etc.")
    with qa_cols2[1]:
        qa_conf = st.selectbox("Confidence", ['medium', 'high', 'low'], key="qa_conf")
    with qa_cols2[2]:
        qa_notes = st.text_input("Notes", key="qa_notes")

    if st.button("Add Requirement", key="qa_add") and qa_company and qa_sf_min:
        add_requirement(qa_company, qa_sf_min,
                       qa_sf_max if qa_sf_max > 0 else None,
                       qa_submarket or None, qa_broker or None,
                       qa_source or None, qa_conf, qa_notes or None)
        st.success(f"Added: {qa_company} seeking {qa_sf_min:,} SF")
        st.rerun()

st.markdown("---")

# Filters
f1, f2, f3, f4 = st.columns(4)
with f1:
    filt_sf_min = st.number_input("SF Min Filter", min_value=0, step=5000, value=0, key="f_sfmin")
with f2:
    filt_sf_max = st.number_input("SF Max Filter", min_value=0, step=5000, value=0, key="f_sfmax")
with f3:
    filt_sub = st.selectbox("Submarket", submarkets, key="f_sub")
with f4:
    filt_status = st.selectbox("Status", statuses, key="f_status")

# Get requirements
reqs = get_requirements(
    sf_min=filt_sf_min if filt_sf_min > 0 else None,
    sf_max=filt_sf_max if filt_sf_max > 0 else None,
    submarket=filt_sub,
    status=filt_status
)

# KPIs
k1, k2, k3 = st.columns(3)
k1.metric("Total Requirements", len(reqs))
total_sf = sum(r.get('sf_min', 0) for r in reqs)
k2.metric("Total SF Demand", f"{total_sf:,}")
high_conf = len([r for r in reqs if r.get('confidence') == 'high'])
k3.metric("High Confidence", high_conf)

st.markdown("---")

# Requirements Table
if reqs:
    for r in reqs:
        conf_badge = {'high': '🟢', 'medium': '🟡', 'low': '🔴'}.get(
            r.get('confidence', ''), '⚪')
        status_badge = {'active': '🔵', 'signed': '✅', 'withdrawn': '⚫', 'lost': '❌'}.get(
            r.get('status', ''), '⚪')

        company_display = r.get('company_name') or f"Company #{r.get('company_id', '?')}"
        sf_range = f"{r['sf_min']:,}" if r.get('sf_min') else "?"
        if r.get('sf_max') and r['sf_max'] != r.get('sf_min'):
            sf_range += f" - {r['sf_max']:,}"
        sf_range += " SF"

        with st.container():
            cols = st.columns([3, 1, 1, 1, 1])
            with cols[0]:
                st.markdown(f"{status_badge} {conf_badge} **{company_display}** — {sf_range}")
                meta = []
                if r.get('submarket'):
                    meta.append(r['submarket'])
                if r.get('broker'):
                    meta.append(f"Broker: {r['broker']}")
                if r.get('source'):
                    meta.append(f"Source: {r['source']}")
                if r.get('created_at'):
                    meta.append(f"First heard: {r['created_at'][:10]}")
                if meta:
                    st.caption(" · ".join(meta))
                if r.get('notes'):
                    st.caption(f"Notes: {r['notes'][:120]}")

            with cols[1]:
                if r.get('company_name'):
                    st.caption("In DB ✓")
                else:
                    st.caption("Not in DB")

            with cols[2]:
                st.caption(r.get('status', '?'))

            with cols[3]:
                st.caption(r.get('confidence', '?'))

            with cols[4]:
                pass

            # Agency matches
            sf_target = r.get('sf_min', 0)
            if sf_target > 0:
                matches = get_agency_matches(sf_target, r.get('submarket'))
                if matches:
                    with st.expander(f"📍 {len(matches)} Agency Matches", expanded=False):
                        for m in matches:
                            st.caption(
                                f"  {m.get('building_name', '?')} ({m.get('address', '?')}) — "
                                f"Floor {m.get('floor', '?')}, {m.get('available_sf', 0):,} SF, "
                                f"${m.get('asking_rent', 0)}/SF — {m.get('submarket', '?')}"
                            )

        st.markdown("---")
else:
    st.info("No requirements found. Add one above or adjust filters.")

# Sidebar stats
with st.sidebar:
    st.subheader("Demand by Submarket")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COALESCE(submarket, 'Unknown') as sub,
                   COUNT(*) as cnt,
                   SUM(sf_min) as total_sf
            FROM market_requirements
            WHERE status = 'active'
            GROUP BY submarket
            ORDER BY total_sf DESC
        """)
        for r in cur.fetchall():
            r_dict = dict(r)
            st.caption(f"**{r_dict['sub']}**: {r_dict['cnt']} reqs, {r_dict['total_sf']:,.0f} SF")
    except Exception:
        st.caption("No data yet.")
    conn.close()
