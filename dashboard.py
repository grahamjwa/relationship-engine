#!/usr/bin/env python3
"""
Relationship Engine — Streamlit Dashboard
Run: python3 -m streamlit run dashboard.py
"""

import sqlite3
import os
import pandas as pd
import streamlit as st

DB_PATH = os.path.expanduser("~/relationship_engine/data/relationship_engine.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def query_df(sql):
    conn = get_db()
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df

# ── Page Config ──
st.set_page_config(page_title="Relationship Engine", page_icon="🏢", layout="wide")

# ── Sidebar ──
st.sidebar.title("🏢 Relationship Engine")
page = st.sidebar.radio("Navigate", [
    "📊 Dashboard",
    "🔗 Intro Paths",
    "🎯 Untouched Targets",
    "📅 Lease Expirations",
    "💰 Recent Funding",
    "👥 Hiring Signals",
    "📞 Outreach Effectiveness",
    "⏰ Overdue Follow-ups",
    "🏗️ All Companies",
    "👤 All Contacts",
    "🏢 All Buildings",
])

# ── Dashboard ──
if page == "📊 Dashboard":
    st.title("📊 Dashboard")

    conn = get_db()
    tables = ['companies', 'contacts', 'buildings', 'leases', 'deals',
              'relationships', 'outreach_log', 'funding_events', 'hiring_signals']

    cols = st.columns(3)
    for i, t in enumerate(tables):
        count = conn.execute(f"SELECT COUNT(*) as c FROM {t}").fetchone()['c']
        cols[i % 3].metric(t.replace('_', ' ').title(), count)
    conn.close()

    st.divider()
    st.subheader("Quick Views")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**🎯 Untouched Targets**")
        df = query_df("SELECT * FROM v_untouched_targets")
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.success("All targets have been contacted!")

    with col2:
        st.markdown("**⏰ Overdue Follow-ups**")
        df = query_df("SELECT * FROM v_overdue_followups")
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.success("No overdue follow-ups!")

# ── Intro Paths ──
elif page == "🔗 Intro Paths":
    st.title("🔗 Intro Paths to Decision Makers")
    st.caption("How to reach C-suite and decision makers through your network")
    df = query_df("SELECT * FROM v_intro_paths")
    if not df.empty:
        # Filter by target company
        companies = ["All"] + sorted(df['target_company'].unique().tolist())
        selected = st.selectbox("Filter by target company", companies)
        if selected != "All":
            df = df[df['target_company'] == selected]
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No intro paths found. Add more relationships to build the graph.")

# ── Untouched Targets ──
elif page == "🎯 Untouched Targets":
    st.title("🎯 Untouched Targets")
    st.caption("High-value companies with no outreach logged")
    df = query_df("SELECT * FROM v_untouched_targets")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.success("All targets have been contacted!")

# ── Lease Expirations ──
elif page == "📅 Lease Expirations":
    st.title("📅 Upcoming Lease Expirations")
    st.caption("Leases expiring in the next 24 months")
    df = query_df("SELECT * FROM v_upcoming_expirations")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No lease expirations in the next 24 months. Add lease data to track expirations.")

# ── Recent Funding ──
elif page == "💰 Recent Funding":
    st.title("💰 Recent Funding Events")
    st.caption("Companies that raised money in the last 6 months — potential space needs")
    df = query_df("SELECT * FROM v_recent_funding")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No recent funding events. Add funding data or run the scraper.")

# ── Hiring Signals ──
elif page == "👥 Hiring Signals":
    st.title("👥 High-Value Hiring Signals")
    st.caption("Companies hiring RE decision makers or expanding in NYC")
    df = query_df("SELECT * FROM v_high_value_hiring")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No high-value hiring signals. Add hiring data or run the scraper.")

# ── Outreach Effectiveness ──
elif page == "📞 Outreach Effectiveness":
    st.title("📞 Outreach Effectiveness")
    st.caption("Which outreach methods actually work?")
    df = query_df("SELECT * FROM v_outreach_effectiveness")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No outreach data yet. Log outreach to start tracking effectiveness.")

# ── Overdue Follow-ups ──
elif page == "⏰ Overdue Follow-ups":
    st.title("⏰ Overdue Follow-ups")
    st.caption("Promised follow-ups that haven't been completed")
    df = query_df("SELECT * FROM v_overdue_followups")
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.success("No overdue follow-ups!")

# ── All Companies ──
elif page == "🏗️ All Companies":
    st.title("🏗️ All Companies")
    df = query_df("SELECT id, name, type, status, sector, hq_city, employee_count FROM companies ORDER BY name")
    if not df.empty:
        status_filter = st.multiselect("Filter by status", df['status'].unique().tolist(), default=df['status'].unique().tolist())
        filtered = df[df['status'].isin(status_filter)]
        st.dataframe(filtered, use_container_width=True, hide_index=True)

# ── All Contacts ──
elif page == "👤 All Contacts":
    st.title("👤 All Contacts")
    df = query_df(
        "SELECT c.id, c.first_name, c.last_name, c.title, c.role_level, "
        "co.name as company, c.alma_mater, c.email "
        "FROM contacts c LEFT JOIN companies co ON c.company_id = co.id "
        "ORDER BY c.last_name"
    )
    if not df.empty:
        role_filter = st.multiselect("Filter by role", df['role_level'].unique().tolist(), default=df['role_level'].unique().tolist())
        filtered = df[df['role_level'].isin(role_filter)]
        st.dataframe(filtered, use_container_width=True, hide_index=True)

# ── All Buildings ──
elif page == "🏢 All Buildings":
    st.title("🏢 All Buildings")
    df = query_df(
        "SELECT b.id, b.name, b.address, b.submarket, b.building_class, "
        "b.total_sf, co.name as owner, b.we_rep "
        "FROM buildings b LEFT JOIN companies co ON b.owner_company_id = co.id "
        "ORDER BY b.name"
    )
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)

# ── Footer ──
st.sidebar.divider()
st.sidebar.caption("Relationship Engine v1.0")
st.sidebar.caption(f"DB: {DB_PATH}")
