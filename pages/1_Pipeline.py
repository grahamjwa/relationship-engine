"""
Pipeline / Funnel View
Tracks companies through BD stages: Target → Outreach → Meeting → Deal
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from graph_engine import get_db_path

st.set_page_config(page_title="Pipeline", page_icon="📊", layout="wide")

def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# PIPELINE STAGE LOGIC
# =============================================================================

def get_pipeline_data():
    """
    Classify companies into pipeline stages based on outreach history and deal status.

    Stages:
        Target      — No outreach ever
        Outreach    — Outreach logged but no meeting booked
        Meeting     — Meeting booked or held
        Proposal    — Deal in negotiation
        Active Deal — Deal in progress
        Closed      — Deal closed
    """
    conn = get_conn()
    cur = conn.cursor()

    # All target/prospect companies with their latest outreach and deal status
    cur.execute("""
        SELECT
            c.id,
            c.name,
            c.status,
            c.industry,
            COALESCE(c.opportunity_score, 0) as opp_score,
            latest_outreach.outreach_date as last_outreach_date,
            latest_outreach.outcome as last_outcome,
            latest_outreach.outreach_type as last_type,
            active_deal.deal_type,
            active_deal.deal_status,
            active_deal.square_feet as deal_sf
        FROM companies c
        LEFT JOIN (
            SELECT target_company_id,
                   outreach_date, outcome, outreach_type,
                   ROW_NUMBER() OVER (PARTITION BY target_company_id ORDER BY outreach_date DESC) as rn
            FROM outreach_log
        ) latest_outreach ON c.id = latest_outreach.target_company_id AND latest_outreach.rn = 1
        LEFT JOIN (
            SELECT company_id, deal_type, status as deal_status, square_feet,
                   ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY COALESCE(started_date, created_at) DESC) as rn
            FROM deals
            WHERE status NOT IN ('lost', 'dead')
        ) active_deal ON c.id = active_deal.company_id AND active_deal.rn = 1
        WHERE c.status IN ('high_growth_target', 'prospect', 'active_client', 'watching')
        ORDER BY c.opportunity_score DESC
    """)

    stages = {
        "Target": [],
        "Outreach": [],
        "Meeting": [],
        "Proposal": [],
        "Active Deal": [],
        "Closed": [],
    }

    for row in cur.fetchall():
        company = dict(row)

        # Classify into stage
        if company["deal_status"] in ("closed", "won"):
            stage = "Closed"
        elif company["deal_status"] in ("in_progress", "active", "executing"):
            stage = "Active Deal"
        elif company["deal_status"] in ("proposal", "negotiating", "pitching"):
            stage = "Proposal"
        elif company["last_outcome"] in ("meeting_booked", "meeting_held"):
            stage = "Meeting"
        elif company["last_outreach_date"] is not None:
            stage = "Outreach"
        else:
            stage = "Target"

        stages[stage].append(company)

    conn.close()
    return stages


# =============================================================================
# UI
# =============================================================================

st.title("Pipeline")

# Filters
with st.expander("Filters", expanded=False):
    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        min_score = st.slider("Min Opportunity Score", 0, 100, 0)
    with filter_col2:
        industry_filter = st.text_input("Industry filter (partial match)", "")

pipeline = get_pipeline_data()

# Stage summary bar
st.markdown("---")
cols = st.columns(len(pipeline))
stage_colors = {
    "Target": "🔵",
    "Outreach": "🟡",
    "Meeting": "🟠",
    "Proposal": "🔴",
    "Active Deal": "🟢",
    "Closed": "✅",
}

for col, (stage_name, companies) in zip(cols, pipeline.items()):
    # Apply filters
    filtered = [
        c for c in companies
        if c["opp_score"] >= min_score
        and (not industry_filter or (c["industry"] and industry_filter.lower() in c["industry"].lower()))
    ]
    icon = stage_colors.get(stage_name, "⬜")
    col.metric(f"{icon} {stage_name}", len(filtered))

st.markdown("---")

# Detailed stage columns
cols = st.columns(len(pipeline))

for col, (stage_name, companies) in zip(cols, pipeline.items()):
    filtered = [
        c for c in companies
        if c["opp_score"] >= min_score
        and (not industry_filter or (c["industry"] and industry_filter.lower() in c["industry"].lower()))
    ]

    with col:
        st.markdown(f"**{stage_colors.get(stage_name, '')} {stage_name}**")
        st.markdown(f"*{len(filtered)} companies*")
        st.markdown("---")

        for company in filtered[:20]:  # Cap at 20 per column
            score_bar = "█" * int(company["opp_score"] / 10)
            with st.container():
                st.markdown(f"**{company['name']}**")
                detail_parts = []
                if company["opp_score"] > 0:
                    detail_parts.append(f"Score: {company['opp_score']:.0f}")
                if company["industry"]:
                    detail_parts.append(company["industry"])
                if company["last_outreach_date"]:
                    detail_parts.append(f"Last: {company['last_outreach_date']}")
                if company["deal_sf"]:
                    detail_parts.append(f"{company['deal_sf']:,.0f} SF")
                st.caption(" · ".join(detail_parts) if detail_parts else company["status"])
                st.markdown("---")

        if len(filtered) > 20:
            st.caption(f"+ {len(filtered) - 20} more")

# Move company between stages
st.markdown("---")
st.subheader("Quick Actions")

action_col1, action_col2, action_col3 = st.columns(3)

conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT id, name FROM companies WHERE status IN ('high_growth_target', 'prospect', 'active_client', 'watching') ORDER BY name")
all_companies = cur.fetchall()
company_options = {r["name"]: r["id"] for r in all_companies}
conn.close()

with action_col1:
    st.markdown("**Log Outreach**")
    selected_company = st.selectbox("Company", list(company_options.keys()), key="outreach_company")
    outreach_type = st.selectbox("Type", ["email", "call", "meeting", "linkedin", "event"], key="outreach_type")
    outcome = st.selectbox("Outcome", ["pending", "no_response", "responded_positive", "responded_negative", "meeting_booked", "meeting_held", "referred", "declined"], key="outreach_outcome")
    notes = st.text_input("Notes", key="outreach_notes")

    if st.button("Log Outreach"):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO outreach_log (target_company_id, outreach_date, outreach_type, outcome, notes)
            VALUES (?, date('now'), ?, ?, ?)
        """, (company_options[selected_company], outreach_type, outcome, notes))
        conn.commit()
        conn.close()
        st.success(f"Logged {outreach_type} to {selected_company}")
        st.rerun()

with action_col2:
    st.markdown("**Update Company Status**")
    status_company = st.selectbox("Company", list(company_options.keys()), key="status_company")
    new_status = st.selectbox("New Status", [
        "high_growth_target", "prospect", "active_client", "watching",
        "former_client", "network_portfolio"
    ], key="new_status")

    if st.button("Update Status"):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE companies SET status = ? WHERE id = ?", (new_status, company_options[status_company]))
        conn.commit()
        conn.close()
        st.success(f"Updated {status_company} to {new_status}")
        st.rerun()

with action_col3:
    st.markdown("**Pipeline Stats**")
    total_in_pipeline = sum(len(v) for v in pipeline.values())
    st.metric("Total in Pipeline", total_in_pipeline)

    # Conversion funnel
    target_count = len(pipeline.get("Target", []))
    outreach_count = len(pipeline.get("Outreach", []))
    meeting_count = len(pipeline.get("Meeting", []))
    deal_count = len(pipeline.get("Active Deal", [])) + len(pipeline.get("Closed", []))

    if target_count + outreach_count > 0:
        outreach_rate = outreach_count / (target_count + outreach_count) * 100
        st.metric("Outreach Rate", f"{outreach_rate:.0f}%")
    if outreach_count > 0:
        meeting_rate = meeting_count / outreach_count * 100
        st.metric("Meeting Rate", f"{meeting_rate:.0f}%")
