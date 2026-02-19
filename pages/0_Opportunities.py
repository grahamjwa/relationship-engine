"""
Find Me New Opportunities — Main Intelligence Page
=====================================================
The primary action page for the Relationship Engine.
Surfaces ranked opportunities with signals, reasons, and recommended actions.
Includes inline outreach logging, follow-up tracking, and outreach gap display.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path
from core.opportunity_engine import (find_new_opportunities, infer_opportunities,
                                     get_outreach_gaps)
from core.outreach_manager import (log_outreach, get_due_followups,
                                   mark_followup_done, get_followup_summary)
from core.signals import SIGNAL_TYPES

st.set_page_config(page_title="Opportunities", page_icon="🎯", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# UI
# =============================================================================

st.title("🎯 New Opportunities")
st.caption("Signal-driven CRE opportunities ranked by actionability")

# Controls
with st.sidebar:
    st.subheader("Settings")
    lookback_days = st.slider("Lookback (days)", 1, 90, 7)
    show_all = st.checkbox("Show all companies (not just recent signals)", False)
    min_score = st.slider("Minimum score", 0, 100, 10)
    max_results = st.slider("Max results", 5, 50, 10)

    st.markdown("---")
    st.subheader("Signal Legend")
    for key, sig in SIGNAL_TYPES.items():
        impact = sig['space_impact']
        arrow = "↑" if impact > 0 else "↓" if impact < 0 else "→"
        st.caption(f"{arrow} **{key}**: {sig['description']}")

# Refresh button
col_refresh, col_info = st.columns([1, 4])
with col_refresh:
    refresh = st.button("🔄 Refresh", type="primary")
with col_info:
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

st.markdown("---")

# =============================================================================
# FOLLOW-UPS DUE (top of page — most urgent)
# =============================================================================

followups = get_due_followups()
if followups:
    st.subheader(f"📋 Follow-ups Due ({len(followups)})")
    for fu in followups:
        fu_cols = st.columns([3, 1, 1, 1, 1])
        with fu_cols[0]:
            overdue_tag = f" (**{fu['days_overdue']}d overdue**)" if fu['days_overdue'] > 0 else ""
            st.markdown(f"**{fu['company_name']}** — {fu['outreach_type']} on "
                       f"{fu['outreach_date']}{overdue_tag}")
            if fu.get('notes'):
                st.caption(fu['notes'])
        with fu_cols[1]:
            st.caption(f"Due: {fu['follow_up_date']}")
        with fu_cols[2]:
            if st.button("✅ Done", key=f"fu_done_{fu['id']}"):
                mark_followup_done(fu['id'])
                st.rerun()
        with fu_cols[3]:
            if st.button("🔄 +7d", key=f"fu_reschedule_{fu['id']}"):
                mark_followup_done(fu['id'], reschedule_days=7)
                st.rerun()
        with fu_cols[4]:
            if st.button("🔄 +14d", key=f"fu_reschedule14_{fu['id']}"):
                mark_followup_done(fu['id'], reschedule_days=14)
                st.rerun()
    st.markdown("---")

# =============================================================================
# FETCH OPPORTUNITIES
# =============================================================================

if show_all:
    opportunities = infer_opportunities(min_score=min_score)
else:
    opportunities = find_new_opportunities(since_days=lookback_days)
    opportunities = [o for o in opportunities if o['score'] >= min_score]

# Summary KPIs
if opportunities:
    summary = get_followup_summary()
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Opportunities", len(opportunities))
    avg_score = sum(o['score'] for o in opportunities) / len(opportunities)
    k2.metric("Avg Score", f"{avg_score:.0f}")
    expanding = sum(1 for o in opportunities if o['space_impact'] > 0)
    k3.metric("Expanding", expanding)
    contracting = sum(1 for o in opportunities if o['space_impact'] < 0)
    k4.metric("Contracting", contracting)
    k5.metric("Follow-ups Due", summary['due'])
else:
    st.info(f"No opportunities found with score >= {min_score} "
            f"in the last {lookback_days} days. "
            f"Try widening the lookback or lowering the minimum score.")

st.markdown("---")

# =============================================================================
# OPPORTUNITY CARDS WITH INLINE OUTREACH LOGGING
# =============================================================================

for i, opp in enumerate(opportunities[:max_results], 1):
    impact_arrow = "↑" if opp['space_impact'] > 0 else "↓" if opp['space_impact'] < 0 else "→"
    score = opp['score']

    with st.container():
        # Header
        header_cols = st.columns([1, 3, 1, 1])

        with header_cols[0]:
            st.markdown(f"### #{i}")
            st.caption(f"Score: **{score}**")

        with header_cols[1]:
            st.subheader(opp['company'])
            status_display = (opp.get('status') or '').replace('_', ' ').title()
            sector_display = opp.get('sector', '')
            hq_display = opp.get('hq', '')
            meta_parts = [p for p in [status_display, sector_display, hq_display] if p]
            st.caption(" · ".join(meta_parts))

        with header_cols[2]:
            st.markdown(f"**Space:** {impact_arrow}")
            st.caption(f"Impact: {opp['space_impact']}")

        with header_cols[3]:
            confidence_pct = f"{opp['confidence']:.0%}"
            st.markdown(f"**Conf:** {confidence_pct}")
            if opp.get('lean_score') is not None:
                st.caption(f"Lean: {opp['lean_score']}")
            if opp.get('adjacency_score') is not None:
                st.caption(f"Adj: {opp['adjacency_score']}")

        # Why section
        st.markdown(f"**Why:** {opp['reason']}")

        # Signals
        signals_str = ", ".join(opp['signals'][:5])
        if len(opp['signals']) > 5:
            signals_str += f" (+{len(opp['signals'])-5} more)"
        st.markdown(f"**Signals:** {signals_str}")

        # Signal details with why-it-matters
        if opp.get('signal_details'):
            with st.expander("Signal Details"):
                for sig in opp['signal_details'][:8]:
                    impact = "↑" if sig['space_impact'] > 0 else "↓" if sig['space_impact'] < 0 else "→"
                    st.markdown(f"- [{sig['signal_type']}] {sig.get('detail', '')} "
                               f"({impact} {abs(sig['space_impact']):.0%} impact)")
                    st.caption(f"  Why: {sig['why_it_matters']}")

        # Recommended action
        st.success(f"**Action:** {opp['recommended_action']}")

        # Inline outreach logging
        with st.expander(f"Log Outreach — {opp['company']}"):
            oc1, oc2, oc3 = st.columns(3)
            with oc1:
                o_type = st.selectbox(
                    "Type", ["email", "call", "linkedin", "meeting", "text"],
                    key=f"otype_{opp['company_id']}"
                )
            with oc2:
                o_outcome = st.selectbox(
                    "Outcome",
                    ["sent", "connected", "voicemail", "no_answer",
                     "bounced", "meeting_set", "replied"],
                    key=f"ooutcome_{opp['company_id']}"
                )
            with oc3:
                o_followup = st.number_input(
                    "Follow-up (days)", min_value=0, value=7,
                    key=f"ofu_{opp['company_id']}"
                )
            o_notes = st.text_input("Notes", key=f"onotes_{opp['company_id']}")
            o_angle = st.text_input("Angle", key=f"oangle_{opp['company_id']}")

            if st.button(f"Log Touch", key=f"olog_{opp['company_id']}"):
                log_outreach(
                    company_id=opp['company_id'],
                    outreach_type=o_type,
                    outcome=o_outcome,
                    notes=o_notes,
                    angle=o_angle,
                    follow_up_days=o_followup,
                )
                st.success(f"Logged {o_type} to {opp['company']}. "
                          f"Follow-up in {o_followup} days.")

    st.divider()

# =============================================================================
# OUTREACH GAPS
# =============================================================================

st.subheader("🕳️ Outreach Gaps")
st.caption("High-value targets with no recent outreach (30+ days)")

gaps = get_outreach_gaps()
if gaps:
    for g in gaps[:15]:
        g_cols = st.columns([3, 1, 1, 1])
        with g_cols[0]:
            st.markdown(f"**{g['company']}** — {g['status'].replace('_', ' ').title()}")
        with g_cols[1]:
            st.caption(f"Lean: {g['lean_score']}")
        with g_cols[2]:
            if g['last_outreach']:
                st.caption(f"Last: {g['last_outreach']} ({g['days_since_outreach']}d ago)")
            else:
                st.caption("**Never contacted**")
        with g_cols[3]:
            st.caption(g['sector'])
else:
    st.info("No outreach gaps detected. All high-value targets have recent touches.")

# =============================================================================
# FOOTER
# =============================================================================

if opportunities and len(opportunities) > max_results:
    st.caption(f"Showing {max_results} of {len(opportunities)} opportunities. "
              f"Increase max results in sidebar to see more.")

st.markdown("---")
st.caption("Powered by Relationship Engine — Lean v1 Signal Intelligence")
