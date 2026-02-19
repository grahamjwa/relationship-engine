"""
Deal Pipeline — Kanban-style view of active deals by stage.

Stages: Lead → Meeting Set → Touring → Proposal → Negotiation → LOI → Lease → Closed
Lost deals shown separately.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="Deal Pipeline", page_icon="🎯", layout="wide")

STAGES = ['lead', 'meeting_set', 'touring', 'proposal', 'negotiation', 'loi', 'lease', 'closed']
STAGE_LABELS = {
    'lead': 'Lead', 'meeting_set': 'Meeting Set', 'touring': 'Touring',
    'proposal': 'Proposal', 'negotiation': 'Negotiation', 'loi': 'LOI',
    'lease': 'Lease', 'closed': 'Closed'
}
STAGE_COLORS = {
    'lead': '⚪', 'meeting_set': '🔵', 'touring': '🟣',
    'proposal': '🟡', 'negotiation': '🟠', 'loi': '🔴',
    'lease': '🟢', 'closed': '✅'
}


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def get_deals_by_stage():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *,
            CAST(julianday('now') - julianday(stage_entered_at) AS INTEGER) as calc_days_in_stage
        FROM deal_stages
        WHERE stage != 'lost'
        ORDER BY
            CASE stage
                WHEN 'lead' THEN 1 WHEN 'meeting_set' THEN 2 WHEN 'touring' THEN 3
                WHEN 'proposal' THEN 4 WHEN 'negotiation' THEN 5 WHEN 'loi' THEN 6
                WHEN 'lease' THEN 7 WHEN 'closed' THEN 8
            END,
            probability DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    by_stage = {s: [] for s in STAGES}
    for r in rows:
        stage = r.get('stage', 'lead')
        if stage in by_stage:
            by_stage[stage].append(r)
    return by_stage


def get_lost_deals():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM deal_stages WHERE stage = 'lost' ORDER BY stage_entered_at DESC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def update_deal_stage(deal_id, new_stage):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE deal_stages
        SET stage = ?, stage_entered_at = datetime('now'), days_in_stage = 0
        WHERE id = ?
    """, (new_stage, deal_id))
    conn.commit()
    conn.close()


def update_deal_field(deal_id, field, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE deal_stages SET {field} = ? WHERE id = ?", (value, deal_id))
    conn.commit()
    conn.close()


# =============================================================================
# UI
# =============================================================================

st.title("Deal Pipeline")

deals_by_stage = get_deals_by_stage()

# KPIs
all_active = [d for deals in deals_by_stage.values() for d in deals if d['stage'] != 'closed']
total_pipeline = sum(d.get('estimated_value', 0) or 0 for d in all_active)
weighted_pipeline = sum((d.get('estimated_value', 0) or 0) * (d.get('probability', 0) or 0) / 100
                       for d in all_active)
total_deals = len(all_active)

# Avg days across stages
all_days = [d.get('calc_days_in_stage', 0) or 0 for d in all_active]
avg_days = sum(all_days) / len(all_days) if all_days else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Active Deals", total_deals)
k2.metric("Pipeline Value", f"${total_pipeline:,.0f}")
k3.metric("Weighted Value", f"${weighted_pipeline:,.0f}")
k4.metric("Avg Days in Stage", f"{avg_days:.0f}")

st.markdown("---")

# Kanban View
st.subheader("Pipeline")

# Show in rows of 4 columns
for row_start in range(0, len(STAGES), 4):
    row_stages = STAGES[row_start:row_start + 4]
    cols = st.columns(len(row_stages))

    for i, stage in enumerate(row_stages):
        with cols[i]:
            stage_deals = deals_by_stage.get(stage, [])
            stage_value = sum(d.get('estimated_value', 0) or 0 for d in stage_deals)
            icon = STAGE_COLORS.get(stage, '')

            st.markdown(f"### {icon} {STAGE_LABELS[stage]}")
            st.caption(f"{len(stage_deals)} deals · ${stage_value:,.0f}")

            for d in stage_deals:
                days = d.get('calc_days_in_stage', 0) or 0
                sf_str = f"{d['square_feet']:,} SF" if d.get('square_feet') else ""
                val_str = f"${d['estimated_value']:,.0f}" if d.get('estimated_value') else ""

                with st.container():
                    st.markdown(f"**{d['company_name']}**")
                    if d.get('building_address'):
                        st.caption(d['building_address'])
                    meta_parts = []
                    if sf_str:
                        meta_parts.append(sf_str)
                    if val_str:
                        meta_parts.append(val_str)
                    meta_parts.append(f"{d.get('probability', 0)}%")
                    meta_parts.append(f"{days}d in stage")
                    st.caption(" · ".join(meta_parts))

                    if d.get('next_action'):
                        is_overdue = (d.get('next_action_date', '') or '') < datetime.now().strftime('%Y-%m-%d')
                        prefix = "🔴 " if is_overdue else "→ "
                        st.caption(f"{prefix}{d['next_action']}")
                        if d.get('next_action_date'):
                            st.caption(f"  Due: {d['next_action_date']}")

                st.markdown("---")

st.markdown("---")

# Deal Editor
st.subheader("Update Deal")

conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT id, company_name, stage FROM deal_stages ORDER BY company_name")
all_deals_list = [dict(r) for r in cur.fetchall()]
conn.close()

if all_deals_list:
    deal_options = {f"{d['company_name']} ({d['stage']})": d['id'] for d in all_deals_list}
    selected_deal_label = st.selectbox("Select Deal", list(deal_options.keys()))
    selected_deal_id = deal_options[selected_deal_label]

    ed_cols = st.columns([1, 1, 1, 1])
    with ed_cols[0]:
        new_stage = st.selectbox("Move to Stage",
                                 STAGES + ['lost'],
                                 key="ed_stage")
        if st.button("Update Stage", key="ed_stage_btn"):
            update_deal_stage(selected_deal_id, new_stage)
            st.success(f"Moved to {new_stage}")
            st.rerun()
    with ed_cols[1]:
        new_action = st.text_input("Next Action", key="ed_action")
        if new_action and st.button("Update Action", key="ed_action_btn"):
            update_deal_field(selected_deal_id, 'next_action', new_action)
            st.success("Updated")
            st.rerun()
    with ed_cols[2]:
        new_date = st.date_input("Next Action Date", key="ed_date")
        if st.button("Update Date", key="ed_date_btn"):
            update_deal_field(selected_deal_id, 'next_action_date',
                            new_date.strftime('%Y-%m-%d'))
            st.success("Updated")
            st.rerun()
    with ed_cols[3]:
        new_notes = st.text_area("Notes", key="ed_notes")
        if new_notes and st.button("Add Notes", key="ed_notes_btn"):
            update_deal_field(selected_deal_id, 'notes', new_notes)
            st.success("Updated")
            st.rerun()

# Add New Deal
with st.expander("➕ Add New Deal"):
    nc = st.columns([2, 2, 1, 1])
    with nc[0]:
        nd_company = st.text_input("Company", key="nd_co")
    with nc[1]:
        nd_bldg = st.text_input("Building", key="nd_bldg")
    with nc[2]:
        nd_sf = st.number_input("SF", min_value=0, step=1000, key="nd_sf")
    with nc[3]:
        nd_stage = st.selectbox("Stage", STAGES, key="nd_stage")

    nc2 = st.columns([1, 1, 2])
    with nc2[0]:
        nd_prob = st.number_input("Probability %", 0, 100, 10, key="nd_prob")
    with nc2[1]:
        nd_val = st.number_input("Est. Value $", min_value=0, step=10000, key="nd_val")
    with nc2[2]:
        nd_action = st.text_input("Next Action", key="nd_next")

    if st.button("Add Deal", key="nd_add") and nd_company:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO deal_stages
            (company_name, building_address, square_feet, stage, probability,
             estimated_value, assigned_to, next_action)
            VALUES (?, ?, ?, ?, ?, ?, 'Graham', ?)
        """, (nd_company, nd_bldg or None, nd_sf or None, nd_stage,
              nd_prob, nd_val or None, nd_action or None))
        conn.commit()
        conn.close()
        st.success(f"Added: {nd_company}")
        st.rerun()

# Lost deals
st.markdown("---")
lost = get_lost_deals()
if lost:
    with st.expander(f"Lost Deals ({len(lost)})"):
        for d in lost:
            st.caption(f"❌ **{d['company_name']}** — {d.get('building_address', '?')} — "
                      f"{d.get('notes', 'No notes')[:80]}")
