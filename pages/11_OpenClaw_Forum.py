"""
OpenClaw Forum — Review, approve, and respond to AI-generated suggestions.

OpenClaw proposes ideas, findings, trends, and tasks.
Graham approves, rejects, or responds.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="OpenClaw Forum", page_icon="🤖", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def get_suggestions(status='pending'):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM openclaw_suggestions
        WHERE status = ?
        ORDER BY
            CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
            created_at DESC
    """, (status,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def update_suggestion(suggestion_id, status, user_response=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE openclaw_suggestions
        SET status = ?, user_response = ?, responded_at = datetime('now')
        WHERE id = ?
    """, (status, user_response, suggestion_id))
    conn.commit()
    conn.close()


def add_suggestion(suggestion_type, title, description, source='analysis',
                   source_url=None, priority='medium'):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO openclaw_suggestions
        (suggestion_type, title, description, source, source_url, priority)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (suggestion_type, title, description, source, source_url, priority))
    conn.commit()
    conn.close()


# =============================================================================
# UI
# =============================================================================

st.title("OpenClaw Forum")
st.caption("AI-generated suggestions for review")

# KPIs
pending = get_suggestions('pending')
approved = get_suggestions('approved')
rejected = get_suggestions('rejected')
completed = get_suggestions('completed')

k1, k2, k3, k4 = st.columns(4)
k1.metric("Pending", len(pending))
k2.metric("Approved", len(approved))
k3.metric("Rejected", len(rejected))
k4.metric("Completed", len(completed))

st.markdown("---")

# =============================================================================
# PENDING SUGGESTIONS
# =============================================================================

st.subheader(f"Pending Suggestions ({len(pending)})")

if pending:
    for s in pending:
        type_icon = {
            'idea': '💡', 'task': '📋', 'finding': '🔍',
            'trend': '📈', 'improvement': '🔧'
        }.get(s.get('suggestion_type', ''), '📌')

        pri_badge = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(
            s.get('priority', ''), '')

        with st.container():
            cols = st.columns([4, 1])
            with cols[0]:
                st.markdown(f"{type_icon} {pri_badge} **{s['title']}**")
                if s.get('description'):
                    st.markdown(s['description'])
                meta = []
                if s.get('source'):
                    meta.append(f"Source: {s['source']}")
                if s.get('source_url'):
                    meta.append(f"[Link]({s['source_url']})")
                if s.get('created_at'):
                    meta.append(s['created_at'][:16])
                st.caption(" · ".join(meta))

            with cols[1]:
                bc1, bc2 = st.columns(2)
                with bc1:
                    if st.button("✅", key=f"approve_{s['id']}"):
                        update_suggestion(s['id'], 'approved')
                        st.rerun()
                with bc2:
                    if st.button("❌", key=f"reject_{s['id']}"):
                        update_suggestion(s['id'], 'rejected')
                        st.rerun()

            # Response input
            resp = st.text_input("Response (optional)", key=f"resp_{s['id']}")
            if resp and st.button("💬 Respond", key=f"respond_{s['id']}"):
                update_suggestion(s['id'], 'approved', resp)
                st.rerun()

        st.markdown("---")
else:
    st.info("No pending suggestions. OpenClaw will propose ideas based on scanning.")

# =============================================================================
# HISTORY
# =============================================================================

st.subheader("History")

history_tab = st.radio("Filter", ["Approved", "Rejected", "Completed"], horizontal=True)
history_map = {"Approved": approved, "Rejected": rejected, "Completed": completed}
history_items = history_map[history_tab]

if history_items:
    for s in history_items[:20]:
        type_icon = {
            'idea': '💡', 'task': '📋', 'finding': '🔍',
            'trend': '📈', 'improvement': '🔧'
        }.get(s.get('suggestion_type', ''), '📌')

        st.markdown(f"{type_icon} **{s['title']}** — *{s['status']}*")
        if s.get('user_response'):
            st.caption(f"Response: {s['user_response']}")
        if s.get('responded_at'):
            st.caption(f"Responded: {s['responded_at'][:16]}")
else:
    st.caption(f"No {history_tab.lower()} suggestions yet.")

# =============================================================================
# MANUAL ADD (for testing)
# =============================================================================

with st.sidebar:
    st.subheader("Add Suggestion")
    new_type = st.selectbox("Type", ['idea', 'task', 'finding', 'trend', 'improvement'])
    new_title = st.text_input("Title")
    new_desc = st.text_area("Description")
    new_pri = st.selectbox("Priority", ['medium', 'high', 'low'])

    if st.button("Add") and new_title:
        add_suggestion(new_type, new_title, new_desc, priority=new_pri)
        st.success("Added!")
        st.rerun()
