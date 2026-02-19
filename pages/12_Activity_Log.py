"""
Activity Log — Track all OpenClaw actions with undo capability.

Shows what OpenClaw did, when, and lets Graham reverse reversible actions.
"""

import os
import sys
import sqlite3
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="Activity Log", page_icon="📜", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def log_activity(action_type, description, target_table=None, target_id=None,
                 old_value=None, new_value=None, reversible=False, agent='openclaw',
                 db_path=None):
    """Log an activity. Called by other modules when they take actions."""
    conn = sqlite3.connect(db_path or get_db_path())
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO openclaw_activity
        (action_type, description, target_table, target_id, old_value, new_value,
         reversible, agent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (action_type, description, target_table, target_id,
          old_value, new_value, 1 if reversible else 0, agent))
    activity_id = cur.lastrowid
    conn.commit()
    conn.close()
    return activity_id


def undo_activity(activity_id, db_path=None):
    """Attempt to undo a reversible activity."""
    conn = sqlite3.connect(db_path or get_db_path())
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM openclaw_activity WHERE id = ?", (activity_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False, "Activity not found."

    activity = dict(row)

    if not activity['reversible']:
        conn.close()
        return False, "This action is not reversible."
    if activity['reversed']:
        conn.close()
        return False, "Already reversed."

    # Attempt reversal based on action type
    success = False
    msg = ""

    try:
        if activity['action_type'] == 'update' and activity['target_table'] and activity['old_value']:
            # Restore old value — old_value should be JSON with column:value pairs
            old_data = json.loads(activity['old_value'])
            set_clauses = ', '.join(f"{k} = ?" for k in old_data.keys())
            values = list(old_data.values()) + [activity['target_id']]
            cur.execute(f"UPDATE {activity['target_table']} SET {set_clauses} WHERE id = ?",
                       values)
            success = True
            msg = f"Restored {activity['target_table']} #{activity['target_id']} to previous state."

        elif activity['action_type'] == 'insert' and activity['target_table'] and activity['target_id']:
            cur.execute(f"DELETE FROM {activity['target_table']} WHERE id = ?",
                       (activity['target_id'],))
            success = True
            msg = f"Deleted {activity['target_table']} #{activity['target_id']}."

        elif activity['action_type'] == 'delete' and activity['target_table'] and activity['old_value']:
            old_data = json.loads(activity['old_value'])
            cols = ', '.join(old_data.keys())
            placeholders = ', '.join('?' * len(old_data))
            cur.execute(f"INSERT INTO {activity['target_table']} ({cols}) VALUES ({placeholders})",
                       list(old_data.values()))
            success = True
            msg = f"Re-inserted into {activity['target_table']}."

        else:
            msg = "Cannot determine how to reverse this action."

    except Exception as e:
        msg = f"Undo failed: {e}"

    if success:
        cur.execute("""
            UPDATE openclaw_activity
            SET reversed = 1, reversed_at = datetime('now')
            WHERE id = ?
        """, (activity_id,))

    conn.commit()
    conn.close()
    return success, msg


def get_activity_stats(db_path=None):
    """Get activity summary stats."""
    conn = sqlite3.connect(db_path or get_db_path())
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM openclaw_activity")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM openclaw_activity WHERE created_at >= datetime('now', '-24 hours')")
    last_24h = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM openclaw_activity WHERE reversed = 1")
    reversed_count = cur.fetchone()[0]

    cur.execute("""
        SELECT action_type, COUNT(*) as cnt
        FROM openclaw_activity
        GROUP BY action_type
        ORDER BY cnt DESC
    """)
    by_type = {r['action_type']: r['cnt'] for r in cur.fetchall()}

    conn.close()
    return {
        'total': total,
        'last_24h': last_24h,
        'reversed': reversed_count,
        'by_type': by_type,
    }


# =============================================================================
# UI
# =============================================================================

st.title("Activity Log")
st.caption("Everything OpenClaw has done — with undo capability")

# KPIs
stats = get_activity_stats()
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Actions", stats['total'])
k2.metric("Last 24h", stats['last_24h'])
k3.metric("Reversed", stats['reversed'])
k4.metric("Action Types", len(stats['by_type']))

st.markdown("---")

# Filters
with st.sidebar:
    st.subheader("Filters")
    type_filter = st.selectbox("Action Type", ["All"] + list(stats['by_type'].keys()))
    agent_filter = st.text_input("Agent", "")
    show_reversed = st.checkbox("Show reversed", True)
    days_back = st.slider("Days back", 1, 90, 30)

# Fetch activities
conn = get_conn()
cur = conn.cursor()

conditions = [f"created_at >= datetime('now', '-{days_back} days')"]
params = []

if type_filter != "All":
    conditions.append("action_type = ?")
    params.append(type_filter)
if agent_filter:
    conditions.append("agent LIKE ?")
    params.append(f"%{agent_filter}%")
if not show_reversed:
    conditions.append("reversed = 0")

where = " AND ".join(conditions)
cur.execute(f"""
    SELECT * FROM openclaw_activity
    WHERE {where}
    ORDER BY created_at DESC
    LIMIT 100
""", params)
activities = [dict(r) for r in cur.fetchall()]
conn.close()

# Render
if activities:
    for a in activities:
        type_icon = {
            'insert': '➕', 'update': '✏️', 'delete': '🗑️',
            'scan': '🔍', 'alert': '🔔', 'import': '📥',
            'outreach': '📋', 'suggestion': '💡', 'memory': '🧠',
        }.get(a['action_type'], '📝')

        reversed_tag = " ↩️ REVERSED" if a['reversed'] else ""

        with st.container():
            cols = st.columns([1, 4, 1])
            with cols[0]:
                st.markdown(f"**{a['created_at'][:16]}**")
                st.caption(f"{type_icon} {a['action_type']}")
            with cols[1]:
                st.markdown(f"{a['description']}{reversed_tag}")
                meta = []
                if a.get('target_table'):
                    meta.append(f"Table: {a['target_table']}")
                if a.get('target_id'):
                    meta.append(f"ID: {a['target_id']}")
                if a.get('agent'):
                    meta.append(f"Agent: {a['agent']}")
                if meta:
                    st.caption(" · ".join(meta))
            with cols[2]:
                if a['reversible'] and not a['reversed']:
                    if st.button("↩️ Undo", key=f"undo_{a['id']}"):
                        ok, msg = undo_activity(a['id'])
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)
                elif a['reversed']:
                    st.caption("Reversed")

        st.markdown("---")

    st.caption(f"Showing {len(activities)} activities (max 100)")
else:
    st.info("No activities logged yet. OpenClaw will log actions as it works.")
