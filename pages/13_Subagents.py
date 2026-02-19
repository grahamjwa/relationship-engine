"""
Subagent Management — View, enable/disable, and monitor OpenClaw's sub-agents.

Each subagent handles a specific automated task (scanning, alerting, scoring, etc.).
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

st.set_page_config(page_title="Subagents", page_icon="🤖", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def get_subagents():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM subagents ORDER BY name")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_runs(subagent_id=None, limit=20):
    conn = get_conn()
    cur = conn.cursor()
    if subagent_id:
        cur.execute("""
            SELECT r.*, s.name as subagent_name
            FROM subagent_runs r
            JOIN subagents s ON r.subagent_id = s.id
            WHERE r.subagent_id = ?
            ORDER BY r.started_at DESC LIMIT ?
        """, (subagent_id, limit))
    else:
        cur.execute("""
            SELECT r.*, s.name as subagent_name
            FROM subagent_runs r
            JOIN subagents s ON r.subagent_id = s.id
            ORDER BY r.started_at DESC LIMIT ?
        """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def toggle_subagent(subagent_id, enabled):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE subagents SET enabled = ? WHERE id = ?", (1 if enabled else 0, subagent_id))
    conn.commit()
    conn.close()


def register_subagent(name, description, agent_type, schedule=None, config_json=None,
                      db_path=None):
    """Register a new subagent. Called by setup scripts."""
    conn = sqlite3.connect(db_path or get_db_path())
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO subagents
        (name, description, agent_type, schedule, config_json)
        VALUES (?, ?, ?, ?, ?)
    """, (name, description, agent_type, schedule, config_json))
    sid = cur.lastrowid
    conn.commit()
    conn.close()
    return sid


def log_run(subagent_id, status='running', result_summary=None, items_processed=0,
            items_found=0, errors=None, cost_usd=0, duration_sec=None,
            triggered_by='schedule', db_path=None):
    """Log a subagent run. Returns run_id."""
    conn = sqlite3.connect(db_path or get_db_path())
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO subagent_runs
        (subagent_id, status, result_summary, items_processed, items_found,
         errors, cost_usd, duration_sec, triggered_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (subagent_id, status, result_summary, items_processed, items_found,
          errors, cost_usd, duration_sec, triggered_by))
    run_id = cur.lastrowid

    # Update subagent stats
    if status in ('completed', 'failed'):
        cur.execute("""
            UPDATE subagents
            SET last_run = datetime('now'),
                run_count = run_count + 1
            WHERE id = ?
        """, (subagent_id,))
        if duration_sec:
            cur.execute("""
                UPDATE subagents
                SET avg_duration_sec = (avg_duration_sec * (run_count - 1) + ?) / run_count
                WHERE id = ?
            """, (duration_sec, subagent_id))

    conn.commit()
    conn.close()
    return run_id


def complete_run(run_id, status='completed', result_summary=None, items_processed=0,
                 items_found=0, errors=None, cost_usd=0, db_path=None):
    """Mark a run as completed/failed."""
    conn = sqlite3.connect(db_path or get_db_path())
    cur = conn.cursor()
    cur.execute("""
        UPDATE subagent_runs
        SET status = ?, finished_at = datetime('now'),
            duration_sec = (julianday(datetime('now')) - julianday(started_at)) * 86400,
            result_summary = COALESCE(?, result_summary),
            items_processed = ?, items_found = ?,
            errors = ?, cost_usd = ?
        WHERE id = ?
    """, (status, result_summary, items_processed, items_found, errors, cost_usd, run_id))
    conn.commit()
    conn.close()


# =============================================================================
# UI
# =============================================================================

st.title("Subagent Management")
st.caption("Monitor and control OpenClaw's automated agents")

subagents = get_subagents()

# KPIs
conn = get_conn()
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM subagents WHERE enabled = 1")
active_count = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM subagent_runs WHERE started_at >= datetime('now', '-24 hours')")
runs_24h = cur.fetchone()[0]

cur.execute("SELECT COALESCE(SUM(cost_usd), 0) FROM subagent_runs WHERE started_at >= datetime('now', '-24 hours')")
cost_24h = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM subagent_runs WHERE status = 'failed' AND started_at >= datetime('now', '-7 days')")
failures_7d = cur.fetchone()[0]

conn.close()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Active Agents", active_count)
k2.metric("Runs (24h)", runs_24h)
k3.metric("Cost (24h)", f"${cost_24h:.4f}")
k4.metric("Failures (7d)", failures_7d)

st.markdown("---")

# Subagent cards
if subagents:
    st.subheader("Registered Subagents")

    for sa in subagents:
        status_icon = "🟢" if sa['enabled'] else "🔴"
        type_icon = {
            'scanner': '🔍', 'scorer': '📊', 'alerter': '🔔',
            'importer': '📥', 'analyst': '🧠', 'monitor': '👁️',
        }.get(sa['agent_type'], '🤖')

        with st.container():
            cols = st.columns([3, 1, 1, 1])
            with cols[0]:
                st.markdown(f"{status_icon} {type_icon} **{sa['name']}**")
                if sa.get('description'):
                    st.caption(sa['description'])
                meta = []
                if sa.get('schedule'):
                    meta.append(f"Schedule: {sa['schedule']}")
                if sa.get('last_run'):
                    meta.append(f"Last run: {sa['last_run'][:16]}")
                meta.append(f"Runs: {sa['run_count']}")
                if sa['avg_duration_sec'] > 0:
                    meta.append(f"Avg: {sa['avg_duration_sec']:.1f}s")
                st.caption(" · ".join(meta))
            with cols[1]:
                st.caption(sa['agent_type'])
            with cols[2]:
                if sa['enabled']:
                    if st.button("⏸️ Disable", key=f"dis_{sa['id']}"):
                        toggle_subagent(sa['id'], False)
                        st.rerun()
                else:
                    if st.button("▶️ Enable", key=f"en_{sa['id']}"):
                        toggle_subagent(sa['id'], True)
                        st.rerun()
            with cols[3]:
                pass  # placeholder for future "Run Now" button

        # Recent runs for this subagent
        with st.expander(f"Recent runs — {sa['name']}", expanded=False):
            runs = get_runs(sa['id'], limit=10)
            if runs:
                for r in runs:
                    status_badge = {
                        'completed': '✅', 'failed': '❌', 'running': '🔄'
                    }.get(r['status'], '❓')
                    duration = f"{r['duration_sec']:.1f}s" if r.get('duration_sec') else "—"
                    cost = f"${r['cost_usd']:.4f}" if r.get('cost_usd') else "$0"

                    st.caption(
                        f"{status_badge} {r['started_at'][:16]} | "
                        f"{r['status']} | {duration} | "
                        f"Processed: {r['items_processed']} | Found: {r['items_found']} | "
                        f"Cost: {cost}"
                    )
                    if r.get('result_summary'):
                        st.caption(f"  → {r['result_summary'][:120]}")
                    if r.get('errors'):
                        st.caption(f"  ⚠️ {r['errors'][:120]}")
            else:
                st.caption("No runs yet.")

        st.markdown("---")
else:
    st.info("No subagents registered yet. They'll appear here as OpenClaw sets them up.")

# All recent runs
st.subheader("All Recent Runs")
all_runs = get_runs(limit=30)
if all_runs:
    for r in all_runs:
        status_badge = {
            'completed': '✅', 'failed': '❌', 'running': '🔄'
        }.get(r['status'], '❓')
        st.caption(
            f"{status_badge} {r['started_at'][:16]} | **{r['subagent_name']}** | "
            f"{r['status']} | Items: {r['items_processed']}/{r['items_found']}"
        )
else:
    st.caption("No runs logged yet.")

# Sidebar: register new subagent
with st.sidebar:
    st.subheader("Register Subagent")
    new_name = st.text_input("Name", key="sa_name")
    new_desc = st.text_area("Description", key="sa_desc")
    new_type = st.selectbox("Type", ['scanner', 'scorer', 'alerter', 'importer', 'analyst', 'monitor'],
                            key="sa_type")
    new_sched = st.text_input("Schedule (cron)", key="sa_sched", placeholder="e.g. daily, hourly")

    if st.button("Register", key="sa_register") and new_name:
        register_subagent(new_name, new_desc, new_type, new_sched)
        st.success(f"Registered: {new_name}")
        st.rerun()
