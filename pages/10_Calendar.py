"""
Calendar View — Follow-ups, SPOC check-ins, agency tasks, meetings.

Views: Week (default), Month, List (next 30 days)
Color: Red=overdue, Yellow=today/tomorrow, Blue=upcoming
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="Calendar", page_icon="📅", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def get_calendar_items(start_date, end_date):
    """Gather all calendar items in date range."""
    conn = get_conn()
    cur = conn.cursor()
    items = []

    sd = start_date.strftime('%Y-%m-%d')
    ed = end_date.strftime('%Y-%m-%d')

    # 1. Follow-ups from outreach_log
    cur.execute("""
        SELECT o.id, o.follow_up_date as date, c.name as company,
               o.outreach_type, o.notes, 'follow_up' as item_type
        FROM outreach_log o
        LEFT JOIN companies c ON o.target_company_id = c.id
        WHERE o.follow_up_done = 0
        AND o.follow_up_date IS NOT NULL
        AND o.follow_up_date BETWEEN ? AND ?
        ORDER BY o.follow_up_date ASC
    """, (sd, ed))
    for r in cur.fetchall():
        d = dict(r)
        d['label'] = f"Follow-up: {d['company']} ({d['outreach_type']})"
        d['source'] = 'outreach_log'
        items.append(d)

    # Also get overdue ones
    cur.execute("""
        SELECT o.id, o.follow_up_date as date, c.name as company,
               o.outreach_type, o.notes, 'follow_up' as item_type
        FROM outreach_log o
        LEFT JOIN companies c ON o.target_company_id = c.id
        WHERE o.follow_up_done = 0
        AND o.follow_up_date IS NOT NULL
        AND o.follow_up_date < ?
        ORDER BY o.follow_up_date ASC
    """, (sd,))
    for r in cur.fetchall():
        d = dict(r)
        d['label'] = f"OVERDUE Follow-up: {d['company']} ({d['outreach_type']})"
        d['source'] = 'outreach_log'
        items.append(d)

    # 2. SPOC check-ins
    try:
        cur.execute("""
            SELECT id, name as company, spoc_follow_up_date as date,
                   spoc_status, spoc_broker, 'spoc_checkin' as item_type
            FROM companies
            WHERE spoc_follow_up_date IS NOT NULL
            AND spoc_follow_up_date BETWEEN ? AND ?
        """, (sd, ed))
        for r in cur.fetchall():
            d = dict(r)
            d['label'] = f"SPOC check-in: {d['company']}"
            d['notes'] = f"Status: {d.get('spoc_status', '')} — Broker: {d.get('spoc_broker', '')}"
            d['source'] = 'spoc'
            items.append(d)

        # Overdue SPOC
        cur.execute("""
            SELECT id, name as company, spoc_follow_up_date as date,
                   spoc_status, spoc_broker, 'spoc_checkin' as item_type
            FROM companies
            WHERE spoc_follow_up_date IS NOT NULL
            AND spoc_follow_up_date < ?
            AND spoc_status IS NOT NULL
        """, (sd,))
        for r in cur.fetchall():
            d = dict(r)
            d['label'] = f"OVERDUE SPOC check-in: {d['company']}"
            d['notes'] = f"Status: {d.get('spoc_status', '')} — Broker: {d.get('spoc_broker', '')}"
            d['source'] = 'spoc'
            items.append(d)
    except Exception:
        pass

    # 3. Agency tasks
    try:
        cur.execute("""
            SELECT t.id, t.due_date as date, t.tenant_or_company as company,
                   t.task_text, t.priority, t.task_type,
                   b.name as building_name, 'agency_task' as item_type
            FROM agency_tasks t
            LEFT JOIN agency_buildings b ON t.building_id = b.id
            WHERE t.status != 'done'
            AND t.due_date IS NOT NULL
            AND t.due_date BETWEEN ? AND ?
        """, (sd, ed))
        for r in cur.fetchall():
            d = dict(r)
            bldg = d.get('building_name') or ''
            d['label'] = f"Agency: [{bldg}] {d['company']} — {d['task_text']}"
            d['notes'] = d.get('task_text', '')
            d['source'] = 'agency'
            items.append(d)
    except Exception:
        pass

    # 4. Meetings logged
    cur.execute("""
        SELECT o.id, o.outreach_date as date, c.name as company,
               o.notes, 'meeting' as item_type
        FROM outreach_log o
        LEFT JOIN companies c ON o.target_company_id = c.id
        WHERE o.outreach_type = 'meeting'
        AND o.outreach_date BETWEEN ? AND ?
        ORDER BY o.outreach_date ASC
    """, (sd, ed))
    for r in cur.fetchall():
        d = dict(r)
        d['label'] = f"Meeting: {d['company']}"
        d['source'] = 'meeting'
        items.append(d)

    conn.close()
    return items


def color_for_date(date_str):
    """Return color code based on date vs today."""
    if not date_str:
        return 'blue'
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d').date()
    except Exception:
        return 'blue'
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    if dt < today:
        return 'red'
    if dt <= tomorrow:
        return 'yellow'
    return 'blue'


def render_item(item):
    """Render a calendar item with color coding."""
    color = color_for_date(item.get('date'))
    badge = {'red': '🔴', 'yellow': '🟡', 'blue': '🔵'}[color]
    source_icon = {
        'outreach_log': '📋', 'spoc': '🔒', 'agency': '🏢', 'meeting': '🤝'
    }.get(item.get('source', ''), '📌')

    cols = st.columns([1, 4, 1])
    with cols[0]:
        st.markdown(f"{badge} **{item.get('date', '?')}**")
    with cols[1]:
        st.markdown(f"{source_icon} {item['label']}")
        if item.get('notes'):
            st.caption(item['notes'][:100])
    with cols[2]:
        st.caption(item.get('source', '').replace('_', ' '))


# =============================================================================
# UI
# =============================================================================

st.title("Calendar")

today = datetime.now()

# View selector
view = st.radio("View", ["Week", "Month", "List (30d)"], horizontal=True)

if view == "Week":
    # Start of current week (Monday)
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    week_offset = st.number_input("Week offset", -12, 12, 0, key="week_off")
    start += timedelta(weeks=week_offset)
    end += timedelta(weeks=week_offset)
    st.subheader(f"Week of {start.strftime('%b %d')} — {end.strftime('%b %d, %Y')}")

elif view == "Month":
    month_offset = st.number_input("Month offset", -6, 6, 0, key="month_off")
    month = today.month + month_offset
    year = today.year
    while month > 12:
        month -= 12
        year += 1
    while month < 1:
        month += 12
        year -= 1
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = datetime(year, month + 1, 1) - timedelta(days=1)
    st.subheader(f"{start.strftime('%B %Y')}")

else:  # List 30d
    start = today - timedelta(days=7)  # Include recent overdue
    end = today + timedelta(days=30)
    st.subheader(f"Next 30 Days (from {today.strftime('%b %d')})")

items = get_calendar_items(start, end)

# Summary
overdue = [i for i in items if color_for_date(i.get('date')) == 'red']
today_items = [i for i in items if color_for_date(i.get('date')) == 'yellow']
upcoming = [i for i in items if color_for_date(i.get('date')) == 'blue']

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Items", len(items))
k2.metric("Overdue", len(overdue))
k3.metric("Today/Tomorrow", len(today_items))
k4.metric("Upcoming", len(upcoming))

st.markdown("---")

# Render overdue first
if overdue:
    st.subheader("🔴 Overdue")
    for item in sorted(overdue, key=lambda x: x.get('date', '')):
        render_item(item)
    st.markdown("---")

# Today/tomorrow
if today_items:
    st.subheader("🟡 Today / Tomorrow")
    for item in sorted(today_items, key=lambda x: x.get('date', '')):
        render_item(item)
    st.markdown("---")

# Upcoming
if upcoming:
    st.subheader("🔵 Upcoming")
    for item in sorted(upcoming, key=lambda x: x.get('date', '')):
        render_item(item)

if not items:
    st.info("No calendar items in this period.")

st.markdown("---")
st.caption("Powered by Relationship Engine — Calendar Intelligence")
