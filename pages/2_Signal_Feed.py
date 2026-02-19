"""
Signal Feed
Real-time feed of funding events, hiring signals, and lease expirations.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="Signal Feed", page_icon="📡", layout="wide")

def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# DATA FETCHERS
# =============================================================================

def get_funding_events(days_back=90, company_filter=None):
    conn = get_conn()
    cur = conn.cursor()
    query = """
        SELECT f.id, f.event_date, f.round_type, f.amount, f.lead_investor, f.source_url,
               c.name as company_name, c.id as company_id, c.status
        FROM funding_events f
        JOIN companies c ON f.company_id = c.id
        WHERE f.event_date >= date('now', ? || ' days')
    """
    params = [f"-{days_back}"]
    if company_filter:
        query += " AND c.name LIKE ?"
        params.append(f"%{company_filter}%")
    query += " ORDER BY f.event_date DESC"
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_hiring_signals(days_back=90, relevance_filter=None, company_filter=None):
    conn = get_conn()
    cur = conn.cursor()
    query = """
        SELECT h.id, h.signal_date, h.signal_type, h.details, h.relevance, h.source_url,
               c.name as company_name, c.id as company_id, c.status
        FROM hiring_signals h
        JOIN companies c ON h.company_id = c.id
        WHERE h.signal_date >= date('now', ? || ' days')
    """
    params = [f"-{days_back}"]
    if relevance_filter and relevance_filter != "All":
        query += " AND h.relevance = ?"
        params.append(relevance_filter.lower())
    if company_filter:
        query += " AND c.name LIKE ?"
        params.append(f"%{company_filter}%")
    query += " ORDER BY h.signal_date DESC"
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_executive_changes(days_back=90, company_filter=None, priority_filter=None):
    conn = get_conn()
    cur = conn.cursor()
    query = """
        SELECT ec.id, ec.effective_date, ec.person_name, ec.old_title, ec.new_title,
               ec.old_company, ec.new_company, ec.change_type, ec.priority,
               ec.headline, ec.source_url,
               ec.company_name, ec.company_id
        FROM executive_changes ec
        WHERE ec.effective_date >= date('now', ? || ' days')
    """
    params = [f"-{days_back}"]
    if company_filter:
        query += " AND ec.company_name LIKE ?"
        params.append(f"%{company_filter}%")
    if priority_filter and priority_filter != "All":
        query += " AND ec.priority = ?"
        params.append(priority_filter.lower())
    query += " ORDER BY ec.effective_date DESC"
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_upcoming_lease_expirations(months_ahead=18, company_filter=None):
    conn = get_conn()
    cur = conn.cursor()
    query = """
        SELECT l.id, l.lease_expiry, l.square_feet, l.rent_psf,
               c.name as company_name, c.id as company_id, c.status,
               b.address, b.submarket
        FROM leases l
        JOIN companies c ON l.company_id = c.id
        LEFT JOIN buildings b ON l.building_id = b.id
        WHERE l.lease_expiry BETWEEN date('now') AND date('now', ? || ' months')
    """
    params = [f"+{months_ahead}"]
    if company_filter:
        query += " AND c.name LIKE ?"
        params.append(f"%{company_filter}%")
    query += " ORDER BY l.lease_expiry ASC"
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# =============================================================================
# UI
# =============================================================================

st.title("Signal Feed")

# Filters
with st.sidebar:
    st.subheader("Filters")
    company_search = st.text_input("Company name", "")
    days_back = st.slider("Lookback (days)", 7, 365, 90)
    signal_types = st.multiselect(
        "Signal types",
        ["Funding", "Hiring", "Executive", "Lease Expiry"],
        default=["Funding", "Hiring", "Executive", "Lease Expiry"]
    )
    relevance = st.selectbox("Hiring relevance", ["All", "High", "Medium", "Low"])
    exec_priority = st.selectbox("Executive priority", ["All", "High", "Medium", "Low"])

# Summary KPIs
conn = get_conn()
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM funding_events WHERE event_date >= date('now', '-30 days')")
funding_30d = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM hiring_signals WHERE signal_date >= date('now', '-30 days')")
hiring_30d = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM leases WHERE lease_expiry BETWEEN date('now') AND date('now', '+12 months')")
leases_12m = cur.fetchone()[0]
try:
    cur.execute("SELECT COUNT(*) FROM executive_changes WHERE effective_date >= date('now', '-30 days')")
    exec_30d = cur.fetchone()[0]
except Exception:
    exec_30d = 0
conn.close()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Funding (30d)", funding_30d)
k2.metric("Hiring Signals (30d)", hiring_30d)
k3.metric("Exec Changes (30d)", exec_30d)
k4.metric("Lease Expirations (12mo)", leases_12m)

st.markdown("---")

# Combined feed (interleaved by date)
feed_items = []

if "Funding" in signal_types:
    for f in get_funding_events(days_back, company_search):
        amount_str = f"${f['amount']:,.0f}" if f["amount"] else "undisclosed"
        feed_items.append({
            "date": f["event_date"],
            "type": "Funding",
            "icon": "💰",
            "company": f["company_name"],
            "company_id": f["company_id"],
            "headline": f"{f['round_type'] or 'Round'} — {amount_str}",
            "detail": f"Lead: {f['lead_investor'] or 'undisclosed'}",
            "status": f["status"],
            "url": f.get("source_url", ""),
            "relevance": "high",
        })

if "Hiring" in signal_types:
    for h in get_hiring_signals(days_back, relevance, company_search):
        feed_items.append({
            "date": h["signal_date"],
            "type": "Hiring",
            "icon": "👔",
            "company": h["company_name"],
            "company_id": h["company_id"],
            "headline": h["signal_type"].replace("_", " ").title(),
            "detail": h["details"] or "",
            "status": h["status"],
            "url": h.get("source_url", ""),
            "relevance": h["relevance"],
        })

if "Executive" in signal_types:
    for ec in get_executive_changes(days_back, company_search, exec_priority):
        from_part = f" from {ec['old_company']}" if ec.get('old_company') else ""
        re_flag = ec.get('change_type', '') in ('new_re_head', 'new_facilities')
        icon = "🧑‍💼" if re_flag else "👔"
        feed_items.append({
            "date": ec.get("effective_date") or "unknown",
            "type": "Executive",
            "icon": icon,
            "company": ec["company_name"],
            "company_id": ec.get("company_id"),
            "headline": f"{ec['person_name']} → {ec.get('new_title', '?')}{from_part}",
            "detail": ec.get("headline") or "",
            "status": ec.get("change_type", "").replace("_", " "),
            "url": ec.get("source_url", ""),
            "relevance": ec.get("priority", "medium"),
        })

if "Lease Expiry" in signal_types:
    # Size-based thresholds: <15K=18mo, 15-100K=30mo, >100K=48mo
    for l in get_upcoming_lease_expirations(48, company_search):
        sf = l.get("square_feet") or 0
        if sf < 15000:
            max_months = 18
        elif sf <= 100000:
            max_months = 30
        else:
            max_months = 48
        # Check if within window
        try:
            from datetime import datetime as _dt
            exp = _dt.strptime(l["lease_expiry"], "%Y-%m-%d")
            months_until = (exp.year - _dt.now().year) * 12 + (exp.month - _dt.now().month)
            if months_until > max_months:
                continue  # Outside window for this size
        except Exception:
            pass

        sf_str = f"{sf:,.0f} SF" if sf else "unknown SF"
        addr = l.get("address") or "unknown location"
        # Priority: overdue/critical = high, flagged = medium
        if sf > 100000:
            rel = "high"
        elif sf > 50000:
            rel = "high"
        elif months_until <= 12:
            rel = "high"
        else:
            rel = "medium"
        feed_items.append({
            "date": l["lease_expiry"],
            "type": "Lease Expiry",
            "icon": "🏢",
            "company": l["company_name"],
            "company_id": l["company_id"],
            "headline": f"{sf_str} expiring ({months_until}mo)",
            "detail": f"at {addr}" + (f" ({l['submarket']})" if l.get("submarket") else ""),
            "status": l["status"],
            "url": "",
            "relevance": rel,
        })

# Sort by date descending
feed_items.sort(key=lambda x: x["date"] or "0000-00-00", reverse=True)

# Render feed
if not feed_items:
    st.info("No signals found for the selected filters. Try widening the date range or clearing filters.")
else:
    st.subheader(f"Showing {len(feed_items)} signals")

    for item in feed_items[:100]:  # Cap display at 100
        relevance_color = {"high": "🔴", "medium": "🟡", "low": "⚪"}.get(item["relevance"], "⚪")

        with st.container():
            cols = st.columns([1, 3, 2, 1])

            with cols[0]:
                st.markdown(f"**{item['date']}**")
                st.caption(f"{item['icon']} {item['type']}")

            with cols[1]:
                st.markdown(f"**{item['company']}** — {item['headline']}")
                if item["detail"]:
                    st.caption(item["detail"])

            with cols[2]:
                st.caption(f"Status: {item['status']} · {relevance_color} {item['relevance']}")

            with cols[3]:
                if item["url"]:
                    st.markdown(f"[Source]({item['url']})")

        st.markdown("---")

    if len(feed_items) > 100:
        st.caption(f"Showing 100 of {len(feed_items)} signals. Narrow your filters to see more.")
