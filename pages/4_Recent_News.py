"""
Recent News
Aggregated market intelligence: funding rounds, executive moves, hiring surges,
and lease expirations — summarized as actionable news items.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from graph_engine import get_db_path

st.set_page_config(page_title="Recent News", page_icon="📰", layout="wide")

def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# NEWS GENERATORS
# =============================================================================

def generate_news_items(days_back=30):
    """
    Build a news feed from DB events, translated into natural-language headlines.
    Each item: date, category, headline, body, company_id, priority.
    """
    conn = get_conn()
    cur = conn.cursor()
    items = []

    # --- FUNDING ---
    cur.execute("""
        SELECT f.event_date, f.round_type, f.amount, f.lead_investor,
               c.name, c.id as company_id, c.status
        FROM funding_events f
        JOIN companies c ON f.company_id = c.id
        WHERE f.event_date >= date('now', ? || ' days')
        ORDER BY f.event_date DESC
    """, (f"-{days_back}",))

    for row in cur.fetchall():
        amount = f"${row['amount']:,.0f}" if row["amount"] else "an undisclosed amount"
        investor = row["lead_investor"] or "undisclosed investors"
        round_type = row["round_type"] or "funding round"

        headline = f"{row['name']} raises {amount} in {round_type}"
        body = f"Led by {investor}. {row['name']} is currently a {row['status'].replace('_', ' ')} in our pipeline."

        # Priority: higher for targets and larger amounts
        priority = 3  # default
        if row["status"] in ("high_growth_target", "prospect"):
            priority = 1
        elif row["status"] == "active_client":
            priority = 2

        items.append({
            "date": row["event_date"],
            "category": "Funding",
            "icon": "💰",
            "headline": headline,
            "body": body,
            "company": row["name"],
            "company_id": row["company_id"],
            "priority": priority,
        })

    # --- HIRING: leadership / new_office / headcount_growth ---
    cur.execute("""
        SELECT h.signal_date, h.signal_type, h.details, h.relevance,
               c.name, c.id as company_id, c.status
        FROM hiring_signals h
        JOIN companies c ON h.company_id = c.id
        WHERE h.signal_date >= date('now', ? || ' days')
        AND h.relevance IN ('high', 'medium')
        ORDER BY h.signal_date DESC
    """, (f"-{days_back}",))

    for row in cur.fetchall():
        signal = row["signal_type"].replace("_", " ").title()
        details = row["details"] or ""

        if row["signal_type"] == "leadership_hire":
            headline = f"{row['name']} makes key leadership hire"
        elif row["signal_type"] == "new_office":
            headline = f"{row['name']} opening new office"
        elif row["signal_type"] == "headcount_growth":
            headline = f"{row['name']} on hiring spree"
        else:
            headline = f"{row['name']}: {signal}"

        body = details if details else f"{signal} signal detected for {row['name']}."
        if row["status"] in ("high_growth_target", "prospect"):
            body += " This is a target company — consider outreach."

        priority = 2 if row["relevance"] == "high" else 3

        items.append({
            "date": row["signal_date"],
            "category": "Hiring",
            "icon": "👔",
            "headline": headline,
            "body": body,
            "company": row["name"],
            "company_id": row["company_id"],
            "priority": priority,
        })

    # --- LEASE EXPIRATIONS (upcoming) ---
    cur.execute("""
        SELECT l.lease_expiry, l.square_feet,
               c.name, c.id as company_id, c.status,
               b.address, b.submarket
        FROM leases l
        JOIN companies c ON l.company_id = c.id
        LEFT JOIN buildings b ON l.building_id = b.id
        WHERE l.lease_expiry BETWEEN date('now') AND date('now', '+6 months')
        ORDER BY l.lease_expiry ASC
    """)

    for row in cur.fetchall():
        sf = f"{row['square_feet']:,.0f} SF" if row["square_feet"] else "unknown SF"
        addr = row.get("address") or "unknown location"
        submarket = row.get("submarket") or ""

        headline = f"{row['name']}: {sf} lease expiring {row['lease_expiry']}"
        body = f"At {addr}"
        if submarket:
            body += f" ({submarket})"
        body += f". Company status: {row['status'].replace('_', ' ')}."

        priority = 1 if row.get("square_feet") and row["square_feet"] >= 50000 else 2

        items.append({
            "date": row["lease_expiry"],
            "category": "Lease Expiry",
            "icon": "🏢",
            "headline": headline,
            "body": body,
            "company": row["name"],
            "company_id": row["company_id"],
            "priority": priority,
        })

    # --- STALE OUTREACH (active clients not contacted in 60+ days) ---
    cur.execute("""
        SELECT c.id, c.name, c.status,
               MAX(o.outreach_date) as last_contact,
               CAST(julianday('now') - julianday(MAX(o.outreach_date)) AS INTEGER) as days_silent
        FROM companies c
        LEFT JOIN outreach_log o ON c.id = o.target_company_id
        WHERE c.status = 'active_client'
        GROUP BY c.id
        HAVING last_contact IS NULL OR last_contact < date('now', '-60 days')
        ORDER BY days_silent DESC
    """)

    for row in cur.fetchall():
        days = row["days_silent"] or 999
        last = row["last_contact"] or "never"
        headline = f"Client at risk: {row['name']} — {days} days since last contact"
        body = f"Last outreach: {last}. Active client needs a check-in."

        items.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "category": "Client Risk",
            "icon": "⚠️",
            "headline": headline,
            "body": body,
            "company": row["name"],
            "company_id": row["id"],
            "priority": 1,
        })

    conn.close()

    # Sort: priority first, then date descending
    items.sort(key=lambda x: (x["priority"], x["date"]))
    # Re-sort to interleave by date but keep priority ordering within same date
    items.sort(key=lambda x: (x["date"], x["priority"]), reverse=True)

    return items


# =============================================================================
# UI
# =============================================================================

st.title("Recent News & Intelligence")

# Sidebar filters
with st.sidebar:
    st.subheader("Filters")
    days = st.slider("Lookback (days)", 7, 180, 30)
    categories = st.multiselect(
        "Categories",
        ["Funding", "Hiring", "Lease Expiry", "Client Risk"],
        default=["Funding", "Hiring", "Lease Expiry", "Client Risk"]
    )
    priority_filter = st.selectbox("Priority", ["All", "High (1)", "Medium (2)", "Low (3)"])
    company_search = st.text_input("Company search", "")

# Generate feed
all_items = generate_news_items(days)

# Apply filters
filtered = all_items
if categories:
    filtered = [i for i in filtered if i["category"] in categories]
if priority_filter != "All":
    p = int(priority_filter.split("(")[1].rstrip(")"))
    filtered = [i for i in filtered if i["priority"] == p]
if company_search:
    filtered = [i for i in filtered if company_search.lower() in i["company"].lower()]

# Summary
st.markdown("---")
cat_counts = {}
for item in filtered:
    cat_counts[item["category"]] = cat_counts.get(item["category"], 0) + 1

cols = st.columns(len(cat_counts) if cat_counts else 1)
for col, (cat, count) in zip(cols, cat_counts.items()):
    col.metric(cat, count)

st.markdown("---")

# Render news feed
if not filtered:
    st.info("No news items match your filters.")
else:
    for item in filtered[:75]:
        priority_badge = {1: "🔴 HIGH", 2: "🟡 MED", 3: "⚪ LOW"}.get(item["priority"], "")

        with st.container():
            top_cols = st.columns([1, 5, 1])

            with top_cols[0]:
                st.markdown(f"### {item['icon']}")
                st.caption(item["category"])

            with top_cols[1]:
                st.markdown(f"**{item['headline']}**")
                st.caption(item["body"])

            with top_cols[2]:
                st.caption(f"{item['date']}")
                st.caption(priority_badge)

        st.markdown("---")

    if len(filtered) > 75:
        st.caption(f"Showing 75 of {len(filtered)} items.")

# Daily digest section
st.markdown("---")
st.subheader("Quick Digest")

high_priority = [i for i in all_items if i["priority"] == 1]
if high_priority:
    st.markdown(f"**{len(high_priority)} high-priority items today:**")
    for item in high_priority[:10]:
        st.markdown(f"- {item['icon']} {item['headline']}")
else:
    st.success("No high-priority items. Pipeline is clean.")
