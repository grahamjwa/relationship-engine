"""
Market Notes
Freeform intel log — store rumors, broker intel, observations.
Query by company, building, contact, tag, or date range.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from graph_engine import get_db_path
from market_notes import add_note, search_notes, get_recent_intel

st.set_page_config(page_title="Market Notes", page_icon="📝", layout="wide")

def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# ADD NOTE
# =============================================================================

st.title("Market Notes")

with st.expander("Add New Note", expanded=True):
    note_text = st.text_area(
        "Note",
        placeholder='e.g. "Blue Owl rumored to be expanding into 625 Madison with Mark Weiss (CW)"',
        height=100,
        key="new_note"
    )

    col1, col2 = st.columns(2)
    with col1:
        source = st.selectbox(
            "Source",
            ["", "broker call", "market tour", "email", "conference", "cold call",
             "linkedin", "news article", "internal meeting", "client meeting", "other"],
            key="note_source"
        )
    with col2:
        note_date = st.date_input("Date", value=datetime.now(), key="note_date")

    if st.button("Save Note", type="primary"):
        if note_text.strip():
            result = add_note(
                note_text.strip(),
                source=source if source else None,
                note_date=note_date.strftime("%Y-%m-%d")
            )

            # Show what was extracted
            st.success(f"Note #{result['note_id']} saved.")

            if result["entities"]["companies"]:
                st.info(f"Companies matched: {', '.join(result['entities']['companies'])}")
            if result["entities"]["buildings"]:
                st.info(f"Buildings matched: {', '.join(result['entities']['buildings'])}")
            if result["entities"]["contacts"]:
                st.info(f"Contacts matched: {', '.join(result['entities']['contacts'])}")
            if result["tags"]:
                st.caption(f"Auto-tags: {', '.join('#' + t for t in result['tags'])}")
        else:
            st.warning("Enter a note first.")

st.markdown("---")


# =============================================================================
# SEARCH
# =============================================================================

st.subheader("Search Notes")

search_col1, search_col2, search_col3, search_col4 = st.columns(4)

with search_col1:
    search_query = st.text_input("Keyword search", "", key="search_query")
with search_col2:
    search_company = st.text_input("Company", "", key="search_company")
with search_col3:
    search_building = st.text_input("Building / Address", "", key="search_building")
with search_col4:
    search_days = st.selectbox("Time range", [
        ("Last 7 days", 7),
        ("Last 14 days", 14),
        ("Last 30 days", 30),
        ("Last 90 days", 90),
        ("Last 6 months", 180),
        ("All time", None),
    ], format_func=lambda x: x[0], key="search_days")

tag_filter = st.multiselect(
    "Filter by tag",
    ["expansion", "relocation", "lease", "deal", "hiring", "funding",
     "rumor", "broker_intel", "tenant_rep", "landlord"],
    key="tag_filter"
)

# Run search
has_filter = any([search_query, search_company, search_building, tag_filter])

if has_filter:
    # If multiple tags selected, search for each and combine
    all_results = []
    if tag_filter:
        for tag in tag_filter:
            results = search_notes(
                query=search_query if search_query else None,
                company=search_company if search_company else None,
                building=search_building if search_building else None,
                tag=tag,
                days_back=search_days[1],
            )
            all_results.extend(results)
        # Deduplicate by id
        seen_ids = set()
        results = []
        for r in all_results:
            if r["id"] not in seen_ids:
                seen_ids.add(r["id"])
                results.append(r)
    else:
        results = search_notes(
            query=search_query if search_query else None,
            company=search_company if search_company else None,
            building=search_building if search_building else None,
            days_back=search_days[1],
        )
else:
    # Default: show recent notes
    results = get_recent_intel(days_back=30)


# =============================================================================
# DISPLAY
# =============================================================================

st.markdown("---")

if not results:
    st.info("No notes found. Add your first note above.")
else:
    st.subheader(f"{len(results)} notes")

    for note in results:
        with st.container():
            # Header row
            header_cols = st.columns([1, 5, 2])

            with header_cols[0]:
                st.markdown(f"**{note['note_date']}**")
                if note.get("source"):
                    st.caption(note["source"])

            with header_cols[1]:
                st.markdown(note["note_text"])

                # Entity chips
                entity_parts = []
                if note.get("companies_mentioned"):
                    for co in note["companies_mentioned"].split("|"):
                        entity_parts.append(f"🏢 {co}")
                if note.get("buildings_mentioned"):
                    for bldg in note["buildings_mentioned"].split("|"):
                        entity_parts.append(f"🏗️ {bldg}")
                if note.get("contacts_mentioned"):
                    for ct in note["contacts_mentioned"].split("|"):
                        entity_parts.append(f"👤 {ct}")

                if entity_parts:
                    st.caption(" · ".join(entity_parts))

            with header_cols[2]:
                if note.get("tags"):
                    tags_display = " ".join(f"`#{t}`" for t in note["tags"].split("|"))
                    st.markdown(tags_display)

        st.markdown("---")


# =============================================================================
# STATS SIDEBAR
# =============================================================================

with st.sidebar:
    st.subheader("Note Stats")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM market_notes")
    total = cur.fetchone()[0]
    st.metric("Total Notes", total)

    cur.execute("SELECT COUNT(*) FROM market_notes WHERE note_date >= date('now', '-7 days')")
    week = cur.fetchone()[0]
    st.metric("This Week", week)

    # Top mentioned companies
    cur.execute("""
        SELECT companies_mentioned, COUNT(*) as cnt
        FROM market_notes
        WHERE companies_mentioned IS NOT NULL
        GROUP BY companies_mentioned
        ORDER BY cnt DESC
        LIMIT 5
    """)
    rows = cur.fetchall()
    if rows:
        st.markdown("**Top Companies Mentioned**")
        for r in rows:
            names = r["companies_mentioned"].replace("|", ", ")
            st.caption(f"{names} ({r['cnt']})")

    # Top tags
    cur.execute("""
        SELECT tags FROM market_notes WHERE tags IS NOT NULL
    """)
    tag_counts = {}
    for r in cur.fetchall():
        for t in r["tags"].split("|"):
            tag_counts[t] = tag_counts.get(t, 0) + 1

    if tag_counts:
        st.markdown("**Top Tags**")
        sorted_tags = sorted(tag_counts.items(), key=lambda x: -x[1])[:8]
        for tag, count in sorted_tags:
            st.caption(f"#{tag} ({count})")

    conn.close()
