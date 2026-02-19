"""
Agency Dashboard — Manage agency buildings, availabilities, activity, and tasks.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path
from agency import (
    parse_quick_input, add_availability, add_activity, add_task,
    get_tasks, mark_task_done, get_availabilities, get_activity,
    get_tenant_roll, get_expiring_tenants, get_building_summary,
    get_buildings_by_type, add_building, toggle_building_type,
    check_building_exists, add_market_requirement, get_market_requirements,
    update_availability_status,
)
from core.agency_engine import match_market_to_building

st.set_page_config(page_title="Agency", page_icon="🏢", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# BUILDING SELECTOR
# =============================================================================

st.title("🏢 Agency Dashboard")

# Get buildings by type
active_buildings = get_buildings_by_type('active_agency')
watchlist_buildings = get_buildings_by_type('watchlist')
all_buildings = active_buildings + watchlist_buildings

# Filter
filter_col1, filter_col2 = st.columns([2, 3])
with filter_col1:
    view_type = st.selectbox("View", ["Active Agency", "Watchlist", "All"],
                              key="agency_view")
with filter_col2:
    if view_type == "Active Agency":
        display_buildings = active_buildings
    elif view_type == "Watchlist":
        display_buildings = watchlist_buildings
    else:
        display_buildings = all_buildings

    building_options = {f"{b['name']} ({b['address']})": b['id'] for b in display_buildings}
    building_options["-- All --"] = None

    selected_label = st.selectbox("Building", list(building_options.keys()), key="agency_building")
    selected_building_id = building_options[selected_label]

st.markdown("---")

# =============================================================================
# QUICK INPUT
# =============================================================================

if selected_building_id:
    st.subheader("⚡ Quick Input")
    quick_text = st.text_input(
        "Type update, availability, or task...",
        placeholder='e.g. "24th floor, 5374 RSF, available 12/1/26" or "IMC: follow up on lease"',
        key="quick_input"
    )
    st.caption("Examples: `proposal 38th fl, IMC with Savills` · "
              "`tour 42 fl, Citadel with CBRE` · `IMC: set up attorney meeting`")

    if quick_text:
        parsed = parse_quick_input(quick_text)
        st.caption(f"Parsed as: **{parsed['type']}**")

        if parsed['type'] == 'availability':
            if st.button("Add Availability", key="qi_avail"):
                add_availability(
                    selected_building_id,
                    floor=parsed.get('floor', '?'),
                    square_feet=parsed.get('sf', 0),
                    available_date=parsed.get('available_date'),
                    asking_rent=parsed.get('asking_rent'),
                )
                st.success("Availability added.")
                st.rerun()

        elif parsed['type'] in ('proposal', 'tour', 'loi'):
            if st.button(f"Log {parsed['type'].title()}", key="qi_activity"):
                add_activity(
                    selected_building_id,
                    activity_type=parsed['type'],
                    company_name=parsed.get('company'),
                    floor=parsed.get('floor'),
                    broker_name=parsed.get('broker_name'),
                    broker_firm=parsed.get('broker_firm'),
                    square_feet=parsed.get('sf'),
                )
                st.success(f"{parsed['type'].title()} logged.")
                st.rerun()

        elif parsed['type'] == 'task':
            if st.button("Add Task", key="qi_task"):
                add_task(
                    building_id=selected_building_id,
                    tenant_or_company=parsed.get('tenant'),
                    task_text=parsed.get('task', ''),
                    task_type=parsed.get('task_type', 'follow_up'),
                )
                st.success("Task added.")
                st.rerun()

    st.markdown("---")

# =============================================================================
# TASKS
# =============================================================================

with st.expander("📋 Tasks", expanded=True):
    tasks = get_tasks(building_id=selected_building_id, status='pending')
    if tasks:
        for t in tasks:
            tc1, tc2, tc3, tc4 = st.columns([3, 1, 1, 1])
            with tc1:
                priority_badge = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(
                    t['priority'], "⚪")
                label = t.get('tenant_or_company') or ''
                prefix = f"**[{label}]** " if label else ""
                st.markdown(f"{priority_badge} {prefix}{t['task_text']}")
            with tc2:
                st.caption(f"Due: {t.get('due_date') or 'none'}")
            with tc3:
                st.caption(t.get('task_type', ''))
            with tc4:
                if st.button("✅", key=f"task_done_{t['id']}"):
                    mark_task_done(t['id'])
                    st.rerun()
    else:
        st.info("No pending tasks.")

    # Inline add
    with st.form("add_task_form", clear_on_submit=True):
        atc1, atc2, atc3, atc4 = st.columns([2, 3, 1, 1])
        with atc1:
            new_tenant = st.text_input("Tenant/Company", key="at_tenant")
        with atc2:
            new_task = st.text_input("Task", key="at_task")
        with atc3:
            new_priority = st.selectbox("Priority", ["medium", "high", "low"], key="at_pri")
        with atc4:
            new_type = st.selectbox("Type", ["follow_up", "meeting", "legal", "tour",
                                              "proposal", "negotiation", "other"], key="at_type")
        if st.form_submit_button("Add Task"):
            if new_task:
                add_task(
                    building_id=selected_building_id,
                    tenant_or_company=new_tenant,
                    task_text=new_task,
                    priority=new_priority,
                    task_type=new_type,
                )
                st.rerun()

# =============================================================================
# AVAILABILITIES
# =============================================================================

if selected_building_id:
    st.subheader("📐 Availabilities")
    avails = get_availabilities(selected_building_id)
    if avails:
        for a in avails:
            ac1, ac2, ac3, ac4, ac5 = st.columns([1, 1, 1, 1, 1])
            with ac1:
                st.markdown(f"**Floor {a['floor']}**")
            with ac2:
                st.caption(f"{a['square_feet']:,} SF")
            with ac3:
                st.caption(f"Available: {a.get('available_date') or 'now'}")
            with ac4:
                rent = a.get('asking_rent')
                st.caption(f"${rent:.2f}/SF" if rent else "TBD")
            with ac5:
                status_color = {
                    'available': '🟢', 'in_negotiation': '🟡',
                    'leased': '🔴', 'coming_available': '🔵'
                }.get(a['status'], '⚪')
                st.caption(f"{status_color} {a['status']}")
    else:
        st.info("No availabilities listed.")

    st.markdown("---")

# =============================================================================
# ACTIVITY (proposals, tours, other)
# =============================================================================

if selected_building_id:
    st.subheader("📊 Activity")

    # Proposals
    proposals = get_activity(selected_building_id, 'proposal', limit=10)
    if proposals:
        st.markdown("**Proposals:**")
        for p in proposals:
            pc1, pc2, pc3, pc4 = st.columns([1, 2, 2, 2])
            with pc1:
                st.caption(p.get('activity_date', ''))
            with pc2:
                fl = f"Fl {p['floor']}" if p.get('floor') else ''
                sf = f" ({p['square_feet']:,} SF)" if p.get('square_feet') else ''
                st.markdown(f"**{p.get('company_name', '?')}** {fl}{sf}")
            with pc3:
                broker = p.get('broker_name') or ''
                firm = p.get('broker_firm') or ''
                st.caption(f"{broker} / {firm}" if broker or firm else "")
            with pc4:
                st.caption(p.get('notes') or '')

    # Tours
    tours = get_activity(selected_building_id, 'tour', limit=10)
    if tours:
        st.markdown("**Tours:**")
        for t in tours:
            tc1, tc2, tc3 = st.columns([1, 3, 2])
            with tc1:
                st.caption(t.get('activity_date', ''))
            with tc2:
                fl = f"Fl {t['floor']}" if t.get('floor') else ''
                st.markdown(f"**{t.get('company_name', '?')}** {fl}")
            with tc3:
                broker = t.get('broker_name') or ''
                firm = t.get('broker_firm') or ''
                st.caption(f"{broker} / {firm}" if broker or firm else "")

    # Other (LOI, lease_signed, inquiry)
    other = get_activity(selected_building_id, limit=10)
    other = [a for a in other if a['activity_type'] not in ('proposal', 'tour')]
    if other:
        st.markdown("**Other Activity:**")
        for o in other:
            st.caption(f"{o.get('activity_date', '')} | {o['activity_type']} | "
                      f"{o.get('company_name', '')} | {o.get('notes', '')}")

    if not proposals and not tours and not other:
        st.info("No activity logged.")

    st.markdown("---")

# =============================================================================
# MATCHING PROSPECTS
# =============================================================================

if selected_building_id:
    st.subheader("🎯 Matching Prospects")
    matches = match_market_to_building(selected_building_id)
    if matches:
        for m in matches:
            mc1, mc2, mc3, mc4 = st.columns([2, 1, 2, 2])
            with mc1:
                st.markdown(f"**{m['company']}**")
            with mc2:
                st.caption(f"{m['sf_min']:,}–{m['sf_max']:,} SF")
            with mc3:
                broker = m.get('broker_name') or ''
                firm = m.get('broker_firm') or ''
                st.caption(f"{broker} / {firm}" if broker or firm else "")
            with mc4:
                floors = ", ".join(f"Fl {f['floor']} ({f['available_sf']:,})"
                                   for f in m['matched_floors'])
                st.caption(floors)
    else:
        st.info("No matching market requirements.")

    st.markdown("---")

# =============================================================================
# TENANT ROLL
# =============================================================================

if selected_building_id:
    with st.expander("🏠 Tenant Roll", expanded=False):
        tenants = get_tenant_roll(selected_building_id)
        if tenants:
            for t in tenants:
                trc1, trc2, trc3, trc4, trc5 = st.columns([2, 1, 1, 1, 1])
                with trc1:
                    st.markdown(f"**{t['tenant_name']}**")
                with trc2:
                    st.caption(f"Fl {t.get('floor', '?')}")
                with trc3:
                    sf = t.get('square_feet')
                    st.caption(f"{sf:,} SF" if sf else "?")
                with trc4:
                    expiry = t.get('lease_expiry')
                    if expiry:
                        exp_dt = datetime.strptime(expiry, "%Y-%m-%d")
                        months_out = (exp_dt - datetime.now()).days / 30
                        if months_out <= 6:
                            st.markdown(f"🔴 {expiry}")
                        elif months_out <= 18:
                            st.markdown(f"🟡 {expiry}")
                        else:
                            st.caption(expiry)
                    else:
                        st.caption("?")
                with trc5:
                    st.caption(t.get('status', ''))
        else:
            st.info("No tenants on file.")

# =============================================================================
# WATCHLIST SECTION
# =============================================================================

st.markdown("---")
with st.expander("👁️ Watchlist Buildings", expanded=False):
    wl = get_buildings_by_type('watchlist')
    if wl:
        for w in wl:
            wc1, wc2, wc3, wc4 = st.columns([3, 1, 1, 1])
            with wc1:
                st.markdown(f"**{w['name']}** — {w['address']}")
            with wc2:
                st.caption(w.get('client_name') or '')
            with wc3:
                st.caption(w.get('deal_type') or '')
            with wc4:
                if st.button("→ Active", key=f"wl_activate_{w['id']}"):
                    toggle_building_type(w['id'])
                    st.rerun()
    else:
        st.info("No watchlist buildings.")

    # Add new building
    st.markdown("**Add Building:**")
    with st.form("add_building_form", clear_on_submit=True):
        nbc1, nbc2, nbc3 = st.columns(3)
        with nbc1:
            nb_address = st.text_input("Address", key="nb_addr")
        with nbc2:
            nb_type = st.selectbox("Type", ["watchlist", "active_agency", "pitch"], key="nb_type")
        with nbc3:
            nb_client = st.text_input("Client", key="nb_client")
        nbc4, nbc5 = st.columns(2)
        with nbc4:
            nb_deal = st.text_input("Deal Type", key="nb_deal",
                                     placeholder="sublease, direct, disposal")
        with nbc5:
            nb_submarket = st.text_input("Submarket", key="nb_sub",
                                          placeholder="Midtown, Hudson Yards")
        if st.form_submit_button("Add Building"):
            if nb_address:
                existing = check_building_exists(nb_address)
                if existing:
                    st.warning(f"Already tracking: {existing['name']} ({existing['building_type']})")
                else:
                    add_building(nb_address, building_type=nb_type, deal_type=nb_deal or None,
                                 client_name=nb_client or None, submarket=nb_submarket or None)
                    st.success(f"Added {nb_address}")
                    st.rerun()

# =============================================================================
# BUILDING STATS (sidebar)
# =============================================================================

if selected_building_id:
    summary = get_building_summary(selected_building_id)
    if summary:
        with st.sidebar:
            st.subheader(summary.get('name', ''))
            st.caption(summary.get('address', ''))
            st.metric("Available Spaces", summary['available_spaces'])
            st.metric("Available SF", f"{summary['available_sf']:,}")
            st.metric("Tenants", summary['tenant_count'])
            if summary.get('total_sf'):
                st.metric("Occupancy", f"{summary['occupancy_pct']}%")
            st.metric("Pending Tasks", summary['pending_tasks'])
            st.metric("Activity (30d)", summary['recent_activity_30d'])
            st.metric("Expiring (18m)", summary['expiring_tenants_18m'])

            if summary.get('building_type') == 'watchlist':
                st.caption(f"📋 Watchlist — {summary.get('client_name', '')}")
                st.caption(f"Deal: {summary.get('deal_type', '')}")

# Footer
st.markdown("---")
st.caption("Agency Dashboard — Relationship Engine")
