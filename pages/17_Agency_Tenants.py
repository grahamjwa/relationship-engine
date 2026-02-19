"""
Agency Tenant Tracker — All tenants across agency buildings.

Table view with building, tenant, floor, RSF, broker info, LXD, status.
Color-coded expiry warnings. Export to Excel.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="Agency Tenants", page_icon="🏢", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def get_all_tenants(building_filter=None, lxd_year=None, sf_min=None, sf_max=None,
                    sort_by='lxd_asc'):
    conn = get_conn()
    cur = conn.cursor()

    conditions = ["1=1"]
    params = []

    if building_filter and building_filter != "All":
        conditions.append("b.name = ?")
        params.append(building_filter)
    if lxd_year:
        conditions.append("t.lease_expiry_date LIKE ?")
        params.append(f"{lxd_year}%")
    if sf_min:
        conditions.append("t.occupied_sf >= ?")
        params.append(sf_min)
    if sf_max:
        conditions.append("t.occupied_sf <= ?")
        params.append(sf_max)

    where = " AND ".join(conditions)

    order = "t.lease_expiry_date ASC"
    if sort_by == 'sf_desc':
        order = "t.occupied_sf DESC"
    elif sort_by == 'sf_asc':
        order = "t.occupied_sf ASC"
    elif sort_by == 'lxd_desc':
        order = "t.lease_expiry_date DESC"

    cur.execute(f"""
        SELECT t.*, b.name as building_name, b.address as building_address
        FROM agency_tenants t
        JOIN agency_buildings b ON t.building_id = b.id
        WHERE {where}
        ORDER BY {order}
    """, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_buildings():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT name FROM agency_buildings ORDER BY name")
    rows = [r['name'] for r in cur.fetchall()]
    conn.close()
    return rows


# =============================================================================
# UI
# =============================================================================

st.title("Agency Tenants")
st.caption("All tenants across agency buildings")

buildings = get_buildings()
today = datetime.now().date()

# Filters
f1, f2, f3, f4, f5 = st.columns(5)
with f1:
    filt_bldg = st.selectbox("Building", ["All"] + buildings, key="f_bldg")
with f2:
    years = [str(y) for y in range(today.year, today.year + 6)]
    filt_year = st.selectbox("LXD Year", ["All"] + years, key="f_year")
with f3:
    filt_sf_min = st.number_input("Min RSF", 0, step=5000, key="f_sfmin")
with f4:
    filt_sf_max = st.number_input("Max RSF", 0, step=5000, key="f_sfmax")
with f5:
    filt_sort = st.selectbox("Sort", [
        ('LXD (soonest)', 'lxd_asc'),
        ('LXD (latest)', 'lxd_desc'),
        ('RSF (largest)', 'sf_desc'),
        ('RSF (smallest)', 'sf_asc'),
    ], format_func=lambda x: x[0], key="f_sort")

tenants = get_all_tenants(
    building_filter=filt_bldg,
    lxd_year=filt_year if filt_year != "All" else None,
    sf_min=filt_sf_min if filt_sf_min > 0 else None,
    sf_max=filt_sf_max if filt_sf_max > 0 else None,
    sort_by=filt_sort[1]
)

# KPIs
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Tenants", len(tenants))
total_sf = sum(t.get('occupied_sf', 0) or 0 for t in tenants)
k2.metric("Total RSF", f"{total_sf:,}")

# Count by expiry urgency
within_6mo = 0
within_18mo = 0
for t in tenants:
    lxd = t.get('lease_expiry_date')
    if lxd:
        try:
            exp_date = datetime.strptime(lxd[:10], '%Y-%m-%d').date()
            months = (exp_date - today).days / 30
            if months <= 6:
                within_6mo += 1
            elif months <= 18:
                within_18mo += 1
        except (ValueError, TypeError):
            pass

k3.metric("LXD < 6 months", within_6mo)
k4.metric("LXD 6-18 months", within_18mo)

st.markdown("---")

# Table
if tenants:
    for t in tenants:
        lxd = t.get('lease_expiry_date', '')
        lxd_display = lxd[:10] if lxd else "Unknown"

        # Color code
        urgency = ""
        if lxd:
            try:
                exp_date = datetime.strptime(lxd[:10], '%Y-%m-%d').date()
                months = (exp_date - today).days / 30
                if months <= 0:
                    urgency = "🔴 EXPIRED"
                elif months <= 6:
                    urgency = "🔴"
                elif months <= 18:
                    urgency = "🟡"
                else:
                    urgency = "🟢"
            except (ValueError, TypeError):
                urgency = "⚪"

        sf_display = f"{t.get('occupied_sf', 0):,}" if t.get('occupied_sf') else "?"

        cols = st.columns([2, 2, 1, 1, 1, 1, 1])
        with cols[0]:
            st.markdown(f"**{t.get('building_name', '?')}**")
            if t.get('building_address'):
                st.caption(t['building_address'])
        with cols[1]:
            st.markdown(f"**{t.get('tenant_name', '?')}**")
            floor = t.get('floor') or t.get('floors', '?')
            st.caption(f"Floor: {floor}")
        with cols[2]:
            st.markdown(f"{sf_display} SF")
        with cols[3]:
            prev_broker = t.get('previous_broker', '')
            prev_firm = t.get('previous_broker_firm', '')
            if prev_broker or prev_firm:
                st.caption(f"{prev_broker}")
                if prev_firm:
                    st.caption(prev_firm)
            else:
                st.caption("—")
        with cols[4]:
            st.markdown(f"{urgency} {lxd_display}")
        with cols[5]:
            st.caption(t.get('status', '?'))
        with cols[6]:
            pass

        st.markdown("---")

    # Export
    st.subheader("Export")
    if st.button("📥 Export to CSV"):
        import csv
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Building', 'Address', 'Tenant', 'Floor', 'RSF',
                        'Previous Broker', 'Broker Firm', 'LXD', 'Status'])
        for t in tenants:
            writer.writerow([
                t.get('building_name', ''),
                t.get('building_address', ''),
                t.get('tenant_name', ''),
                t.get('floor', t.get('floors', '')),
                t.get('occupied_sf', ''),
                t.get('previous_broker', ''),
                t.get('previous_broker_firm', ''),
                t.get('lease_expiry_date', '')[:10] if t.get('lease_expiry_date') else '',
                t.get('status', ''),
            ])
        csv_data = output.getvalue()
        st.download_button(
            "Download CSV",
            data=csv_data,
            file_name=f"agency_tenants_{today.strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
else:
    st.info("No tenants found. Adjust filters or add tenants via the Agency module.")
