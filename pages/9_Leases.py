"""
Lease Expiry Tracker — Size-based threshold tracking.

Thresholds:
- Under 15K RSF: Flag 12-18 months before expiry
- 15K - 100K RSF: Flag 12-30 months before expiry
- Over 100K RSF: Flag 24-48 months before expiry
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path

st.set_page_config(page_title="Lease Tracker", page_icon="📋", layout="wide")


def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# Size-based flag windows (months before expiry)
def get_flag_window(sf):
    """Return (min_months, max_months) for flagging based on SF."""
    if sf is None or sf <= 0:
        return (12, 18)
    if sf < 15000:
        return (12, 18)
    if sf <= 100000:
        return (12, 30)
    return (24, 48)


def get_all_leases():
    """Get combined leases from leases table + agency_tenants."""
    conn = get_conn()
    cur = conn.cursor()

    rows = []

    # Main leases table
    cur.execute("""
        SELECT l.id, l.lease_expiry, l.square_feet, l.rent_psf,
               c.name as company_name, c.id as company_id, c.status,
               b.address, b.submarket,
               'leases' as source
        FROM leases l
        JOIN companies c ON l.company_id = c.id
        LEFT JOIN buildings b ON l.building_id = b.id
        WHERE l.lease_expiry IS NOT NULL
        ORDER BY l.lease_expiry ASC
    """)
    for r in cur.fetchall():
        rows.append(dict(r))

    # Agency tenants
    try:
        cur.execute("""
            SELECT t.id, t.lease_expiry, t.square_feet, NULL as rent_psf,
                   t.tenant_name as company_name, NULL as company_id, t.status,
                   b.address, b.submarket,
                   'agency' as source
            FROM agency_tenants t
            JOIN agency_buildings b ON t.building_id = b.id
            WHERE t.lease_expiry IS NOT NULL
        """)
        for r in cur.fetchall():
            rows.append(dict(r))
    except Exception:
        pass  # agency tables may not exist

    conn.close()
    return rows


# =============================================================================
# UI
# =============================================================================

st.title("Lease Expiry Tracker")

# Filters
with st.sidebar:
    st.subheader("Filters")
    size_band = st.selectbox("Size Band", ["All", "Under 15K", "15K - 100K", "Over 100K"])
    submarket_filter = st.text_input("Submarket", "")
    year_filter = st.selectbox("Year", ["All", "2026", "2027", "2028", "2029", "2030+"])

all_leases = get_all_leases()

# Apply filters
filtered = all_leases
if size_band == "Under 15K":
    filtered = [l for l in filtered if (l.get('square_feet') or 0) < 15000]
elif size_band == "15K - 100K":
    filtered = [l for l in filtered if 15000 <= (l.get('square_feet') or 0) <= 100000]
elif size_band == "Over 100K":
    filtered = [l for l in filtered if (l.get('square_feet') or 0) > 100000]

if submarket_filter:
    filtered = [l for l in filtered if submarket_filter.lower() in (l.get('submarket') or '').lower()]

if year_filter != "All":
    if year_filter == "2030+":
        filtered = [l for l in filtered if (l.get('lease_expiry') or '9999') >= '2030']
    else:
        filtered = [l for l in filtered if (l.get('lease_expiry') or '').startswith(year_filter)]

# KPIs
today = datetime.now()
total_sf = sum(l.get('square_feet') or 0 for l in filtered)
expiring_12mo = [l for l in filtered if l.get('lease_expiry') and
                 l['lease_expiry'] <= (datetime(today.year + 1, today.month, today.day)).strftime('%Y-%m-%d')]

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Leases", len(filtered))
k2.metric("Total SF", f"{total_sf:,.0f}")
k3.metric("Expiring 12mo", len(expiring_12mo))
k4.metric("Sources", f"{sum(1 for l in filtered if l['source']=='leases')} main + "
          f"{sum(1 for l in filtered if l['source']=='agency')} agency")

st.markdown("---")

# =============================================================================
# SECTION 1: EXPIRING SOON (threshold-based)
# =============================================================================

st.subheader("Expiring Soon (Size-Adjusted Thresholds)")
st.caption("Under 15K: 12-18mo | 15K-100K: 12-30mo | Over 100K: 24-48mo")

flagged = []
for l in filtered:
    expiry = l.get('lease_expiry')
    sf = l.get('square_feet') or 0
    if not expiry:
        continue

    try:
        exp_dt = datetime.strptime(expiry, '%Y-%m-%d')
    except Exception:
        continue

    months_until = (exp_dt.year - today.year) * 12 + (exp_dt.month - today.month)
    if months_until < 0:
        months_until = 0  # Already expired

    min_mo, max_mo = get_flag_window(sf)

    if months_until <= max_mo:
        urgency = 'overdue' if months_until <= 0 else 'critical' if months_until <= min_mo else 'flagged'
        l['months_until'] = months_until
        l['urgency'] = urgency
        l['flag_window'] = f"{min_mo}-{max_mo}mo"
        flagged.append(l)

flagged.sort(key=lambda x: x['months_until'])

if flagged:
    for l in flagged:
        sf_str = f"{l['square_feet']:,.0f} SF" if l.get('square_feet') else "? SF"
        addr = l.get('address') or 'unknown'
        sub = f" ({l['submarket']})" if l.get('submarket') else ""

        if l['urgency'] == 'overdue':
            badge = "🔴 OVERDUE"
        elif l['urgency'] == 'critical':
            badge = "🔴 CRITICAL"
        else:
            badge = "🟡 FLAGGED"

        cols = st.columns([1, 3, 1, 1])
        with cols[0]:
            st.markdown(f"**{badge}**")
            st.caption(f"{l['months_until']}mo left")
        with cols[1]:
            st.markdown(f"**{l['company_name']}** — {sf_str} at {addr}{sub}")
        with cols[2]:
            st.caption(f"Expires: {l['lease_expiry']}")
        with cols[3]:
            st.caption(f"Window: {l['flag_window']}")
else:
    st.info("No leases within threshold windows.")

st.markdown("---")

# =============================================================================
# SECTION 2: BIGGEST EXPIRATIONS BY YEAR
# =============================================================================

st.subheader("Biggest Expirations by Year")

# Group by year
by_year = {}
for l in filtered:
    expiry = l.get('lease_expiry', '')
    if not expiry:
        continue
    year = expiry[:4]
    if year not in by_year:
        by_year[year] = []
    by_year[year].append(l)

for year in sorted(by_year.keys()):
    leases_yr = sorted(by_year[year], key=lambda x: -(x.get('square_feet') or 0))
    total_yr_sf = sum(l.get('square_feet') or 0 for l in leases_yr)

    with st.expander(f"**{year}** — {len(leases_yr)} leases, {total_yr_sf:,.0f} SF total"):
        for l in leases_yr[:10]:
            sf_str = f"{l['square_feet']:,.0f} SF" if l.get('square_feet') else "? SF"
            addr = l.get('address') or 'unknown'
            sub = f" ({l['submarket']})" if l.get('submarket') else ""
            st.markdown(f"- **{l['company_name']}** — {sf_str} at {addr}{sub} — expires {l['lease_expiry']}")
        if len(leases_yr) > 10:
            st.caption(f"+ {len(leases_yr) - 10} more")

st.markdown("---")
st.caption("Powered by Relationship Engine — Lease Intelligence")
