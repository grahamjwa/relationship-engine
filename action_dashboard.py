"""
Unified Action Dashboard — Streamlit app with tabbed interface.

Run: streamlit run action_dashboard.py

Tabs:
  1. Opportunities — ranked signal-driven targets
  2. Follow-ups Due — outreach reminders from outreach_manager
  3. Import Data — CSV upload and validation
  4. Data Quality — missing fields, duplicates, health metrics
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path
from core.opportunity_engine import (find_new_opportunities, infer_opportunities,
                                     get_outreach_gaps)
from core.outreach_manager import (log_outreach, get_due_followups,
                                   mark_followup_done, get_followup_summary,
                                   get_outreach_history)
from core.signals import SIGNAL_TYPES
from importers.validate_csv import validate_csv, REQUIRED_COLUMNS
from import_all import IMPORT_DIR, run_all_imports, list_pending_imports
from core.cost_tracker import get_sidebar_widget_data

st.set_page_config(page_title="Relationship Engine", page_icon="🏢", layout="wide")

DB_PATH = get_db_path()
os.makedirs(IMPORT_DIR, exist_ok=True)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# HEADER
# =============================================================================

st.title("🏢 Relationship Engine — Action Dashboard")

summary = get_followup_summary()
pending_imports = list_pending_imports()

h1, h2, h3, h4 = st.columns(4)
h1.metric("Follow-ups Due", summary['due'])
h2.metric("Upcoming", summary['upcoming'])
h3.metric("Completed", summary['completed'])
h4.metric("Pending Imports", len(pending_imports))

# =============================================================================
# THIS WEEK'S FOLLOW-UPS (top of page)
# =============================================================================

st.subheader("📅 This Week's Follow-ups")

conn_fw = get_conn()
cur_fw = conn_fw.cursor()

# Outreach follow-ups (next 7 days + overdue)
cur_fw.execute("""
    SELECT o.id, o.follow_up_date, c.name as company, o.outreach_type,
           o.notes, 'outreach' as source
    FROM outreach_log o
    LEFT JOIN companies c ON o.target_company_id = c.id
    WHERE o.follow_up_done = 0 AND o.follow_up_date IS NOT NULL
    AND o.follow_up_date <= date('now', '+7 days')
    ORDER BY o.follow_up_date ASC
""")
week_items = [dict(r) for r in cur_fw.fetchall()]

# SPOC check-ins due
try:
    cur_fw.execute("""
        SELECT id, name as company, spoc_follow_up_date as follow_up_date,
               spoc_broker as notes, 'spoc' as source
        FROM companies
        WHERE spoc_follow_up_date IS NOT NULL
        AND spoc_follow_up_date <= date('now', '+7 days')
        AND spoc_status IS NOT NULL
    """)
    week_items.extend([dict(r) for r in cur_fw.fetchall()])
except Exception:
    pass

# Agency tasks due
try:
    cur_fw.execute("""
        SELECT t.id, t.due_date as follow_up_date, t.tenant_or_company as company,
               t.task_text as notes, 'agency' as source
        FROM agency_tasks t
        WHERE t.status != 'done' AND t.due_date IS NOT NULL
        AND t.due_date <= date('now', '+7 days')
    """)
    week_items.extend([dict(r) for r in cur_fw.fetchall()])
except Exception:
    pass

conn_fw.close()
week_items.sort(key=lambda x: x.get('follow_up_date', '9999'))

if week_items:
    for wi in week_items[:15]:
        today_str = datetime.now().strftime('%Y-%m-%d')
        is_overdue = (wi.get('follow_up_date') or '9999') < today_str
        badge = "🔴" if is_overdue else "🟡"
        src_icon = {"outreach": "📋", "spoc": "🔒", "agency": "🏢"}.get(wi['source'], "📌")
        wi_cols = st.columns([1, 3, 1, 1])
        with wi_cols[0]:
            st.markdown(f"{badge} {wi.get('follow_up_date', '?')}")
        with wi_cols[1]:
            st.markdown(f"{src_icon} **{wi.get('company', '?')}**"
                        + (f" — {wi.get('notes', '')[:60]}" if wi.get('notes') else ""))
        with wi_cols[2]:
            if wi['source'] == 'outreach' and st.button("✅", key=f"wk_done_{wi['id']}"):
                mark_followup_done(wi['id'])
                st.rerun()
        with wi_cols[3]:
            if wi['source'] == 'outreach' and st.button("+7d", key=f"wk_7d_{wi['id']}"):
                mark_followup_done(wi['id'], reschedule_days=7)
                st.rerun()
else:
    st.success("No follow-ups due this week.")

st.markdown("---")

# =============================================================================
# TABS
# =============================================================================

tab_opps, tab_followups, tab_import, tab_quality = st.tabs([
    "🎯 Opportunities", "📋 Follow-ups Due", "📥 Import Data", "🔍 Data Quality"
])

# ─────────────────────────────────────────────────────────────────────────────
# TAB 1: OPPORTUNITIES
# ─────────────────────────────────────────────────────────────────────────────

with tab_opps:
    st.subheader("Signal-Driven Opportunities")

    opp_col1, opp_col2, opp_col3 = st.columns(3)
    with opp_col1:
        lookback = st.slider("Lookback (days)", 1, 90, 7, key="opp_lookback")
    with opp_col2:
        min_score = st.slider("Min score", 0, 100, 10, key="opp_min")
    with opp_col3:
        show_all = st.checkbox("All companies", False, key="opp_all")

    if show_all:
        opportunities = infer_opportunities(min_score=min_score)
    else:
        opportunities = find_new_opportunities(since_days=lookback)
        opportunities = [o for o in opportunities if o['score'] >= min_score]

    if opportunities:
        for i, opp in enumerate(opportunities[:15], 1):
            impact_arrow = "↑" if opp['space_impact'] > 0 else "↓" if opp['space_impact'] < 0 else "→"
            cols = st.columns([1, 4, 1])
            with cols[0]:
                st.markdown(f"**#{i}** — {opp['score']}")
            with cols[1]:
                st.markdown(f"**{opp['company']}** — {opp['reason']}")
                st.caption(f"Signals: {', '.join(opp['signals'][:4])} | "
                          f"Action: {opp['recommended_action']}")
            with cols[2]:
                st.markdown(f"{impact_arrow} {opp['space_impact']}")

            with st.expander(f"Log outreach — {opp['company']}", expanded=False):
                lc1, lc2, lc3 = st.columns(3)
                with lc1:
                    o_type = st.selectbox("Type", ["email", "call", "linkedin", "meeting"],
                                          key=f"d_otype_{opp['company_id']}")
                with lc2:
                    o_out = st.selectbox("Outcome",
                                         ["sent", "connected", "voicemail", "meeting_set", "replied"],
                                         key=f"d_oout_{opp['company_id']}")
                with lc3:
                    o_fu = st.number_input("Follow-up days", 0, 90, 7,
                                           key=f"d_ofu_{opp['company_id']}")
                o_notes = st.text_input("Notes", key=f"d_onotes_{opp['company_id']}")
                if st.button("Log", key=f"d_olog_{opp['company_id']}"):
                    log_outreach(company_id=opp['company_id'], outreach_type=o_type,
                                 outcome=o_out, notes=o_notes, follow_up_days=o_fu)
                    st.success(f"Logged {o_type} → {opp['company']}")

        # Outreach gaps
        st.markdown("---")
        st.subheader("🕳️ Outreach Gaps")
        gaps = get_outreach_gaps()
        if gaps:
            for g in gaps[:10]:
                gc1, gc2, gc3 = st.columns([3, 1, 1])
                with gc1:
                    st.markdown(f"**{g['company']}** — {g['status'].replace('_', ' ').title()}")
                with gc2:
                    st.caption(f"Lean: {g['lean_score']}")
                with gc3:
                    if g['last_outreach']:
                        st.caption(f"{g['days_since_outreach']}d ago")
                    else:
                        st.caption("Never contacted")
        else:
            st.info("No outreach gaps.")
    else:
        st.info("No opportunities found. Widen lookback or lower min score.")

# ─────────────────────────────────────────────────────────────────────────────
# TAB 2: FOLLOW-UPS DUE
# ─────────────────────────────────────────────────────────────────────────────

with tab_followups:
    st.subheader("Follow-ups Due / Overdue")

    followups = get_due_followups()
    if followups:
        for fu in followups:
            fu_cols = st.columns([3, 1, 1, 1, 1])
            with fu_cols[0]:
                overdue = f" (**{fu['days_overdue']}d overdue**)" if fu['days_overdue'] > 0 else ""
                st.markdown(f"**{fu['company_name']}** — {fu['outreach_type']} "
                           f"on {fu['outreach_date']}{overdue}")
                if fu.get('notes'):
                    st.caption(fu['notes'])
            with fu_cols[1]:
                st.caption(f"Due: {fu['follow_up_date']}")
            with fu_cols[2]:
                if st.button("✅ Done", key=f"d_fu_done_{fu['id']}"):
                    mark_followup_done(fu['id'])
                    st.rerun()
            with fu_cols[3]:
                if st.button("+7d", key=f"d_fu_7_{fu['id']}"):
                    mark_followup_done(fu['id'], reschedule_days=7)
                    st.rerun()
            with fu_cols[4]:
                if st.button("+14d", key=f"d_fu_14_{fu['id']}"):
                    mark_followup_done(fu['id'], reschedule_days=14)
                    st.rerun()
    else:
        st.success("No follow-ups due. You're caught up.")

    # Recent outreach log
    st.markdown("---")
    st.subheader("Recent Outreach Log")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT o.outreach_date, c.name, o.outreach_type, o.outcome,
               o.notes, o.follow_up_date, o.follow_up_done
        FROM outreach_log o
        LEFT JOIN companies c ON o.target_company_id = c.id
        ORDER BY o.outreach_date DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    conn.close()
    if rows:
        for r in rows:
            rd = dict(r)
            done_tag = "✅" if rd.get('follow_up_done') else ""
            st.caption(f"{rd['outreach_date']} | {rd['name']} | {rd['outreach_type']} | "
                      f"{rd['outcome']} | FU: {rd.get('follow_up_date', 'none')} {done_tag}")
    else:
        st.info("No outreach logged yet.")

# ─────────────────────────────────────────────────────────────────────────────
# TAB 3: IMPORT DATA
# ─────────────────────────────────────────────────────────────────────────────

with tab_import:
    st.subheader("Upload & Import CSV Data")

    with st.expander("CSV Column Requirements"):
        for csv_type, cols in REQUIRED_COLUMNS.items():
            st.markdown(f"**{csv_type}:** {', '.join(cols)}")

    upload_types = {
        'linkedin': ("LinkedIn Connections", 'linkedin_connections.csv', 'linkedin'),
        'contacts': ("Contacts", 'contacts.csv', 'contacts'),
        'relationships': ("Relationships", 'relationships.csv', 'relationships'),
        'clients': ("Clients", 'clients.csv', 'clients'),
        'buildings': ("Buildings / Leases", 'buildings.csv', 'buildings'),
    }

    for key, (label, filename, vtype) in upload_types.items():
        uploaded = st.file_uploader(f"Upload {label} CSV", type=['csv'],
                                     key=f"d_upload_{key}")
        if uploaded:
            dest = os.path.join(IMPORT_DIR, filename)
            with open(dest, 'wb') as f:
                f.write(uploaded.getvalue())
            valid, msg = validate_csv(dest, vtype)
            if valid:
                st.success(f"{label}: {msg}")
            else:
                st.error(f"{label}: {msg}")

    st.divider()
    if st.button("Run All Imports", type="primary", key="d_run_imports"):
        with st.spinner("Importing..."):
            results = run_all_imports()
        if results:
            st.json(results)
            st.success("Import complete.")
        else:
            st.warning("No CSV files in imports directory.")

# ─────────────────────────────────────────────────────────────────────────────
# TAB 4: DATA QUALITY
# ─────────────────────────────────────────────────────────────────────────────

with tab_quality:
    st.subheader("Data Quality Report")

    conn = get_conn()
    cur = conn.cursor()

    # Missing fields
    st.markdown("**Missing Fields:**")
    q1, q2, q3 = st.columns(3)

    cur.execute("SELECT COUNT(*) FROM companies WHERE sector IS NULL OR sector = '' OR sector = 'unknown'")
    q1.metric("Companies missing sector", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM contacts WHERE email IS NULL OR email = ''")
    q2.metric("Contacts missing email", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM contacts WHERE previous_companies IS NULL OR previous_companies = ''")
    q3.metric("Contacts missing prev companies", cur.fetchone()[0])

    q4, q5, q6 = st.columns(3)
    cur.execute("SELECT COUNT(*) FROM companies WHERE hq_city IS NULL OR hq_city = ''")
    q4.metric("Companies missing HQ city", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM companies WHERE status IS NULL OR status = ''")
    q5.metric("Companies missing status", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM contacts WHERE title IS NULL OR title = ''")
    q6.metric("Contacts missing title", cur.fetchone()[0])

    # Duplicates
    st.markdown("---")
    st.markdown("**Duplicate Company Names:**")
    cur.execute("""
        SELECT LOWER(name) as lname, COUNT(*) as cnt
        FROM companies
        GROUP BY LOWER(name)
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)
    dupes = cur.fetchall()
    if dupes:
        for d in dupes:
            dd = dict(d)
            st.warning(f"\"{dd['lname']}\" appears {dd['cnt']} times")
    else:
        st.success("No duplicate company names.")

    # Orphans
    st.markdown("---")
    st.markdown("**Orphaned Records:**")
    o1, o2 = st.columns(2)

    cur.execute("""
        SELECT COUNT(*) FROM contacts
        WHERE company_id IS NOT NULL
        AND company_id NOT IN (SELECT id FROM companies)
    """)
    o1.metric("Contacts with invalid company_id", cur.fetchone()[0])

    cur.execute("""
        SELECT COUNT(*) FROM outreach_log
        WHERE target_company_id IS NOT NULL
        AND target_company_id NOT IN (SELECT id FROM companies)
    """)
    o2.metric("Outreach with invalid company_id", cur.fetchone()[0])

    # Table counts
    st.markdown("---")
    st.markdown("**Table Row Counts:**")
    tables = ['companies', 'contacts', 'relationships', 'funding_events',
              'hiring_signals', 'outreach_log', 'buildings', 'leases',
              'deals', 'market_notes']
    tc = st.columns(5)
    for i, t in enumerate(tables):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            tc[i % 5].metric(t.replace('_', ' ').title(), cur.fetchone()[0])
        except Exception:
            tc[i % 5].metric(t.replace('_', ' ').title(), "N/A")

    conn.close()

# =============================================================================
# HIGH ALERT TICKER
# =============================================================================

st.markdown("---")
st.subheader("🔴 High Alerts")

try:
    alert_conn = sqlite3.connect(DB_PATH)
    alert_conn.row_factory = sqlite3.Row
    alert_cur = alert_conn.cursor()
    alerts = []

    # High-priority exec changes (last 7 days)
    try:
        alert_cur.execute("""
            SELECT person_name, new_title, new_company, old_company, change_type,
                   effective_date
            FROM executive_changes
            WHERE priority = 'high'
            AND effective_date >= date('now', '-7 days')
            ORDER BY effective_date DESC
            LIMIT 3
        """)
        for r in alert_cur.fetchall():
            r = dict(r)
            alerts.append(f"👔 [{r['effective_date'][:10]}] **{r['person_name']}** — "
                        f"{r['change_type']}: {r.get('new_title', '?')} at {r.get('new_company', '?')}")
    except Exception:
        pass

    # Large funding at prospects (>$50M, last 7 days)
    try:
        alert_cur.execute("""
            SELECT c.name, f.amount, f.round_type, f.event_date
            FROM funding_events f
            JOIN companies c ON f.company_id = c.id
            WHERE f.amount >= 50000000
            AND f.event_date >= date('now', '-7 days')
            AND c.status IN ('prospect', 'high_growth_target', 'watching')
            ORDER BY f.amount DESC
            LIMIT 2
        """)
        for r in alert_cur.fetchall():
            r = dict(r)
            alerts.append(f"💰 [{r['event_date'][:10]}] **{r['name']}** — "
                        f"${r['amount']:,.0f} ({r.get('round_type', '?')})")
    except Exception:
        pass

    # Agency tenants giving notice (lease expiry < 6 months)
    try:
        alert_cur.execute("""
            SELECT t.tenant_name, t.occupied_sf, t.lease_expiry_date, b.name as building
            FROM agency_tenants t
            JOIN agency_buildings b ON t.building_id = b.id
            WHERE t.lease_expiry_date IS NOT NULL
            AND t.lease_expiry_date BETWEEN date('now') AND date('now', '+6 months')
            AND t.occupied_sf >= 10000
            ORDER BY t.lease_expiry_date ASC
            LIMIT 2
        """)
        for r in alert_cur.fetchall():
            r = dict(r)
            alerts.append(f"🏢 [{r['lease_expiry_date'][:10]}] **{r['tenant_name']}** — "
                        f"{r['occupied_sf']:,} SF at {r['building']} expiring")
    except Exception:
        pass

    alert_conn.close()

    if alerts:
        for a in alerts[:5]:
            st.markdown(a)
    else:
        st.caption("No high alerts this week.")
except Exception:
    st.caption("Alert system initializing.")

# =============================================================================
# SIDEBAR: COST TRACKER
# =============================================================================

with st.sidebar:
    st.subheader("API Spend")
    try:
        spend = get_sidebar_widget_data()
        st.metric("Today", f"${spend['today']:.4f}", f"{spend['calls_today']} calls")
        st.metric("This Week", f"${spend['week']:.4f}")
        st.metric("This Month", f"${spend['month']:.4f}")
    except Exception:
        st.caption("Cost tracking not yet active.")

# =============================================================================
# FOOTER
# =============================================================================

st.markdown("---")
st.caption(f"Relationship Engine — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
