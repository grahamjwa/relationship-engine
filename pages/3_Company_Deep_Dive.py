"""
Company Deep Dive
Single-company profile: scores, contacts, outreach history, signals, warm paths, deals.
"""

import os
import sys
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

import streamlit as st
from core.graph_engine import get_db_path, build_graph, find_shortest_path
from core.path_finder import find_warm_intros
from scrapers.executive_scanner import get_executives, get_recent_changes, classify_title

st.set_page_config(page_title="Company Deep Dive", page_icon="🔍", layout="wide")

def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# DATA
# =============================================================================

def get_company_list():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, status FROM companies ORDER BY name")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_company_detail(company_id):
    conn = get_conn()
    cur = conn.cursor()

    # Company info
    cur.execute("SELECT * FROM companies WHERE id = ?", (company_id,))
    company = dict(cur.fetchone())

    # Contacts
    cur.execute("""
        SELECT id, first_name, last_name, title, role_level, email,
               COALESCE(priority_score, 0) as priority_score,
               COALESCE(centrality_score, 0) as centrality_score
        FROM contacts WHERE company_id = ?
        ORDER BY priority_score DESC
    """, (company_id,))
    contacts = [dict(r) for r in cur.fetchall()]

    # Outreach history
    cur.execute("""
        SELECT o.outreach_date, o.outreach_type, o.outcome, o.notes,
               COALESCE(ct.first_name || ' ' || ct.last_name, 'Company-level') as contact_name
        FROM outreach_log o
        LEFT JOIN contacts ct ON o.target_contact_id = ct.id
        WHERE o.target_company_id = ?
        ORDER BY o.outreach_date DESC
        LIMIT 25
    """, (company_id,))
    outreach = [dict(r) for r in cur.fetchall()]

    # Funding events
    cur.execute("""
        SELECT event_date, round_type, amount, lead_investor, source_url
        FROM funding_events WHERE company_id = ?
        ORDER BY event_date DESC
        LIMIT 10
    """, (company_id,))
    funding = [dict(r) for r in cur.fetchall()]

    # Hiring signals
    cur.execute("""
        SELECT signal_date, signal_type, details, relevance, source_url
        FROM hiring_signals WHERE company_id = ?
        ORDER BY signal_date DESC
        LIMIT 15
    """, (company_id,))
    hiring = [dict(r) for r in cur.fetchall()]

    # Deals
    cur.execute("""
        SELECT deal_type, status, square_feet, deal_value, our_role, started_date, closed_date
        FROM deals WHERE company_id = ?
        ORDER BY COALESCE(started_date, closed_date) DESC
    """, (company_id,))
    deals = [dict(r) for r in cur.fetchall()]

    # Leases
    cur.execute("""
        SELECT l.lease_expiry, l.square_feet, l.rent_psf,
               b.address, b.submarket
        FROM leases l
        LEFT JOIN buildings b ON l.building_id = b.id
        WHERE l.company_id = ?
        ORDER BY l.lease_expiry ASC
    """, (company_id,))
    leases = [dict(r) for r in cur.fetchall()]

    # Executives
    cur.execute("""
        SELECT person_name, title, title_category, priority, start_date,
               previous_company, previous_title, linkedin_url, last_verified
        FROM executives WHERE company_id = ?
        ORDER BY
            CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
            title_category ASC
    """, (company_id,))
    executives = [dict(r) for r in cur.fetchall()]

    # Executive changes (last 90 days)
    cur.execute("""
        SELECT person_name, old_title, new_title, old_company, new_company,
               change_type, priority, effective_date, source_url, headline
        FROM executive_changes WHERE company_id = ?
        AND created_at >= datetime('now', '-90 days')
        ORDER BY effective_date DESC
        LIMIT 10
    """, (company_id,))
    exec_changes = [dict(r) for r in cur.fetchall()]

    conn.close()

    return {
        "company": company,
        "contacts": contacts,
        "outreach": outreach,
        "funding": funding,
        "hiring": hiring,
        "deals": deals,
        "leases": leases,
        "executives": executives,
        "exec_changes": exec_changes,
    }


# =============================================================================
# UI
# =============================================================================

st.title("Company Deep Dive")

# Company selector
companies = get_company_list()
company_names = {r["name"]: r["id"] for r in companies}

selected = st.selectbox(
    "Select company",
    list(company_names.keys()),
    index=0
)

if selected:
    company_id = company_names[selected]
    data = get_company_detail(company_id)
    co = data["company"]

    # Header
    st.markdown("---")

    h1, h2, h3, h4, h5, h6 = st.columns(6)
    h1.metric("Status", co.get("status", "—").replace("_", " ").title())
    h2.metric("Opportunity Score", f"{co.get('opportunity_score', 0) or 0:.1f}")
    h3.metric("Contacts", len(data["contacts"]))
    h4.metric("Outreach Events", len(data["outreach"]))
    h5.metric("Active Deals", sum(1 for d in data["deals"] if d["status"] not in ("lost", "dead", "closed", None)))

    # SPOC status display
    spoc_display = (co.get("spoc_status") or "available").replace("_", " ").title()
    spoc_icon = {"spoced": "🔒", "available": "✅"}.get(co.get("spoc_status", ""), "🟡")
    h6.metric("SPOC", f"{spoc_icon} {spoc_display}")

    # SPOC management
    with st.expander("SPOC / Competition Status"):
        spoc_options = ["available", "spoced", "follow_up_1mo", "follow_up_6mo", "follow_up_1yr"]
        current_spoc = co.get("spoc_status") or "available"
        current_idx = spoc_options.index(current_spoc) if current_spoc in spoc_options else 0

        new_spoc = st.selectbox(
            "SPOC Status", spoc_options,
            index=current_idx,
            format_func=lambda x: {
                "available": "Available",
                "spoced": "SPOCed (locked out)",
                "follow_up_1mo": "Follow up in 1 month",
                "follow_up_6mo": "Follow up in 6 months",
                "follow_up_1yr": "Follow up in 1 year",
            }.get(x, x),
            key="spoc_status_dd"
        )
        broker_name = st.text_input("Broker Name", value=co.get("spoc_broker") or "",
                                     key="spoc_broker_input")

        if st.button("Update SPOC Status", key="spoc_update_btn"):
            sconn = get_conn()
            scur = sconn.cursor()
            from datetime import timedelta
            fu_date = None
            if new_spoc == "follow_up_1mo":
                fu_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
            elif new_spoc == "follow_up_6mo":
                fu_date = (datetime.now() + timedelta(days=180)).strftime('%Y-%m-%d')
            elif new_spoc == "follow_up_1yr":
                fu_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
            elif new_spoc == "spoced":
                fu_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')

            scur.execute("""
                UPDATE companies SET spoc_status = ?, spoc_broker = ?,
                spoc_follow_up_date = ?, spoc_updated_at = datetime('now')
                WHERE id = ?
            """, (new_spoc if new_spoc != "available" else None,
                  broker_name or None, fu_date, company_id))
            sconn.commit()
            sconn.close()
            st.success(f"SPOC updated to {new_spoc}" +
                       (f" — follow up {fu_date}" if fu_date else ""))
            st.rerun()

    st.markdown("---")

    # Two-column layout: left = intel, right = relationships
    left, right = st.columns([3, 2])

    with left:
        # Funding
        st.subheader("Funding History")
        if data["funding"]:
            for f in data["funding"]:
                amount_str = f"${f['amount']:,.0f}" if f["amount"] else "undisclosed"
                investor = f["lead_investor"] or "undisclosed"
                st.markdown(f"**{f['event_date']}** — {f['round_type'] or 'Round'}: {amount_str} (Lead: {investor})")
                if f.get("source_url"):
                    st.caption(f"[Source]({f['source_url']})")
        else:
            st.caption("No funding events recorded.")

        st.markdown("")

        # Hiring Signals
        st.subheader("Hiring Signals")
        if data["hiring"]:
            for h in data["hiring"]:
                relevance_icon = {"high": "🔴", "medium": "🟡", "low": "⚪"}.get(h["relevance"], "⚪")
                st.markdown(f"{relevance_icon} **{h['signal_date']}** — {h['signal_type'].replace('_', ' ').title()}")
                if h.get("details"):
                    st.caption(h["details"])
        else:
            st.caption("No hiring signals recorded.")

        st.markdown("")

        # Leases
        st.subheader("Leases")
        if data["leases"]:
            for l in data["leases"]:
                sf = f"{l['square_feet']:,.0f} SF" if l["square_feet"] else "? SF"
                addr = l.get("address") or "unknown"
                rent = f"${l['rent_psf']:.2f}/SF" if l.get("rent_psf") else ""
                expiry = l["lease_expiry"] or "unknown"
                st.markdown(f"**{addr}** — {sf} {rent}")
                st.caption(f"Expires: {expiry}" + (f" · {l['submarket']}" if l.get("submarket") else ""))
        else:
            st.caption("No leases recorded.")

        st.markdown("")

        # Deals
        st.subheader("Deals")
        if data["deals"]:
            for d in data["deals"]:
                sf = f"{d['square_feet']:,.0f} SF" if d.get("square_feet") else ""
                val = f"${d['deal_value']:,.0f}" if d.get("deal_value") else ""
                role = d.get("our_role") or ""
                status_icon = {
                    "active": "🟢", "in_progress": "🟢", "closed": "✅",
                    "won": "✅", "lost": "❌", "dead": "⚫"
                }.get(d["status"], "🔵")
                st.markdown(f"{status_icon} **{d['deal_type']}** — {d['status']} {sf} {val}")
                if role:
                    st.caption(f"Our role: {role}")
        else:
            st.caption("No deals recorded.")

        # Executives
        st.subheader("Executives")
        if data["executives"]:
            import pandas as pd
            exec_rows = []
            for ex in data["executives"]:
                pri_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(ex["priority"], "")
                linkedin = f"[link]({ex['linkedin_url']})" if ex.get("linkedin_url") else ""
                exec_rows.append({
                    "": pri_icon,
                    "Name": ex["person_name"],
                    "Title": ex["title"],
                    "Since": ex.get("start_date") or "—",
                    "Previous": ex.get("previous_company") or "—",
                    "LinkedIn": linkedin,
                })
            st.dataframe(exec_rows, use_container_width=True, hide_index=True)
        else:
            st.caption("No executives tracked yet.")

        if data["exec_changes"]:
            st.markdown("**Recent Changes (90d):**")
            for ec in data["exec_changes"]:
                badge = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(ec["priority"], "")
                from_part = f" from {ec['old_company']}" if ec.get("old_company") else ""
                st.markdown(f"{badge} **{ec['person_name']}** → {ec.get('new_title', '?')}{from_part}")
                if ec.get("headline"):
                    st.caption(ec["headline"][:120])
                if ec.get("effective_date"):
                    st.caption(f"Date: {ec['effective_date']}")

        st.markdown("")

    with right:
        # Contacts
        st.subheader("Contacts")
        if data["contacts"]:
            for c in data["contacts"]:
                name = f"{c['first_name']} {c['last_name']}"
                title = c.get("title") or ""
                role = (c.get("role_level") or "").replace("_", " ").title()
                st.markdown(f"**{name}** — {title}")
                st.caption(f"{role} · Priority: {c['priority_score']:.0f} · Centrality: {c['centrality_score']:.2f}")
                if c.get("email"):
                    st.caption(c["email"])
                st.markdown("")
        else:
            st.caption("No contacts recorded.")

        st.markdown("---")

        # Warm Paths
        st.subheader("Warm Intro Paths")
        try:
            intros = find_warm_intros(company_id)
            if intros:
                for intro in intros:
                    path_str = " → ".join(intro.get("path", []))
                    st.markdown(f"**Path:** {path_str}")
                    st.caption(f"Weight: {intro.get('weight', 0):.2f} · Hops: {intro.get('hops', 0)}")
                    st.markdown("")
            else:
                st.caption("No warm paths found. Cold outreach needed.")
        except Exception as e:
            st.caption(f"Path analysis unavailable: {e}")

        st.markdown("---")

        # Outreach Timeline
        st.subheader("Outreach History")
        if data["outreach"]:
            for o in data["outreach"]:
                type_icon = {"email": "📧", "call": "📞", "meeting": "🤝", "linkedin": "💼", "event": "🎫"}.get(o["outreach_type"], "📝")
                outcome_color = {
                    "meeting_booked": "🟢", "meeting_held": "🟢", "responded_positive": "🟢",
                    "pending": "🟡", "no_response": "🟠",
                    "responded_negative": "🔴", "declined": "🔴"
                }.get(o["outcome"], "⚪")

                st.markdown(f"{type_icon} {outcome_color} **{o['outreach_date']}** — {o['outreach_type']} → {o['outcome']}")
                if o.get("notes"):
                    st.caption(o["notes"])
                st.caption(f"Contact: {o['contact_name']}")
                st.markdown("")
        else:
            st.caption("No outreach logged.")
