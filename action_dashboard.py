"""
Action Dashboard for Relationship Engine
Interactive, action-oriented interface for daily BD work.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False

from graph_engine import get_db_path, find_shortest_path, build_graph

# Page config
st.set_page_config(page_title="RE Action Center", layout="wide", initial_sidebar_state="collapsed")

def get_conn():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn

def log_outreach(company_id, contact_id, outreach_type, outcome, notes):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO outreach_log (target_company_id, target_contact_id, outreach_date, outreach_type, outcome, notes)
        VALUES (?, ?, date('now'), ?, ?, ?)
    """, (company_id, contact_id, outreach_type, outcome, notes))
    conn.commit()
    conn.close()

def get_todays_priorities():
    conn = get_conn()
    cur = conn.cursor()
    
    priorities = []
    
    # Recently funded, not contacted
    cur.execute("""
        SELECT c.id, c.name, 'Funded' as reason, f.round_type || ' - $' || COALESCE(CAST(f.amount as TEXT), '?') as detail
        FROM companies c
        JOIN funding_events f ON c.id = f.company_id
        WHERE f.event_date >= date('now', '-14 days')
        AND c.spoc_covered = 0
        AND NOT EXISTS (SELECT 1 FROM outreach_log o WHERE o.target_company_id = c.id AND o.outreach_date >= f.event_date)
        ORDER BY f.amount DESC LIMIT 5
    """)
    for row in cur.fetchall():
        priorities.append(dict(row))
    
    # High hiring activity, not contacted in 30 days
    cur.execute("""
        SELECT c.id, c.name, 'Hiring Spike' as reason, COUNT(h.id) || ' signals' as detail
        FROM companies c
        JOIN hiring_signals h ON c.id = h.company_id
        WHERE h.signal_date >= date('now', '-14 days')
        AND h.relevance IN ('high', 'medium')
        AND c.spoc_covered = 0
        AND NOT EXISTS (SELECT 1 FROM outreach_log o WHERE o.target_company_id = c.id AND o.outreach_date >= date('now', '-30 days'))
        GROUP BY c.id
        HAVING COUNT(h.id) >= 2
        ORDER BY COUNT(h.id) DESC LIMIT 5
    """)
    for row in cur.fetchall():
        priorities.append(dict(row))
    
    # Top opportunity score, never contacted
    cur.execute("""
        SELECT c.id, c.name, 'High Score' as reason, 'Score: ' || CAST(ROUND(c.opportunity_score) as TEXT) as detail
        FROM companies c
        WHERE c.opportunity_score > 30
        AND c.spoc_covered = 0
        AND c.status IN ('high_growth_target', 'prospect')
        AND NOT EXISTS (SELECT 1 FROM outreach_log o WHERE o.target_company_id = c.id)
        ORDER BY c.opportunity_score DESC LIMIT 5
    """)
    for row in cur.fetchall():
        priorities.append(dict(row))
    
    conn.close()
    return priorities

def get_follow_ups_due():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT o.id, c.name as company, o.follow_up_date, o.notes, o.outreach_type
        FROM outreach_log o
        JOIN companies c ON o.target_company_id = c.id
        WHERE o.follow_up_date <= date('now', '+3 days')
        AND o.follow_up_done = 0
        ORDER BY o.follow_up_date ASC
    """)
    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results

def get_warm_paths(target_company_id):
    """Find paths from team to target company contacts."""
    conn = get_conn()
    cur = conn.cursor()
    
    # Get target company contacts
    cur.execute("""
        SELECT id, first_name || ' ' || last_name as name, title
        FROM contacts WHERE company_id = ?
    """, (target_company_id,))
    target_contacts = [dict(row) for row in cur.fetchall()]
    
    # Get team contacts
    cur.execute("""
        SELECT id, first_name || ' ' || last_name as name
        FROM contacts WHERE role_level = 'team'
    """)
    team = [dict(row) for row in cur.fetchall()]
    
    # Check for former employee connections
    cur.execute("""
        SELECT c.first_name || ' ' || c.last_name as name, c.title, c.previous_companies, comp.name as current_company
        FROM contacts c
        JOIN companies comp ON c.company_id = comp.id
        WHERE comp.id = ?
        AND c.previous_companies IS NOT NULL
    """, (target_company_id,))
    former_employees = [dict(row) for row in cur.fetchall()]
    
    conn.close()
    
    # Build graph and find paths
    graph = build_graph()
    paths = []
    
    for tc in target_contacts:
        for tm in team:
            path, weight = find_shortest_path(graph, f"contact_{tm['id']}", f"contact_{tc['id']}")
            if path and len(path) <= 4:
                paths.append({
                    'from': tm['name'],
                    'to': tc['name'],
                    'to_title': tc.get('title', ''),
                    'hops': len(path) - 1,
                    'path': path
                })
    
    return paths, former_employees

def get_executive_moves():
    """Find contacts at targets who came from clients."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            c.first_name || ' ' || c.last_name as name,
            c.title,
            comp.name as current_company,
            c.previous_companies,
            client.name as from_client
        FROM contacts c
        JOIN companies comp ON c.company_id = comp.id
        JOIN companies client ON c.previous_companies LIKE '%' || client.name || '%'
        WHERE comp.status IN ('high_growth_target', 'prospect')
        AND client.status = 'active_client'
    """)
    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results

def main():
    st.title("🎯 Action Center")
    st.caption(f"Today: {datetime.now().strftime('%A, %B %d')}")
    
    # Top row: Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    conn = get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM companies WHERE status IN ('high_growth_target', 'prospect')")
    targets = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM outreach_log WHERE outreach_date >= date('now', '-7 days')")
    outreach_week = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM outreach_log WHERE follow_up_date <= date('now') AND follow_up_done = 0")
    overdue = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM funding_events WHERE event_date >= date('now', '-7 days')")
    funding_week = cur.fetchone()[0]
    
    conn.close()
    
    with col1:
        st.metric("Active Targets", targets)
    with col2:
        st.metric("Outreach This Week", outreach_week)
    with col3:
        st.metric("Overdue Follow-ups", overdue, delta_color="inverse")
    with col4:
        st.metric("New Funding (7d)", funding_week)
    
    st.divider()
    
    # Main content: Two columns
    left, right = st.columns([2, 1])
    
    with left:
        # TODAY'S PRIORITIES
        st.subheader("🔥 Today's Priorities")
        priorities = get_todays_priorities()
        
        if priorities:
            for i, p in enumerate(priorities):
                with st.container():
                    col1, col2, col3 = st.columns([3, 2, 1])
                    with col1:
                        st.write(f"**{p['name']}**")
                    with col2:
                        st.caption(f"{p['reason']}: {p['detail']}")
                    with col3:
                        if st.button("Log Touch", key=f"log_{p['id']}"):
                            st.session_state[f"show_log_{p['id']}"] = True
                    
                    # Show log form if button clicked
                    if st.session_state.get(f"show_log_{p['id']}"):
                        with st.form(key=f"form_{p['id']}"):
                            otype = st.selectbox("Type", ["email", "call", "linkedin", "meeting"], key=f"type_{p['id']}")
                            outcome = st.selectbox("Outcome", ["pending", "no_response", "responded_positive", "meeting_booked"], key=f"out_{p['id']}")
                            notes = st.text_input("Notes", key=f"notes_{p['id']}")
                            if st.form_submit_button("Save"):
                                log_outreach(p['id'], None, otype, outcome, notes)
                                st.success(f"Logged {otype} to {p['name']}")
                                st.session_state[f"show_log_{p['id']}"] = False
                                st.rerun()
        else:
            st.success("No urgent priorities — you're caught up!")
        
        st.divider()
        
        # FOLLOW-UPS DUE
        st.subheader("📅 Follow-ups Due")
        follow_ups = get_follow_ups_due()
        
        if follow_ups:
            for f in follow_ups:
                col1, col2, col3 = st.columns([2, 2, 1])
                with col1:
                    st.write(f"**{f['company']}**")
                with col2:
                    st.caption(f"Due: {f['follow_up_date']} | {f['notes'][:30] if f['notes'] else 'No notes'}...")
                with col3:
                    if st.button("Done", key=f"done_{f['id']}"):
                        conn = get_conn()
                        conn.execute("UPDATE outreach_log SET follow_up_done = 1 WHERE id = ?", (f['id'],))
                        conn.commit()
                        conn.close()
                        st.rerun()
        else:
            st.success("No follow-ups due!")
        
        st.divider()
        
        # EXECUTIVE MOVES
        st.subheader("🔄 Executive Moves (Client → Target)")
        moves = get_executive_moves()
        if moves:
            for m in moves:
                st.write(f"**{m['name']}** ({m['title']}) at **{m['current_company']}** — from {m['from_client']}")
        else:
            st.info("No tracked executive moves yet. Add previous_companies to contacts.")
    
    with right:
        # QUICK ACTIONS
        st.subheader("⚡ Quick Actions")
        
        # Quick log
        with st.expander("Log Outreach"):
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM companies WHERE status IN ('active_client', 'high_growth_target', 'prospect') ORDER BY name")
            companies = {row['name']: row['id'] for row in cur.fetchall()}
            conn.close()
            
            company = st.selectbox("Company", list(companies.keys()))
            otype = st.selectbox("Type", ["email", "call", "linkedin", "meeting", "intro_request"])
            outcome = st.selectbox("Outcome", ["pending", "no_response", "responded_positive", "meeting_booked", "declined"])
            notes = st.text_area("Notes", height=60)
            follow_up = st.checkbox("Set follow-up (7 days)")
            
            if st.button("Log It"):
                conn = get_conn()
                cur = conn.cursor()
                if follow_up:
                    cur.execute("""
                        INSERT INTO outreach_log (target_company_id, outreach_date, outreach_type, outcome, notes, follow_up_date)
                        VALUES (?, date('now'), ?, ?, ?, date('now', '+7 days'))
                    """, (companies[company], otype, outcome, notes))
                else:
                    cur.execute("""
                        INSERT INTO outreach_log (target_company_id, outreach_date, outreach_type, outcome, notes)
                        VALUES (?, date('now'), ?, ?, ?)
                    """, (companies[company], otype, outcome, notes))
                conn.commit()
                conn.close()
                st.success(f"Logged {otype} to {company}")
        
        # Path finder
        with st.expander("Find Path to Company"):
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM companies WHERE status IN ('high_growth_target', 'prospect') ORDER BY name")
            targets = {row['name']: row['id'] for row in cur.fetchall()}
            conn.close()
            
            target = st.selectbox("Target Company", list(targets.keys()), key="path_target")
            
            if st.button("Find Paths"):
                paths, former = get_warm_paths(targets[target])
                
                if former:
                    st.write("**Former Client Employees:**")
                    for f in former:
                        st.write(f"• {f['name']} ({f['title']}) — from {f['previous_companies']}")
                
                if paths:
                    st.write("**Relationship Paths:**")
                    for p in paths:
                        st.write(f"• {p['from']} → {p['to']} ({p['to_title']}) [{p['hops']} hops]")
                
                if not paths and not former:
                    st.warning("No paths found. Build more relationships!")
        
        # Recent activity
        st.subheader("📊 Recent Activity")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT c.name, o.outreach_type, o.outcome, o.outreach_date
            FROM outreach_log o
            JOIN companies c ON o.target_company_id = c.id
            ORDER BY o.outreach_date DESC LIMIT 5
        """)
        recent = cur.fetchall()
        conn.close()
        
        for r in recent:
            st.caption(f"{r['outreach_date']}: {r['outreach_type']} → {r['name']} ({r['outcome']})")


if __name__ == "__main__":
    if HAS_STREAMLIT:
        main()
    else:
        print("Streamlit required")
