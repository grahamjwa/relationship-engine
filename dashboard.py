"""
Dashboard for Relationship Engine
Streamlit-based dashboard with graph intelligence visualizations.

Run with: streamlit run dashboard.py
"""

import os
import sys
import sqlite3
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from graph_engine import (
    get_db_path, build_graph, compute_centrality, compute_two_hop_leverage,
    detect_clusters, find_shortest_path, get_top_centrality, get_top_leverage
)


def get_conn():
    return sqlite3.connect(get_db_path())


def get_stats():
    """Get record counts for all tables."""
    conn = get_conn()
    cur = conn.cursor()
    tables = ['companies', 'contacts', 'relationships', 'buildings', 'leases', 
              'deals', 'outreach_log', 'funding_events', 'hiring_signals']
    stats = {}
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cur.fetchone()[0]
        except:
            stats[table] = 0
    conn.close()
    return stats


def get_upcoming_expirations():
    """Get upcoming lease expirations."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT l.id, c.name as company_name, b.address as building_address,
                   l.expiration_date, l.square_footage
            FROM leases l
            JOIN companies c ON l.tenant_id = c.id
            JOIN buildings b ON l.building_id = b.id
            WHERE l.expiration_date >= date('now')
            ORDER BY l.expiration_date ASC
            LIMIT 20
        """)
        return [dict(row) for row in cur.fetchall()]
    except:
        return []
    finally:
        conn.close()


def get_recent_funding():
    """Get recent funding events."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT f.id, c.name as company_name, f.round_type, f.amount,
                   f.date, f.lead_investor, f.source_url
            FROM funding_events f
            JOIN companies c ON f.company_id = c.id
            ORDER BY f.date DESC
            LIMIT 20
        """)
        return [dict(row) for row in cur.fetchall()]
    except:
        return []
    finally:
        conn.close()


def get_hiring_signals():
    """Get high-value hiring signals."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT h.id, c.name as company_name, h.signal_type, h.description,
                   h.relevance, h.date, h.source_url
            FROM hiring_signals h
            JOIN companies c ON h.company_id = c.id
            ORDER BY h.date DESC
            LIMIT 20
        """)
        return [dict(row) for row in cur.fetchall()]
    except:
        return []
    finally:
        conn.close()


def get_untouched_targets():
    """Get high-value targets with no outreach."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT c.id, c.name, c.status, c.sector
            FROM companies c
            WHERE c.status IN ('high_growth_target', 'prospect')
            AND c.id NOT IN (
                SELECT DISTINCT company_id FROM outreach_log WHERE company_id IS NOT NULL
            )
            ORDER BY c.name
        """)
        return [dict(row) for row in cur.fetchall()]
    except:
        return []
    finally:
        conn.close()


def get_overdue_followups():
    """Get overdue follow-ups."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT o.id, c.name as company_name, ct.first_name || ' ' || ct.last_name as contact_name,
                   o.outreach_type, o.date, o.follow_up_date, o.notes
            FROM outreach_log o
            LEFT JOIN companies c ON o.company_id = c.id
            LEFT JOIN contacts ct ON o.contact_id = ct.id
            WHERE o.follow_up_date < date('now')
            AND o.status != 'completed'
            ORDER BY o.follow_up_date ASC
        """)
        return [dict(row) for row in cur.fetchall()]
    except:
        return []
    finally:
        conn.close()


def get_all_contacts():
    """Get all contacts for path finder dropdown."""
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT id, first_name || ' ' || last_name as name FROM contacts ORDER BY last_name")
    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results


def run_streamlit_dashboard():
    """Run the Streamlit dashboard."""
    st.set_page_config(page_title="Relationship Engine", layout="wide")
    st.title("🔗 Relationship Engine Dashboard")
    
    # Sidebar navigation
    page = st.sidebar.selectbox(
        "Navigate",
        ["Overview", "Centrality Leaderboard", "2-Hop Leverage", "Clusters", 
         "Path Finder", "Upcoming Expirations", "Funding Events", 
         "Hiring Signals", "Untouched Targets", "Overdue Follow-ups"]
    )
    
    if page == "Overview":
        st.header("📊 Database Overview")
        stats = get_stats()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Companies", stats.get('companies', 0))
            st.metric("Contacts", stats.get('contacts', 0))
            st.metric("Relationships", stats.get('relationships', 0))
        with col2:
            st.metric("Buildings", stats.get('buildings', 0))
            st.metric("Leases", stats.get('leases', 0))
            st.metric("Deals", stats.get('deals', 0))
        with col3:
            st.metric("Outreach Logs", stats.get('outreach_log', 0))
            st.metric("Funding Events", stats.get('funding_events', 0))
            st.metric("Hiring Signals", stats.get('hiring_signals', 0))
        
        # Quick stats
        st.subheader("Top 5 by Centrality")
        top_cent = get_top_centrality(5)
        for i, row in enumerate(top_cent, 1):
            score = row.get('centrality_score', 0) or 0
            st.write(f"{i}. **{row['name']}** ({row['type']}): {score:.2f}")
    
    elif page == "Centrality Leaderboard":
        st.header("🏆 Centrality Leaderboard")
        st.write("Ranked by weighted out-degree centrality (sum of outgoing relationship weights)")
        
        top = get_top_centrality(20)
        if top:
            # Create bar chart
            names = [r['name'][:20] for r in top]
            scores = [r.get('centrality_score', 0) or 0 for r in top]
            
            fig, ax = plt.subplots(figsize=(10, 8))
            y_pos = np.arange(len(names))
            ax.barh(y_pos, scores, color='steelblue')
            ax.set_yticks(y_pos)
            ax.set_yticklabels(names)
            ax.invert_yaxis()
            ax.set_xlabel('Centrality Score')
            ax.set_title('Top 20 by Centrality')
            plt.tight_layout()
            st.pyplot(fig)
            
            # Table view
            st.dataframe(top)
        else:
            st.info("No centrality scores computed yet. Run graph_engine.compute_all() first.")
    
    elif page == "2-Hop Leverage":
        st.header("🔗 2-Hop Leverage Rankings")
        st.write("Ranked by indirect reach (value accessible through your connections' connections)")
        
        top = get_top_leverage(20)
        if top:
            names = [r['name'][:20] for r in top]
            scores = [r.get('leverage_score', 0) or 0 for r in top]
            
            fig, ax = plt.subplots(figsize=(10, 8))
            y_pos = np.arange(len(names))
            ax.barh(y_pos, scores, color='darkgreen')
            ax.set_yticks(y_pos)
            ax.set_yticklabels(names)
            ax.invert_yaxis()
            ax.set_xlabel('Leverage Score')
            ax.set_title('Top 20 by 2-Hop Leverage')
            plt.tight_layout()
            st.pyplot(fig)
            
            st.dataframe(top)
        else:
            st.info("No leverage scores computed yet. Run graph_engine.compute_all() first.")
    
    elif page == "Clusters":
        st.header("🔵 Network Clusters")
        st.write("Communities detected in the relationship graph")
        
        conn = get_conn()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Get cluster distribution
        cur.execute("""
            SELECT cluster_id, COUNT(*) as count
            FROM (
                SELECT cluster_id FROM contacts WHERE cluster_id IS NOT NULL
                UNION ALL
                SELECT cluster_id FROM companies WHERE cluster_id IS NOT NULL
            )
            GROUP BY cluster_id
            ORDER BY count DESC
        """)
        clusters = [dict(row) for row in cur.fetchall()]
        conn.close()
        
        if clusters:
            st.write(f"**{len(clusters)} clusters detected**")
            
            # Show cluster sizes
            fig, ax = plt.subplots(figsize=(10, 6))
            cluster_ids = [f"C{c['cluster_id']}" for c in clusters[:15]]
            counts = [c['count'] for c in clusters[:15]]
            ax.bar(cluster_ids, counts, color='purple')
            ax.set_xlabel('Cluster ID')
            ax.set_ylabel('Members')
            ax.set_title('Cluster Sizes (Top 15)')
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.info("No clusters computed yet. Run graph_engine.compute_all() first.")
    
    elif page == "Path Finder":
        st.header("🛤️ Path Finder")
        st.write("Find the shortest relationship path between two contacts")
        
        contacts = get_all_contacts()
        contact_options = {f"{c['name']} (ID: {c['id']})": c['id'] for c in contacts}
        
        col1, col2 = st.columns(2)
        with col1:
            source_name = st.selectbox("From:", list(contact_options.keys()))
        with col2:
            target_name = st.selectbox("To:", list(contact_options.keys()))
        
        if st.button("Find Path"):
            source_id = contact_options[source_name]
            target_id = contact_options[target_name]
            
            G = build_graph()
            path, weight = find_shortest_path(G, f"contact_{source_id}", f"contact_{target_id}")
            
            if path:
                st.success(f"Path found! Total weight: {weight:.2f}")
                
                # Display path
                conn = get_conn()
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                
                path_names = []
                for node in path:
                    if node.startswith("contact_"):
                        cid = int(node.split("_")[1])
                        cur.execute("SELECT first_name || ' ' || last_name as name FROM contacts WHERE id = ?", (cid,))
                        row = cur.fetchone()
                        path_names.append(row['name'] if row else node)
                    elif node.startswith("company_"):
                        cid = int(node.split("_")[1])
                        cur.execute("SELECT name FROM companies WHERE id = ?", (cid,))
                        row = cur.fetchone()
                        path_names.append(row['name'] if row else node)
                    else:
                        path_names.append(node)
                conn.close()
                
                st.write(" → ".join(path_names))
            else:
                st.warning("No path found between these contacts.")
    
    elif page == "Upcoming Expirations":
        st.header("📅 Upcoming Lease Expirations")
        expirations = get_upcoming_expirations()
        if expirations:
            st.dataframe(expirations)
        else:
            st.info("No upcoming lease expirations found.")
    
    elif page == "Funding Events":
        st.header("💰 Recent Funding Events")
        funding = get_recent_funding()
        if funding:
            st.dataframe(funding)
        else:
            st.info("No funding events recorded yet.")
    
    elif page == "Hiring Signals":
        st.header("👔 Hiring Signals")
        signals = get_hiring_signals()
        if signals:
            st.dataframe(signals)
        else:
            st.info("No hiring signals recorded yet.")
    
    elif page == "Untouched Targets":
        st.header("🎯 Untouched Targets")
        st.write("High-value prospects with no outreach logged")
        targets = get_untouched_targets()
        if targets:
            st.dataframe(targets)
        else:
            st.info("All targets have been contacted!")
    
    elif page == "Overdue Follow-ups":
        st.header("⚠️ Overdue Follow-ups")
        overdue = get_overdue_followups()
        if overdue:
            st.dataframe(overdue)
        else:
            st.success("No overdue follow-ups!")


if __name__ == "__main__":
    if HAS_STREAMLIT:
        run_streamlit_dashboard()
    else:
        print("Streamlit not installed. Run: pip install streamlit")
