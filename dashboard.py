"""
Dashboard for Relationship Engine
Streamlit-based dashboard with graph intelligence visualizations.
Uses matplotlib for visualizations (plotly unavailable in this environment).

Run with: streamlit run dashboard.py
Or import and call run_dashboard() / generate_report()
"""

import os
import sys
import sqlite3
import json
import io

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from graph_engine import (
    DB_PATH, build_graph, weighted_out_degree_centrality,
    two_hop_leverage, detect_clusters, shortest_weighted_path, compute_all
)


def get_conn():
    return sqlite3.connect(DB_PATH)


# ============================================================
# STANDALONE REPORT (no Streamlit required)
# ============================================================

def generate_report(db_path: str = DB_PATH) -> dict:
    """Generate a full dashboard report as a dict (no Streamlit needed)."""
    results = compute_all(db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Centrality leaderboard — contacts
    cur.execute("""
        SELECT c.first_name || ' ' || c.last_name, c.title, co.name,
               c.centrality_score, c.leverage_score, c.cluster_id
        FROM contacts c LEFT JOIN companies co ON c.company_id = co.id
        ORDER BY c.centrality_score DESC LIMIT 20
    """)
    contact_centrality = [
        {"name": r[0], "title": r[1], "company": r[2],
         "centrality": r[3], "leverage": r[4], "cluster": r[5]}
        for r in cur.fetchall()
    ]

    # Centrality leaderboard — companies
    cur.execute("""
        SELECT name, industry, centrality_score, leverage_score, cluster_id
        FROM companies ORDER BY centrality_score DESC LIMIT 20
    """)
    company_centrality = [
        {"name": r[0], "industry": r[1], "centrality": r[2], "leverage": r[3], "cluster": r[4]}
        for r in cur.fetchall()
    ]

    # Leverage — contacts
    cur.execute("""
        SELECT c.first_name || ' ' || c.last_name, c.title, co.name, c.leverage_score
        FROM contacts c LEFT JOIN companies co ON c.company_id = co.id
        ORDER BY c.leverage_score DESC LIMIT 20
    """)
    contact_leverage = [{"name": r[0], "title": r[1], "company": r[2], "leverage": r[3]} for r in cur.fetchall()]

    # Leverage — companies
    cur.execute("""
        SELECT name, industry, leverage_score FROM companies ORDER BY leverage_score DESC LIMIT 20
    """)
    company_leverage = [{"name": r[0], "industry": r[1], "leverage": r[2]} for r in cur.fetchall()]

    conn.close()

    return {
        "graph_stats": results["graph_stats"],
        "top_centrality": results["top_centrality"],
        "top_leverage": results["top_leverage"],
        "clusters": results["clusters"],
        "contact_centrality_table": contact_centrality,
        "company_centrality_table": company_centrality,
        "contact_leverage_table": contact_leverage,
        "company_leverage_table": company_leverage,
    }


def plot_centrality_bar(report: dict, output_path: str = None) -> str:
    """Generate a bar chart of top 10 centrality scores."""
    top = report["top_centrality"][:10]
    names = [e["name"] for e in top]
    scores = [e["score"] for e in top]
    colors = ["steelblue" if e["type"] == "contact" else "coral" for e in top]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.barh(range(len(names)), scores, color=colors)
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names)
    ax.invert_yaxis()
    ax.set_xlabel("Weighted Out-Degree Centrality")
    ax.set_title("Top 10 Centrality Scores")
    ax.legend(handles=[
        mpatches.Patch(color="steelblue", label="Contact"),
        mpatches.Patch(color="coral", label="Company")
    ])
    plt.tight_layout()

    path = output_path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "centrality_chart.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_cluster_network(db_path: str = DB_PATH, output_path: str = None) -> str:
    """Generate a network visualization colored by cluster."""
    G = build_graph(db_path)
    clusters = detect_clusters(G)
    nodes = G.all_nodes()

    if not nodes:
        return ""

    # Simple force-directed layout (spring embedding)
    np.random.seed(42)
    pos = {n: np.random.randn(2) for n in nodes}

    # Iterative spring layout
    for _ in range(50):
        forces = {n: np.zeros(2) for n in nodes}
        # Repulsion
        for i, u in enumerate(nodes):
            for j, v in enumerate(nodes):
                if i >= j:
                    continue
                diff = pos[u] - pos[v]
                dist = max(np.linalg.norm(diff), 0.01)
                force = diff / (dist ** 2) * 0.5
                forces[u] += force
                forces[v] -= force
        # Attraction along edges
        for src in nodes:
            for _, tgt, d in G.out_edges(src):
                diff = pos[tgt] - pos[src]
                dist = np.linalg.norm(diff)
                force = diff * dist * 0.01
                forces[src] += force
                forces[tgt] -= force
        # Apply
        for n in nodes:
            pos[n] += forces[n] * 0.1

    # Plot
    cmap = plt.cm.Set3
    unique_clusters = sorted(set(clusters.values()))
    n_clusters = max(len(unique_clusters), 1)

    fig, ax = plt.subplots(figsize=(14, 10))

    # Draw edges
    for src in nodes:
        for _, tgt, d in G.out_edges(src):
            x = [pos[src][0], pos[tgt][0]]
            y = [pos[src][1], pos[tgt][1]]
            ax.plot(x, y, "gray", alpha=0.3, linewidth=0.5)

    # Draw nodes
    for node in nodes:
        cid = clusters.get(node, 0)
        color = cmap(cid / n_clusters)
        entity_type = G.nodes[node].get("entity_type", "unknown")
        marker = "o" if entity_type == "contact" else "s"
        ax.scatter(pos[node][0], pos[node][1], c=[color], s=100, marker=marker, edgecolors="black", linewidth=0.5, zorder=5)
        ax.annotate(G.nodes[node].get("name", node), pos[node],
                    fontsize=6, ha="center", va="bottom", xytext=(0, 5), textcoords="offset points")

    ax.set_title("Relationship Network (colored by cluster)")
    ax.legend(handles=[
        mpatches.Patch(color=cmap(i / n_clusters), label=f"Cluster {i}")
        for i in range(n_clusters)
    ], loc="upper left", fontsize=8)
    ax.axis("off")
    plt.tight_layout()

    path = output_path or os.path.join(os.path.dirname(os.path.abspath(__file__)), "network_chart.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


# ============================================================
# STREAMLIT DASHBOARD
# ============================================================

def run_dashboard():
    if not HAS_STREAMLIT:
        print("Streamlit not available. Use generate_report() for standalone output.")
        print("Or install streamlit and run: streamlit run dashboard.py")
        return

    st.set_page_config(page_title="Relationship Engine", layout="wide")
    st.title("Relationship Engine Dashboard")

    if st.sidebar.button("Recompute Graph Scores"):
        with st.spinner("Running graph computations..."):
            results = compute_all()
            st.sidebar.success(
                f"Done. {results['graph_stats']['nodes']} nodes, "
                f"{results['graph_stats']['edges']} edges, "
                f"{results['graph_stats']['clusters']} clusters."
            )

    tabs = st.tabs(["Overview", "Centrality Leaderboard", "2-Hop Leverage",
                     "Clusters", "Path Finder", "Recompute Log"])

    # OVERVIEW
    with tabs[0]:
        st.header("Overview")
        conn = get_conn()
        cur = conn.cursor()
        col1, col2, col3, col4 = st.columns(4)
        for col, query, label in [
            (col1, "SELECT count(*) FROM contacts", "Contacts"),
            (col2, "SELECT count(*) FROM companies", "Companies"),
            (col3, "SELECT count(*) FROM relationships", "Relationships"),
            (col4, "SELECT count(*) FROM deals", "Deals"),
        ]:
            cur.execute(query)
            col.metric(label, cur.fetchone()[0])
        conn.close()

    # CENTRALITY
    with tabs[1]:
        st.header("Centrality Leaderboard")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT c.first_name || ' ' || c.last_name, c.title, co.name,
                   c.centrality_score, c.leverage_score, c.cluster_id
            FROM contacts c LEFT JOIN companies co ON c.company_id = co.id
            ORDER BY c.centrality_score DESC LIMIT 20
        """)
        rows = cur.fetchall()
        if rows:
            st.table([{"Rank": i+1, "Name": r[0], "Title": r[1] or "", "Company": r[2] or "",
                        "Centrality": round(r[3], 4), "Leverage": round(r[4], 4)} for i, r in enumerate(rows)])
            # Bar chart
            fig, ax = plt.subplots(figsize=(10, 5))
            names = [r[0] for r in rows[:10]]
            scores = [r[3] for r in rows[:10]]
            ax.barh(range(len(names)), scores, color="steelblue")
            ax.set_yticks(range(len(names)))
            ax.set_yticklabels(names)
            ax.invert_yaxis()
            ax.set_xlabel("Centrality Score")
            ax.set_title("Top 10 Contacts by Centrality")
            plt.tight_layout()
            st.pyplot(fig)
            plt.close(fig)
        conn.close()

    # 2-HOP LEVERAGE
    with tabs[2]:
        st.header("2-Hop Leverage Rankings")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT c.first_name || ' ' || c.last_name, c.title, co.name, c.leverage_score
            FROM contacts c LEFT JOIN companies co ON c.company_id = co.id
            ORDER BY c.leverage_score DESC LIMIT 20
        """)
        rows = cur.fetchall()
        if rows:
            st.table([{"Rank": i+1, "Name": r[0], "Title": r[1] or "", "Company": r[2] or "",
                        "Leverage": round(r[3], 4)} for i, r in enumerate(rows)])
        conn.close()

    # CLUSTERS
    with tabs[3]:
        st.header("Cluster Visualization")
        G = build_graph()
        clusters = detect_clusters(G)
        cluster_groups = {}
        for node, cid in clusters.items():
            if cid not in cluster_groups:
                cluster_groups[cid] = []
            cluster_groups[cid].append(G.nodes[node].get("name", node))
        for cid in sorted(cluster_groups.keys()):
            with st.expander(f"Cluster {cid} ({len(cluster_groups[cid])} members)"):
                for name in sorted(cluster_groups[cid]):
                    st.write(f"- {name}")

        # Network chart
        chart_path = plot_cluster_network()
        if chart_path and os.path.exists(chart_path):
            st.image(chart_path)

    # PATH FINDER
    with tabs[4]:
        st.header("Path Finder")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT 'contact' AS type, id, first_name || ' ' || last_name AS name FROM contacts
            UNION ALL SELECT 'company', id, name FROM companies ORDER BY name
        """)
        entities = cur.fetchall()
        conn.close()
        entity_options = {f"{r[0]}:{r[1]} — {r[2]}": (r[0], r[1]) for r in entities}
        col1, col2 = st.columns(2)
        with col1:
            src_key = st.selectbox("Source", list(entity_options.keys()), key="src")
        with col2:
            tgt_key = st.selectbox("Target", list(entity_options.keys()), key="tgt")
        if st.button("Find Path"):
            src_t, src_id = entity_options[src_key]
            tgt_t, tgt_id = entity_options[tgt_key]
            result = shortest_weighted_path(build_graph(), src_t, src_id, tgt_t, tgt_id)
            if "error" in result:
                st.error(result["error"])
            else:
                st.success(f"{result['hops']} hops, total weight: {result['total_weight']:.4f}")
                for step in result["path_details"]:
                    arrow = ""
                    if "edge_to_next" in step:
                        e = step["edge_to_next"]
                        arrow = f" -> ({e['relationship_type']}, w={e['weight']:.3f})"
                    st.write(f"**{step['name']}** ({step['type']}){arrow}")

    # RECOMPUTE LOG
    with tabs[5]:
        st.header("Recompute Log")
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT run_timestamp, nodes_computed, edges_computed, clusters_found,
                   top_centrality_node, top_leverage_node, duration_seconds, status
            FROM recompute_log ORDER BY run_timestamp DESC LIMIT 20
        """)
        rows = cur.fetchall()
        if rows:
            st.table([{"Timestamp": r[0], "Nodes": r[1], "Edges": r[2],
                        "Top Centrality": r[4], "Duration": r[6], "Status": r[7]}
                       for r in rows])
        else:
            st.info("No runs logged yet.")
        conn.close()


if __name__ == "__main__":
    if HAS_STREAMLIT:
        run_dashboard()
    else:
        print("Generating standalone report...")
        report = generate_report()
        print(json.dumps(report, indent=2))
        chart = plot_centrality_bar(report)
        print(f"Centrality chart saved to: {chart}")
        net = plot_cluster_network()
        print(f"Network chart saved to: {net}")
