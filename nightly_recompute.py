"""
Nightly Recompute Script for Relationship Engine
Runs graph computations, updates scores, logs results, posts to Discord.
"""

import os
import sys
import time
import sqlite3
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_engine import compute_all, get_db_path, get_top_centrality, get_top_leverage

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass


def _get_discord_webhook():
    return os.environ.get("DISCORD_WEBHOOK_URL", "")


def _log_recompute(db_path, nodes, edges, clusters, top_cent_name, top_lev_name, duration, status, error_message=None):
    """Log recomputation results to recompute_log table."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO recompute_log
            (nodes_computed, edges_computed, clusters_found,
             top_centrality_node, top_leverage_node,
             duration_seconds, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (nodes, edges, clusters, top_cent_name, top_lev_name, round(duration, 2), status, error_message))
    
    conn.commit()
    conn.close()


def _post_to_discord(webhook_url, nodes, edges, clusters, top_centrality, top_leverage, duration):
    """Post recomputation summary to Discord webhook."""
    if not webhook_url or not HAS_REQUESTS:
        return

    cent_lines = "\n".join(
        f"  {i+1}. {e['name']} ({e['type']}) — {e.get('centrality_score', 0):.2f}"
        for i, e in enumerate(top_centrality[:5])
    ) or "  No data"
    
    lev_lines = "\n".join(
        f"  {i+1}. {e['name']} ({e['type']}) — {e.get('leverage_score', 0):.2f}"
        for i, e in enumerate(top_leverage[:5])
    ) or "  No data"

    message = (
        f"**Relationship Engine — Nightly Recompute**\n"
        f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Duration: {duration:.1f}s\n"
        f"Nodes: {nodes} | Edges: {edges} | Clusters: {clusters}\n\n"
        f"**Top 5 Centrality:**\n{cent_lines}\n\n"
        f"**Top 5 Leverage:**\n{lev_lines}"
    )

    try:
        requests.post(webhook_url, json={"content": message}, timeout=10)
    except Exception as e:
        print(f"Discord webhook failed: {e}")


def run_nightly(db_path=None):
    """Execute full nightly recomputation pipeline."""
    if db_path is None:
        db_path = get_db_path()
    
    start = time.time()
    status = "success"
    error_msg = None
    nodes = edges = clusters = 0
    top_centrality = []
    top_leverage = []

    try:
        # Run graph computation
        results = compute_all(db_path, verbose=True)
        
        nodes = results.get('nodes', 0)
        edges = results.get('edges', 0)
        clusters = results.get('clusters', 0)
        
        # Get top entities from database
        top_centrality = get_top_centrality(5, db_path)
        top_leverage = get_top_leverage(5, db_path)
        
        duration = time.time() - start
        print(f"\nRecomputation complete in {duration:.1f}s")
        
    except Exception as e:
        duration = time.time() - start
        status = "failed"
        error_msg = str(e)
        print(f"Recomputation FAILED after {duration:.1f}s: {e}")

    # Get top names for logging
    top_cent_name = top_centrality[0]['name'] if top_centrality else "N/A"
    top_lev_name = top_leverage[0]['name'] if top_leverage else "N/A"

    # Log to database
    try:
        _log_recompute(db_path, nodes, edges, clusters, top_cent_name, top_lev_name, duration, status, error_msg)
    except Exception as e:
        print(f"Failed to log recompute: {e}")

    # Post to Discord
    webhook_url = _get_discord_webhook()
    if webhook_url:
        _post_to_discord(webhook_url, nodes, edges, clusters, top_centrality, top_leverage, duration)

    return {
        'nodes': nodes,
        'edges': edges,
        'clusters': clusters,
        'top_centrality': top_centrality,
        'top_leverage': top_leverage,
        'duration': duration,
        'status': status
    }


if __name__ == "__main__":
    run_nightly()
