"""
Nightly Recompute Script for Relationship Engine
Runs graph computations, updates scores, logs results, posts to Discord.
"""

import os
import sys
import json
import time
import sqlite3
from datetime import datetime

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from graph_engine import compute_all, DB_PATH

try:
    from opportunity_scoring import save_opportunity_scores
    HAS_OPPORTUNITY_SCORING = True
except ImportError:
    HAS_OPPORTUNITY_SCORING = False

try:
    from predictive_chain import score_all_companies as run_chain_scoring
    HAS_CHAIN = True
except ImportError:
    HAS_CHAIN = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from dotenv import load_dotenv
    load_dotenv()  # config.py already loaded .env
except ImportError:
    pass


def _get_discord_webhook() -> str:
    """Get Discord webhook URL from environment."""
    return os.environ.get("DISCORD_WEBHOOK_URL", "")


def _log_recompute(db_path: str, results: dict, duration: float,
                   status: str = "success", error_message: str = None):
    """Log recomputation results to recompute_log table."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    top_cent = results.get("top_centrality", [{}])[0].get("name", "N/A") if results.get("top_centrality") else "N/A"
    top_lev = results.get("top_leverage", [{}])[0].get("name", "N/A") if results.get("top_leverage") else "N/A"

    cur.execute("""
        INSERT INTO recompute_log
            (nodes_computed, edges_computed, clusters_found,
             top_centrality_node, top_leverage_node,
             duration_seconds, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        results.get("graph_stats", {}).get("nodes", 0),
        results.get("graph_stats", {}).get("edges", 0),
        results.get("graph_stats", {}).get("clusters", 0),
        top_cent, top_lev,
        round(duration, 2),
        status,
        error_message
    ))
    conn.commit()
    conn.close()


def _post_to_discord(webhook_url: str, results: dict, duration: float):
    """Post recomputation summary to Discord webhook."""
    if not webhook_url or not HAS_REQUESTS:
        return

    stats = results.get("graph_stats", {})
    top5_cent = results.get("top_centrality", [])[:5]
    top5_lev = results.get("top_leverage", [])[:5]

    cent_lines = "\n".join(
        f"  {i+1}. {e['name']} ({e['type']}) — {e['score']}"
        for i, e in enumerate(top5_cent)
    )
    lev_lines = "\n".join(
        f"  {i+1}. {e['name']} ({e['type']}) — {e['score']}"
        for i, e in enumerate(top5_lev)
    )

    message = (
        f"**Relationship Engine — Nightly Recompute**\n"
        f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Duration: {duration:.1f}s\n"
        f"Nodes: {stats.get('nodes', 0)} | Edges: {stats.get('edges', 0)} | "
        f"Clusters: {stats.get('clusters', 0)}\n\n"
        f"**Top 5 Centrality:**\n{cent_lines}\n\n"
        f"**Top 5 Leverage:**\n{lev_lines}"
    )

    payload = {"content": message}
    try:
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        print(f"Discord webhook failed: {e}")


def run_nightly(db_path: str = DB_PATH) -> dict:
    """Execute full nightly recomputation pipeline."""
    start = time.time()
    status = "success"
    error_msg = None
    results = {}

    try:
        results = compute_all(db_path)
        duration = time.time() - start
        print(f"Recomputation complete in {duration:.1f}s")
        print(f"  Nodes: {results['graph_stats']['nodes']}")
        print(f"  Edges: {results['graph_stats']['edges']}")
        print(f"  Clusters: {results['graph_stats']['clusters']}")
        print(f"  Records updated: {results['records_updated']}")
        print(f"\nTop 5 Centrality:")
        for i, e in enumerate(results.get("top_centrality", [])[:5]):
            print(f"  {i+1}. {e['name']} ({e['type']}) — {e['score']}")

        # Run opportunity scoring after graph recomputation
        if HAS_OPPORTUNITY_SCORING:
            print("\nRunning opportunity scoring (v2.1)...")
            try:
                save_opportunity_scores(db_path)
                print("Opportunity scores updated.")
            except Exception as opp_err:
                print(f"Opportunity scoring failed: {opp_err}")

        # Run predictive chain scoring
        if HAS_CHAIN:
            print("\nRunning predictive chain (Capital → Expansion → Lease)...")
            try:
                chain_results = run_chain_scoring(db_path, verbose=True)
                above = sum(1 for r in chain_results if r["lease_prob"] >= 0.5)
                print(f"Chain scoring complete: {above} companies above 50% threshold.")
            except Exception as chain_err:
                print(f"Chain scoring failed: {chain_err}")
    except Exception as e:
        duration = time.time() - start
        status = "failed"
        error_msg = str(e)
        print(f"Recomputation FAILED after {duration:.1f}s: {e}")

    _log_recompute(db_path, results, duration, status, error_msg)

    webhook_url = _get_discord_webhook()
    if webhook_url:
        _post_to_discord(webhook_url, results, duration)

    return results


if __name__ == "__main__":
    run_nightly()
