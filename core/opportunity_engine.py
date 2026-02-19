"""
Opportunity Inference Engine
=============================
Converts signals into ranked, actionable CRE opportunities.
Combines signal classification, Lean v1 scoring, and relationship data
to produce prioritized recommendations.

No paid APIs. No speculative projections. Signal-driven only.
"""

import sqlite3
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path, build_graph
from core.signals import (score_opportunity, classify_signal, SIGNAL_TYPES,
                          STATUS_WEIGHTS, get_status_weight, SCORE_BOOSTS)
from core.scoring_v1 import compute_lean_score
from core.pipeline_enterprise import score_relationship_adjacency
from core.geo_qualification import is_geo_qualified, get_msa_name


def infer_opportunities(db_path: Optional[str] = None,
                        min_score: int = 10,
                        exclude_clients: bool = False) -> List[Dict]:
    """
    Scan all companies, score signals, return ranked opportunities.

    Returns list of dicts sorted by score descending:
        {
            'company': name,
            'company_id': id,
            'score': 0-100,
            'signals': [list of contributing signals],
            'reason': 'Why this surfaced',
            'recommended_action': 'What to do',
            'confidence': 0-1,
            'space_impact': -1.0 to +1.0,
            'status': company status,
            'sector': company sector,
        }
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    exclude_clause = ""
    if exclude_clients:
        exclude_clause = "AND status != 'active_client'"

    cur.execute(f"""
        SELECT id, name, status, sector, hq_city, hq_state, category,
               employee_count
        FROM companies
        WHERE status NOT IN ('former_client', 'network_portfolio')
        {exclude_clause}
        ORDER BY name
    """)
    companies = cur.fetchall()

    graph = None  # Lazy-load only if needed
    opportunities = []

    for row in companies:
        cid, name, status, sector, city, state, category, emp_count = row

        # Score signals
        sig_result = score_opportunity(cid, db_path)
        if sig_result['total_score'] < min_score:
            continue

        # Enrich with Lean v1 if emerging
        is_institutional = (category == 'institutional' or
                           sector in ('hedge_fund', 'private_equity', 'asset_management'))
        lean_score = None
        adj_score = None

        if not is_institutional:
            try:
                lean = compute_lean_score(cid, conn)
                lean_score = lean['lean_score']
            except Exception:
                pass
        else:
            if graph is None:
                graph = build_graph(db_path)
            try:
                adj = score_relationship_adjacency(cid, conn, graph)
                adj_score = adj['adjacency_score']
            except Exception:
                pass

        # Build composite score with status weight
        composite = sig_result['total_score']
        if lean_score:
            composite = int(composite * 0.5 + lean_score * 0.5)
        if adj_score and adj_score > 50:
            composite = min(composite + 10, 100)  # Adjacency bonus

        # Apply status weight — prospects get boosted, existing clients dampened
        status_weight = get_status_weight(status)
        composite = min(int(composite * status_weight), 100)

        # Determine recommended action
        action = _recommend_action(
            sig_result['signals'], status, lean_score, adj_score
        )

        # Build reason
        reason = sig_result['reason']
        if lean_score:
            reason += f" | Lean: {lean_score}"
        if adj_score:
            reason += f" | Adjacency: {adj_score}"

        # Confidence
        avg_conf = (sum(s['confidence'] for s in sig_result['signals']) /
                    max(len(sig_result['signals']), 1))

        opportunities.append({
            'company': name,
            'company_id': cid,
            'score': composite,
            'signals': [s['detail'] for s in sig_result['signals']],
            'signal_details': sig_result['signals'],
            'reason': reason,
            'recommended_action': action,
            'confidence': round(avg_conf, 2),
            'space_impact': sig_result['space_impact_estimate'],
            'status': status,
            'sector': sector or '',
            'hq': f"{city or ''}, {state or ''}".strip(', '),
            'lean_score': lean_score,
            'adjacency_score': adj_score,
        })

    conn.close()

    # Sort by score descending
    opportunities.sort(key=lambda x: x['score'], reverse=True)
    return opportunities


def _recommend_action(signals: List[Dict], status: str,
                      lean_score: Optional[int],
                      adj_score: Optional[int]) -> str:
    """Generate a recommended action based on signals and context."""
    has_cre_exec = any(s['signal_type'] == 'exec_hire_cre' for s in signals)
    has_large_funding = any(s['signal_type'] == 'funding_large' for s in signals)
    has_lease_expiry = any(s['signal_type'] == 'lease_expiry_near' for s in signals)
    has_layoffs = any(s['signal_type'] == 'layoffs' for s in signals)
    has_expansion = any(s['signal_type'] == 'office_expansion' for s in signals)

    if has_cre_exec:
        return ("URGENT: CRE/facilities exec hired — company is actively seeking space. "
                "Direct outreach to new hire immediately.")
    if has_expansion:
        return ("Confirmed expansion — reach out with available inventory and market comps.")
    if has_layoffs:
        return ("Contraction signal — prospect for sublease representation. "
                "Approach with discretion.")
    if has_lease_expiry and has_large_funding:
        return ("Lease expiring + large raise = prime relocation candidate. "
                "Lead with market options and expansion scenarios.")
    if has_lease_expiry:
        return ("Lease expiry approaching — present renewal alternatives and relocation options.")
    if has_large_funding:
        return ("Large funding round — congratulate and offer market perspective. "
                "Expect space needs within 6-12 months.")
    if lean_score and lean_score >= 50:
        return ("High-growth emerging company — establish relationship before competitors. "
                "Lead with market intel.")
    if adj_score and adj_score >= 75:
        return ("Strong relationship adjacency — leverage warm intro path for initial meeting.")
    if status == 'active_client':
        return ("Active client with new signals — schedule check-in to discuss evolving needs.")

    return ("Monitor and engage when additional signals emerge.")


def find_new_opportunities(db_path: Optional[str] = None,
                           since_days: int = 7,
                           exclude_clients: bool = True) -> List[Dict]:
    """
    Find opportunities from signals in the last N days.
    Only returns companies with NEW signals (not just existing data).
    Excludes active_client by default.
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=since_days)).strftime("%Y-%m-%d")

    # Find companies with recent funding or hiring signals
    cur.execute("""
        SELECT DISTINCT company_id FROM (
            SELECT company_id FROM funding_events WHERE event_date >= ?
            UNION
            SELECT company_id FROM hiring_signals WHERE signal_date >= ?
        )
    """, (cutoff, cutoff))
    recent_company_ids = {row[0] for row in cur.fetchall()}

    # Also add companies with upcoming lease expirations entering the 12-month window
    cur.execute("""
        SELECT DISTINCT company_id FROM leases
        WHERE lease_expiry BETWEEN date('now') AND date('now', '+12 months')
    """)
    lease_company_ids = {row[0] for row in cur.fetchall()}

    target_ids = recent_company_ids | lease_company_ids
    conn.close()

    if not target_ids:
        return []

    # Score only companies with recent activity
    all_opps = infer_opportunities(db_path=db_path, min_score=5,
                                   exclude_clients=exclude_clients)
    filtered = [o for o in all_opps if o['company_id'] in target_ids]

    return filtered


def get_outreach_gaps(db_path: Optional[str] = None,
                      stale_days: int = 30,
                      min_score: int = 20) -> List[Dict]:
    """
    Find high-value targets with no recent outreach.

    Returns companies WHERE status IN ('high_growth_target', 'prospect')
    AND opportunity score > min_score AND (no outreach OR last outreach > stale_days ago).
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=stale_days)).strftime("%Y-%m-%d")

    cur.execute("""
        SELECT c.id, c.name, c.status, c.sector, c.lean_score,
               MAX(o.outreach_date) as last_outreach
        FROM companies c
        LEFT JOIN outreach_log o ON c.id = o.target_company_id
        WHERE c.status IN ('high_growth_target', 'prospect')
        GROUP BY c.id
        HAVING last_outreach IS NULL OR last_outreach < ?
        ORDER BY COALESCE(c.lean_score, 0) DESC
    """, (cutoff,))

    gaps = []
    for row in cur.fetchall():
        row_d = dict(row)
        lean = row_d.get('lean_score') or 0
        if lean < min_score:
            continue
        days_since = None
        if row_d['last_outreach']:
            last_dt = datetime.strptime(row_d['last_outreach'], "%Y-%m-%d")
            days_since = (datetime.now() - last_dt).days
        gaps.append({
            'company_id': row_d['id'],
            'company': row_d['name'],
            'status': row_d['status'],
            'sector': row_d['sector'] or '',
            'lean_score': lean,
            'last_outreach': row_d['last_outreach'],
            'days_since_outreach': days_since,
        })

    conn.close()
    return gaps


def generate_opportunity_digest(db_path: Optional[str] = None,
                                since_days: int = 7,
                                top_n: int = 10) -> str:
    """Generate a text digest of top opportunities."""
    opps = find_new_opportunities(db_path=db_path, since_days=since_days)

    if not opps:
        return "No new opportunities detected in the last {since_days} days."

    lines = [
        f"# Opportunity Digest — Last {since_days} Days",
        f"*Generated {datetime.now().strftime('%B %d, %Y %H:%M')}*",
        "",
    ]

    for i, opp in enumerate(opps[:top_n], 1):
        impact_arrow = "↑" if opp['space_impact'] > 0 else "↓" if opp['space_impact'] < 0 else "→"
        lines.append(f"## {i}. {opp['company']} — Score: {opp['score']}")
        lines.append(f"**Why:** {opp['reason']}")
        lines.append(f"**Signals:** {', '.join(opp['signals'][:5])}")
        lines.append(f"**Space Impact:** {impact_arrow} {opp['space_impact']}")
        lines.append(f"**Action:** {opp['recommended_action']}")
        lines.append(f"**Confidence:** {opp['confidence']:.0%}")
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Opportunity Inference Engine")
    parser.add_argument("--days", type=int, default=7, help="Lookback days")
    parser.add_argument("--top", type=int, default=10, help="Top N results")
    parser.add_argument("--all", action="store_true", help="Score all companies")
    parser.add_argument("--digest", action="store_true", help="Print digest")
    args = parser.parse_args()

    if args.digest:
        print(generate_opportunity_digest(since_days=args.days, top_n=args.top))
    elif args.all:
        opps = infer_opportunities()
        for i, o in enumerate(opps[:args.top], 1):
            print(f"{i}. {o['company']}: {o['score']} — {o['reason']}")
            print(f"   Action: {o['recommended_action']}")
    else:
        opps = find_new_opportunities(since_days=args.days)
        if opps:
            for i, o in enumerate(opps[:args.top], 1):
                print(f"{i}. {o['company']}: {o['score']} — {o['reason']}")
                print(f"   Action: {o['recommended_action']}")
        else:
            print(f"No new opportunities in last {args.days} days.")
