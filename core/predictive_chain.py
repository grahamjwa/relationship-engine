"""
Predictive Chain Scorer: Capital → Expansion → Lease

Models the causal chain:
  Capital inflow → Hiring/expansion signals → Lease event (6-18 month lag)

For each company, computes a probability that a lease event (new lease,
expansion, relocation) will occur within t months based on:
  1. Capital signal strength (funding amount × recency decay)
  2. Expansion signal strength (hiring velocity, headcount growth)
  3. Existing lease context (approaching expiry = higher prob)
  4. Entity category adjustment (high_growth vs institutional)
  5. Relationship depth multiplier (closer = more actionable)

Output: lease_prob (0-1), chain_score (0-100), chain_stage ('capital', 'expansion', 'lease_imminent')
"""

import sqlite3
import math
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from graph_engine import get_db_path, build_graph, _node_key
from thresholds import (
    CHAIN_LAG_MIN_MONTHS, CHAIN_LAG_MAX_MONTHS,
    CHAIN_SURFACE_THRESHOLD, CHAIN_HIGH_CONFIDENCE,
    CHAIN_LARGE_RAISE, CHAIN_MEDIUM_RAISE,
    HALF_LIFE_FUNDING, HALF_LIFE_HIRING, HALF_LIFE_CASH,
    REVENUE_THRESHOLD, SF_THRESHOLD,
)


def _sigmoid(x: float) -> float:
    """Sigmoid function clamped to avoid overflow."""
    x = max(min(x, 10), -10)
    return 1.0 / (1.0 + math.exp(-x))


def _decay(days: int, half_life: int) -> float:
    if days <= 0:
        return 1.0
    return math.exp(-0.693 * days / half_life)


def _days_since(date_str: Optional[str]) -> int:
    if not date_str:
        return 9999
    try:
        dt = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
        return (datetime.now() - dt).days
    except (ValueError, TypeError):
        return 9999


# =============================================================================
# CHAIN COMPONENT SCORES
# =============================================================================

def _capital_signal(company_id: int, conn: sqlite3.Connection,
                    mature: bool = False) -> float:
    """
    Capital signal score: recent funding weighted by amount and recency.
    For mature companies, funding is downweighted; cash reserves boosted.
    """
    cur = conn.cursor()

    # Funding events
    cur.execute("""
        SELECT event_date, amount
        FROM funding_events
        WHERE company_id = ?
        ORDER BY event_date DESC LIMIT 5
    """, (company_id,))
    events = cur.fetchall()

    funding_score = 0.0
    for date_str, amount in events:
        days = _days_since(date_str)
        decay = _decay(days, HALF_LIFE_FUNDING)
        if amount and amount > 0:
            if amount >= CHAIN_LARGE_RAISE:
                amt_factor = 1.0
            elif amount >= CHAIN_MEDIUM_RAISE:
                amt_factor = 0.6
            else:
                amt_factor = min(math.log10(amount + 1) / 9, 0.4)
        else:
            amt_factor = 0.2
        funding_score += decay * amt_factor

    # Category adjustment
    if mature:
        funding_score *= 0.3  # Downweight for institutional

        # Cash reserves bonus for institutional
        cur.execute("""
            SELECT cash_reserves, cash_updated_at
            FROM companies WHERE id = ?
        """, (company_id,))
        row = cur.fetchone()
        if row and row[0] and row[0] > 0:
            cash_ratio = min(row[0] / CHAIN_LARGE_RAISE, 1.0)
            cash_decay = _decay(_days_since(row[1]), HALF_LIFE_CASH)
            funding_score += cash_ratio * cash_decay * 0.5
    else:
        funding_score *= 0.7  # High-growth: funding is strong signal

    return min(funding_score, 1.0)


def _expansion_signal(company_id: int, conn: sqlite3.Connection) -> float:
    """
    Expansion signal: hiring velocity + headcount growth in recent window.
    """
    cur = conn.cursor()

    # Count hiring signals in last 6 months
    cur.execute("""
        SELECT signal_date, signal_type, relevance
        FROM hiring_signals
        WHERE company_id = ?
        AND signal_date >= date('now', '-180 days')
        ORDER BY signal_date DESC
    """, (company_id,))
    signals = cur.fetchall()

    if not signals:
        return 0.0

    relevance_map = {'high': 1.0, 'medium': 0.5, 'low': 0.2}
    type_map = {
        'leadership_hire': 1.0, 'new_office': 0.9,
        'headcount_growth': 0.7, 'job_posting': 0.3,
        'press_announcement': 0.4
    }

    total = 0.0
    for date_str, stype, relevance in signals:
        days = _days_since(date_str)
        decay = _decay(days, HALF_LIFE_HIRING)
        rel_w = relevance_map.get(relevance, 0.3)
        type_w = type_map.get(stype, 0.3)
        total += decay * rel_w * type_w

    # Normalize: 5+ weighted signals = max score
    return min(total / 5.0, 1.0)


def _lease_context(company_id: int, conn: sqlite3.Connection,
                   t_months: int = 12) -> float:
    """
    Lease expiry context: approaching expiry increases probability.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT lease_expiry, square_feet
        FROM leases
        WHERE company_id = ?
        AND lease_expiry >= date('now')
        ORDER BY lease_expiry ASC
        LIMIT 3
    """, (company_id,))
    leases = cur.fetchall()

    if not leases:
        return 0.0  # No known leases — can't score context

    best = 0.0
    for expiry_str, sf in leases:
        days_until = -_days_since(expiry_str)
        if days_until <= 0:
            continue

        months_until = days_until / 30.44
        if months_until <= t_months:
            # Within prediction window: high score
            urgency = 1.0 - (months_until / t_months)  # 1.0 at expiry, 0.0 at t
            sf_factor = min((sf or 10000) / 50000, 1.0)
            best = max(best, urgency * 0.7 + sf_factor * 0.3)
        elif months_until <= t_months * 2:
            # Just outside window: moderate
            best = max(best, 0.3)

    return best


def _relationship_proximity(company_id: int, conn: sqlite3.Connection,
                            graph=None) -> float:
    """
    How close is this company to our network?
    Closer = more actionable prediction.
    """
    if graph is None:
        return 0.5  # Neutral if no graph

    cur = conn.cursor()
    cur.execute("SELECT id FROM contacts WHERE role_level = 'team'")
    team = [f"contact_{r[0]}" for r in cur.fetchall()]
    if not team:
        return 0.5

    company_node = _node_key("company", company_id)
    if company_node not in graph.nodes:
        return 0.3

    # Check if any team member is within 3 hops
    from graph_engine import find_shortest_path
    best = 0.0
    for t in team:
        path, _ = find_shortest_path(graph, t, company_node)
        if path:
            hops = len(path) - 1
            best = max(best, {1: 1.0, 2: 0.7, 3: 0.4}.get(hops, 0.2))

    return best


# =============================================================================
# MAIN CHAIN PREDICTION
# =============================================================================

def predict_lease_probability(
    company_id: int,
    conn: sqlite3.Connection,
    graph=None,
    t_months: int = 12
) -> Dict:
    """
    Predict the probability of a lease event within t_months.

    Returns dict with:
        lease_prob: float (0-1)
        chain_score: float (0-100)
        chain_stage: str ('capital', 'expansion', 'lease_imminent', 'dormant')
        components: dict of sub-scores
    """
    # Check maturity
    cur = conn.cursor()
    cur.execute("SELECT mature, category FROM companies WHERE id = ?", (company_id,))
    row = cur.fetchone()
    mature = bool(row[0]) if row else False
    category = row[1] if row else None

    # Compute component signals
    capital = _capital_signal(company_id, conn, mature)
    expansion = _expansion_signal(company_id, conn)
    lease_ctx = _lease_context(company_id, conn, t_months)
    proximity = _relationship_proximity(company_id, conn, graph)

    # Weight components based on category
    if category == "institutional" or mature:
        # Institutional: connections + cash matter more than raw funding
        raw = (0.2 * capital + 0.25 * expansion + 0.25 * lease_ctx
               + 0.3 * proximity)
    else:
        # High-growth: funding is strongest leading indicator
        raw = (0.35 * capital + 0.3 * expansion + 0.2 * lease_ctx
               + 0.15 * proximity)

    # Sigmoid transform: map raw 0-1 input to calibrated probability
    # Shift so that raw=0.5 maps to ~0.5 prob
    lease_prob = _sigmoid((raw - 0.35) * 6)

    # Determine chain stage
    if lease_ctx > 0.5:
        stage = "lease_imminent"
    elif expansion > 0.3:
        stage = "expansion"
    elif capital > 0.3:
        stage = "capital"
    else:
        stage = "dormant"

    chain_score = lease_prob * 100

    return {
        "company_id": company_id,
        "lease_prob": round(lease_prob, 4),
        "chain_score": round(chain_score, 2),
        "chain_stage": stage,
        "components": {
            "capital": round(capital, 4),
            "expansion": round(expansion, 4),
            "lease_context": round(lease_ctx, 4),
            "proximity": round(proximity, 4),
        },
        "category": category,
        "t_months": t_months,
    }


# =============================================================================
# BATCH SCORING + PERSISTENCE
# =============================================================================

def score_all_companies(db_path: str = None, t_months: int = 12,
                        verbose: bool = True) -> List[Dict]:
    """Score all companies and persist chain predictions to DB."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    graph = build_graph(db_path)

    cur.execute("SELECT id, name FROM companies")
    companies = cur.fetchall()

    results = []
    for company_id, name in companies:
        pred = predict_lease_probability(company_id, conn, graph, t_months)
        pred["name"] = name
        results.append(pred)

        # Persist to DB
        cur.execute("""
            UPDATE companies
            SET chain_lease_prob = ?, chain_score = ?
            WHERE id = ?
        """, (pred["lease_prob"], pred["chain_score"], company_id))

        if verbose and pred["lease_prob"] >= CHAIN_SURFACE_THRESHOLD:
            conf = "HIGH" if pred["lease_prob"] >= CHAIN_HIGH_CONFIDENCE else "MED"
            print(f"  [{conf}] {name}: {pred['lease_prob']:.0%} "
                  f"({pred['chain_stage']}) — "
                  f"C:{pred['components']['capital']:.2f} "
                  f"E:{pred['components']['expansion']:.2f} "
                  f"L:{pred['components']['lease_context']:.2f} "
                  f"P:{pred['components']['proximity']:.2f}")

    conn.commit()
    conn.close()

    # Sort by probability
    results.sort(key=lambda x: x["lease_prob"], reverse=True)
    return results


def get_chain_predictions(min_prob: float = 0.0, db_path: str = None) -> List[Dict]:
    """Retrieve stored chain predictions from DB."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, status, category, chain_lease_prob, chain_score
        FROM companies
        WHERE chain_lease_prob IS NOT NULL AND chain_lease_prob >= ?
        ORDER BY chain_lease_prob DESC
    """, (min_prob,))

    results = [dict(r) for r in cur.fetchall()]
    conn.close()
    return results


if __name__ == "__main__":
    print("Running predictive chain scoring...")
    results = score_all_companies()

    above_threshold = [r for r in results if r["lease_prob"] >= CHAIN_SURFACE_THRESHOLD]
    print(f"\n{len(above_threshold)} companies above {CHAIN_SURFACE_THRESHOLD:.0%} threshold")

    print(f"\nTop 10 lease predictions:")
    for i, r in enumerate(results[:10], 1):
        print(f"  {i}. {r['name']}: {r['lease_prob']:.1%} "
              f"({r['chain_stage']}, {r['category'] or 'uncategorized'})")
