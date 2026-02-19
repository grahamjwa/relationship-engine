"""
Opportunity Scoring Engine for Relationship Engine
Computes priority scores for companies and contacts based on multiple signals.
"""

import sqlite3
import math
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import (get_db_path, build_graph, find_shortest_path,
                               broker_coverage_overlap, EDGE_TYPE_WEIGHTS,
                               _node_key)
from core.thresholds import (WEIGHTS_DEFAULT, WEIGHTS_HIGH_GROWTH,
                             WEIGHTS_INSTITUTIONAL, CASH_BONUS_THRESHOLD,
                             HALF_LIFE_CASH)


# =============================================================================
# DECAY FUNCTIONS
# =============================================================================

def days_since(date_str: Optional[str]) -> int:
    """Calculate days since a date string."""
    if not date_str:
        return 9999  # Very old
    try:
        if isinstance(date_str, str):
            date_str = date_str.split()[0]  # Handle datetime strings
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            dt = date_str
        return (datetime.now() - dt).days
    except:
        return 9999


def decay_factor(days: int, half_life_days: int) -> float:
    """
    Calculate exponential decay factor.
    Returns 1.0 for today, 0.5 at half-life, approaches 0 over time.
    """
    if days <= 0:
        return 1.0
    lambda_rate = 0.693 / half_life_days  # ln(2) / half_life
    return math.exp(-lambda_rate * days)


# Half-lives in days
HALF_LIFE_FUNDING = 180      # 6 months
HALF_LIFE_HIRING = 90        # 3 months
HALF_LIFE_OUTREACH = 30      # 1 month
HALF_LIFE_RELATIONSHIP = 730  # 2 years


# =============================================================================
# COMPANY OPPORTUNITY SCORING
# =============================================================================

def score_company_funding(company_id: int, conn: sqlite3.Connection) -> float:
    """
    Score based on recent funding events.
    Returns 0-100 score.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT event_date, amount 
        FROM funding_events 
        WHERE company_id = ?
        ORDER BY event_date DESC
        LIMIT 5
    """, (company_id,))
    
    events = cur.fetchall()
    if not events:
        return 0.0
    
    total_score = 0.0
    for event_date, amount in events:
        days = days_since(event_date)
        decay = decay_factor(days, HALF_LIFE_FUNDING)
        
        # Amount factor (log scale, normalized)
        if amount and amount > 0:
            amount_factor = min(math.log10(amount + 1) / 9, 1.0)  # $1B = 1.0
        else:
            amount_factor = 0.3  # Unknown amount
        
        total_score += decay * amount_factor * 100
    
    return min(total_score, 100.0)


def score_company_hiring(company_id: int, conn: sqlite3.Connection) -> float:
    """
    Score based on recent hiring signals.
    Returns 0-100 score.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT signal_date, relevance, signal_type
        FROM hiring_signals 
        WHERE company_id = ?
        ORDER BY signal_date DESC
        LIMIT 10
    """, (company_id,))
    
    signals = cur.fetchall()
    if not signals:
        return 0.0
    
    relevance_weights = {'high': 1.0, 'medium': 0.5, 'low': 0.2}
    type_weights = {
        'leadership_hire': 1.0,
        'new_office': 0.9,
        'headcount_growth': 0.6,
        'job_posting': 0.4,
        'press_announcement': 0.3
    }
    
    total_score = 0.0
    for signal_date, relevance, signal_type in signals:
        days = days_since(signal_date)
        decay = decay_factor(days, HALF_LIFE_HIRING)
        
        rel_weight = relevance_weights.get(relevance, 0.3)
        type_weight = type_weights.get(signal_type, 0.3)
        
        total_score += decay * rel_weight * type_weight * 50
    
    return min(total_score, 100.0)


def score_company_lease_expiry(company_id: int, conn: sqlite3.Connection) -> float:
    """
    Score based on upcoming lease expirations.
    Returns 0-100 score.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT lease_expiry, square_feet
        FROM leases 
        WHERE company_id = ?
        AND lease_expiry >= date('now')
        ORDER BY lease_expiry ASC
        LIMIT 5
    """, (company_id,))
    
    leases = cur.fetchall()
    if not leases:
        return 0.0
    
    total_score = 0.0
    for expiry, sf in leases:
        days = days_since(expiry)
        days_until = -days  # Positive = future
        
        if days_until <= 0:
            continue  # Already expired
        
        # Score higher for sooner expirations (within 24 months)
        if days_until <= 365:  # Within 1 year
            time_score = 1.0
        elif days_until <= 730:  # 1-2 years
            time_score = 0.5
        else:
            time_score = 0.2
        
        # Size factor
        if sf and sf > 0:
            size_factor = min(sf / 100000, 1.0)  # 100k SF = max
        else:
            size_factor = 0.3
        
        total_score += time_score * size_factor * 50
    
    return min(total_score, 100.0)


def score_company_relationship_proximity(company_id: int, conn: sqlite3.Connection, graph=None) -> float:
    """
    Score based on relationship proximity to our team.
    Considers both contacts employed at the company AND contacts connected
    to the company via relationship edges in the graph.
    Returns 0-100 score.
    """
    cur = conn.cursor()

    if graph is None:
        graph = build_graph()

    company_node = _node_key("company", company_id)

    # Collect target nodes: the company node itself + contacts employed there
    targets = set()
    if company_node in graph.nodes:
        targets.add(company_node)

    cur.execute("SELECT id FROM contacts WHERE company_id = ?", (company_id,))
    for row in cur.fetchall():
        targets.add(f"contact_{row[0]}")

    # Also include contacts connected to this company via graph edges
    if company_node in graph.nodes:
        for neighbor in graph.neighbors_undirected(company_node):
            if graph.nodes.get(neighbor, {}).get("entity_type") == "contact":
                targets.add(neighbor)

    if not targets:
        return 0.0

    # Get our team contacts
    cur.execute("SELECT id FROM contacts WHERE role_level = 'team'")
    team_contacts = [f"contact_{row[0]}" for row in cur.fetchall()]

    if not team_contacts:
        return 0.0

    # Find shortest path from any team member to any target
    best_score = 0.0
    for team in team_contacts:
        if team in targets:
            best_score = 100.0  # Team member is directly at/connected to company
            break
        for target in targets:
            path, weight = find_shortest_path(graph, team, target)
            if path:
                hops = len(path) - 1
                if hops == 1:
                    path_score = 100.0
                elif hops == 2:
                    path_score = 70.0
                elif hops == 3:
                    path_score = 40.0
                else:
                    path_score = 20.0
                best_score = max(best_score, path_score)

    return best_score


# =============================================================================
# SCORING 2.0 — VELOCITY, ACCELERATION, DEPTH, COVERAGE
# =============================================================================

def hiring_velocity_delta(company_id: int, conn: sqlite3.Connection,
                          window_days: int = 90) -> float:
    """
    Measure rate-of-change in hiring activity.
    Compares signal count in the recent window vs. the prior window.
    Returns: positive = accelerating, negative = decelerating.
    Score range: -100 to +100.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT signal_date FROM hiring_signals
        WHERE company_id = ?
        AND signal_date >= date('now', ?)
    """, (company_id, f"-{window_days * 2} days"))
    rows = cur.fetchall()
    if not rows:
        return 0.0

    cutoff = datetime.now() - timedelta(days=window_days)
    recent = sum(1 for r in rows if r[0] and datetime.strptime(r[0].split()[0], "%Y-%m-%d") >= cutoff)
    prior = len(rows) - recent

    if prior == 0 and recent == 0:
        return 0.0
    if prior == 0:
        return min(recent * 25.0, 100.0)  # New activity from zero

    velocity = (recent - prior) / max(prior, 1)
    return max(min(velocity * 50, 100.0), -100.0)


def funding_acceleration_index(company_id: int, conn: sqlite3.Connection) -> float:
    """
    Second derivative of funding: are rounds getting bigger / more frequent?
    Looks at last 3 funding events and measures acceleration.
    Returns 0-100 score.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT event_date, amount
        FROM funding_events
        WHERE company_id = ? AND amount IS NOT NULL AND amount > 0
        ORDER BY event_date DESC
        LIMIT 3
    """, (company_id,))
    events = cur.fetchall()
    if len(events) < 2:
        return 0.0

    # Compute intervals and amount deltas
    amounts = []
    dates = []
    for date_str, amount in events:
        amounts.append(amount)
        try:
            dates.append(datetime.strptime(date_str.split()[0], "%Y-%m-%d"))
        except (ValueError, TypeError):
            dates.append(datetime.now())

    # Amount acceleration: are rounds getting bigger?
    amount_accel = 0.0
    if len(amounts) >= 2:
        ratio = amounts[0] / max(amounts[1], 1)
        amount_accel = min((ratio - 1.0) * 30, 50.0)  # Cap at 50

    # Frequency acceleration: are rounds closer together?
    freq_accel = 0.0
    if len(dates) >= 3:
        gap_recent = abs((dates[0] - dates[1]).days)
        gap_prior = abs((dates[1] - dates[2]).days)
        if gap_prior > 0:
            freq_ratio = gap_prior / max(gap_recent, 30)  # higher = accelerating
            freq_accel = min((freq_ratio - 1.0) * 25, 50.0)

    return max(0.0, min(amount_accel + freq_accel, 100.0))


def relationship_depth_multiplier(company_id: int, conn: sqlite3.Connection,
                                  graph=None) -> float:
    """
    Score that combines path length with edge quality along the path.
    A 2-hop path through a 'client' edge is worth more than through an 'alumni' edge.
    Returns 0-100 score.
    """
    cur = conn.cursor()

    if graph is None:
        graph = build_graph()

    company_node = _node_key("company", company_id)

    # Collect targets: company node + contacts employed there + graph-connected contacts
    targets = set()
    if company_node in graph.nodes:
        targets.add(company_node)

    cur.execute("SELECT id FROM contacts WHERE company_id = ?", (company_id,))
    for row in cur.fetchall():
        targets.add(f"contact_{row[0]}")

    if company_node in graph.nodes:
        for neighbor in graph.neighbors_undirected(company_node):
            if graph.nodes.get(neighbor, {}).get("entity_type") == "contact":
                targets.add(neighbor)

    if not targets:
        return 0.0

    # Get team contacts
    cur.execute("SELECT id FROM contacts WHERE role_level = 'team'")
    team_contacts = [f"contact_{row[0]}" for row in cur.fetchall()]
    if not team_contacts:
        return 0.0

    best_score = 0.0
    for team in team_contacts:
        for target in targets:
            path, weight = find_shortest_path(graph, team, target)
            if not path or len(path) < 2:
                continue

            hops = len(path) - 1

            # Compute edge quality along path
            path_quality = 0.0
            for i in range(len(path) - 1):
                u, v = path[i], path[i + 1]
                # Check both directions
                edata = graph.edge_data(u, v) or graph.edge_data(v, u) or {}
                rel_type = edata.get("relationship_type", "")
                type_weight = EDGE_TYPE_WEIGHTS.get(rel_type, 1.0)
                edge_w = edata.get("weight", 1.0)
                path_quality += type_weight * min(edge_w, 3.0) / 3.0

            avg_quality = path_quality / hops if hops > 0 else 0

            # Shorter path + higher quality = better score
            length_factor = {1: 1.0, 2: 0.7, 3: 0.4}.get(hops, 0.2)
            score = length_factor * avg_quality * 100.0
            best_score = max(best_score, score)

    return min(best_score, 100.0)


def competitive_coverage_factor(company_id: int, conn: sqlite3.Connection,
                                graph=None) -> float:
    """
    How well-covered is this company vs. competitors?
    Higher score = less covered = more opportunity for us.
    Returns 0-100 score.
    """
    if graph is None:
        graph = build_graph()

    company_node = f"company_{company_id}"
    if company_node not in graph.nodes:
        return 50.0  # Unknown = assume moderate opportunity

    coverage = broker_coverage_overlap(graph)
    info = coverage.get(company_node, {})

    total_bridges = info.get("total_bridges", 0)
    if total_bridges == 0:
        return 100.0  # No coverage at all = max opportunity
    elif total_bridges == 1:
        return 80.0   # Single-threaded = high opportunity
    elif total_bridges <= 3:
        return 50.0
    else:
        return max(20.0, 100.0 - total_bridges * 10)  # Well-covered


def score_cash_adjacency(company_id: int, conn: sqlite3.Connection) -> float:
    """
    For institutional/mature companies: score based on cash reserves.
    Cash > $100M with recent update = high score.
    Returns 0-100 score.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT cash_reserves, cash_updated_at
        FROM companies WHERE id = ?
    """, (company_id,))
    row = cur.fetchone()
    if not row or not row[0]:
        return 0.0

    cash = row[0]
    updated_at = row[1]

    # Normalize: cash / threshold, capped at 1.0
    cash_ratio = min(cash / CASH_BONUS_THRESHOLD, 1.0)

    # Time decay on freshness of cash data
    days = days_since(updated_at)
    decay = decay_factor(days, HALF_LIFE_CASH)

    return cash_ratio * decay * 100.0


def _get_company_category(company_id: int, conn: sqlite3.Connection,
                          graph=None) -> Optional[str]:
    """Look up the category for a company (from DB or graph)."""
    if graph:
        node = _node_key("company", company_id)
        if node in graph.nodes:
            return graph.nodes[node].get("category")

    cur = conn.cursor()
    cur.execute("SELECT category FROM companies WHERE id = ?", (company_id,))
    row = cur.fetchone()
    return row[0] if row else None


def compute_company_opportunity_score(
    company_id: int,
    conn: sqlite3.Connection,
    graph=None,
    weights: Dict[str, float] = None
) -> Dict:
    """
    Compute overall opportunity score for a company.
    Scoring 2.1: weights are conditional on entity category.

    Returns dict with component scores and total.
    """
    # Determine category-specific weights
    category = _get_company_category(company_id, conn, graph)

    if weights is None:
        if category == "high_growth":
            weights = dict(WEIGHTS_HIGH_GROWTH)
        elif category == "institutional":
            weights = dict(WEIGHTS_INSTITUTIONAL)
        else:
            weights = dict(WEIGHTS_DEFAULT)

    scores = {
        'funding': score_company_funding(company_id, conn),
        'hiring': score_company_hiring(company_id, conn),
        'lease_expiry': score_company_lease_expiry(company_id, conn),
        'relationship': score_company_relationship_proximity(company_id, conn, graph),
        'hiring_velocity': max(0, hiring_velocity_delta(company_id, conn)),
        'funding_accel': funding_acceleration_index(company_id, conn),
        'rel_depth': relationship_depth_multiplier(company_id, conn, graph),
        'coverage': competitive_coverage_factor(company_id, conn, graph),
        'momentum': 0.0,  # Placeholder for market momentum
    }

    # Cash adjacency — only computed for institutional, but stored regardless
    scores['cash_adjacency'] = score_cash_adjacency(company_id, conn)

    # Compute weighted total (only sum dimensions present in the weight profile)
    total = sum(scores.get(k, 0) * weights.get(k, 0) for k in weights)
    scores['total'] = total
    scores['company_id'] = company_id
    scores['category'] = category

    return scores


# =============================================================================
# CONTACT PRIORITY SCORING
# =============================================================================

def score_contact_role(role_level: str) -> float:
    """Score based on role level."""
    role_scores = {
        'c_suite': 100.0,
        'decision_maker': 80.0,
        'influencer': 50.0,
        'team': 30.0,
        'external_partner': 40.0
    }
    return role_scores.get(role_level, 30.0)


def score_contact_engagement(contact_id: int, conn: sqlite3.Connection) -> float:
    """Score based on recent engagement."""
    cur = conn.cursor()
    cur.execute("""
        SELECT outreach_date, outcome
        FROM outreach_log 
        WHERE target_contact_id = ?
        ORDER BY outreach_date DESC
        LIMIT 5
    """, (contact_id,))
    
    outreach = cur.fetchall()
    if not outreach:
        return 0.0
    
    outcome_weights = {
        'deal_started': 1.0,
        'meeting_held': 0.9,
        'meeting_booked': 0.8,
        'responded_positive': 0.7,
        'referred': 0.6,
        'pending': 0.4,
        'no_response': 0.2,
        'responded_negative': 0.1,
        'declined': 0.05
    }
    
    total_score = 0.0
    for outreach_date, outcome in outreach:
        days = days_since(outreach_date)
        decay = decay_factor(days, HALF_LIFE_OUTREACH)
        outcome_weight = outcome_weights.get(outcome, 0.3)
        total_score += decay * outcome_weight * 50
    
    return min(total_score, 100.0)


def compute_contact_priority_score(
    contact_id: int,
    conn: sqlite3.Connection,
    company_scores: Dict[int, float] = None,
    graph=None
) -> Dict:
    """
    Compute priority score for a contact.
    
    Returns dict with component scores and total.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT role_level, company_id, centrality_score, leverage_score
        FROM contacts WHERE id = ?
    """, (contact_id,))
    
    row = cur.fetchone()
    if not row:
        return {'total': 0.0, 'contact_id': contact_id}
    
    role_level, company_id, centrality, leverage = row
    
    scores = {
        'role': score_contact_role(role_level),
        'engagement': score_contact_engagement(contact_id, conn),
        'centrality': min((centrality or 0) * 10, 100),  # Scale centrality
        'leverage': min((leverage or 0) * 5, 100),  # Scale leverage
    }
    
    # Add company opportunity score if available
    if company_scores and company_id in company_scores:
        scores['company_opportunity'] = company_scores[company_id]
    else:
        scores['company_opportunity'] = 0.0
    
    # Weighted total
    weights = {
        'role': 0.30,
        'company_opportunity': 0.25,
        'centrality': 0.15,
        'leverage': 0.10,
        'engagement': 0.20
    }
    
    total = sum(scores[k] * weights[k] for k in weights)
    scores['total'] = total
    scores['contact_id'] = contact_id
    
    return scores


# =============================================================================
# OPPORTUNITY CATEGORIES
# =============================================================================

def identify_high_priority(conn: sqlite3.Connection) -> List[Dict]:
    """Identify high-priority opportunities (deduplicated by company)."""
    cur = conn.cursor()
    results = []
    seen_companies = set()
    
    # Funding in last 90 days for target companies
    cur.execute("""
        SELECT DISTINCT c.id, c.name, f.round_type, MAX(f.amount), MAX(f.event_date)
        FROM companies c
        JOIN funding_events f ON c.id = f.company_id
        WHERE c.status IN ('high_growth_target', 'prospect')
        AND f.event_date >= date('now', '-90 days')
        GROUP BY c.id
        ORDER BY MAX(f.event_date) DESC
    """)
    for row in cur.fetchall():
        if row[0] not in seen_companies:
            seen_companies.add(row[0])
            results.append({
                'type': 'recent_funding',
                'company_id': row[0],
                'company_name': row[1],
                'detail': f"{row[2]}: ${row[3]:,.0f}" if row[3] else row[2],
                'date': row[4],
                'priority': 'high'
            })
    
    # Lease expiry in next 12 months
    cur.execute("""
        SELECT c.id, c.name, MIN(l.lease_expiry), SUM(l.square_feet), b.address
        FROM companies c
        JOIN leases l ON c.id = l.company_id
        JOIN buildings b ON l.building_id = b.id
        WHERE c.status IN ('prospect', 'active_client', 'high_growth_target')
        AND l.lease_expiry BETWEEN date('now') AND date('now', '+12 months')
        GROUP BY c.id
        ORDER BY MIN(l.lease_expiry) ASC
    """)
    for row in cur.fetchall():
        if row[0] not in seen_companies:
            seen_companies.add(row[0])
            results.append({
                'type': 'lease_expiring',
                'company_id': row[0],
                'company_name': row[1],
                'detail': f"{row[3]:,} SF at {row[4]}" if row[3] else row[4],
                'date': row[2],
                'priority': 'high'
            })
    
    # High-relevance hiring signal in last 30 days
    cur.execute("""
        SELECT c.id, c.name, h.signal_type, h.details, MAX(h.signal_date)
        FROM companies c
        JOIN hiring_signals h ON c.id = h.company_id
        WHERE h.relevance = 'high'
        AND h.signal_date >= date('now', '-30 days')
        GROUP BY c.id
        ORDER BY MAX(h.signal_date) DESC
    """)
    for row in cur.fetchall():
        if row[0] not in seen_companies:
            seen_companies.add(row[0])
            results.append({
                'type': 'high_value_hire',
                'company_id': row[0],
                'company_name': row[1],
                'detail': row[3] or row[2],
                'date': row[4],
                'priority': 'high'
            })
    
    return results


def identify_undercovered(conn: sqlite3.Connection) -> List[Dict]:
    """Identify undercovered opportunities (no outreach in 90 days)."""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT c.id, c.name, c.status, c.sector,
               MAX(o.outreach_date) as last_outreach
        FROM companies c
        LEFT JOIN outreach_log o ON c.id = o.target_company_id
        WHERE c.status IN ('high_growth_target', 'prospect')
        GROUP BY c.id
        HAVING last_outreach IS NULL 
           OR last_outreach < date('now', '-90 days')
        ORDER BY c.status, c.name
    """)
    
    results = []
    for row in cur.fetchall():
        results.append({
            'type': 'undercovered',
            'company_id': row[0],
            'company_name': row[1],
            'status': row[2],
            'sector': row[3],
            'last_outreach': row[4],
            'priority': 'medium'
        })
    
    return results


def identify_relationships_at_risk(conn: sqlite3.Connection) -> List[Dict]:
    """Identify client relationships at risk."""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT c.id, c.name, 
               MAX(o.outreach_date) as last_outreach,
               o.outcome
        FROM companies c
        LEFT JOIN outreach_log o ON c.id = o.target_company_id
        WHERE c.status = 'active_client'
        GROUP BY c.id
        HAVING last_outreach IS NULL 
           OR last_outreach < date('now', '-60 days')
        ORDER BY last_outreach ASC
    """)
    
    results = []
    for row in cur.fetchall():
        results.append({
            'type': 'relationship_at_risk',
            'company_id': row[0],
            'company_name': row[1],
            'last_outreach': row[2],
            'last_outcome': row[3],
            'priority': 'high'
        })
    
    return results


# =============================================================================
# MAIN SCORING FUNCTIONS
# =============================================================================

def compute_all_company_scores(db_path: Optional[str] = None, verbose: bool = True) -> Dict[int, Dict]:
    """Compute opportunity scores for all companies."""
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Build graph once
    graph = build_graph(db_path)
    
    cur.execute("SELECT id, name FROM companies")
    companies = cur.fetchall()
    
    scores = {}
    for company_id, name in companies:
        score = compute_company_opportunity_score(company_id, conn, graph)
        score['name'] = name
        scores[company_id] = score
        
        if verbose:
            print(f"{name}: {score['total']:.1f}")
    
    conn.close()
    return scores


def save_opportunity_scores(db_path: Optional[str] = None):
    """Compute and save opportunity scores to database."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Ensure columns exist
    company_cols = [
        ("opportunity_score", "REAL"), ("opp_funding", "REAL"),
        ("opp_hiring", "REAL"), ("opp_lease", "REAL"),
        ("opp_relationship", "REAL"), ("opp_hiring_velocity", "REAL"),
        ("opp_funding_accel", "REAL"), ("opp_rel_depth", "REAL"),
        ("opp_coverage", "REAL"),
    ]
    for col, ctype in company_cols:
        try:
            cur.execute(f"ALTER TABLE companies ADD COLUMN {col} {ctype}")
        except Exception:
            pass
    try:
        cur.execute("ALTER TABLE contacts ADD COLUMN priority_score REAL")
    except Exception:
        pass

    # Compute company scores
    graph = build_graph(db_path)
    cur.execute("SELECT id FROM companies")
    company_ids = [row[0] for row in cur.fetchall()]
    company_scores = {}

    for company_id in company_ids:
        score = compute_company_opportunity_score(company_id, conn, graph)
        company_scores[company_id] = score['total']
        cur.execute("""
            UPDATE companies SET
                opportunity_score = ?,
                opp_funding = ?, opp_hiring = ?, opp_lease = ?,
                opp_relationship = ?, opp_hiring_velocity = ?,
                opp_funding_accel = ?, opp_rel_depth = ?, opp_coverage = ?
            WHERE id = ?
        """, (
            score['total'],
            score.get('funding', 0), score.get('hiring', 0),
            score.get('lease_expiry', 0), score.get('relationship', 0),
            score.get('hiring_velocity', 0), score.get('funding_accel', 0),
            score.get('rel_depth', 0), score.get('coverage', 0),
            company_id
        ))

    # Compute contact scores
    cur.execute("SELECT id FROM contacts")
    for (contact_id,) in cur.fetchall():
        score = compute_contact_priority_score(contact_id, conn, company_scores, graph)
        cur.execute(
            "UPDATE contacts SET priority_score = ? WHERE id = ?",
            (score['total'], contact_id)
        )

    conn.commit()
    conn.close()
    print("Opportunity scores saved (v2.0).")


def get_top_opportunities(n: int = 20, db_path: Optional[str] = None) -> List[Dict]:
    """Get top N companies by opportunity score."""
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("""
        SELECT id, name, status, sector, opportunity_score
        FROM companies
        WHERE opportunity_score IS NOT NULL
        ORDER BY opportunity_score DESC
        LIMIT ?
    """, (n,))
    
    results = [dict(row) for row in cur.fetchall()]
    conn.close()
    return results


def generate_daily_insights(db_path: Optional[str] = None) -> Dict:
    """Generate daily insight report."""
    if db_path is None:
        db_path = get_db_path()
    
    conn = sqlite3.connect(db_path)
    
    insights = {
        'high_priority': identify_high_priority(conn),
        'undercovered': identify_undercovered(conn),
        'at_risk': identify_relationships_at_risk(conn),
        'generated_at': datetime.now().isoformat()
    }
    
    conn.close()
    return insights


if __name__ == "__main__":
    print("Computing opportunity scores...")
    save_opportunity_scores()
    
    print("\nTop 10 Opportunities:")
    for i, opp in enumerate(get_top_opportunities(10), 1):
        print(f"  {i}. {opp['name']} ({opp['status']}): {opp['opportunity_score']:.1f}")
    
    print("\nDaily Insights:")
    insights = generate_daily_insights()
    print(f"  High Priority: {len(insights['high_priority'])}")
    print(f"  Undercovered: {len(insights['undercovered'])}")
    print(f"  At Risk: {len(insights['at_risk'])}")
