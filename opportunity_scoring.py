"""
Opportunity Scoring Engine for Relationship Engine
Computes priority scores for companies and contacts based on multiple signals.
"""

import sqlite3
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_engine import get_db_path, build_graph, find_shortest_path


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
    Returns 0-100 score.
    """
    cur = conn.cursor()
    
    # Get contacts at this company
    cur.execute("""
        SELECT id FROM contacts WHERE company_id = ?
    """, (company_id,))
    company_contacts = [f"contact_{row[0]}" for row in cur.fetchall()]
    
    if not company_contacts:
        return 0.0
    
    # Get our team contacts
    cur.execute("""
        SELECT id FROM contacts WHERE role_level = 'team'
    """)
    team_contacts = [f"contact_{row[0]}" for row in cur.fetchall()]
    
    if not team_contacts:
        return 0.0
    
    # Build graph if not provided
    if graph is None:
        graph = build_graph()
    
    # Find shortest path from any team member to any company contact
    best_score = 0.0
    for team in team_contacts:
        for target in company_contacts:
            path, weight = find_shortest_path(graph, team, target)
            if path:
                # Shorter path = higher score
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


def compute_company_opportunity_score(
    company_id: int,
    conn: sqlite3.Connection,
    graph=None,
    weights: Dict[str, float] = None
) -> Dict:
    """
    Compute overall opportunity score for a company.
    
    Returns dict with component scores and total.
    """
    if weights is None:
        weights = {
            'funding': 0.25,
            'hiring': 0.20,
            'lease_expiry': 0.20,
            'relationship': 0.20,
            'momentum': 0.15
        }
    
    scores = {
        'funding': score_company_funding(company_id, conn),
        'hiring': score_company_hiring(company_id, conn),
        'lease_expiry': score_company_lease_expiry(company_id, conn),
        'relationship': score_company_relationship_proximity(company_id, conn, graph),
        'momentum': 0.0  # Placeholder for market momentum
    }
    
    # Compute weighted total
    total = sum(scores[k] * weights[k] for k in weights)
    scores['total'] = total
    scores['company_id'] = company_id
    
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
    """Identify high-priority opportunities."""
    cur = conn.cursor()
    results = []
    
    # Funding in last 90 days for target companies
    cur.execute("""
        SELECT DISTINCT c.id, c.name, f.round_type, f.amount, f.event_date
        FROM companies c
        JOIN funding_events f ON c.id = f.company_id
        WHERE c.status IN ('high_growth_target', 'prospect')
        AND f.event_date >= date('now', '-90 days')
        ORDER BY f.event_date DESC
    """)
    for row in cur.fetchall():
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
        SELECT c.id, c.name, l.lease_expiry, l.square_feet, b.address
        FROM companies c
        JOIN leases l ON c.id = l.company_id
        JOIN buildings b ON l.building_id = b.id
        WHERE c.status IN ('prospect', 'active_client', 'high_growth_target')
        AND l.lease_expiry BETWEEN date('now') AND date('now', '+12 months')
        ORDER BY l.lease_expiry ASC
    """)
    for row in cur.fetchall():
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
        SELECT c.id, c.name, h.signal_type, h.details, h.signal_date
        FROM companies c
        JOIN hiring_signals h ON c.id = h.company_id
        WHERE h.relevance = 'high'
        AND h.signal_date >= date('now', '-30 days')
        ORDER BY h.signal_date DESC
    """)
    for row in cur.fetchall():
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
    
    # Add column if not exists
    try:
        cur.execute("ALTER TABLE companies ADD COLUMN opportunity_score REAL")
    except:
        pass
    
    try:
        cur.execute("ALTER TABLE contacts ADD COLUMN priority_score REAL")
    except:
        pass
    
    # Compute company scores
    graph = build_graph(db_path)
    cur.execute("SELECT id FROM companies")
    company_scores = {}
    
    for (company_id,) in cur.fetchall():
        score = compute_company_opportunity_score(company_id, conn, graph)
        company_scores[company_id] = score['total']
        cur.execute(
            "UPDATE companies SET opportunity_score = ? WHERE id = ?",
            (score['total'], company_id)
        )
    
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
    print("Opportunity scores saved.")


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
