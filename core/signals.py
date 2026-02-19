"""
Signal Classification Engine
==============================
Classifies events into actionable CRE signals with space-demand impact estimates.

Each signal maps an event type to a real estate consequence:
- Positive space_impact = company likely needs MORE space
- Negative space_impact = company likely shedding space (sublease candidate)
- Magnitude 0.0-1.0 = confidence/probability of space impact

No paid APIs. Works entirely from local DB signals.
"""

import sqlite3
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path


# =============================================================================
# SIGNAL TYPE DEFINITIONS
# =============================================================================

SIGNAL_TYPES = {
    'funding_large': {
        'threshold': 50_000_000,
        'space_impact': 0.25,
        'description': 'Large funding round = HQ expansion likely',
        'why_it_matters': 'Companies raising $50M+ typically expand headcount 20-40% within 12 months, driving office demand.',
    },
    'funding_medium': {
        'threshold': 10_000_000,
        'space_impact': 0.10,
        'description': 'Medium funding = team growth probable',
        'why_it_matters': 'Series A/B rounds of $10M+ fund team buildout. Watch for hiring signals within 90 days.',
    },
    'funding_small': {
        'threshold': 1_000_000,
        'space_impact': 0.05,
        'description': 'Seed/small round = early-stage, coworking likely',
        'why_it_matters': 'Early funding rarely drives office needs, but signals potential future demand if company scales.',
    },
    'hiring_surge': {
        'threshold': 0.15,  # 15% headcount growth
        'space_impact': 0.20,
        'description': '>15% headcount growth = 15-25% more space needed',
        'why_it_matters': 'Every 100 new hires typically require 12,000-15,000 SF of additional office space.',
    },
    'hiring_moderate': {
        'threshold': 0.05,
        'space_impact': 0.08,
        'description': '5-15% headcount growth = incremental space need',
        'why_it_matters': 'Moderate growth may trigger sublease/expansion review within 6-12 months.',
    },
    'exec_hire_cre': {
        'keywords': ['real estate', 'facilities', 'workplace', 'office manager', 'space planning'],
        'space_impact': 0.80,
        'description': 'CRE/facilities exec hire = active space search',
        'why_it_matters': 'Hiring a real estate or facilities executive is the strongest signal of imminent space activity.',
    },
    'exec_hire_cfo': {
        'keywords': ['cfo', 'chief financial', 'vp finance', 'head of finance'],
        'space_impact': 0.30,
        'description': 'CFO hire = strategic scaling phase',
        'why_it_matters': 'New CFO typically reviews all real estate obligations within first 90 days.',
    },
    'exec_hire_coo': {
        'keywords': ['coo', 'chief operating', 'vp operations', 'head of operations'],
        'space_impact': 0.25,
        'description': 'COO hire = operational scaling',
        'why_it_matters': 'Operations leadership often triggers workplace optimization and potential relocation.',
    },
    'layoffs': {
        'space_impact': -0.50,
        'description': 'Layoffs = sublease candidate',
        'why_it_matters': 'Companies reducing headcount often sublease excess space within 3-6 months.',
    },
    'office_expansion': {
        'space_impact': 1.0,
        'description': 'Confirmed expansion announcement',
        'why_it_matters': 'Direct signal — company has announced plans to take additional space.',
    },
    'office_exit': {
        'space_impact': -1.0,
        'description': 'Confirmed office exit or sublease listing',
        'why_it_matters': 'Company vacating space — may generate sublease inventory or represent lost tenant.',
    },
    'leadership_change': {
        'space_impact': 0.15,
        'description': 'C-suite change = potential strategy shift',
        'why_it_matters': 'New leadership often reviews real estate within 6 months. Watch for consolidation or expansion.',
    },
    'lease_expiry_near': {
        'space_impact': 0.60,
        'description': 'Lease expiring within 12 months',
        'why_it_matters': 'Tenant must decide: renew, relocate, or expand. Active decision window.',
    },
}


# =============================================================================
# STATUS WEIGHTS — Prioritize prospects over existing clients
# =============================================================================

STATUS_WEIGHTS = {
    'high_growth_target': 1.5,
    'prospect': 1.2,
    'watching': 1.0,
    'network_portfolio': 0.7,
    'active_client': 0.5,   # Deprioritize — we already have them
    'former_client': 0.8,
    'team_affiliated': 0.3,
}

# Score boosts by signal type (additive, pre-status-weight)
SCORE_BOOSTS = {
    'funding_large': 30,
    'funding_medium': 15,
    'funding_small': 5,
    'hiring_surge': 25,
    'hiring_moderate': 10,
    'exec_hire_cre': 50,
    'exec_hire_cfo': 20,
    'exec_hire_coo': 15,
    'layoffs': -20,
    'office_expansion': 40,
    'office_exit': -30,
    'leadership_change': 10,
    'lease_expiry_near': 20,
}


def get_status_weight(status: str) -> float:
    """Return weight multiplier for company status."""
    return STATUS_WEIGHTS.get(status, 1.0)


# =============================================================================
# CLASSIFICATION FUNCTIONS
# =============================================================================

def classify_funding_signal(amount: Optional[float], round_type: str = "") -> Tuple[str, Dict]:
    """Classify a funding event into a signal type."""
    if not amount or amount <= 0:
        return 'funding_small', SIGNAL_TYPES['funding_small']

    if amount >= SIGNAL_TYPES['funding_large']['threshold']:
        return 'funding_large', SIGNAL_TYPES['funding_large']
    elif amount >= SIGNAL_TYPES['funding_medium']['threshold']:
        return 'funding_medium', SIGNAL_TYPES['funding_medium']
    else:
        return 'funding_small', SIGNAL_TYPES['funding_small']


def classify_hiring_signal(signal_type: str, details: str = "",
                           relevance: str = "medium") -> Tuple[str, Dict]:
    """Classify a hiring signal based on type and details."""
    details_lower = (details or "").lower()

    # Check for CRE-specific exec hires
    for sig_key in ('exec_hire_cre', 'exec_hire_cfo', 'exec_hire_coo'):
        sig = SIGNAL_TYPES[sig_key]
        for kw in sig.get('keywords', []):
            if kw in details_lower:
                return sig_key, sig

    # Leadership hire
    if signal_type == 'leadership_hire':
        return 'leadership_change', SIGNAL_TYPES['leadership_change']

    # Headcount growth signals
    if signal_type in ('headcount_growth', 'job_posting'):
        if relevance == 'high':
            return 'hiring_surge', SIGNAL_TYPES['hiring_surge']
        return 'hiring_moderate', SIGNAL_TYPES['hiring_moderate']

    # Office-related
    if signal_type == 'new_office':
        return 'office_expansion', SIGNAL_TYPES['office_expansion']

    # Default
    return 'hiring_moderate', SIGNAL_TYPES['hiring_moderate']


def classify_signal(event_type: str, event_data: Dict) -> Dict:
    """
    Classify an event and return full signal analysis.

    Args:
        event_type: 'funding', 'hiring', 'lease_expiry', 'layoff', 'expansion', 'exit'
        event_data: dict with event-specific fields

    Returns:
        Dict with signal_type, space_impact, description, why_it_matters, confidence
    """
    if event_type == 'funding':
        sig_key, sig = classify_funding_signal(
            event_data.get('amount'),
            event_data.get('round_type', '')
        )
    elif event_type == 'hiring':
        sig_key, sig = classify_hiring_signal(
            event_data.get('signal_type', ''),
            event_data.get('details', ''),
            event_data.get('relevance', 'medium')
        )
    elif event_type == 'layoff':
        sig_key = 'layoffs'
        sig = SIGNAL_TYPES['layoffs']
    elif event_type == 'expansion':
        sig_key = 'office_expansion'
        sig = SIGNAL_TYPES['office_expansion']
    elif event_type == 'exit':
        sig_key = 'office_exit'
        sig = SIGNAL_TYPES['office_exit']
    elif event_type == 'lease_expiry':
        sig_key = 'lease_expiry_near'
        sig = SIGNAL_TYPES['lease_expiry_near']
    else:
        sig_key = 'hiring_moderate'
        sig = SIGNAL_TYPES['hiring_moderate']

    # Confidence based on data completeness
    confidence = 0.5
    if event_data.get('amount') and event_data['amount'] > 0:
        confidence += 0.2
    if event_data.get('source_url'):
        confidence += 0.1
    if event_data.get('relevance') == 'high':
        confidence += 0.2
    confidence = min(confidence, 1.0)

    return {
        'signal_type': sig_key,
        'space_impact': sig['space_impact'],
        'description': sig['description'],
        'why_it_matters': sig['why_it_matters'],
        'confidence': confidence,
    }


# =============================================================================
# COMPANY-LEVEL SIGNAL SCORING
# =============================================================================

def score_opportunity(company_id: int, db_path: Optional[str] = None) -> Dict:
    """
    Score a company's CRE opportunity based on all signals.

    Returns dict with:
        - total_score (0-100)
        - signals: list of classified signals
        - space_impact_estimate: net space impact (-1.0 to +1.0)
        - reason: human-readable summary
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    signals = []

    # Funding events (last 2 years)
    cur.execute("""
        SELECT event_date, round_type, amount, lead_investor, source_url
        FROM funding_events
        WHERE company_id = ? AND event_date >= date('now', '-730 days')
        ORDER BY event_date DESC
    """, (company_id,))
    for row in cur.fetchall():
        event_data = {
            'amount': row[2], 'round_type': row[1], 'source_url': row[4],
            'event_date': row[0], 'lead_investor': row[3],
        }
        sig = classify_signal('funding', event_data)
        sig['event_date'] = row[0]
        sig['detail'] = f"{row[1] or 'Funding'}: ${row[2]:,.0f}" if row[2] else f"{row[1] or 'Funding'}"
        signals.append(sig)

    # Hiring signals (last 180 days)
    cur.execute("""
        SELECT signal_date, signal_type, details, relevance, source_url
        FROM hiring_signals
        WHERE company_id = ? AND signal_date >= date('now', '-180 days')
        ORDER BY signal_date DESC
    """, (company_id,))
    for row in cur.fetchall():
        event_data = {
            'signal_type': row[1], 'details': row[2],
            'relevance': row[3], 'source_url': row[4],
        }
        sig = classify_signal('hiring', event_data)
        sig['event_date'] = row[0]
        sig['detail'] = row[2] or row[1]
        signals.append(sig)

    # Upcoming lease expirations (next 18 months)
    cur.execute("""
        SELECT l.lease_expiry, l.square_feet, b.address
        FROM leases l
        LEFT JOIN buildings b ON l.building_id = b.id
        WHERE l.company_id = ?
        AND l.lease_expiry BETWEEN date('now') AND date('now', '+18 months')
    """, (company_id,))
    for row in cur.fetchall():
        sig = classify_signal('lease_expiry', {})
        sig['event_date'] = row[0]
        sf = f"{row[1]:,.0f} SF" if row[1] else "? SF"
        sig['detail'] = f"Lease expiring {row[0]}: {sf} at {row[2] or 'unknown'}"
        signals.append(sig)

    conn.close()

    # Aggregate scoring
    if not signals:
        return {
            'total_score': 0,
            'signals': [],
            'space_impact_estimate': 0.0,
            'reason': 'No signals detected',
        }

    # Weighted sum of space impacts
    net_impact = sum(s['space_impact'] * s['confidence'] for s in signals)
    avg_confidence = sum(s['confidence'] for s in signals) / len(signals)

    # Normalize to 0-100 score
    raw_score = (net_impact / max(len(signals), 1)) * 100
    total_score = max(0, min(int(abs(raw_score) * 2 + len(signals) * 5), 100))

    # Build reason string
    positive = [s for s in signals if s['space_impact'] > 0]
    negative = [s for s in signals if s['space_impact'] < 0]

    reason_parts = []
    if positive:
        reason_parts.append(f"{len(positive)} expansion signal(s)")
    if negative:
        reason_parts.append(f"{len(negative)} contraction signal(s)")

    reason = f"{len(signals)} signals detected: " + ", ".join(reason_parts)

    return {
        'total_score': total_score,
        'signals': signals,
        'space_impact_estimate': round(net_impact, 2),
        'reason': reason,
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Signal Classification Engine")
    parser.add_argument("--company-id", type=int, help="Score a specific company")
    parser.add_argument("--all", action="store_true", help="Score all companies")
    args = parser.parse_args()

    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    if args.company_id:
        cur.execute("SELECT name FROM companies WHERE id = ?", (args.company_id,))
        name = cur.fetchone()
        if name:
            print(f"\nSignal Analysis: {name[0]}")
            result = score_opportunity(args.company_id, db_path)
            print(f"  Score: {result['total_score']}")
            print(f"  Space Impact: {result['space_impact_estimate']}")
            print(f"  Reason: {result['reason']}")
            for s in result['signals']:
                print(f"    [{s['signal_type']}] {s['detail']}")
                print(f"      Why: {s['why_it_matters']}")
    elif args.all:
        cur.execute("SELECT id, name FROM companies ORDER BY name")
        for cid, name in cur.fetchall():
            result = score_opportunity(cid, db_path)
            if result['total_score'] > 0:
                print(f"  {name}: {result['total_score']} ({result['reason']})")
    else:
        parser.print_help()

    conn.close()

_ENTERPRISE_KEYWORDS = [
    'blackstone', 'kkr', 'apollo', 'carlyle', 'tpg', 'warburg', 'advent',
    'citadel', 'millennium', 'point72', 'two sigma', 'de shaw', 'bridgewater',
    'elliott', 'viking', 'baupost', 'renaissance', 'aqr', 'man group',
    'goldman', 'morgan stanley', 'jpmorgan', 'bank of america', 'citi',
    'blackrock', 'vanguard', 'fidelity', 'state street', 'pimco'
]

_ENTERPRISE_SECTORS = ['hedge_fund', 'pe_vc', 'bank', 'asset_manager', 'private_equity']


def _classify_company_size(employee_count, total_raised, sector, company_name=None):
    """Classify company as startup, growth, or enterprise."""
    # Check enterprise keywords in name
    if company_name:
        name_lower = company_name.lower()
        if any(kw in name_lower for kw in _ENTERPRISE_KEYWORDS):
            return 'enterprise'
    
    # Check sector
    if sector and sector.lower() in _ENTERPRISE_SECTORS:
        return 'enterprise'
    
    # Check employee count
    if employee_count:
        if employee_count >= 500:
            return 'enterprise'
        elif employee_count >= 50:
            return 'growth'
        else:
            return 'startup'
    
    # Check total raised
    if total_raised:
        if total_raised >= 500_000_000:
            return 'enterprise'
        elif total_raised >= 100_000_000:
            return 'growth'
    
    return 'startup'  # Default to startup (higher signal value)


def score_funding_impact(funding_amount, company_id, conn=None, db_path=None):
    """
    Score funding relative to company size.
    $50M for Blackstone = noise (20). $50M for startup = transformative (90).
    """
    import sqlite3
    from graph_engine import get_db_path as _get_db_path
    
    close_conn = False
    if conn is None:
        if db_path is None:
            db_path = _get_db_path()
        try:
            conn = sqlite3.connect(db_path)
            close_conn = True
        except:
            return 50  # Default mid-score
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                employee_count,
                (SELECT SUM(amount) FROM funding_events WHERE company_id = c.id) as total_raised,
                sector,
                name
            FROM companies c WHERE id = ?
        """, (company_id,))
        row = cur.fetchone()
        
        if not row:
            return 50  # Default mid-score
        
        employee_count = row[0]
        total_raised = row[1] or 0
        sector = row[2]
        company_name = row[3]
        
        tier = _classify_company_size(employee_count, total_raised, sector, company_name)
        
        if tier == 'startup':
            if funding_amount >= 10_000_000:
                return 90  # High - likely need first real office
            elif funding_amount >= 5_000_000:
                return 70
            else:
                return 50
        elif tier == 'growth':
            if funding_amount >= 50_000_000:
                return 80  # Expansion likely
            elif funding_amount >= 20_000_000:
                return 60
            else:
                return 40
        else:  # Enterprise
            if funding_amount >= 500_000_000:
                return 70  # Major move possible
            elif funding_amount >= 200_000_000:
                return 50
            else:
                return 20  # Business as usual
    
    except Exception as e:
        return 50  # Default on error
    
    finally:
        if close_conn:
            conn.close()
