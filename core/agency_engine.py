"""
Agency Intelligence Engine
===========================
Matches market requirements to building availabilities,
scans signals for potential space needs, and generates alerts.
"""

import os
import sys
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path


def match_market_to_building(building_id, tolerance=0.20, db_path=None):
    """
    Find market requirements matching a building's availabilities within tolerance.

    A match occurs when:
      availability_sf * (1 - tolerance) <= requirement_sf <= availability_sf * (1 + tolerance)
    or the requirement range overlaps the availability.

    Returns list of dicts with company, requirement, matched availability info.
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get available spaces
    cur.execute("""
        SELECT id, floor, square_feet, available_date, asking_rent
        FROM agency_availabilities
        WHERE building_id = ? AND status IN ('available', 'coming_available')
        ORDER BY CAST(floor AS INTEGER)
    """, (building_id,))
    availabilities = [dict(r) for r in cur.fetchall()]

    if not availabilities:
        conn.close()
        return []

    # Get active market requirements
    cur.execute("""
        SELECT id, company_name, company_id, requirement_sf_min, requirement_sf_max,
               target_submarket, target_move_date, broker_name, broker_firm, source, notes
        FROM market_requirements
        WHERE status IN ('active', 'touring')
    """)
    requirements = [dict(r) for r in cur.fetchall()]

    conn.close()

    matches = []
    for req in requirements:
        req_min = req['requirement_sf_min'] or 0
        req_max = req['requirement_sf_max'] or req_min
        if req_min == 0 and req_max == 0:
            continue

        matched_floors = []
        for avail in availabilities:
            avail_sf = avail['square_feet']
            low = avail_sf * (1 - tolerance)
            high = avail_sf * (1 + tolerance)

            # Match if requirement range overlaps tolerance range
            if req_min <= high and req_max >= low:
                matched_floors.append({
                    'availability_id': avail['id'],
                    'floor': avail['floor'],
                    'available_sf': avail_sf,
                    'available_date': avail['available_date'],
                    'asking_rent': avail['asking_rent'],
                })

        if matched_floors:
            matches.append({
                'requirement_id': req['id'],
                'company': req['company_name'],
                'company_id': req['company_id'],
                'sf_min': req_min,
                'sf_max': req_max,
                'broker_name': req['broker_name'],
                'broker_firm': req['broker_firm'],
                'source': req['source'],
                'move_date': req['target_move_date'],
                'matched_floors': matched_floors,
            })

    return matches


def match_market_to_all_buildings(tolerance=0.20, db_path=None):
    """Run matching against all active agency buildings."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT id, address, name FROM agency_buildings
        WHERE building_type IN ('active_agency', 'watchlist')
    """)
    buildings = [dict(r) for r in cur.fetchall()]
    conn.close()

    all_matches = {}
    for b in buildings:
        matches = match_market_to_building(b['id'], tolerance, db_path)
        if matches:
            all_matches[b['name']] = {
                'building_id': b['id'],
                'address': b['address'],
                'matches': matches,
            }
    return all_matches


def scan_signals_for_requirements(db_path=None):
    """
    Scan hiring_signals and funding_events for companies that might need space.
    Returns companies with recent growth signals not already in market_requirements.
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Companies with recent funding (30 days) not already in market_requirements
    cur.execute("""
        SELECT c.id, c.name, c.sector, c.status, f.amount, f.round_type, f.event_date
        FROM companies c
        JOIN funding_events f ON c.id = f.company_id
        WHERE f.event_date >= date('now', '-30 days')
        AND c.id NOT IN (SELECT company_id FROM market_requirements WHERE company_id IS NOT NULL)
        AND f.amount >= 20000000
        ORDER BY f.amount DESC
    """)
    funded = [dict(r) for r in cur.fetchall()]

    # Companies with high hiring signals (30 days)
    cur.execute("""
        SELECT c.id, c.name, c.sector, c.status, COUNT(h.id) as signal_count
        FROM companies c
        JOIN hiring_signals h ON c.id = h.company_id
        WHERE h.signal_date >= date('now', '-30 days')
        AND h.relevance IN ('high', 'medium')
        AND c.id NOT IN (SELECT company_id FROM market_requirements WHERE company_id IS NOT NULL)
        GROUP BY c.id
        HAVING signal_count >= 2
        ORDER BY signal_count DESC
    """)
    hiring = [dict(r) for r in cur.fetchall()]

    conn.close()

    prospects = []
    seen = set()
    for f in funded:
        if f['id'] not in seen:
            prospects.append({
                'company_id': f['id'],
                'company_name': f['name'],
                'reason': f"Raised ${f['amount']:,.0f} ({f['round_type']}) on {f['event_date']}",
                'source': 'funding_signal',
            })
            seen.add(f['id'])

    for h in hiring:
        if h['id'] not in seen:
            prospects.append({
                'company_id': h['id'],
                'company_name': h['name'],
                'reason': f"{h['signal_count']} hiring signals in 30 days",
                'source': 'hiring_signal',
            })
            seen.add(h['id'])

    return prospects


def alert_new_matches(building_id, db_path=None):
    """
    Find new matches since last check (activity not logged for this company+building).
    For Discord/notification alerts.
    """
    if db_path is None:
        db_path = get_db_path()

    matches = match_market_to_building(building_id, db_path=db_path)
    if not matches:
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    new_matches = []
    for m in matches:
        # Check if we already have activity for this company at this building
        cur.execute("""
            SELECT COUNT(*) FROM agency_activity
            WHERE building_id = ? AND LOWER(company_name) = LOWER(?)
        """, (building_id, m['company']))
        count = cur.fetchone()[0]
        if count == 0:
            new_matches.append(m)

    conn.close()
    return new_matches


def get_agency_briefing_data(db_path=None):
    """Get agency-specific data for morning briefing."""
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    data = {}

    # Tasks due today or overdue
    cur.execute("""
        SELECT t.*, b.name as building_name
        FROM agency_tasks t
        LEFT JOIN agency_buildings b ON t.building_id = b.id
        WHERE t.status IN ('pending', 'in_progress')
        AND (t.due_date IS NULL OR t.due_date <= date('now'))
        ORDER BY t.priority = 'high' DESC, t.due_date
        LIMIT 10
    """)
    data['tasks_due'] = [dict(r) for r in cur.fetchall()]

    # Recent activity (7 days)
    cur.execute("""
        SELECT a.*, b.name as building_name
        FROM agency_activity a
        JOIN agency_buildings b ON a.building_id = b.id
        WHERE a.activity_date >= date('now', '-7 days')
        ORDER BY a.activity_date DESC
        LIMIT 10
    """)
    data['recent_activity'] = [dict(r) for r in cur.fetchall()]

    # Expiring tenants (6 months)
    cutoff = (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT t.*, b.name as building_name
        FROM agency_tenants t
        JOIN agency_buildings b ON t.building_id = b.id
        WHERE t.lease_expiry IS NOT NULL
        AND t.lease_expiry <= ?
        AND t.lease_expiry >= date('now')
        ORDER BY t.lease_expiry
        LIMIT 5
    """, (cutoff,))
    data['expiring_soon'] = [dict(r) for r in cur.fetchall()]

    conn.close()

    # New matches across all buildings
    all_matches = match_market_to_all_buildings(db_path=db_path)
    match_count = sum(len(v['matches']) for v in all_matches.values())
    data['new_market_matches'] = match_count
    data['match_details'] = all_matches

    return data


if __name__ == "__main__":
    print("Agency Intelligence Engine")
    print("=" * 40)

    # Matches
    all_matches = match_market_to_all_buildings()
    if all_matches:
        for bname, info in all_matches.items():
            print(f"\n{bname}:")
            for m in info['matches']:
                floors = ", ".join(f"Fl {f['floor']} ({f['available_sf']:,} SF)"
                                   for f in m['matched_floors'])
                print(f"  {m['company']} ({m['sf_min']:,}-{m['sf_max']:,} SF) → {floors}")
    else:
        print("No market matches found.")

    # Signal scan
    print("\nSignal-based prospects:")
    prospects = scan_signals_for_requirements()
    for p in prospects[:5]:
        print(f"  {p['company_name']}: {p['reason']}")
