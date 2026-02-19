"""
Agency Module — Manage agency buildings, availabilities, activity, tenants, and tasks.

Core CRUD + natural language quick-input parser for rapid data entry.
"""

import os
import sys
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: F401

from core.graph_engine import get_db_path


def _conn(db_path=None):
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# =============================================================================
# QUICK INPUT PARSER
# =============================================================================

_FLOOR_RE = re.compile(r'(\d+)(?:st|nd|rd|th)?\s*(?:floor|fl)', re.IGNORECASE)
_SF_RE = re.compile(r'([\d,]+)\s*(?:rsf|sf|sq\s*ft|square\s*feet)', re.IGNORECASE)
_DATE_RE = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{2,4})')
_RENT_RE = re.compile(r'\$?([\d.]+)\s*(?:psf|/sf|per\s*sf)', re.IGNORECASE)

# Broker pattern: "with NAME (FIRM)" or "NAME / FIRM" or "NAME, FIRM"
_BROKER_RE = re.compile(
    r'(?:with\s+)?([A-Z][.\w\s]+?)\s*[\(/]\s*([A-Za-z&\s]+?)\s*[\)]?$',
    re.IGNORECASE
)
# Simpler: "with FIRM" at end
_FIRM_ONLY_RE = re.compile(r'with\s+([A-Za-z&\s]+?)$', re.IGNORECASE)


def _parse_floor(text):
    m = _FLOOR_RE.search(text)
    return m.group(1) if m else None


def _parse_sf(text):
    m = _SF_RE.search(text)
    if m:
        return int(m.group(1).replace(',', ''))
    return None


def _parse_date(text):
    m = _DATE_RE.search(text)
    if m:
        month, day, year = m.group(1), m.group(2), m.group(3)
        if len(year) == 2:
            year = '20' + year
        return f"{year}-{int(month):02d}-{int(day):02d}"
    return None


def _parse_rent(text):
    m = _RENT_RE.search(text)
    return float(m.group(1)) if m else None


def _parse_broker(text):
    m = _BROKER_RE.search(text)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m = _FIRM_ONLY_RE.search(text)
    if m:
        return None, m.group(1).strip()
    return None, None


def parse_quick_input(text: str) -> Dict:
    """
    Parse natural language input into structured data.

    Returns dict with 'type' key and relevant fields.
    """
    text = text.strip()
    lower = text.lower()

    # New building
    if lower.startswith('add building:') or lower.startswith('new building:'):
        parts = text.split(':', 1)[1].strip().split(',')
        result = {'type': 'new_building', 'address': parts[0].strip()}
        for p in parts[1:]:
            p = p.strip().lower()
            if p in ('watchlist', 'active_agency', 'pitch', 'active'):
                result['building_type'] = 'active_agency' if p == 'active' else p
            elif p in ('sublease', 'direct', 'disposal', 'extension',
                        'sublease/direct ext'):
                result['deal_type'] = p
            else:
                result['client'] = p.strip()
        return result

    # Toggle building type
    if lower.startswith('move ') and ' to ' in lower:
        m = re.match(r'move\s+(.+?)\s+to\s+(active|watchlist|pitch)', lower)
        if m:
            return {
                'type': 'toggle_building',
                'building': m.group(1).strip(),
                'new_type': 'active_agency' if m.group(2) == 'active' else m.group(2),
            }

    # Availability
    if any(kw in lower for kw in ['available', 'rsf', ' sf,', 'sq ft', 'vacancy']):
        floor = _parse_floor(text)
        sf = _parse_sf(text)
        avail_date = _parse_date(text)
        rent = _parse_rent(text)
        if floor or sf:
            return {
                'type': 'availability',
                'floor': floor,
                'sf': sf,
                'available_date': avail_date,
                'asking_rent': rent,
            }

    # Proposal
    if 'proposal' in lower:
        floor = _parse_floor(text)
        broker_name, broker_firm = _parse_broker(text)
        sf = _parse_sf(text)
        # Extract company — typically after "proposal" and before "with"
        company = None
        m = re.search(r'proposal\s+(?:submitted\s+)?(?:on\s+)?(?:\d+\w*\s*(?:floor|fl)\s*,?\s*)?(.+?)(?:\s+with\s+|\s*[\(/])', text, re.IGNORECASE)
        if m:
            company = m.group(1).strip().rstrip(',')
        return {
            'type': 'proposal',
            'floor': floor,
            'company': company,
            'broker_name': broker_name,
            'broker_firm': broker_firm,
            'sf': sf,
        }

    # Tour
    if 'tour' in lower:
        floor = _parse_floor(text)
        broker_name, broker_firm = _parse_broker(text)
        company = None
        m = re.search(r'tour\s+(?:scheduled\s+)?(?:on\s+)?(?:\d+\w*\s*(?:floor|fl)\s*,?\s*)?(.+?)(?:\s+with\s+|\s*[\(/])', text, re.IGNORECASE)
        if m:
            company = m.group(1).strip().rstrip(',')
        return {
            'type': 'tour',
            'floor': floor,
            'company': company,
            'broker_name': broker_name,
            'broker_firm': broker_firm,
        }

    # LOI
    if 'loi' in lower:
        floor = _parse_floor(text)
        company = None
        m = re.search(r'loi\s+(?:from\s+|submitted\s+(?:by\s+)?)?(.+?)(?:\s+(?:on|for)\s+|\s*$)', text, re.IGNORECASE)
        if m:
            company = m.group(1).strip()
        return {'type': 'loi', 'floor': floor, 'company': company}

    # Task — "COMPANY: task text" pattern
    if ':' in text and not lower.startswith(('add ', 'new ', 'move ', 'show ')):
        parts = text.split(':', 1)
        tenant = parts[0].strip()
        task_text = parts[1].strip()
        if len(tenant) < 60 and len(task_text) > 3:
            task_type = 'follow_up'
            if 'attorney' in lower or 'legal' in lower:
                task_type = 'legal'
            elif 'meeting' in lower:
                task_type = 'meeting'
            elif 'tour' in lower:
                task_type = 'tour'
            elif 'proposal' in lower:
                task_type = 'proposal'
            return {
                'type': 'task',
                'tenant': tenant,
                'task': task_text,
                'task_type': task_type,
            }

    # Fallback — treat as note/other
    return {'type': 'other', 'text': text}


# =============================================================================
# BUILDING CRUD
# =============================================================================

def add_building(address, building_type='watchlist', deal_type=None,
                 client_name=None, name=None, submarket=None,
                 target_sf_min=None, target_sf_max=None, db_path=None):
    """Add new building to tracking."""
    conn = _conn(db_path)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO agency_buildings
                (address, name, building_type, deal_type, client_name,
                 submarket, target_sf_min, target_sf_max)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (address, name or address, building_type, deal_type, client_name,
              submarket, target_sf_min, target_sf_max))
        bid = cur.lastrowid
        conn.commit()
        return bid
    except sqlite3.IntegrityError:
        return None  # Already exists
    finally:
        conn.close()


def toggle_building_type(building_id, db_path=None):
    """Switch between active_agency and watchlist."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT building_type FROM agency_buildings WHERE id = ?", (building_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None
    new_type = 'watchlist' if row['building_type'] == 'active_agency' else 'active_agency'
    cur.execute("UPDATE agency_buildings SET building_type = ? WHERE id = ?",
                (new_type, building_id))
    conn.commit()
    conn.close()
    return new_type


def get_buildings_by_type(building_type='active_agency', db_path=None):
    """Get buildings filtered by type."""
    conn = _conn(db_path)
    cur = conn.cursor()
    if building_type == 'all':
        cur.execute("SELECT * FROM agency_buildings ORDER BY building_type, address")
    else:
        cur.execute("SELECT * FROM agency_buildings WHERE building_type = ? ORDER BY address",
                    (building_type,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def check_building_exists(address, db_path=None):
    """Check if building already in system."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM agency_buildings WHERE LOWER(address) = LOWER(?)", (address,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def remove_building(building_id, db_path=None):
    """Remove building from tracking."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("DELETE FROM agency_buildings WHERE id = ?", (building_id,))
    conn.commit()
    conn.close()


# =============================================================================
# AVAILABILITIES
# =============================================================================

def add_availability(building_id, floor, square_feet, available_date=None,
                     asking_rent=None, status='available', notes=None, db_path=None):
    """Add availability to a building."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO agency_availabilities
            (building_id, floor, square_feet, available_date, asking_rent, status, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (building_id, floor, square_feet, available_date, asking_rent, status, notes))
    aid = cur.lastrowid
    conn.commit()
    conn.close()
    return aid


def get_availabilities(building_id, status=None, db_path=None):
    """Get availabilities for a building."""
    conn = _conn(db_path)
    cur = conn.cursor()
    if status:
        cur.execute("""
            SELECT * FROM agency_availabilities
            WHERE building_id = ? AND status = ?
            ORDER BY CAST(floor AS INTEGER)
        """, (building_id, status))
    else:
        cur.execute("""
            SELECT * FROM agency_availabilities
            WHERE building_id = ?
            ORDER BY CAST(floor AS INTEGER)
        """, (building_id,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def update_availability_status(avail_id, new_status, db_path=None):
    """Update availability status."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("UPDATE agency_availabilities SET status = ? WHERE id = ?",
                (new_status, avail_id))
    conn.commit()
    conn.close()


# =============================================================================
# ACTIVITY
# =============================================================================

def add_activity(building_id, activity_type, company_name=None, floor=None,
                 broker_name=None, broker_firm=None, square_feet=None,
                 notes=None, company_id=None, activity_date=None, db_path=None):
    """Log activity (proposal, tour, LOI, etc.)."""
    conn = _conn(db_path)
    cur = conn.cursor()
    if activity_date is None:
        activity_date = datetime.now().strftime("%Y-%m-%d")
    cur.execute("""
        INSERT INTO agency_activity
            (building_id, activity_type, company_name, company_id, floor,
             broker_name, broker_firm, square_feet, notes, activity_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (building_id, activity_type, company_name, company_id, floor,
          broker_name, broker_firm, square_feet, notes, activity_date))
    aid = cur.lastrowid
    conn.commit()
    conn.close()
    return aid


def get_activity(building_id=None, activity_type=None, limit=20, db_path=None):
    """Get activity log, optionally filtered."""
    conn = _conn(db_path)
    cur = conn.cursor()
    query = "SELECT * FROM agency_activity WHERE 1=1"
    params = []
    if building_id:
        query += " AND building_id = ?"
        params.append(building_id)
    if activity_type:
        query += " AND activity_type = ?"
        params.append(activity_type)
    query += " ORDER BY activity_date DESC, created_at DESC LIMIT ?"
    params.append(limit)
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# =============================================================================
# TASKS
# =============================================================================

def add_task(building_id=None, tenant_or_company=None, task_text='',
             task_type='follow_up', priority='medium', due_date=None,
             assigned_to=None, db_path=None):
    """Add a task."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO agency_tasks
            (building_id, tenant_or_company, task_text, task_type,
             priority, due_date, assigned_to)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (building_id, tenant_or_company, task_text, task_type,
          priority, due_date, assigned_to))
    tid = cur.lastrowid
    conn.commit()
    conn.close()
    return tid


def get_tasks(building_id=None, status='pending', db_path=None):
    """Get tasks, optionally filtered by building and status."""
    conn = _conn(db_path)
    cur = conn.cursor()
    query = "SELECT * FROM agency_tasks WHERE 1=1"
    params = []
    if building_id:
        query += " AND building_id = ?"
        params.append(building_id)
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, due_date"
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def mark_task_done(task_id, notes='', db_path=None):
    """Mark a task as done."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        UPDATE agency_tasks
        SET status = 'done', completed_at = ?, notes = COALESCE(notes || ' | ', '') || ?
        WHERE id = ?
    """, (datetime.now().strftime("%Y-%m-%d %H:%M"), notes, task_id))
    conn.commit()
    conn.close()


# =============================================================================
# TENANTS
# =============================================================================

def get_tenant_roll(building_id, db_path=None):
    """Get all tenants for a building."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM agency_tenants
        WHERE building_id = ?
        ORDER BY CAST(floor AS INTEGER)
    """, (building_id,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_expiring_tenants(building_id=None, months_ahead=18, db_path=None):
    """Get tenants with leases expiring within N months."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cutoff = (datetime.now() + timedelta(days=months_ahead * 30)).strftime("%Y-%m-%d")
    query = """
        SELECT t.*, b.address, b.name as building_name
        FROM agency_tenants t
        JOIN agency_buildings b ON t.building_id = b.id
        WHERE t.lease_expiry IS NOT NULL
        AND t.lease_expiry <= ?
        AND t.lease_expiry >= date('now')
    """
    params = [cutoff]
    if building_id:
        query += " AND t.building_id = ?"
        params.append(building_id)
    query += " ORDER BY t.lease_expiry"
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# =============================================================================
# BUILDING SUMMARY
# =============================================================================

def get_building_summary(building_id, db_path=None):
    """Get comprehensive building summary."""
    conn = _conn(db_path)
    cur = conn.cursor()

    cur.execute("SELECT * FROM agency_buildings WHERE id = ?", (building_id,))
    building = dict(cur.fetchone()) if cur.fetchone else None
    if not building:
        conn.close()
        return None

    # Re-fetch since fetchone consumed it
    cur.execute("SELECT * FROM agency_buildings WHERE id = ?", (building_id,))
    building = dict(cur.fetchone())

    # Availabilities
    cur.execute("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(square_feet), 0) as total_sf
        FROM agency_availabilities WHERE building_id = ? AND status = 'available'
    """, (building_id,))
    avail = dict(cur.fetchone())

    # Tenants
    cur.execute("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(square_feet), 0) as total_sf
        FROM agency_tenants WHERE building_id = ? AND status = 'active'
    """, (building_id,))
    tenants = dict(cur.fetchone())

    # Tasks
    cur.execute("SELECT COUNT(*) FROM agency_tasks WHERE building_id = ? AND status = 'pending'",
                (building_id,))
    pending_tasks = cur.fetchone()[0]

    # Recent activity
    cur.execute("""
        SELECT COUNT(*) FROM agency_activity
        WHERE building_id = ? AND activity_date >= date('now', '-30 days')
    """, (building_id,))
    recent_activity = cur.fetchone()[0]

    # Expiring tenants (18 months)
    cutoff = (datetime.now() + timedelta(days=540)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT COUNT(*) FROM agency_tenants
        WHERE building_id = ? AND lease_expiry <= ? AND lease_expiry >= date('now')
    """, (building_id, cutoff))
    expiring = cur.fetchone()[0]

    conn.close()

    total_sf = building.get('total_sf') or 0
    occupied_sf = tenants['total_sf']
    available_sf = avail['total_sf']
    occupancy = (occupied_sf / total_sf * 100) if total_sf > 0 else 0

    return {
        **building,
        'available_spaces': avail['cnt'],
        'available_sf': available_sf,
        'tenant_count': tenants['cnt'],
        'occupied_sf': occupied_sf,
        'occupancy_pct': round(occupancy, 1),
        'pending_tasks': pending_tasks,
        'recent_activity_30d': recent_activity,
        'expiring_tenants_18m': expiring,
    }


# =============================================================================
# MARKET REQUIREMENTS
# =============================================================================

def add_market_requirement(company_name, sf_min=None, sf_max=None,
                           submarket=None, move_date=None, broker_name=None,
                           broker_firm=None, source=None, company_id=None,
                           notes=None, db_path=None):
    """Add a market requirement."""
    conn = _conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO market_requirements
            (company_name, company_id, requirement_sf_min, requirement_sf_max,
             target_submarket, target_move_date, broker_name, broker_firm,
             source, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (company_name, company_id, sf_min, sf_max, submarket, move_date,
          broker_name, broker_firm, source, notes))
    rid = cur.lastrowid
    conn.commit()
    conn.close()
    return rid


def get_market_requirements(status='active', db_path=None):
    """Get active market requirements."""
    conn = _conn(db_path)
    cur = conn.cursor()
    if status:
        cur.execute("SELECT * FROM market_requirements WHERE status = ? ORDER BY company_name",
                    (status,))
    else:
        cur.execute("SELECT * FROM market_requirements ORDER BY company_name")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


if __name__ == "__main__":
    # Quick test
    buildings = get_buildings_by_type('all')
    print(f"Buildings tracked: {len(buildings)}")
    for b in buildings:
        print(f"  {b['name']} ({b['building_type']})")

    tasks = get_tasks()
    print(f"\nPending tasks: {len(tasks)}")

    # Test parser
    tests = [
        "24th floor, 5,374 RSF, available 12/1/2026",
        "proposal submitted on 38th floor, IMC/Polar with G. Marans (Savills)",
        "tour scheduled 42nd floor, Citadel with CBRE",
        "IMC/Polar: Set up meeting between attorneys",
        "Add building: 100 Park Ave, watchlist, sublease, ABC Corp",
        "Move 30 HY to active",
    ]
    print("\nParser tests:")
    for t in tests:
        result = parse_quick_input(t)
        print(f"  \"{t}\"")
        print(f"    → {result}")


def parse_update_input(text, building_id=None):
    """
    Parse quick update inputs:
    - "Floor 24 asking rent now $95" → update asking_rent
    - "Floor 38 leased" → update status to 'leased'
    - "Floor 42 in negotiation" → update status
    - "Floor 24 available 3/1/2027" → update available_date
    """
    import re
    from graph_engine import get_db_path
    import sqlite3
    
    text_lower = text.lower()
    result = {'type': 'update', 'success': False, 'message': ''}
    
    # Extract floor
    floor_match = re.search(r'floor\s*(\d+)', text_lower)
    if not floor_match:
        result['message'] = 'Could not find floor number'
        return result
    
    floor = floor_match.group(1)
    
    conn = sqlite3.connect(get_db_path())
    cur = conn.cursor()
    
    # Find the availability
    if building_id:
        cur.execute("SELECT id FROM agency_availabilities WHERE building_id = ? AND floor LIKE ?", 
                    (building_id, f"%{floor}%"))
    else:
        cur.execute("SELECT id FROM agency_availabilities WHERE floor LIKE ?", (f"%{floor}%",))
    
    row = cur.fetchone()
    if not row:
        result['message'] = f'No availability found for floor {floor}'
        conn.close()
        return result
    
    avail_id = row[0]
    
    # Check for rent update
    rent_match = re.search(r'\$\s*(\d+(?:\.\d+)?)', text)
    if rent_match and ('rent' in text_lower or 'asking' in text_lower):
        new_rent = float(rent_match.group(1))
        cur.execute("UPDATE agency_availabilities SET asking_rent = ? WHERE id = ?", (new_rent, avail_id))
        conn.commit()
        result['success'] = True
        result['message'] = f'Updated floor {floor} asking rent to ${new_rent}'
        conn.close()
        return result
    
    # Check for status update
    if 'leased' in text_lower:
        cur.execute("UPDATE agency_availabilities SET status = 'leased' WHERE id = ?", (avail_id,))
        conn.commit()
        result['success'] = True
        result['message'] = f'Floor {floor} marked as leased'
    elif 'in negotiation' in text_lower or 'negotiation' in text_lower:
        cur.execute("UPDATE agency_availabilities SET status = 'in_negotiation' WHERE id = ?", (avail_id,))
        conn.commit()
        result['success'] = True
        result['message'] = f'Floor {floor} marked as in negotiation'
    elif 'available' in text_lower:
        # Check for date
        date_match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
        if date_match:
            month, day, year = date_match.groups()
            new_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            cur.execute("UPDATE agency_availabilities SET available_date = ?, status = 'coming_available' WHERE id = ?", 
                        (new_date, avail_id))
            result['message'] = f'Floor {floor} available date set to {new_date}'
        else:
            cur.execute("UPDATE agency_availabilities SET status = 'available' WHERE id = ?", (avail_id,))
            result['message'] = f'Floor {floor} marked as available'
        conn.commit()
        result['success'] = True
    
    conn.close()
    return result
