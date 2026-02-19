"""
Comps Import - Maps Graham's Excel format to lease_comps table

Excel columns:
- Date (short date - date signed)
- Tenant (tenant name)
- Address (building address)
- Floors (E=entire, P=partial, e.g., "P4, E5-11")
- RSF (square feet)
- Term (years with 'y', e.g., "3.5y")
- Base Rent ($/RSF)
- Months Free (integer)
- TI ($ amount)
- Type (direct expansion, direct renewal, direct lease, sublease, etc.)
- Market (Midtown, Midtown South, Downtown, New Jersey, Brooklyn)
- Submarket (further breakdown)

Dedupe logic: Flag if same tenant + same address + same floors + similar SF
(within 10%) + within 1 year
"""

import os
import sys
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from core.graph_engine import get_db_path


def parse_term(term_str):
    """Parse term like '3.5y' to months."""
    if not term_str:
        return None
    term_str = str(term_str).strip().lower().replace('y', '')
    try:
        years = float(term_str)
        return int(years * 12)
    except Exception:
        return None


def parse_floors(floor_str):
    """Parse floors like 'P4, E5-11' - keep as-is for storage."""
    if not floor_str:
        return None
    return str(floor_str).strip()


def check_duplicate(cur, tenant, address, floors, sf, date_signed):
    """
    Check for potential duplicate:
    - Same tenant name (fuzzy)
    - Same address
    - Same floors
    - Similar SF (within 10%)
    - Within 1 year of each other
    """
    if not all([tenant, address, date_signed]):
        return None

    # Parse date
    if isinstance(date_signed, str):
        try:
            date_obj = datetime.strptime(date_signed, '%Y-%m-%d')
        except Exception:
            return None
    else:
        date_obj = date_signed

    date_min = (date_obj - timedelta(days=365)).strftime('%Y-%m-%d')
    date_max = (date_obj + timedelta(days=365)).strftime('%Y-%m-%d')

    sf_min = int(sf * 0.9) if sf else 0
    sf_max = int(sf * 1.1) if sf else 999999999

    cur.execute("""
        SELECT id, tenant_name, building_address, floor, square_feet, commencement_date
        FROM lease_comps
        WHERE LOWER(tenant_name) LIKE LOWER(?)
        AND LOWER(building_address) = LOWER(?)
        AND (floor = ? OR ? IS NULL)
        AND square_feet BETWEEN ? AND ?
        AND commencement_date BETWEEN ? AND ?
    """, (f"%{tenant}%", address, floors, floors, sf_min, sf_max, date_min, date_max))

    return cur.fetchone()


def import_comps_excel(filepath, db_path=None, skip_dupes=False):
    """
    Import comps from Graham's Excel format.

    Returns dict with:
    - imported: count of imported records
    - duplicates: list of potential duplicates found
    - errors: list of rows that failed
    """
    if db_path is None:
        db_path = get_db_path()

    # Read Excel
    df = pd.read_excel(filepath)

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    imported = 0
    duplicates = []
    errors = []

    for idx, row in df.iterrows():
        try:
            # Extract values
            date_val = row.get('date')
            if pd.notna(date_val):
                if isinstance(date_val, datetime):
                    date_str = date_val.strftime('%Y-%m-%d')
                else:
                    date_str = str(date_val)
            else:
                date_str = None

            tenant = str(row.get('tenant', '')).strip() if pd.notna(row.get('tenant')) else None
            address = str(row.get('address', '')).strip() if pd.notna(row.get('address')) else None
            floors = parse_floors(row.get('floors'))
            sf = int(row.get('rsf')) if pd.notna(row.get('rsf')) else None
            term_months = parse_term(row.get('term'))
            base_rent = float(row.get('base rent')) if pd.notna(row.get('base rent')) else None
            months_free = int(row.get('months free')) if pd.notna(row.get('months free')) else None
            ti = float(row.get('ti')) if pd.notna(row.get('ti')) else None
            lease_type = str(row.get('type', '')).strip() if pd.notna(row.get('type')) else None
            market = str(row.get('market', '')).strip() if pd.notna(row.get('market')) else None
            submarket = str(row.get('submarket', '')).strip() if pd.notna(row.get('submarket')) else None

            if not tenant or not address:
                errors.append({'row': idx + 2, 'reason': 'Missing tenant or address'})
                continue

            # Check for duplicate
            dupe = check_duplicate(cur, tenant, address, floors, sf, date_str)
            if dupe:
                duplicates.append({
                    'row': idx + 2,
                    'new': {'tenant': tenant, 'address': address, 'floors': floors,
                            'sf': sf, 'date': date_str},
                    'existing_id': dupe[0],
                    'existing': {'tenant': dupe[1], 'address': dupe[2],
                                 'floors': dupe[3], 'sf': dupe[4], 'date': dupe[5]}
                })
                if skip_dupes:
                    continue

            # Insert
            cur.execute("""
                INSERT INTO lease_comps
                (building_address, tenant_name, floor, square_feet, lease_type,
                 commencement_date, term_months, starting_rent, free_rent_months,
                 ti_allowance, market, submarket, source, source_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'excel_import', date('now'))
            """, (address, tenant, floors, sf, lease_type, date_str, term_months,
                  base_rent, months_free, ti, market, submarket))

            imported += 1

        except Exception as e:
            errors.append({'row': idx + 2, 'reason': str(e)})

    conn.commit()
    conn.close()

    return {
        'imported': imported,
        'duplicates': duplicates,
        'errors': errors
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python comps_import.py <excel_file>")
        print("  --skip-dupes    Skip rows that look like duplicates")
        sys.exit(1)

    skip = '--skip-dupes' in sys.argv
    filepath = [a for a in sys.argv[1:] if not a.startswith('--')][0]

    result = import_comps_excel(filepath, skip_dupes=skip)
    print(f"Imported: {result['imported']}")
    if result['duplicates']:
        print(f"Potential duplicates found: {len(result['duplicates'])}")
        for d in result['duplicates']:
            print(f"  Row {d['row']}: {d['new']['tenant']} @ {d['new']['address']}")
    if result['errors']:
        print(f"Errors: {len(result['errors'])}")
        for e in result['errors']:
            print(f"  Row {e['row']}: {e['reason']}")
