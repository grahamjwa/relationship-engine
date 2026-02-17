"""
Import buildings and leases from CSV.
Links buildings to companies by name match.
"""

import os
import sys
import csv
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
  # noqa: F401

from graph_engine import get_db_path


def import_buildings_csv(filepath, db_path=None):
    """
    Import buildings and lease data.

    Expected CSV columns:
        company_name, building_address, floor, square_feet,
        lease_start, lease_expiry, rent_psf, city, state

    Returns:
        dict with 'buildings' and 'leases' counts
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    buildings_added = 0
    leases_added = 0
    skipped = 0

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            company_name = row.get('company_name', '').strip()
            address = row.get('building_address', '').strip()
            floor = row.get('floor', '').strip()
            sf = row.get('square_feet', '').strip()
            start = row.get('lease_start', '').strip()
            expiry = row.get('lease_expiry', '').strip()
            rent = row.get('rent_psf', '').strip()
            city = row.get('city', 'New York').strip()
            state = row.get('state', 'NY').strip()

            if not company_name or not address:
                skipped += 1
                continue

            # Find company (case-insensitive partial match)
            cur.execute("SELECT id FROM companies WHERE LOWER(name) LIKE LOWER(?)",
                        (f"%{company_name}%",))
            company_row = cur.fetchone()
            if not company_row:
                skipped += 1
                continue
            company_id = company_row[0]

            # Get or create building
            cur.execute("SELECT id FROM buildings WHERE address = ?", (address,))
            building_row = cur.fetchone()
            if building_row:
                building_id = building_row[0]
            else:
                cur.execute("""
                    INSERT INTO buildings (address, name, city, state)
                    VALUES (?, ?, ?, ?)
                """, (address, address, city, state))
                building_id = cur.lastrowid
                buildings_added += 1

            # Add lease (skip if duplicate company+building)
            cur.execute("""
                SELECT id FROM leases
                WHERE company_id = ? AND building_id = ?
            """, (company_id, building_id))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO leases
                        (company_id, building_id, floor, square_feet,
                         lease_start, lease_expiry, rent_psf)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (company_id, building_id,
                      floor or None,
                      int(sf) if sf else None,
                      start or None,
                      expiry or None,
                      float(rent) if rent else None))
                leases_added += 1

    conn.commit()
    conn.close()
    return {'buildings': buildings_added, 'leases': leases_added, 'skipped': skipped}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Import buildings/leases CSV")
    parser.add_argument("filepath", help="Path to buildings CSV")
    args = parser.parse_args()

    result = import_buildings_csv(args.filepath)
    print(f"Buildings: {result['buildings']}, Leases: {result['leases']}, "
          f"Skipped: {result['skipped']}")
