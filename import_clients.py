"""
Import client list with deal history.
Updates company status and creates monitored_clients entries.
"""

import os
import sys
import csv
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
  # noqa: F401

from graph_engine import get_db_path


def import_clients_csv(filepath, db_path=None):
    """
    Import clients and update company status / monitored_clients.

    Expected CSV columns:
        company_name, status, last_deal_date, last_deal_sf, deal_value,
        relationship_owner, check_in_frequency

    Returns:
        dict with 'imported' and 'updated' counts
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    imported = 0
    updated = 0

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('company_name', '').strip()
            status = row.get('status', 'active_client').strip()
            last_deal = row.get('last_deal_date', '').strip()
            frequency = row.get('check_in_frequency', 'monthly').strip()

            if not name:
                continue

            # Check if company exists
            cur.execute("SELECT id FROM companies WHERE LOWER(name) = LOWER(?)", (name,))
            row_result = cur.fetchone()

            if row_result:
                company_id = row_result[0]
                cur.execute("UPDATE companies SET status = ? WHERE id = ?",
                            (status, company_id))
                updated += 1
            else:
                cur.execute("""
                    INSERT INTO companies (name, type, status, sector)
                    VALUES (?, 'tenant', ?, 'unknown')
                """, (name, status))
                company_id = cur.lastrowid
                imported += 1

            # Add to monitored_clients if active and has deal date
            if status == 'active_client' and last_deal:
                # Check if monitored_clients table exists
                cur.execute("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='monitored_clients'
                """)
                if cur.fetchone():
                    cur.execute("SELECT id FROM monitored_clients WHERE company_id = ?",
                                (company_id,))
                    if not cur.fetchone():
                        cur.execute("""
                            INSERT INTO monitored_clients
                                (company_id, last_deal_date, check_in_frequency)
                            VALUES (?, ?, ?)
                        """, (company_id, last_deal, frequency))

    conn.commit()
    conn.close()
    return {'imported': imported, 'updated': updated}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Import client CSV")
    parser.add_argument("filepath", help="Path to clients CSV")
    args = parser.parse_args()

    result = import_clients_csv(args.filepath)
    print(f"Imported: {result['imported']}, Updated: {result['updated']}")
