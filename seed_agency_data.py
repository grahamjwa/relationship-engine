"""
Seed Agency Data — Insert initial buildings, availabilities, activity, tasks,
tenants, and market requirements for the agency module.

Run: python3 seed_agency_data.py
"""

import os
import sys
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: F401

from core.graph_engine import get_db_path
from agency import (
    add_building, add_availability, add_activity, add_task,
    add_market_requirement,
)


def seed(db_path=None):
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Check if already seeded
    cur.execute("SELECT COUNT(*) FROM agency_buildings")
    if cur.fetchone()[0] > 0:
        print("Agency data already seeded. Skipping.")
        conn.close()
        return

    conn.close()

    # ── BUILDINGS ─────────────────────────────────────────────────────────
    # Active agency
    b1 = add_building(
        '1095 Avenue of the Americas', building_type='active_agency',
        name='1095 AoA', submarket='Midtown',
        target_sf_min=15000, target_sf_max=20000, db_path=db_path)
    b2 = add_building(
        '346 Madison Avenue', building_type='active_agency',
        name='346 Madison', submarket='Midtown', db_path=db_path)

    # Watchlist
    b3 = add_building(
        '30 Hudson Yards', building_type='watchlist',
        name='30 HY', deal_type='sublease/direct ext',
        client_name='WBD', submarket='Hudson Yards', db_path=db_path)
    b4 = add_building(
        '1285 Avenue of the Americas', building_type='watchlist',
        name='1285 AoA', deal_type='sublease/direct ext',
        client_name='UBS', submarket='Midtown', db_path=db_path)
    b5 = add_building(
        '520 Madison Avenue', building_type='watchlist',
        name='520 Madison', deal_type='disposal',
        client_name='Carlyle', submarket='Midtown', db_path=db_path)

    print(f"Inserted 5 buildings: IDs {b1}, {b2}, {b3}, {b4}, {b5}")

    # ── AVAILABILITIES ────────────────────────────────────────────────────
    # 1095 AoA
    add_availability(b1, '24', 5374, '2026-07-01', 85.00, db_path=db_path)
    add_availability(b1, '38', 18200, '2026-03-01', 92.00, db_path=db_path)
    add_availability(b1, '42', 12500, None, 95.00, status='coming_available', db_path=db_path)

    # 346 Madison
    add_availability(b2, '12', 8400, '2026-04-01', 78.00, db_path=db_path)
    add_availability(b2, '15', 6200, '2026-06-01', 80.00, db_path=db_path)

    # 30 HY (watchlist — WBD sublease)
    add_availability(b3, '28', 42000, '2026-09-01', 110.00, db_path=db_path)
    add_availability(b3, '29', 42000, '2026-09-01', 110.00, db_path=db_path)
    add_availability(b3, '30', 21000, '2027-01-01', 115.00, status='coming_available', db_path=db_path)

    # 1285 AoA (watchlist — UBS)
    add_availability(b4, '35', 30000, '2027-03-01', 88.00, db_path=db_path)
    add_availability(b4, '36', 30000, '2027-03-01', 88.00, db_path=db_path)

    # 520 Madison (watchlist — Carlyle)
    add_availability(b5, '18', 15000, '2026-12-01', 100.00, db_path=db_path)
    add_availability(b5, '19', 15000, '2026-12-01', 100.00, db_path=db_path)

    print("Inserted 12 availabilities")

    # ── ACTIVITY ──────────────────────────────────────────────────────────
    add_activity(b1, 'proposal', company_name='IMC Trading',
                 floor='38', broker_name='John Smith', broker_firm='Savills',
                 square_feet=18200, activity_date='2026-02-10', db_path=db_path)
    add_activity(b1, 'tour', company_name='Citadel Securities',
                 floor='42', broker_name='Mike Chen', broker_firm='CBRE',
                 activity_date='2026-02-14', db_path=db_path)
    add_activity(b1, 'tour', company_name='Millennium Management',
                 floor='38', broker_name='Sarah Lee', broker_firm='JLL',
                 activity_date='2026-02-05', db_path=db_path)
    add_activity(b2, 'proposal', company_name='Kirkland & Ellis',
                 floor='12', broker_name='Tom Brown', broker_firm='Cushman',
                 square_feet=8400, activity_date='2026-02-12', db_path=db_path)

    print("Inserted 4 activity entries")

    # ── TENANTS ───────────────────────────────────────────────────────────
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    tenants = [
        # 1095 AoA
        (b1, 'PDT Partners', '30', 22000, '2027-08-31', 'active'),
        (b1, 'Bridgewater Associates', '35', 15000, '2026-06-30', 'active'),
        (b1, 'Nomura Securities', '20', 28000, '2028-12-31', 'active'),
        # 346 Madison
        (b2, 'Wachtell Lipton', '8', 18000, '2029-03-31', 'active'),
        (b2, 'Ares Management', '10', 12000, '2026-09-30', 'active'),
        # 30 HY
        (b3, 'Warner Bros. Discovery', '25', 210000, '2030-12-31', 'active'),
        (b3, 'Warner Bros. Discovery', '26', 42000, '2026-08-31', 'active'),
        # 1285 AoA
        (b4, 'UBS', '30', 120000, '2028-06-30', 'active'),
        # 520 Madison
        (b5, 'Carlyle Group', '15', 35000, '2027-12-31', 'active'),
    ]

    for t in tenants:
        cur.execute("""
            INSERT INTO agency_tenants
                (building_id, tenant_name, floor, square_feet, lease_expiry, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, t)

    conn.commit()
    conn.close()
    print(f"Inserted {len(tenants)} tenants")

    # ── TASKS ─────────────────────────────────────────────────────────────
    add_task(building_id=b1, tenant_or_company='IMC Trading',
             task_text='Follow up on proposal — 38th floor',
             priority='high', task_type='follow_up', db_path=db_path)
    add_task(building_id=b1, tenant_or_company='Citadel Securities',
             task_text='Schedule second tour — 42nd floor',
             priority='medium', task_type='tour', db_path=db_path)
    add_task(building_id=b1, tenant_or_company='Bridgewater Associates',
             task_text='Lease expiring June 2026 — retention meeting',
             priority='high', task_type='meeting', db_path=db_path)
    add_task(building_id=b2, tenant_or_company='Kirkland & Ellis',
             task_text='Send updated floor plan for 12th fl',
             priority='medium', task_type='follow_up', db_path=db_path)

    print("Inserted 4 tasks")

    # ── MARKET REQUIREMENTS ───────────────────────────────────────────────
    add_market_requirement(
        company_name='Two Sigma', sf_min=15000, sf_max=25000,
        submarket='Midtown', broker_name='David Park', broker_firm='CBRE',
        source='broker_intel', notes='Exploring relocation from 100 AoA', db_path=db_path)
    add_market_requirement(
        company_name='Elliott Management', sf_min=30000, sf_max=50000,
        submarket='Midtown/HY', broker_name='Lisa Wang', broker_firm='JLL',
        source='market_rumor', notes='Consolidating floors', db_path=db_path)
    add_market_requirement(
        company_name='Coatue Management', sf_min=8000, sf_max=12000,
        submarket='Midtown', broker_name='', broker_firm='',
        source='hiring_signal', notes='Rapid headcount growth', db_path=db_path)

    print("Inserted 3 market requirements")
    print("\nAgency seed data complete.")


if __name__ == '__main__':
    seed()
