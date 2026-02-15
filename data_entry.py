#!/usr/bin/env python3
"""
Relationship Engine — Manual Data Entry CLI
Quick add companies, contacts, relationships, outreach, and more from terminal.
Usage: python3 data_entry.py
"""

import sqlite3
import sys
import os
from datetime import datetime, date

DB_PATH = os.path.expanduser("~/relationship_engine/data/relationship_engine.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn

def print_menu():
    print("\n" + "=" * 50)
    print("  RELATIONSHIP ENGINE — Data Entry")
    print("=" * 50)
    print("  1. Add company")
    print("  2. Add contact")
    print("  3. Add relationship")
    print("  4. Log outreach")
    print("  5. Add funding event")
    print("  6. Add hiring signal")
    print("  7. Add lease")
    print("  8. Add building")
    print("  9. Quick search")
    print(" 10. View stats")
    print(" 11. View intro paths")
    print(" 12. View untouched targets")
    print(" 13. View overdue follow-ups")
    print("  q. Quit")
    print("-" * 50)

def prompt(label, required=True, default=None):
    suffix = f" [{default}]" if default else ""
    suffix += " *" if required and not default else ""
    val = input(f"  {label}{suffix}: ").strip()
    if not val and default:
        return default
    if not val and required:
        print("    ⚠ Required field.")
        return prompt(label, required, default)
    return val if val else None

def pick(label, options, required=True):
    print(f"\n  {label}:")
    for i, opt in enumerate(options, 1):
        print(f"    {i}. {opt}")
    while True:
        val = input("  Choice: ").strip()
        if not val and not required:
            return None
        try:
            idx = int(val)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        except ValueError:
            if val in options:
                return val
        print("    ⚠ Invalid choice.")

def pick_company(conn, label="Company"):
    companies = conn.execute("SELECT id, name, status FROM companies ORDER BY name").fetchall()
    if not companies:
        print("    No companies in database. Add one first.")
        return None
    print(f"\n  {label}:")
    for c in companies:
        print(f"    {c['id']:3d}. {c['name']} ({c['status']})")
    while True:
        val = input("  ID (or 'n' for new): ").strip()
        if val.lower() == 'n':
            return add_company(conn)
        try:
            cid = int(val)
            if any(c['id'] == cid for c in companies):
                return cid
        except ValueError:
            pass
        print("    ⚠ Invalid ID.")

def pick_contact(conn, label="Contact", allow_none=False):
    contacts = conn.execute(
        "SELECT c.id, c.first_name, c.last_name, c.role_level, co.name as company "
        "FROM contacts c LEFT JOIN companies co ON c.company_id = co.id "
        "ORDER BY c.last_name"
    ).fetchall()
    if not contacts:
        print("    No contacts in database. Add one first.")
        return None
    print(f"\n  {label}:")
    for c in contacts:
        company = c['company'] or 'No company'
        print(f"    {c['id']:3d}. {c['first_name']} {c['last_name']} — {c['role_level']} @ {company}")
    while True:
        val = input("  ID (or 'n' for new, blank to skip): ").strip()
        if not val and allow_none:
            return None
        if val.lower() == 'n':
            return add_contact(conn)
        try:
            cid = int(val)
            if any(c['id'] == cid for c in contacts):
                return cid
        except ValueError:
            pass
        print("    ⚠ Invalid ID.")

def pick_building(conn, label="Building", allow_none=False):
    buildings = conn.execute("SELECT id, name, address FROM buildings ORDER BY name").fetchall()
    if not buildings:
        print("    No buildings in database.")
        return None
    print(f"\n  {label}:")
    for b in buildings:
        name = b['name'] or b['address']
        print(f"    {b['id']:3d}. {name} — {b['address']}")
    while True:
        val = input("  ID (or 'n' for new, blank to skip): ").strip()
        if not val and allow_none:
            return None
        if val.lower() == 'n':
            return add_building(conn)
        try:
            bid = int(val)
            if any(b['id'] == bid for b in buildings):
                return bid
        except ValueError:
            pass
        print("    ⚠ Invalid ID.")

# ── ADD FUNCTIONS ──

def add_company(conn):
    print("\n  ── Add Company ──")
    name = prompt("Company name")
    type_ = pick("Type", ['tenant', 'landlord', 'investor', 'lender', 'developer', 'advisory', 'other'])
    status = pick("Status", ['active_client', 'former_client', 'high_growth_target', 'prospect',
                              'network_portfolio', 'team_affiliated', 'watching'])
    sector = prompt("Sector (e.g. tech, financial_services, media, vc_pe)", required=False)
    hq_city = prompt("HQ city", required=False, default="New York")
    hq_state = prompt("HQ state", required=False, default="NY")
    website = prompt("Website", required=False)
    employee_count = prompt("Employee count", required=False)
    notes = prompt("Notes", required=False)

    emp = int(employee_count) if employee_count and employee_count.isdigit() else None

    cur = conn.execute(
        "INSERT INTO companies (name, type, status, sector, hq_city, hq_state, website, employee_count, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (name, type_, status, sector, hq_city, hq_state, website, emp, notes)
    )
    conn.commit()
    print(f"    ✓ Added company: {name} (ID: {cur.lastrowid})")
    return cur.lastrowid

def add_contact(conn):
    print("\n  ── Add Contact ──")
    first = prompt("First name")
    last = prompt("Last name")
    company_id = pick_company(conn, "Their company")
    title = prompt("Title (e.g. CEO, VP Real Estate)", required=False)
    role_level = pick("Role level", ['c_suite', 'decision_maker', 'influencer', 'team', 'external_partner'])
    email = prompt("Email", required=False)
    phone = prompt("Phone", required=False)
    linkedin = prompt("LinkedIn URL", required=False)
    alma_mater = prompt("Alma mater", required=False)
    prev_companies = prompt("Previous companies (comma-separated)", required=False)
    notes = prompt("Notes", required=False)

    cur = conn.execute(
        "INSERT INTO contacts (first_name, last_name, company_id, title, role_level, "
        "email, phone, linkedin_url, alma_mater, previous_companies, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (first, last, company_id, title, role_level, email, phone, linkedin, alma_mater, prev_companies, notes)
    )
    conn.commit()
    print(f"    ✓ Added contact: {first} {last} (ID: {cur.lastrowid})")
    return cur.lastrowid

def add_relationship(conn):
    print("\n  ── Add Relationship ──")
    print("  Person A:")
    a_id = pick_contact(conn, "Person A")
    print("  Person B:")
    b_id = pick_contact(conn, "Person B")
    if a_id == b_id:
        print("    ⚠ Can't create relationship with self.")
        return
    rel_type = pick("Relationship type", [
        'colleague', 'former_colleague', 'alumni', 'investor', 'client',
        'friend', 'board', 'deal_counterpart', 'introduced_by', 'other'
    ])
    strength = pick("Strength", ['5 (weekly contact)', '4 (quarterly)', '3 (annual)',
                                  '2 (met once)', '1 (aware of, no contact)'])
    strength_val = int(strength[0])
    context = prompt("Context (e.g. 'Harvard 2018', 'BofA deal')", required=False)
    notes = prompt("Notes", required=False)

    conn.execute(
        "INSERT INTO relationships (contact_id_a, contact_id_b, relationship_type, strength, context, notes) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (a_id, b_id, rel_type, strength_val, context, notes)
    )
    conn.commit()
    print("    ✓ Relationship added.")

def log_outreach(conn):
    print("\n  ── Log Outreach ──")
    company_id = pick_company(conn, "Target company")
    contact_id = pick_contact(conn, "Target contact (blank if unknown)", allow_none=True)
    outreach_date = prompt("Date (YYYY-MM-DD)", default=date.today().isoformat())
    outreach_type = pick("Type", ['email', 'call', 'linkedin', 'text', 'in_person', 'event', 'intro_request', 'other'])
    intro_path = prompt("Intro path used (e.g. 'Graham → Ryan → target')", required=False)
    angle = prompt("Angle/hook used", required=False)
    outcome = pick("Outcome", [
        'pending', 'no_response', 'responded_positive', 'responded_negative',
        'meeting_booked', 'meeting_held', 'deal_started', 'referred', 'declined'
    ])
    follow_up = prompt("Follow-up date (YYYY-MM-DD, blank if none)", required=False)
    notes = prompt("Notes", required=False)

    conn.execute(
        "INSERT INTO outreach_log (target_company_id, target_contact_id, outreach_date, outreach_type, "
        "intro_path_used, angle, outcome, follow_up_date, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (company_id, contact_id, outreach_date, outreach_type, intro_path, angle, outcome, follow_up, notes)
    )
    conn.commit()
    print("    ✓ Outreach logged.")

def add_funding(conn):
    print("\n  ── Add Funding Event ──")
    company_id = pick_company(conn, "Company")
    event_date = prompt("Date (YYYY-MM-DD)", default=date.today().isoformat())
    round_type = prompt("Round type (e.g. Series B, Growth Equity, IPO)")
    amount = prompt("Amount in USD (e.g. 50000000)", required=False)
    lead_investor = prompt("Lead investor", required=False)
    all_investors = prompt("All investors (comma-separated)", required=False)
    source_url = prompt("Source URL", required=False)
    notes = prompt("Notes", required=False)

    amt = float(amount) if amount else None

    conn.execute(
        "INSERT INTO funding_events (company_id, event_date, round_type, amount, lead_investor, "
        "all_investors, source_url, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (company_id, event_date, round_type, amt, lead_investor, all_investors, source_url, notes)
    )
    conn.commit()
    print("    ✓ Funding event added.")

def add_hiring(conn):
    print("\n  ── Add Hiring Signal ──")
    company_id = pick_company(conn, "Company")
    signal_date = prompt("Date (YYYY-MM-DD)", default=date.today().isoformat())
    signal_type = pick("Signal type", ['job_posting', 'headcount_growth', 'new_office', 'leadership_hire', 'press_announcement'])
    role_title = prompt("Role title (e.g. VP Real Estate)", required=False)
    location = prompt("Location", required=False, default="New York, NY")
    details = prompt("Details", required=False)
    source_url = prompt("Source URL", required=False)
    relevance = pick("Relevance", ['high', 'medium', 'low'])
    notes = prompt("Notes", required=False)

    conn.execute(
        "INSERT INTO hiring_signals (company_id, signal_date, signal_type, role_title, location, "
        "details, source_url, relevance, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (company_id, signal_date, signal_type, role_title, location, details, source_url, relevance, notes)
    )
    conn.commit()
    print("    ✓ Hiring signal added.")

def add_lease(conn):
    print("\n  ── Add Lease ──")
    company_id = pick_company(conn, "Tenant company")
    building_id = pick_building(conn, "Building")
    floor = prompt("Floor(s)", required=False)
    sf = prompt("Square feet", required=False)
    lease_start = prompt("Lease start (YYYY-MM-DD)", required=False)
    lease_expiry = prompt("Lease expiry (YYYY-MM-DD)", required=False)
    rent_psf = prompt("Rent per SF", required=False)
    lease_type = pick("Lease type", ['direct', 'sublease', 'renewal', 'expansion'])
    source = prompt("Source (e.g. CoStar, press, broker intel)", required=False)
    confidence = pick("Confidence", ['confirmed', 'estimated', 'rumored'])
    notes = prompt("Notes", required=False)

    sq = int(sf) if sf and sf.isdigit() else None
    rent = float(rent_psf) if rent_psf else None

    conn.execute(
        "INSERT INTO leases (company_id, building_id, floor, square_feet, lease_start, lease_expiry, "
        "rent_psf, lease_type, source, confidence, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (company_id, building_id, floor, sq, lease_start, lease_expiry, rent, lease_type, source, confidence, notes)
    )
    conn.commit()
    print("    ✓ Lease added.")

def add_building(conn):
    print("\n  ── Add Building ──")
    name = prompt("Building name (e.g. One Vanderbilt)", required=False)
    address = prompt("Address")
    city = prompt("City", default="New York")
    state = prompt("State", default="NY")
    submarket = prompt("Submarket (e.g. Midtown, Hudson Yards, FiDi)", required=False)
    building_class = pick("Class", ['Trophy', 'A', 'B', 'C'])
    total_sf = prompt("Total SF", required=False)
    owner_id = pick_company(conn, "Owner company")
    managing_agent = prompt("Managing agent", required=False)
    we_rep = input("  Do we rep this building? (y/n): ").strip().lower() == 'y'
    notes = prompt("Notes", required=False)

    tsf = int(total_sf) if total_sf and total_sf.isdigit() else None

    cur = conn.execute(
        "INSERT INTO buildings (name, address, city, state, submarket, building_class, total_sf, "
        "owner_company_id, managing_agent, we_rep, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (name, address, city, state, submarket, building_class, tsf, owner_id, managing_agent, we_rep, notes)
    )
    conn.commit()
    print(f"    ✓ Added building: {name or address} (ID: {cur.lastrowid})")
    return cur.lastrowid

# ── QUERY FUNCTIONS ──

def quick_search(conn):
    print("\n  ── Quick Search ──")
    term = prompt("Search term")
    term_like = f"%{term}%"

    print("\n  Companies:")
    rows = conn.execute("SELECT id, name, status, sector FROM companies WHERE name LIKE ?", (term_like,)).fetchall()
    if rows:
        for r in rows:
            print(f"    {r['id']:3d}. {r['name']} — {r['status']} ({r['sector'] or 'no sector'})")
    else:
        print("    (none)")

    print("\n  Contacts:")
    rows = conn.execute(
        "SELECT c.id, c.first_name, c.last_name, c.title, co.name as company "
        "FROM contacts c LEFT JOIN companies co ON c.company_id = co.id "
        "WHERE c.first_name LIKE ? OR c.last_name LIKE ? OR c.title LIKE ?",
        (term_like, term_like, term_like)
    ).fetchall()
    if rows:
        for r in rows:
            print(f"    {r['id']:3d}. {r['first_name']} {r['last_name']} — {r['title'] or 'no title'} @ {r['company'] or 'no company'}")
    else:
        print("    (none)")

    print("\n  Buildings:")
    rows = conn.execute("SELECT id, name, address FROM buildings WHERE name LIKE ? OR address LIKE ?",
                        (term_like, term_like)).fetchall()
    if rows:
        for r in rows:
            print(f"    {r['id']:3d}. {r['name'] or r['address']} — {r['address']}")
    else:
        print("    (none)")

def view_stats(conn):
    print("\n  ── Database Stats ──")
    tables = ['companies', 'contacts', 'buildings', 'leases', 'deals',
              'relationships', 'outreach_log', 'funding_events', 'hiring_signals']
    for t in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"    {t:20s}: {count}")

def view_intro_paths(conn):
    print("\n  ── Intro Paths to Decision Makers ──")
    rows = conn.execute("SELECT * FROM v_intro_paths LIMIT 30").fetchall()
    if not rows:
        print("    No intro paths found. Add more relationships.")
        return
    for r in rows:
        print(f"    {r['team_member']} → {r['connector']} ({r['team_to_connector']}, str:{r['connection_strength']}) "
              f"→ {r['target_person']} ({r['target_title']}) @ {r['target_company']}")

def view_untouched(conn):
    print("\n  ── Untouched Targets ──")
    rows = conn.execute("SELECT * FROM v_untouched_targets").fetchall()
    if not rows:
        print("    All targets have been contacted. Nice work.")
        return
    for r in rows:
        print(f"    {r['name']} — {r['status']} ({r['sector'] or 'no sector'})")

def view_overdue(conn):
    print("\n  ── Overdue Follow-ups ──")
    rows = conn.execute("SELECT * FROM v_overdue_followups").fetchall()
    if not rows:
        print("    No overdue follow-ups. You're on top of it.")
        return
    for r in rows:
        print(f"    {r['follow_up_date']} — {r['company_name']} / {r['contact_name'] or 'unknown'} "
              f"({r['outreach_type']}) — {r['days_overdue']} days overdue")

# ── MAIN LOOP ──

def main():
    if not os.path.exists(DB_PATH):
        print(f"  ⚠ Database not found at {DB_PATH}")
        print("  Run the schema SQL first.")
        sys.exit(1)

    conn = get_db()
    print("\n  Connected to:", DB_PATH)

    while True:
        print_menu()
        choice = input("  → ").strip()

        try:
            if choice == '1':
                add_company(conn)
            elif choice == '2':
                add_contact(conn)
            elif choice == '3':
                add_relationship(conn)
            elif choice == '4':
                log_outreach(conn)
            elif choice == '5':
                add_funding(conn)
            elif choice == '6':
                add_hiring(conn)
            elif choice == '7':
                add_lease(conn)
            elif choice == '8':
                add_building(conn)
            elif choice == '9':
                quick_search(conn)
            elif choice == '10':
                view_stats(conn)
            elif choice == '11':
                view_intro_paths(conn)
            elif choice == '12':
                view_untouched(conn)
            elif choice == '13':
                view_overdue(conn)
            elif choice.lower() == 'q':
                print("\n  Done.\n")
                break
            else:
                print("  ⚠ Invalid choice.")
        except KeyboardInterrupt:
            print("\n\n  Interrupted. Back to menu.\n")
        except Exception as e:
            print(f"\n  ⚠ Error: {e}\n")

    conn.close()

if __name__ == "__main__":
    main()
