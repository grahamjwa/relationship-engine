"""
Data Ingestion — CSV loader for simulated and real signals.

Reads a CSV with mixed entity rows (companies, contacts, funding, hiring,
leases, relationships) and upserts into the SQLite database.

No dedup complexity yet — exact name match for entity resolution.
Designed to be extended with fuzzy matching (rapidfuzz) later.
"""

import csv
import sqlite3
import os
import sys
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from graph_engine import get_db_path, _resolve_db_path


def _get_conn(db_path: str = None) -> sqlite3.Connection:
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _resolve_company_id(conn: sqlite3.Connection, name: str) -> Optional[int]:
    """Exact name match for company resolution."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM companies WHERE LOWER(name) = LOWER(?)", (name.strip(),))
    row = cur.fetchone()
    return row["id"] if row else None


def _resolve_contact_id(conn: sqlite3.Connection, name: str) -> Optional[int]:
    """Exact name match for contact resolution. Name = 'First Last'."""
    parts = name.strip().split(None, 1)
    if len(parts) < 2:
        return None
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM contacts WHERE LOWER(first_name) = LOWER(?) AND LOWER(last_name) = LOWER(?)",
        (parts[0], parts[1])
    )
    row = cur.fetchone()
    return row["id"] if row else None


def _ensure_company(conn: sqlite3.Connection, name: str, industry: str = None,
                    status: str = None) -> int:
    """Get or create a company by name. Returns company_id."""
    cid = _resolve_company_id(conn, name)
    if cid:
        return cid
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO companies (name, industry, status) VALUES (?, ?, ?)",
        (name.strip(), industry, status)
    )
    conn.commit()
    return cur.lastrowid


def _ensure_contact(conn: sqlite3.Connection, name: str,
                    company_id: int = None) -> int:
    """Get or create a contact by name. Returns contact_id."""
    cid = _resolve_contact_id(conn, name)
    if cid:
        return cid
    parts = name.strip().split(None, 1)
    first = parts[0] if parts else name
    last = parts[1] if len(parts) > 1 else ""
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO contacts (first_name, last_name, company_id) VALUES (?, ?, ?)",
        (first, last, company_id)
    )
    conn.commit()
    return cur.lastrowid


def _add_funding_event(conn: sqlite3.Connection, company_id: int,
                       event_date: str, amount: float,
                       round_type: str = None):
    """Insert a funding event (skip if exact duplicate)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id FROM funding_events
        WHERE company_id = ? AND event_date = ? AND amount = ?
    """, (company_id, event_date, amount))
    if cur.fetchone():
        return  # Duplicate
    cur.execute("""
        INSERT INTO funding_events (company_id, event_date, amount, round_type, source)
        VALUES (?, ?, ?, ?, 'csv_import')
    """, (company_id, event_date, amount, round_type))


def _add_hiring_signal(conn: sqlite3.Connection, company_id: int,
                       delta_pct: float, signal_date: str = None):
    """Insert a hiring signal."""
    if not signal_date:
        signal_date = datetime.now().strftime("%Y-%m-%d")
    cur = conn.cursor()

    relevance = "high" if delta_pct >= 25 else ("medium" if delta_pct >= 10 else "low")
    signal_type = "headcount_growth"

    cur.execute("""
        SELECT id FROM hiring_signals
        WHERE company_id = ? AND signal_date = ? AND signal_type = ?
    """, (company_id, signal_date, signal_type))
    if cur.fetchone():
        return
    cur.execute("""
        INSERT INTO hiring_signals
            (company_id, signal_date, signal_type, relevance, details, source)
        VALUES (?, ?, ?, ?, ?, 'csv_import')
    """, (company_id, signal_date, signal_type, relevance,
          f"Headcount growth {delta_pct}%"))


def _add_lease(conn: sqlite3.Connection, company_id: int,
               expiry: str, sf: int, building_name: str = None):
    """Insert a lease record (needs building_id)."""
    cur = conn.cursor()

    # Resolve or create building
    building_id = None
    if building_name:
        cur.execute("SELECT id FROM buildings WHERE LOWER(address) = LOWER(?)",
                    (building_name,))
        row = cur.fetchone()
        if row:
            building_id = row["id"]
        else:
            cur.execute("INSERT INTO buildings (address) VALUES (?)", (building_name,))
            building_id = cur.lastrowid

    # Check for existing lease
    cur.execute("""
        SELECT id FROM leases
        WHERE company_id = ? AND lease_expiry = ?
    """, (company_id, expiry))
    if cur.fetchone():
        return

    cur.execute("""
        INSERT INTO leases (company_id, building_id, lease_expiry, square_feet)
        VALUES (?, ?, ?, ?)
    """, (company_id, building_id, expiry, sf))


def _add_relationship(conn: sqlite3.Connection,
                      source_type: str, source_id: int,
                      target_type: str, target_id: int,
                      rel_type: str, strength: int, confidence: float):
    """Insert a relationship (skip if exact duplicate)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id FROM relationships
        WHERE source_type = ? AND source_id = ?
        AND target_type = ? AND target_id = ?
        AND relationship_type = ?
    """, (source_type, source_id, target_type, target_id, rel_type))
    if cur.fetchone():
        return
    cur.execute("""
        INSERT INTO relationships
            (source_type, source_id, target_type, target_id,
             relationship_type, strength, confidence, base_weight,
             last_interaction)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1.0, date('now'))
    """, (source_type, source_id, target_type, target_id,
          rel_type, strength, confidence))


def _to_float(val: str) -> Optional[float]:
    if not val or not val.strip():
        return None
    try:
        return float(val.strip())
    except ValueError:
        return None


def _to_int(val: str) -> Optional[int]:
    if not val or not val.strip():
        return None
    try:
        return int(float(val.strip()))
    except ValueError:
        return None


# =============================================================================
# MAIN LOADER
# =============================================================================

def load_csv(csv_path: str, db_path: str = None, verbose: bool = True) -> dict:
    """
    Load a CSV of mixed entity/signal rows into the database.

    Expected columns (all optional except entity_type + name):
        entity_type, name, industry, status, revenue_est, office_sf,
        cash_reserves, cash_updated_at, funding_date, funding_amount,
        funding_round, hiring_delta_pct, lease_expiry, lease_sf,
        lease_building, relationship_to, relationship_type, strength, confidence

    Returns summary dict.
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    stats = {
        "companies_created": 0, "companies_updated": 0,
        "contacts_created": 0,
        "funding_events": 0, "hiring_signals": 0,
        "leases": 0, "relationships": 0,
        "rows_processed": 0, "errors": [],
    }

    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            stats["rows_processed"] += 1
            try:
                etype = (row.get("entity_type") or "").strip().lower()
                name = (row.get("name") or "").strip()
                if not name:
                    continue

                if etype == "company":
                    # Upsert company
                    company_id = _resolve_company_id(conn, name)
                    if company_id is None:
                        company_id = _ensure_company(
                            conn, name,
                            row.get("industry"), row.get("status")
                        )
                        stats["companies_created"] += 1
                        if verbose:
                            print(f"  + Company: {name} (id={company_id})")

                    # Update attributes
                    updates = {}
                    rev = _to_float(row.get("revenue_est"))
                    if rev is not None:
                        updates["revenue_est"] = rev
                    sf = _to_int(row.get("office_sf"))
                    if sf is not None:
                        updates["office_sf"] = sf
                    cash = _to_float(row.get("cash_reserves"))
                    if cash is not None:
                        updates["cash_reserves"] = cash
                        updates["cash_updated_at"] = row.get("cash_updated_at") or datetime.now().strftime("%Y-%m-%d")
                    ind = row.get("industry")
                    if ind:
                        updates["industry"] = ind
                    st = row.get("status")
                    if st:
                        updates["status"] = st

                    if updates:
                        set_clause = ", ".join(f"{k} = ?" for k in updates)
                        vals = list(updates.values()) + [company_id]
                        cur.execute(f"UPDATE companies SET {set_clause} WHERE id = ?", vals)
                        stats["companies_updated"] += 1

                    # Funding event
                    fdate = row.get("funding_date", "").strip()
                    famount = _to_float(row.get("funding_amount"))
                    if fdate and famount:
                        _add_funding_event(conn, company_id, fdate, famount,
                                           row.get("funding_round"))
                        stats["funding_events"] += 1

                    # Hiring signal
                    hdelta = _to_float(row.get("hiring_delta_pct"))
                    if hdelta:
                        sig_date = fdate or datetime.now().strftime("%Y-%m-%d")
                        _add_hiring_signal(conn, company_id, hdelta, sig_date)
                        stats["hiring_signals"] += 1

                    # Lease
                    lexpiry = row.get("lease_expiry", "").strip()
                    lsf = _to_int(row.get("lease_sf"))
                    if lexpiry:
                        _add_lease(conn, company_id, lexpiry, lsf,
                                   row.get("lease_building"))
                        stats["leases"] += 1

                elif etype == "contact":
                    # Resolve or create contact
                    contact_id = _resolve_contact_id(conn, name)
                    if contact_id is None:
                        contact_id = _ensure_contact(conn, name)
                        stats["contacts_created"] += 1
                        if verbose:
                            print(f"  + Contact: {name} (id={contact_id})")

                    # Relationship
                    rel_to = (row.get("relationship_to") or "").strip()
                    rel_type = (row.get("relationship_type") or "").strip()
                    if rel_to and rel_type:
                        # Resolve target (company or contact)
                        target_company = _resolve_company_id(conn, rel_to)
                        if target_company:
                            _add_relationship(
                                conn, "contact", contact_id,
                                "company", target_company,
                                rel_type,
                                _to_int(row.get("strength")) or 5,
                                _to_float(row.get("confidence")) or 0.7
                            )
                            stats["relationships"] += 1
                        else:
                            target_contact = _resolve_contact_id(conn, rel_to)
                            if target_contact:
                                _add_relationship(
                                    conn, "contact", contact_id,
                                    "contact", target_contact,
                                    rel_type,
                                    _to_int(row.get("strength")) or 5,
                                    _to_float(row.get("confidence")) or 0.7
                                )
                                stats["relationships"] += 1

            except Exception as e:
                stats["errors"].append(f"Row {i}: {e}")
                if verbose:
                    print(f"  ! Row {i} error: {e}")

    conn.commit()
    conn.close()

    if verbose:
        print(f"\n--- Ingestion Summary ---")
        print(f"  Rows processed: {stats['rows_processed']}")
        print(f"  Companies created: {stats['companies_created']}")
        print(f"  Companies updated: {stats['companies_updated']}")
        print(f"  Contacts created: {stats['contacts_created']}")
        print(f"  Funding events: {stats['funding_events']}")
        print(f"  Hiring signals: {stats['hiring_signals']}")
        print(f"  Leases: {stats['leases']}")
        print(f"  Relationships: {stats['relationships']}")
        if stats["errors"]:
            print(f"  Errors: {len(stats['errors'])}")
            for e in stats["errors"][:5]:
                print(f"    {e}")

    return stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load CSV signals into relationship engine DB")
    parser.add_argument("csv_path", help="Path to CSV file")
    parser.add_argument("--db", default=None, help="Database path (optional)")
    args = parser.parse_args()

    load_csv(args.csv_path, args.db)
