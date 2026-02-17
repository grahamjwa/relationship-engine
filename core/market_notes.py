"""
Market Notes — Freeform Intel Log
Store market rumors, broker intel, and observations. Query by company, building,
contact, tag, or date range.
"""

import os
import sys
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: F401

from graph_engine import get_db_path


# =============================================================================
# ENTITY EXTRACTION
# =============================================================================

def _extract_entities(note_text: str, db_path: str = None) -> Dict:
    """
    Extract company names, building addresses, and contact names from note text
    by matching against the database. Returns dict of matched entity lists.
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    note_lower = note_text.lower()

    # Match companies
    cur.execute("SELECT id, name FROM companies")
    matched_companies = []
    for cid, name in cur.fetchall():
        if name.lower() in note_lower:
            matched_companies.append(name)

    # Match buildings by address
    cur.execute("SELECT id, address, name FROM buildings")
    matched_buildings = []
    for bid, addr, bname in cur.fetchall():
        if addr and addr.lower() in note_lower:
            matched_buildings.append(addr)
        elif bname and bname.lower() in note_lower:
            matched_buildings.append(bname)

    # Match contacts (first last)
    cur.execute("SELECT id, first_name, last_name FROM contacts")
    matched_contacts = []
    for cid, first, last in cur.fetchall():
        full_name = f"{first} {last}".lower()
        if full_name in note_lower:
            matched_contacts.append(f"{first} {last}")
        elif last and last.lower() in note_lower and len(last) > 3:
            # Partial match on distinctive last names only
            matched_contacts.append(f"{first} {last}")

    conn.close()

    return {
        "companies": matched_companies,
        "buildings": matched_buildings,
        "contacts": matched_contacts,
    }


def _extract_tags(note_text: str) -> List[str]:
    """
    Auto-tag notes based on keywords.
    """
    tags = []
    note_lower = note_text.lower()

    tag_keywords = {
        "expansion": ["expand", "expansion", "growing", "new space", "additional space", "more space"],
        "relocation": ["relocat", "moving to", "move to", "new hq"],
        "lease": ["lease", "renew", "expir", "sublease", "sublet"],
        "deal": ["deal", "signed", "transaction", "closed", "awarded"],
        "hiring": ["hiring", "recruit", "headcount", "new hire", "leadership"],
        "funding": ["fund", "raise", "series", "ipo", "valuation", "capital"],
        "rumor": ["rumor", "heard", "apparently", "supposedly", "might be", "could be"],
        "broker_intel": ["broker", "cw", "jll", "cbre", "colliers", "newmark", "savills"],
        "tenant_rep": ["tenant rep", "t-rep", "representing"],
        "landlord": ["landlord", "owner", "building owner", "management"],
    }

    for tag, keywords in tag_keywords.items():
        if any(kw in note_lower for kw in keywords):
            tags.append(tag)

    return tags


# =============================================================================
# CORE FUNCTIONS
# =============================================================================

def add_note(
    note_text: str,
    source: str = None,
    note_date: str = None,
    db_path: str = None
) -> Dict:
    """
    Add a market note. Auto-extracts entities and tags from the text.

    Args:
        note_text: The raw note (e.g. "Blue Owl expanding into 625 Madison with Mark Weiss (CW)")
        source: Optional source label (e.g. "broker call", "market tour", "email")
        note_date: Optional date string YYYY-MM-DD (defaults to today)
        db_path: Optional database path

    Returns:
        Dict with note_id and extracted entities/tags
    """
    if db_path is None:
        db_path = get_db_path()

    entities = _extract_entities(note_text, db_path)
    tags = _extract_tags(note_text)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO market_notes
            (note_text, source, companies_mentioned, buildings_mentioned,
             contacts_mentioned, tags, note_date)
        VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, date('now')))
    """, (
        note_text,
        source,
        "|".join(entities["companies"]) if entities["companies"] else None,
        "|".join(entities["buildings"]) if entities["buildings"] else None,
        "|".join(entities["contacts"]) if entities["contacts"] else None,
        "|".join(tags) if tags else None,
        note_date,
    ))

    note_id = cur.lastrowid
    conn.commit()
    conn.close()

    return {
        "note_id": note_id,
        "entities": entities,
        "tags": tags,
        "note_text": note_text,
    }


def search_notes(
    query: str = None,
    company: str = None,
    building: str = None,
    contact: str = None,
    tag: str = None,
    days_back: int = None,
    limit: int = 50,
    db_path: str = None
) -> List[Dict]:
    """
    Search market notes by keyword, entity, tag, or date range.
    All filters are AND-combined.
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    where_clauses = []
    params = []

    if query:
        where_clauses.append("note_text LIKE ?")
        params.append(f"%{query}%")

    if company:
        where_clauses.append("(companies_mentioned LIKE ? OR note_text LIKE ?)")
        params.extend([f"%{company}%", f"%{company}%"])

    if building:
        where_clauses.append("(buildings_mentioned LIKE ? OR note_text LIKE ?)")
        params.extend([f"%{building}%", f"%{building}%"])

    if contact:
        where_clauses.append("(contacts_mentioned LIKE ? OR note_text LIKE ?)")
        params.extend([f"%{contact}%", f"%{contact}%"])

    if tag:
        where_clauses.append("tags LIKE ?")
        params.append(f"%{tag}%")

    if days_back:
        where_clauses.append("note_date >= date('now', ? || ' days')")
        params.append(f"-{days_back}")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    cur.execute(f"""
        SELECT id, note_text, source, companies_mentioned, buildings_mentioned,
               contacts_mentioned, tags, note_date, created_at
        FROM market_notes
        WHERE {where_sql}
        ORDER BY note_date DESC, created_at DESC
        LIMIT ?
    """, params + [limit])

    results = [dict(r) for r in cur.fetchall()]
    conn.close()

    return results


def get_latest_on(entity_name: str, db_path: str = None) -> List[Dict]:
    """
    Get the latest notes mentioning a specific entity (company, building, or person).
    Searches across all entity fields and note text.
    """
    return search_notes(query=entity_name, limit=20, db_path=db_path)


def get_recent_intel(days_back: int = 14, db_path: str = None) -> List[Dict]:
    """Get all notes from the last N days."""
    return search_notes(days_back=days_back, db_path=db_path)


def format_notes(notes: List[Dict]) -> str:
    """Format notes list into readable output."""
    if not notes:
        return "No notes found."

    lines = []
    for n in notes:
        date = n.get("note_date", "unknown")
        source = f" [{n['source']}]" if n.get("source") else ""
        tags = ""
        if n.get("tags"):
            tags = f" #{' #'.join(n['tags'].split('|'))}"

        lines.append(f"**{date}**{source}{tags}")
        lines.append(f"  {n['note_text']}")

        entities = []
        if n.get("companies_mentioned"):
            entities.append(f"Companies: {n['companies_mentioned'].replace('|', ', ')}")
        if n.get("buildings_mentioned"):
            entities.append(f"Buildings: {n['buildings_mentioned'].replace('|', ', ')}")
        if n.get("contacts_mentioned"):
            entities.append(f"Contacts: {n['contacts_mentioned'].replace('|', ', ')}")
        if entities:
            lines.append(f"  *{' · '.join(entities)}*")

        lines.append("")

    return "\n".join(lines)


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Market Notes CLI")
    sub = parser.add_subparsers(dest="command")

    # Add note
    add_parser = sub.add_parser("add", help="Add a market note")
    add_parser.add_argument("note", help="The note text")
    add_parser.add_argument("--source", "-s", help="Source label")
    add_parser.add_argument("--date", "-d", help="Note date (YYYY-MM-DD)")

    # Search
    search_parser = sub.add_parser("search", help="Search notes")
    search_parser.add_argument("query", nargs="?", help="Search text")
    search_parser.add_argument("--company", "-c", help="Filter by company")
    search_parser.add_argument("--building", "-b", help="Filter by building")
    search_parser.add_argument("--contact", help="Filter by contact")
    search_parser.add_argument("--tag", "-t", help="Filter by tag")
    search_parser.add_argument("--days", "-d", type=int, help="Lookback days")

    # Latest on
    latest_parser = sub.add_parser("latest", help="Latest intel on an entity")
    latest_parser.add_argument("entity", help="Company, building, or person name")

    # Recent
    recent_parser = sub.add_parser("recent", help="Recent intel")
    recent_parser.add_argument("--days", "-d", type=int, default=14, help="Days back")

    args = parser.parse_args()

    if args.command == "add":
        result = add_note(args.note, source=args.source, note_date=args.date)
        print(f"Note #{result['note_id']} saved.")
        if result["entities"]["companies"]:
            print(f"  Companies matched: {', '.join(result['entities']['companies'])}")
        if result["entities"]["buildings"]:
            print(f"  Buildings matched: {', '.join(result['entities']['buildings'])}")
        if result["entities"]["contacts"]:
            print(f"  Contacts matched: {', '.join(result['entities']['contacts'])}")
        if result["tags"]:
            print(f"  Tags: {', '.join(result['tags'])}")

    elif args.command == "search":
        results = search_notes(
            query=args.query, company=args.company, building=args.building,
            contact=args.contact, tag=args.tag, days_back=args.days
        )
        print(format_notes(results))

    elif args.command == "latest":
        results = get_latest_on(args.entity)
        print(format_notes(results))

    elif args.command == "recent":
        results = get_recent_intel(args.days)
        print(format_notes(results))

    else:
        parser.print_help()
