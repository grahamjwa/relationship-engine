#!/usr/bin/env python3
"""
Generate comprehensive company profiles from the relationship engine database.

Usage:
    python reports/company_profile.py --company "Citadel LLC"
    python reports/company_profile.py --id 42
"""

import sys
import os
import sqlite3
import argparse
from typing import Optional, Dict, List, Any
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.graph_engine import get_db_path


def _get_conn(db_path: str) -> sqlite3.Connection:
    """Get database connection with row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _find_company_id(db_path: str, company_id: Optional[int] = None,
                     company_name: Optional[str] = None) -> Optional[int]:
    """Find company ID by name or ID. Returns None if not found."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    if company_id:
        cur.execute("SELECT id FROM companies WHERE id = ?", (company_id,))
        row = cur.fetchone()
        conn.close()
        return row["id"] if row else None

    if company_name:
        cur.execute("SELECT id FROM companies WHERE name = ?", (company_name,))
        row = cur.fetchone()
        conn.close()
        return row["id"] if row else None

    conn.close()
    return None


def _fetch_company_info(db_path: str, company_id: int) -> Dict[str, Any]:
    """Fetch core company information."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id, name, industry, sector, hq_city, hq_state, website,
            employee_count, revenue_est, status, type, founded_year,
            notes, opportunity_score, centrality_score, leverage_score,
            influence_score, adjacency_index, cluster_id, category, mature,
            office_sf, cash_reserves, cash_updated_at, chain_lease_prob,
            chain_score
        FROM companies WHERE id = ?
    """, (company_id,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return {}

    return {
        "id": row["id"],
        "name": row["name"],
        "industry": row["industry"],
        "sector": row["sector"],
        "hq_city": row["hq_city"],
        "hq_state": row["hq_state"],
        "website": row["website"],
        "employee_count": row["employee_count"],
        "revenue_est": row["revenue_est"],
        "status": row["status"],
        "type": row["type"],
        "founded_year": row["founded_year"],
        "notes": row["notes"],
        "opportunity_score": row["opportunity_score"],
        "centrality_score": row["centrality_score"],
        "leverage_score": row["leverage_score"],
        "influence_score": row["influence_score"],
        "adjacency_index": row["adjacency_index"],
        "cluster_id": row["cluster_id"],
        "category": row["category"],
        "mature": row["mature"],
        "office_sf": row["office_sf"],
        "cash_reserves": row["cash_reserves"],
        "cash_updated_at": row["cash_updated_at"],
        "chain_lease_prob": row["chain_lease_prob"],
        "chain_score": row["chain_score"],
    }


def _fetch_funding_history(db_path: str, company_id: int) -> List[Dict[str, Any]]:
    """Fetch all funding events for a company."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id, company_id, round_type, amount, event_date,
            details, source, lead_investor, post_valuation
        FROM funding_events
        WHERE company_id = ?
        ORDER BY event_date DESC
    """, (company_id,))

    events = []
    for row in cur.fetchall():
        events.append({
            "id": row["id"],
            "round_type": row["round_type"],
            "amount": row["amount"],
            "event_date": row["event_date"],
            "details": row["details"],
            "source": row["source"],
            "lead_investor": row["lead_investor"],
            "post_valuation": row["post_valuation"],
        })

    conn.close()
    return events


def _fetch_hiring_signals(db_path: str, company_id: int) -> List[Dict[str, Any]]:
    """Fetch all hiring signals for a company."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id, company_id, signal_type, title, role_title,
            signal_date, location, source_url, description, details, relevance
        FROM hiring_signals
        WHERE company_id = ?
        ORDER BY signal_date DESC
    """, (company_id,))

    signals = []
    for row in cur.fetchall():
        signals.append({
            "id": row["id"],
            "title": row["title"],
            "role_title": row["role_title"],
            "signal_type": row["signal_type"],
            "signal_date": row["signal_date"],
            "location": row["location"],
            "source_url": row["source_url"],
            "description": row["description"],
            "details": row["details"],
            "relevance": row["relevance"],
        })

    conn.close()
    return signals


def _fetch_contacts(db_path: str, company_id: int) -> List[Dict[str, Any]]:
    """Fetch all contacts at a company."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id, first_name, last_name, title, email, phone, linkedin_url,
            status, role_level, priority_score, centrality_score,
            leverage_score, influence_score, adjacency_index
        FROM contacts
        WHERE company_id = ?
        ORDER BY priority_score DESC, last_name ASC
    """, (company_id,))

    contacts = []
    for row in cur.fetchall():
        contacts.append({
            "id": row["id"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "title": row["title"],
            "email": row["email"],
            "phone": row["phone"],
            "linkedin_url": row["linkedin_url"],
            "status": row["status"],
            "role_level": row["role_level"],
            "priority_score": row["priority_score"],
            "centrality_score": row["centrality_score"],
            "leverage_score": row["leverage_score"],
            "influence_score": row["influence_score"],
            "adjacency_index": row["adjacency_index"],
        })

    conn.close()
    return contacts


def _fetch_relationships(db_path: str, company_id: int) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all relationships for a company (as source or target)."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    # Outgoing relationships
    cur.execute("""
        SELECT
            r.id, r.source_type, r.source_id, r.target_type, r.target_id,
            r.relationship_type, r.strength, r.confidence, r.last_interaction
        FROM relationships r
        WHERE r.source_type = 'company' AND r.source_id = ?
        ORDER BY r.strength DESC, r.confidence DESC
    """, (company_id,))

    outgoing = []
    for row in cur.fetchall():
        target_name = _get_entity_name(db_path, row["target_type"], row["target_id"])
        outgoing.append({
            "relationship_type": row["relationship_type"],
            "target": target_name,
            "target_type": row["target_type"],
            "strength": row["strength"],
            "confidence": row["confidence"],
            "last_interaction": row["last_interaction"],
        })

    # Incoming relationships
    cur.execute("""
        SELECT
            r.id, r.source_type, r.source_id, r.target_type, r.target_id,
            r.relationship_type, r.strength, r.confidence, r.last_interaction
        FROM relationships r
        WHERE r.target_type = 'company' AND r.target_id = ?
        ORDER BY r.strength DESC, r.confidence DESC
    """, (company_id,))

    incoming = []
    for row in cur.fetchall():
        source_name = _get_entity_name(db_path, row["source_type"], row["source_id"])
        incoming.append({
            "relationship_type": row["relationship_type"],
            "source": source_name,
            "source_type": row["source_type"],
            "strength": row["strength"],
            "confidence": row["confidence"],
            "last_interaction": row["last_interaction"],
        })

    conn.close()
    return {"outgoing": outgoing, "incoming": incoming}


def _get_entity_name(db_path: str, entity_type: str, entity_id: int) -> str:
    """Get the name of an entity by type and ID."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    if entity_type == "company":
        cur.execute("SELECT name FROM companies WHERE id = ?", (entity_id,))
    elif entity_type == "contact":
        cur.execute("SELECT first_name, last_name FROM contacts WHERE id = ?", (entity_id,))
    elif entity_type == "building":
        cur.execute("SELECT name FROM buildings WHERE id = ?", (entity_id,))
    else:
        conn.close()
        return f"Unknown {entity_type}"

    row = cur.fetchone()
    conn.close()

    if not row:
        return f"Unknown {entity_type} #{entity_id}"

    if entity_type == "contact":
        return f"{row['first_name']} {row['last_name']}"

    return row["name"]


def _fetch_leases(db_path: str, company_id: int) -> List[Dict[str, Any]]:
    """Fetch lease information for a company as tenant."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            l.id, l.tenant_company_id, l.building_id, b.name AS building_name,
            b.address, l.square_footage, l.annual_rent, l.rent_psf,
            l.lease_start, l.lease_end, l.status, l.lease_type
        FROM leases l
        JOIN buildings b ON l.building_id = b.id
        WHERE l.tenant_company_id = ?
        ORDER BY l.lease_end DESC
    """, (company_id,))

    leases = []
    for row in cur.fetchall():
        leases.append({
            "id": row["id"],
            "building_name": row["building_name"],
            "address": row["address"],
            "square_footage": row["square_footage"],
            "annual_rent": row["annual_rent"],
            "rent_psf": row["rent_psf"],
            "lease_start": row["lease_start"],
            "lease_end": row["lease_end"],
            "status": row["status"],
            "lease_type": row["lease_type"],
        })

    conn.close()
    return leases


def _fetch_outreach_history(db_path: str, company_id: int) -> List[Dict[str, Any]]:
    """Fetch outreach history for a company."""
    conn = _get_conn(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id, contact_id, company_id, outreach_type, channel, direction,
            outcome, outreach_date, notes, subject, follow_up_date
        FROM outreach_log
        WHERE company_id = ? OR target_company_id = ?
        ORDER BY outreach_date DESC
        LIMIT 20
    """, (company_id, company_id))

    outreach = []
    for row in cur.fetchall():
        contact_name = ""
        if row["contact_id"]:
            contact_name = _get_entity_name(db_path, "contact", row["contact_id"])

        outreach.append({
            "id": row["id"],
            "contact": contact_name,
            "outreach_type": row["outreach_type"],
            "channel": row["channel"],
            "direction": row["direction"],
            "outcome": row["outcome"],
            "outreach_date": row["outreach_date"],
            "notes": row["notes"],
            "subject": row["subject"],
            "follow_up_date": row["follow_up_date"],
        })

    conn.close()
    return outreach


def generate_profile(db_path: str, company_id: int) -> str:
    """Generate a complete markdown profile for a company."""
    # Fetch all data
    info = _fetch_company_info(db_path, company_id)
    if not info:
        return f"# Company Profile\n\nNo company found with ID {company_id}."

    funding = _fetch_funding_history(db_path, company_id)
    hiring = _fetch_hiring_signals(db_path, company_id)
    contacts = _fetch_contacts(db_path, company_id)
    relationships = _fetch_relationships(db_path, company_id)
    leases = _fetch_leases(db_path, company_id)
    outreach = _fetch_outreach_history(db_path, company_id)

    # Build markdown
    md = []
    md.append(f"# {info['name']}")
    md.append("")
    md.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md.append("")

    # Overview section
    md.append("## Overview")
    md.append("")
    md.append(f"- **Industry:** {info['industry'] or 'N/A'}")
    md.append(f"- **Sector:** {info['sector'] or 'N/A'}")
    md.append(f"- **Status:** {info['status'] or 'N/A'}")
    md.append(f"- **Type:** {info['type'] or 'N/A'}")
    md.append(f"- **Category:** {info['category'] or 'Uncategorized'}")
    md.append(f"- **Headquarters:** {info['hq_city'] or 'N/A'}, {info['hq_state'] or 'N/A'}")
    md.append(f"- **Founded:** {info['founded_year'] or 'N/A'}")
    md.append(f"- **Website:** {info['website'] or 'N/A'}")
    md.append("")

    # Financial & Operational Info
    md.append("## Financial & Operational Information")
    md.append("")
    md.append(f"- **Employee Count:** {info['employee_count'] or 'Unknown'}")
    md.append(f"- **Revenue (Est.):** ${info['revenue_est']:,.0f}" if info['revenue_est'] else "- **Revenue (Est.):** Unknown")
    md.append(f"- **Office Space:** {info['office_sf']:,.0f} sq ft" if info['office_sf'] else "- **Office Space:** Unknown")
    md.append(f"- **Cash Reserves:** ${info['cash_reserves']:,.0f}" if info['cash_reserves'] else "- **Cash Reserves:** Unknown")
    if info['cash_updated_at']:
        md.append(f"  - *Updated: {info['cash_updated_at']}*")
    md.append("")

    # Graph Scores
    md.append("## Network Scores")
    md.append("")
    md.append(f"- **Centrality Score:** {info['centrality_score'] or 0:.4f}")
    md.append(f"- **Leverage Score:** {info['leverage_score'] or 0:.4f}")
    md.append(f"- **Influence Score:** {info['influence_score'] or 0:.4f}")
    md.append(f"- **Adjacency Index:** {info['adjacency_index'] or 0:.4f}")
    md.append(f"- **Cluster ID:** {info['cluster_id']}")
    md.append(f"- **Maturity:** {'Yes' if info['mature'] else 'No'}")
    md.append("")

    # Opportunity Scores
    md.append("## Opportunity Assessment")
    md.append("")
    md.append(f"- **Opportunity Score:** {info['opportunity_score'] or 0:.2f}")
    md.append(f"- **Chain Lease Probability:** {info['chain_lease_prob'] or 0:.2%}")
    md.append(f"- **Chain Score:** {info['chain_score'] or 0:.2f}")
    md.append("")

    # Funding History
    md.append("## Funding History")
    md.append("")
    if funding:
        for event in funding:
            md.append(f"### {event['round_type']} - {event['event_date']}")
            if event['amount']:
                md.append(f"**Amount:** ${event['amount']:,.0f}")
            if event['lead_investor']:
                md.append(f"**Lead Investor:** {event['lead_investor']}")
            if event['post_valuation']:
                md.append(f"**Post-Money Valuation:** ${event['post_valuation']:,.0f}")
            if event['details']:
                md.append(f"**Details:** {event['details']}")
            if event['source']:
                md.append(f"**Source:** {event['source']}")
            md.append("")
    else:
        md.append("No data available.")
        md.append("")

    # Hiring Signals
    md.append("## Hiring Activity")
    md.append("")
    if hiring:
        for signal in hiring:
            title = signal['title'] or signal['role_title'] or "Job Opening"
            md.append(f"### {title}")
            md.append(f"**Type:** {signal['signal_type'] or 'N/A'}")
            md.append(f"**Location:** {signal['location'] or 'N/A'}")
            md.append(f"**Date:** {signal['signal_date']}")
            md.append(f"**Relevance:** {signal['relevance'] or 'N/A'}")
            if signal['description']:
                md.append(f"**Description:** {signal['description']}")
            if signal['details']:
                md.append(f"**Details:** {signal['details']}")
            if signal['source_url']:
                md.append(f"**Link:** {signal['source_url']}")
            md.append("")
    else:
        md.append("No data available.")
        md.append("")

    # Contacts
    md.append("## Key Contacts")
    md.append("")
    if contacts:
        for contact in contacts:
            name = f"{contact['first_name']} {contact['last_name']}"
            md.append(f"### {name}")
            md.append(f"**Title:** {contact['title'] or 'N/A'}")
            md.append(f"**Role Level:** {contact['role_level'] or 'N/A'}")
            if contact['email']:
                md.append(f"**Email:** {contact['email']}")
            if contact['phone']:
                md.append(f"**Phone:** {contact['phone']}")
            if contact['linkedin_url']:
                md.append(f"**LinkedIn:** {contact['linkedin_url']}")
            md.append(f"**Status:** {contact['status'] or 'Unknown'}")
            md.append(f"**Priority Score:** {contact['priority_score'] or 0}")
            md.append("")
    else:
        md.append("No data available.")
        md.append("")

    # Relationships
    md.append("## Relationships")
    md.append("")
    if relationships['outgoing']:
        md.append("### Outgoing Relationships")
        for rel in relationships['outgoing']:
            md.append(f"- **{rel['relationship_type']}** → {rel['target']} ({rel['target_type']})")
            md.append(f"  - Strength: {rel['strength']}, Confidence: {rel['confidence']:.2f}")
            if rel['last_interaction']:
                md.append(f"  - Last Interaction: {rel['last_interaction']}")
        md.append("")
    else:
        md.append("### Outgoing Relationships")
        md.append("No data available.")
        md.append("")

    if relationships['incoming']:
        md.append("### Incoming Relationships")
        for rel in relationships['incoming']:
            md.append(f"- **{rel['relationship_type']}** ← {rel['source']} ({rel['source_type']})")
            md.append(f"  - Strength: {rel['strength']}, Confidence: {rel['confidence']:.2f}")
            if rel['last_interaction']:
                md.append(f"  - Last Interaction: {rel['last_interaction']}")
        md.append("")
    else:
        md.append("### Incoming Relationships")
        md.append("No data available.")
        md.append("")

    # Leases
    md.append("## Real Estate Leases")
    md.append("")
    if leases:
        for lease in leases:
            md.append(f"### {lease['building_name']}")
            md.append(f"**Address:** {lease['address']}")
            md.append(f"**Square Footage:** {lease['square_footage']:,.0f}" if lease['square_footage'] else "**Square Footage:** Unknown")
            md.append(f"**Annual Rent:** ${lease['annual_rent']:,.0f}" if lease['annual_rent'] else "**Annual Rent:** Unknown")
            md.append(f"**Rent/SF:** ${lease['rent_psf']:.2f}" if lease['rent_psf'] else "**Rent/SF:** Unknown")
            md.append(f"**Lease Period:** {lease['lease_start']} to {lease['lease_end']}")
            md.append(f"**Status:** {lease['status']}")
            md.append(f"**Type:** {lease['lease_type']}")
            md.append("")
    else:
        md.append("No data available.")
        md.append("")

    # Outreach History
    md.append("## Outreach History (Last 20)")
    md.append("")
    if outreach:
        for item in outreach:
            contact_str = f" ({item['contact']})" if item['contact'] else ""
            md.append(f"- **{item['outreach_date']}**: {item['outreach_type']}{contact_str}")
            if item['subject']:
                md.append(f"  - Subject: {item['subject']}")
            md.append(f"  - Channel: {item['channel']}, Direction: {item['direction']}")
            md.append(f"  - Outcome: {item['outcome']}")
            if item['follow_up_date']:
                md.append(f"  - Follow-up due: {item['follow_up_date']}")
            if item['notes']:
                md.append(f"  - Notes: {item['notes']}")
        md.append("")
    else:
        md.append("No data available.")
        md.append("")

    # Notes
    if info['notes']:
        md.append("## Notes")
        md.append("")
        md.append(info['notes'])
        md.append("")

    md.append("---")
    md.append("")
    md.append(f"*Profile ID: {company_id}*")

    return "\n".join(md)


def save_profile(db_path: str, company_id: int, output_dir: str) -> str:
    """Generate and save a company profile to a markdown file.

    Args:
        db_path: Path to the SQLite database
        company_id: Company ID to profile
        output_dir: Directory to save the profile in

    Returns:
        Path to the created file
    """
    # Get company name
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM companies WHERE id = ?", (company_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise ValueError(f"Company with ID {company_id} not found")

    company_name = row["name"]

    # Generate profile
    profile_md = generate_profile(db_path, company_id)

    # Create safe filename
    safe_name = "".join(c if c.isalnum() or c in " -_" else "" for c in company_name)
    safe_name = safe_name.replace(" ", "_")
    filename = f"{safe_name}_{company_id}_profile.md"
    filepath = os.path.join(output_dir, filename)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Write file
    with open(filepath, "w") as f:
        f.write(profile_md)

    return filepath


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate company profiles from the relationship engine database."
    )
    parser.add_argument("--company", type=str, help="Company name to profile")
    parser.add_argument("--id", type=int, help="Company ID to profile")
    parser.add_argument("--output", type=str, default="./", help="Output directory (default: current directory)")

    args = parser.parse_args()

    if not args.company and not args.id:
        parser.print_help()
        sys.exit(1)

    db_path = get_db_path()
    company_id = _find_company_id(db_path, company_id=args.id, company_name=args.company)

    if not company_id:
        print(f"Error: Company not found (name={args.company}, id={args.id})", file=sys.stderr)
        sys.exit(1)

    profile_md = generate_profile(db_path, company_id)
    print(profile_md)

    # Also save if output directory specified
    if args.output and args.output != "./":
        try:
            filepath = save_profile(db_path, company_id, args.output)
            print(f"\nProfile saved to: {filepath}", file=sys.stderr)
        except Exception as e:
            print(f"Error saving profile: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
