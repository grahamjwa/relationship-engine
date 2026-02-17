"""
Data Quality Audit Script for Relationship Engine
Identifies missing data, stale records, orphaned references, and duplicates.
Generates comprehensive audit reports and optionally fixes issues.
"""

import os
import sys
import sqlite3
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config  # noqa: F401

from core.graph_engine import get_db_path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _get_conn(db_path: str = None) -> sqlite3.Connection:
    """Get database connection."""
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def audit_missing_emails(db_path: str = None) -> List[Dict]:
    """
    Find contacts without email addresses.

    Returns:
        List of contact dicts with id, name, company_id
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    results = []

    try:
        cur.execute("""
            SELECT
                c.id,
                c.first_name,
                c.last_name,
                c.company_id,
                co.name as company_name
            FROM contacts c
            LEFT JOIN companies co ON c.company_id = co.id
            WHERE c.email IS NULL OR c.email = ''
            ORDER BY c.last_name, c.first_name
        """)

        for row in cur.fetchall():
            results.append({
                "contact_id": row[0],
                "name": f"{row[1]} {row[2]}",
                "company_id": row[3],
                "company_name": row[4]
            })

        logger.info(f"Found {len(results)} contacts without email addresses")

    except Exception as e:
        logger.error(f"Error in audit_missing_emails: {str(e)}")
    finally:
        conn.close()

    return results


def audit_stale_contacts(days: int = 180, db_path: str = None) -> List[Dict]:
    """
    Find contacts not updated in N days.

    Args:
        days: Number of days (default 180)
        db_path: Database path

    Returns:
        List of contact dicts with id, name, last update date
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    results = []

    try:
        cutoff_date = (date.today() - timedelta(days=days)).isoformat()

        cur.execute("""
            SELECT
                c.id,
                c.first_name,
                c.last_name,
                c.company_id,
                co.name as company_name,
                c.updated_at
            FROM contacts c
            LEFT JOIN companies co ON c.company_id = co.id
            WHERE c.updated_at < ?
            ORDER BY c.updated_at ASC
        """, (cutoff_date,))

        for row in cur.fetchall():
            results.append({
                "contact_id": row[0],
                "name": f"{row[1]} {row[2]}",
                "company_id": row[3],
                "company_name": row[4],
                "last_updated": row[5]
            })

        logger.info(f"Found {len(results)} contacts not updated in {days} days")

    except Exception as e:
        logger.error(f"Error in audit_stale_contacts: {str(e)}")
    finally:
        conn.close()

    return results


def audit_orphaned_records(db_path: str = None) -> Dict:
    """
    Find orphaned records (relationships/funding pointing to non-existent entities).

    Returns:
        Dict with different types of orphaned records
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    orphaned = {
        "orphaned_relationships_source": [],
        "orphaned_relationships_target": [],
        "orphaned_funding": [],
        "count": 0
    }

    try:
        # Check relationships with non-existent source contact
        cur.execute("""
            SELECT r.id, r.contact_id_a, r.company_id_a
            FROM relationships r
            WHERE r.contact_id_a IS NOT NULL
                AND r.contact_id_a NOT IN (SELECT id FROM contacts)
        """)

        for row in cur.fetchall():
            orphaned["orphaned_relationships_source"].append({
                "relationship_id": row[0],
                "contact_id": row[1],
                "company_id": row[2],
                "issue": "Source contact does not exist"
            })

        # Check relationships with non-existent target contact
        cur.execute("""
            SELECT r.id, r.contact_id_b, r.company_id_b
            FROM relationships r
            WHERE r.contact_id_b IS NOT NULL
                AND r.contact_id_b NOT IN (SELECT id FROM contacts)
        """)

        for row in cur.fetchall():
            orphaned["orphaned_relationships_target"].append({
                "relationship_id": row[0],
                "contact_id": row[1],
                "company_id": row[2],
                "issue": "Target contact does not exist"
            })

        # Check funding with non-existent company
        cur.execute("""
            SELECT f.id, f.company_id, f.investor_company_id
            FROM funding f
            WHERE f.company_id NOT IN (SELECT id FROM companies)
                OR (f.investor_company_id IS NOT NULL
                    AND f.investor_company_id NOT IN (SELECT id FROM companies))
        """)

        for row in cur.fetchall():
            issue = ""
            if row[1] and row[1] not in [r[0] for r in cur.execute("SELECT id FROM companies")]:
                issue = "Company does not exist"
            elif row[2] and row[2] not in [r[0] for r in cur.execute("SELECT id FROM companies")]:
                issue = "Investor company does not exist"

            orphaned["orphaned_funding"].append({
                "funding_id": row[0],
                "company_id": row[1],
                "investor_company_id": row[2],
                "issue": issue
            })

        orphaned["count"] = (
            len(orphaned["orphaned_relationships_source"]) +
            len(orphaned["orphaned_relationships_target"]) +
            len(orphaned["orphaned_funding"])
        )

        logger.info(f"Found {orphaned['count']} orphaned records")

    except Exception as e:
        logger.error(f"Error in audit_orphaned_records: {str(e)}")
    finally:
        conn.close()

    return orphaned


def audit_duplicates(db_path: str = None) -> Dict:
    """
    Find potential duplicate companies and contacts.

    Returns:
        Dict with duplicate entries
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    duplicates = {
        "duplicate_companies": [],
        "duplicate_contacts": [],
        "count": 0
    }

    try:
        # Find companies with similar names
        cur.execute("""
            SELECT id, name FROM companies
            ORDER BY name
        """)

        companies = cur.fetchall()
        company_dict = {row[0]: row[1] for row in companies}

        # Simple substring matching for duplicates
        checked = set()
        for i, comp1 in enumerate(companies):
            if comp1[0] in checked:
                continue

            for comp2 in companies[i+1:]:
                if comp2[0] in checked:
                    continue

                # Check if one is contained in other (substring match)
                name1_lower = comp1[1].lower().strip()
                name2_lower = comp2[1].lower().strip()

                if name1_lower in name2_lower or name2_lower in name1_lower:
                    if comp1[1] != comp2[1]:  # Not exact match (those are real duplicates)
                        duplicates["duplicate_companies"].append({
                            "company_id_1": comp1[0],
                            "name_1": comp1[1],
                            "company_id_2": comp2[0],
                            "name_2": comp2[1],
                            "reason": "Similar names (substring match)"
                        })
                        checked.add(comp2[0])

        # Find duplicate contacts (same first + last at same company)
        cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.company_id, c.email
            FROM contacts c
            ORDER BY c.company_id, c.first_name, c.last_name
        """)

        contacts = cur.fetchall()
        contact_dict = {}

        for contact in contacts:
            key = (contact[3], contact[1].lower().strip(), contact[2].lower().strip())

            if key in contact_dict:
                # Found duplicate
                prev_contact = contact_dict[key]
                duplicates["duplicate_contacts"].append({
                    "contact_id_1": prev_contact[0],
                    "name_1": f"{prev_contact[1]} {prev_contact[2]}",
                    "email_1": prev_contact[4],
                    "contact_id_2": contact[0],
                    "name_2": f"{contact[1]} {contact[2]}",
                    "email_2": contact[4],
                    "company_id": contact[3],
                    "reason": "Same first + last name at same company"
                })
            else:
                contact_dict[key] = contact

        duplicates["count"] = (
            len(duplicates["duplicate_companies"]) +
            len(duplicates["duplicate_contacts"])
        )

        logger.info(f"Found {duplicates['count']} potential duplicates")

    except Exception as e:
        logger.error(f"Error in audit_duplicates: {str(e)}")
    finally:
        conn.close()

    return duplicates


def audit_data_completeness(db_path: str = None) -> Dict:
    """
    Audit data completeness for companies.

    Returns:
        Dict with completion percentages
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    completeness = {
        "total_companies": 0,
        "companies_with_industry": 0,
        "companies_with_sector": 0,
        "companies_with_status": 0,
        "companies_with_contacts": 0,
        "companies_with_relationships": 0,
        "completion_scores": {}
    }

    try:
        # Get total companies
        cur.execute("SELECT COUNT(*) FROM companies")
        total = cur.fetchone()[0]
        completeness["total_companies"] = total

        if total == 0:
            return completeness

        # Industry
        cur.execute("SELECT COUNT(*) FROM companies WHERE industry IS NOT NULL AND industry != ''")
        completeness["companies_with_industry"] = cur.fetchone()[0]

        # Sector
        cur.execute("SELECT COUNT(*) FROM companies WHERE sector IS NOT NULL AND sector != ''")
        completeness["companies_with_sector"] = cur.fetchone()[0]

        # Status
        cur.execute("SELECT COUNT(*) FROM companies WHERE status IS NOT NULL AND status != ''")
        completeness["companies_with_status"] = cur.fetchone()[0]

        # Companies with at least one contact
        cur.execute("""
            SELECT COUNT(DISTINCT company_id) FROM contacts
            WHERE company_id IS NOT NULL
        """)
        completeness["companies_with_contacts"] = cur.fetchone()[0]

        # Companies with at least one relationship
        cur.execute("""
            SELECT COUNT(DISTINCT company_id_a) FROM relationships
            WHERE company_id_a IS NOT NULL
        """)
        completeness["companies_with_relationships"] = cur.fetchone()[0]

        # Calculate percentages
        if total > 0:
            completeness["completion_scores"] = {
                "industry_pct": round(100 * completeness["companies_with_industry"] / total, 1),
                "sector_pct": round(100 * completeness["companies_with_sector"] / total, 1),
                "status_pct": round(100 * completeness["companies_with_status"] / total, 1),
                "has_contacts_pct": round(100 * completeness["companies_with_contacts"] / total, 1),
                "has_relationships_pct": round(100 * completeness["companies_with_relationships"] / total, 1),
            }

        logger.info(f"Data completeness: {completeness['completion_scores']}")

    except Exception as e:
        logger.error(f"Error in audit_data_completeness: {str(e)}")
    finally:
        conn.close()

    return completeness


def generate_audit_report(db_path: str = None) -> str:
    """
    Generate comprehensive audit report in markdown format.

    Returns:
        Markdown report
    """
    if db_path is None:
        db_path = get_db_path()

    missing_emails = audit_missing_emails(db_path)
    stale_contacts = audit_stale_contacts(180, db_path)
    orphaned = audit_orphaned_records(db_path)
    duplicates = audit_duplicates(db_path)
    completeness = audit_data_completeness(db_path)

    report_lines = [
        "# Data Quality Audit Report",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "## Summary",
        ""
    ]

    # Summary counts
    total_issues = (
        len(missing_emails) +
        len(stale_contacts) +
        orphaned["count"] +
        duplicates["count"]
    )

    report_lines.extend([
        f"- **Total Issues Found:** {total_issues}",
        f"- **Contacts without email:** {len(missing_emails)}",
        f"- **Stale contacts (>180 days):** {len(stale_contacts)}",
        f"- **Orphaned records:** {orphaned['count']}",
        f"- **Potential duplicates:** {duplicates['count']}",
        "",
        "## Data Completeness",
        ""
    ])

    if completeness["total_companies"] > 0:
        scores = completeness["completion_scores"]
        report_lines.extend([
            f"- **Total Companies:** {completeness['total_companies']}",
            f"- **With Industry:** {scores.get('industry_pct', 0)}%",
            f"- **With Sector:** {scores.get('sector_pct', 0)}%",
            f"- **With Status:** {scores.get('status_pct', 0)}%",
            f"- **With Contacts:** {scores.get('has_contacts_pct', 0)}%",
            f"- **With Relationships:** {scores.get('has_relationships_pct', 0)}%",
        ])
    else:
        report_lines.append("No companies in database.")

    report_lines.extend([
        "",
        "## Missing Emails",
        ""
    ])

    if missing_emails:
        report_lines.append(f"Found {len(missing_emails)} contacts without email addresses:")
        report_lines.append("")
        for contact in missing_emails[:20]:
            company = contact['company_name'] or 'Unknown'
            report_lines.append(f"- {contact['name']} ({company})")
    else:
        report_lines.append("All contacts have email addresses ✓")

    report_lines.extend([
        "",
        "## Stale Contacts",
        ""
    ])

    if stale_contacts:
        report_lines.append(f"Found {len(stale_contacts)} contacts not updated in 180+ days:")
        report_lines.append("")
        for contact in stale_contacts[:20]:
            report_lines.append(f"- {contact['name']} - Last updated: {contact['last_updated']}")
    else:
        report_lines.append("No stale contacts found ✓")

    report_lines.extend([
        "",
        "## Orphaned Records",
        ""
    ])

    if orphaned["count"] > 0:
        report_lines.append(f"Found {orphaned['count']} orphaned records:")
        report_lines.append("")

        if orphaned["orphaned_relationships_source"]:
            report_lines.append(f"### Orphaned Relationships (Invalid Source)")
            for record in orphaned["orphaned_relationships_source"][:10]:
                report_lines.append(f"- Relationship {record['relationship_id']}: {record['issue']}")

        if orphaned["orphaned_relationships_target"]:
            report_lines.append(f"### Orphaned Relationships (Invalid Target)")
            for record in orphaned["orphaned_relationships_target"][:10]:
                report_lines.append(f"- Relationship {record['relationship_id']}: {record['issue']}")

        if orphaned["orphaned_funding"]:
            report_lines.append(f"### Orphaned Funding Records")
            for record in orphaned["orphaned_funding"][:10]:
                report_lines.append(f"- Funding {record['funding_id']}: {record['issue']}")
    else:
        report_lines.append("No orphaned records found ✓")

    report_lines.extend([
        "",
        "## Duplicate Records",
        ""
    ])

    if duplicates["count"] > 0:
        report_lines.append(f"Found {duplicates['count']} potential duplicates:")
        report_lines.append("")

        if duplicates["duplicate_companies"]:
            report_lines.append("### Duplicate Companies")
            for dup in duplicates["duplicate_companies"][:10]:
                report_lines.append(f"- '{dup['name_1']}' (ID: {dup['company_id_1']}) ≈ '{dup['name_2']}' (ID: {dup['company_id_2']})")

        if duplicates["duplicate_contacts"]:
            report_lines.append("### Duplicate Contacts")
            for dup in duplicates["duplicate_contacts"][:10]:
                report_lines.append(f"- {dup['name_1']} (ID: {dup['contact_id_1']}) ≈ {dup['name_2']} (ID: {dup['contact_id_2']})")
    else:
        report_lines.append("No duplicate records found ✓")

    return "\n".join(report_lines)


def auto_fix_orphans(dry_run: bool = True, db_path: str = None) -> Dict:
    """
    Automatically delete orphaned records.

    Args:
        dry_run: If True, only report what would be deleted (default: True)
        db_path: Database path

    Returns:
        Dict with results
    """
    if db_path is None:
        db_path = get_db_path()

    conn = _get_conn(db_path)
    cur = conn.cursor()

    result = {
        "dry_run": dry_run,
        "deleted": {
            "relationships_source": 0,
            "relationships_target": 0,
            "funding": 0
        },
        "status": "success"
    }

    try:
        # Find orphaned relationships (source)
        cur.execute("""
            SELECT id FROM relationships
            WHERE contact_id_a IS NOT NULL
                AND contact_id_a NOT IN (SELECT id FROM contacts)
        """)
        orphaned_source_ids = [row[0] for row in cur.fetchall()]

        # Find orphaned relationships (target)
        cur.execute("""
            SELECT id FROM relationships
            WHERE contact_id_b IS NOT NULL
                AND contact_id_b NOT IN (SELECT id FROM contacts)
        """)
        orphaned_target_ids = [row[0] for row in cur.fetchall()]

        # Find orphaned funding
        cur.execute("""
            SELECT id FROM funding
            WHERE company_id NOT IN (SELECT id FROM companies)
                OR (investor_company_id IS NOT NULL
                    AND investor_company_id NOT IN (SELECT id FROM companies))
        """)
        orphaned_funding_ids = [row[0] for row in cur.fetchall()]

        if not dry_run:
            # Delete orphaned relationships
            if orphaned_source_ids:
                placeholders = ','.join('?' * len(orphaned_source_ids))
                cur.execute(f"DELETE FROM relationships WHERE id IN ({placeholders})", orphaned_source_ids)
                result["deleted"]["relationships_source"] = len(orphaned_source_ids)

            if orphaned_target_ids:
                placeholders = ','.join('?' * len(orphaned_target_ids))
                cur.execute(f"DELETE FROM relationships WHERE id IN ({placeholders})", orphaned_target_ids)
                result["deleted"]["relationships_target"] = len(orphaned_target_ids)

            if orphaned_funding_ids:
                placeholders = ','.join('?' * len(orphaned_funding_ids))
                cur.execute(f"DELETE FROM funding WHERE id IN ({placeholders})", orphaned_funding_ids)
                result["deleted"]["funding"] = len(orphaned_funding_ids)

            conn.commit()
            logger.info(f"Deleted {sum(result['deleted'].values())} orphaned records")
        else:
            result["deleted"]["relationships_source"] = len(orphaned_source_ids)
            result["deleted"]["relationships_target"] = len(orphaned_target_ids)
            result["deleted"]["funding"] = len(orphaned_funding_ids)
            logger.info(f"DRY RUN: Would delete {sum(result['deleted'].values())} orphaned records")

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        logger.error(f"Error in auto_fix_orphans: {str(e)}")
    finally:
        conn.close()

    return result


def main():
    """CLI interface."""
    parser = argparse.ArgumentParser(description="Data quality audit for relationship engine")
    parser.add_argument("--report", action="store_true", help="Generate full audit report")
    parser.add_argument("--fix-orphans", action="store_true", help="Delete orphaned records")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Dry run (default for --fix-orphans)")
    parser.add_argument("--db", type=str, help="Database path (optional)")

    args = parser.parse_args()

    if args.report:
        report = generate_audit_report(args.db)
        print(report)

    elif args.fix_orphans:
        dry_run = args.dry_run
        result = auto_fix_orphans(dry_run=dry_run, db_path=args.db)

        mode = "DRY RUN" if dry_run else "EXECUTE"
        print(f"\n{mode} - Auto-fix Orphaned Records")
        print(f"Relationships (invalid source): {result['deleted']['relationships_source']}")
        print(f"Relationships (invalid target): {result['deleted']['relationships_target']}")
        print(f"Funding records: {result['deleted']['funding']}")
        print(f"Total: {sum(result['deleted'].values())}")

        if dry_run:
            print("\nTo actually delete these records, run with: --fix-orphans (without --dry-run)")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
