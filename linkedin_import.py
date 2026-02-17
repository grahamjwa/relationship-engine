import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from graph_engine import get_db_path

import logging
import argparse
import sqlite3
import csv
from datetime import datetime
from typing import List, Dict, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_linkedin_csv(file_path: str) -> List[Dict]:
    """
    Parse LinkedIn exported connections CSV.

    Expected headers: First Name, Last Name, Email Address, Company, Position, Connected On

    Args:
        file_path: Path to LinkedIn export CSV

    Returns:
        List of connection dictionaries
    """
    connections = []

    try:
        # Handle UTF-8 BOM encoding
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                logger.error("CSV file has no headers")
                return []

            for row_num, row in enumerate(reader, start=2):  # start=2 accounts for header row
                try:
                    connection = {
                        'first_name': row.get('First Name', '').strip(),
                        'last_name': row.get('Last Name', '').strip(),
                        'email': row.get('Email Address', '').strip(),
                        'company': row.get('Company', '').strip(),
                        'position': row.get('Position', '').strip(),
                        'connected_on': row.get('Connected On', '').strip(),
                    }

                    # Validate required fields
                    if not connection['first_name'] or not connection['last_name']:
                        logger.warning(f"Row {row_num}: Missing name fields, skipping")
                        continue

                    connections.append(connection)

                except Exception as e:
                    logger.warning(f"Row {row_num}: Error parsing row: {e}")
                    continue

        logger.info(f"Parsed {len(connections)} connections from {file_path}")
        return connections

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error parsing CSV: {e}")
        return []


def match_contact(first_name: str, last_name: str, db_path: str) -> Optional[int]:
    """
    Check if contact exists in database.

    Args:
        first_name: Contact's first name
        last_name: Contact's last name
        db_path: Path to database

    Returns:
        Contact ID if found, None otherwise
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            """SELECT id FROM contacts
               WHERE first_name = ? AND last_name = ? LIMIT 1""",
            (first_name, last_name)
        )
        result = cursor.fetchone()
        conn.close()

        if result:
            return result[0]
        return None

    except Exception as e:
        logger.error(f"Error matching contact {first_name} {last_name}: {e}")
        return None


def match_company(company_name: str, db_path: str) -> Optional[int]:
    """
    Check if company exists, using fuzzy LIKE match.

    Args:
        company_name: Company name to match
        db_path: Path to database

    Returns:
        Company ID if found, None otherwise
    """
    if not company_name:
        return None

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Use LIKE for fuzzy matching
        cursor.execute(
            """SELECT id FROM companies WHERE name LIKE ? LIMIT 1""",
            (f"%{company_name}%",)
        )
        result = cursor.fetchone()
        conn.close()

        if result:
            return result[0]
        return None

    except Exception as e:
        logger.error(f"Error matching company '{company_name}': {e}")
        return None


def flag_target_contacts(contacts: List[Dict], db_path: str) -> Tuple[List[int], int]:
    """
    Identify contacts at target companies (high_growth_target, prospect status).

    Args:
        contacts: List of contact dictionaries
        db_path: Path to database

    Returns:
        Tuple of (list of contact IDs at target companies, count of matches)
    """
    target_contact_ids = []

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for contact in contacts:
            contact_id = match_contact(contact['first_name'], contact['last_name'], db_path)
            if not contact_id:
                continue

            # Check if contact's company is a target
            if contact.get('company'):
                company_id = match_company(contact['company'], db_path)

                if company_id:
                    cursor.execute(
                        """SELECT status FROM companies WHERE id = ?""",
                        (company_id,)
                    )
                    result = cursor.fetchone()

                    if result and result[0] in ('high_growth_target', 'prospect'):
                        target_contact_ids.append(contact_id)
                        logger.info(f"Flagged target contact: {contact['first_name']} {contact['last_name']} "
                                  f"at {contact['company']}")

        conn.close()
        return target_contact_ids, len(target_contact_ids)

    except Exception as e:
        logger.error(f"Error flagging target contacts: {e}")
        return [], 0


def import_connections(file_path: str,
                      db_path: Optional[str] = None,
                      dry_run: bool = False,
                      create_companies: bool = False) -> Dict:
    """
    Import LinkedIn connections to database.

    Args:
        file_path: Path to LinkedIn CSV export
        db_path: Path to database (uses config default if None)
        dry_run: If True, don't write to database
        create_companies: If True, create unknown companies

    Returns:
        Dictionary with import results
    """
    if db_path is None:
        db_path = get_db_path()

    connections = parse_linkedin_csv(file_path)
    if not connections:
        return {
            'status': 'error',
            'message': 'No connections parsed from CSV',
            'results': []
        }

    results = {
        'status': 'success',
        'total_connections': len(connections),
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'skipped_reasons': {},
        'target_contacts_flagged': 0,
        'results': [],
        'dry_run': dry_run
    }

    logger.info(f"Starting import of {len(connections)} connections")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for connection in connections:
            try:
                first_name = connection['first_name']
                last_name = connection['last_name']
                company_name = connection['company']
                email = connection['email']
                position = connection['position']

                result_entry = {
                    'name': f"{first_name} {last_name}",
                    'company': company_name,
                    'status': None,
                    'reason': None
                }

                # Check if contact exists
                contact_id = match_contact(first_name, last_name, db_path)

                if contact_id:
                    # Update existing contact
                    if not dry_run:
                        cursor.execute(
                            """UPDATE contacts SET title = ?, updated_at = ?
                               WHERE id = ?""",
                            (position, datetime.utcnow().isoformat(), contact_id)
                        )
                        if email and not cursor.execute(
                            "SELECT linkedin_url FROM contacts WHERE id = ?",
                            (contact_id,)
                        ).fetchone()[0]:
                            cursor.execute(
                                """UPDATE contacts SET linkedin_url = ? WHERE id = ?""",
                                (email, contact_id)
                            )

                    result_entry['status'] = 'updated'
                    results['updated'] += 1
                    logger.info(f"Updated: {first_name} {last_name}")

                else:
                    # Check if company exists
                    company_id = match_company(company_name, db_path) if company_name else None

                    if company_id:
                        # Create new contact
                        if not dry_run:
                            cursor.execute(
                                """INSERT INTO contacts
                                   (first_name, last_name, email, title, company_id, linkedin_url, created_at)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                                (first_name, last_name, email, position, company_id,
                                 email, datetime.utcnow().isoformat())
                            )

                        result_entry['status'] = 'created'
                        results['created'] += 1
                        logger.info(f"Created: {first_name} {last_name} at {company_name}")

                    else:
                        # Company not found
                        reason = 'unknown_company'
                        result_entry['status'] = 'skipped'
                        result_entry['reason'] = reason
                        results['skipped'] += 1
                        results['skipped_reasons'][reason] = results['skipped_reasons'].get(reason, 0) + 1
                        logger.debug(f"Skipped: {first_name} {last_name} ({reason})")

                results['results'].append(result_entry)

            except Exception as e:
                logger.error(f"Error processing connection {connection}: {e}")
                results['skipped'] += 1
                continue

        if not dry_run:
            conn.commit()
        conn.close()

        # Flag target contacts
        created_and_updated = [r for r in results['results']
                              if r['status'] in ('created', 'updated')]
        target_contacts, count = flag_target_contacts(
            [c for c in connections
             if f"{c['first_name']} {c['last_name']}" in [r['name'] for r in created_and_updated]],
            db_path
        )
        results['target_contacts_flagged'] = count

        logger.info(f"Import complete: created={results['created']}, "
                   f"updated={results['updated']}, "
                   f"skipped={results['skipped']}")

    except Exception as e:
        logger.error(f"Error during import: {e}")
        results['status'] = 'error'
        results['message'] = str(e)

    return results


def generate_import_report(results: Dict) -> str:
    """
    Generate markdown summary of import.

    Args:
        results: Results from import_connections

    Returns:
        Markdown formatted report
    """
    report = []
    report.append("# LinkedIn Import Report\n")
    report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report.append(f"**Status:** {results.get('status', 'unknown').upper()}\n")

    if results.get('dry_run'):
        report.append("**(DRY RUN - No data written to database)**\n")

    report.append(f"\n## Summary\n")
    report.append(f"- Total Connections: {results.get('total_connections', 0)}")
    report.append(f"- Created: {results.get('created', 0)}")
    report.append(f"- Updated: {results.get('updated', 0)}")
    report.append(f"- Skipped: {results.get('skipped', 0)}")
    report.append(f"- Target Contacts Flagged: {results.get('target_contacts_flagged', 0)}\n")

    if results.get('skipped_reasons'):
        report.append(f"\n## Skip Reasons\n")
        for reason, count in results['skipped_reasons'].items():
            report.append(f"- {reason}: {count}")
        report.append("")

    if results.get('results'):
        created = [r for r in results['results'] if r['status'] == 'created']
        updated = [r for r in results['results'] if r['status'] == 'updated']

        if created:
            report.append(f"\n## Created ({len(created)})\n")
            for item in created[:10]:
                report.append(f"- {item['name']} at {item['company']}")
            if len(created) > 10:
                report.append(f"- ... and {len(created) - 10} more")
            report.append("")

        if updated:
            report.append(f"\n## Updated ({len(updated)})\n")
            for item in updated[:10]:
                report.append(f"- {item['name']} at {item['company']}")
            if len(updated) > 10:
                report.append(f"- ... and {len(updated) - 10} more")
            report.append("")

    if results.get('message'):
        report.append(f"\n## Error\n\n{results['message']}\n")

    return "\n".join(report)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Import LinkedIn connections CSV export to database'
    )
    parser.add_argument(
        'csv_file',
        help='Path to LinkedIn connections CSV export'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without writing to database'
    )
    parser.add_argument(
        '--create-companies',
        action='store_true',
        help='Create unknown companies (not implemented yet)'
    )
    parser.add_argument(
        '--report',
        action='store_true',
        help='Generate and print markdown report'
    )

    args = parser.parse_args()

    if not os.path.exists(args.csv_file):
        logger.error(f"CSV file not found: {args.csv_file}")
        sys.exit(1)

    logger.info(f"Starting LinkedIn import from {args.csv_file} (dry_run={args.dry_run})")

    results = import_connections(
        args.csv_file,
        dry_run=args.dry_run,
        create_companies=args.create_companies
    )

    if args.report:
        report = generate_import_report(results)
        print(report)
    else:
        print(f"\nImport complete: {results['created']} created, "
              f"{results['updated']} updated, {results['skipped']} skipped")


if __name__ == '__main__':
    main()
