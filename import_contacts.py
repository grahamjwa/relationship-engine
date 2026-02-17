import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from graph_engine import get_db_path

import csv
import sqlite3
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_or_create_company(conn, company_name):
    """Get company ID by name (case-insensitive), create if not found."""
    if not company_name or company_name.strip() == '':
        return None

    company_name = company_name.strip()
    cursor = conn.cursor()

    # Check if company exists (case-insensitive)
    cursor.execute(
        'SELECT id FROM companies WHERE LOWER(name) = LOWER(?)',
        (company_name,)
    )
    result = cursor.fetchone()

    if result:
        return result[0]

    # Create new company
    cursor.execute(
        'INSERT INTO companies (name) VALUES (?)',
        (company_name,)
    )
    conn.commit()
    logger.info(f"Created new company: {company_name}")
    return cursor.lastrowid


def parse_contacts_csv(filepath):
    """Parse contacts CSV file."""
    contacts = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                contacts.append(row)
        logger.info(f"Parsed {len(contacts)} contacts from {filepath}")
    except Exception as e:
        logger.error(f"Error parsing CSV: {e}")
        sys.exit(1)

    return contacts


def import_contacts(filepath, dry_run=False):
    """Import contacts from CSV file."""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)

    contacts = parse_contacts_csv(filepath)

    inserted = 0
    updated = 0
    skipped = 0

    for row in contacts:
        try:
            first_name = row.get('first_name', '').strip()
            last_name = row.get('last_name', '').strip()
            company = row.get('company', '').strip()
            title = row.get('title', '').strip()
            role_level = row.get('role_level', '').strip()
            email = row.get('email', '').strip()
            phone = row.get('phone', '').strip()
            linkedin_url = row.get('linkedin_url', '').strip()
            alma_mater = row.get('alma_mater', '').strip()
            previous_companies = row.get('previous_companies', '').strip()
            notes = row.get('notes', '').strip()

            # Validate required fields
            if not first_name or not last_name:
                logger.warning(f"Skipping row with missing first_name or last_name: {row}")
                skipped += 1
                continue

            if not company:
                logger.warning(f"Skipping {first_name} {last_name} with missing company")
                skipped += 1
                continue

            # Get or create company
            company_id = get_or_create_company(conn, company)
            if not company_id:
                logger.warning(f"Skipping {first_name} {last_name} - could not get company ID")
                skipped += 1
                continue

            cursor = conn.cursor()

            # Check if contact exists (by first_name, last_name, company_id)
            cursor.execute(
                'SELECT id FROM contacts WHERE first_name = ? AND last_name = ? AND company_id = ?',
                (first_name, last_name, company_id)
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing contact
                contact_id = existing[0]
                cursor.execute(
                    '''UPDATE contacts
                       SET title = ?, role_level = ?, email = ?, phone = ?,
                           linkedin_url = ?, alma_mater = ?, previous_companies = ?, notes = ?
                       WHERE id = ?''',
                    (title, role_level, email, phone, linkedin_url, alma_mater, previous_companies, notes, contact_id)
                )
                conn.commit()
                logger.info(f"Updated contact: {first_name} {last_name} at {company}")
                updated += 1
            else:
                # Insert new contact
                cursor.execute(
                    '''INSERT INTO contacts
                       (first_name, last_name, company_id, title, role_level, email, phone, linkedin_url, alma_mater, previous_companies, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (first_name, last_name, company_id, title, role_level, email, phone, linkedin_url, alma_mater, previous_companies, notes)
                )
                conn.commit()
                logger.info(f"Inserted contact: {first_name} {last_name} at {company}")
                inserted += 1

        except Exception as e:
            logger.error(f"Error processing row {row}: {e}")
            skipped += 1
            continue

    conn.close()

    # Print summary
    print("\n" + "="*60)
    print("IMPORT SUMMARY")
    print("="*60)
    print(f"Inserted: {inserted}")
    print(f"Updated:  {updated}")
    print(f"Skipped:  {skipped}")
    print(f"Total:    {inserted + updated + skipped}")
    print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Import contacts from CSV file'
    )
    parser.add_argument('filepath', help='Path to contacts CSV file')
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview import without making changes'
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    if not os.path.exists(args.filepath):
        logger.error(f"File not found: {args.filepath}")
        sys.exit(1)

    import_contacts(args.filepath, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
