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


def find_contact_by_name(conn, first_name, last_name):
    """Find contact ID by first_name and last_name."""
    first_name = first_name.strip()
    last_name = last_name.strip()

    cursor = conn.cursor()
    cursor.execute(
        'SELECT id FROM contacts WHERE first_name = ? AND last_name = ?',
        (first_name, last_name)
    )
    result = cursor.fetchone()

    if result:
        return result[0]
    return None


def relationship_exists(conn, contact_a_id, contact_b_id):
    """Check if relationship already exists between two contacts."""
    cursor = conn.cursor()

    # Check in either direction since relationships might be bidirectional or directional
    cursor.execute(
        '''SELECT id FROM relationships
           WHERE (contact_a_id = ? AND contact_b_id = ?)
              OR (contact_a_id = ? AND contact_b_id = ?)''',
        (contact_a_id, contact_b_id, contact_b_id, contact_a_id)
    )
    result = cursor.fetchone()
    return result is not None


def parse_relationships_csv(filepath):
    """Parse relationships CSV file."""
    relationships = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                relationships.append(row)
        logger.info(f"Parsed {len(relationships)} relationships from {filepath}")
    except Exception as e:
        logger.error(f"Error parsing CSV: {e}")
        sys.exit(1)

    return relationships


def import_relationships(filepath, dry_run=False):
    """Import relationships from CSV file."""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)

    relationships = parse_relationships_csv(filepath)

    inserted = 0
    skipped = 0
    failed = 0

    for row in relationships:
        try:
            contact_a_first = row.get('contact_a_first', '').strip()
            contact_a_last = row.get('contact_a_last', '').strip()
            contact_b_first = row.get('contact_b_first', '').strip()
            contact_b_last = row.get('contact_b_last', '').strip()
            relationship_type = row.get('relationship_type', '').strip()
            strength = row.get('strength', '').strip()
            direction = row.get('direction', '').strip()
            context = row.get('context', '').strip()
            notes = row.get('notes', '').strip()

            # Validate required fields
            if not contact_a_first or not contact_a_last:
                logger.warning(f"Skipping row with missing contact_a names: {row}")
                failed += 1
                continue

            if not contact_b_first or not contact_b_last:
                logger.warning(f"Skipping row with missing contact_b names: {row}")
                failed += 1
                continue

            if not relationship_type:
                logger.warning(f"Skipping relationship with missing type: {contact_a_first} {contact_a_last} <-> {contact_b_first} {contact_b_last}")
                failed += 1
                continue

            # Find contacts
            contact_a_id = find_contact_by_name(conn, contact_a_first, contact_a_last)
            contact_b_id = find_contact_by_name(conn, contact_b_first, contact_b_last)

            if not contact_a_id:
                logger.warning(f"Contact not found: {contact_a_first} {contact_a_last}")
                failed += 1
                continue

            if not contact_b_id:
                logger.warning(f"Contact not found: {contact_b_first} {contact_b_last}")
                failed += 1
                continue

            # Check if relationship already exists
            if relationship_exists(conn, contact_a_id, contact_b_id):
                logger.info(f"Relationship already exists: {contact_a_first} {contact_a_last} <-> {contact_b_first} {contact_b_last}")
                skipped += 1
                continue

            # Parse strength as integer
            try:
                strength_val = int(strength) if strength else None
            except ValueError:
                logger.warning(f"Invalid strength value '{strength}' for {contact_a_first} {contact_a_last} <-> {contact_b_first} {contact_b_last}")
                strength_val = None

            # Insert relationship
            cursor = conn.cursor()
            cursor.execute(
                '''INSERT INTO relationships
                   (contact_a_id, contact_b_id, relationship_type, strength, direction, context, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (contact_a_id, contact_b_id, relationship_type, strength_val, direction, context, notes)
            )
            conn.commit()
            logger.info(f"Inserted relationship: {contact_a_first} {contact_a_last} ({relationship_type}) {contact_b_first} {contact_b_last}")
            inserted += 1

        except Exception as e:
            logger.error(f"Error processing row {row}: {e}")
            failed += 1
            continue

    conn.close()

    # Print summary
    print("\n" + "="*60)
    print("IMPORT SUMMARY")
    print("="*60)
    print(f"Inserted:       {inserted}")
    print(f"Skipped:        {skipped} (already exists)")
    print(f"Failed:         {failed} (contact not found or invalid)")
    print(f"Total Rows:     {inserted + skipped + failed}")
    print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Import relationships from CSV file'
    )
    parser.add_argument('filepath', help='Path to relationships CSV file')
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

    import_relationships(args.filepath, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
