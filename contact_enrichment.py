import sqlite3
import sys
import logging
from typing import Optional, Dict, List
from datetime import datetime

sys.path.insert(0, '/sessions/sharp-admiring-curie/relationship_engine')
from graph_engine import get_db_path

# Try to import Anthropic for enrichment
try:
    from anthropic import Anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False
    Anthropic = None

# Try to import search client
try:
    from scrapers.search_client import search_general
    HAS_SEARCH = True
except ImportError:
    HAS_SEARCH = False
    search_general = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def migrate_contact_schema(db_path: str) -> None:
    """Migrate database schema to add contact enrichment columns."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE contacts ADD COLUMN education TEXT")
        logger.info("Added education column to contacts table")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            logger.debug("education column already exists")
        else:
            raise

    try:
        cursor.execute("ALTER TABLE contacts ADD COLUMN linkedin_url TEXT")
        logger.info("Added linkedin_url column to contacts table")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            logger.debug("linkedin_url column already exists")
        else:
            raise

    try:
        cursor.execute("ALTER TABLE contacts ADD COLUMN previous_companies TEXT")
        logger.info("Added previous_companies column to contacts table")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            logger.debug("previous_companies column already exists")
        else:
            raise

    try:
        cursor.execute("ALTER TABLE contacts ADD COLUMN alma_mater TEXT")
        logger.info("Added alma_mater column to contacts table")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            logger.debug("alma_mater column already exists")
        else:
            raise

    conn.commit()
    conn.close()


def get_contact(contact_id: int, db_path: Optional[str] = None) -> Optional[Dict]:
    """Retrieve contact from database.

    Args:
        contact_id: ID of contact to retrieve
        db_path: Path to database (uses default if None)

    Returns:
        Dict with contact data or None if not found
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM contacts WHERE id = ?", (contact_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def search_contact_info(name: str, company: str) -> Optional[str]:
    """Search for contact information online.

    Args:
        name: Contact name
        company: Company name

    Returns:
        Search results as string or None if search unavailable
    """
    if not HAS_SEARCH:
        logger.warning("search_general not available - skipping web search")
        return None

    try:
        query = f"{name} {company} linkedin profile"
        results = search_general(query)
        logger.info(f"Searched for {name} at {company}")
        return results
    except Exception as e:
        logger.error(f"Error searching for contact info: {e}")
        return None


def extract_enrichment_data(name: str, company: str, search_results: str) -> Dict:
    """Extract structured enrichment data from search results using Claude.

    Args:
        name: Contact name
        company: Company name
        search_results: Raw search results text

    Returns:
        Dict with extracted: linkedin_url, previous_companies, alma_mater, education
    """
    if not HAS_ANTHROPIC:
        logger.warning("Anthropic not available - cannot extract enrichment data")
        return {}

    try:
        client = Anthropic()
        prompt = f"""Extract contact enrichment information from these search results for {name} at {company}.

Search results:
{search_results}

Extract and return only the following fields in JSON format:
- linkedin_url: URL to LinkedIn profile (if found)
- previous_companies: Comma-separated list of previous employer names
- alma_mater: Name of university/college
- education: Degree and field of study

Return only valid JSON with empty strings for missing fields. Example:
{{"linkedin_url": "https://linkedin.com/in/...", "previous_companies": "...", "alma_mater": "...", "education": "..."}}"""

        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=500,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        # Parse response
        import json
        response_text = message.content[0].text
        data = json.loads(response_text)
        logger.info(f"Extracted enrichment data for {name}")
        return data
    except Exception as e:
        logger.error(f"Error extracting enrichment data: {e}")
        return {}


def enrich_contact(contact_id: int, db_path: Optional[str] = None) -> Dict:
    """Enrich contact profile with LinkedIn URL, companies, education.

    Args:
        contact_id: ID of contact to enrich
        db_path: Path to database (uses default if None)

    Returns:
        Dict with enrichment results
    """
    if db_path is None:
        db_path = get_db_path()

    contact = get_contact(contact_id, db_path)
    if not contact:
        logger.error(f"Contact {contact_id} not found")
        return {}

    name = contact.get('name', '')
    company = contact.get('company', '')

    if not name:
        logger.warning(f"Contact {contact_id} has no name - skipping enrichment")
        return {}

    logger.info(f"Enriching contact {contact_id}: {name}")

    # Search for information
    search_results = search_contact_info(name, company)
    if not search_results:
        logger.warning(f"No search results for {name}")
        return {}

    # Extract structured data
    enrichment_data = extract_enrichment_data(name, company, search_results)
    if not enrichment_data:
        logger.warning(f"Could not extract enrichment data for {name}")
        return {}

    # Save enrichment
    save_enrichment(contact_id, enrichment_data, db_path)
    return enrichment_data


def save_enrichment(contact_id: int, data: Dict, db_path: Optional[str] = None) -> None:
    """Update contacts table with enrichment data.

    Args:
        contact_id: ID of contact to update
        data: Dict with enrichment fields
        db_path: Path to database (uses default if None)
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE contacts
            SET linkedin_url = ?, previous_companies = ?, alma_mater = ?, education = ?
            WHERE id = ?
        """, (
            data.get('linkedin_url', ''),
            data.get('previous_companies', ''),
            data.get('alma_mater', ''),
            data.get('education', ''),
            contact_id
        ))

        conn.commit()
        logger.info(f"Saved enrichment data for contact {contact_id}")
    except Exception as e:
        logger.error(f"Error saving enrichment: {e}")
        raise
    finally:
        conn.close()


def batch_enrich(limit: int = 10, db_path: Optional[str] = None) -> List[Dict]:
    """Enrich contacts missing linkedin_url.

    Args:
        limit: Maximum number of contacts to enrich
        db_path: Path to database (uses default if None)

    Returns:
        List of enriched contact records
    """
    if db_path is None:
        db_path = get_db_path()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT id FROM contacts
            WHERE linkedin_url IS NULL OR linkedin_url = ''
            LIMIT ?
        """, (limit,))

        contacts_to_enrich = [row['id'] for row in cursor.fetchall()]
        logger.info(f"Found {len(contacts_to_enrich)} contacts to enrich")

        results = []
        for contact_id in contacts_to_enrich:
            try:
                result = enrich_contact(contact_id, db_path)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to enrich contact {contact_id}: {e}")

        logger.info(f"Batch enriched {len(results)} contacts")
        return results
    finally:
        conn.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Enrich contact profiles")
    parser.add_argument('--contact-id', type=int, help='Enrich specific contact')
    parser.add_argument('--batch', action='store_true', help='Batch enrich contacts missing enrichment')
    parser.add_argument('--limit', type=int, default=10, help='Batch limit (default: 10)')
    parser.add_argument('--db', help='Database path (uses default if not specified)')

    args = parser.parse_args()
    db_path = args.db or get_db_path()

    # Initialize schema
    migrate_contact_schema(db_path)

    if args.contact_id:
        enrichment = enrich_contact(args.contact_id, db_path)
        if enrichment:
            print(f"Enriched contact {args.contact_id}:")
            for key, value in enrichment.items():
                print(f"  {key}: {value}")
        else:
            print(f"Failed to enrich contact {args.contact_id}")

    elif args.batch:
        results = batch_enrich(args.limit, db_path)
        print(f"Batch enriched {len(results)} contacts")

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
