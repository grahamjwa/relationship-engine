import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from graph_engine import get_db_path

import logging
import argparse
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SEARCH_QUERIES = [
    "NYC hedge fund office expansion 2025 2026",
    "hedge fund new office lease Manhattan",
    "asset manager relocating to NYC",
    "private equity firm expanding office New York",
    "hedge fund office space growth Midtown",
    "financial firm office relocation Hudson Yards",
    "hedge fund lease renewal NYC",
    "asset management firm new headquarters New York",
    "hedge fund moving offices New York City",
    "Manhattan asset manager office expansion announcement"
]


def get_search_client():
    """Get search client with graceful fallback if API key missing."""
    try:
        from scrapers.search_client import search_general
        return search_general
    except ImportError:
        logger.warning("Could not import search_client. Web search disabled.")
        return None
    except Exception as e:
        logger.warning(f"Search client initialization failed: {e}. Web search disabled.")
        return None


def get_classifier():
    """Get signal classifier with graceful fallback if unavailable."""
    try:
        from scrapers.signal_classifier import classify_batch
        return classify_batch
    except ImportError:
        logger.warning("Could not import signal_classifier. Classification disabled.")
        return None
    except Exception as e:
        logger.warning(f"Signal classifier initialization failed: {e}. Classification disabled.")
        return None


def match_to_existing(company_name: str, db_path: str) -> Optional[Tuple[int, str]]:
    """
    Fuzzy match company against existing companies in database.

    Args:
        company_name: Company name to match
        db_path: Path to database

    Returns:
        Tuple of (company_id, matched_name) if found, None otherwise
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Search for similar companies using LIKE
        cursor.execute(
            "SELECT id, name FROM companies WHERE name LIKE ? LIMIT 1",
            (f"%{company_name}%",)
        )
        result = cursor.fetchone()
        conn.close()

        if result:
            return result
        return None
    except Exception as e:
        logger.error(f"Error matching company '{company_name}': {e}")
        return None


def insert_opportunity(company_name: str, signal_type: str, details: str,
                      source_url: str, db_path: str) -> bool:
    """
    Insert hiring signal into database.

    Args:
        company_name: Company name
        signal_type: Type of signal ('new_office' or 'press_announcement')
        details: Details about the opportunity
        source_url: Source URL
        db_path: Path to database

    Returns:
        True if successful, False otherwise
    """
    try:
        # First try to match to existing company
        match = match_to_existing(company_name, db_path)
        if not match:
            logger.debug(f"No matching company found for '{company_name}'")
            return False

        company_id = match[0]

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(
            """INSERT INTO hiring_signals
               (company_id, signal_type, relevance, details, source_url, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (company_id, signal_type, 'high', details, source_url, datetime.utcnow().isoformat())
        )

        conn.commit()
        conn.close()

        logger.info(f"Inserted signal for {company_name}: {signal_type}")
        return True

    except Exception as e:
        logger.error(f"Error inserting opportunity for '{company_name}': {e}")
        return False


def run_opportunity_scan(max_queries: Optional[int] = None,
                        db_path: Optional[str] = None,
                        dry_run: bool = False) -> Dict:
    """
    Main opportunity scanning function.

    Args:
        max_queries: Limit number of queries (None for all)
        db_path: Path to database (uses config default if None)
        dry_run: If True, don't write to database

    Returns:
        Dictionary with results summary
    """
    if db_path is None:
        db_path = get_db_path()

    search_client = get_search_client()
    classifier = get_classifier()

    if not search_client:
        logger.error("Cannot run scan without search client. Check API configuration.")
        return {
            'status': 'error',
            'message': 'Search client unavailable',
            'opportunities': []
        }

    results = {
        'status': 'success',
        'queries_executed': 0,
        'opportunities_found': 0,
        'opportunities_inserted': 0,
        'opportunities': [],
        'dry_run': dry_run
    }

    queries = SEARCH_QUERIES[:max_queries] if max_queries else SEARCH_QUERIES
    logger.info(f"Starting opportunity scan with {len(queries)} queries")

    for query in queries:
        try:
            logger.info(f"Searching: {query}")
            search_results = search_client(query, max_results=5)

            if not search_results:
                logger.debug(f"No results for query: {query}")
                continue

            results['queries_executed'] += 1

            # Process each search result
            for result in search_results:
                try:
                    title = result.get('title', '')
                    url = result.get('url', '')
                    snippet = result.get('snippet', '')

                    # Extract company names and classify if classifier available
                    if classifier:
                        classified = classifier([{
                            'title': title,
                            'snippet': snippet,
                            'url': url
                        }])

                        for classification in classified:
                            company_name = classification.get('company_name')
                            signal_type = classification.get('signal_type', 'press_announcement')
                            confidence = classification.get('confidence', 0)

                            if company_name and confidence > 0.6:
                                results['opportunities_found'] += 1
                                logger.info(f"Found opportunity: {company_name} ({signal_type})")

                                opportunity = {
                                    'company_name': company_name,
                                    'signal_type': signal_type,
                                    'confidence': confidence,
                                    'source_url': url,
                                    'details': f"{title}\n{snippet}",
                                    'inserted': False
                                }

                                # Insert if not dry run
                                if not dry_run:
                                    if insert_opportunity(
                                        company_name, signal_type,
                                        opportunity['details'],
                                        url, db_path
                                    ):
                                        results['opportunities_inserted'] += 1
                                        opportunity['inserted'] = True

                                results['opportunities'].append(opportunity)
                    else:
                        # If no classifier, log result but don't insert
                        logger.debug(f"Skipping classification for: {title[:50]}...")

                except Exception as e:
                    logger.error(f"Error processing search result: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error executing search query '{query}': {e}")
            continue

    logger.info(f"Scan complete. Found {results['opportunities_found']} opportunities, "
                f"inserted {results['opportunities_inserted']}")

    return results


def generate_opportunity_report(results: Dict) -> str:
    """
    Generate markdown summary of findings.

    Args:
        results: Results from run_opportunity_scan

    Returns:
        Markdown formatted report
    """
    report = []
    report.append("# Opportunity Finder Report\n")
    report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report.append(f"**Status:** {results.get('status', 'unknown').upper()}\n")

    if results.get('dry_run'):
        report.append("**(DRY RUN - No data written to database)**\n")

    report.append(f"\n## Summary\n")
    report.append(f"- Queries Executed: {results.get('queries_executed', 0)}")
    report.append(f"- Opportunities Found: {results.get('opportunities_found', 0)}")
    report.append(f"- Opportunities Inserted: {results.get('opportunities_inserted', 0)}\n")

    if results.get('opportunities'):
        report.append(f"\n## Opportunities\n")
        for opp in results['opportunities']:
            report.append(f"\n### {opp.get('company_name', 'Unknown')}\n")
            report.append(f"- **Signal Type:** {opp.get('signal_type', 'unknown')}")
            report.append(f"- **Confidence:** {opp.get('confidence', 0):.1%}")
            report.append(f"- **Inserted:** {'Yes' if opp.get('inserted') else 'No'}")
            report.append(f"- **Source:** [{opp.get('source_url', 'N/A')[:60]}...]({opp.get('source_url', '#')})")
            report.append(f"\n**Details:**\n```\n{opp.get('details', 'N/A')[:300]}...\n```\n")
    else:
        report.append("\n## Opportunities\n\nNo opportunities found.\n")

    return "\n".join(report)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Scan web for NYC hedge fund/asset manager office expansion signals'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without writing to database'
    )
    parser.add_argument(
        '--max-queries',
        type=int,
        default=None,
        help='Limit number of queries to execute'
    )
    parser.add_argument(
        '--report',
        action='store_true',
        help='Generate and print markdown report'
    )

    args = parser.parse_args()

    logger.info(f"Starting opportunity finder (dry_run={args.dry_run})")

    results = run_opportunity_scan(
        max_queries=args.max_queries,
        dry_run=args.dry_run
    )

    if args.report:
        report = generate_opportunity_report(results)
        print(report)
    else:
        print(f"\nScan complete: {results['opportunities_inserted']} opportunities inserted")


if __name__ == '__main__':
    main()
